"""
AWS Lambda Kinesis Stream Processor Handler.

Consumes Kinesis record batches, decodes Base64 data, validates JSON & schema contracts,
enforces DynamoDB conditional-write idempotency, archives partitioned historical events
to Amazon S3, publishes CloudWatch operational metrics, and reports partial batch failures
to prevent duplicate processing of successful records.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
import json
import logging
import os
from typing import Any, Dict, List, Optional

from analytics.metrics import CloudWatchMetricsEmitter
from lambda_src.error_handler import ErrorCategory, StreamErrorHandler
from lambda_src.idempotency import IdempotencyDecision, IdempotencyManager
from lambda_src.validators import BusinessRuleViolation, EventValidator, ValidationError
from storage.dynamodb_repository import DynamoDBRepository
from storage.s3_archive import S3ArchiveManager

logger = logging.getLogger("stream_processor")
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


# Initialize module-level singletons (reused across warm Lambda invocations)
REGION_NAME = os.getenv("AWS_REGION", "us-east-1")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE_NAME", "ecommerce-transactions")
S3_BUCKET = os.getenv("S3_ARCHIVE_BUCKET", "ecommerce-streaming-analytics-archive")
CW_NAMESPACE = os.getenv("CLOUDWATCH_METRIC_NAMESPACE", "EcommerceStreamingAnalytics")
ENVIRONMENT = os.getenv("METRIC_ENVIRONMENT", "development")
FLAGGED_THRESHOLD = float(os.getenv("FLAGGED_AMOUNT_THRESHOLD", "10000.00"))

# Service dependencies
_dynamodb_repo: Optional[DynamoDBRepository] = None
_s3_archive: Optional[S3ArchiveManager] = None
_metrics_emitter: Optional[CloudWatchMetricsEmitter] = None
_validator: Optional[EventValidator] = None
_idempotency_mgr: Optional[IdempotencyManager] = None
_error_handler: Optional[StreamErrorHandler] = None


def get_dependencies(
    custom_dynamo: Optional[DynamoDBRepository] = None,
    custom_s3: Optional[S3ArchiveManager] = None,
    custom_metrics: Optional[CloudWatchMetricsEmitter] = None,
) -> tuple[
    DynamoDBRepository,
    S3ArchiveManager,
    CloudWatchMetricsEmitter,
    EventValidator,
    IdempotencyManager,
    StreamErrorHandler,
]:
    """Lazy initialization of services for warm Lambda containers and unit testing."""
    global _dynamodb_repo, _s3_archive, _metrics_emitter, _validator, _idempotency_mgr, _error_handler

    if custom_dynamo is not None:
        _dynamodb_repo = custom_dynamo
    elif _dynamodb_repo is None:
        _dynamodb_repo = DynamoDBRepository(table_name=DYNAMODB_TABLE, region_name=REGION_NAME)

    if custom_s3 is not None:
        _s3_archive = custom_s3
    elif _s3_archive is None:
        _s3_archive = S3ArchiveManager(bucket_name=S3_BUCKET, region_name=REGION_NAME)

    if custom_metrics is not None:
        _metrics_emitter = custom_metrics
    elif _metrics_emitter is None:
        _metrics_emitter = CloudWatchMetricsEmitter(
            namespace=CW_NAMESPACE,
            environment=ENVIRONMENT,
            region_name=REGION_NAME,
        )

    if _validator is None:
        _validator = EventValidator(flagged_threshold_amount=FLAGGED_THRESHOLD)

    if _idempotency_mgr is None or custom_dynamo is not None:
        _idempotency_mgr = IdempotencyManager(_dynamodb_repo)

    if _error_handler is None or custom_s3 is not None:
        _error_handler = StreamErrorHandler(_s3_archive)

    return (
        _dynamodb_repo,
        _s3_archive,
        _metrics_emitter,
        _validator,
        _idempotency_mgr,
        _error_handler,
    )


def lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    AWS Lambda entrypoint triggered by Kinesis event source mapping.

    Args:
        event: AWS Lambda Kinesis event containing 'Records'.
        context: Lambda execution context.

    Returns:
        Dictionary containing 'batchItemFailures' for Kinesis partial batch response.
    """
    (
        repo,
        s3_archiver,
        metrics,
        validator,
        idempotency_mgr,
        error_handler,
    ) = get_dependencies()

    records = event.get("Records", [])
    batch_item_failures: List[Dict[str, str]] = []
    events_to_archive: List[Dict[str, Any]] = []

    metrics.add_metric("EventsReceived", len(records))
    now_utc = datetime.now(timezone.utc)

    for record in records:
        kinesis_data = record.get("kinesis", {})
        seq_num = kinesis_data.get("sequenceNumber", "")
        partition_key = kinesis_data.get("partitionKey", "")
        raw_b64 = kinesis_data.get("data", "")

        record_meta = {
            "sequenceNumber": seq_num,
            "partitionKey": partition_key,
            "approximateArrivalTimestamp": kinesis_data.get("approximateArrivalTimestamp"),
        }

        # 1. Base64 Decode
        try:
            raw_bytes = base64.b64decode(raw_b64)
            raw_str = raw_bytes.decode("utf-8")
        except Exception as ex:
            logger.error(f"Malformed Base64 record {seq_num}: {ex}")
            metrics.add_metric("InvalidEvents", 1)
            error_handler.handle_poison_pill(
                raw_payload=str(raw_b64),
                reason=ErrorCategory.MALFORMED_JSON,
                error_message=f"Base64 decode failed: {ex}",
                record_metadata=record_meta,
            )
            # Poison pill is quarantined -> do not retry
            continue

        # 2. JSON Parse
        try:
            payload_dict = json.loads(raw_str)
        except json.JSONDecodeError as ex:
            logger.error(f"Malformed JSON payload in record {seq_num}: {ex}")
            metrics.add_metric("InvalidEvents", 1)
            error_handler.handle_poison_pill(
                raw_payload=raw_str,
                reason=ErrorCategory.MALFORMED_JSON,
                error_message=f"JSON parse error: {ex}",
                record_metadata=record_meta,
            )
            continue

        # 3. Schema & Business Rules Validation
        try:
            sanitized_event, flagged_for_review = validator.validate_event(payload_dict)
        except (ValidationError, BusinessRuleViolation) as ex:
            category = (
                ErrorCategory.BUSINESS_RULE_VIOLATION
                if isinstance(ex, BusinessRuleViolation)
                else ErrorCategory.SCHEMA_VIOLATION
            )
            logger.warning(f"Record {seq_num} validation failed: {ex}")
            metrics.add_metric("InvalidEvents", 1)
            error_handler.handle_poison_pill(
                raw_payload=raw_str,
                reason=category,
                error_message=str(ex),
                record_metadata=record_meta,
            )
            continue

        # 4. Idempotency Check & Conditional Persistence
        decision, err_msg = idempotency_mgr.process_and_persist(
            event=sanitized_event,
            flagged_for_review=flagged_for_review,
        )

        if decision == IdempotencyDecision.DUPLICATE:
            metrics.add_metric("DuplicateEvents", 1)
            logger.info(
                f"Duplicate event {sanitized_event['event_id']} safely ignored without re-processing."
            )
            continue

        elif decision == IdempotencyDecision.STORAGE_ERROR:
            # Infrastructure failure (DynamoDB throttle, outage) -> mark for partial batch retry
            logger.error(
                f"Transient storage failure for record {seq_num}: {err_msg}. Triggering partial batch retry."
            )
            batch_item_failures.append({"itemIdentifier": seq_num})
            # In Kinesis FIFO shard ordering, failure on record N halts subsequent processing for this batch
            break

        # 5. Successfully Processed Event
        metrics.add_metric("EventsProcessed", 1)

        # Record payment status outcome metrics
        if sanitized_event["status"] == "SUCCESS":
            metrics.add_metric("PaymentSuccess", 1)
        elif sanitized_event["status"] == "FAILED":
            metrics.add_metric("PaymentFailed", 1)

        if flagged_for_review:
            metrics.add_metric("FlaggedTransactions", 1)

        # Processing latency calculation: (now_utc - event_timestamp)
        try:
            evt_ts = datetime.fromisoformat(sanitized_event["event_timestamp"])
            latency_ms = (now_utc - evt_ts).total_seconds() * 1000.0
            if latency_ms >= 0:
                metrics.add_metric("ProcessingLatencyMs", latency_ms, unit="Milliseconds")
        except Exception:
            pass

        events_to_archive.append(sanitized_event)

    # 6. S3 Batch Historical Archiving
    if events_to_archive:
        try:
            s3_archiver.archive_processed_batch(events_to_archive)
        except Exception as ex:
            logger.error(f"S3 archival failure: {ex}. Marking batch for retry.")
            # If S3 write fails, report the first archived item as failed so Kinesis retries
            for rec in records:
                k_data = rec.get("kinesis", {})
                first_seq = k_data.get("sequenceNumber")
                if first_seq and not any(f["itemIdentifier"] == first_seq for f in batch_item_failures):
                    batch_item_failures.append({"itemIdentifier": first_seq})
                    break

    # 7. Publish CloudWatch Metrics
    metrics.flush()

    return {"batchItemFailures": batch_item_failures}
