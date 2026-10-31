"""
Kinesis Data Streams Producer.

Publishes transaction events to Amazon Kinesis Data Streams using the PutRecords
API with robust batching, exponential backoff retries with full jitter, individual
record failure inspection, and partition key distribution strategies.
"""

from __future__ import annotations

import base64
import json
import logging
import math
import random
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from producer.event_schema import DecimalEncoder

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [Producer] %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class KinesisProducer:
    """
    High-throughput, resilient Kinesis Data Streams record producer.
    """

    # Maximum records allowed per PutRecords request by AWS Kinesis
    MAX_KINESIS_BATCH_SIZE: int = 500
    # Maximum payload size in bytes per PutRecords request (5MB)
    MAX_KINESIS_BATCH_BYTES: int = 5 * 1024 * 1024

    def __init__(
        self,
        stream_name: str,
        region_name: str = "us-east-1",
        kinesis_client: Optional[Any] = None,
        max_retries: int = 5,
        base_backoff_sec: float = 0.3,
        max_backoff_sec: float = 5.0,
        dry_run: bool = False,
    ) -> None:
        """
        Initialize Kinesis Producer.

        Args:
            stream_name: Name of target Amazon Kinesis Data Stream.
            region_name: AWS region where the stream is hosted.
            kinesis_client: Pre-configured boto3 client or mock.
            max_retries: Maximum exponential backoff retries for throttled records.
            base_backoff_sec: Starting delay for backoff.
            max_backoff_sec: Ceiling delay for backoff.
            dry_run: When True, simulates publishing without calling AWS.
        """
        self.stream_name = stream_name
        self.region_name = region_name
        self.max_retries = max_retries
        self.base_backoff_sec = base_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self.dry_run = dry_run

        if kinesis_client is not None:
            self.client = kinesis_client
        elif not self.dry_run:
            retry_config = Config(
                retries={"max_attempts": 3, "mode": "standard"},
                region_name=self.region_name,
            )
            self.client = boto3.client("kinesis", config=retry_config)
        else:
            self.client = None

        self.stats = {
            "total_attempted": 0,
            "total_successful": 0,
            "total_failed": 0,
            "total_retries": 0,
        }

    def select_partition_key(self, event: Dict[str, Any]) -> str:
        """
        Derive partition key for Kinesis record.

        Using customer_id guarantees that events for the same customer arrive in
        strict sequence within the same shard. Fallbacks to transaction_id or event_id.
        """
        customer_id = event.get("customer_id")
        if customer_id and str(customer_id).strip():
            return str(customer_id)
        return event.get("transaction_id") or event.get("event_id") or "DEFAULT_PARTITION"

    def put_single_record(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Publish a single event to Kinesis (convenience method).
        """
        batch_result = self.put_records_batch([event])
        return {
            "successful": batch_result["successful_count"] == 1,
            "failed": batch_result["failed_count"],
            "details": batch_result,
        }

    def put_records_batch(
        self,
        events: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Publish a batch of events to Kinesis with automatic retry of partial failures.

        Inspects individual record responses from PutRecords to handle
        ProvisionedThroughputExceededException on individual records.

        Args:
            events: List of event dictionaries to publish.

        Returns:
            Dictionary containing publish telemetry (successful_count, failed_count, etc.).
        """
        if not events:
            return {"successful_count": 0, "failed_count": 0, "retries": 0}

        self.stats["total_attempted"] += len(events)

        if self.dry_run or self.client is None:
            logger.info(
                f"[DRY-RUN] Simulating publish of {len(events)} records to {self.stream_name}"
            )
            self.stats["total_successful"] += len(events)
            return {
                "successful_count": len(events),
                "failed_count": 0,
                "retries": 0,
                "dry_run": True,
            }

        # Format records for PutRecords
        records_to_send: List[Dict[str, Any]] = []
        for event in events:
            payload_bytes = json.dumps(event, cls=DecimalEncoder).encode("utf-8")
            partition_key = self.select_partition_key(event)
            records_to_send.append({
                "Data": payload_bytes,
                "PartitionKey": partition_key,
                "OriginalEvent": event,
            })

        successful_records = 0
        retries_used = 0
        unprocessed_records = records_to_send

        for attempt in range(self.max_retries + 1):
            if not unprocessed_records:
                break

            # Respect Kinesis PutRecords batch limit (500 records)
            current_batch = unprocessed_records[: self.MAX_KINESIS_BATCH_SIZE]
            remaining_records = unprocessed_records[self.MAX_KINESIS_BATCH_SIZE :]

            # Prepare boto3 payload
            boto3_records = [
                {"Data": item["Data"], "PartitionKey": item["PartitionKey"]}
                for item in current_batch
            ]

            try:
                response = self.client.put_records(
                    StreamName=self.stream_name,
                    Records=boto3_records,
                )

                failed_count = response.get("FailedRecordCount", 0)
                records_response = response.get("Records", [])

                next_retry_records: List[Dict[str, Any]] = []

                for idx, record_resp in enumerate(records_response):
                    if "ErrorCode" in record_resp:
                        error_code = record_resp["ErrorCode"]
                        error_msg = record_resp.get("ErrorMessage", "")
                        # Throttling or internal server error is retryable
                        if error_code in (
                            "ProvisionedThroughputExceededException",
                            "InternalFailure",
                            "ServiceUnavailable",
                        ):
                            next_retry_records.append(current_batch[idx])
                        else:
                            logger.error(
                                f"Non-retryable record rejection: {error_code} - {error_msg}"
                            )
                            self.stats["total_failed"] += 1
                    else:
                        successful_records += 1
                        self.stats["total_successful"] += 1

                # If there are records to retry
                if next_retry_records:
                    retries_used += 1
                    self.stats["total_retries"] += 1
                    sleep_time = self._calculate_backoff(attempt)
                    logger.warning(
                        f"Partial failure: {len(next_retry_records)}/{len(current_batch)} records throttled. "
                        f"Retrying in {sleep_time:.2f}s (Attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(sleep_time)
                    unprocessed_records = next_retry_records + remaining_records
                else:
                    unprocessed_records = remaining_records

            except ClientError as e:
                logger.error(f"Kinesis PutRecords ClientError on attempt {attempt}: {e}")
                if attempt < self.max_retries:
                    sleep_time = self._calculate_backoff(attempt)
                    time.sleep(sleep_time)
                else:
                    # Final attempt failed
                    self.stats["total_failed"] += len(unprocessed_records)
                    unprocessed_records = []
            except Exception as ex:
                logger.exception(f"Unexpected exception during Kinesis put_records: {ex}")
                self.stats["total_failed"] += len(unprocessed_records)
                unprocessed_records = []

        total_failed = len(unprocessed_records)
        self.stats["total_failed"] += total_failed

        return {
            "stream_name": self.stream_name,
            "attempted_count": len(events),
            "successful_count": successful_records,
            "failed_count": total_failed,
            "retries": retries_used,
        }

    def _calculate_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff with full jitter to avoid thundering herd.
        Formula: sleep = random_between(0, min(max_backoff, base * 2^attempt))
        """
        raw_backoff = min(
            self.max_backoff_sec,
            self.base_backoff_sec * math.pow(2, attempt),
        )
        # Full jitter between 50% and 100% of raw_backoff
        return random.uniform(raw_backoff * 0.5, raw_backoff)
