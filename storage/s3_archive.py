"""
Amazon S3 Historical Event Archiving Manager.

Implements partitioned, date-sharded event storage in S3 optimized for AWS Glue
Data Catalog partition discovery and Amazon Athena serverless SQL analytics.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional
import uuid

import boto3
from botocore.exceptions import ClientError

from producer.event_schema import DecimalEncoder

logger = logging.getLogger(__name__)


class S3ArchiveManager:
    """
    Manages structured archival writes of raw, processed, and rejected streaming events to S3.
    """

    def __init__(
        self,
        bucket_name: str,
        region_name: str = "us-east-1",
        s3_client: Optional[Any] = None,
        raw_prefix: str = "raw-events",
        processed_prefix: str = "processed-events",
        rejected_prefix: str = "rejected-events",
    ) -> None:
        """
        Initialize S3 Archive Manager.

        Args:
            bucket_name: S3 bucket name for archival storage.
            region_name: AWS Region.
            s3_client: Pre-configured boto3 S3 client or mock.
            raw_prefix: Key prefix for raw ingestion events.
            processed_prefix: Key prefix for successfully processed events.
            rejected_prefix: Key prefix for quarantined or malformed events.
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.raw_prefix = raw_prefix.strip("/")
        self.processed_prefix = processed_prefix.strip("/")
        self.rejected_prefix = rejected_prefix.strip("/")

        if s3_client is not None:
            self.s3 = s3_client
        else:
            self.s3 = boto3.client("s3", region_name=self.region_name)

    def _generate_partition_key(
        self,
        prefix: str,
        dt: datetime,
        extra_partition: Optional[str] = None,
    ) -> str:
        """
        Construct Hive-style partition path: prefix/event_date=YYYY-MM-DD/[extra_partition/]
        """
        date_partition = f"event_date={dt.strftime('%Y-%m-%d')}"
        if extra_partition:
            return f"{prefix}/{date_partition}/{extra_partition}"
        return f"{prefix}/{date_partition}"

    def archive_processed_batch(
        self,
        events: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Archive processed events partitioned by event_date and event_type.

        Groups records by partition and writes compact multi-record JSON Lines files
        to avoid the small-file antipattern.

        Args:
            events: List of successfully validated event dictionaries.

        Returns:
            Dict summarizing uploaded files and keys.
        """
        if not events:
            return {"uploaded_keys": [], "total_records": 0}

        now_utc = datetime.now(timezone.utc)
        # Group events by (event_date, event_type)
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for event in events:
            raw_ts = event.get("event_timestamp")
            try:
                if isinstance(raw_ts, str):
                    evt_dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                elif isinstance(raw_ts, datetime):
                    evt_dt = raw_ts
                else:
                    evt_dt = now_utc
            except Exception:
                evt_dt = now_utc

            date_str = evt_dt.strftime("%Y-%m-%d")
            event_type = event.get("event_type", "UNKNOWN")
            partition_key = f"{self.processed_prefix}/event_date={date_str}/event_type={event_type}"

            if partition_key not in grouped:
                grouped[partition_key] = []
            grouped[partition_key].append(event)

        uploaded_keys: List[str] = []
        for partition_path, records in grouped.items():
            file_id = uuid.uuid4().hex[:8]
            timestamp_str = now_utc.strftime("%Y%m%d_%H%M%S")
            s3_key = f"{partition_path}/batch_{timestamp_str}_{file_id}.jsonl"

            # Serialize batch to JSON Lines
            body_content = "\n".join(
                json.dumps(rec, cls=DecimalEncoder) for rec in records
            ) + "\n"

            try:
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=body_content.encode("utf-8"),
                    ContentType="application/x-ndjson",
                )
                uploaded_keys.append(s3_key)
                logger.info(
                    f"Archived {len(records)} events to s3://{self.bucket_name}/{s3_key}"
                )
            except ClientError as e:
                logger.error(f"Failed to upload archive batch to {s3_key}: {e}")
                raise

        return {
            "uploaded_keys": uploaded_keys,
            "total_records": len(events),
        }

    def archive_rejected_event(
        self,
        raw_payload: str,
        reason: str,
        error_details: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Quarantine an invalid or corrupt event to rejected-events partition.

        Args:
            raw_payload: Original decoded string payload.
            reason: Error classification code (e.g. SCHEMA_VIOLATION).
            error_details: Detailed error message or traceback.
            metadata: Optional Kinesis sequence number or stream info.

        Returns:
            The S3 key where the rejected event was stored.
        """
        now_utc = datetime.now(timezone.utc)
        date_str = now_utc.strftime("%Y-%m-%d")
        file_id = uuid.uuid4().hex[:10]
        s3_key = (
            f"{self.rejected_prefix}/event_date={date_str}/reason={reason}/"
            f"rejected_{now_utc.strftime('%Y%m%d_%H%M%S')}_{file_id}.json"
        )

        record = {
            "quarantined_at": now_utc.isoformat(),
            "reason": reason,
            "error_details": error_details,
            "raw_payload": raw_payload,
            "metadata": metadata or {},
        }

        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(record, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            logger.warning(
                f"Quarantined rejected record ({reason}) to s3://{self.bucket_name}/{s3_key}"
            )
            return s3_key
        except ClientError as e:
            logger.error(f"Failed to store rejected event to S3: {e}")
            raise
