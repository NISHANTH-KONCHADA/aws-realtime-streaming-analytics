"""
Failure Routing and Error Handling for Stream Processor.

Separates non-retryable poison pills (quarantined to S3 rejected partition)
from transient system errors (reported via Kinesis partial batch failure mechanism).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from storage.s3_archive import S3ArchiveManager

logger = logging.getLogger(__name__)


class ErrorCategory:
    MALFORMED_JSON = "MALFORMED_JSON"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    BUSINESS_RULE_VIOLATION = "BUSINESS_RULE_VIOLATION"
    SYSTEM_RETRYABLE_ERROR = "SYSTEM_RETRYABLE_ERROR"


class StreamErrorHandler:
    """
    Handles error disposition, dead-letter quarantine, and failure categorization.
    """

    def __init__(self, s3_archive: S3ArchiveManager) -> None:
        """
        Args:
            s3_archive: Configured S3ArchiveManager instance.
        """
        self.s3_archive = s3_archive

    def handle_poison_pill(
        self,
        raw_payload: str,
        reason: str,
        error_message: str,
        record_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Quarantine non-retryable invalid records to S3 to avoid blocking the shard.

        Args:
            raw_payload: Raw string representation of the bad event.
            reason: Error classification category.
            error_message: Specific error explanation.
            record_metadata: Shard ID, sequence number, partition key.

        Returns:
            S3 quarantine key.
        """
        logger.warning(
            f"Quarantining poison pill record: Category={reason}, Error={error_message}"
        )
        try:
            return self.s3_archive.archive_rejected_event(
                raw_payload=raw_payload,
                reason=reason,
                error_details=error_message,
                metadata=record_metadata,
            )
        except Exception as ex:
            logger.error(
                f"Failed to quarantine rejected event to S3: {ex}. Payload: {raw_payload[:100]}..."
            )
            return None
