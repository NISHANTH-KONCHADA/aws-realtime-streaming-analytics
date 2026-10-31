"""
Storage Coordinator for Lambda Stream Processor.

Bridges operational transactional writes to DynamoDB with analytical batch
archiving to S3.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from storage.dynamodb_repository import DynamoDBRepository
from storage.s3_archive import S3ArchiveManager

logger = logging.getLogger(__name__)


class StorageCoordinator:
    """
    Coordinates DynamoDB operational state and S3 analytical archiving.
    """

    def __init__(
        self,
        dynamodb_repo: DynamoDBRepository,
        s3_archive: S3ArchiveManager,
    ) -> None:
        self.dynamodb_repo = dynamodb_repo
        self.s3_archive = s3_archive

    def persist_operational(
        self,
        event: Dict[str, Any],
        flagged_for_review: bool,
    ) -> Tuple[bool, bool]:
        """
        Write to DynamoDB with conditional idempotency.
        Returns: (success: bool, is_duplicate: bool)
        """
        return self.dynamodb_repo.save_transaction_idempotent(
            event=event,
            flagged_for_review=flagged_for_review,
        )

    def archive_batch_to_s3(
        self,
        events: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Archive valid, non-duplicate processed batch to S3.
        """
        return self.s3_archive.archive_processed_batch(events)
