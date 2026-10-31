"""
Idempotency Manager for Kinesis Stream Processing.

Enforces deduplication semantics across streaming retries using DynamoDB conditional
writes. Ensures that at-least-once message deliveries do not duplicate side-effects.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from storage.dynamodb_repository import DynamoDBRepository

logger = logging.getLogger(__name__)


class IdempotencyDecision:
    PROCEED = "PROCEED"
    DUPLICATE = "DUPLICATE"
    STORAGE_ERROR = "STORAGE_ERROR"


class IdempotencyManager:
    """
    Guards stream consumers against duplicate event processing.
    """

    def __init__(self, dynamodb_repo: DynamoDBRepository) -> None:
        """
        Args:
            dynamodb_repo: Initialized DynamoDBRepository instance.
        """
        self.repo = dynamodb_repo

    def process_and_persist(
        self,
        event: Dict[str, Any],
        flagged_for_review: bool = False,
    ) -> Tuple[str, Optional[str]]:
        """
        Execute atomic insert with conditional expression.

        Args:
            event: Validated event dictionary.
            flagged_for_review: Whether the event was flagged for high-value review.

        Returns:
            Tuple of (decision_status, error_message).
            - (PROCEED, None): Fresh event, successfully persisted.
            - (DUPLICATE, None): Duplicate event_id detected and safely discarded.
            - (STORAGE_ERROR, details): Transient or infrastructure storage fault.
        """
        event_id = event.get("event_id", "")

        try:
            success, is_duplicate = self.repo.save_transaction_idempotent(
                event=event,
                flagged_for_review=flagged_for_review,
            )

            if success:
                return IdempotencyDecision.PROCEED, None
            elif is_duplicate:
                return IdempotencyDecision.DUPLICATE, None
            else:
                return (
                    IdempotencyDecision.STORAGE_ERROR,
                    f"Storage write rejected for event_id '{event_id}'",
                )

        except Exception as ex:
            logger.exception(f"Exception during idempotency execution for event {event_id}: {ex}")
            return IdempotencyDecision.STORAGE_ERROR, str(ex)
