"""
DynamoDB Transaction Repository.

Provides operational state storage, idempotency protection via conditional writes,
and query access patterns for transaction records and customer histories.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal
import logging
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DuplicateEventError(Exception):
    """Raised when an event with the same event_id was already persisted."""
    pass


class DynamoDBRepository:
    """
    Manages persistence and conditional idempotency checks against DynamoDB.
    """

    def __init__(
        self,
        table_name: str = "ecommerce-transactions",
        region_name: str = "us-east-1",
        dynamodb_resource: Optional[Any] = None,
        ttl_days: int = 90,
    ) -> None:
        """
        Initialize DynamoDB Repository.

        Args:
            table_name: DynamoDB table name.
            region_name: AWS region.
            dynamodb_resource: Pre-configured boto3 DynamoDB resource or mock.
            ttl_days: Days before item expiration via DynamoDB TTL.
        """
        self.table_name = table_name
        self.region_name = region_name
        self.ttl_days = ttl_days

        if dynamodb_resource is not None:
            self.dynamodb = dynamodb_resource
        else:
            self.dynamodb = boto3.resource("dynamodb", region_name=self.region_name)

        self.table = self.dynamodb.Table(self.table_name)

    def _convert_floats_to_decimal(self, obj: Any) -> Any:
        """Recursively convert float types to Decimal for DynamoDB serialization."""
        if isinstance(obj, float):
            return Decimal(str(obj))
        if isinstance(obj, dict):
            return {k: self._convert_floats_to_decimal(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._convert_floats_to_decimal(x) for x in obj]
        return obj

    def save_transaction_idempotent(
        self,
        event: Dict[str, Any],
        flagged_for_review: bool = False,
    ) -> Tuple[bool, bool]:
        """
        Persist transaction record with conditional write ensuring event_id uniqueness.

        Args:
            event: Validated event dictionary.
            flagged_for_review: Flag if transaction exceeded risk thresholds.

        Returns:
            Tuple of (success: bool, is_duplicate: bool).
            - (True, False): Successfully inserted first-time record.
            - (False, True): Duplicate detected (condition failed); no overwrite occurred.
            - (False, False): Unexpected non-duplicate storage failure.
        """
        event_id = event["event_id"]
        now_utc = datetime.now(timezone.utc)
        ttl_timestamp = int((now_utc + timedelta(days=self.ttl_days)).timestamp())

        # Construct storage item
        item = {
            "event_id": event_id,
            "transaction_id": event["transaction_id"],
            "customer_id": event["customer_id"],
            "order_id": event["order_id"],
            "event_type": event["event_type"],
            "status": event["status"],
            "amount": Decimal(str(event["amount"])),
            "currency": event.get("currency", "INR"),
            "payment_method": event.get("payment_method", "UNKNOWN"),
            "location": event.get("location", "UNKNOWN"),
            "event_timestamp": event["event_timestamp"],
            "processed_at": now_utc.isoformat(),
            "flagged_for_review": flagged_for_review,
            "ttl": ttl_timestamp,
        }

        # Include additional non-null attributes if present
        if "metadata" in event:
            item["metadata"] = self._convert_floats_to_decimal(event["metadata"])

        try:
            # Conditional write: Only succeed if this event_id does not already exist
            self.table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(event_id)",
            )
            logger.debug(f"Successfully stored transaction item for event_id: {event_id}")
            return True, False

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "ConditionalCheckFailedException":
                logger.warning(
                    f"Idempotency Guard: Duplicate event detected for event_id '{event_id}'. "
                    f"Skipping duplicate write."
                )
                return False, True
            else:
                logger.error(
                    f"DynamoDB ClientError saving event_id '{event_id}': {e}"
                )
                return False, False
        except Exception as ex:
            logger.exception(
                f"Unexpected exception writing event_id '{event_id}' to DynamoDB: {ex}"
            )
            return False, False

    def get_transaction_by_event_id(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a transaction record by its unique event ID."""
        try:
            response = self.table.get_item(Key={"event_id": event_id})
            return response.get("Item")
        except ClientError as e:
            logger.error(f"Error querying event_id {event_id}: {e}")
            return None

    def get_transactions_by_customer(
        self,
        customer_id: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Query customer transactions ordered by timestamp using CustomerIndex GSI.
        """
        try:
            response = self.table.query(
                IndexName="CustomerIndex",
                KeyConditionExpression="customer_id = :cid",
                ExpressionAttributeValues={":cid": customer_id},
                ScanIndexForward=False,  # Reverse chronological order
                Limit=limit,
            )
            return response.get("Items", [])
        except ClientError as e:
            logger.error(f"Error querying transactions for customer {customer_id}: {e}")
            return []
