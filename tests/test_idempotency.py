"""Unit tests for DynamoDB conditional-write idempotency and duplicate protection."""

from decimal import Decimal
from unittest.mock import MagicMock
import pytest
from botocore.exceptions import ClientError

from lambda_src.idempotency import IdempotencyDecision, IdempotencyManager
from storage.dynamodb_repository import DynamoDBRepository


@pytest.fixture
def sample_event():
    return {
        "event_id": "EVT-999001",
        "event_type": "PAYMENT_COMPLETED",
        "event_version": 1,
        "transaction_id": "TXN-999001",
        "customer_id": "CUST-0112",
        "order_id": "ORD-50009",
        "amount": 3499.00,
        "currency": "INR",
        "payment_method": "UPI",
        "status": "SUCCESS",
        "location": "Mumbai",
        "event_timestamp": "2026-10-31T18:00:00Z",
    }


class TestIdempotency:
    """Verifies that duplicate deliveries never trigger repeat writes."""

    def test_first_time_event_succeeds(self, sample_event):
        # Mock DynamoDB resource
        mock_resource = MagicMock()
        mock_table = MagicMock()
        mock_resource.Table.return_value = mock_table
        mock_table.put_item.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

        repo = DynamoDBRepository(
            table_name="ecommerce-transactions",
            dynamodb_resource=mock_resource,
        )
        manager = IdempotencyManager(repo)

        decision, err = manager.process_and_persist(sample_event)
        assert decision == IdempotencyDecision.PROCEED
        assert err is None
        mock_table.put_item.assert_called_once()

    def test_duplicate_event_detected_and_skipped(self, sample_event):
        mock_resource = MagicMock()
        mock_table = MagicMock()
        mock_resource.Table.return_value = mock_table

        # Simulate ConditionalCheckFailedException from AWS
        error_response = {
            "Error": {
                "Code": "ConditionalCheckFailedException",
                "Message": "The conditional request failed",
            }
        }
        mock_table.put_item.side_effect = ClientError(error_response, "PutItem")

        repo = DynamoDBRepository(
            table_name="ecommerce-transactions",
            dynamodb_resource=mock_resource,
        )
        manager = IdempotencyManager(repo)

        decision, err = manager.process_and_persist(sample_event)
        assert decision == IdempotencyDecision.DUPLICATE
        assert err is None

    def test_infrastructure_error_returns_storage_error(self, sample_event):
        mock_resource = MagicMock()
        mock_table = MagicMock()
        mock_resource.Table.return_value = mock_table

        # Simulate transient InternalServerError
        error_response = {
            "Error": {
                "Code": "InternalServerError",
                "Message": "An internal error occurred",
            }
        }
        mock_table.put_item.side_effect = ClientError(error_response, "PutItem")

        repo = DynamoDBRepository(
            table_name="ecommerce-transactions",
            dynamodb_resource=mock_resource,
        )
        manager = IdempotencyManager(repo)

        decision, err = manager.process_and_persist(sample_event)
        assert decision == IdempotencyDecision.STORAGE_ERROR
        assert err is not None
