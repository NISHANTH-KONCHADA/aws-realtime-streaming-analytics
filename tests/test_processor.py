"""End-to-End Unit & Integration tests for AWS Lambda Stream Processor."""

import base64
import json
from unittest.mock import MagicMock
import pytest

from analytics.metrics import CloudWatchMetricsEmitter
from lambda_src.stream_processor import get_dependencies, lambda_handler
from storage.dynamodb_repository import DynamoDBRepository
from storage.s3_archive import S3ArchiveManager


def _create_kinesis_record(payload_dict: dict, seq_num: str = "seq-001", partition_key: str = "CUST-001") -> dict:
    """Helper to construct an AWS Lambda Kinesis record."""
    raw_json = json.dumps(payload_dict)
    b64_data = base64.b64encode(raw_json.encode("utf-8")).decode("utf-8")
    return {
        "kinesis": {
            "sequenceNumber": seq_num,
            "partitionKey": partition_key,
            "data": b64_data,
            "approximateArrivalTimestamp": 1700000000.0,
        }
    }


class TestStreamProcessor:
    """Comprehensive tests for stream processor execution and batch recovery."""

    @pytest.fixture(autouse=True)
    def setup_mocks(self):
        self.mock_dynamo_resource = MagicMock()
        self.mock_table = MagicMock()
        self.mock_dynamo_resource.Table.return_value = self.mock_table

        self.mock_s3_client = MagicMock()
        self.mock_cw_client = MagicMock()

        self.repo = DynamoDBRepository(
            table_name="ecommerce-transactions",
            dynamodb_resource=self.mock_dynamo_resource,
        )
        self.s3_archive = S3ArchiveManager(
            bucket_name="ecommerce-archive",
            s3_client=self.mock_s3_client,
        )
        self.metrics = CloudWatchMetricsEmitter(
            namespace="TestNamespace",
            cloudwatch_client=self.mock_cw_client,
            disabled=True,
        )

        # Wire mocked dependencies into stream processor
        get_dependencies(
            custom_dynamo=self.repo,
            custom_s3=self.s3_archive,
            custom_metrics=self.metrics,
        )

    def test_process_valid_batch_success(self):
        """Verifies clean batch execution: DynamoDB insert, S3 archive, no batch failures."""
        event_payload = {
            "event_id": "EVT-TEST-1",
            "event_type": "PAYMENT_COMPLETED",
            "event_version": 1,
            "transaction_id": "TXN-TEST-1",
            "customer_id": "CUST-0100",
            "order_id": "ORD-1000",
            "amount": 2500.00,
            "currency": "INR",
            "payment_method": "UPI",
            "status": "SUCCESS",
            "location": "Hyderabad",
            "event_timestamp": "2026-10-31T18:00:00Z",
        }

        lambda_event = {"Records": [_create_kinesis_record(event_payload, "seq-1")]}
        response = lambda_handler(lambda_event)

        assert response == {"batchItemFailures": []}
        self.mock_table.put_item.assert_called_once()
        self.mock_s3_client.put_object.assert_called_once()

    def test_poison_pill_malformed_json_quarantined(self):
        """Verifies that invalid JSON is quarantined to S3 and does NOT fail the batch."""
        bad_record = {
            "kinesis": {
                "sequenceNumber": "seq-corrupt",
                "partitionKey": "PK",
                "data": base64.b64encode(b"{not valid json").decode("utf-8"),
            }
        }
        lambda_event = {"Records": [bad_record]}
        response = lambda_handler(lambda_event)

        # Non-retryable error should NOT be in batchItemFailures
        assert response == {"batchItemFailures": []}
        # S3 quarantine put_object should be invoked for rejected event
        self.mock_s3_client.put_object.assert_called_once()

    def test_partial_batch_failure_on_storage_outage(self):
        """Verifies that transient DynamoDB outage reports failing sequence number for retry."""
        from botocore.exceptions import ClientError
        self.mock_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Throttled"}},
            "PutItem",
        )

        event_payload = {
            "event_id": "EVT-RETRY-1",
            "event_type": "PAYMENT_COMPLETED",
            "event_version": 1,
            "transaction_id": "TXN-RETRY-1",
            "customer_id": "CUST-0100",
            "order_id": "ORD-1000",
            "amount": 1200.00,
            "currency": "INR",
            "payment_method": "UPI",
            "status": "SUCCESS",
            "location": "Delhi",
            "event_timestamp": "2026-10-31T18:00:00Z",
        }

        lambda_event = {"Records": [_create_kinesis_record(event_payload, "seq-throttled-100")]}
        response = lambda_handler(lambda_event)

        # Must report the throttled sequence number for Kinesis partial batch retry
        assert response == {"batchItemFailures": [{"itemIdentifier": "seq-throttled-100"}]}
