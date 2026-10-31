"""
CloudWatch Operational Metrics Publisher.

Emits structured custom metrics for streaming pipelines including latency,
throughput, failure classifications, duplicate counts, and business outcomes.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class CloudWatchMetricsEmitter:
    """
    Batches and publishes custom operational metrics to Amazon CloudWatch.
    """

    # Maximum metrics per PutMetricData call allowed by AWS CloudWatch API
    MAX_METRIC_DATA_PER_REQUEST: int = 20

    def __init__(
        self,
        namespace: str = "EcommerceStreamingAnalytics",
        environment: str = "development",
        region_name: str = "us-east-1",
        cloudwatch_client: Optional[Any] = None,
        disabled: bool = False,
    ) -> None:
        """
        Initialize CloudWatch Metrics Emitter.

        Args:
            namespace: CloudWatch custom metric namespace.
            environment: Deployment environment (development, staging, production).
            region_name: AWS region.
            cloudwatch_client: Pre-configured boto3 CloudWatch client or mock.
            disabled: If True, suppresses API calls (e.g., in local unit tests).
        """
        self.namespace = namespace
        self.environment = environment
        self.region_name = region_name
        self.disabled = disabled
        self._metric_buffer: List[Dict[str, Any]] = []

        if cloudwatch_client is not None:
            self.client = cloudwatch_client
        elif not self.disabled:
            self.client = boto3.client("cloudwatch", region_name=self.region_name)
        else:
            self.client = None

    def add_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        extra_dimensions: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Queue a metric datum to be published.

        Args:
            metric_name: Metric name (e.g. EventsProcessed, ProcessingLatencyMs).
            value: Numerical metric value.
            unit: CloudWatch Unit (Count, Milliseconds, Bytes, None).
            extra_dimensions: Additional dimension key-values.
        """
        dimensions = [{"Name": "Environment", "Value": self.environment}]
        if extra_dimensions:
            for k, v in extra_dimensions.items():
                dimensions.append({"Name": k, "Value": str(v)})

        metric_datum = {
            "MetricName": metric_name,
            "Value": float(value),
            "Unit": unit,
            "Timestamp": datetime.now(timezone.utc),
            "Dimensions": dimensions,
        }
        self._metric_buffer.append(metric_datum)

    def flush(self) -> int:
        """
        Publish all buffered metrics to CloudWatch in batches of 20.

        Returns:
            Total number of metrics published.
        """
        if self.disabled or self.client is None or not self._metric_buffer:
            count = len(self._metric_buffer)
            self._metric_buffer.clear()
            return count

        total_published = 0
        while self._metric_buffer:
            batch = self._metric_buffer[: self.MAX_METRIC_DATA_PER_REQUEST]
            self._metric_buffer = self._metric_buffer[self.MAX_METRIC_DATA_PER_REQUEST :]

            try:
                self.client.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch,
                )
                total_published += len(batch)
            except ClientError as e:
                logger.error(f"Failed to publish CloudWatch metrics: {e}")
            except Exception as ex:
                logger.exception(f"Unexpected error publishing metrics: {ex}")

        return total_published
