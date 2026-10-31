"""
Validation and Business Rules Engine for Stream Processor.

Applies strict data hygiene, schema enforcement, monetary boundary checks,
and operational business flag evaluations (such as high-value review flagging).
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import logging
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

SUPPORTED_EVENT_TYPES = {
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
    "PAYMENT_FAILED",
    "ORDER_CANCELLED",
    "REFUND_ISSUED",
}

SUPPORTED_STATUSES = {"SUCCESS", "FAILED", "PENDING", "CANCELLED"}
SUPPORTED_CURRENCIES = {"INR", "USD", "EUR", "GBP"}
SUPPORTED_PAYMENT_METHODS = {"UPI", "CARD", "NET_BANKING", "WALLET", "COD"}
SUPPORTED_SCHEMA_VERSIONS = {1}


class ValidationError(Exception):
    """Raised when an incoming record violates schema contracts."""
    pass


class BusinessRuleViolation(Exception):
    """Raised when an event violates domain-level consistency."""
    pass


class EventValidator:
    """
    Validates streaming transaction payloads against schema and business policies.
    """

    def __init__(self, flagged_threshold_amount: Decimal = Decimal("10000.00")) -> None:
        """
        Args:
            flagged_threshold_amount: Monetary ceiling beyond which a transaction
                                      is marked `flagged_for_review`.
        """
        self.flagged_threshold_amount = flagged_threshold_amount

    def validate_event(self, raw_data: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        """
        Validate required fields, types, and domain rules.

        Args:
            raw_data: Unsanitized dictionary parsed from incoming Kinesis payload.

        Returns:
            Tuple of (sanitized_event_dict, flagged_for_review_boolean).

        Raises:
            ValidationError: On missing required fields, type mismatches, or schema violations.
            BusinessRuleViolation: On contradictory states (e.g. PAYMENT_COMPLETED with FAILED status).
        """
        # 1. Required string identifiers
        for field_name in ("event_id", "transaction_id", "customer_id", "order_id"):
            val = raw_data.get(field_name)
            if not val or not isinstance(val, str) or not val.strip():
                raise ValidationError(f"Field '{field_name}' is required and cannot be empty.")

        # 2. Schema version
        version = raw_data.get("event_version")
        if version not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValidationError(
                f"Unsupported event_version: {version}. Expected one of {SUPPORTED_SCHEMA_VERSIONS}"
            )

        # 3. Event type & status
        event_type = raw_data.get("event_type")
        if event_type not in SUPPORTED_EVENT_TYPES:
            raise ValidationError(
                f"Unsupported event_type: '{event_type}'. Allowed: {SUPPORTED_EVENT_TYPES}"
            )

        status = raw_data.get("status")
        if status not in SUPPORTED_STATUSES:
            raise ValidationError(
                f"Unsupported status: '{status}'. Allowed: {SUPPORTED_STATUSES}"
            )

        # 4. Domain consistency between event_type and status
        if event_type == "PAYMENT_FAILED" and status == "SUCCESS":
            raise BusinessRuleViolation("Event type PAYMENT_FAILED cannot have status SUCCESS.")
        if event_type == "PAYMENT_COMPLETED" and status == "FAILED":
            raise BusinessRuleViolation("Event type PAYMENT_COMPLETED cannot have status FAILED.")

        # 5. Currency & Payment Method
        currency = str(raw_data.get("currency", "")).upper()
        if currency not in SUPPORTED_CURRENCIES:
            raise ValidationError(
                f"Unsupported currency: '{currency}'. Allowed: {SUPPORTED_CURRENCIES}"
            )

        payment_method = raw_data.get("payment_method")
        if payment_method not in SUPPORTED_PAYMENT_METHODS:
            raise ValidationError(
                f"Unsupported payment_method: '{payment_method}'. Allowed: {SUPPORTED_PAYMENT_METHODS}"
            )

        # 6. Monetary Amount Validation
        raw_amount = raw_data.get("amount")
        if raw_amount is None:
            raise ValidationError("Missing required 'amount' field.")
        try:
            amount_dec = Decimal(str(raw_amount))
            if amount_dec < Decimal("0.00"):
                raise ValidationError(f"Transaction amount cannot be negative: {amount_dec}")
            # Ensure safe 2-decimal quantize
            amount_dec = amount_dec.quantize(Decimal("0.01"))
        except (InvalidOperation, TypeError, ValueError) as err:
            raise ValidationError(f"Invalid monetary format for amount: {raw_amount}") from err

        # 7. Timestamp validation
        raw_timestamp = raw_data.get("event_timestamp")
        if not raw_timestamp:
            raise ValidationError("Missing required 'event_timestamp' field.")
        try:
            if isinstance(raw_timestamp, str):
                dt = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
            elif isinstance(raw_timestamp, datetime):
                dt = raw_timestamp
            else:
                raise ValidationError("Invalid timestamp format.")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            sanitized_ts = dt.isoformat()
        except Exception as err:
            raise ValidationError(f"Cannot parse event_timestamp '{raw_timestamp}': {err}") from err

        # 8. Business Rule: High-Value Flagging
        # Note: As per architecture guidance, transactions exceeding threshold are marked
        # 'flagged_for_review', not labeled 'fraudulent'.
        flagged_for_review = amount_dec >= self.flagged_threshold_amount

        # Return sanitized normalized record
        sanitized = {
            "event_id": raw_data["event_id"].strip(),
            "event_type": event_type,
            "event_version": version,
            "transaction_id": raw_data["transaction_id"].strip(),
            "customer_id": raw_data["customer_id"].strip(),
            "order_id": raw_data["order_id"].strip(),
            "amount": float(amount_dec),
            "currency": currency,
            "payment_method": payment_method,
            "status": status,
            "location": str(raw_data.get("location", "Unknown")),
            "event_timestamp": sanitized_ts,
        }

        return sanitized, flagged_for_review
