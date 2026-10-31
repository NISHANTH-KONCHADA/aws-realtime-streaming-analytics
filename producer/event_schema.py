"""
Event Schema Module for Real-Time E-Commerce Streaming.

This module defines the canonical data contract for events traversing
the Kinesis Data Stream. It enforces strict schema validation using Pydantic v2,
ensures monetary precision via Python's Decimal type, and provides helper
methods for serialization and schema compatibility verification.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Dict, Optional, Set
import json

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class EventType(str, Enum):
    """Supported e-commerce business event types."""
    ORDER_CREATED = "ORDER_CREATED"
    PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    REFUND_ISSUED = "REFUND_ISSUED"


class TransactionStatus(str, Enum):
    """Allowed transaction lifecycle statuses."""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PENDING = "PENDING"
    CANCELLED = "CANCELLED"


class PaymentMethod(str, Enum):
    """Allowed payment instruments."""
    UPI = "UPI"
    CARD = "CARD"
    NET_BANKING = "NET_BANKING"
    WALLET = "WALLET"
    COD = "COD"


# Supported schema versions for backward/forward compatibility
SUPPORTED_SCHEMA_VERSIONS: Set[int] = {1}
SUPPORTED_CURRENCIES: Set[str] = {"INR", "USD", "EUR", "GBP"}


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to accurately serialize Python Decimal types."""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class TransactionEvent(BaseModel):
    """
    Canonical Transaction Event Model.

    Attributes:
        event_id: Unique UUID/alphanumeric identifier for the event (idempotency key).
        event_type: Domain event classification (e.g., PAYMENT_COMPLETED).
        event_version: Schema version number (default: 1).
        transaction_id: Business identifier for the financial transaction.
        customer_id: Unique identifier of the purchasing customer (partition key candidate).
        order_id: Associated e-commerce order identifier.
        amount: Non-negative monetary transaction amount (Decimal precision).
        currency: ISO-4217 3-letter currency code (e.g., INR).
        payment_method: Instrument used for payment.
        status: Current status of the transaction.
        location: City or geographic origin of the transaction.
        event_timestamp: ISO-8601 UTC timestamp of when the event occurred.
    """

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    event_id: str = Field(
        ...,
        min_length=1,
        description="Globally unique identifier for the event",
        examples=["EVT-100001"],
    )
    event_type: EventType = Field(
        ...,
        description="Type of domain event emitted",
    )
    event_version: int = Field(
        default=1,
        ge=1,
        description="Semantic schema version",
    )
    transaction_id: str = Field(
        ...,
        min_length=1,
        description="Business transaction reference",
        examples=["TXN-100001"],
    )
    customer_id: str = Field(
        ...,
        min_length=1,
        description="Customer account identifier",
        examples=["CUST-102"],
    )
    order_id: str = Field(
        ...,
        min_length=1,
        description="Associated customer order identifier",
        examples=["ORD-5001"],
    )
    amount: Decimal = Field(
        ...,
        ge=Decimal("0.00"),
        description="Monetary transaction amount (must be non-negative)",
    )
    currency: str = Field(
        default="INR",
        description="ISO 4217 Currency Code",
    )
    payment_method: PaymentMethod = Field(
        ...,
        description="Method used for transaction",
    )
    status: TransactionStatus = Field(
        ...,
        description="Transaction execution status",
    )
    location: Optional[str] = Field(
        default="Unknown",
        description="Transaction originating location/city",
    )
    event_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC event occurrence timestamp",
    )

    @field_validator("event_version")
    @classmethod
    def validate_schema_version(cls, v: int) -> int:
        """Enforce that the event complies with recognized schema versions."""
        if v not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(
                f"Unsupported schema version: {v}. Supported versions: {SUPPORTED_SCHEMA_VERSIONS}"
            )
        return v

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        """Enforce standard currency code."""
        upper_v = v.strip().upper()
        if upper_v not in SUPPORTED_CURRENCIES:
            raise ValueError(
                f"Unsupported currency: {v}. Must be one of {SUPPORTED_CURRENCIES}"
            )
        return upper_v

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount_to_decimal(cls, v: Any) -> Decimal:
        """Ensure monetary values are converted safely to Decimal without precision loss."""
        if isinstance(v, Decimal):
            return v
        try:
            dec = Decimal(str(v))
            # Quantize to 2 decimal places for currency
            return dec.quantize(Decimal("0.01"))
        except (InvalidOperation, TypeError, ValueError) as err:
            raise ValueError(f"Invalid monetary amount format: {v}") from err

    @field_validator("event_timestamp", mode="before")
    @classmethod
    def parse_event_timestamp(cls, v: Any) -> datetime:
        """Ensure timestamps are aware datetime objects in UTC."""
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)
        if isinstance(v, str):
            # Parse ISO string
            try:
                dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except ValueError as err:
                raise ValueError(f"Invalid ISO-8601 timestamp string: {v}") from err
        raise ValueError(f"Unsupported timestamp type: {type(v)}")

    @model_validator(mode="after")
    def validate_status_event_consistency(self) -> TransactionEvent:
        """Verify logical consistency between event_type and status."""
        if self.event_type == EventType.PAYMENT_FAILED.value and self.status == TransactionStatus.SUCCESS.value:
            raise ValueError("Event PAYMENT_FAILED cannot have status SUCCESS.")
        if self.event_type == EventType.PAYMENT_COMPLETED.value and self.status == TransactionStatus.FAILED.value:
            raise ValueError("Event PAYMENT_COMPLETED cannot have status FAILED.")
        return self

    def to_kinesis_payload(self) -> bytes:
        """
        Serialize the event to UTF-8 JSON bytes ready for Kinesis PutRecord(s).
        Uses float representation of Decimal for standard JSON parser compatibility.
        """
        data = self.to_dict()
        return json.dumps(data, cls=DecimalEncoder).encode("utf-8")

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model into a dictionary with ISO formatted timestamps and float amount."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "transaction_id": self.transaction_id,
            "customer_id": self.customer_id,
            "order_id": self.order_id,
            "amount": float(self.amount),
            "currency": self.currency,
            "payment_method": self.payment_method,
            "status": self.status,
            "location": self.location,
            "event_timestamp": self.event_timestamp.isoformat(),
        }


def validate_raw_event_payload(payload: Dict[str, Any]) -> TransactionEvent:
    """
    Validate an untrusted raw dictionary payload against the TransactionEvent schema.

    Raises:
        pydantic.ValidationError if payload violates schema constraints.
    """
    return TransactionEvent.model_validate(payload)
