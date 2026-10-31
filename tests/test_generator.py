"""Unit tests for the Transaction Generator and Event Schema."""

import json
from decimal import Decimal
import pytest

from producer.event_schema import (
    EventType,
    PaymentMethod,
    TransactionEvent,
    TransactionStatus,
    validate_raw_event_payload,
)
from producer.transaction_generator import TransactionGenerator


class TestTransactionGenerator:
    """Tests for simulated event generation mechanics."""

    def test_deterministic_generation_with_seed(self):
        """Verifies that identical seeds produce identical event sequences."""
        gen1 = TransactionGenerator(seed=42, failure_rate=0.0, duplicate_rate=0.0, invalid_rate=0.0)
        gen2 = TransactionGenerator(seed=42, failure_rate=0.0, duplicate_rate=0.0, invalid_rate=0.0)

        events1 = gen1.generate_batch(5)
        events2 = gen2.generate_batch(5)

        for e1, e2 in zip(events1, events2):
            assert e1["customer_id"] == e2["customer_id"]
            assert e1["amount"] == e2["amount"]
            assert e1["payment_method"] == e2["payment_method"]
            assert e1["location"] == e2["location"]

    def test_valid_event_schema_compliance(self):
        """Verifies that generated events conform to TransactionEvent schema."""
        gen = TransactionGenerator(seed=101, failure_rate=0.1, duplicate_rate=0.0, invalid_rate=0.0)
        events = gen.generate_batch(20)

        for event_dict in events:
            validated = validate_raw_event_payload(event_dict)
            assert isinstance(validated, TransactionEvent)
            assert validated.event_id.startswith("EVT-")
            assert validated.transaction_id.startswith("TXN-")
            assert validated.amount >= Decimal("0.00")
            assert validated.currency in {"INR", "USD", "EUR", "GBP"}

    def test_forced_duplicate_generation(self):
        """Verifies that duplicate generator mechanics reuse existing event IDs."""
        gen = TransactionGenerator(seed=202, failure_rate=0.0, duplicate_rate=0.0, invalid_rate=0.0)
        # Populate history pool
        first_event = gen.generate_single_event()
        # Force duplicate
        dup_event = gen.generate_single_event(force_duplicate=True)

        assert dup_event["event_id"] == first_event["event_id"]
        assert dup_event["transaction_id"] == first_event["transaction_id"]

    def test_forced_invalid_event_generation(self):
        """Verifies that invalid events trigger schema validation errors."""
        gen = TransactionGenerator(seed=303, failure_rate=0.0, duplicate_rate=0.0, invalid_rate=0.0)
        malformed = gen.generate_single_event(force_invalid=True)

        with pytest.raises(Exception):
            validate_raw_event_payload(malformed)

    def test_json_serialization_safe_amounts(self):
        """Verifies monetary values serialize to JSON cleanly without precision exceptions."""
        gen = TransactionGenerator(seed=404)
        event = gen.generate_single_event()
        json_str = json.dumps(event)
        deserialized = json.loads(json_str)

        assert isinstance(deserialized["amount"], float)
        assert deserialized["amount"] > 0
