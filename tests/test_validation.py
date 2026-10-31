"""Unit tests for the EventValidator and business rule engine."""

from decimal import Decimal
import pytest

from lambda_src.validators import (
    BusinessRuleViolation,
    EventValidator,
    ValidationError,
)


@pytest.fixture
def validator():
    return EventValidator(flagged_threshold_amount=Decimal("10000.00"))


@pytest.fixture
def valid_event_dict():
    return {
        "event_id": "EVT-888001",
        "event_type": "PAYMENT_COMPLETED",
        "event_version": 1,
        "transaction_id": "TXN-888001",
        "customer_id": "CUST-0105",
        "order_id": "ORD-50001",
        "amount": 2499.50,
        "currency": "INR",
        "payment_method": "UPI",
        "status": "SUCCESS",
        "location": "Bengaluru",
        "event_timestamp": "2026-10-31T18:30:00Z",
    }


class TestEventValidation:
    """Tests for field constraints and boundary validations."""

    def test_valid_event_passes(self, validator, valid_event_dict):
        sanitized, flagged = validator.validate_event(valid_event_dict)
        assert sanitized["event_id"] == "EVT-888001"
        assert sanitized["amount"] == 2499.50
        assert flagged is False

    def test_missing_event_id_raises_validation_error(self, validator, valid_event_dict):
        valid_event_dict["event_id"] = "  "
        with pytest.raises(ValidationError, match="Field 'event_id' is required"):
            validator.validate_event(valid_event_dict)

    def test_negative_amount_raises_validation_error(self, validator, valid_event_dict):
        valid_event_dict["amount"] = -10.00
        with pytest.raises(ValidationError, match="cannot be negative"):
            validator.validate_event(valid_event_dict)

    def test_unsupported_currency_raises_validation_error(self, validator, valid_event_dict):
        valid_event_dict["currency"] = "BITCOIN"
        with pytest.raises(ValidationError, match="Unsupported currency"):
            validator.validate_event(valid_event_dict)

    def test_unsupported_version_raises_validation_error(self, validator, valid_event_dict):
        valid_event_dict["event_version"] = 99
        with pytest.raises(ValidationError, match="Unsupported event_version"):
            validator.validate_event(valid_event_dict)

    def test_inconsistent_event_status_raises_business_rule_violation(
        self, validator, valid_event_dict
    ):
        valid_event_dict["event_type"] = "PAYMENT_FAILED"
        valid_event_dict["status"] = "SUCCESS"
        with pytest.raises(BusinessRuleViolation, match="PAYMENT_FAILED cannot have status SUCCESS"):
            validator.validate_event(valid_event_dict)

    def test_high_value_transaction_sets_flagged_for_review(self, validator, valid_event_dict):
        valid_event_dict["amount"] = 15000.00
        sanitized, flagged = validator.validate_event(valid_event_dict)
        assert sanitized["amount"] == 15000.00
        assert flagged is True
