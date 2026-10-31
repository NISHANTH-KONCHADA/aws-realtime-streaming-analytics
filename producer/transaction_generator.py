"""
Simulated E-Commerce Transaction Generator.

Generates realistic streaming events conforming to TransactionEvent schemas.
Supports configurable event distributions, reproducible seed generation,
and intentional injection of duplicate or malformed events for robust pipeline
resilience and idempotency verification.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json
import random
import time
from typing import Any, Dict, Generator, List, Optional
import uuid

from producer.event_schema import (
    EventType,
    PaymentMethod,
    TransactionEvent,
    TransactionStatus,
)


# Curated sample entities for realistic simulations
SAMPLE_CUSTOMERS: List[str] = [
    f"CUST-{idx:04d}" for idx in range(101, 131)
]

SAMPLE_LOCATIONS: List[str] = [
    "Hyderabad",
    "Bengaluru",
    "Mumbai",
    "Delhi",
    "Chennai",
    "Kolkata",
    "Pune",
    "Ahmedabad",
    "Jaipur",
    "Kochi",
]

SAMPLE_PRODUCTS: List[Dict[str, Any]] = [
    {"name": "Wireless Noise-Canceling Headphones", "min_price": 2499.00, "max_price": 8999.00},
    {"name": "Ergonomic Mechanical Keyboard", "min_price": 3299.00, "max_price": 6499.00},
    {"name": "Smart Fitness Band", "min_price": 1499.00, "max_price": 3999.00},
    {"name": "4K Ultra HD Streaming Stick", "min_price": 2999.00, "max_price": 4999.00},
    {"name": "USB-C Fast Charging Hub", "min_price": 899.00, "max_price": 1899.00},
    {"name": "Leather Laptop Backpack", "min_price": 1999.00, "max_price": 3499.00},
    {"name": "Stainless Steel Thermal Bottle", "min_price": 499.00, "max_price": 1299.00},
    {"name": "High-End Gaming Monitor", "min_price": 14999.00, "max_price": 28999.00},
]


class TransactionGenerator:
    """
    Stateful and configurable transaction event generator.
    """

    def __init__(
        self,
        seed: Optional[int] = None,
        failure_rate: float = 0.15,
        duplicate_rate: float = 0.05,
        invalid_rate: float = 0.03,
        default_currency: str = "INR",
    ) -> None:
        """
        Initialize transaction generator.

        Args:
            seed: Optional integer seed for repeatable, deterministic generation.
            failure_rate: Ratio of transactions that result in payment failure (0.0 to 1.0).
            duplicate_rate: Ratio of events that duplicate a previously generated event_id.
            invalid_rate: Ratio of events containing invalid schemas for resilience testing.
            default_currency: Base currency code (default: INR).
        """
        self.seed = seed
        self.rng = random.Random(seed)
        self.failure_rate = failure_rate
        self.duplicate_rate = duplicate_rate
        self.invalid_rate = invalid_rate
        self.default_currency = default_currency

        self._event_counter = 100000
        self._order_counter = 50000
        self._history_pool: List[TransactionEvent] = []
        self._max_history_pool_size = 500

    def generate_single_event(
        self,
        force_duplicate: bool = False,
        force_invalid: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate a single event dictionary.

        Args:
            force_duplicate: If True, forces generation of a duplicate event.
            force_invalid: If True, deliberately injects invalid data.

        Returns:
            Dictionary containing transaction payload.
        """
        # Duplicate injection check
        should_duplicate = force_duplicate or (
            self.rng.random() < self.duplicate_rate and len(self._history_pool) > 0
        )
        if should_duplicate and self._history_pool:
            cached_event = self.rng.choice(self._history_pool)
            # Duplicate the event payload exactly (same event_id to test idempotency)
            return cached_event.to_dict()

        # Invalid event injection check
        should_invalidate = force_invalid or (self.rng.random() < self.invalid_rate)
        if should_invalidate:
            return self._generate_malformed_event()

        # Standard valid event generation
        self._event_counter += 1
        self._order_counter += 1

        event_id = f"EVT-{self._event_counter}"
        transaction_id = f"TXN-{self._event_counter}"
        order_id = f"ORD-{self._order_counter}"
        customer_id = self.rng.choice(SAMPLE_CUSTOMERS)
        location = self.rng.choice(SAMPLE_LOCATIONS)

        product = self.rng.choice(SAMPLE_PRODUCTS)
        raw_price = self.rng.uniform(product["min_price"], product["max_price"])
        amount = Decimal(str(round(raw_price, 2)))

        payment_method = self.rng.choice(list(PaymentMethod))

        # Outcome determination
        is_failure = self.rng.random() < self.failure_rate
        if is_failure:
            event_type = EventType.PAYMENT_FAILED
            status = TransactionStatus.FAILED
        else:
            event_type = EventType.PAYMENT_COMPLETED
            status = TransactionStatus.SUCCESS

        timestamp = datetime.now(timezone.utc) - timedelta(
            seconds=self.rng.randint(0, 30)
        )

        event = TransactionEvent(
            event_id=event_id,
            event_type=event_type,
            event_version=1,
            transaction_id=transaction_id,
            customer_id=customer_id,
            order_id=order_id,
            amount=amount,
            currency=self.default_currency,
            payment_method=payment_method,
            status=status,
            location=location,
            event_timestamp=timestamp,
        )

        # Store in historical cache for realistic duplicate simulation
        if len(self._history_pool) >= self._max_history_pool_size:
            self._history_pool.pop(0)
        self._history_pool.append(event)

        return event.to_dict()

    def _generate_malformed_event(self) -> Dict[str, Any]:
        """Generate deliberately malformed event for negative testing."""
        corruption_type = self.rng.choice([
            "missing_event_id",
            "negative_amount",
            "unsupported_currency",
            "unsupported_version",
            "invalid_timestamp",
            "status_mismatch",
        ])

        self._event_counter += 1
        base_event = {
            "event_id": f"EVT-{self._event_counter}",
            "event_type": "PAYMENT_COMPLETED",
            "event_version": 1,
            "transaction_id": f"TXN-{self._event_counter}",
            "customer_id": self.rng.choice(SAMPLE_CUSTOMERS),
            "order_id": f"ORD-{self._order_counter}",
            "amount": 1999.00,
            "currency": "INR",
            "payment_method": "UPI",
            "status": "SUCCESS",
            "location": "Mumbai",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if corruption_type == "missing_event_id":
            base_event["event_id"] = ""
        elif corruption_type == "negative_amount":
            base_event["amount"] = -500.00
        elif corruption_type == "unsupported_currency":
            base_event["currency"] = "XYZ_BAD"
        elif corruption_type == "unsupported_version":
            base_event["event_version"] = 99
        elif corruption_type == "invalid_timestamp":
            base_event["event_timestamp"] = "corrupted-date-format"
        elif corruption_type == "status_mismatch":
            base_event["event_type"] = "PAYMENT_FAILED"
            base_event["status"] = "SUCCESS"  # Contradiction

        return base_event

    def generate_batch(self, batch_size: int = 10) -> List[Dict[str, Any]]:
        """Generate a list of transaction events."""
        return [self.generate_single_event() for _ in range(batch_size)]

    def stream_events(
        self,
        rate_per_sec: float = 5.0,
        total_events: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Continuously stream simulated events at a controlled rate.

        Args:
            rate_per_sec: Target frequency of events emitted per second.
            total_events: Total events to emit (None for infinite stream).
        """
        delay = 1.0 / max(rate_per_sec, 0.01)
        count = 0

        while total_events is None or count < total_events:
            start_time = time.time()
            yield self.generate_single_event()
            count += 1
            elapsed = time.time() - start_time
            sleep_time = max(0.0, delay - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    def write_to_jsonl(self, filepath: str, count: int = 50) -> None:
        """Write simulated events to a local JSON Lines file for Phase 1 testing."""
        with open(filepath, "w", encoding="utf-8") as f:
            for _ in range(count):
                event = self.generate_single_event()
                f.write(json.dumps(event) + "\n")
