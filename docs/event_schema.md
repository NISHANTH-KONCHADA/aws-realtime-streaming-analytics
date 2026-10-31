# Event Schema Specification & Contract

This document outlines the canonical schema specification for all events emitted into the streaming pipeline.

---

## 1. Schema Versioning

- **Current Version:** `1`
- Schema version is declared on every event payload via the `event_version` integer attribute.
- Backward-compatible extensions (adding optional fields) maintain the current version.
- Breaking schema modifications (modifying or removing required fields, changing type contracts) require incrementing `event_version` and updating the `SUPPORTED_SCHEMA_VERSIONS` set in both producer and consumer validators.

---

## 2. Event Specification

| Field Name | Type | Required | Allowed Values / Constraints | Example | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | **Yes** | Non-empty unique string | `EVT-100001` | Unique event identifier; serves as the idempotency key. |
| `event_type` | String (Enum) | **Yes** | `ORDER_CREATED`<br>`PAYMENT_COMPLETED`<br>`PAYMENT_FAILED`<br>`ORDER_CANCELLED`<br>`REFUND_ISSUED` | `PAYMENT_COMPLETED` | Domain categorization of the business event. |
| `event_version` | Integer | **Yes** | Supported versions: `[1]` | `1` | Schema version contract. |
| `transaction_id` | String | **Yes** | Non-empty string | `TXN-100001` | Unique financial transaction reference. |
| `customer_id` | String | **Yes** | Non-empty string | `CUST-0102` | Customer account identifier (Kinesis partition key). |
| `order_id` | String | **Yes** | Non-empty string | `ORD-50001` | Associated e-commerce order ID. |
| `amount` | Decimal / Float | **Yes** | $\ge 0.00$, 2-decimal precision | `2499.50` | Monetary value of the transaction. |
| `currency` | String | **Yes** | `INR`, `USD`, `EUR`, `GBP` | `INR` | ISO 4217 Currency Code. |
| `payment_method` | String (Enum) | **Yes** | `UPI`, `CARD`, `NET_BANKING`, `WALLET`, `COD` | `UPI` | Payment instrument used. |
| `status` | String (Enum) | **Yes** | `SUCCESS`, `FAILED`, `PENDING`, `CANCELLED` | `SUCCESS` | Current execution status. |
| `location` | String | No | Geographical city | `Hyderabad` | City originating the transaction. |
| `event_timestamp`| String (ISO-8601)| **Yes** | UTC ISO-8601 timestamp | `2026-10-31T18:30:00Z` | Timestamp when event occurred at client. |

---

## 3. Sample Payloads

### Successful Payment Event
```json
{
  "event_id": "EVT-100001",
  "event_type": "PAYMENT_COMPLETED",
  "event_version": 1,
  "transaction_id": "TXN-100001",
  "customer_id": "CUST-0102",
  "order_id": "ORD-50001",
  "amount": 2499.00,
  "currency": "INR",
  "payment_method": "UPI",
  "status": "SUCCESS",
  "location": "Hyderabad",
  "event_timestamp": "2026-10-31T18:30:00Z"
}
```

### Failed Payment Event
```json
{
  "event_id": "EVT-100002",
  "event_type": "PAYMENT_FAILED",
  "event_version": 1,
  "transaction_id": "TXN-100002",
  "customer_id": "CUST-0103",
  "order_id": "ORD-50002",
  "amount": 799.00,
  "currency": "INR",
  "payment_method": "CARD",
  "status": "FAILED",
  "location": "Chennai",
  "event_timestamp": "2026-10-31T18:31:00Z"
}
```
