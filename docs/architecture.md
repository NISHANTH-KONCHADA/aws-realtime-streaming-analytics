# System Architecture & Technical Design

This document details the architectural choices, streaming data flows, and infrastructure design for the **Real-Time E-Commerce Streaming & Analytics Platform**.

---

## 1. High-Level Architecture Flow

```text
  +--------------------------+
  |  Python Event Generator  |
  |  - Seeded simulation     |
  |  - Fault/Dup injection   |
  +------------+-------------+
               |
               v (PutRecords batching)
  +--------------------------+
  | Amazon Kinesis Streams   |
  | PartitionKey: customer_id|
  +------------+-------------+
               |
               | (Event Source Mapping: Batch=100, Window=5s, BisectBatch)
               v
  +---------------------------------------------------------------+
  | AWS Lambda Stream Processor (Python 3.11/3.14)                |
  |                                                               |
  |  [Base64 Decode] -> [JSON Parse] -> [Schema & Rules Engine]   |
  |                              |                                |
  |                              | (If Poison Pill: Quarantined)  |
  |                              v                                |
  |                +---------------------------+                  |
  |                | S3 Rejected Quarantine    |                  |
  |                | (rejected-events/...)     |                  |
  |                +---------------------------+                  |
  |                                                               |
  |  [DynamoDB Conditional PutItem (attribute_not_exists)]       |
  |       |                                                       |
  |       +---> Duplicate? -> Discard without repeat effects      |
  |       |                                                       |
  |       +---> Fresh Event? -> Persist State                     |
  |                                 |                             |
  |                                 v                             |
  |                   [S3 Partitioned Batch Archive]              |
  |                   (processed-events/date=.../type=...)        |
  |                                 |                             |
  |                                 v                             |
  |                   [CloudWatch Custom Metrics]                 |
  +---------------------------------+-----------------------------+
                                    |
      +-----------------------------+-----------------------------+
      |                                                           |
      v                                                           v
+-------------------------------+                   +-------------------------------+
| Amazon DynamoDB               |                   | Amazon S3 & Athena            |
| - Fast key-value lookups      |                   | - Partitioned Parquet/JSONL   |
| - CustomerIndex GSI           |                   | - Glue Data Catalog           |
| - TTL 90-day auto-expiry      |                   | - Ad-hoc SQL analytics        |
+-------------------------------+                   +-------------------------------+
```

---

## 2. Key Engineering Decisions

### 2.1 Partition Key Selection Strategy
- **Partition Key Used:** `customer_id`
- **Rationale:** In an e-commerce transaction stream, customer order sequence matters. Using `customer_id` ensures that all events for a specific customer route to the exact same Kinesis shard, maintaining strict FIFO ordering within that shard.
- **Hot-Shard Mitigation:** To prevent power users or bulk accounts from concentrating traffic on a single shard, a compound partition key (`f"{customer_id}#{uuid[:4]}"`) or random salt can be applied if a single customer exceeds shard write limits (1 MB/sec or 1,000 records/sec).

### 2.2 At-Least-Once Delivery & Idempotency
- **The Challenge:** Distributed streaming systems cannot guarantee end-to-end "exactly-once" delivery across independent distributed state stores (Kinesis -> Lambda -> DynamoDB -> S3). Network retries or Lambda timeouts can cause identical records to be re-delivered.
- **The Solution:**
  1. Each event carries an immutable `event_id`.
  2. DynamoDB persistence uses a conditional write:
     ```python
     ConditionExpression="attribute_not_exists(event_id)"
     ```
  3. If a duplicate is received, DynamoDB raises `ConditionalCheckFailedException`. The processor logs the deduplication event, increments the `DuplicateEvents` CloudWatch metric, and skips downstream business side effects.

### 2.3 Partial Batch Failure Reporting
- Standard Lambda-Kinesis mappings fail the **entire batch** if any single record errors out, causing already-processed records in that batch to be reprocessed upon retry.
- By configuring `FunctionResponseTypes: ["ReportBatchItemFailures"]`, our processor returns:
  ```json
  {
    "batchItemFailures": [
      { "itemIdentifier": "failing-sequence-number" }
    ]
  }
  ```
- Kinesis then rewinds its shard iterator only to the failing record, preventing duplicate writes for the preceding successful events.

### 2.4 Poison Pill Isolation
- Malformed Base64 data, invalid JSON, or schema-violating records cannot be cured by retrying. Retrying them exhausts Lambda retries, creates consumer lag, and risks deadlocks.
- The stream processor immediately isolates and writes unprocessable records to the `rejected-events/` S3 prefix, records an `InvalidEvents` metric, and removes them from the retry path.

---

## 3. Storage Layer Architecture

| Storage Service | Purpose | Key Schema | Retention / Lifecycle |
| :--- | :--- | :--- | :--- |
| **Amazon DynamoDB** | Operational state & real-time lookups | PK: `event_id`<br>GSI: `customer_id` + `event_timestamp` | 90-day TTL via attribute `ttl` |
| **Amazon S3** | Long-term analytical data lake | Hive partition layout: `processed-events/event_date=YYYY-MM-DD/event_type=.../` | Standard $\to$ Standard-IA after 30 days |
| **Amazon Athena** | Serverless ad-hoc SQL queries | Schema defined via AWS Glue Catalog | Query results stored in `athena-results/` |
