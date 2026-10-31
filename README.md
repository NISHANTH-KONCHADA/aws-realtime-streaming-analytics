# Real-Time E-Commerce Streaming & Analytics Pipeline

[![CI Tests](https://img.shields.io/badge/pytest-18%20passed-brightgreen.svg)](#testing)
[![AWS Architecture](https://img.shields.io/badge/AWS-Kinesis%20%7C%20Lambda%20%7C%20DynamoDB%20%7C%20S3-orange.svg)](#architecture)
[![Python Version](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.14-blue.svg)](#tech-stack)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade, event-driven streaming data engineering platform designed to process high-throughput e-commerce transactions in real time. Demonstrates streaming fundamentals, partition key strategies, idempotency protection via DynamoDB conditional writes, partial batch failure handling in AWS Lambda, long-term date-partitioned S3 archiving, and serverless historical SQL analytics using AWS Glue and Amazon Athena.

---

## Architecture

```text
       PYTHON PRODUCER
(Seeded, Fault & Dup Injection)
             |
             v (PutRecords Batching + Jitter Backoff)
   AMAZON KINESIS STREAMS
   (PartitionKey: customer_id)
             |
             v (Batch: 100, Window: 5s, BisectBatchOnFunctionError)
     AWS LAMBDA CONSUMER
             |
             +-----------------------+
             |                       |
             v                       v
      VALIDATION ENGINE        POISON PILL DLQ
    (Schema & Domain Rules)   (S3 Rejected Partition)
             |
             v
     IDEMPOTENCY GUARD
  (DynamoDB Conditional Put)
             |
             +-----------------------+
             |                       |
             v                       v
   OPERATIONAL DATABASE      ANALYTICAL LAKE
    (Amazon DynamoDB)       (Amazon S3 Partitioned)
             |                       |
             v                       v
    CLOUDWATCH METRICS          GLUE & ATHENA
 (Latency, Lag, Outcomes)    (Historical SQL Queries)
```

---

## Key Technical Features

1. **Precision & Data Hygiene:**
   - Strict contract validation using **Pydantic v2**.
   - Currency and monetary values handled with `Decimal` to avoid floating-point rounding errors.
   - Comprehensive domain rules preventing illegal state transitions (e.g. `PAYMENT_FAILED` with `SUCCESS` status).

2. **Resilient Streaming Producer:**
   - AWS Kinesis `PutRecords` batching with per-record failure inspection.
   - Truncated exponential backoff with full jitter for throttled records (`ProvisionedThroughputExceededException`).
   - Customer-level partition key routing ensuring in-order processing per customer.

3. **Stream Processor with Partial Batch Responses:**
   - Implements AWS Lambda `ReportBatchItemFailures` so that when a transient failure occurs on record $N$, already-processed records $1 \dots N-1$ are not re-processed upon retry.
   - Poison pills (malformed JSON, broken Base64, schema violations) are quarantined to S3 rather than stalling the Kinesis shard.

4. **Guaranteed Idempotency:**
   - Atomic conditional writes (`attribute_not_exists(event_id)`) protect against duplicated side-effects caused by at-least-once streaming delivery.

5. **Storage Optimization & SQL Analytics:**
   - Fast operational lookups in **DynamoDB** with a `CustomerIndex` Global Secondary Index and automatic 90-day TTL.
   - Date-sharded Hive partitioned storage in **S3** (`event_date=YYYY-MM-DD/event_type=.../`) preventing small-file proliferation.
   - Ready-to-query SQL suite for **Amazon Athena** and **AWS Glue Data Catalog**.

6. **Infrastructure as Code:**
   - Production-ready **AWS SAM (CloudFormation)** template with least-privilege IAM roles, encryption, and CloudWatch alarms.

---

## Project Structure

```text
aws-realtime-streaming-analytics/
├── .gitignore
├── .env.example
├── requirements.txt
├── README.md
├── config/
│   └── dev.yaml                   # Stream name, batch size, and thresholds
├── producer/
│   ├── __init__.py
│   ├── event_schema.py            # Pydantic v2 schemas & Decimal precision
│   ├── transaction_generator.py   # Realistic event generator with seed & fault injection
│   ├── kinesis_producer.py        # PutRecords batcher with jittered backoff
│   └── main.py                    # Producer CLI for local, stream, or batch execution
├── lambda_src/
│   ├── __init__.py
│   ├── stream_processor.py        # Lambda handler with ReportBatchItemFailures
│   ├── validators.py              # Schema & domain validation rules
│   ├── idempotency.py             # DynamoDB conditional-write manager
│   ├── storage.py                 # Multi-storage coordinator
│   └── error_handler.py           # Poison pill quarantine router
├── storage/
│   ├── __init__.py
│   ├── dynamodb_repository.py     # DynamoDB CRUD, GSI queries, and TTL
│   └── s3_archive.py              # Partitioned multi-record JSONL archiver
├── analytics/
│   ├── __init__.py
│   ├── metrics.py                 # CloudWatch custom metric emitter
│   └── dashboard_config.json      # CloudWatch Dashboard definition
├── sql/
│   ├── 01_glue_catalog_tables.sql # External table DDLs for Athena
│   ├── 02_hourly_metrics.sql      # Volume, hourly trends, and latency
│   └── 03_business_analytics.sql  # Payment methods, GMV, and repeats
├── infrastructure/
│   ├── template.yaml              # AWS SAM Infrastructure as Code
│   └── iam_policies.json          # Least-privilege IAM policies
├── tests/
│   ├── __init__.py
│   ├── test_generator.py          # Generator determinism & schema compliance
│   ├── test_validation.py         # Boundary & schema validation tests
│   ├── test_idempotency.py        # Duplicate detection & condition tests
│   └── test_processor.py          # End-to-end simulated Lambda execution
└── docs/
    ├── architecture.md            # In-depth architectural documentation
    ├── event_schema.md            # Schema contract specification
    ├── failure_recovery.md        # At-least-once recovery & operations guide
    └── runbook.md                 # Deployment and execution guide
```

---

## Quickstart

### 1. Installation

```bash
git clone https://github.com/NISHANTH-KONCHADA/aws-realtime-streaming-analytics.git
cd aws-realtime-streaming-analytics
pip install -r requirements.txt
```

### 2. Run the Test Suite

```bash
python -m pytest -v
```

Output:
```text
tests/test_generator.py::TestTransactionGenerator::test_deterministic_generation_with_seed PASSED
tests/test_generator.py::TestTransactionGenerator::test_valid_event_schema_compliance PASSED
tests/test_generator.py::TestTransactionGenerator::test_forced_duplicate_generation PASSED
tests/test_generator.py::TestTransactionGenerator::test_forced_invalid_event_generation PASSED
tests/test_generator.py::TestTransactionGenerator::test_json_serialization_safe_amounts PASSED
tests/test_idempotency.py::TestIdempotency::test_first_time_event_succeeds PASSED
tests/test_idempotency.py::TestIdempotency::test_duplicate_event_detected_and_skipped PASSED
tests/test_idempotency.py::TestIdempotency::test_infrastructure_error_returns_storage_error PASSED
tests/test_processor.py::TestStreamProcessor::test_process_valid_batch_success PASSED
tests/test_processor.py::TestStreamProcessor::test_poison_pill_malformed_json_quarantined PASSED
tests/test_processor.py::TestStreamProcessor::test_partial_batch_failure_on_storage_outage PASSED
tests/test_validation.py::TestEventValidation::test_valid_event_passes PASSED
tests/test_validation.py::TestEventValidation::test_missing_event_id_raises_validation_error PASSED
tests/test_validation.py::TestEventValidation::test_negative_amount_raises_validation_error PASSED
tests/test_validation.py::TestEventValidation::test_unsupported_currency_raises_validation_error PASSED
tests/test_validation.py::TestEventValidation::test_unsupported_version_raises_validation_error PASSED
tests/test_validation.py::TestEventValidation::test_inconsistent_event_status_raises_business_rule_violation PASSED
tests/test_validation.py::TestEventValidation::test_high_value_transaction_sets_flagged_for_review PASSED

============================= 18 passed in 0.23s ==============================
```

### 3. Generate Local Test Events

```bash
# Generate 50 events to local file
python -m producer.main --mode local --count 50 --output local_events.jsonl

# Run dry-run streaming simulation
python -m producer.main --mode stream --rate 5 --dry-run
```

### 4. Deploy Infrastructure (AWS SAM)

```bash
cd infrastructure
sam build
sam deploy --guided
```

### 5. Stream Live Transactions to Kinesis

```bash
python -m producer.main \
  --mode stream \
  --stream-name ecommerce-transaction-stream-development \
  --region us-east-1 \
  --rate 10
```

---

## Athena Analytics Queries

Once events are archived to S3, initialize the Glue Catalog using [sql/01_glue_catalog_tables.sql](file:///c:/Users/konch/OneDrive/Desktop/AWS_Realtime_Streaming_Analytics/sql/01_glue_catalog_tables.sql) and run:

```sql
-- Hourly Transaction Success Rate
SELECT
    date_trunc('hour', from_iso8601_timestamp(event_timestamp)) AS transaction_hour,
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS successful_transactions,
    ROUND(
        100.0 * COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) / NULLIF(COUNT(*), 0),
        2
    ) AS success_rate_percentage
FROM ecommerce_analytics_db.processed_events
GROUP BY 1
ORDER BY transaction_hour DESC;
```

---

## Interview & Technical Q&A

<details>
<summary><strong>1. Why use customer_id as the Kinesis partition key?</strong></summary>

Using `customer_id` ensures that all events generated by the same customer (e.g. `ORDER_CREATED`, followed by `PAYMENT_COMPLETED` or `ORDER_CANCELLED`) map to the exact same shard. Within any single Kinesis shard, records are strictly ordered by sequence number, eliminating race conditions between dependent events for a customer.
</details>

<details>
<summary><strong>2. Why not claim 'exactly-once' processing across the pipeline?</strong></summary>

In distributed systems, true end-to-end exactly-once delivery across multiple independent distributed systems (Kinesis -> Lambda -> DynamoDB -> S3) requires two-phase commit protocols that add massive latency. Kinesis provides at-least-once delivery. We achieve **effective exactly-once side-effects** through idempotency: atomic conditional writes (`attribute_not_exists(event_id)`) in DynamoDB guarantee that duplicate event deliveries do not cause repeat state changes.
</details>

<details>
<summary><strong>3. How does Lambda handle partial batch failures?</strong></summary>

Without `ReportBatchItemFailures`, an error on record #90 of a 100-record batch causes the entire batch to fail and be retried from record #1. With `ReportBatchItemFailures`, our Lambda returns `{"batchItemFailures": [{"itemIdentifier": "sequenceNumber-90"}]}`. AWS Kinesis advances its checkpoint to record #89 and only redelivers records starting at #90.
</details>

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
