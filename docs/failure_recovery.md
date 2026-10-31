# Failure Recovery, Resilience & Operations Guide

This runbook documents failure modes, mitigation architectures, retry semantics, and manual recovery procedures for the streaming pipeline.

---

## 1. Failure Modes & Mitigations

| Failure Mode | Root Cause | System Response | Recovery Strategy |
| :--- | :--- | :--- | :--- |
| **Kinesis Throttling** | Write rate exceeds 1 MB/s or 1000 records/s per shard | Producer catches `ProvisionedThroughputExceededException` | Full jitter exponential backoff retry; add shards to the stream. |
| **Poison Pill Record** | Corrupt Base64, invalid JSON, missing required fields | Handled in Lambda processor | Quarantined to `s3://.../rejected-events/`; record skipped so shard is not blocked. |
| **Duplicate Event** | Network retry, producer resend, or Lambda re-invocation | DynamoDB conditional check fails (`ConditionalCheckFailedException`) | Handled idempotently; record ignored, `DuplicateEvents` metric incremented. |
| **DynamoDB Outage / Throttle**| Capacity exhaustion or temporary AWS internal error | Lambda catches error, appends record sequence number to `batchItemFailures` | Kinesis retries only starting from the failed record. |
| **Lambda Timeout** | Heavy processing or network latency exceeds 60s timeout | Lambda execution terminates | Kinesis redelivers the un-checkpointed batch; idempotent DynamoDB writes prevent duplicate effects. |

---

## 2. Partial Batch Failure Mechanics

When processing Kinesis records in batches (e.g. 100 records per invocation):

1. **Without Partial Batch Failures (Antipattern):**
   If record #50 fails and Lambda raises an exception, the Lambda service fails the *entire batch of 100 records*. On retry, records #1 through #49 are processed a second time.

2. **With Partial Batch Response (Our Implementation):**
   - The EventSourceMapping is configured with `FunctionResponseTypes: ["ReportBatchItemFailures"]`.
   - If record #50 fails due to a transient infrastructure fault, the Lambda processor returns:
     ```json
     {
       "batchItemFailures": [
         { "itemIdentifier": "sequence-number-of-record-50" }
       ]
     }
     ```
   - AWS Lambda informs Kinesis that records #1 through #49 succeeded and only re-invokes starting at record #50.

---

## 3. Quarantined Record Replay Procedure

When upstream bugs cause valid events to be rejected or quarantined, operations teams can replay records from S3:

1. Locate rejected records in S3:
   ```bash
   aws s3 ls s3://ecommerce-streaming-analytics-archive/rejected-events/event_date=2026-10-31/
   ```
2. Download and review the quarantine JSON file:
   ```bash
   aws s3 cp s3://ecommerce-streaming-analytics-archive/rejected-events/event_date=2026-10-31/reason=SCHEMA_VIOLATION/rejected_xxx.json .
   ```
3. Extract `raw_payload`, apply corrective schema transformations, and republish through the producer CLI:
   ```bash
   python -m producer.main --mode batch --count 1
   ```
