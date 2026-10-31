# Project 2: Real-Time E-Commerce Transaction Streaming & Analytics

> **Project type:** Event-driven / streaming data engineering\
> **Level:** Intermediate to advanced\
> **Primary stack:** Python, Amazon Kinesis Data Streams, AWS Lambda,
> Amazon S3, DynamoDB, CloudWatch, SQL, Athena\
> **Optional extension:** Kinesis Data Firehose, Glue, EventBridge,
> Terraform\
> **Suggested duration:** 1.5--2.5 weeks for a solid implementation

------------------------------------------------------------------------

## 1. Project objective

Build a real-time transaction processing system that continuously
receives simulated e-commerce events, validates and processes them,
stores them reliably, and exposes useful operational and historical
analytics.

The project should demonstrate:

1.  Event generation and ingestion.
2.  Streaming fundamentals.
3.  Partition keys and ordering.
4.  Event processing with Lambda.
5.  Data validation and error handling.
6.  Idempotency and duplicate-event handling.
7.  Storage in S3 and DynamoDB.
8.  Monitoring and operational metrics.
9.  Historical SQL analytics.
10. Testing and failure recovery.

### Business scenario

An e-commerce application generates events whenever customers place
orders, make payments, cancel orders or receive payment updates.

Operations teams need to know:

-   How many transactions are being received?
-   How many payments are successful or failed?
-   What is the transaction value over time?
-   Which customers are generating repeated events?
-   Are processing delays increasing?
-   Are invalid or suspicious-looking events being detected?
-   Can the system recover from temporary processing failures?

The platform will simulate the event flow and process it using AWS
managed services.

This is a learning simulation, not a production payment processor or a
certified fraud-detection system.

------------------------------------------------------------------------

## 2. Scope and expected outcome

### Minimum viable version

Build a working system that:

-   Generates JSON transaction events using Python.
-   Publishes events to Amazon Kinesis Data Streams.
-   Triggers a Lambda processor through an event source mapping.
-   Validates event structure and required fields.
-   Handles duplicate transaction IDs using an idempotency strategy.
-   Stores transaction records in DynamoDB.
-   Archives processed records in S3.
-   Exposes useful processing metrics through CloudWatch.
-   Queries historical S3 data using Athena.
-   Includes tests, documentation and failure scenarios.

### Advanced version

After the basic system works, add:

-   Event-time window aggregation.
-   Kinesis Data Firehose for managed S3 delivery.
-   A dead-letter or failure destination.
-   Replay and recovery procedures.
-   Schema versioning.
-   A small monitoring dashboard.
-   Infrastructure deployment using AWS SAM or Terraform.
-   Load testing and throughput measurements.

------------------------------------------------------------------------

## 3. Event design

Generate simulated events using Python. Each event should represent a
business action.

### Example successful transaction

``` json
{
  "event_id": "EVT-100001",
  "event_type": "PAYMENT_COMPLETED",
  "event_version": 1,
  "transaction_id": "TXN-100001",
  "customer_id": "CUST-102",
  "order_id": "ORD-5001",
  "amount": 2499.00,
  "currency": "INR",
  "payment_method": "UPI",
  "status": "SUCCESS",
  "location": "Hyderabad",
  "event_timestamp": "2026-10-30T18:30:00Z"
}
```

### Example failed transaction

``` json
{
  "event_id": "EVT-100002",
  "event_type": "PAYMENT_FAILED",
  "event_version": 1,
  "transaction_id": "TXN-100002",
  "customer_id": "CUST-103",
  "order_id": "ORD-5002",
  "amount": 799.00,
  "currency": "INR",
  "payment_method": "CARD",
  "status": "FAILED",
  "location": "Chennai",
  "event_timestamp": "2026-10-30T18:31:00Z"
}
```

### Event types to simulate

-   `ORDER_CREATED`
-   `PAYMENT_COMPLETED`
-   `PAYMENT_FAILED`
-   `ORDER_CANCELLED`
-   `REFUND_ISSUED`

For a manageable first version, begin with payment events only. Add
other event types once the processing logic is stable.

### Required fields

  Field               Type             Validation
  ------------------- ---------------- -------------------------------
  `event_id`          String           Required and unique per event
  `event_type`        String           Must be an allowed event type
  `event_version`     Integer          Supported schema version
  `transaction_id`    String           Required
  `customer_id`       String           Required
  `amount`            Decimal/number   Non-negative and valid
  `currency`          String           Supported currency code
  `status`            String           Allowed status
  `event_timestamp`   Timestamp        Valid timestamp

Use decimal-safe handling for monetary values. Avoid relying on binary
floating-point equality for financial calculations.

------------------------------------------------------------------------

## 4. Architecture

``` text
             PYTHON EVENT GENERATOR
          creates simulated transaction events
                         |
                         v
              KINESIS DATA STREAMS
             receives and partitions events
                         |
                         v
                 AWS LAMBDA
           consumes records in batches
                         |
              +----------+----------+
              |                     |
              v                     v
       VALIDATION LOGIC       INVALID EVENTS
       schema and rules       failure handling
              |
              v
       IDEMPOTENCY CHECK
       event ID / transaction key
              |
              v
       BUSINESS PROCESSING
       status, amount, metrics
              |
          +---+----------------+
          |                    |
          v                    v
      DYNAMODB                AMAZON S3
  current status/lookup   historical event archive
          |                    |
          |                    v
          |               GLUE CATALOG
          |                    |
          |                    v
          |                 ATHENA
          |                    |
          +--------------> REPORTS
                               |
                               v
                      CLOUDWATCH MONITORING
```

### Service responsibilities

**Python producer**

-   Creates realistic event payloads.
-   Sends events to Kinesis.
-   Uses a partition key.
-   Handles producer errors and retries.

**Kinesis Data Streams**

-   Receives and retains streaming records.
-   Distributes records across shards.
-   Provides sequence numbers and partition-key-based ordering within a
    shard.

**AWS Lambda**

-   Processes Kinesis records in batches.
-   Decodes the payload.
-   Validates records.
-   Applies idempotency and business rules.
-   Writes results to storage.

**DynamoDB**

-   Provides fast transaction lookups.
-   Stores current processing status.
-   Supports conditional writes for idempotency.

**Amazon S3**

-   Archives event data for historical analysis.
-   Stores processed records in an analytics-friendly layout.

**CloudWatch**

-   Stores logs and metrics.
-   Tracks errors, throttling, duration and processing activity.
-   Supports alarms.

**Athena**

-   Queries historical files in S3 using SQL.
-   Supports reporting and trend analysis.

------------------------------------------------------------------------

## 5. Kinesis fundamentals to understand

### Stream

A Kinesis Data Stream is a sequence of records. Producers write records
and consumers read them.

### Partition key

The partition key determines how a record is mapped to a shard.

Example:

``` python
partition_key = event["customer_id"]
```

Using a customer ID can help group related events by partition key, but
a very active customer can cause a hot partition. Choose a key based on
the ordering and distribution requirements.

### Shards

Shards provide stream capacity. Understand the throughput and retention
limits applicable to the stream mode and current AWS configuration. Do
not assume a fixed capacity without checking the selected mode and AWS
documentation.

### Sequence number

Kinesis assigns sequence numbers to records within a shard. They help
identify record position and ordering.

### Delivery behavior

Design the consumer for possible duplicate processing. Do not assume
end-to-end exactly-once processing merely because Kinesis and Lambda are
managed services.

------------------------------------------------------------------------

## 6. Implementation phases

### Phase 1 -- Local event generator

Create a Python application that generates transaction events.

Suggested modules:

-   `transaction_generator.py`: creates event payloads.
-   `event_schema.py`: defines required fields and schema version.
-   `producer.py`: publishes events.
-   `config.py`: reads environment variables.
-   `logger.py`: configures application logging.

Generator requirements:

1.  Generate unique event IDs.
2.  Generate transaction IDs.
3.  Select customers and products from a sample set.
4.  Generate realistic amounts.
5.  Randomly choose success and failure outcomes.
6.  Generate timestamps.
7.  Support a configurable event rate.
8.  Support a fixed random seed for repeatable tests.
9.  Allow invalid and duplicate events to be generated intentionally for
    testing.

Start with a local JSON Lines file before sending anything to AWS.

### Phase 2 -- Create Kinesis Data Stream

1.  Create a development stream.
2.  Choose an appropriate stream mode.
3.  Configure retention based on the experiment.
4.  Create a producer IAM role or user with minimum required
    permissions.
5.  Configure the Python producer with the stream name and AWS region.
6.  Publish records using `boto3`.
7.  Handle throttling and retryable errors.
8.  Record successful and failed publish counts.

Use the Kinesis `PutRecords` operation for batching where appropriate.
Inspect individual record results because a batch request can contain
records that failed even if the request itself returns successfully.

**Deliverable:** A Python producer publishing records to Kinesis.

### Phase 3 -- Create the Lambda consumer

Create a Lambda function and configure a Kinesis event source mapping.

The processor should:

1.  Read the event batch.
2.  Decode each Kinesis record from Base64.
3.  Parse the JSON payload.
4.  Validate required fields.
5.  Check the event schema version.
6.  Validate amounts and statuses.
7.  Apply business rules.
8.  Persist accepted events.
9.  Route invalid events to a failure destination or quarantine.
10. Log processing outcomes.

A Kinesis event source mapping invokes Lambda with batches of records.
Understand batch size, batching window, retry behavior and partial batch
response configuration.

**Deliverable:** Lambda processes valid events and handles invalid
records deliberately.

### Phase 4 -- Validation and business rules

Example validation rules:

-   Event ID cannot be empty.
-   Transaction ID cannot be empty.
-   Amount must be valid and non-negative.
-   Currency must be supported.
-   Status must be recognized.
-   Event timestamp must parse correctly.
-   Event version must be supported.

Example business rules:

-   Count successful transactions.
-   Count failed transactions.
-   Calculate successful transaction value.
-   Flag transactions above a configured threshold.
-   Detect repeated events with the same event ID.

Do not call a transaction fraudulent merely because it exceeds a
threshold. Use the term `flagged_for_review` for a demonstration rule.

### Phase 5 -- Idempotency and duplicate handling

This is one of the most important engineering components.

A stream consumer can process the same record again after retries or
failures. The system must avoid creating duplicate business effects.

One approach:

-   Use `event_id` as an idempotency key.
-   Store the processed event ID in DynamoDB.
-   Use a conditional write so the same event ID cannot be accepted
    twice.
-   If the condition fails because the event already exists, record it
    as a duplicate and avoid applying the business effect again.

For example, use a conditional `PutItem` operation with an
attribute-not-exists condition.

Think carefully about failure order. If you write to DynamoDB and then
fail before archiving to S3, a retry must not silently lose the archive.
A robust implementation may use a durable processing-state design,
retryable outbox pattern, or a separate archival delivery path.

Document the exact guarantee your implementation provides. Do not claim
global exactly-once delivery across Kinesis, Lambda, DynamoDB and S3.

### Phase 6 -- DynamoDB storage design

Create a DynamoDB table for transaction processing.

Example design:

  Attribute           Purpose
  ------------------- ----------------------------------------
  `event_id`          Partition key / idempotency identifier
  `transaction_id`    Business transaction identifier
  `customer_id`       Customer lookup attribute
  `status`            Processing or payment status
  `amount`            Transaction amount
  `event_timestamp`   Event time
  `processed_at`      Processing time

Choose keys and indexes based on the access patterns you actually need.

Possible access patterns:

-   Get event by event ID.
-   Get transaction by transaction ID.
-   Query a customer's recent transactions.
-   Track current status.

If you need efficient queries by customer and time, design a suitable
partition/sort key or secondary index rather than scanning the entire
table.

### Phase 7 -- S3 historical archive

Archive accepted events to S3 for historical analysis.

Suggested layout:

``` text
s3://<bucket-name>/
|
|-- raw-events/
|   |-- event_date=YYYY-MM-DD/
|
|-- processed-events/
|   |-- event_date=YYYY-MM-DD/
|       |-- event_type=PAYMENT_COMPLETED/
|       |-- event_type=PAYMENT_FAILED/
|
|-- rejected-events/
|   |-- event_date=YYYY-MM-DD/
|
|-- athena-results/
```

For the first implementation, Lambda can write small batches of records
to S3. For higher-volume or managed delivery, consider Kinesis Data
Firehose.

Avoid creating one tiny S3 object for every event. Buffer records into
appropriately sized files. Use Parquet for analytics-ready data when
practical.

### Phase 8 -- CloudWatch monitoring

Monitor:

-   Incoming records.
-   Successful records.
-   Failed records.
-   Duplicate events.
-   Invalid events.
-   Lambda invocation errors.
-   Lambda duration.
-   Lambda throttles.
-   Iterator age for Kinesis event source mapping.
-   DynamoDB throttling.
-   S3 archive failures.

Create a few useful CloudWatch alarms, such as:

-   Lambda error count above a threshold.
-   Iterator age exceeding a chosen threshold.
-   Sustained processing throttling.

Use structured logs with event IDs and processing outcomes. Do not log
sensitive payment credentials or secrets.

### Phase 9 -- Historical SQL analytics

Catalog the S3 processed-event data with AWS Glue Data Catalog and query
it using Athena.

Suggested questions:

1.  How many events arrived each hour?
2.  What is the success rate?
3.  What is the failed payment count?
4.  What is the successful transaction value by day?
5.  Which payment method is most frequently used?
6.  Which locations generate the most transactions?
7.  What is the average transaction amount?
8.  How many events were duplicates?
9.  How many events were rejected?
10. What is the distribution of event types?

Example query (adapt the table and column names to your schema):

``` sql
SELECT
    status,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM processed_events
GROUP BY status
ORDER BY transaction_count DESC;
```

Make sure your archive contains one clearly defined row per event before
using this query. Deduplicate or model event revisions explicitly if
your system archives retries.

### Phase 10 -- Testing and recovery

Test:

1.  Valid event.
2.  Invalid JSON.
3.  Missing event ID.
4.  Negative amount.
5.  Unsupported schema version.
6.  Duplicate event ID.
7.  Repeated Kinesis delivery.
8.  Lambda timeout.
9.  DynamoDB conditional-write failure.
10. S3 write failure.
11. Kinesis throttling.
12. Out-of-order event timestamps.
13. Large event batch.
14. Consumer recovery after failure.

For Kinesis-triggered Lambda, configure and test partial batch failure
reporting if using it. Understand how retry and record checkpointing
work. A failed batch can otherwise cause already-processed records to be
retried.

Document the recovery strategy and demonstrate at least one failure and
successful recovery.

------------------------------------------------------------------------

## 7. Suggested repository structure

``` text
aws-realtime-streaming-analytics/
|
|-- README.md
|-- requirements.txt
|-- .gitignore
|-- .env.example
|
|-- config/
|   |-- dev.yaml
|
|-- producer/
|   |-- transaction_generator.py
|   |-- event_schema.py
|   |-- kinesis_producer.py
|
|-- lambda/
|   |-- stream_processor.py
|   |-- validators.py
|   |-- idempotency.py
|   |-- storage.py
|   |-- error_handler.py
|
|-- storage/
|   |-- dynamodb_repository.py
|   |-- s3_archive.py
|
|-- analytics/
|   |-- metrics.py
|   |-- dashboard.py
|
|-- sql/
|   |-- transaction_analysis.sql
|   |-- hourly_metrics.sql
|
|-- infrastructure/
|   |-- template.yaml
|   |-- iam/
|
|-- tests/
|   |-- test_generator.py
|   |-- test_validation.py
|   |-- test_idempotency.py
|   |-- test_processor.py
|
|-- docs/
|   |-- architecture.md
|   |-- event_schema.md
|   |-- failure_recovery.md
|   |-- decisions.md
|
└-- screenshots/
```

Python package/module names should avoid ambiguous import paths. In a
real implementation, you may prefer `lambda_src/` rather than `lambda/`
because `lambda` is a Python keyword and cannot be used as a normal
import identifier.

------------------------------------------------------------------------

## 8. Testing strategy

### Unit tests

Test:

-   Event generation.
-   JSON serialization.
-   Required-field validation.
-   Timestamp parsing.
-   Amount validation.
-   Event-type validation.
-   Duplicate detection.
-   Idempotency key generation.
-   Business metric calculations.

### Integration tests

Test:

-   Producer to Kinesis.
-   Kinesis to Lambda.
-   Lambda to DynamoDB.
-   Lambda or delivery service to S3.
-   Glue Catalog and Athena queries.

Use mocks or local test doubles for fast unit tests. Use a dedicated AWS
development environment for cloud integration tests.

### Load tests

Run controlled tests at different event rates.

Record:

-   Events generated per second.
-   Events accepted per second.
-   Processing latency.
-   Error rate.
-   Retry count.
-   Approximate cost.

Do not publish unsupported throughput claims. Measure the results and
document the environment and configuration.

------------------------------------------------------------------------

## 9. Suggested monitoring dashboard

A simple dashboard can show:

  Metric               Meaning
  -------------------- -----------------------------------------------
  Events received      Incoming event volume
  Events processed     Successfully processed events
  Success rate         Successful events / valid payment events
  Failed payments      Payment failures
  Invalid events       Schema or validation failures
  Duplicate events     Repeated event IDs
  Average amount       Average accepted transaction amount
  Processing latency   Difference between event and processing times
  Lambda errors        Failed invocations or records
  Iterator age         Consumer lag indicator

Clearly distinguish payment failure from processing failure. A failed
payment can be processed successfully by the pipeline.

------------------------------------------------------------------------

## 10. Definition of done

-   [ ] Python event generator works.
-   [ ] Event schema is documented.
-   [ ] Kinesis stream receives events.
-   [ ] Lambda consumes batches.
-   [ ] Required-field and business validation works.
-   [ ] Duplicate handling is tested.
-   [ ] DynamoDB stores transaction state.
-   [ ] S3 stores historical event records.
-   [ ] CloudWatch logs and metrics are available.
-   [ ] Athena can query archived events.
-   [ ] Failure and retry scenarios are tested.
-   [ ] README and architecture diagram are complete.
-   [ ] AWS resources can be cleaned up safely.

------------------------------------------------------------------------

## 11. Interview preparation

Be prepared to explain:

-   What is stream processing?
-   How is stream processing different from batch processing?
-   What is Amazon Kinesis Data Streams?
-   What is a shard?
-   What is a partition key?
-   How does Kinesis ordering work?
-   What is a Kinesis sequence number?
-   How does Lambda consume Kinesis records?
-   What is an event source mapping?
-   What happens when Lambda fails on a batch?
-   What are partial batch failures?
-   Why can duplicate processing occur?
-   What is idempotency?
-   How did you implement duplicate protection?
-   Why did you choose DynamoDB?
-   What are DynamoDB partition and sort keys?
-   Why archive events in S3?
-   How do you avoid creating too many small files?
-   How do you monitor consumer lag?
-   How would you scale the stream?
-   How would you handle late-arriving events?
-   How would you recover from an S3 write failure?
-   What is the difference between event time and processing time?

Be ready to trace one event from the Python producer through Kinesis,
Lambda, DynamoDB and S3, including what happens if the Lambda function
fails and retries.

------------------------------------------------------------------------

## 12. Security and cost checklist

-   Use least-privilege IAM roles.
-   Keep Kinesis and DynamoDB access restricted.
-   Enable appropriate encryption.
-   Block public S3 access.
-   Never commit AWS keys or secrets.
-   Do not use real payment card details.
-   Use synthetic transaction data only.
-   Configure AWS Budget alerts.
-   Monitor Kinesis, Lambda, DynamoDB and S3 costs.
-   Delete unused streams, tables, buckets and log groups when
    appropriate.
-   Keep the environment small during development.

------------------------------------------------------------------------

## 13. What makes this project technically strong?

The strongest parts are not simply connecting AWS services. They are:

1.  Correct partition-key selection.
2.  Understanding at-least-once delivery and duplicate processing.
3.  Implementing idempotent business processing.
4.  Designing a reliable failure and retry strategy.
5.  Separating payment outcomes from pipeline outcomes.
6.  Managing event time and processing time.
7.  Measuring throughput and consumer lag.
8.  Designing storage for both operational lookup and historical
    analytics.

**Success criterion:** A working streaming pipeline that can ingest
events continuously, process them safely, recover from failures, store
them for lookup and historical analysis, and expose measurable
operational metrics.
