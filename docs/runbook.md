# Deployment and Operations Runbook

This guide walks through local simulation, automated testing, and deploying the pipeline to AWS using the AWS Serverless Application Model (SAM).

---

## 1. Prerequisites

- Python 3.11+ (Python 3.14 compatible)
- AWS CLI v2 configured with valid credentials (`aws configure`)
- AWS SAM CLI installed (`sam --version`)

---

## 2. Local Setup and Testing

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Execute the automated test suite:**
   ```bash
   python -m pytest -v
   ```

3. **Generate local simulated transaction events:**
   ```bash
   python -m producer.main --mode local --count 100 --output local_events.jsonl
   ```

4. **Run continuous streaming simulation in dry-run mode:**
   ```bash
   python -m producer.main --mode stream --rate 5 --dry-run
   ```

---

## 3. Deploying to AWS via SAM

1. **Build the SAM application:**
   ```bash
   cd infrastructure
   sam build
   ```

2. **Deploy the stack interactively:**
   ```bash
   sam deploy --guided
   ```
   *Follow the prompts (Stack Name: `ecommerce-streaming-pipeline`, Region: `us-east-1`).*

3. **Verify deployed resources:**
   - Kinesis Data Stream: `ecommerce-transaction-stream-development`
   - DynamoDB Table: `ecommerce-transactions-development`
   - S3 Bucket: `ecommerce-streaming-analytics-development-<account_id>`

---

## 4. Streaming Live Events to AWS

Once deployed, publish live events to your Kinesis stream:

```bash
python -m producer.main \
  --mode stream \
  --stream-name ecommerce-transaction-stream-development \
  --region us-east-1 \
  --rate 10 \
  --failure-rate 0.15 \
  --duplicate-rate 0.05
```

---

## 5. Athena Analytics Execution

1. Navigate to the **Amazon Athena** console in `us-east-1`.
2. Execute the table creation script: [sql/01_glue_catalog_tables.sql](file:///c:/Users/konch/OneDrive/Desktop/AWS_Realtime_Streaming_Analytics/sql/01_glue_catalog_tables.sql)
3. Run analytical queries:
   - [sql/02_hourly_metrics.sql](file:///c:/Users/konch/OneDrive/Desktop/AWS_Realtime_Streaming_Analytics/sql/02_hourly_metrics.sql)
   - [sql/03_business_analytics.sql](file:///c:/Users/konch/OneDrive/Desktop/AWS_Realtime_Streaming_Analytics/sql/03_business_analytics.sql)

---

## 6. Teardown / Resource Cleanup

To prevent unnecessary AWS charges when testing is finished:

```bash
cd infrastructure
sam delete --stack-name ecommerce-streaming-pipeline
```
*(Empty the S3 bucket via AWS console or `aws s3 rm s3://<bucket-name> --recursive` before deleting the stack).*
