-- ============================================================================
-- AWS Glue Catalog / Amazon Athena External Table Definitions
-- ============================================================================

-- 1. Database Creation
CREATE DATABASE IF NOT EXISTS ecommerce_analytics_db
COMMENT 'Real-Time E-Commerce Streaming Analytics Database'
LOCATION 's3://ecommerce-streaming-analytics-archive/';

-- 2. Processed Events External Table
-- Partitioned by event_date (YYYY-MM-DD) and event_type
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_analytics_db.processed_events (
    event_id STRING COMMENT 'Unique event identifier',
    event_version INT COMMENT 'Schema contract version',
    transaction_id STRING COMMENT 'Financial transaction identifier',
    customer_id STRING COMMENT 'Customer account identifier',
    order_id STRING COMMENT 'Order reference identifier',
    amount DOUBLE COMMENT 'Monetary transaction amount',
    currency STRING COMMENT 'ISO currency code',
    payment_method STRING COMMENT 'Payment instrument used',
    status STRING COMMENT 'Transaction status (SUCCESS, FAILED)',
    location STRING COMMENT 'City / geographical location',
    event_timestamp STRING COMMENT 'UTC event occurrence timestamp'
)
PARTITIONED BY (
    event_date STRING,
    event_type STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true',
    'mapping.event_timestamp' = 'event_timestamp'
)
STORED AS TEXTFILE
LOCATION 's3://ecommerce-streaming-analytics-archive/processed-events/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.event_date.type' = 'date',
    'projection.event_date.range' = '2026-01-01,NOW',
    'projection.event_date.format' = 'yyyy-MM-dd',
    'projection.event_date.interval' = '1',
    'projection.event_date.interval.unit' = 'DAYS',
    'projection.event_type.type' = 'enum',
    'projection.event_type.values' = 'PAYMENT_COMPLETED,PAYMENT_FAILED,ORDER_CREATED,ORDER_CANCELLED,REFUND_ISSUED',
    'storage.location.template' = 's3://ecommerce-streaming-analytics-archive/processed-events/event_date=${event_date}/event_type=${event_type}'
);

-- 3. Rejected & Quarantined Events External Table
-- Partitioned by event_date and quarantine reason
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_analytics_db.rejected_events (
    quarantined_at STRING COMMENT 'Timestamp when record was quarantined',
    raw_payload STRING COMMENT 'Unmodified raw payload string',
    error_details STRING COMMENT 'Validation error message'
)
PARTITIONED BY (
    event_date STRING,
    reason STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://ecommerce-streaming-analytics-archive/rejected-events/';
