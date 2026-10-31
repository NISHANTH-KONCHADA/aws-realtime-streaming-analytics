-- ============================================================================
-- Hourly Operational & Throughput Metrics (Amazon Athena)
-- ============================================================================

-- Query 1: Total Events and Volume by Hour
SELECT
    date_trunc('hour', from_iso8601_timestamp(event_timestamp)) AS processing_hour,
    event_type,
    COUNT(*) AS total_events,
    ROUND(SUM(amount), 2) AS total_gross_value,
    ROUND(AVG(amount), 2) AS average_amount
FROM ecommerce_analytics_db.processed_events
WHERE event_date = current_date
GROUP BY 1, 2
ORDER BY processing_hour DESC, total_events DESC;

-- Query 2: Hourly Success vs Failure Rate
SELECT
    date_trunc('hour', from_iso8601_timestamp(event_timestamp)) AS transaction_hour,
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS successful_transactions,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_transactions,
    ROUND(
        100.0 * COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) / NULLIF(COUNT(*), 0),
        2
    ) AS success_rate_percentage
FROM ecommerce_analytics_db.processed_events
WHERE event_date >= date_add('day', -7, current_date)
GROUP BY 1
ORDER BY transaction_hour DESC;

-- Query 3: Cumulative Daily Financial Settlement Value
SELECT
    event_date,
    currency,
    COUNT(*) AS successful_transactions,
    ROUND(SUM(amount), 2) AS settled_volume_sum,
    ROUND(MIN(amount), 2) AS minimum_order,
    ROUND(MAX(amount), 2) AS maximum_order
FROM ecommerce_analytics_db.processed_events
WHERE status = 'SUCCESS'
GROUP BY event_date, currency
ORDER BY event_date DESC;
