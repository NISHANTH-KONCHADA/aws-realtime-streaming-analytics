-- ============================================================================
-- Historical E-Commerce Business Intelligence & Analytics Queries
-- ============================================================================

-- Query 1: Payment Method Breakdown and Failure Frequency
SELECT
    payment_method,
    COUNT(*) AS total_attempts,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS successful_count,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count,
    ROUND(
        100.0 * COUNT(CASE WHEN status = 'FAILED' THEN 1 END) / NULLIF(COUNT(*), 0),
        2
    ) AS failure_rate_percentage,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END), 2) AS successful_gmv
FROM ecommerce_analytics_db.processed_events
GROUP BY payment_method
ORDER BY total_attempts DESC;

-- Query 2: Geographical Distribution of High-Volume Cities
SELECT
    location,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(amount), 2) AS total_city_gmv,
    ROUND(AVG(amount), 2) AS avg_basket_size
FROM ecommerce_analytics_db.processed_events
WHERE status = 'SUCCESS'
GROUP BY location
ORDER BY total_city_gmv DESC
LIMIT 10;

-- Query 3: High-Value Transactions Flagged for Review (>= 10,000 INR)
SELECT
    event_id,
    transaction_id,
    customer_id,
    amount,
    currency,
    payment_method,
    location,
    event_timestamp
FROM ecommerce_analytics_db.processed_events
WHERE amount >= 10000.00
ORDER BY amount DESC;

-- Query 4: Customer Repeat Purchase Behavior
SELECT
    customer_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(amount), 2) AS customer_lifetime_value,
    MIN(event_timestamp) AS first_seen_timestamp,
    MAX(event_timestamp) AS latest_seen_timestamp
FROM ecommerce_analytics_db.processed_events
WHERE status = 'SUCCESS'
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY customer_lifetime_value DESC
LIMIT 20;

-- Query 5: Quarantined & Rejected Events Analysis (Poison Pills)
SELECT
    reason,
    COUNT(*) AS rejected_count,
    MAX(quarantined_at) AS most_recent_occurrence
FROM ecommerce_analytics_db.rejected_events
GROUP BY reason
ORDER BY rejected_count DESC;
