USE DATABASE FRAUD_DB;
USE SCHEMA FRAUD_SCHEMA;

-- Q11: Fraud rate by merchant category
SELECT merchant_category,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM BANK_TRANSACTIONS
GROUP BY merchant_category
ORDER BY fraud_rate DESC;

-- Q12: Fraud rate by payment method
SELECT payment_method,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM BANK_TRANSACTIONS
GROUP BY payment_method
ORDER BY fraud_rate DESC;

-- Q13: Fraud rate by device type
SELECT device_type,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM BANK_TRANSACTIONS
GROUP BY device_type
ORDER BY fraud_rate DESC;

-- Q14: Most common fraud type with percentage
SELECT fraud_type,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM BANK_TRANSACTIONS
WHERE is_fraud = 1
GROUP BY fraud_type
ORDER BY count DESC;

-- Q15: Fraud timing — night vs day, weekend vs weekday
SELECT CASE
        WHEN is_night_transaction = 1 THEN 'Night'
        ELSE 'Day'
    END AS time_of_day,
    CASE
        WHEN is_weekend = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM BANK_TRANSACTIONS
GROUP BY time_of_day, day_type
ORDER BY fraud_rate DESC;
