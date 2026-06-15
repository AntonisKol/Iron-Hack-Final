USE DATABASE FRAUD_DB;
-- work inside this database
USE SCHEMA FRAUD_SCHEMA;
-- narrow down to this schema
-- Q11: Fraud rate by merchant category
SELECT merchant_category,
    COUNT(*) AS txn_count,
    -- total transactions per category
    SUM(is_fraud) AS fraud_count,
    -- fraud cases per category
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate -- fraud % per category
FROM BANK_TRANSACTIONS
GROUP BY merchant_category
ORDER BY fraud_rate DESC;
-- Q12: Fraud rate by payment method
SELECT payment_method,
    COUNT(*) AS txn_count,
    -- total transactions per method
    SUM(is_fraud) AS fraud_count,
    -- fraud cases per method
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate -- fraud % per method
FROM BANK_TRANSACTIONS
GROUP BY payment_method
ORDER BY fraud_rate DESC;
-- Q13: Fraud rate by device type
SELECT device_type,
    COUNT(*) AS txn_count,
    -- total transactions per device
    SUM(is_fraud) AS fraud_count,
    -- fraud cases per device
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate -- fraud % per device
FROM BANK_TRANSACTIONS
GROUP BY device_type
ORDER BY fraud_rate DESC;
-- Q14: Most common fraud type
SELECT fraud_type,
    COUNT(*) AS count,
    -- how many times this fraud type appears
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage -- % of all transactions
FROM BANK_TRANSACTIONS
WHERE is_fraud = 1 -- only look at fraud cases
GROUP BY fraud_type
ORDER BY count DESC;
-- Q15: Fraud during night vs day and weekend vs weekday
SELECT CASE
        WHEN is_night_transaction = 1 THEN 'Night'
        ELSE 'Day'
    END AS time_of_day,
    CASE
        WHEN is_weekend = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) AS txn_count,
    -- total transactions
    SUM(is_fraud) AS fraud_count,
    -- fraud cases
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate -- fraud %
FROM BANK_TRANSACTIONS
GROUP BY time_of_day,
    day_type
ORDER BY fraud_rate DESC;