USE DATABASE FRAUD_DB;
USE SCHEMA FRAUD_SCHEMA;
-- Q16: Transactions exceeding AVG + 3 * STDDEV (anomalies)
SELECT transaction_id,
    customer_id,
    transaction_amount,
    ROUND(AVG(transaction_amount) OVER (), 2) AS avg_amount,
    ROUND(STDDEV(transaction_amount) OVER (), 2) AS stddev_amount,
    is_fraud,
    CASE
        WHEN is_fraud = 1 THEN 'Confirmed Fraud Anomaly'
        ELSE 'Suspicious — Not Flagged as Fraud'
    END AS anomaly_report -- labels each anomaly for reporting
FROM BANK_TRANSACTIONS
WHERE transaction_amount > (
        SELECT AVG(transaction_amount) + 3 * STDDEV(transaction_amount)
        FROM BANK_TRANSACTIONS
    )
ORDER BY transaction_amount DESC;
-- Q17: Customers with more than 3 failed attempts AND fraud = 1
SELECT customer_id,
    MAX(failed_attempts) AS max_failed_attempts,
    -- highest failed attempts recorded
    COUNT(*) AS txn_count,
    -- total transactions
    SUM(is_fraud) AS fraud_count -- confirmed fraud cases
FROM BANK_TRANSACTIONS
WHERE is_fraud = 1
    AND failed_attempts > 3 -- both conditions from the task requirement
GROUP BY customer_id
ORDER BY max_failed_attempts DESC;
-- Q18: Does recent PIN change increase fraud probability?
SELECT CASE
        WHEN pin_changed_recently = 1 THEN 'PIN Changed Recently'
        ELSE 'PIN Not Changed'
    END AS pin_status,
    COUNT(*) AS txn_count,
    -- total transactions
    SUM(is_fraud) AS fraud_count,
    -- fraud cases
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate -- fraud % per group
FROM BANK_TRANSACTIONS
GROUP BY pin_status
ORDER BY fraud_rate DESC;
-- Q19: Fraud rate for international vs domestic transactions
SELECT CASE
        WHEN is_international = 1 THEN 'International'
        ELSE 'Domestic'
    END AS transaction_type,
    COUNT(*) AS txn_count,
    -- total transactions
    SUM(is_fraud) AS fraud_count,
    -- fraud cases
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate -- fraud %
FROM BANK_TRANSACTIONS
GROUP BY transaction_type
ORDER BY fraud_rate DESC;

-- Q20: Customers with international + night + failed_attempts > 2, ranked by risk
SELECT
    customer_id,
    COUNT(*)                                    AS txn_count,        -- total qualifying transactions
    SUM(is_fraud)                               AS fraud_count,      -- confirmed frauds
    MAX(failed_attempts)                        AS max_failed,       -- worst failed attempts recorded
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate,       -- fraud %
    RANK() OVER (ORDER BY SUM(is_fraud) DESC)  AS risk_rank         -- rank by fraud count
FROM BANK_TRANSACTIONS
WHERE is_international = 1          -- must be international
AND is_night_transaction = 1        -- must be at night
AND failed_attempts > 2             -- must have more than 2 failed attempts
GROUP BY customer_id
ORDER BY risk_rank;
