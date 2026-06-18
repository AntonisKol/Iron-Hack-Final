USE DATABASE FRAUD_DB;
USE SCHEMA FRAUD_SCHEMA;

SELECT transaction_id,
    customer_id,
    transaction_amount,
    ROUND(AVG(transaction_amount) OVER (), 2) AS avg_amount,
    ROUND(STDDEV(transaction_amount) OVER (), 2) AS stddev_amount,
    is_fraud,
    CASE
        WHEN is_fraud = 1 THEN 'Confirmed Fraud Anomaly'
        ELSE 'Suspicious — Not Flagged as Fraud'
    END AS anomaly_report
FROM BANK_TRANSACTIONS
WHERE transaction_amount > (
        SELECT AVG(transaction_amount) + 3 * STDDEV(transaction_amount)
        FROM BANK_TRANSACTIONS
    )
ORDER BY transaction_amount DESC;

SELECT customer_id,
    MAX(failed_attempts) AS max_failed_attempts,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count
FROM BANK_TRANSACTIONS
WHERE is_fraud = 1
    AND failed_attempts > 3
GROUP BY customer_id
ORDER BY max_failed_attempts DESC;

SELECT CASE
        WHEN pin_changed_recently = 1 THEN 'PIN Changed Recently'
        ELSE 'PIN Not Changed'
    END AS pin_status,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM BANK_TRANSACTIONS
GROUP BY pin_status
ORDER BY fraud_rate DESC;

SELECT CASE
        WHEN is_international = 1 THEN 'International'
        ELSE 'Domestic'
    END AS transaction_type,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM BANK_TRANSACTIONS
GROUP BY transaction_type
ORDER BY fraud_rate DESC;

SELECT
    customer_id,
    COUNT(*)                                    AS txn_count,
    SUM(is_fraud)                               AS fraud_count,
    MAX(failed_attempts)                        AS max_failed,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate,
    RANK() OVER (ORDER BY SUM(is_fraud) DESC)  AS risk_rank
FROM BANK_TRANSACTIONS
WHERE is_international = 1
AND is_night_transaction = 1
AND failed_attempts > 2
GROUP BY customer_id
ORDER BY risk_rank;
