-- Q25: Executive fraud report — CTEs, CASE, window functions, aggregations
WITH transaction_kpis AS (
    SELECT COUNT(*) AS total_transactions,
        ROUND(SUM(transaction_amount), 2) AS total_transaction_value,
        ROUND(AVG(transaction_amount), 2) AS avg_transaction_amount
    FROM BANK_TRANSACTIONS
),
fraud_kpis AS (
    SELECT SUM(is_fraud) AS total_fraud_cases,
        ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_percentage,
        (
            SELECT MODE(fraud_type)
            FROM BANK_TRANSACTIONS
            WHERE is_fraud = 1
        ) AS top_fraud_type
    FROM BANK_TRANSACTIONS
),
risk_kpis AS (
    SELECT FIRST_VALUE(country) OVER (
            ORDER BY SUM(is_fraud) DESC
        ) AS highest_risk_country,
        FIRST_VALUE(merchant_category) OVER (
            ORDER BY SUM(is_fraud) DESC
        ) AS highest_risk_merchant,
        FIRST_VALUE(payment_method) OVER (
            ORDER BY SUM(is_fraud) DESC
        ) AS highest_risk_payment
    FROM BANK_TRANSACTIONS
    GROUP BY country,
        merchant_category,
        payment_method
    LIMIT 1
), customer_kpis AS (
    SELECT ROUND(AVG(credit_score), 2) AS avg_credit_score_fraudsters,
        ROUND(AVG(account_balance), 2) AS avg_balance_fraudsters,
        ROUND(AVG(failed_attempts), 2) AS avg_failed_attempts_fraudsters
    FROM BANK_TRANSACTIONS
    WHERE is_fraud = 1
)
SELECT t.total_transactions,
    t.total_transaction_value,
    t.avg_transaction_amount,
    f.total_fraud_cases,
    f.fraud_percentage,
    f.top_fraud_type,
    r.highest_risk_country,
    r.highest_risk_merchant,
    r.highest_risk_payment,
    c.avg_credit_score_fraudsters,
    c.avg_balance_fraudsters,
    c.avg_failed_attempts_fraudsters
FROM transaction_kpis t,
    fraud_kpis f,
    risk_kpis r,
    customer_kpis c;
-- Q26: Create view VW_HIGH_RISK_TRANSACTIONS
CREATE OR REPLACE VIEW VW_HIGH_RISK_TRANSACTIONS AS
SELECT transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    country,
    merchant_category,
    payment_method,
    failed_attempts,
    is_fraud
FROM BANK_TRANSACTIONS
WHERE is_international = 1
    AND is_night_transaction = 1
    AND failed_attempts > 2;
SELECT COUNT(*)
FROM VW_HIGH_RISK_TRANSACTIONS;
-- Q27: Create materialized view MV_DAILY_FRAUD_REPORT for daily fraud reporting
CREATE OR REPLACE MATERIALIZED VIEW MV_DAILY_FRAUD_REPORT AS
SELECT transaction_date,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_cases,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM BANK_TRANSACTIONS
GROUP BY transaction_date;
SELECT *
FROM MV_DAILY_FRAUD_REPORT
LIMIT 10;
-- Q28: Detect suspicious customers using SQL
SELECT customer_id,
    ROUND(AVG(transaction_amount), 2) AS avg_transaction_amount,
    ROUND(AVG(credit_score), 2) AS avg_credit_score,
    MAX(failed_attempts) AS max_failed_attempts,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS confirmed_frauds
FROM BANK_TRANSACTIONS
WHERE is_international = 1
    AND failed_attempts > 2
    AND credit_score < 600
    AND transaction_amount > (
        SELECT AVG(transaction_amount)
        FROM BANK_TRANSACTIONS
    )
GROUP BY customer_id
ORDER BY confirmed_frauds DESC,
    max_failed_attempts DESC;