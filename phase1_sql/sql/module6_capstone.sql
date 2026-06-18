-- Q25: Executive fraud report
-- One query combining CTEs, CASE, Window Functions and Aggregations
WITH transaction_kpis AS (
    -- CTE 1: overall transaction metrics
    SELECT COUNT(*) AS total_transactions,
        -- total number of transactions
        ROUND(SUM(transaction_amount), 2) AS total_transaction_value,
        -- total money moved
        ROUND(AVG(transaction_amount), 2) AS avg_transaction_amount -- average transaction size
    FROM BANK_TRANSACTIONS
),
fraud_kpis AS (
    SELECT SUM(is_fraud) AS total_fraud_cases,
        ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_percentage,
        -- remove WHERE here
        (
            SELECT MODE(fraud_type)
            FROM BANK_TRANSACTIONS
            WHERE is_fraud = 1
        ) AS top_fraud_type -- subquery for top fraud type
    FROM BANK_TRANSACTIONS -- no WHERE clause here so percentage is correct
),
risk_kpis AS (
    -- CTE 3: highest risk dimensions
    SELECT FIRST_VALUE(country) OVER (
            ORDER BY SUM(is_fraud) DESC
        ) AS highest_risk_country,
        -- country with most frauds
        FIRST_VALUE(merchant_category) OVER (
            ORDER BY SUM(is_fraud) DESC
        ) AS highest_risk_merchant,
        -- merchant with most frauds
        FIRST_VALUE(payment_method) OVER (
            ORDER BY SUM(is_fraud) DESC
        ) AS highest_risk_payment -- payment method with most frauds
    FROM BANK_TRANSACTIONS
    GROUP BY country,
        merchant_category,
        payment_method
    LIMIT 1
), customer_kpis AS (
    -- CTE 4: profile of fraudulent customers only
    SELECT ROUND(AVG(credit_score), 2) AS avg_credit_score_fraudsters,
        -- how creditworthy are fraudsters
        ROUND(AVG(account_balance), 2) AS avg_balance_fraudsters,
        -- how much money fraudsters have
        ROUND(AVG(failed_attempts), 2) AS avg_failed_attempts_fraudsters -- how many failed attempts before fraud
    FROM BANK_TRANSACTIONS
    WHERE is_fraud = 1 -- only look at fraud rows
) -- final SELECT joins all 4 CTEs into one executive row
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
-- cross join all CTEs into one row
-- Q26: Create view for high risk transactions
-- International + Night + failed_attempts > 2
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
WHERE is_international = 1 -- must be international
    AND is_night_transaction = 1 -- must be at night
    AND failed_attempts > 2;
-- more than 2 failed attempts
-- verify the view works
SELECT COUNT(*)
FROM VW_HIGH_RISK_TRANSACTIONS;
-- Q27: Materialized view for daily fraud reporting
CREATE OR REPLACE MATERIALIZED VIEW MV_DAILY_FRAUD_REPORT AS
SELECT transaction_date,
    COUNT(*) AS total_transactions,
    -- transactions per day
    SUM(is_fraud) AS fraud_cases,
    -- frauds per day
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate,
    -- fraud % per day
    ROUND(SUM(transaction_amount), 2) AS total_amount -- total value per day
FROM BANK_TRANSACTIONS
GROUP BY transaction_date;
-- verify it works
SELECT *
FROM MV_DAILY_FRAUD_REPORT
LIMIT 10;
-- Q28: Detect suspicious customers using only SQL
SELECT customer_id,
    ROUND(AVG(transaction_amount), 2) AS avg_transaction_amount,
    -- their average transaction
    ROUND(AVG(credit_score), 2) AS avg_credit_score,
    -- their credit score
    MAX(failed_attempts) AS max_failed_attempts,
    -- worst failed attempts
    COUNT(*) AS txn_count,
    -- total transactions
    SUM(is_fraud) AS confirmed_frauds -- confirmed fraud cases
FROM BANK_TRANSACTIONS
WHERE is_international = 1 -- must be international
    AND failed_attempts > 2 -- more than 2 failed attempts
    AND credit_score < 600 -- low credit score
    AND transaction_amount > (
        SELECT AVG(transaction_amount)
        FROM BANK_TRANSACTIONS -- above overall average amount
    )
GROUP BY customer_id
ORDER BY confirmed_frauds DESC,
    max_failed_attempts DESC;