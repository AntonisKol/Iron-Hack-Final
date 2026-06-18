USE DATABASE FRAUD_DB;
USE SCHEMA FRAUD_SCHEMA;

-- Q6: Top 20 customers by total transaction amount
SELECT customer_id,
    ROUND(SUM(transaction_amount), 2) AS total_amount,
    COUNT(*) AS txn_count
FROM BANK_TRANSACTIONS
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 20;

-- Q7: Average transaction amount by age group
SELECT CASE
        WHEN customer_age BETWEEN 18 AND 25 THEN '18-25'
        WHEN customer_age BETWEEN 26 AND 35 THEN '26-35'
        WHEN customer_age BETWEEN 36 AND 50 THEN '36-50'
        WHEN customer_age BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS txn_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM BANK_TRANSACTIONS
GROUP BY age_group
ORDER BY avg_amount DESC;

-- Q8: Fraud rate by age group
SELECT CASE
        WHEN customer_age BETWEEN 18 AND 25 THEN '18-25'
        WHEN customer_age BETWEEN 26 AND 35 THEN '26-35'
        WHEN customer_age BETWEEN 36 AND 50 THEN '36-50'
        WHEN customer_age BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM BANK_TRANSACTIONS
GROUP BY age_group
ORDER BY fraud_rate DESC;

-- Q9: High-balance low-frequency potential dormant accounts
SELECT customer_id,
    ROUND(AVG(account_balance), 2) AS avg_balance,
    COUNT(*) AS txn_count,
    ROUND(AVG(transaction_freq_monthly), 2) AS avg_monthly_freq
FROM BANK_TRANSACTIONS
GROUP BY customer_id
HAVING AVG(account_balance) > (
        SELECT AVG(account_balance)
        FROM BANK_TRANSACTIONS
    )
    AND AVG(transaction_freq_monthly) < (
        SELECT AVG(transaction_freq_monthly)
        FROM BANK_TRANSACTIONS
    )
ORDER BY avg_balance DESC
LIMIT 20;

-- Q10: Average account balance and credit score by country
SELECT country,
    ROUND(AVG(account_balance), 2) AS avg_balance,
    ROUND(AVG(credit_score), 2) AS avg_credit_score
FROM BANK_TRANSACTIONS
GROUP BY country
ORDER BY avg_balance DESC;
