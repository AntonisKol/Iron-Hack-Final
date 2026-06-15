USE DATABASE FRAUD_DB;
USE SCHEMA FRAUD_SCHEMA;
-- Q6: Top 20 customers by SUM(transaction_amount)
SELECT customer_id,
    ROUND(SUM(transaction_amount), 2) AS total_amount,
    -- total spent by each customer
    COUNT(*) AS txn_count -- how many transactions they made
FROM BANK_TRANSACTIONS
GROUP BY customer_id -- one row per customer
ORDER BY total_amount DESC -- highest spenders first
LIMIT 20;
-- top 20 only
-- Q7: Average transaction amount by age group
SELECT CASE
        WHEN customer_age BETWEEN 18 AND 25 THEN '18-25'
        WHEN customer_age BETWEEN 26 AND 35 THEN '26-35'
        WHEN customer_age BETWEEN 36 AND 50 THEN '36-50'
        WHEN customer_age BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END AS age_group,
    -- bucket each customer into a group
    COUNT(*) AS txn_count,
    -- how many transactions in this group
    ROUND(AVG(transaction_amount), 2) AS avg_amount -- average transaction value
FROM BANK_TRANSACTIONS
GROUP BY age_group
ORDER BY avg_amount DESC;
-- Q8: Age group with highest fraud rate
SELECT CASE
        WHEN customer_age BETWEEN 18 AND 25 THEN '18-25'
        WHEN customer_age BETWEEN 26 AND 35 THEN '26-35'
        WHEN customer_age BETWEEN 36 AND 50 THEN '36-50'
        WHEN customer_age BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    -- total frauds in group
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate -- fraud % per age group
FROM BANK_TRANSACTIONS
GROUP BY age_group
ORDER BY fraud_rate DESC;
-- Q9: Customers with high balance, low frequency, potential dormant accounts
SELECT customer_id,
    ROUND(AVG(account_balance), 2) AS avg_balance,
    -- how much money they have
    COUNT(*) AS txn_count,
    -- how active they are
    ROUND(AVG(transaction_freq_monthly), 2) AS avg_monthly_freq -- average monthly transactions
FROM BANK_TRANSACTIONS
GROUP BY customer_id
HAVING AVG(account_balance) > (
        SELECT AVG(account_balance)
        FROM BANK_TRANSACTIONS
    ) -- above average balance
    AND AVG(transaction_freq_monthly) < (
        SELECT AVG(transaction_freq_monthly)
        FROM BANK_TRANSACTIONS
    ) -- below average frequency
ORDER BY avg_balance DESC
LIMIT 20;
-- Q10: Average account balance and credit score by country
SELECT country,
    ROUND(AVG(account_balance), 2) AS avg_balance,
    -- average money held per country
    ROUND(AVG(credit_score), 2) AS avg_credit_score -- average creditworthiness per country
FROM BANK_TRANSACTIONS
GROUP BY country
ORDER BY avg_balance DESC;