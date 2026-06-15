USE DATABASE FRAUD_DB;
-- work inside this database
USE SCHEMA FRAUD_SCHEMA;
-- narrow down to this schema
-- Q1: Aggregate metrics
SELECT COUNT(*) AS total_transactions,
    -- every row = one transaction
    COUNT(DISTINCT customer_id) AS total_customers,
    -- unique customers only
    SUM(is_fraud) AS total_fraud_transactions,
    -- 0+1 column, sum = count of frauds
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_percentage -- fraud cases / total * 100
FROM BANK_TRANSACTIONS;
-- Q2: Top 10 countries by transaction volume
SELECT country,
    COUNT(*) AS transaction_count -- count rows per country
FROM BANK_TRANSACTIONS
GROUP BY country -- one row per country
ORDER BY transaction_count DESC -- highest first
LIMIT 10;
-- Q3: Top 10 cities by transaction value
SELECT city,
    ROUND(SUM(transaction_amount), 2) AS total_value -- sum all amounts per city, round to 2 decimals
FROM BANK_TRANSACTIONS
GROUP BY city -- one row per city
ORDER BY total_value DESC -- highest spending cities first
LIMIT 10;
-- Q4: Data profiling report
SELECT 'transaction_id' AS column_name,
    COUNT(DISTINCT transaction_id) AS distinct_values,
    SUM(
        CASE
            WHEN transaction_id IS NULL THEN 1
            ELSE 0
        END
    ) AS null_count
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'customer_id',
    COUNT(DISTINCT customer_id),
    SUM(
        CASE
            WHEN customer_id IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'transaction_date',
    COUNT(DISTINCT transaction_date),
    SUM(
        CASE
            WHEN transaction_date IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'transaction_time',
    COUNT(DISTINCT transaction_time),
    SUM(
        CASE
            WHEN transaction_time IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'merchant_category',
    COUNT(DISTINCT merchant_category),
    SUM(
        CASE
            WHEN merchant_category IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'payment_method',
    COUNT(DISTINCT payment_method),
    SUM(
        CASE
            WHEN payment_method IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'device_type',
    COUNT(DISTINCT device_type),
    SUM(
        CASE
            WHEN device_type IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'country',
    COUNT(DISTINCT country),
    SUM(
        CASE
            WHEN country IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'city',
    COUNT(DISTINCT city),
    SUM(
        CASE
            WHEN city IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'customer_age',
    COUNT(DISTINCT customer_age),
    SUM(
        CASE
            WHEN customer_age IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'credit_score',
    COUNT(DISTINCT credit_score),
    SUM(
        CASE
            WHEN credit_score IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'account_balance',
    COUNT(DISTINCT account_balance),
    SUM(
        CASE
            WHEN account_balance IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'transaction_amount',
    COUNT(DISTINCT transaction_amount),
    SUM(
        CASE
            WHEN transaction_amount IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'is_fraud',
    COUNT(DISTINCT is_fraud),
    SUM(
        CASE
            WHEN is_fraud IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
UNION ALL
SELECT 'fraud_type',
    COUNT(DISTINCT fraud_type),
    SUM(
        CASE
            WHEN fraud_type IS NULL THEN 1
            ELSE 0
        END
    )
FROM BANK_TRANSACTIONS
ORDER BY column_name;
--No data quality issues found.
-- Q5: % distribution by payment method, device type, merchant category
SELECT 'payment_method' AS dimension,
    payment_method AS value,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage -- count per group / total * 100
FROM BANK_TRANSACTIONS
GROUP BY payment_method
UNION ALL
SELECT 'device_type' AS dimension,
    device_type AS value,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM BANK_TRANSACTIONS
GROUP BY device_type
UNION ALL
SELECT 'merchant_category' AS dimension,
    merchant_category AS value,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM BANK_TRANSACTIONS
GROUP BY merchant_category
ORDER BY dimension,
    percentage DESC;