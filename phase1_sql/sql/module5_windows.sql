USE DATABASE FRAUD_DB;
USE SCHEMA FRAUD_SCHEMA;

SELECT country,
    customer_id,
    ROUND(SUM(transaction_amount), 2) AS total_amount,
    RANK() OVER (
        PARTITION BY country
        ORDER BY SUM(transaction_amount) DESC
    ) AS country_rank
FROM BANK_TRANSACTIONS
GROUP BY country, customer_id
ORDER BY country, country_rank;

SELECT *
FROM (
        SELECT merchant_category,
            transaction_id,
            customer_id,
            transaction_amount,
            ROW_NUMBER() OVER (
                PARTITION BY merchant_category
                ORDER BY transaction_amount DESC
            ) AS row_num
        FROM BANK_TRANSACTIONS
    )
WHERE row_num <= 5
ORDER BY merchant_category, row_num;

SELECT customer_id,
    transaction_date,
    transaction_amount,
    ROUND(
        SUM(transaction_amount) OVER (
            PARTITION BY customer_id
            ORDER BY transaction_date
        ),
        2
    ) AS cumulative_amount
FROM BANK_TRANSACTIONS
ORDER BY customer_id, transaction_date;

SELECT customer_id,
    ROUND(SUM(transaction_amount), 2) AS total_amount,
    NTILE(10) OVER (
        ORDER BY SUM(transaction_amount) DESC
    ) AS decile,
    CASE
        WHEN NTILE(10) OVER (ORDER BY SUM(transaction_amount) DESC) = 1  THEN 'Top 10%'
        WHEN NTILE(10) OVER (ORDER BY SUM(transaction_amount) DESC) <= 2 THEN 'Top 20%'
        WHEN NTILE(10) OVER (ORDER BY SUM(transaction_amount) DESC) <= 3 THEN 'Top 30%'
        WHEN NTILE(10) OVER (ORDER BY SUM(transaction_amount) DESC) <= 5 THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END AS customer_tier
FROM BANK_TRANSACTIONS
GROUP BY customer_id
ORDER BY decile;
