CREATE DATABASE IF NOT EXISTS FRAUD_DB;
USE DATABASE FRAUD_DB;
CREATE SCHEMA IF NOT EXISTS FRAUD_SCHEMA;
USE SCHEMA FRAUD_SCHEMA;
CREATE OR REPLACE TABLE BANK_TRANSACTIONS (
        transaction_id VARCHAR,
        customer_id VARCHAR,
        transaction_date DATE,
        transaction_time TIME,
        hour_of_day INT,
        is_weekend INT,
        is_night_transaction INT,
        country VARCHAR,
        city VARCHAR,
        merchant_category VARCHAR,
        payment_method VARCHAR,
        device_type VARCHAR,
        customer_age INT,
        credit_score INT,
        account_age_years FLOAT,
        account_balance FLOAT,
        transaction_amount FLOAT,
        num_prev_transactions INT,
        transaction_freq_monthly INT,
        distance_from_home_km FLOAT,
        time_since_last_txn_hrs FLOAT,
        is_international INT,
        failed_attempts INT,
        pin_changed_recently INT,
        is_fraud INT,
        fraud_type VARCHAR
    );
CREATE OR REPLACE FILE FORMAT fraud_csv_format TYPE = 'CSV' FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    SKIP_HEADER = 1;
CREATE OR REPLACE STAGE fraud_stage FILE_FORMAT = fraud_csv_format COMMENT = 'Internal stage for bank fraud CSV files';
COPY INTO BANK_TRANSACTIONS
FROM @fraud_stage / bank_fraud.csv FILE_FORMAT = (FORMAT_NAME = 'fraud_csv_format') ON_ERROR = 'CONTINUE';
