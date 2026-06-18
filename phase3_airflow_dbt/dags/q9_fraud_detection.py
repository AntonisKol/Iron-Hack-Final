# Q9: Banking Fraud Detection Project
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import snowflake.connector
import os
from dag_utils import send_failure_email

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fraud_detection',
    default_args=default_args,
    description='Q9 - Banking Fraud Detection Pipeline',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    def load_check():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM BANK_TRANSACTIONS')
        count = cursor.fetchone()[0]
        print(f'Source data loaded: {count} transactions found')
        conn.close()

    task_load_check = PythonOperator(
        task_id='load_check',
        python_callable=load_check,
        on_failure_callback=send_failure_email,
    )

    def apply_fraud_rules():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE OR REPLACE TABLE FRAUD_ALERTS AS
            SELECT
                transaction_id,
                customer_id,
                transaction_amount,
                country,
                merchant_category,
                failed_attempts,
                credit_score,
                is_international,
                is_night_transaction,
                is_fraud,
                CASE
                    WHEN transaction_amount > (
                        SELECT AVG(transaction_amount) + 3 * STDDEV(transaction_amount)
                        FROM BANK_TRANSACTIONS
                    ) THEN 'High Amount Anomaly'
                    WHEN failed_attempts > 3 THEN 'Multiple Failed Attempts'
                    WHEN is_international = 1 AND is_night_transaction = 1
                        AND failed_attempts > 2 THEN 'International Night Fraud'
                    WHEN is_international = 1 AND credit_score < 600
                        AND transaction_amount > (
                            SELECT AVG(transaction_amount) FROM BANK_TRANSACTIONS
                        ) THEN 'High Risk International'
                    ELSE 'Other'
                END AS alert_type
            FROM BANK_TRANSACTIONS
            WHERE
                transaction_amount > (
                    SELECT AVG(transaction_amount) + 3 * STDDEV(transaction_amount)
                    FROM BANK_TRANSACTIONS
                )
                OR failed_attempts > 3
                OR (is_international = 1 AND is_night_transaction = 1 AND failed_attempts > 2)
                OR (is_international = 1 AND credit_score < 600 AND transaction_amount > (
                    SELECT AVG(transaction_amount) FROM BANK_TRANSACTIONS
                ))
        """)
        cursor.execute('SELECT COUNT(*) FROM FRAUD_ALERTS')
        count = cursor.fetchone()[0]
        print(f'Fraud rules applied: {count} suspicious transactions flagged')
        conn.close()

    task_apply_rules = PythonOperator(
        task_id='apply_fraud_rules',
        python_callable=apply_fraud_rules,
        on_failure_callback=send_failure_email,
    )

    def generate_summary():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE OR REPLACE TABLE FRAUD_DAILY_SUMMARY AS
            SELECT
                alert_type,
                COUNT(*)                                    AS total_alerts,
                SUM(is_fraud)                               AS confirmed_fraud,
                ROUND(SUM(is_fraud) / COUNT(*) * 100, 2)   AS fraud_confirmation_rate,
                ROUND(AVG(transaction_amount), 2)           AS avg_transaction_amount,
                ROUND(SUM(transaction_amount), 2)           AS total_amount_at_risk
            FROM FRAUD_ALERTS
            GROUP BY alert_type
            ORDER BY total_alerts DESC
        """)
        print('Fraud daily summary table created')
        conn.close()

    task_generate_summary = PythonOperator(
        task_id='generate_summary',
        python_callable=generate_summary,
        on_failure_callback=send_failure_email,
    )

    def fraud_report():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM FRAUD_DAILY_SUMMARY')
        rows = cursor.fetchall()
        print('=== DAILY FRAUD REPORT ===')
        print(f'{"Alert Type":<30} {"Alerts":>8} {"Confirmed":>10} {"Rate%":>8} {"Avg Amount":>12} {"Total At Risk":>15}')
        print('-' * 90)
        for row in rows:
            print(f'{row[0]:<30} {row[1]:>8} {row[2]:>10} {row[3]:>8} {row[4]:>12} {row[5]:>15}')
        conn.close()

    task_fraud_report = PythonOperator(
        task_id='fraud_report',
        python_callable=fraud_report,
        on_failure_callback=send_failure_email,
    )

    task_load_check >> task_apply_rules >> task_generate_summary >> task_fraud_report
