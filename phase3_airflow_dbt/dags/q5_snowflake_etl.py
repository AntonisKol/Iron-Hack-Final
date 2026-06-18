from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import snowflake.connector
import os

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
    dag_id='snowflake_etl',
    default_args=default_args,
    description='Q5 - Snowflake ETL Pipeline',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    def check_source_data():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM BANK_TRANSACTIONS')
        count = cursor.fetchone()[0]
        print(f'Source rows found: {count}')
        conn.close()

    task_check = PythonOperator(
        task_id='check_source_data',
        python_callable=check_source_data,
    )

    def run_transformation():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE OR REPLACE TABLE FRAUD_SUMMARY AS
            SELECT
                country,
                merchant_category,
                COUNT(*) AS total_transactions,
                SUM(is_fraud) AS total_fraud,
                ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct,
                ROUND(AVG(transaction_amount), 2) AS avg_amount
            FROM BANK_TRANSACTIONS
            GROUP BY country, merchant_category
            ORDER BY fraud_rate_pct DESC
        """)
        print('Transformation complete — FRAUD_SUMMARY created')
        conn.close()

    task_transform = PythonOperator(
        task_id='run_transformation',
        python_callable=run_transformation,
    )

    def row_count_report():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM FRAUD_SUMMARY')
        count = cursor.fetchone()[0]
        print(f'FRAUD_SUMMARY table has {count} rows')
        cursor.execute('SELECT * FROM FRAUD_SUMMARY LIMIT 5')
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        conn.close()

    task_report = PythonOperator(
        task_id='row_count_report',
        python_callable=row_count_report,
    )

    task_check >> task_transform >> task_report
