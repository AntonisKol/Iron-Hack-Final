from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import snowflake.connector
import smtplib
from email.mime.text import MIMEText
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

DBT_PROJECT_DIR = '/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/phase3_airflow_dbt/churn_mart'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def send_failure_email(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    email = os.getenv('EMAIL_ADDRESS')
    msg = MIMEText(f'Task {task_id} in DAG {dag_id} has failed.')
    msg['Subject'] = f'Airflow Failure: {dag_id} - {task_id}'
    msg['From'] = email
    msg['To'] = email
    with smtplib.SMTP('mail.gmx.net', 587) as server:
        server.starttls()
        server.login(email, os.getenv('GMX_PASSWORD'))
        server.send_message(msg)

with DAG(
    dag_id='enterprise_elt',
    default_args=default_args,
    description='Q10 - Enterprise-Grade ELT Pipeline',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # ── STEP 1: INGEST CHECK ──────────────────────────────────────────────────
    # verify both datasets are loaded and accessible in Snowflake
    # bank fraud (1M rows) + telecom churn (7,043 rows)
    # if either is missing the pipeline stops immediately
    def ingest_check():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM FRAUD_SCHEMA.BANK_TRANSACTIONS')
        fraud_count = cursor.fetchone()[0]
        print(f'Bank transactions: {fraud_count} rows')

        cursor.execute('SELECT COUNT(*) FROM CHURN_RAW.RAW_CHURN')
        churn_count = cursor.fetchone()[0]
        print(f'Telecom churn records: {churn_count} rows')

        if fraud_count == 0 or churn_count == 0:
            raise Exception('Data missing — pipeline aborted')

        print('All data sources verified')
        conn.close()

    task_ingest_check = PythonOperator(
        task_id='ingest_check',
        python_callable=ingest_check,
        on_failure_callback=send_failure_email,
    )

    # ── STEP 2 & 3: BUILD DBT STAGING AND MARTS ───────────────────────────────
    # runs the full dbt project: staging → intermediate → marts
    # rebuilds all 6 models in Snowflake in the correct order
    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd "{DBT_PROJECT_DIR}" && /opt/homebrew/bin/dbt run --project-dir .',
        on_failure_callback=send_failure_email,
    )

    # ── STEP 4: RUN DBT TESTS ─────────────────────────────────────────────────
    # validates data quality rules defined in schema.yml
    # checks: unique customer_id, no nulls in key columns
    # pipeline fails here if data quality is broken — nothing bad reaches the report
    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd "{DBT_PROJECT_DIR}" && /opt/homebrew/bin/dbt test --project-dir .',
        on_failure_callback=send_failure_email,
    )

    # ── STEP 5: GENERATE FRAUD BUSINESS REPORT ────────────────────────────────
    # pulls from FRAUD_DAILY_SUMMARY (built by Q9) and FRAUD_SUMMARY (built by Q5)
    # produces the fraud side of the daily business report
    def fraud_business_report(**context):
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM FRAUD_SCHEMA.BANK_TRANSACTIONS WHERE is_fraud = 1')
        total_fraud = cursor.fetchone()[0]

        cursor.execute('SELECT ROUND(AVG(transaction_amount), 2) FROM FRAUD_SCHEMA.BANK_TRANSACTIONS WHERE is_fraud = 1')
        avg_fraud_amount = cursor.fetchone()[0]

        cursor.execute("""
            SELECT country, SUM(is_fraud) as fraud_count
            FROM FRAUD_SCHEMA.BANK_TRANSACTIONS
            GROUP BY country
            ORDER BY fraud_count DESC
            LIMIT 3
        """)
        top_countries = cursor.fetchall()

        print('=== FRAUD BUSINESS REPORT ===')
        print(f'Total confirmed fraud: {total_fraud}')
        print(f'Average fraud amount: ${avg_fraud_amount}')
        print('Top 3 countries by fraud:')
        for row in top_countries:
            print(f'  {row[0]}: {row[1]} cases')

        # push key metrics to XCom for the email task
        context['ti'].xcom_push(key='total_fraud', value=total_fraud)
        context['ti'].xcom_push(key='avg_fraud_amount', value=float(avg_fraud_amount))
        conn.close()

    task_fraud_report = PythonOperator(
        task_id='fraud_business_report',
        python_callable=fraud_business_report,
        on_failure_callback=send_failure_email,
    )

    # ── STEP 5: GENERATE CHURN BUSINESS REPORT ────────────────────────────────
    # pulls from MART_CHURN_KPIS built by dbt (Q7)
    # produces the churn side of the daily business report
    def churn_business_report(**context):
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM CHURN_RAW.MART_CHURN_KPIS ORDER BY churn_rate_pct DESC')
        rows = cursor.fetchall()

        print('=== CHURN BUSINESS REPORT ===')
        print(f'{"Contract":<20} {"Customers":>10} {"Churn%":>8} {"Annual Loss":>15}')
        print('-' * 60)
        for row in rows:
            print(f'{row[0]:<20} {row[1]:>10} {row[2]:>8} ${row[5]:>14}')

        # total annual revenue lost across all contracts
        cursor.execute('SELECT SUM(annual_revenue_lost) FROM CHURN_RAW.MART_CHURN_KPIS')
        total_loss = cursor.fetchone()[0]
        print(f'Total annual revenue at risk: ${total_loss}')

        context['ti'].xcom_push(key='total_churn_loss', value=float(total_loss))
        conn.close()

    task_churn_report = PythonOperator(
        task_id='churn_business_report',
        python_callable=churn_business_report,
        on_failure_callback=send_failure_email,
    )

    # ── STEP 6 & 7: SEND COMBINED BUSINESS REPORT EMAIL ──────────────────────
    # pulls metrics from XCom and sends a summary email
    # this is what the executive team would receive every morning
    def send_business_report(**context):
        total_fraud = context['ti'].xcom_pull(task_ids='fraud_business_report', key='total_fraud')
        avg_fraud = context['ti'].xcom_pull(task_ids='fraud_business_report', key='avg_fraud_amount')
        churn_loss = context['ti'].xcom_pull(task_ids='churn_business_report', key='total_churn_loss')

        email = os.getenv('EMAIL_ADDRESS')
        body = f"""
Daily Business Intelligence Report — {datetime.now().strftime('%Y-%m-%d')}

FRAUD SUMMARY
  Total confirmed fraud cases: {total_fraud}
  Average fraud transaction amount: ${avg_fraud}

CHURN SUMMARY
  Total annual revenue at risk from churn: ${churn_loss:,.2f}

Pipeline Status: All dbt models rebuilt and tested successfully.
        """

        msg = MIMEText(body)
        msg['Subject'] = f'Daily BI Report — {datetime.now().strftime("%Y-%m-%d")}'
        msg['From'] = email
        msg['To'] = email

        with smtplib.SMTP('mail.gmx.net', 587) as server:
            server.starttls()
            server.login(email, os.getenv('GMX_PASSWORD'))
            server.send_message(msg)

        print('Business report email sent')

    task_send_report = PythonOperator(
        task_id='send_business_report',
        python_callable=send_business_report,
        on_failure_callback=send_failure_email,
    )

    # full enterprise ELT flow:
    # verify data → build dbt models → test quality → fraud report → churn report → email
    task_ingest_check >> task_dbt_run >> task_dbt_test >> [task_fraud_report, task_churn_report] >> task_send_report
