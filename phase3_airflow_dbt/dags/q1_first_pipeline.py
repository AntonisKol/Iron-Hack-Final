from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import time


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='first_pipeline',
    default_args=default_args,
    description='Q1 - First Airflow Pipeline',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    def print_started():
        print('Data Pipeline Started')

    task_start = PythonOperator(
        task_id='pipeline_started',
        python_callable=print_started,
    )

    def wait_ten_seconds():
        print('Waiting 10 seconds...')
        time.sleep(10)
        print('Done waiting')

    task_wait = PythonOperator(
        task_id='wait_10_seconds',
        python_callable=wait_ten_seconds,
    )

    def print_completed():
        print('Data Pipeline Completed')

    task_end = PythonOperator(
        task_id='pipeline_completed',
        python_callable=print_completed,
    )

    task_start >> task_wait >> task_end
