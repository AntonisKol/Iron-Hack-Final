# Q1: First Airflow Pipeline
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import time

# default_args apply to every task in this DAG unless a task overrides them
default_args = {
    'owner': 'airflow',
    'retries': 1,              # retry once automatically if a task fails
    'retry_delay': timedelta(minutes=5),  # wait 5 minutes before retrying
}

# Task 1: Create a DAG named first_pipeline
# schedule='0 8 * * *' → cron syntax for "every day at 08:00 AM"
# catchup=False → if Airflow was offline, do NOT backfill missed past runs
with DAG(
    dag_id='first_pipeline',
    default_args=default_args,
    description='Q1 - First Airflow Pipeline',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Task 2: Add a task that prints 'Data Pipeline Started'
    def print_started():
        print('Data Pipeline Started')

    task_start = PythonOperator(
        task_id='pipeline_started',
        python_callable=print_started,  # Airflow calls this function when the task executes
    )

    # Task 3: Add a task that waits for 10 seconds
    def wait_ten_seconds():
        print('Waiting 10 seconds...')
        time.sleep(10)
        print('Done waiting')

    task_wait = PythonOperator(
        task_id='wait_10_seconds',
        python_callable=wait_ten_seconds,
    )

    # Task 4: Add a final task that prints 'Data Pipeline Completed'
    def print_completed():
        print('Data Pipeline Completed')

    task_end = PythonOperator(
        task_id='pipeline_completed',
        python_callable=print_completed,
    )

    # Task 5: Define execution order using >> (bitshift operator)
    # >> means "then run" — task_start must finish before task_wait starts, and so on
    # Airflow UI will show this as a linear graph: started → wait → completed
    task_start >> task_wait >> task_end
