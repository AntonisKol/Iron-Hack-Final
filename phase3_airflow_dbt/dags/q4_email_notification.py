# Q4: Email Notification
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
# send_failure_email: connects to GMX SMTP on failure, sends an email naming the DAG, task, and time
from dag_utils import send_failure_email

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='email_notification',
    default_args=default_args,
    description='Q4 - Email Notification on Failure',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    def task_ok():
        print('This task works fine')

    # this task succeeds — no email triggered
    task_success = PythonOperator(
        task_id='task_success',
        python_callable=task_ok,
    )

    def task_fail():
        raise Exception('Simulated failure — triggers the email notification')

    # on_failure_callback: when this task fails, Airflow calls send_failure_email automatically
    # Airflow passes the full context dict (dag_id, task_id, execution_date, etc.) to the callback
    task_failure = PythonOperator(
        task_id='task_failure',
        python_callable=task_fail,
        on_failure_callback=send_failure_email,
    )

    task_success >> task_failure
