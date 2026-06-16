from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

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
    print('Failure email sent')

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

    task_success = PythonOperator(
        task_id='task_success',
        python_callable=task_ok,
    )

    # on_failure_callback runs send_failure_email when this task fails
    def task_fail():
        raise Exception('Simulated failure — triggers the email notification')

    task_failure = PythonOperator(
        task_id='task_failure',
        python_callable=task_fail,
        on_failure_callback=send_failure_email,  # called automatically on failure
    )

    task_success >> task_failure
