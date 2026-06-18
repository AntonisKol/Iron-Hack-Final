import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')


def send_failure_email(context):
    task_id = context['task_instance'].task_id
    dag_id  = context['task_instance'].dag_id
    email   = os.getenv('EMAIL_ADDRESS')
    msg = MIMEText(f'Task {task_id} in DAG {dag_id} has failed.')
    msg['Subject'] = f'Airflow Failure: {dag_id} - {task_id}'
    msg['From']    = email
    msg['To']      = email
    with smtplib.SMTP('mail.gmx.net', 587) as server:
        server.starttls()
        server.login(email, os.getenv('GMX_PASSWORD'))
        server.send_message(msg)
    print('Failure email sent')
