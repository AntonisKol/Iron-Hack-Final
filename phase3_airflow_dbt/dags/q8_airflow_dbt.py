from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dag_utils import send_failure_email

# path to the dbt project — BashOperator needs to run commands from here
DBT_PROJECT_DIR = '/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/phase3_airflow_dbt/churn_mart'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='airflow_dbt',
    default_args=default_args,
    description='Q8 - Airflow + dbt Integration',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # task 1: run all dbt models — rebuilds all tables in Snowflake
    # BashOperator runs a shell command, just like typing in the terminal
    # --project-dir tells dbt where the project lives
    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd "{DBT_PROJECT_DIR}" && /opt/homebrew/bin/dbt run --project-dir .',
        on_failure_callback=send_failure_email,
    )

    # task 2: run dbt tests — checks unique and not_null rules we defined in schema.yml
    # only runs if dbt_run succeeded
    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd "{DBT_PROJECT_DIR}" && /opt/homebrew/bin/dbt test --project-dir .',
        on_failure_callback=send_failure_email,
    )

    # task 3: read the dbt log file and print it to Airflow logs
    # this captures what dbt did so you can review it in the Airflow UI
    def capture_logs():
        log_path = f'{DBT_PROJECT_DIR}/logs/dbt.log'
        if os.path.exists(log_path):
            with open(log_path, 'r') as f:
                lines = f.readlines()
            # print last 50 lines — most recent activity
            print('--- dbt log (last 50 lines) ---')
            for line in lines[-50:]:
                print(line.strip())
        else:
            print('No dbt log file found')

    task_capture_logs = PythonOperator(
        task_id='capture_logs',
        python_callable=capture_logs,
    )

    # dbt_run must pass before testing, tests must pass before capturing logs
    task_dbt_run >> task_dbt_test >> task_capture_logs
