from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='branching_workflow',
    default_args=default_args,
    description='Q3 - Branching Workflow',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # checks what day it is and returns the task_id of the branch to run
    # weekday() returns 0 for Monday, 1 for Tuesday, ... 6 for Sunday
    def choose_branch():
        if datetime.now().weekday() == 0:  # 0 = Monday
            return 'full_load'
        return 'incremental_load'

    # BranchPythonOperator runs choose_branch() and tells Airflow which task to execute next
    # the other task gets skipped automatically
    task_branch = BranchPythonOperator(
        task_id='choose_branch',
        python_callable=choose_branch,
    )

    # runs on Monday — reloads everything from scratch
    def full_load():
        print('Running Full Load')

    task_full = PythonOperator(
        task_id='full_load',
        python_callable=full_load,
    )

    # runs every other day — only updates what changed
    def incremental_load():
        print('Running Incremental Load')

    task_incremental = PythonOperator(
        task_id='incremental_load',
        python_callable=incremental_load,
    )

    def pipeline_complete():
        print('Pipeline Complete')

    # trigger_rule tells Airflow: run this task even if one branch was skipped
    # without it, Airflow would wait for both branches and get stuck
    task_end = PythonOperator(
        task_id='pipeline_complete',
        python_callable=pipeline_complete,
        trigger_rule='none_failed_min_one_success',
    )

    # branch fans out to both tasks, but only one runs
    # both reconnect to task_end at the finish
    task_branch >> [task_full, task_incremental] >> task_end
