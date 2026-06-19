# Q3: Branching Workflow
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

    # Task 1: BranchPythonOperator — returns the task_id of the branch to execute
    # .weekday() returns 0 on Monday, 1 on Tuesday ... 6 on Sunday
    def choose_branch():
        if datetime.now().weekday() == 0:   # Task 2: Monday → run Full Load
            return 'full_load'
        return 'incremental_load'           # Task 3: any other day → run Incremental Load

    task_branch = BranchPythonOperator(
        task_id='choose_branch',
        python_callable=choose_branch,
    )

    def full_load():
        print('Running Full Load')

    task_full = PythonOperator(
        task_id='full_load',
        python_callable=full_load,
    )

    def incremental_load():
        print('Running Incremental Load')

    task_incremental = PythonOperator(
        task_id='incremental_load',
        python_callable=incremental_load,
    )

    def pipeline_complete():
        print('Pipeline Complete')

    # Task 4: Merge both branches back into one final task
    # trigger_rule='none_failed_min_one_success' is required here:
    # without it Airflow waits for BOTH upstream tasks and stalls forever
    # when one branch is intentionally skipped
    task_end = PythonOperator(
        task_id='pipeline_complete',
        python_callable=pipeline_complete,
        trigger_rule='none_failed_min_one_success',
    )

    # fan out to both options → only one runs → both reconnect to task_end
    task_branch >> [task_full, task_incremental] >> task_end
