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

    def choose_branch():
        if datetime.now().weekday() == 0:
            return 'full_load'
        return 'incremental_load'

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

    task_end = PythonOperator(
        task_id='pipeline_complete',
        python_callable=pipeline_complete,
        trigger_rule='none_failed_min_one_success',
    )

    task_branch >> [task_full, task_incremental] >> task_end
