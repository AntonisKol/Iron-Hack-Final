# Q2: Dynamic Task Mapping
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dynamic_mapping',
    default_args=default_args,
    description='Q2 - Dynamic Task Mapping',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Task 3: Function that processes one country — called once per country independently
    def process_country(country):
        print(f'Processing data for: {country}')

    # Task 1: The list of countries is defined inline in .expand() below
    # Task 2 & 4: Dynamic Task Mapping — instead of writing one task per country manually,
    # .partial() sets the shared arguments (task_id and callable) that are the same for every copy,
    # .expand() generates one task per item in the list at runtime — Airflow creates 5 tasks automatically.
    # Each country runs in its own task, so they execute in parallel (Task 4: verify in Airflow UI Grid view)
    task_process = PythonOperator.partial(
        task_id='process_country',
        python_callable=process_country,
    ).expand(op_args=[['USA'], ['India'], ['UK'], ['Germany'], ['France']])
