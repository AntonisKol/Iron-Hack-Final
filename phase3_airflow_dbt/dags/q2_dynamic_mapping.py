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

    def process_country(country):
        print(f'Processing data for: {country}')

    task_process = PythonOperator.partial(
        task_id='process_country',
        python_callable=process_country,
    ).expand(op_args=[['USA'], ['India'], ['UK'], ['Germany'], ['France']])
