from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import time


# default settings applied to all tasks in this DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,                            
    'retry_delay': timedelta(minutes=5), 
}

# define the DAG
with DAG(
    dag_id='first_pipeline',                   
    default_args=default_args,
    description='Q1 - First Airflow Pipeline',
    schedule='0 8 * * *',                      # run daily at 8:00 AM (cron format)
    start_date=datetime(2026, 1, 1),         
    catchup=False,                       
) as dag:

    # task 1: print pipeline started
    def print_started():
        print('Data Pipeline Started')

    task_start = PythonOperator(
        task_id='pipeline_started',        
        python_callable=print_started,         # function to run
    )

    # task 2: wait 10 seconds
    def wait_ten_seconds():
        print('Waiting 10 seconds...')
        time.sleep(10)
        print('Done waiting')

    task_wait = PythonOperator(
        task_id='wait_10_seconds',
        python_callable=wait_ten_seconds,
    )

    # task 3: print pipeline completed
    def print_completed():
        print('Data Pipeline Completed')

    task_end = PythonOperator(
        task_id='pipeline_completed',
        python_callable=print_completed,
    )

    # define the order: start → wait → end
    task_start >> task_wait >> task_end
