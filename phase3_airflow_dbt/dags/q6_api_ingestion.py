from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import snowflake.connector
import requests
import json
import os

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='api_ingestion',
    default_args=default_args,
    description='Q6 - API Data Ingestion',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    def call_api(**context):
        url = 'https://jsonplaceholder.typicode.com/users'
        response = requests.get(url)
        data = response.json()
        print(f'API returned {len(data)} records')
        context['ti'].xcom_push(key='raw_data', value=json.dumps(data))

    task_call_api = PythonOperator(
        task_id='call_api',
        python_callable=call_api,
    )

    def parse_response(**context):
        raw = context['ti'].xcom_pull(task_ids='call_api', key='raw_data')
        users = json.loads(raw)
        parsed = []
        for user in users:
            parsed.append({
                'id': user['id'],
                'name': user['name'],
                'email': user['email'],
                'city': user['address']['city'],
                'company': user['company']['name'],
            })
        print(f'Parsed {len(parsed)} users')
        context['ti'].xcom_push(key='parsed_data', value=json.dumps(parsed))

    task_parse = PythonOperator(
        task_id='parse_response',
        python_callable=parse_response,
    )

    def save_to_snowflake(**context):
        parsed = context['ti'].xcom_pull(task_ids='parse_response', key='parsed_data')
        users = json.loads(parsed)

        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS API_USERS (
                id INT,
                name VARCHAR,
                email VARCHAR,
                city VARCHAR,
                company VARCHAR
            )
        """)

        for user in users:
            cursor.execute("""
                INSERT INTO API_USERS (id, name, email, city, company)
                VALUES (%s, %s, %s, %s, %s)
            """, (user['id'], user['name'], user['email'], user['city'], user['company']))

        print(f'Saved {len(users)} users to API_USERS table')
        conn.close()

    task_save = PythonOperator(
        task_id='save_to_snowflake',
        python_callable=save_to_snowflake,
    )

    task_call_api >> task_parse >> task_save
