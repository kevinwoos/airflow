# from airflow.sdk import dag
# from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
from datetime import datetime
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
# import pandas as pd

@dag(
    dag_id="user_processing",
    start_date=datetime(2026, 4, 19),
    schedule="@daily",
    catchup=False,
    tags=["data-processing"],
)

def user_processing():
    
    @task
    def extract_user_data():
        # 테이블 생성 task
        create_table = SQLExecuteQueryOperator(
            task_id='create_table',
            sql="""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                firstname VARCHAR(255),
                lastname VARCHAR(255),
                email VARCHAR(255),
                created_at timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            conn_id='postgres',
            database='airflow'
        )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available()  -> PokeReturnValue:
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(f"API Response Status Code: {response.status_code}")
        if  response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)
    
    @task
    def process_user(user_info):
        import csv

        user_info =  {
            "id": 1,
            "firstname": "John",
            "lastname": "Doe",
            "email": "jone.doe@example.com",
        }
    
        with open('/tmp/processed_user.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename='/tmp/user_info.csv'
        )

    fake_user = is_api_available()
    user_info = extract_user_data()
    process_user(fake_user)
    store_user()

user_processing()
