# from airflow.sdk import dag
# from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
from datetime import datetime
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
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
        create_table()

    # 데이터 변환 task
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available()  -> PokeReturnValue:
        # headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        # response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json", headers=headers)
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(f"API Response Status Code: {response.status_code}")
        if  response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    # Task 의존성 설정
    # extract_user_data() >> is_api_available()
    is_api_available()

user_processing()
