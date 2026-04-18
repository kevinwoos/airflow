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
    @task.sensor(poke_interval=30, timeout=600)
    def is_api_available():
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(f"API Response Status Code: {response.status_code}")
        if  response.status_code == 200:
            condition = True
            data = response.json()
            ip_address = data['metadata']['ipAddress']
        else:
            condition = False
            ip_address = None
        
        return PokeReturnValue(is_done=condition, xcom_value={'ipAddress': ip_address})

    # Task 의존성 설정
    extract_user_data() >> is_api_available()

class CheckUserDataSensor(BaseSensorOperator):
    def poke(self, context):
        # 조건 체크: 예를 들어, users 테이블에 데이터가 있는지 확인
        # 간단한 예시로 True를 반환
        return PokeReturnValue(is_done=True, xcom_value={'status': 'data_ready', 'message': 'User data is available'})

user_processing()