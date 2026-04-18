# from airflow.sdk import dag
from airflow.models import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
from datetime import datetime
# import pandas as pd

@dag(
    dag_id='user_processing',
    start_date=datetime(2026, 4, 19),
    schedule_interval='@daily',
    catchup=False,
    tags=['data-processing']
)

def user_processing():
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

    # 데이터 변환 task
    # @task
    # def transform_user_data(sql_result):
    #     """SQL 쿼리 결과를 변환합니다"""
    #     df = pd.DataFrame(sql_result)
    #     # 데이터 변환 로직 추가
    #     df['processed_date'] = datetime.now()
    #     return df.to_dict('records')

    # # Task 의존성 설정
    # result = create_table
    # transformed_data = transform_user_data(result)

user_processing()