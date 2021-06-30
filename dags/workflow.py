from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from src.Pipeline import run

default_args = {
    'owner': 'paty-oliveira',
    'email': 'patriciia.mota@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='earthquake_pipeline',
    default_args=default_args,
    description='DAG for EarthQuake pipeline',
    schedule_interval='@daily',
    start_date=datetime(2021, 6, 30)
) as dag:

    task_1 = BashOperator(
        task_id='initialization',
        bash_command='echo Starting EarthQuake pipeline'
    )

    task_2 = PythonOperator(
        task_id='pipeline',
        python_callable=run()
    )

task_1 >> task_2
