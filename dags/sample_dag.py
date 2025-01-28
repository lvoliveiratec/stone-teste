from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG('meu_dag_teste', default_args=default_args, schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')
