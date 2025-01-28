from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

# Função para enviar o arquivo para o S3
def upload_file_to_s3(bucket_name, s3_key, file_path, aws_conn_id='aws_default'):
    hook = S3Hook(aws_conn_id=aws_conn_id)
    hook.load_file(
        filename=file_path,  # Caminho local do arquivo
        key=s3_key,          # Nome do arquivo no S3
        bucket_name=bucket_name,
        replace=True         # Sobrescreve o arquivo caso já exista
    )
    print(f"Arquivo enviado para S3: s3://{bucket_name}/{s3_key}")

# Configuração da DAG
default_args = {
    'owner': 'Lucas',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'upload_file_to_s3',
    default_args=default_args,
    description='DAG para enviar um arquivo para o S3',
    schedule_interval=None,  # Execute manualmente ou configure um cron
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task para enviar o arquivo
    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_file_to_s3,
        op_kwargs={
            'bucket_name': 'stn-airflow',      # Substitua pelo nome do bucket
            's3_key': 'meu-diretorio/arquivo.txt', # Caminho no bucket
            'file_path': '/home/ubuntu/airflow/stone-teste/dags/teste.txt', # Caminho local do arquivo
        },
    )

    upload_task
