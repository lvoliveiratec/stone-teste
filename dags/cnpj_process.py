from airflow import DAG
from datetime import datetime, timedelta
from tasks.s3_to_redshift_task import s3_to_redshift_task
from airflow.providers.docker.operators.docker import DockerOperator


# Configuração padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cnpj_process_dag',
    default_args=default_args,
    description='Leitura de CSV do S3 e Criação de Tabela no Redshift (Com Task Reutilizável)',
    schedule_interval=None,  # Execute manualmente
)


# Parâmetros
bucket_name = 'stn-airflow'
s3_file_key = 'meu-diretorio/2025-01-28/'
schema = 'public'
table_name = 'bronze_socios'
redshift_conn_id = "redshift_default"  # Substitua pelo ID da sua conexão Redshift no Airflow
iam_role = "arn:aws:iam::885394832474:role/role_redshift"

# SQL para criar a tabela (ajuste conforme necessário)
create_sql = """
        id VARCHAR(50),
        type VARCHAR(50),
        name VARCHAR(255),
        reference_id VARCHAR(50),
        value VARCHAR(50),
        date DATE,
        description VARCHAR(255),
        another_reference_id VARCHAR(50),
        person_name VARCHAR(255),
        some_value VARCHAR(50),
        another_number VARCHAR(50)
"""

run_docker = DockerOperator(
    task_id='run_docker_ecr_image',
    image='885394832474.dkr.ecr.us-east-1.amazonaws.com/stn-images/extract-cnpjs:latest',  # Substitua pela sua URL do ECR
    api_version='auto',  # Use 'auto' or a specific Docker API version
    auto_remove='success',  # Use 'success', 'never', or 'force'
    command=["sh", "-c", "'{\"start_date\":\"2024-06\",\"bucket_name\":\"stn-airflow\",\"s3_directory\":\"meu-diretorio/2025-01-29/\",\"tap_stream_id\":\"socios\"}' | extract-cnpjs --config -"],
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    timeout=1800,
    dag=dag,
)

# Chamada da função que cria as tasks
run_s3_to_redshift = s3_to_redshift_task(
    dag=dag,
    bucket_name=bucket_name,
    s3_file_key=s3_file_key,
    schema=schema,
    table_name=table_name,
    redshift_conn_id=redshift_conn_id,
    create_sql=create_sql,
    iam_role=iam_role,
)

run_docker >> run_s3_to_redshift