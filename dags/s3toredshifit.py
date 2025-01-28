from airflow import DAG
from datetime import datetime, timedelta
from tasks.s3_to_redshift_task import s3_to_redshift_task

# Configuração padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_to_redshift_dynamic',
    default_args=default_args,
    description='Leitura de CSV do S3 e Criação de Tabela no Redshift (Com Task Reutilizável)',
    schedule_interval=None,  # Execute manualmente
)


# Parâmetros
bucket_name = 'stn-airflow'
s3_file_key = 'meu-diretorio/2025-01-25/empresas6.csv'
schema = 'public'
table_name = 'bronze_empresas'
redshift_conn_id = "redshift_default"  # Substitua pelo ID da sua conexão Redshift no Airflow
iam_role = "arn:aws:redshift-serverless:us-east-1:885394832474:workgroup/a285a07f-23c5-4809-b0b4-0c9910e405a4"

# SQL para criar a tabela (ajuste conforme necessário)
create_sql = """
    coluna1 VARCHAR(255),
    coluna2 INT,
    coluna3 DATE
    -- Adicione as colunas conforme necessário
"""

# Chamada da função que cria as tasks
s3_to_redshift_task(
    dag=dag,
    bucket_name=bucket_name,
    s3_file_key=s3_file_key,
    schema=schema,
    table_name=table_name,
    redshift_conn_id=redshift_conn_id,
    create_sql=create_sql,
    iam_role=iam_role,
)
