from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils.dates import days_ago
import boto3

boto3.setup_default_session(region_name='us-east-1')

# Configurações
BUCKET_NAME = "meu-data-lake"
GLUE_JOB_BRONZE_TO_SILVER = "job_bronze_to_silver"
GLUE_JOB_SILVER_TO_GOLD = "job_silver_to_gold"
DATABASE_ATHENA = "meu_database"
OUTPUT_QUERY_S3 = f"s3://{BUCKET_NAME}/athena-results/"

dag = DAG(
    "s3_glue_athena_pipeline",
    schedule_interval="0 6 * * *",  # Executa todo dia às 6h
    start_date=days_ago(1),
    catchup=False
)

# 1️⃣ Executar o Docker para extrair dados e salvar no S3 (Bronze)
extract_data = DockerOperator(
    task_id='extract_data',
    image='885394832474.dkr.ecr.us-east-1.amazonaws.com/stn-images/extract-cnpjs:latest',  # Substitua pela sua URL do ECR
    api_version='auto',  # Use 'auto' or a specific Docker API version
    auto_remove='success',  # Use 'success', 'never', or 'force'
    command=["sh", "-c", "extract-cnpjs"],
    docker_url="unix://var/run/docker.sock",
    environment={
        "CONFIG":{"start_date":"2025-01","bucket_name":"stn-bronze","s3_directory":"cnpjs/empresas/2025-01-29/","tap_stream_id":"empresas","fieldnames":"cnpj,razao_social,natureza_juridica,qualificacao_responsavel,capital_social,cod_porte"},
        "TAP_NAME": "extract-cnpjs"
        },
    network_mode="bridge",
    timeout=1800,
    dag=dag,
)

# 2️⃣ Executar o AWS Glue Job para transformar Bronze → Silver
transform_bronze_to_silver = GlueJobOperator(
    task_id="transform_bronze_to_silver",
    job_name=GLUE_JOB_BRONZE_TO_SILVER,
    script_location=f"s3://{BUCKET_NAME}/scripts/bronze_to_silver.py",
    s3_bucket=BUCKET_NAME,
    iam_role_name="AWSGlueServiceRole",
    region_name='us-east-1',
    dag=dag
)

# 3️⃣ Executar o AWS Glue Job para transformar Silver → Gold
transform_silver_to_gold = GlueJobOperator(
    task_id="transform_silver_to_gold",
    job_name=GLUE_JOB_SILVER_TO_GOLD,
    script_location=f"s3://{BUCKET_NAME}/scripts/silver_to_gold.py",
    s3_bucket=BUCKET_NAME,
    iam_role_name="AWSGlueServiceRole",
    region_name='us-east-1',
    dag=dag
)

# 4️⃣ Criar Views no Athena para consumo de BI
create_athena_views = AthenaOperator(
    task_id="create_athena_views",
    query=f"""
        CREATE OR REPLACE VIEW gold_faturamento_mensal AS
        SELECT 
            EXTRACT(YEAR FROM data_compra) AS ano,
            EXTRACT(MONTH FROM data_compra) AS mes,
            SUM(valor_total) AS total_faturado
        FROM {DATABASE_ATHENA}.silver_pedidos
        GROUP BY 1, 2;
    """,
    database=DATABASE_ATHENA,
    output_location=OUTPUT_QUERY_S3,
    aws_conn_id="aws_default",
    dag=dag
)

# Orquestração das tasks
extract_data >> transform_bronze_to_silver >> transform_silver_to_gold >> create_athena_views
