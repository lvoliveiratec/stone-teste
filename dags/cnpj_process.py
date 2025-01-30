from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import json
import boto3


# Configuração do boto3 para a região desejada
boto3.setup_default_session(region_name='us-east-1')

# Carregar a configuração do arquivo JSON
file_path = "/home/ubuntu/airflow/stone-teste/dags/pipeline_config/extract_cnpj_config.json"

with open(file_path, "r", encoding="utf-8") as file:
    config = json.load(file)

# Variáveis de configuração
GLUE_JOB_SILVER_TO_GOLD = config.get("GLUE_JOB_SILVER_TO_GOLD")  # job_silver_to_gold
DATABASE_ATHENA = config.get("DATABASE_ATHENA")  # meu_database
OUTPUT_QUERY_S3 = config.get("OUTPUT_QUERY_S3")  # f"s3://{BUCKET_NAME}/athena-results/"
DOCKER_IMAGE = config.get("DOCKER_IMAGE")
DOCKER_RUN = config.get("DOCKER_RUN")
ENDPOINTS = config.get("ENDPOINTS")
START_DATE = "{{ ds }}"  # Correto
START_ENDPOINT = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m') }}"
TARGET_S3_GOLD = config.get("TARGET_S3_GOLD")
TARGET_S3_GOLD = TARGET_S3_GOLD.replace("STARTDATE", START_DATE)
SOURCES_S3_GOLD = {}

# Definição da DAG
dag = DAG(
    "cnpjs_etl",
    schedule_interval="0 6 1 * *",  # Executa todo dia às 6h
    start_date=days_ago(1),
    catchup=False
)

# Inicializando lista de tasks
t = []

# Loop para criar as tasks de extração e transformação
for task in ENDPOINTS:
    # Ajuste de configuração com base na data
    data = json.loads(json.dumps(task).replace("STARTDATE", START_DATE).replace("START_ENDPOINT", START_ENDPOINT))
    task_id = data.get("tap_stream_id")
    glue_script_location = data.get("glue_scrip_location")
    source_s3_path = data.get("source_s3_path")
    target_s3_path = data.get("target_s3_path")
    
    # Adiciona os paths das fontes para o processo final de transformação
    if 'empresas' in target_s3_path:
        SOURCES_S3_GOLD["empresas"] = target_s3_path
    elif 'socios' in target_s3_path:
        SOURCES_S3_GOLD["socios"] = target_s3_path

    # Criando o grupo de tarefas para extração e transformação
    with TaskGroup(group_id=f"{task_id}_group", dag=dag) as task_group:
        
        # 1️⃣ Executar o Docker para extrair dados e salvar no S3 (Bronze)
        extract_data = DockerOperator(
            task_id=f'{task_id}_extract_data',
            image=DOCKER_IMAGE,
            api_version='auto',  # Use 'auto' ou uma versão específica da API do Docker
            auto_remove='success',  # Use 'success', 'never' ou 'force'
            command=["sh", "-c", f"{DOCKER_RUN}"],
            docker_url="unix://var/run/docker.sock",
            environment={
                "CONFIG": data,
                "TAP_NAME": f"{DOCKER_RUN}"
            },
            network_mode="bridge",
            timeout=1800,
            dag=dag,
        )

        # 2️⃣ Executar o AWS Glue Job para transformar Bronze → Silver
        transform_bronze_to_silver = GlueJobOperator(
            task_id=f"{task_id}_transform_bronze_to_silver",
            job_name=data.get("GLUE_JOB_BRONZE_TO_SILVER"),  # "job_bronze_to_silver"
            script_location=glue_script_location,
            iam_role_name="stn-ec2-role",
            region_name='us-east-1',
            script_args={
                "--SOURCE_S3_PATH": source_s3_path,
                "--TARGET_S3_PATH": target_s3_path
            },
            dag=dag
        )

        # Orquestração dentro do grupo de tarefas
        extract_data >> transform_bronze_to_silver
    
    # Adicionando o task group à lista de orquestração final
    t.append(task_group)

# 3️⃣ Executar o AWS Glue Job para transformar Silver → Gold
transform_silver_to_gold = GlueJobOperator(
    task_id=f"transform_silver_to_gold",
    job_name=GLUE_JOB_SILVER_TO_GOLD,
    script_location="s3://stn-glue/scripts/silver_to_gold.py",
    iam_role_name="stn-ec2-role",
    region_name='us-east-1',
    script_args={
        "--SOURCE_S3_PATH_EMPRESAS": SOURCES_S3_GOLD.get("empresas"),
        "--SOURCE_S3_PATH_SOCIOS": SOURCES_S3_GOLD.get("socios"),
        "--TARGET_S3_PATH": TARGET_S3_GOLD
    },
    dag=dag
)

# 4️⃣ Criar Views no Athena para consumo de BI
# TARGET_S3_GOLD = TARGET_S3_GOLD.rsplit("/", 2)[0] + "/"
create_athena_views = AthenaOperator(
    task_id=f"create_athena_views",
    query=f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_ATHENA}.cnpjs (
            cnpj STRING,
            qtde_socios INTEGER,
            flag_socio_estrangeiro BOOLEAN,
            doc_alvo BOOLEAN
        )
        STORED AS PARQUET
        LOCATION '{TARGET_S3_GOLD}'
    """,
    # PARTITIONED BY (data_arquivo STRING)
    database=DATABASE_ATHENA,
    output_location=OUTPUT_QUERY_S3,
    aws_conn_id="aws_default",
    region_name='us-east-1',
    dag=dag
)

# 5️⃣ Verificar o resultado da tabela criada no Athena
check_athena_table = AthenaOperator(
    task_id="check_athena_table",
    query=f"""
        SELECT * FROM {DATABASE_ATHENA}.cnpjs LIMIT 40;
    """,
    database=DATABASE_ATHENA,
    output_location=OUTPUT_QUERY_S3,
    aws_conn_id="aws_default",
    region_name='us-east-1',
    dag=dag
)


# Orquestração final das tasks
for task_group in t:
    task_group >> transform_silver_to_gold >> create_athena_views >> check_athena_table 
