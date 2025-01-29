from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
from airflow.utils.helpers import chain



def check_file_in_s3(bucket_name,prefix,aws_conn_id='aws_default'):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id, region_name='us-east-1')
    
    # Listar os arquivos no bucket
    file_list = s3_hook.list_keys(bucket_name,prefix)
    
    if file_list:  # Se a lista não estiver vazia, significa que há arquivos no bucket
        logging.info(f"Arquivos encontrados no bucket {bucket_name}: {file_list}")
    else:
        raise FileNotFoundError(f"Nenhum arquivo encontrado no bucket {bucket_name}.")

# Função para criar a tabela no Redshift
def create_redshift_table(redshift_conn_id, schema, table_name, create_sql):
    postgres_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            {create_sql}
        )
        SORTKEY (date)
        ;
    """
    postgres_hook.run(create_sql)
    logging.info(f"Tabela {table_name} criada ou já existente no Redshift.")


# Função para carregar os dados do S3 para o Redshift
def load_csv_to_redshift(redshift_conn_id, bucket_name, s3_file_key, schema, table_name, iam_role, csv_delimiter=';'):
    load_sql = f"""
        COPY {schema}.{table_name}
        FROM 's3://{bucket_name}/{s3_file_key}'
        IAM_ROLE '{iam_role}'
        FORMAT AS CSV
        DELIMITER '{csv_delimiter}' QUOTE '"'
        REGION AS 'us-east-1'
        IGNOREHEADER 1;
    """
    postgres_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    postgres_hook.run(load_sql)
    logging.info(f"Dados carregados para a tabela {table_name} no Redshift.")


# Função que encapsula as tasks
def s3_to_redshift_task(dag, bucket_name, s3_file_key, schema, table_name, redshift_conn_id, create_sql, iam_role):
    # Task para verificar o arquivo no S3
    check_s3_task = PythonOperator(
        task_id=f'check_s3_{table_name}_file',
        python_callable=check_file_in_s3,
        op_args=[bucket_name, s3_file_key],
        dag=dag,
    )

    # Task para criar a tabela no Redshift
    create_table_task = PythonOperator(
        task_id=f'create_{table_name}_table',
        python_callable=create_redshift_table,
        op_args=[redshift_conn_id, schema, table_name, create_sql],
        dag=dag,
    )

    # Task para carregar os dados para o Redshift
    load_data_task = PythonOperator(
        task_id=f'load_{table_name}_from_s3',
        python_callable=load_csv_to_redshift,
        op_args=[redshift_conn_id, bucket_name, s3_file_key, schema, table_name, iam_role],
        dag=dag,
    )

    # Definindo a ordem de execução das tasks
    check_s3_task >> create_table_task >> load_data_task
    
    return (check_s3_task >> create_table_task >> load_data_task)
