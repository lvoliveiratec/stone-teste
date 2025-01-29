import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, countDistinct, max, coalesce, lit

# Lendo argumentos passados pelo Glue
args = getResolvedOptions(sys.argv, ['SOURCE_S3_PATH_EMPRESAS', 'SOURCE_S3_PATH_SOCIOS', 'TARGET_S3_PATH'])

# Criando a Spark Session
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Leitura dos dados da camada Silver (Empresas)
empresas_df = spark.read.parquet(args["SOURCE_S3_PATH_EMPRESAS"])

# Leitura dos dados da camada Silver (Sócios)
socios_df = spark.read.parquet(args["SOURCE_S3_PATH_SOCIOS"])

# Contar o número de sócios distintos por CNPJ
qtde_socios_df = socios_df.groupBy("cnpj").agg(countDistinct("documento_socio").alias("qtde_socios"))

# Identificar se algum sócio é estrangeiro (supondo que sócios estrangeiros tenham `codigo_qualificacao_socio = 23`)
flag_socio_estrangeiro_df = socios_df.withColumn(
    "flag_socio_estrangeiro",
    when(col("codigo_qualificacao_socio") == 23, True).otherwise(False)  # Sem aspas para tratar como número
).groupBy("cnpj").agg(
    max("flag_socio_estrangeiro").alias("flag_socio_estrangeiro")  # Se houver ao menos 1 True, o CNPJ recebe True
)

# Juntar os DataFrames de empresas e sócios com as transformações
gold_df = (
    empresas_df
    .join(qtde_socios_df, on="cnpj", how="left")
    .join(flag_socio_estrangeiro_df, on="cnpj", how="left")
    .withColumn(
        "qtde_socios",
        coalesce(col("qtde_socios"), lit(0))  # Substitui NULL por 0 quando não há sócios
    )
    .withColumn(
        "flag_socio_estrangeiro",
        coalesce(col("flag_socio_estrangeiro"), lit(False))  # Substitui NULL por False
    )
    .withColumn(
        "doc_alvo",
        when((col("cod_porte") == "03") & (col("qtde_socios") > 1), True).otherwise(False)
    )
)

# Selecionar as colunas desejadas
gold_df = gold_df.select(
    "cnpj",
    "qtde_socios",
    "flag_socio_estrangeiro",
    "doc_alvo"
)

# Salvando na camada Gold (Parquet)
gold_df.write.mode("overwrite").parquet(args["TARGET_S3_PATH"])

print("Transformação de Silver para Gold concluída!")
