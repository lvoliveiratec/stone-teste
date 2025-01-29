import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Lendo argumentos passados pelo Glue
args = getResolvedOptions(sys.argv, ['SOURCE_S3_PATH', 'TARGET_S3_PATH'])

# Criando a Spark Session
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# Definição do Schema
schema = StructType([
    StructField("cnpj", StringType(), True),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", IntegerType(), True),
    StructField("qualificacao_responsavel", IntegerType(), True),
    StructField("capital_social", StringType(), True),  
    StructField("cod_porte", StringType(), True)
])

# Lendo CSV do S3
df = (
    spark.read
    .option("header", False)
    .option("sep", ";")
    .schema(schema)
    .csv(args["SOURCE_S3_PATH"])
)

# Convertendo 'capital_social' para Float (substituindo ',' por '.')
df = df.withColumn("capital_social", col("capital_social").cast("string"))
df = df.withColumn("capital_social", col("capital_social").replace(",", ".", "all").cast(FloatType()))

# Salvando na camada Silver (Parquet)
df.write.mode("overwrite").parquet(args["TARGET_S3_PATH"])

print("Transformação de Bronze para Silver concluída!")
