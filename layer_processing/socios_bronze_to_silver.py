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
    StructField("tipo_socio", IntegerType(), True),
    StructField("nome_socio", StringType(), True),
    StructField("documento_socio", StringType(), True),
    StructField("codigo_qualificacao_socio", StringType(), True)
])

# Lendo CSV do S3
df = (
    spark.read
    .option("header", False)
    .option("sep", ";")
    .schema(schema)
    .csv(args["SOURCE_S3_PATH"])
)


# Salvando na camada Silver (Parquet)
df.write.mode("overwrite").parquet(args["TARGET_S3_PATH"])

print("Transformação de Bronze para Silver concluída!")
