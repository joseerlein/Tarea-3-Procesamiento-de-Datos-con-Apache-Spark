"""
Nombre del estudiante: Jose Erlein Rincon Silva
Grupo: 202016911_155
Programa: Ingenieria Industrial
Código Fuente: Autoría propia (Anexo 3. Instructivo Spark Streaming y Kafka)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("BatchAccidentesTransito") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n====================================")
print("CARGA DEL DATASET")
print("====================================\n")

df = spark.read.csv(
    "LESIONES_ACCIDENTES_DE_TRÁNSITO_20261028.csv",
    header=True,
    inferSchema=True
)

print("Estructura del dataset:")
df.printSchema()

print("\nTotal de registros:")
print(df.count())

print("\n====================================")
print("LIMPIEZA Y TRANSFORMACIÓN")
print("====================================\n")

# Eliminación de valores nulos
df_limpio = df.dropna()

# Conversión de tipos
df_limpio = df_limpio.withColumn(
    "CANTIDAD",
    col("CANTIDAD").cast("int")
)

print("Registros después de la limpieza:")
print(df_limpio.count())

print("\n====================================")
print("GENERANDO BASE DEPURADA")
print("====================================\n")

(
    df_limpio
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv("BDdepurada")
)

print("BDdepurada creada correctamente.")

spark.stop()
