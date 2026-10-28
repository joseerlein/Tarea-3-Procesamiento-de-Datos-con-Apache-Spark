"""
Nombre del estudiante: Jose Erlein Rincon Silva
Grupo: 202016911_155
Programa: Ingenieria Industrial
Código Fuente: Autoría propia (Anexo 3. Instructivo Spark Streaming y Kafka)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
spark = SparkSession.builder \
    .appName("BatchAccidentesTransito") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("\n====================================")
print("CARGA DEL DATASET")
print("====================================")
df = spark.read.csv(
    "LESIONES_ACCIDENTES_DE_TRÁNSITO_20261028.csv",
    header=True,
    inferSchema=True
)
print("\nESQUEMA DEL DATASET")
df.printSchema()
print("\nTOTAL DE REGISTROS:")
print(df.count())
print("\n====================================")
print("LIMPIEZA DE DATOS")
print("====================================")
df_limpio = df.dropna()
df_limpio = df_limpio.withColumn(
    "CANTIDAD",
    col("CANTIDAD").cast("int")
)
print("\n====================================")
print("TOP 20 DEPARTAMENTOS")
print("====================================")
departamentos = (
    df_limpio.groupBy("DEPARTAMENTO")
    .agg(
        spark_sum("CANTIDAD")
        .alias("TOTAL_LESIONADOS")
    )
    .orderBy(
        col("TOTAL_LESIONADOS").desc()
    )
)
departamentos.show(20, False)
print("\n====================================")
print("TOP 20 MUNICIPIOS")
print("====================================")
municipios = (
    df_limpio.groupBy("MUNICIPIO")
    .agg(
        spark_sum("CANTIDAD")
        .alias("TOTAL_LESIONADOS")
    )
    .orderBy(
        col("TOTAL_LESIONADOS").desc()
    )
)
municipios.show(20, False)
print("\n====================================")
print("GENERANDO BASE DEPURADA")
print("====================================")
(
    df_limpio
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv("BDdepurada")
)
print("\nBase depurada creada correctamente.")
print("Ruta: BDdepurada")
spark.stop()
