"""
Nombre del estudiante: Jose Erlein Rincon Silva
Grupo: 202016911_155
Programa: Ingenieria Industrial
Código Fuente: Autoría propia (Anexo 3. Instructivo Spark Streaming y Kafka)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    count,
    sum as spark_sum
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

spark = SparkSession.builder \
    .appName("StreamingAccidentesTransito") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("fecha_hecho", StringType(), True),
    StructField("cod_depto", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("cod_muni", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("cantidad", IntegerType(), True)
])

print("Conectando con Kafka...")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "accidentes_transito")
    .option("startingOffsets", "earliest")
    .load()
)

json_df = kafka_df.selectExpr(
    "CAST(value AS STRING)"
)

datos = (
    json_df
    .select(
        from_json(
            col("value"),
            schema
        ).alias("data")
    )
)

datos = datos.select(
    col("data.fecha_hecho").alias("fecha_hecho"),
    col("data.cod_depto").alias("cod_depto"),
    col("data.departamento").alias("departamento"),
    col("data.cod_muni").alias("cod_muni"),
    col("data.municipio").alias("municipio"),
    col("data.cantidad").alias("cantidad")
)

resultado = (
    datos.groupBy("departamento")
    .agg(
        count("*").alias("eventos_recibidos"),
        spark_sum("cantidad").alias("total_lesionados")
    )
)

def mostrar_top20(df, batch_id):

    print("\n")
    print("=" * 80)
    print(f"TOP 20 DEPARTAMENTOS CON MAYOR NÚMERO DE LESIONADOS - BATCH {batch_id}")
    print("=" * 80)

    (
        df.orderBy(
            col("total_lesionados").desc()
        )
        .limit(20)
        .show(
            20,
            False
        )
    )

consulta = (
    resultado.writeStream
    .outputMode("complete")
    .foreachBatch(mostrar_top20)
    .start()
)

consulta.awaitTermination()
