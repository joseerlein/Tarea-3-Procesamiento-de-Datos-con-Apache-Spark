"""
Nombre del estudiante: Jose Erlein Rincon Silva
Grupo: 202016911_155
Programa: Ingenieria Industrial
Código Fuente: Autoría propia (Anexo 3. Instructivo Spark Streaming y Kafka)
"""
import csv
import json
import glob
import time
from kafka import KafkaProducer
TOPIC = "accidentes_transito"
archivo_limpio = glob.glob(
    "BDdepurada/part*.csv"
)[0]
print("\nArchivo encontrado:")
print(archivo_limpio)
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x:
    json.dumps(x).encode("utf-8")
)
with open(
    archivo_limpio,
    encoding="utf-8-sig"
) as archivo:
    lector = csv.DictReader(archivo)
    for fila in lector:
        evento = {
            "fecha_hecho": fila["FECHA HECHO"],
            "cod_depto": fila["COD_DEPTO"],
            "departamento": fila["DEPARTAMENTO"],
            "cod_muni": fila["COD_MUNI"],
            "municipio": fila["MUNICIPIO"],
            "cantidad": int(fila["CANTIDAD"])
        }
        producer.send(
            TOPIC,
            value=evento
        )
        print(
            f"Enviado -> "
            f"{evento['departamento']} | "
            f"{evento['municipio']} | "
            f"{evento['cantidad']}"
        )
        time.sleep(0.2)
producer.flush()
producer.close()
print("\nTransmisión finalizada.")
