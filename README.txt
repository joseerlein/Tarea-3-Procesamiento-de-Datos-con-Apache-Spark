Procesamiento de Datos con Apache Spark y Kafka

Autor
José Erlein Rincón Silva

Grupo
202016911_155

Programa
Ingeniería Industrial

Descripción del proyecto

Este proyecto fue desarrollado para la Fase 3 del curso Big Data.

Se implementó un proceso de análisis de datos utilizando Apache Spark para el procesamiento batch y Apache Kafka junto con Spark Streaming para el procesamiento de datos en tiempo real.

Conjunto de datos

LESIONES_ACCIDENTES_DE_TRÁNSITO_20261028.csv
Fuente: Datos Abiertos Colombia.

Problema
Identificar los departamentos y municipios con mayor número de personas lesionadas en accidentes de tránsito en Colombia mediante el análisis de grandes volúmenes de datos.

Tecnologías utilizadas

Hadoop 3.3.6
Apache Spark 3.5.8
Apache Kafka 3.9.2
ZooKeeper
Python 3

Procesamiento Batch
El archivo batch_accidentes.py realiza:

Carga del conjunto de datos.
Limpieza de registros.
Conversión y validación de tipos de datos.
Agrupación por departamento.
Agrupación por municipio.
Análisis exploratorio de datos (EDA).
Generación de una base depurada denominada BDdepurada.
Procesamiento en Tiempo Real
Productor Kafka
El archivo kafka_producer_accidentes.py lee la información desde la base depurada (BDdepurada) y envía los registros al tópico de Kafka denominado:

accidentes_transito
Consumidor Spark Streaming
El archivo spark_streaming_accidentes.py consume la información almacenada en Kafka y genera estadísticas en tiempo real por departamento.

Flujo de procesamiento

LESIONES ACCIDENTES DE TRÁNSITO 20261028.csv
                    │
                    ▼
           batch_accidentes.py
                    │
                    ▼
                BDdepurada
                    │
                    ▼
      kafka_producer_accidentes.py
                    │
                    ▼
      Topic: accidentes_transito
                    │
                    ▼
     spark_streaming_accidentes.py
                    │
                    ▼
        Resultados en tiempo real

Ejecución

Requisitos previos
Antes de ejecutar los scripts es necesario iniciar Hadoop, ZooKeeper y Kafka.

1. Iniciar Hadoop

2. Iniciar ZooKeeper "sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties"

3. Iniciar Kafka "sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties"

4. Verificar el tópico "/opt/Kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"

Debe aparecer:

accidentes_transito

5. Procesamiento Batch Genera la base depurada utilizada posteriormente por Kafka.

"spark-submit batch_accidentes.py"

Resultado esperado: BDdepurada

6. Productor Kafka

Lee la base depurada y envía los registros al tópico de Kafka. "python3 kafka_producer_accidentes.py"

7. Procesamiento en Tiempo Real (Spark Streaming): Consume la información enviada por Kafka y genera estadísticas por departamento.

"spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_accidentes.py"


Resultados obtenidos

El sistema permite:

- Identificar los departamentos con mayor número de lesionados.
- Identificar los municipios con mayor número de lesionados.
- Generar una base de datos depurada para posteriores análisis.
- Procesar información en tiempo real mediante Apache Kafka y Spark Streaming.
- Mostrar estadísticas actualizadas continuamente sobre:
	
	Eventos recibidos.
	Total de lesionados.
	Top 20 departamentos con mayor número de lesionados.


Archivos incluidos

- batch_accidentes.py
- kafka_producer_accidentes.py
- spark_streaming_accidentes.py
- README.txt

Conclusión

La implementación permitió aplicar técnicas de procesamiento batch y procesamiento en tiempo real utilizando tecnologías Big Data. Apache Spark facilitó la limpieza y análisis del conjunto de datos, mientras que Apache Kafka y Spark Streaming permitieron procesar eventos continuamente para generar información útil en tiempo real sobre la accidentalidad vial en Colombia.