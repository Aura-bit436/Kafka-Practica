# Proyecto de Apache Kafka: Procesamiento de datos en tiempo real

**Asignatura:** Manejo Masivo de Datos  
**Estudiante:** Aurora García Zarazúa

## Descripción

Este proyecto implementa un sistema de procesamiento de datos en tiempo real utilizando Apache Kafka, MongoDB y Apache Spark Structured Streaming.

Se desarrollan tres actividades para recibir, procesar, filtrar, almacenar y analizar datos provenientes de sensores IoT.

## Tecnologías utilizadas

- Python
- Apache Kafka
- MongoDB
- Apache Spark 3.5.0
- Docker y Docker Compose
- PySpark
- kafka-python-ng
- confluent-kafka
- PyMongo

## Actividad 1. Enriquecimiento de datos IoT

Se implementa un productor que genera datos aleatorios de temperatura de cuatro sensores y los envía al tópico `telemetria-iot`.

El consumidor recibe los mensajes, agrega información de ubicación y responsable de cada sensor, y almacena los documentos enriquecidos en MongoDB.

**Archivos:**
- `productor_iot.py`
- `consumidor_iot.py`

**Base de datos:** `invernadero`  
**Colección:** `telemetria_enriquecida`

## Actividad 2. Pipeline de alertas con dos tópicos

Se desarrolla un flujo de procesamiento que recibe datos de sensores en el tópico `telemetria-bruta`.

Un filtro analiza las temperaturas y solamente envía aquellas mayores a 30 °C al tópico `alertas-criticas`. Posteriormente, un consumidor almacena las alertas en MongoDB.

**Archivos:**
- `productor_sensor.py`
- `filtro_alertas.py`
- `consumidor_mongodb.py`

**Base de datos:** `invernadero`  
**Colección:** `alertas_temperatura`

## Actividad 3. Spark Structured Streaming

Se utiliza Apache Spark Structured Streaming para procesar los datos del tópico `alertas-criticas` en tiempo real.

Se desarrollan dos programas:

**1. Conteo de alertas por sensor**

Agrupa las alertas por identificador de sensor y calcula la cantidad de mensajes recibidos utilizando `groupBy()` y `count()`.

**Archivo:** `spark_conteo.py`

**2. Promedio de temperatura por ventanas**

Calcula el promedio de temperatura por sensor mediante ventanas de tiempo de 30 segundos, con un desplazamiento de 10 segundos. Utiliza marcas de agua (*watermark*) para gestionar los datos por tiempo de evento.

**Archivo:** `spark_ventana.py`

## Configuración de Docker

El archivo `docker-compose.yml` configura los servicios de Apache Kafka y MongoDB, incluyendo sus puertos, listeners y volúmenes persistentes.

Spark se ejecuta en el contenedor `spark-practica`.

## Ejecución

Para iniciar Kafka y MongoDB:

```bash
docker compose up -d
```

Los programas de Python se ejecutan desde la terminal con:

```bash
py nombre_del_archivo.py
```

Los programas de Spark se ejecutan mediante `spark-submit` utilizando el conector de Kafka para Spark 3.5.0.

## Resultados

Se obtuvieron los siguientes resultados:

- Recepción y enriquecimiento de datos IoT.
- Almacenamiento de telemetría en MongoDB.
- Filtrado de temperaturas superiores a 30 °C.
- Almacenamiento de alertas críticas en MongoDB.
- Conteo de alertas por sensor mediante Spark.
- Cálculo de promedios de temperatura mediante ventanas de tiempo.

## Autor

Aurora García Zarazúa