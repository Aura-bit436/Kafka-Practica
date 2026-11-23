from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("PromedioTemperaturaVentana") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

esquema = StructType() \
    .add("sensor_id", StringType()) \
    .add("temperatura", DoubleType()) \
    .add("timestamp", StringType())

datos = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:29092") \
    .option("subscribe", "alertas-criticas") \
    .option("startingOffsets", "latest") \
    .load()

mensajes = datos.select(
    from_json(col("value").cast("string"), esquema).alias("datos")
).select("datos.*")

mensajes = mensajes.withColumn(
    "timestamp", to_timestamp(col("timestamp"))
)

promedios = mensajes \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("sensor_id")
    ) \
    .agg(avg("temperatura").alias("temperatura_promedio"))

consulta = promedios.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .start()

consulta.awaitTermination()
