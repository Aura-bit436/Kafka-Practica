from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("ConteoAlertasPorSensor") \
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

conteo = mensajes.groupBy("sensor_id").count()

consulta = conteo.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

consulta.awaitTermination()
