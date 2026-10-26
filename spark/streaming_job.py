import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    from_json,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban_sensors")
WINDOW_DURATION = os.getenv("SPARK_WINDOW_DURATION", "1 minute")
CHECKPOINT_LOCATION = os.getenv(
    "SPARK_CHECKPOINT_LOCATION",
    "/tmp/spark-checkpoint",
)


schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("humidity", DoubleType(), False),
    StructField("air_quality_index", IntegerType(), False),
    StructField("timestamp", TimestampType(), False),
])


spark = (
    SparkSession.builder
    .appName("UrbanSensorsStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Lee eventos JSON desde Kafka.
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Convierte el valor JSON y prepara los campos tipados.
events = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema,
        ).alias("event")
    )
    .select("event.*")
    .withWatermark("timestamp", "10 seconds")
)

# Calcula los promedios por sensor en ventanas de un minuto.
aggregated = (
    events
    .groupBy(
        window(col("timestamp"), WINDOW_DURATION),
        col("sensor_id"),
    )
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("air_quality_index").alias("avg_air_quality_index"),
    )
)

# Imprime en consola cada resultado agregado.
query = (
    aggregated.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start()
)

query.awaitTermination()