from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

# Création de la session Spark
spark = SparkSession.builder \
    .appName("WeatherStreamTransform") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des données météo venant de weather_stream
schema = StructType([
    StructField("temperature", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("winddirection", DoubleType(), True),
    StructField("time", StringType(), True)
])

# Lecture du topic Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Conversion de la valeur Kafka (bytes) en JSON
df_parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.temperature").alias("temperature"),
        col("data.windspeed").alias("windspeed"),
        col("data.time").alias("event_time")
    )

# Transformation : alertes de vent
df_transformed = df_parsed.withColumn(
    "wind_alert_level",
    when(col("windspeed") < 10, "level_0")
    .when((col("windspeed") >= 10) & (col("windspeed") <= 20), "level_1")
    .otherwise("level_2")
).withColumn(
    "heat_alert_level",
    when(col("temperature") < 25, "level_0")
    .when((col("temperature") >= 25) & (col("temperature") <= 35), "level_1")
    .otherwise("level_2")
).withColumn(
    "event_time", current_timestamp()  # timestamp actuel
)

# Production dans le topic weather_transformed
query = df_transformed.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_transformed") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/weather") \
    .start()

query.awaitTermination()