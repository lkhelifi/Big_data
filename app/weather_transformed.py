from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, min, max, when
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

# Création de la session Spark
spark = SparkSession.builder \
    .appName("WeatherAggregates") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des données transformées
schema = StructType([
    StructField("temperature", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("wind_alert_level", StringType(), True),
    StructField("heat_alert_level", StringType(), True)
])

# Lecture du topic Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_transformed") \
    .option("startingOffsets", "latest") \
    .load()

# Conversion de la valeur Kafka (bytes) en JSON
df_parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Agrégats sur des fenêtres glissantes de 5 minutes (peut être 1 minute)
aggregates = df_parsed.withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute")
    ).agg(
        count(when(col("wind_alert_level") == "level_1", True)).alias("wind_level_1_count"),
        count(when(col("wind_alert_level") == "level_2", True)).alias("wind_level_2_count"),
        count(when(col("heat_alert_level") == "level_1", True)).alias("heat_level_1_count"),
        count(when(col("heat_alert_level") == "level_2", True)).alias("heat_level_2_count"),
        avg("temperature").alias("avg_temp"),
        min("temperature").alias("min_temp"),
        max("temperature").alias("max_temp")
    )

# Écriture des résultats dans la console pour debug / test
query = aggregates.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()