from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .getOrCreate()

# Define the schema for weather data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("main", StructType([
        StructField("temp", FloatType(), True),
        StructField("humidity", FloatType(), True)
    ]))
])

# Read data from Kafka
weather_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .load()

# Deserialize the JSON data and select required fields
weather_data = weather_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.name", "data.main.temp", "data.main.humidity")

# Process the data (e.g., convert temperature from Kelvin to Celsius)
processed_data = weather_data.withColumn("temperature_celsius", col("temp") - 273.15)

# Write processed data to the console (or another Kafka topic)
query = processed_data \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
