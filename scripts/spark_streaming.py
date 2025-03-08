from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.streaming import DataStreamWriter

KAFKA_BROKER = "localhost:9092"
TOPIC = "stock_prices"

# Create Spark Session
spark = SparkSession.builder \
    .appName("StockMarketStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
    .getOrCreate()

# Define Schema
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", IntegerType(), True)
])

# Read Stream from Kafka
stock_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka Data (Binary) to Readable JSON
stock_data = stock_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Print to Console
query = stock_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
