from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

def start_consumer():
    # Initialize Spark for Structured Streaming
    spark = SparkSession.builder \
        .appName("IDS568_StreamingBonus") \
        .master("local[*]") \
        .getOrCreate()
    
    # Hide massive info logs so we can see the data
    spark.sparkContext.setLogLevel("WARN")

    # Match the schema from the producer
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", DoubleType(), True)
    ])

    print("Connecting to Producer on localhost:9999...")

    # Read the raw stream from the TCP socket
    raw_stream = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    # Parse the JSON and calculate exact latency
    parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")
    
    enriched_stream = parsed_stream \
        .withColumn("event_time", expr("cast(timestamp as timestamp)")) \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("latency_ms", (expr("cast(processing_time as double)") - col("timestamp")) * 1000)

    # Apply Stateful Logic: 10-Second Tumbling Window with a 5-second watermark for late data
    windowed_stream = enriched_stream \
        .withWatermark("event_time", "5 seconds") \
        .groupBy(window(col("event_time"), "10 seconds")) \
        .avg("amount", "latency_ms")

    # Output the live calculations to the console
    print("Starting stream processing...")
    query = windowed_stream.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_consumer()