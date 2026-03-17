from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg
import argparse
import time

def run_pipeline(input_path, output_path):
    spark = SparkSession.builder \
        .appName("DistributedPipeline_kalam5") \
        .getOrCreate()

    start = time.time()

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("Columns in CSV:", df.columns)
    print("Partitions:", df.rdd.getNumPartitions())

    # Feature Engineering — adjust to your CSV columns
    df = df.withColumn("total_value", col("transaction_amount") * 1)  # use 1 if no quantity

    result = df.groupBy("user_id").agg(
        sum("total_value").alias("total_spent"),
        avg("transaction_amount").alias("avg_transaction")
    )

    result.write.mode("overwrite").csv(output_path)

    end = time.time()
    print(f"Runtime: {end - start} seconds")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)

    args = parser.parse_args()

    run_pipeline(args.input, args.output)