from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, log, mean, stddev
import argparse
import time

def run_pipeline(input_path, output_path):
    spark = SparkSession.builder \
        .appName("DistributedPipeline_kalam5") \
        .getOrCreate()

    start = time.time()

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("Columns:", df.columns)
    print("Partitions:", df.rdd.getNumPartitions())

    # ✅ REAL FEATURE ENGINEERING
    df = df.withColumn("log_amount", log(col("transaction_amount") + 1))

    stats = df.select(
        mean("transaction_amount").alias("mean"),
        stddev("transaction_amount").alias("std")
    ).collect()[0]

    mean_val = stats["mean"]
    std_val = stats["std"]

    df = df.withColumn(
        "z_score",
        (col("transaction_amount") - mean_val) / std_val
    )

    # Aggregation
    result = df.groupBy("user_id").agg(
        sum("transaction_amount").alias("total_spent"),
        avg("transaction_amount").alias("avg_transaction")
    )

    result.write.mode("overwrite").parquet(output_path)

    end = time.time()
    print(f"Runtime: {end - start} seconds")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)

    args = parser.parse_args()

    run_pipeline(args.input, args.output)