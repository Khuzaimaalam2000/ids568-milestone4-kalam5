from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log, mean, stddev, sum, avg, count
from pyspark.sql.window import Window
import argparse
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline(input_path, output_path, workers):
    try:
        spark = SparkSession.builder \
            .appName("DistributedPipeline_kalam5") \
            .master(f"local[{workers}]") \
            .config("spark.executor.memory", "2g") \
            .config("spark.speculation", "true") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"Starting pipeline | input: {input_path} | workers: {workers}")
        start = time.time()

        # Read data
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        logger.info(f"Columns: {df.columns}")
        logger.info(f"Partitions: {df.rdd.getNumPartitions()}")
        print("Columns:", df.columns)
        print("Partitions:", df.rdd.getNumPartitions())
        print("Row count:", df.count())

        # ✅ Feature 1: Log transformation
        df = df.withColumn("log_amount", log(col("transaction_amount") + 1))

        # ✅ Feature 2: Z-score normalization
        stats = df.select(
            mean("transaction_amount").alias("mean"),
            stddev("transaction_amount").alias("std")
        ).collect()[0]

        mean_val = stats["mean"]
        std_val = stats["std"]
        print(f"Mean: {mean_val:.4f}, Std: {std_val:.4f}")

        df = df.withColumn(
            "z_score",
            (col("transaction_amount") - mean_val) / std_val
        )

        # ✅ Feature 3: Window function (running total per user)
        window = Window.partitionBy("user_id").orderBy("timestamp")
        df = df.withColumn(
            "running_total",
            sum("transaction_amount").over(window)
        )

        # ✅ Aggregation
        result = df.groupBy("user_id").agg(
            sum("transaction_amount").alias("total_spent"),
            avg("transaction_amount").alias("avg_transaction"),
            count("transaction_amount").alias("num_transactions"),
            avg("z_score").alias("avg_zscore")
        )

        # ✅ Write as Parquet
        result.write.mode("overwrite").parquet(output_path)

        end = time.time()
        runtime = end - start
        print(f"Runtime: {runtime:.4f} seconds")
        logger.info(f"Pipeline complete. Runtime: {runtime:.2f}s")

        spark.stop()
        return runtime

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--mode", type=str, default="spark", help="spark or pandas")

    args = parser.parse_args()

    run_pipeline(args.input, args.output, args.workers)