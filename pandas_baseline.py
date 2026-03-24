import pandas as pd
import numpy as np
import glob
import time
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pandas(input_path):
    try:
        logger.info(f"Starting pandas baseline | input: {input_path}")
        start = time.time()

        # Read data
        files = glob.glob(f"{input_path}/*.csv")
        df = pd.concat([pd.read_csv(f) for f in files])

        print("Columns:", list(df.columns))
        print("Row count:", len(df))

        # ✅ Feature 1: Log transformation
        df["log_amount"] = np.log(df["transaction_amount"] + 1)

        # ✅ Feature 2: Z-score normalization
        mean_val = df["transaction_amount"].mean()
        std_val = df["transaction_amount"].std()
        df["z_score"] = (df["transaction_amount"] - mean_val) / std_val

        print(f"Mean: {mean_val:.4f}, Std: {std_val:.4f}")

        # ✅ Feature 3: Running total per user
        df = df.sort_values("timestamp")
        df["running_total"] = df.groupby("user_id")["transaction_amount"].cumsum()

        # ✅ Aggregation
        result = df.groupby("user_id").agg(
            total_spent=("transaction_amount", "sum"),
            avg_transaction=("transaction_amount", "mean"),
            num_transactions=("transaction_amount", "count"),
            avg_zscore=("z_score", "mean")
        )

        end = time.time()
        runtime = end - start
        print(f"Pandas Runtime: {runtime:.4f} seconds")
        logger.info(f"Pandas complete. Runtime: {runtime:.2f}s")
        return runtime

    except Exception as e:
        logger.error(f"Pandas baseline failed: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    args = parser.parse_args()
    run_pandas(args.input)