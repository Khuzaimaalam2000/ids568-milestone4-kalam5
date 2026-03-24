import pandas as pd
import time
import numpy as np
import glob

start = time.time()

# Read all CSV files
files = glob.glob("test_data/*.csv")
df = pd.concat([pd.read_csv(f) for f in files])

df["log_amount"] = np.log(df["transaction_amount"] + 1)

mean = df["transaction_amount"].mean()
std = df["transaction_amount"].std()

df["z_score"] = (df["transaction_amount"] - mean) / std

result = df.groupby("user_id").agg({
    "transaction_amount": ["sum", "mean"]
})

end = time.time()

print("Pandas Runtime:", end - start)