import pandas as pd
import numpy as np
import argparse
import os

def generate_data(rows, seed, output_path):
    # Set the seed for reproducibility [cite: 27]
    np.random.seed(seed)
    
    # Create the directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    
    # Generate data in chunks to avoid memory crashes
    chunk_size = 1_000_000
    num_chunks = (rows // chunk_size) + (1 if rows % chunk_size != 0 else 0)
    
    print(f"Generating {rows} rows in {num_chunks} chunks...")
    
    for i in range(num_chunks):
        current_rows = min(chunk_size, rows - (i * chunk_size))
        data = {
            'user_id': np.random.randint(1000, 9999, size=current_rows),
            'transaction_amount': np.random.uniform(1.0, 500.0, size=current_rows),
            'category_id': np.random.randint(1, 10, size=current_rows),
            'timestamp': np.random.randint(1609459200, 1640995200, size=current_rows)
        }
        df = pd.DataFrame(data)
        # Save as CSV chunks
        df.to_csv(f"{output_path}/data_part_{i}.csv", index=False)
    
    print(f"Success: Data saved to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10000000) # Default to 10M [cite: 25]
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=str, default="raw_data")
    args = parser.parse_args()
    
    generate_data(args.rows, args.seed, args.output)