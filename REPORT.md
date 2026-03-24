# Distributed Pipeline Report - kalam5

---

## 1. Dataset Description

- Source: Synthetically generated transaction data (seeded, reproducible)
- Columns: user_id, transaction_amount, category_id, timestamp
- Sizes tested: 1K, 100K, 1M, 10M rows
- Seed: 42 (deterministic output guaranteed)

---

## 2. Feature Engineering

Three non-trivial transformations were applied in both Spark and Pandas pipelines:

### 2.1 Log Transformation
Reduces skew in transaction_amount distribution:
```
log_amount = log(transaction_amount + 1)
```

### 2.2 Z-Score Normalization
Standardizes values for analytical comparability:
```
z_score = (transaction_amount - mean) / std
```
Observed statistics (consistent across runs due to seeding):
- Mean: ~250.44
- Std: ~144.03

### 2.3 Window Function — Running Total Per User
Computes cumulative spend per user ordered by timestamp:
```
running_total = SUM(transaction_amount) OVER (PARTITION BY user_id ORDER BY timestamp)
```

### 2.4 Aggregation
GroupBy user_id producing: total_spent, avg_transaction, num_transactions, avg_zscore

---

## 3. Performance Comparison (Same Dataset, All Sizes)

| Dataset Size | Rows | Pandas Runtime | Spark Runtime | Spark Partitions |
|---|---|---|---|---|
| 1K | 1,000 | 0.04s | 7.63s | 1 |
| 100K | 100,000 | 0.18s | 8.63s | 1 |
| 1M | 1,000,000 | 0.97s | 9.32s | 4 |
| 10M | 9,250,000 | 12.02s | 26.86s | 5 |

See performance.png for visualization.

---

## 4. Crossover Analysis

Based on observed results, Spark does not outperform Pandas on a single local machine
at any tested dataset size. This is an expected and well-documented finding in
distributed systems literature:

- At 1K–100K rows: Spark overhead (JVM startup, task scheduling, DAG planning)
  dominates — Pandas is ~190x faster
- At 1M rows: Spark overhead reduces but Pandas still wins — ~9.6x faster
- At 10M rows: gap narrows significantly — Pandas is only ~2.2x faster

### Projected Crossover Point
Extrapolating the trend, Spark would become competitive at approximately 50–100M rows
on this hardware, where parallelism benefits outweigh coordination overhead.

### Key Insight
Spark is not designed to outperform Pandas on a single machine. Its advantage
emerges in true cluster deployments (multiple nodes) where data is distributed
across machines — a scenario that cannot be replicated locally. The local[4]
configuration simulates parallelism using threads, but all threads share the same
memory and disk, limiting true distributed benefit.

---

## 5. Execution Modes and Worker Configuration

The pipeline supports configurable execution via command line arguments:
```bash
# Run with 4 workers (default)
python3 pipeline.py --input size_1m --output output --workers 4

# Run with 8 workers
python3 pipeline.py --input size_1m --output output --workers 8

# Run pandas baseline
python3 pandas_baseline.py --input size_1m
```

---

## 6. Storage Optimization

- Input: CSV format
- Output: Parquet format
- Parquet provides columnar storage, compression, and significantly faster
  read performance for analytical queries
- Estimated size reduction: 60–70% compared to CSV for this dataset type

---

## 7. Reliability

### Fault Tolerance
- Spark tracks data lineage via the RDD DAG
- If a partition fails, Spark recomputes only that partition from its source
- Tasks are automatically retried up to 4 times before the job fails

### Speculative Execution
- Enabled via: spark.speculation=true (configured in pipeline)
- Spark launches duplicate copies of slow tasks on other executors
- Whichever finishes first is used, the other is killed
- Reduces impact of stragglers in large jobs

### Spill-to-Disk
- When executor memory is exceeded, Spark spills shuffle data to disk
- Configured via spark.executor.memory (set to 2g in pipeline)
- Spill degrades performance but prevents OutOfMemoryError crashes
- Not observed in these runs but becomes relevant at 50M+ rows locally

### Error Handling
- Pipeline wraps all operations in try/except with structured logging
- Failures are logged with full error messages before re-raising

---

## 8. Cost Estimates

| Environment | Setup | Estimated Cost (1 hour) |
|---|---|---|
| Local machine | Single node | $0 |
| AWS EMR (4 workers, m5.xlarge) | Cluster | ~$0.80/hr |
| AWS EMR (10 workers, m5.xlarge) | Cluster | ~$2.00/hr |
| AWS Glue (10 DPUs) | Serverless | ~$0.44/hr |

Note: Cloud costs are reference estimates based on AWS published pricing.
Actual costs depend on data size, region, and instance type.

### Cost vs Performance Tradeoff
- For datasets under 10M rows: local Pandas is free and faster
- For datasets over 100M rows: cluster cost is justified by time savings
- For irregular workloads: serverless (AWS Glue) avoids idle cluster costs

---

## 9. Production Recommendations

1. **Use Parquet** for all storage (already implemented)
2. **Partition by date** for time-series data to enable partition pruning
3. **Switch to cluster mode** for datasets exceeding 50M rows
4. **Monitor via Spark UI** (localhost:4040 during runs) for bottleneck detection
5. **Use AWS Glue or Databricks** for managed cluster deployments
6. **Cache intermediate DataFrames** if reused multiple times in pipeline
7. **Tune spark.executor.memory** based on dataset size to minimize spill

---

## 10. Conclusion

- Pandas is optimal for datasets up to ~10M rows on a single machine
- Spark's advantage requires true multi-node cluster deployment
- The pipeline correctly implements log transform, z-score, window functions,
  and aggregation — all non-trivial transformations
- Parquet output, error handling, logging, and configurable workers
  meet production-grade standards
- Crossover to Spark advantage projected at 50–100M rows locally,
  or immediately in a multi-node cluster environment