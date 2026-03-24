# Distributed Pipeline Report - kalam5

## 1. Dataset

Dataset size: 1,000,000 rows  
Columns: user_id, transaction_amount, category_id, timestamp  

---

## 2. Performance Comparison (Same Dataset)

| Framework | Runtime |
|----------|--------|
| Pandas (Local) | 0.73 seconds |
| Spark (Distributed) | 9.98 seconds |

Partitions (Spark): 8  

---

## 3. Analysis

- Pandas significantly outperformed Spark on a 1M row dataset.
- This is due to Spark’s overhead (task scheduling, JVM startup, distributed coordination).
- Spark introduces latency that is not justified for moderately sized datasets.

### Key Insight:
Distributed systems are **not always faster** — they become beneficial only at larger scales.

---

## 4. Crossover Point

- Based on experiments:
  - Pandas performs better for datasets up to ~1M rows.
  - Spark becomes advantageous at larger scales (e.g., 10M+ rows).
  
This demonstrates the importance of choosing the right tool based on data size.

---

## 5. Feature Engineering

The pipeline includes non-trivial transformations:

- Log transformation:
  log(transaction_amount + 1)

- Z-score normalization:
  (value - mean) / standard deviation

These transformations improve data normalization and analytical usability.

---

## 6. Storage Optimization

- Initial data is stored as CSV.
- Pipeline converts output to **Parquet format**, which:
  - Improves read/write performance
  - Reduces storage size
  - Is optimized for distributed processing

---

## 7. Reliability Considerations

Spark provides built-in reliability features:

- Fault tolerance through task recomputation
- Data partitioning enables partial recovery
- Spill-to-disk when memory is insufficient

---

## 8. Cost Considerations

- Distributed systems require more resources (CPU, memory, cluster nodes)
- Higher cost compared to local execution
- Justified only for large-scale data processing

---

## 9. Conclusion

- Pandas is optimal for small to medium datasets
- Spark is necessary for large-scale distributed processing
- Proper tool selection is critical for performance and cost efficiency