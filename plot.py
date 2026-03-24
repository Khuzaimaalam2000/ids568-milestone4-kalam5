import matplotlib.pyplot as plt
import numpy as np

sizes = [1000, 100000, 1000000, 9250000]
pandas_times = [0.0418, 0.1765, 0.9653, 12.0213]
spark_times = [7.6305, 8.6300, 9.3215, 26.8575]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Plot 1 — Linear scale
ax1.plot(sizes, pandas_times, marker='o', label="Pandas (Local)", color="blue")
ax1.plot(sizes, spark_times, marker='s', label="Spark (Distributed)", color="red")
ax1.set_xlabel("Dataset Size (rows)")
ax1.set_ylabel("Runtime (seconds)")
ax1.set_title("Pandas vs Spark — Linear Scale")
ax1.legend()
ax1.grid(True)

# Plot 2 — Log scale (clearer for large range)
ax2.plot(sizes, pandas_times, marker='o', label="Pandas (Local)", color="blue")
ax2.plot(sizes, spark_times, marker='s', label="Spark (Distributed)", color="red")
ax2.set_xscale("log")
ax2.set_xlabel("Dataset Size (rows, log scale)")
ax2.set_ylabel("Runtime (seconds)")
ax2.set_title("Pandas vs Spark — Log Scale")
ax2.legend()
ax2.grid(True)

plt.tight_layout()
plt.savefig("performance.png", dpi=150)
plt.show()
print("Graph saved to performance.png")