import matplotlib.pyplot as plt

sizes = [1000, 1000000]
spark_times = [6.12, 16.7]
pandas_times = [2, 30]  # replace with your actual pandas runtime

plt.plot(sizes, spark_times, label="Spark")
plt.plot(sizes, pandas_times, label="Pandas")

plt.xlabel("Dataset Size")
plt.ylabel("Runtime (seconds)")
plt.legend()

plt.savefig("performance.png")