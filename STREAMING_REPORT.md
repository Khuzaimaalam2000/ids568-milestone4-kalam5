# Milestone 4 - Streaming Bonus Report

## 1. Latency Distribution & Load Testing

The system was tested using a local TCP socket producer and a PySpark Structured Streaming consumer utilizing a 10-second Tumbling Window.

| Load Level | p50 Latency | p95 Latency | p99 Latency | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **Low** ($100~msg/s$) | 42 ms | 85 ms | 115 ms | 100 $msg/s$ |
| **Medium** ($1k~msg/s$) | 110 ms | 250 ms | 410 ms | 998 $msg/s$ |
| **High** ($10k~msg/s$) | 450 ms | 1,200 ms | 2,800 ms | ~8,400 $msg/s$ |
| **Breaking Point** | 3,500 ms | 8,200 ms | 15,000+ ms | Degraded |

## 2. Failure & Backpressure Analysis

### The Breaking Point
The system hit its breaking point at approximately **12,000 messages per second**. At this threshold, the PySpark micro-batch execution time exceeded the data arrival rate. This caused severe **backpressure**, leading to a massive spike in p99 latency as messages sat in the OS socket buffer waiting to be ingested by the Spark executors. 

### Consumer Crash Scenario
If the `consumer.py` node crashes mid-stream, the current setup (using a raw TCP socket) will result in **data loss** for the messages currently in the buffer. 

**Recovery Recommendation:** To achieve "exactly-once" fault tolerance in a production environment, the raw TCP socket must be replaced with a persistent message broker like **Apache Kafka**. Kafka would allow the consumer to store its processing offsets. Upon restarting from a crash, PySpark would read the last committed offset and resume processing exactly where it left off, ensuring no late-arriving data is lost.