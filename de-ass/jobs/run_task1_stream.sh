#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export HADOOP_HOME="${HADOOP_HOME:-/home/hduser/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export SPARK_HOME="${SPARK_HOME:-/home/hduser/spark}"
export SPARK_KAFKA_PKG="${SPARK_KAFKA_PKG:-org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1}"
echo "[Task1] Submitting Kafka->HDFS raw stream (Parquet + partitions)"
"$SPARK_HOME/bin/spark-submit" \
  --packages "$SPARK_KAFKA_PKG" \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  classes/kafka_to_hdfs_raw.py
