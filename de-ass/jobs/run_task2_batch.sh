#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export HADOOP_HOME="${HADOOP_HOME:-/home/hduser/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export SPARK_HOME="${SPARK_HOME:-/home/hduser/spark}"
export SPARK_MASTER="${SPARK_MASTER:-local[2]}"

DATE_ARG="${1:-}"
PASSTHRU=()

if [[ -n "${DATE_ARG}" && "${DATE_ARG}" != --* ]]; then
  PASSTHRU+=(--date "$DATE_ARG")
  shift || true
fi

PASSTHRU+=("$@")

echo "[Task2] Submitting batch pipeline"
echo "        spark=${SPARK_HOME}, master=${SPARK_MASTER}"
echo "        args: ${PASSTHRU[*]:-<none>}"

exec "$SPARK_HOME/bin/spark-submit" \
  --master "${SPARK_MASTER}" \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  classes/task2_pipeline.py \
  "${PASSTHRU[@]}"
