#!/usr/bin/env bash
set -euo pipefail

SPARK_HOME=${SPARK_HOME:-"$HOME/spark"}
APP="$HOME/de-ass/classes/task5_streaming.py"

# Ensure HDFS dirs exist (safe to run repeatedly)
hdfs dfs -mkdir -p /user/student/covid19/stream_silver || true
hdfs dfs -mkdir -p /user/student/covid19/stream_agg || true
hdfs dfs -mkdir -p /user/student/checkpoints/task5 || true

SPARK_KAFKA_PKG=${SPARK_KAFKA_PKG:-org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
IN_TOPIC=${IN_TOPIC:-covid19_stream}
OUT_TOPIC=${OUT_TOPIC:-covid19_region_agg}
SILVER=${SILVER:-hdfs://localhost:9000/user/student/covid19/stream_silver}
AGG_PATH=${AGG_PATH:-hdfs://localhost:9000/user/student/covid19/stream_agg}
CKPT=${CKPT:-hdfs://localhost:9000/user/student/checkpoints/task5}
DASH_DIR=${DASH_DIR:-$HOME/de-ass/data/dashboard}
BASELINE=${BASELINE:-hdfs://localhost:9000/user/student/covid19/processed}

# Positional arg wins; else env STARTING; else "latest"
STARTING=${1:-${STARTING:-latest}}

"$SPARK_HOME/bin/spark-submit" \
  --master local[2] \
  --packages "$SPARK_KAFKA_PKG" \
  "$APP" \
  --kafka "$KAFKA_BOOTSTRAP" \
  --in_topic "$IN_TOPIC" \
  --out_topic "$OUT_TOPIC" \
  --startingOffsets "$STARTING" \
  --silver "$SILVER" \
  --agg_path "$AGG_PATH" \
  --ckpt "$CKPT" \
  --dash_dir "$DASH_DIR" \
  --baseline_path "$BASELINE"
