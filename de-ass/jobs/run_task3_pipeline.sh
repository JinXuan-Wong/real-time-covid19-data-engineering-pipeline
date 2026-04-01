#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# --- Java 17 (same as before) ---
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"

# --- Prefer your active venv's Python for PySpark, if any ---
if [[ -n "${VIRTUAL_ENV:-}" ]]; then
  export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
  export PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python"
fi

# --- Spark home (default to student path if unset) ---
export SPARK_HOME="${SPARK_HOME:-/home/hduser/spark}"

# --- Mongo connection (do NOT overwrite if already exported outside) ---
export MONGODB_URI="${MONGODB_URI:-mongodb://localhost:27017/}"
export MONGO_DB="${MONGO_DB:-covid}"
export MONGO_COLL="${MONGO_COLL:-daily_country}"

# --- Output directory & plotting flags used by the queries runner ---
export TASK3_EXPORT_DIR="${TASK3_EXPORT_DIR:-$HOME/de-ass/docs}"
export TASK3_SAVE_PLOTS="${TASK3_SAVE_PLOTS:-1}"

# ---------- Pick Task 2 source (parquet) ----------
LOCAL_PARQUET_DIR="$HOME/de-ass/data/dashboard/stream_latest.parquet"   # pass the DIR, not the *.parquet glob
LOCAL_PARQUET_GLOB="$LOCAL_PARQUET_DIR/*.parquet"

HDFS_PARQUET_DIR="/user/student/covid19/processed"

# If user already provided TASK2_PARQUET, trust it.
if [[ -n "${TASK2_PARQUET:-}" ]]; then
  PICKED_SOURCE="env:$TASK2_PARQUET"
  # Decide defaultFS based on scheme
  if [[ "$TASK2_PARQUET" == hdfs://* || "$TASK2_PARQUET" == /* ]]; then
    export SPARK_DEFAULT_FS="hdfs://localhost:9000"
  else
    export SPARK_DEFAULT_FS="file:///"
  fi
else
  if compgen -G "$LOCAL_PARQUET_GLOB" > /dev/null; then
    export SPARK_DEFAULT_FS="file:///"
    export TASK2_PARQUET="$LOCAL_PARQUET_DIR"
    PICKED_SOURCE="local-parquet:$TASK2_PARQUET"
  elif hdfs dfs -test -e "$HDFS_PARQUET_DIR"; then
    export SPARK_DEFAULT_FS="hdfs://localhost:9000"
    export TASK2_PARQUET="hdfs://localhost:9000$HDFS_PARQUET_DIR"
    PICKED_SOURCE="hdfs-parquet:$TASK2_PARQUET"
  else
    echo "❌ Could not find Task 2 parquet."
    echo "Checked:"
    echo "  - $LOCAL_PARQUET_GLOB"
    echo "  - hdfs://localhost:9000$HDFS_PARQUET_DIR"
    echo "Tip: export TASK2_PARQUET=<path> to override."
    exit 1
  fi
fi

echo "✅ Using source: $PICKED_SOURCE"
echo "   JAVA_HOME        = $JAVA_HOME"
echo "   SPARK_HOME       = $SPARK_HOME"
echo "   SPARK_DEFAULT_FS = ${SPARK_DEFAULT_FS:-<unset>}"
echo "   PYSPARK_PYTHON   = ${PYSPARK_PYTHON:-$(which python)}"
echo

# --------- Mode: load | queries | both (default: both) ----------
MODE="${1:-both}"
case "$MODE" in
  load|queries|both) ;;
  *)
    echo "Usage: $0 [load|queries|both]"
    exit 2
    ;;
esac

# --------- 1) LOAD to Mongo (Spark pipeline) ----------
if [[ "$MODE" == "load" || "$MODE" == "both" ]]; then
  echo "[Task3] Packaging Python modules..."
  PKG_ZIP="/tmp/de-ass_classes.zip"
  rm -f "$PKG_ZIP"
  ( cd "$(pwd)" && zip -qr "$PKG_ZIP" classes )

  echo "[Task3] Running Spark pipeline -> Mongo (parquet: $TASK2_PARQUET)"
  "$SPARK_HOME/bin/spark-submit" \
    --conf "spark.hadoop.fs.defaultFS=${SPARK_DEFAULT_FS:-hdfs://localhost:9000}" \
    --py-files "$PKG_ZIP" \
    classes/task3_pipeline.py \
    --parquet "$TASK2_PARQUET" \
    --mongo_uri "$MONGODB_URI" \
    --mongo_db "$MONGO_DB" \
    --mongo_coll "$MONGO_COLL"
  echo "[Task3] Pipeline complete."
  echo
fi

# --------- 2) Run Mongo queries & exports (pure Python) ----------
if [[ "$MODE" == "queries" || "$MODE" == "both" ]]; then
  echo "[Task3] Running Mongo queries & exports..."
  python3 -u classes/task3_queries.py
  echo "[Task3] Queries complete."
fi
