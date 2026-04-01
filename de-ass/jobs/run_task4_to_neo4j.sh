#!/usr/bin/env bash
set -euo pipefail

# Usage: ./jobs/run_task4_to_neo4j.sh [YYYY-MM-DD]
DATE_ARG="${1-}"

# Neo4j env (must be set by you)
: "${NEO4J_URI:=bolt://localhost:7687}"
: "${NEO4J_USER:=neo4j}"
: "${NEO4J_PASS:?Set NEO4J_PASS}"
: "${NEO4J_DATABASE:=neo4j}"

# Task2 parquet path (HDFS)
: "${TASK2_PARQUET:=hdfs:///user/student/covid19/processed}"
export TASK2_PARQUET

cd "$(dirname "$0")/.."/classes

echo "[i] Loading from $TASK2_PARQUET into Neo4j db=$NEO4J_DATABASE"
if [[ -n "$DATE_ARG" ]]; then
  echo "[i] Single partition date = $DATE_ARG"
  python3 task4_neo4j_loader.py --db "$NEO4J_DATABASE" --date "$DATE_ARG"
else
  echo "[i] All available dates"
  python3 task4_neo4j_loader.py --db "$NEO4J_DATABASE"
fi
echo "[OK] Task 4 load complete."
