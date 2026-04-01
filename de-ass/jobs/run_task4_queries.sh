#!/usr/bin/env bash
set -euo pipefail

# Optional arg: YYYY-MM-DD
DATE_ARG="${1-}"

# Neo4j env
: "${NEO4J_URI:=bolt://localhost:7687}"
: "${NEO4J_USER:=neo4j}"
: "${NEO4J_PASS:?Set NEO4J_PASS}"
: "${NEO4J_DATABASE:=neo4j}"

# If no date provided, try to auto-detect from Neo4j (latest Event date)
if [[ -z "${DATE_ARG}" ]]; then
  echo "[i] No date provided. Trying to detect the latest event_date from Neo4j..."
  DATE_ARG="$(python3 - <<'PY'
import os
from neo4j import GraphDatabase
uri=os.getenv('NEO4J_URI','bolt://localhost:7687')
user=os.getenv('NEO4J_USER','neo4j')
pwd=os.getenv('NEO4J_PASS','neo4j')
db=os.getenv('NEO4J_DATABASE','neo4j')
latest=''
try:
    drv=GraphDatabase.driver(uri, auth=(user,pwd))
    with drv.session(database=db) as s:
        rec=s.run("MATCH (e:Event) RETURN e.event_date AS d ORDER BY d DESC LIMIT 1").single()
        latest = rec['d'] if rec else ''
    drv.close()
except Exception:
    pass
print(latest)
PY
)"
fi

# Fallback: detect from HDFS partitions if Neo4j didn’t return anything
if [[ -z "${DATE_ARG}" ]]; then
  echo "[i] Couldn’t get date from Neo4j. Trying HDFS partitions..."
  TASK2_PARQUET="${TASK2_PARQUET:-hdfs:///user/student/covid19/processed}"
  BASE_PATH="${TASK2_PARQUET%/}"
  LIST="$(hdfs dfs -ls "${BASE_PATH}/event_date="* 2>/dev/null || true)"
  DATE_ARG="$(echo "$LIST" | awk -F'event_date=' '{print $2}' | awk '{print $1}' | sort -u | tail -n 1)"
fi

if [[ -z "${DATE_ARG}" ]]; then
  echo "Could not auto-detect a date. Usage: $0 YYYY-MM-DD"
  exit 1
fi

cd "$(dirname "$0")/.."/classes

echo "[i] Running charts for ${DATE_ARG} → de-ass/docs/"
python3 task4_neo4j_queries.py --date "${DATE_ARG}" --db "${NEO4J_DATABASE}" --limit "${LIMIT:-12}"
echo "[OK] Charts saved."
