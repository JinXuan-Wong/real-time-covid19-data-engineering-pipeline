#!/usr/bin/env bash
set -euo pipefail

: "${NEO4J_URI:=bolt://localhost:7687}"
: "${NEO4J_USER:=neo4j}"
: "${NEO4J_PASS:?Set NEO4J_PASS}"    
: "${NEO4J_DATABASE:=neo4j}"

echo "[i] Checking Neo4j connectivity → $NEO4J_URI (db=$NEO4J_DATABASE)"

python3 - <<'PY'
import os, sys
from neo4j import GraphDatabase, basic_auth

uri = os.environ['NEO4J_URI']
user = os.environ['NEO4J_USER']
pwd = os.environ['NEO4J_PASS']
db = os.environ.get('NEO4J_DATABASE','neo4j')

try:
    drv = GraphDatabase.driver(uri, auth=basic_auth(user, pwd))
    with drv.session(database=db) as s:
        v = s.run("RETURN 1 AS ok").single()["ok"]
    drv.close()
    print(f"[OK] Connected. RETURN 1 = {v}")
except Exception as e:
    print("[ERROR] Could not connect:", e)
    sys.exit(2)
PY
