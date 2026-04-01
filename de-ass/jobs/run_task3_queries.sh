#!/usr/bin/env bash
set -euo pipefail

# Run from repo root
cd "$(dirname "$0")/.."

# --- Mongo connection (respect pre-set env; don't hardcode secrets) ---
export MONGODB_URI="${MONGODB_URI:-mongodb://localhost:27017/}"
export MONGO_DB="${MONGO_DB:-covid}"
export MONGO_COLL="${MONGO_COLL:-daily_country}"

# --- Output directory & plotting flags used by the queries runner ---
export TASK3_EXPORT_DIR="${TASK3_EXPORT_DIR:-$HOME/de-ass/docs}"
export TASK3_SAVE_PLOTS="${TASK3_SAVE_PLOTS:-1}"

# --- Top N controls (optional) ---
export TOP_N_BLIND="${TOP_N_BLIND:-15}"
export TOP_N_STRAIN="${TOP_N_STRAIN:-20}"

python -u classes/task3_queries.py
