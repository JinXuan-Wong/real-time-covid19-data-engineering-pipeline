#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo "[Task1] Starting producer -> topic=covid19_stream"
python3 classes/api_producer.py
