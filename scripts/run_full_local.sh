#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_ROOT"

OCT_FILE="$PROJECT_ROOT/data/raw/2019-Oct.csv"
NOV_FILE="$PROJECT_ROOT/data/raw/2019-Nov.csv"

if [[ ! -s "$OCT_FILE" ]]; then
  echo "[run-full-local] missing raw file: $OCT_FILE"
  echo "[run-full-local] Run ./scripts/download_data.sh first."
  exit 1
fi

if [[ ! -s "$NOV_FILE" ]]; then
  echo "[run-full-local] missing raw file: $NOV_FILE"
  echo "[run-full-local] Run ./scripts/download_data.sh first."
  exit 1
fi

export INPUT_PATHS="${INPUT_PATHS:-data/raw/2019-Oct.csv,data/raw/2019-Nov.csv}"
export OUTPUT_PATH="${OUTPUT_PATH:-data/processed/user_activity_sessions}"
export RUN_ID="${RUN_ID:-oct_nov_2019_v1}"

if [[ -z "${SPARK_SUBMIT_OPTIONS:-}" ]]; then
  export SPARK_SUBMIT_OPTIONS="--driver-memory 8g --conf spark.sql.shuffle.partitions=200"
fi

echo "[run-full-local] input: $INPUT_PATHS"
echo "[run-full-local] output: $OUTPUT_PATH"
echo "[run-full-local] run id: $RUN_ID"
echo "[run-full-local] spark options: $SPARK_SUBMIT_OPTIONS"

"$PROJECT_ROOT/scripts/run_local.sh"