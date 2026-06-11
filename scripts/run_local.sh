 #!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_ROOT"

APP_CLASS="com.ecommerce.spark.EcommerceSessionApp"
JAR_PATH="$PROJECT_ROOT/target/scala-2.12/ecommerce-spark_2.12-0.1.0-SNAPSHOT.jar"

INPUT_PATHS="${INPUT_PATHS:-data/sample/raw_events_sample.csv}"
OUTPUT_PATH="${OUTPUT_PATH:-data/processed/user_activity_sessions_sample}"
RUN_ID="${RUN_ID:-sample_local_$(date +%Y%m%d%H%M%S)}"

if ! command -v spark-submit >/dev/null 2>&1; then
  echo "[run-local] spark-submit command not found."
  echo "[run-local] Please install Spark and make sure SPARK_HOME/bin is in PATH."
  exit 1
fi

if [[ ! -f "$JAR_PATH" ]]; then
  echo "[run-local] jar not found: $JAR_PATH"
  echo "[run-local] Run ./scripts/build.sh first."
  exit 1
fi

echo "[run-local] app class: $APP_CLASS"
echo "[run-local] jar: $JAR_PATH"
echo "[run-local] input: $INPUT_PATHS"
echo "[run-local] output: $OUTPUT_PATH"
echo "[run-local] run id: $RUN_ID"

spark-submit \
  --class "$APP_CLASS" \
  --master local[*] \
  "$JAR_PATH" \
  --input "$INPUT_PATHS" \
  --output "$OUTPUT_PATH" \
  --run-id "$RUN_ID"