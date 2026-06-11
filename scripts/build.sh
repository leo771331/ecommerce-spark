#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_ROOT"

echo "[build] project root: $PROJECT_ROOT"
echo "[build] running sbt compile"
sbt compile

echo "[build] running sbt package"
sbt package

echo "[build] completed"