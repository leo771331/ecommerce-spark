#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW_DIR="${1:-$PROJECT_ROOT/data/raw}"

DATASET_ID="${KAGGLE_DATASET_ID:-mkechinov/ecommerce-behavior-data-from-multi-category-store}"
REQUIRED_FILES=("2019-Oct.csv" "2019-Nov.csv")

mkdir -p "$RAW_DIR"

echo "[download] project root: $PROJECT_ROOT"
echo "[download] raw dir: $RAW_DIR"
echo "[download] kaggle dataset: $DATASET_ID"

if ! command -v kaggle >/dev/null 2>&1; then
  echo "[download] kaggle command not found."
  echo "[download] Install Kaggle CLI with pipx:"
  echo "           sudo apt install -y pipx"
  echo "           pipx ensurepath"
  echo "           pipx install kaggle"
  echo
  echo "[download] Then authenticate with one of these options:"
  echo "           kaggle auth login"
  echo "           or set KAGGLE_API_TOKEN"
  echo "           or place kaggle.json under ~/.kaggle/kaggle.json"
  exit 1
fi

echo "[download] checking Kaggle CLI authentication and dataset access"
kaggle datasets files "$DATASET_ID" >/dev/null

missing_files=()
for file in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "$RAW_DIR/$file" ]]; then
    missing_files+=("$file")
  fi
done

if [[ ${#missing_files[@]} -eq 0 ]]; then
  echo "[download] required files already exist. skip download."
else
  echo "[download] missing files: ${missing_files[*]}"
  echo "[download] downloading dataset archive"

  kaggle datasets download \
    -d "$DATASET_ID" \
    -p "$RAW_DIR" \
    --unzip
fi

echo "[download] validating required files"

for file in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "$RAW_DIR/$file" ]]; then
    echo "[download] required file not found after download: $RAW_DIR/$file"
    exit 1
  fi

  if [[ ! -s "$RAW_DIR/$file" ]]; then
    echo "[download] required file is empty: $RAW_DIR/$file"
    exit 1
  fi
done

echo "[download] completed. downloaded files:"
for file in "${REQUIRED_FILES[@]}"; do
  ls -lh "$RAW_DIR/$file"
done