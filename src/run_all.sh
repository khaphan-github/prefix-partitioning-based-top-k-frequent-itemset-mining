#!/usr/bin/env bash
set -euo pipefail

# Runs all datasets sequentially and in parallel (multiprocessing) for
# top-k values: 1000, 500, 250, 100, 50, 10.
# Usage:
#   bash src/run_all.sh                 # run all datasets
#   WORKERS=8 bash src/run_all.sh       # override worker count for parallel
#
# Notes:
# - Script assumes it is executed from repo root or any location.
# - It will cd into src/ and use relative paths under ./data and ./benchmark.

# Resolve repo root and cd into src
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/src"

TOPKS=(1000 500 250 100 50 10)
WORKERS="${WORKERS:-4}"
REPORT_DIR="./benchmark/report_27_12_v4"

# Map dataset display names to their input file paths
declare -A FILES
# FILES[OnlineRetailZZ]="./data/data_set/OnlineRetailZZ.txt"
# FILES[SUSY]="./data/data_set/SUSY.txt"
# FILES[accidents]="./data/data_set/accidents.txt"
# FILES[accidents_spmf]="./data/data_set/accidents_spmf.txt"
FILES[chainstoreFIM]="./data/data_set/chainstoreFIM.txt"
# FILES[connect]="./data/data_set/connect.txt"
FILES[pumsb]="./data/data_set/pumsb.txt"
# FILES[sample]="./data/data_set/sample.txt"

for dataset in "${!FILES[@]}"; do
  input="${FILES[$dataset]}"
  seq_out="${REPORT_DIR}/${dataset}/sequential"
  par_out="${REPORT_DIR}/${dataset}/multiprocessing"
  mkdir -p "$seq_out" "$par_out"

  echo "==== Dataset: ${dataset} (input: ${input}) ===="
  for k in "${TOPKS[@]}"; do
    echo "[Sequential] top-k=${k} -> ${seq_out}/"
    python3 main_with_args.py \
      --top-k "${k}" \
      --input "${input}" \
      --output "${seq_out}/"

    echo "[Parallel] top-k=${k}, workers=${WORKERS} -> ${par_out}/"
    python3 main_with_args.py \
      --top-k "${k}" \
      --input "${input}" \
      --output "${par_out}/" \
      --parallel \
      --workers "${WORKERS}"
  done
  echo "==== Completed: ${dataset} ===="
  echo
done

echo "All datasets completed. Reports under ${REPORT_DIR}."
