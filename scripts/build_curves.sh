#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [ -f .env ] && source .env; set +a
DATA_DIR="${IVOL_DATA_DIR:-$HOME/ivoldata}"

START_DATE="${1:-2005-01-01}"
END_DATE="${2:-2025-12-31}"
export DATA_DIR START_DATE END_DATE

mkdir -p "$DATA_DIR/curated"

for f in sql/01_pairs.sql sql/02_atm.sql sql/03_slope.sql sql/04_curve_header.sql; do
  echo ">>> $(date -Is) $f  [$START_DATE..$END_DATE]"
  envsubst < "$f" | duckdb
done
echo "<<< done $(date -Is)"
