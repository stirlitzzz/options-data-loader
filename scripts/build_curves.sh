#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Load .env into shell so $DATA_DIR is available for envsubst
set -a; [ -f .env ] && source .env; set +a

DATA_DIR="${IVOL_DATA_DIR:-$HOME/ivoldata}"
mkdir -p "$DATA_DIR/curated"

# Optional year param: ./scripts/build_curves.sh 2020
YEAR="${1:-}"
if [ -n "$YEAR" ]; then
  YEAR_FILTER="year(CAST(c_date AS DATE)) = ${YEAR}"
  YEAR_SUFFIX="_${YEAR}"
else
  YEAR_FILTER="1=1"
  YEAR_SUFFIX=""
fi

export DATA_DIR YEAR_FILTER YEAR_SUFFIX

for f in sql/01_pairs.sql sql/02_atm.sql sql/03_slope.sql sql/04_curve_header.sql; do
  echo ">>> running $f"
  envsubst < "$f" | duckdb
done
