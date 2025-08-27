# load .env so $IVOL_DATA_DIR is set
set -a; [ -f .env ] && source .env; set +a
mkdir -p "$IVOL_DATA_DIR/matrix"

# change N in rn=1/2/3 to get 1st, 2nd, 3rd maturity
for N in {1..5}; do
duckdb -c "
COPY (
WITH a AS (
  SELECT
    stocks_id,
    CAST(c_date AS DATE)          AS c_date,
    CAST(expiration_date AS DATE) AS expiration_date,
    iv_atm,
    ROW_NUMBER() OVER (
      PARTITION BY stocks_id, CAST(c_date AS DATE)
      ORDER BY CAST(expiration_date AS DATE)
    ) AS rn
  FROM read_parquet('$IVOL_DATA_DIR/curated/atm_2006.parquet')
)
SELECT c_date, stocks_id, iv_atm, expiration_date
FROM a
WHERE rn = $N
ORDER BY c_date, stocks_id
) TO '$IVOL_DATA_DIR/matrix/front${N}_long_2006.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 128000000);
"
done
