PRAGMA threads=8;

COPY (
WITH P AS (SELECT * FROM read_parquet('$IVOL_DATA_DIR/curated/pairs.parquet')),
ranked AS (
  SELECT *,
         CASE WHEN K <= S THEN ROW_NUMBER() OVER (
                PARTITION BY stocks_id, c_date, expiration_date
                ORDER BY (S-K) ASC
              ) END AS rn_below,
         CASE WHEN K >= S THEN ROW_NUMBER() OVER (
                PARTITION BY stocks_id, c_date, expiration_date
                ORDER BY (K-S) ASC
              ) END AS rn_above
  FROM P
),
chosen AS (
  SELECT
    b.stocks_id, b.c_date, b.expiration_date,
    b.K AS K_below, b.ivol_mid AS iv_below,
    a.K AS K_above, a.ivol_mid AS iv_above,
    b.S AS S, b.tau AS tau
  FROM ranked b
  JOIN ranked a USING (stocks_id, c_date, expiration_date)
  WHERE b.rn_below = 1 AND a.rn_above = 1 AND a.K >= b.K
)
SELECT
  stocks_id, c_date, expiration_date, S, tau,
  CASE WHEN K_above = K_below THEN (iv_above + iv_below)/2.0
       ELSE (iv_below * (K_above - S) + iv_above * (S - K_below)) / NULLIF(K_above - K_below,0)
  END AS iv_atm
FROM chosen
)  TO '${DATA_DIR}/curated/atm${YEAR_SUFFIX}.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 128000000);
