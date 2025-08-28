PRAGMA threads=8;
COPY (
WITH P AS (SELECT * FROM read_parquet('$IVOL_DATA_DIR/curated/pairs${YEAR_SUFFIX}.parquet')),
A AS (SELECT * FROM read_parquet('$IVOL_DATA_DIR/curated/atm${YEAR_SUFFIX}.parquet')),
J AS (
  SELECT
    P.stocks_id, P.c_date, P.expiration_date, P.tau,
    (10.0 / SQRT(NULLIF(P.tau,1e-12))) * P.x     AS X,
    (P.ivol_mid - A.iv_atm)                       AS Y,
    CASE WHEN P.half_spread_norm IS NULL OR P.half_spread_norm <= 0
         THEN 1.0
         ELSE 1.0 / (P.half_spread_norm*P.half_spread_norm + 1e-6)
    END AS w
  FROM P JOIN A USING (stocks_id, c_date, expiration_date)
  WHERE ABS(P.x) <= 0.3
),
agg AS (
  SELECT
    stocks_id, c_date, expiration_date,
    SUM(w) AS sw, SUM(w*X) AS sx, SUM(w*Y) AS sy, SUM(w*X*X) AS sxx, SUM(w*X*Y) AS sxy
  FROM J GROUP BY 1,2,3
)
SELECT
  stocks_id, c_date, expiration_date,
  (sxy - sx*sy/sw) / NULLIF((sxx - (sx*sx)/sw),0) AS slope
FROM agg
) TO '$IVOL_DATA_DIR/curated/smile_slope${YEAR_SUFFIX}.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 128000000);
