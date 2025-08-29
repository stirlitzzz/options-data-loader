
duckdb -c "
COPY (
WITH src AS (
  SELECT
    upper(ticker)                                 AS symbol,
    -- window_start is epoch *milliseconds*; adjust divisor if secs (/1) or micros (/1e6)
    to_timestamp(window_start / 1000.0)           AS ts_utc,
    CAST(close AS DOUBLE)                         AS close_px
  FROM read_parquet('${POLY_DATA_DIR}/raw/*.parquet')
),
ny AS (
  SELECT
    symbol,
    (ts_utc AT TIME ZONE 'America/New_York')      AS ts_ny,
    close_px
  FROM src
),
rth AS (  -- regular trading hours only
  SELECT
    symbol,
    CAST(ts_ny as DATE)                                   AS trade_date,
    ts_ny,
    close_px
  FROM ny
  WHERE strftime(ts_ny, '%H:%M') BETWEEN '09:30' AND '16:00'
),
params(K) AS (VALUES (1),(5),(15),(30)),
bucketed AS (  -- align to K-minute grid: ..., :00, :05, :10, ...
  SELECT
    r.symbol, r.trade_date, p.K,
    (date_trunc('minute', r.ts_ny)
      - (CAST(EXTRACT(MINUTE FROM r.ts_ny) AS INTEGER) % p.K) * INTERVAL 1 MINUTE) AS t_bucket,
    r.ts_ny, r.close_px
  FROM rth r
  CROSS JOIN params p
),
ranked AS (  -- pick last close in each bucket
  SELECT *,
         row_number() OVER (
           PARTITION BY symbol, trade_date, K, t_bucket
           ORDER BY ts_ny DESC
         ) AS rn
  FROM bucketed
),
kbar AS (
  SELECT symbol, trade_date, K, t_bucket, close_px
  FROM ranked WHERE rn = 1
),
rets AS (  -- K-minute log returns
  SELECT
    symbol, trade_date, K, t_bucket, close_px,
    CASE
      WHEN lag(close_px) OVER (PARTITION BY symbol, trade_date, K ORDER BY t_bucket) IS NULL
      THEN NULL
      ELSE ln(close_px / lag(close_px) OVER (PARTITION BY symbol, trade_date, K ORDER BY t_bucket))
    END AS r
  FROM kbar
),
daily AS (  -- realized variance/vol per day & K
  SELECT
    symbol, trade_date, K,
    COUNT(*)                     AS n_buckets,
    COUNT(r)                     AS n_ret,
    SUM(r*r)                     AS rv,                 -- daily realized variance
    sqrt(SUM(r*r))               AS sigma_daily,        -- daily σ
    sqrt(SUM(r*r)) * sqrt(252.0) AS sigma_annualized    -- annualized σ
  FROM rets
  GROUP BY symbol, trade_date, K
)
SELECT *
FROM daily
-- require enough returns for each K (rough RTH targets: 330(1m),66(5m),22(15m),11(30m))
WHERE n_ret >= CASE K WHEN 1 THEN 330 WHEN 5 THEN 66 WHEN 15 THEN 22 WHEN 30 THEN 11 ELSE 1 END
ORDER BY trade_date, symbol, K
) TO '${POLY_DATA_DIR}/curated/rv_multiK_daily.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 128000000);
"
