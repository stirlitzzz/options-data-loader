PRAGMA threads=8;

COPY (
WITH raw AS (
  SELECT
    CAST(c_date AS DATE)                  AS c_date,
    stocks_id,
    CAST(expiration_date AS DATE)         AS expiration_date,
    lower(call_put)                       AS cp,
    price_strike                          AS K,
    iv, delta, vega, ask, bid,
    underlying_price                      AS S,
    dte
  FROM read_parquet('${DATA_DIR}/raw/*.parquet')
  WHERE iv IS NOT NULL AND vega IS NOT NULL AND dte IS NOT NULL
    AND (${YEAR_FILTER})
),
calls AS (
  SELECT c_date, stocks_id, expiration_date, K,
         iv AS iv_c, delta AS delta_c, vega AS vega_c,
         ask AS ask_c,  bid AS bid_c,  S AS S_c, dte AS dte_c
  FROM raw WHERE cp = 'c'
),
puts AS (
  SELECT c_date, stocks_id, expiration_date, K,
         iv AS iv_p, delta AS delta_p, vega AS vega_p,
         ask AS ask_p,  bid AS bid_p,  S AS S_p, dte AS dte_p
  FROM raw WHERE cp = 'p'
),
pairs AS (
  SELECT
    calls.c_date, calls.stocks_id, calls.expiration_date, calls.K,
    (calls.S_c + puts.S_p)/2.0                                AS S,
    GREATEST((calls.dte_c+puts.dte_p)/2.0, 1e-6)              AS dte,
    ((calls.dte_c+puts.dte_p)/2.0)/365.0                      AS tau,
    LEAST(GREATEST(calls.delta_c,0.0),1.0)                    AS delta_c01,
    calls.iv_c, puts.iv_p,
    calls.vega_c, puts.vega_p,
    calls.ask_c, calls.bid_c, puts.ask_p, puts.bid_p
  FROM calls JOIN puts USING (c_date, stocks_id, expiration_date, K)
),
scored AS (
  SELECT
    *,
    (vega_c + vega_p)/2.0                                     AS vega_avg,
    LEAST(ask_p - bid_p, ask_c - bid_c)                       AS width_lo,
    iv_c*(1.0 - delta_c01) + iv_p*(delta_c01)                 AS ivol_mid,
    CASE WHEN (vega_c + vega_p) > 0 AND LEAST(ask_p-bid_p, ask_c-bid_c) > 0
         THEN LEAST(ask_p-bid_p, ask_c-bid_c) / ( ((vega_c+vega_p)/2.0) * 2.0 )
         ELSE NULL END                                        AS half_spread_norm,
    LN(K / NULLIF(S,0))                                       AS x
  FROM pairs
  WHERE vega_c > 0 AND vega_p > 0
)
SELECT c_date, stocks_id, expiration_date, K, S, dte, tau,
       delta_c01, iv_c, iv_p, vega_c, vega_p, ivol_mid,
       half_spread_norm, x
FROM scored
WHERE ivol_mid BETWEEN 0.01 AND 5.00
  AND ABS(x) < 1.0
  AND dte BETWEEN 1 AND 730
) TO '${DATA_DIR}/curated/pairs${YEAR_SUFFIX}.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 128000000);
