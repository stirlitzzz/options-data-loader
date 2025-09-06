PRAGMA threads=8;

COPY (
WITH P AS (SELECT * FROM read_parquet('${DATA_DIR}/curated/pairs${YEAR_SUFFIX}.parquet')),
G AS (
  SELECT
    stocks_id, c_date, expiration_date,
    any_value(S)   AS S,
    any_value(tau) AS tau,

    -- nearest strike <= S
    first(K        ORDER BY K DESC) FILTER (WHERE K <= S) AS K_below,
    first(ivol_mid ORDER BY K DESC) FILTER (WHERE K <= S) AS iv_below,

    -- nearest strike >= S
    first(K        ORDER BY K ASC)  FILTER (WHERE K >= S) AS K_above,
    first(ivol_mid ORDER BY K ASC)  FILTER (WHERE K >= S) AS iv_above,

    -- exact ATM if present
    min(ivol_mid)  FILTER (WHERE K = S) AS iv_atm
  FROM P
  GROUP BY 1,2,3
),
H AS (
  SELECT
    *,
    CASE
      WHEN K_below IS NOT NULL AND K_above IS NOT NULL AND K_above <> K_below
        THEN iv_below + (iv_above - iv_below) * (S - K_below) / (K_above - K_below)
    END AS iv_interp
  FROM G
)
SELECT
  stocks_id, c_date, expiration_date,
  S, tau, K_below, iv_below, K_above, iv_above,
  COALESCE(iv_atm, iv_interp, iv_below, iv_above) AS iv_at_spot
FROM H
)  TO '${DATA_DIR}/curated/atm${YEAR_SUFFIX}.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 128000000);
