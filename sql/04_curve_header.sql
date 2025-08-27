duckdb -c "
COPY (
SELECT
  A.stocks_id, A.c_date, A.expiration_date, A.S, A.tau, A.iv_atm,
  S.slope
FROM read_parquet('$IVOL_DATA_DIR/curated/atm.parquet') A
LEFT JOIN read_parquet('$IVOL_DATA_DIR/curated/smile_slope.parquet') S
USING (stocks_id, c_date, expiration_date)
) TO '$IVOL_DATA_DIR/curated/curve_header.parquet' (FORMAT PARQUET);
"
