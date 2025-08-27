COPY (
SELECT
  A.stocks_id, A.c_date, A.expiration_date, A.S, A.tau, A.iv_atm,
  S.slope
FROM read_parquet('$IVOL_DATA_DIR/curated/atms${YEAR_SUFFIX}.parquet') A
LEFT JOIN read_parquet('$IVOL_DATA_DIR/curated/smile_slopes${YEAR_SUFFIX}.parquet') S
USING (stocks_id, c_date, expiration_date)
) TO '$IVOL_DATA_DIR/curated/curve_headers${YEAR_SUFFIX}.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 128000000);
