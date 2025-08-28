duckdb -c "
 SELECT   stocks_id,
 MIN(c_date) AS min_d,
     MAX(c_date) AS max_d,
     COUNT(*)    AS n
  FROM read_parquet('$IVOL_DATA_DIR/raw/*.parquet')
  WHERE c_date >= '2007-01-01'
    GROUP BY stocks_id
    ORDER BY n DESC;
    "
