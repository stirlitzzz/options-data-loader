ENV ?= .venv
PY  ?= $(ENV)/bin/python

init:
	python -m venv $(ENV); \
	$(PY) -m pip install -U pip; \
	$(PY) -m pip install -e .

pull:
	IVOL_API_KEY=$$IVOL_API_KEY $(PY) src/fetch_ivol_by_list.py tickers.csv \
	  --start 2025-01-01 --end 2025-03-27 --dte 0 30 --delta 0.20 0.50 --combine

duck:
	duckdb -c "SELECT COUNT(*) FROM read_parquet('$$IVOL_DATA_DIR/raw/*.parquet');"
