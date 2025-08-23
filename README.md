export IVOL_API_KEY=...; export IVOL_DATA_DIR=~/data/options-data
python -m venv .venv && . .venv/bin/activate && pip install -e .
ivol-fetch tickers.csv --start 2025-01-01 --end 2025-03-27 --dte 0 30 --delta 0.20 0.50 --combine
