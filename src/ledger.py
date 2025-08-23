# src/ledger.py
import sqlite3, json, time
from pathlib import Path
from .paths import DATA_DIR
DB = DATA_DIR / "loader_ledger.sqlite"

DDL = """
CREATE TABLE IF NOT EXISTS ingestions(
  id INTEGER PRIMARY KEY,
  symbol TEXT, start_date TEXT, end_date TEXT,
  dte_lo INT, dte_hi INT, abs_lo REAL, abs_hi REAL,
  nrows INT, bytes INT, status TEXT, params TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
def connect():
    DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB)
    conn.execute(DDL); conn.commit()
    return conn

def record(symbol, start, end, dte, delta, nrows, bytes_, status, extra=None):
    conn = connect()
    conn.execute(
        "INSERT INTO ingestions(symbol,start_date,end_date,dte_lo,dte_hi,abs_lo,abs_hi,nrows,bytes,status,params)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (symbol, start, end, dte[0], dte[1], delta[0], delta[1], nrows, bytes_, status, json.dumps(extra or {}))
    )
    conn.commit(); conn.close()
