# src/paths.py
from __future__ import annotations
import os
from pathlib import Path

# 1) Load .env from repo root (works in scripts + notebooks)
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except Exception:
    # dotenv is optional; fine if not installed
    pass

_PROJECT = "options-data"

def resolve_data_dir(override: str | None = None) -> Path:
    # 2) Precedence: CLI override > env > .env > default
    if override:
        base = override
    else:
        base = (
            os.getenv("IVOL_DATA_DIR")
            or os.getenv("DATA_DIR")   # optional alias if you like
            or f"{Path.home()}/data/{_PROJECT}"
        )
    p = Path(base).expanduser().resolve()
    return p

DATA_DIR = resolve_data_dir()
print(f"DATA_DIR {DATA_DIR}")
RAW_DIR  = DATA_DIR / "raw"
LOG_DIR  = DATA_DIR / "logs"
TMP_DIR  = DATA_DIR / "tmp"

for p in (RAW_DIR, LOG_DIR, TMP_DIR):
    p.mkdir(parents=True, exist_ok=True)
