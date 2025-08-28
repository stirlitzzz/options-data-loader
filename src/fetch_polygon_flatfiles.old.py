#!/usr/bin/env python3
# src/fetch_polygon_flatfiles.py
import os
import argparse
import gzip
from io import BytesIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config
import pandas as pd
from dotenv import load_dotenv
import pandas_market_calendars as mcal

# --------- helpers ---------


def load_tickers(path: str) -> set[str]:
    df = pd.read_csv(path)
    # accept 'Symbol' or 'symbol' or 'ticker'
    for c in ("Symbol", "symbol", "ticker", "Ticker"):
        if c in df.columns:
            tickers = set(df[c].astype(str).str.strip().str.upper().unique())
            return {t for t in tickers if t}
    raise ValueError("Ticker CSV must have a 'Symbol' or 'ticker' column")


def nyse_dates(start: str, end: str) -> list[pd.Timestamp]:
    cal = mcal.get_calendar("NYSE")
    sched = cal.schedule(start_date=start, end_date=end)
    return list(pd.DatetimeIndex(sched.index).tz_localize(None))


def mk_s3():
    # Retry + s3v4 signing against Polygon's S3-compatible endpoint
    cfg = Config(
        signature_version="s3v4",
        retries={"max_attempts": 10, "mode": "standard"},
        read_timeout=60,
        connect_timeout=10,
    )
    # return boto3.client("s3", endpoint_url="https://files.polygon.io", config=cfg)
    return boto3.client(
        "s3",
        endpoint_url="https://files.polygon.io",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        or os.getenv("AWS_SECRET"),
        config=cfg,
    )


def key_for(date_str: str) -> tuple[str, str]:
    y, m, _ = date_str.split("-")
    prefix = "us_stocks_sip/minute_aggs_v1"
    return "flatfiles", f"{prefix}/{y}/{m}/{date_str}.csv.gz"


def ensure_outdir(base: Path) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    return base


def save_parquet(df: pd.DataFrame, outdir: Path, date_str: str) -> Path:
    out = outdir / f"{date_str}_spx_1m.parquet"
    df.to_parquet(out, index=False, compression="zstd")
    return out


# --------- core ---------


def fetch_one_day(
    s3, date_str: str, tickers: set[str], outdir: Path, keep_cols=None
) -> tuple[str, str]:
    """Returns (date_str, status) where status is 'OK', 'EMPTY', 'MISSING', or 'ERROR:<msg>'."""
    bucket, key = key_for(date_str)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        with gzip.GzipFile(fileobj=BytesIO(raw)) as gz:
            df = pd.read_csv(gz)
    except s3.exceptions.NoSuchKey:  # type: ignore[attr-defined]
        return date_str, "MISSING"
    except Exception as e:
        return date_str, f"ERROR:{e}"

    if "ticker" not in df.columns:
        return date_str, "ERROR:no_ticker_col"

    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df[df["ticker"].isin(tickers)]
    if df.empty:
        return date_str, "EMPTY"

    if keep_cols:
        cols = [c for c in keep_cols if c in df.columns]
        df = df[cols]

    # normalize schema a bit
    if "t" in df.columns:  # epoch millis for bar start
        df["ts_utc"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    out = save_parquet(df, outdir, date_str)
    return date_str, f"OK:{len(df)}->{out.name}"


def main():
    load_dotenv()  # .env: POLY_DATA_DIR, AWS_KEY/SECRET (if required by your setup)

    p = argparse.ArgumentParser(
        description="Fetch Polygon flatfiles 1m, filter to tickers, write daily Parquet."
    )
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--tickers", required=True, help="CSV with Symbol/ticker column")
    p.add_argument(
        "--outdir", default=None, help="Output dir (default: ${POLY_DATA_DIR}/raw)"
    )
    p.add_argument(
        "--workers", type=int, default=4, help="Parallel days to fetch (default 4)"
    )
    p.add_argument(
        "--cols",
        nargs="*",
        default=None,
        help="Optional column subset to keep (e.g. ticker t o h l c v n vw)",
    )
    args = p.parse_args()

    data_root = Path(os.getenv("POLY_DATA_DIR", f"{Path.home()}/polydata")).expanduser()
    outdir = ensure_outdir(Path(args.outdir) if args.outdir else (data_root / "raw"))

    tickers = load_tickers(args.tickers)
    dates = [d.strftime("%Y-%m-%d") for d in nyse_dates(args.start, args.end)]

    # S3 client (uses env AWS_KEY/AWS_SECRET if required by your Polygon account)
    s3 = mk_s3()

    print(f"[cfg] days={len(dates)} tickers={len(tickers)} outdir={outdir}")
    statuses = []
    # Parallel across days (keeps memory low and S3-friendly)
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {
            ex.submit(fetch_one_day, s3, ds, tickers, outdir, args.cols): ds
            for ds in dates
        }
        for fut in as_completed(futs):
            ds, st = fut.result()
            statuses.append((ds, st))
            print(f"[{ds}] {st}")

    # Quick summary
    ok = sum(st.startswith("OK") for _, st in statuses)
    miss = sum(st == "MISSING" for _, st in statuses)
    empty = sum(st == "EMPTY" for _, st in statuses)
    err = [x for x in statuses if x[1].startswith("ERROR")]
    print(f"[done] OK={ok} EMPTY={empty} MISSING={miss} ERR={len(err)}")
    if err:
        print("Sample error:", err[0])


if __name__ == "__main__":
    main()
