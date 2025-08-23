#!/usr/bin/env python3
# fetch_ivol_by_list.py
import os, time, argparse, datetime as dt
import pandas as pd
import ivolatility as ivol
from paths import RAW_DIR

# -------------------------- Helpers --------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Fetch IVol options for a list of tickers with delta/DTE filters.")
    p.add_argument("tickers_csv", help="CSV with a column named 'symbol' or 'ticker'.")
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end",   required=True, help="End date YYYY-MM-DD (inclusive)")
    p.add_argument("--dte",   nargs=2, type=int, metavar=("DTE_FROM","DTE_TO"), default=(0, 30),
                   help="Min/Max days-to-expiry (inclusive)")
    p.add_argument("--delta", nargs=2, type=float, metavar=("ABS_LO","ABS_HI"), default=(0.20, 0.50),
                   help="Absolute delta band, e.g. 0.20 0.50 -> calls: +0.20..+0.50, puts: -0.50..-0.20")
    p.add_argument("--chunk-days", type=int, default=31, help="Size of date chunks per request (avoid provider limits).")
    p.add_argument("--sleep", type=float, default=0.2, help="Pause between API calls (be polite).")
    p.add_argument("--outdir", default=str(RAW_DIR), help="Output folder (outside repo).")
    p.add_argument("--combine", action="store_true", help="Also write a combined file across all symbols.")
    p.add_argument("--fmt", choices=["parquet","csv"], default="parquet", help="Output format per symbol and combined.")
    p.add_argument("--key", default=None, help="IVOL API key (or set IVOL_API_KEY env var).")
    return p.parse_args()

def load_symbols(path):
    df = pd.read_csv(path)
    cols = [c.lower() for c in df.columns]
    if "symbol" in cols:
        symcol = df.columns[cols.index("symbol")]
    elif "ticker" in cols:
        symcol = df.columns[cols.index("ticker")]
    else:
        raise ValueError("Ticker CSV must have a 'symbol' or 'ticker' column.")
    syms = df[symcol].astype(str).str.strip().unique().tolist()
    return [s for s in syms if s]  # no blanks

def daterange_chunks(start, end, chunk_days):
    start_dt = pd.to_datetime(start)
    end_dt   = pd.to_datetime(end)
    cur = start_dt
    delta = pd.Timedelta(days=chunk_days - 1)  # inclusive chunks
    while cur <= end_dt:
        chunk_end = min(cur + delta, end_dt)
        yield cur.date().isoformat(), chunk_end.date().isoformat()
        cur = chunk_end + pd.Timedelta(days=1)

def delta_band(cp, lo, hi):
    """Calls: +lo..+hi; Puts: -hi..-lo."""
    return (lo, hi) if cp == "C" else (-hi, -lo)

def dedup(df):
    # Use the most stable keys present
    keys_pref = ["c_date","expiration_date","option_symbol","stocks_id","price_strike","call_put"]
    keys = [k for k in keys_pref if k in df.columns]
    if keys:
        return df.drop_duplicates(subset=keys, keep="first")
    return df

def write_frame(df, path, fmt):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if fmt == "parquet":
        try:
            df.to_parquet(path, index=False)
        except Exception:
            # fall back if pyarrow/fastparquet not present
            csv_path = os.path.splitext(path)[0] + ".csv"
            df.to_csv(csv_path, index=False)
            return csv_path
    else:
        df.to_csv(path, index=False)
    return path

# -------------------------- Fetcher --------------------------

def main():
    args = parse_args()

    api_key = args.key or os.environ.get("IVOL_API_KEY")
    if not api_key:
        raise SystemExit("Missing API key. Pass --key or set IVOL_API_KEY.")

    ivol.setLoginParams(apiKey=api_key)
    get_opts = ivol.setMethod('/equities/eod/stock-opts-by-param')

    dte_lo, dte_hi = args.dte
    abs_lo, abs_hi = args.delta

    symbols = load_symbols(args.tickers_csv)
    print(f"Symbols: {len(symbols)} found -> {symbols[:8]}{'...' if len(symbols)>8 else ''}")

    all_frames = []

    for sym in symbols:
        frames = []
        for cs, ce in daterange_chunks(args.start, args.end, args.chunk_days):
            for cp in ("C","P"):
                dlo, dhi = delta_band(cp, abs_lo, abs_hi)
                # Fetch
                try:
                    df = get_opts(symbol=sym, cp=cp,
                                  startDate=cs, endDate=ce,
                                  dteFrom=dte_lo, dteTo=dte_hi,
                                  deltaFrom=dlo, deltaTo=dhi)
                except Exception as e:
                    print(f"[WARN] {sym} {cp} {cs}->{ce}: {e}")
                    df = pd.DataFrame()
                if df is not None and len(df) > 0:
                    frames.append(df)
                time.sleep(args.sleep)
        if frames:
            out_df = dedup(pd.concat(frames, ignore_index=True))
            # optional sanity: keep only rows truly in-band
            if "delta" in out_df.columns:
                # calls should be >= +abs_lo for cp=='C'; puts <= -abs_lo for cp=='P'
                mask = []
                if "call_put" in out_df.columns:
                    cpcol = out_df["call_put"].astype(str)
                    mask = (
                        ((cpcol == "C") & (out_df["delta"].between(abs_lo, abs_hi, inclusive="both"))) |
                        ((cpcol == "P") & (out_df["delta"].between(-abs_hi, -abs_lo, inclusive="both")))
                    )
                    out_df = out_df[mask]
            # write per-symbol
            tag = f"{args.start}_{args.end}_DTE{dte_lo}_{dte_hi}_Δ{abs_lo:g}_{abs_hi:g}"
            out_name = f"ivol_{sym}_{tag}.{args.fmt if args.fmt=='csv' else 'parquet'}"
            out_path = os.path.join(args.outdir, out_name)
            written = write_frame(out_df, out_path, args.fmt)
            print(f"[OK] {sym}: {len(out_df):,} rows -> {written}")
            if args.combine:
                out_df = out_df.copy()
                out_df["symbol"] = sym
                all_frames.append(out_df)
        else:
            print(f"[SKIP] {sym}: no data returned for given filters.")

    if args.combine and all_frames:
        combo = dedup(pd.concat(all_frames, ignore_index=True))
        tag = f"{args.start}_{args.end}_DTE{dte_lo}_{dte_hi}_Δ{abs_lo:g}_{abs_hi:g}"
        combo_name = f"ivol_ALL_{tag}.{args.fmt if args.fmt=='csv' else 'parquet'}"
        combo_path = os.path.join(args.outdir, combo_name)
        written = write_frame(combo, combo_path, args.fmt)
        print(f"[OK] combined: {len(combo):,} rows -> {written}")

if __name__ == "__main__":
    main()
