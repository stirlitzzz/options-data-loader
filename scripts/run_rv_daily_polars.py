#!/usr/bin/env python3
import os
import sys
import re
import argparse
import traceback
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# import the function you already have
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
from rv_daily_polars import rv_daily_for_file  # type: ignore

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def out_path(out_root: Path, date_str: str) -> Path:
    # y = date_str[:4]
    return out_root / f"all_min{date_str}.parquet"


def extract_date(p: Path) -> str:
    m = DATE_RE.search(p.name)
    if not m:
        raise ValueError(f"Cannot find YYYY-MM-DD in filename: {p}")
    return m.group(1)


def do_one(infile: Path, out_root: Path, overwrite: bool) -> tuple[Path, str]:
    try:
        d = extract_date(infile)
        dst = out_path(out_root, d)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() and not overwrite:
            return (infile, f"SKIP {d} (exists)")
        df = rv_daily_for_file(str(infile))  # returns all K (1/5/15/30) in one DF
        df.write_parquet(str(dst))
        return (infile, f"OK   {d} -> {dst.name} ({df.shape[0]} rows)")
    except Exception as e:
        return (infile, f"ERR  {infile.name}: {e}\n{traceback.format_exc()}")


def main():
    ap = argparse.ArgumentParser(
        description="Run rv_daily_polars over many daily files."
    )
    ap.add_argument(
        "inglob",
        help="Glob for daily raw files, e.g. $POLY_DATA_DIR/raw/*_spx_1m.parquet",
    )
    ap.add_argument(
        "--out",
        default=None,
        help="Output root (default: $POLY_DATA_DIR/curated/rv_daily)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Parallel workers (2 is safe on small boxes)",
    )
    ap.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing outputs"
    )
    args = ap.parse_args()

    inglob = os.path.expandvars(os.path.expanduser(args.inglob))
    files = sorted(Path().glob(inglob) if "*" in inglob else [Path(inglob)])
    if not files:
        print(f"No files matched: {inglob}", file=sys.stderr)
        sys.exit(1)

    out_root = Path(
        os.path.expandvars(
            os.path.expanduser(
                args.out
                or f"{os.environ.get('POLY_DATA_DIR', str(Path.home()/'polydata'))}/curated/rv_daily"
            )
        )
    )
    out_root.mkdir(parents=True, exist_ok=True)

    print(f"[cfg] n_files={len(files)} out_root={out_root} workers={args.workers}")
    ok = err = skip = 0
    with ProcessPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(do_one, f, out_root, args.overwrite): f for f in files}
        for fut in as_completed(futs):
            _, msg = fut.result()
            print(msg)
            if msg.startswith("OK"):
                ok += 1
            elif msg.startswith("SKIP"):
                skip += 1
            else:
                err += 1
    print(f"[done] OK={ok} SKIP={skip} ERR={err}")


if __name__ == "__main__":
    main()
