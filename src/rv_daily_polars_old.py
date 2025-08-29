# rv_daily_polars.py
import polars as pl
import math
import sys


def rv_daily_for_file(path, ks=(1, 5, 15, 30)):
    lf = (
        pl.scan_parquet(path)
        .select(
            pl.col("ticker").str.to_uppercase().alias("symbol"),
            pl.col("close").cast(pl.Float64),
            pl.col("window_start").cast(pl.Int64),  # epoch ms
        )
        .with_columns(
            ts_utc=(pl.col("window_start"))
            .cast(pl.Datetime("ms"))
            .dt.replace_time_zone("UTC"),
            ts_ny=pl.col("ts_utc").dt.convert_time_zone("America/New_York"),
        )
        .filter(pl.col("ts_ny").dt.strftime("%H:%M").is_between("09:30", "16:00"))
    )

    outs = []
    for K in ks:
        out = (
            lf.with_columns(t_bucket=pl.col("ts_ny").dt.truncate(f"{K}m"))
            .group_by(["symbol", "t_bucket"])
            .agg(
                close_k=pl.col("close").last(),
                trade_date=pl.col("ts_ny").dt.date().first(),
            )
            .sort(["symbol", "t_bucket"])
            .with_columns(r=(pl.col("close_k") / pl.col("close_k").shift(1)).log())
            .group_by(["symbol", "trade_date"])
            .agg(
                n_buckets=pl.count(),
                n_ret=pl.col("r").count(),
                rv=pl.col("r").pow(2).sum(),
            )
            .with_columns(
                sigma_daily=pl.col("rv").sqrt(),
                sigma_annualized=pl.col("rv").sqrt() * math.sqrt(252.0),
                K=pl.lit(K),
            )
        )
        outs.append(out)
    return pl.concat(outs).collect()


if __name__ == "__main__":
    df = rv_daily_for_file(sys.argv[1])  # path to one daily parquet
    df.write_parquet(sys.argv[2])
