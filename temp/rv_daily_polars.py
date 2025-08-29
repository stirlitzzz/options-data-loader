import math
import sys
import polars as pl

# sane range in MILLISECONDS: 2000-01-01 .. 2100-01-01
MS_MIN = 946_684_800_000
MS_MAX = 4_102_444_800_000


def rv_daily_for_file(path: str, ks=(1, 5, 15, 30)) -> pl.DataFrame:
    ws = pl.col("window_start").cast(pl.Int64)

    # Normalize epoch to MILLISECONDS (handles sec/ms/µs/ns)
    ws_ms = (
        pl.when((ws >= 1_000_000_000) & (ws <= 9_999_999_999))  # seconds
        .then(ws * 1_000)
        .when((ws >= 1_000_000_000_000) & (ws <= 9_999_999_999_999))  # ms
        .then(ws)
        .when((ws >= 1_000_000_000_000_000) & (ws <= 9_999_999_999_999_999))  # µs
        .then(ws // 1_000)
        .when(
            (ws >= 1_000_000_000_000_000_000) & (ws <= 9_999_999_999_999_999_999)
        )  # ns
        .then(ws // 1_000_000)
        .otherwise(None)
        .alias("ws_ms")
    )

    lf = (
        pl.scan_parquet(path)
        .select(
            pl.col("ticker").str.to_uppercase().alias("symbol"),
            pl.col("close").cast(pl.Float64).alias("close"),
            pl.col("window_start").cast(pl.Int64),
        )
        .with_columns(ws_ms)
        .filter(
            pl.col("ws_ms").is_not_null()
            & (pl.col("ws_ms") >= MS_MIN)
            & (pl.col("ws_ms") <= MS_MAX)
        )
        .with_columns(
            [
                pl.col("ws_ms").cast(pl.Datetime("ms", "UTC")).alias("ts_utc"),
                pl.col("ws_ms")
                .cast(pl.Datetime("ms", "UTC"))
                .dt.convert_time_zone("America/New_York")
                .alias("ts_ny"),
            ]
        )
        # RTH filter with TIME literals
        .filter(pl.col("ts_ny").dt.time().is_between(pl.time(9, 29), pl.time(15, 59)))
    )

    outs = []
    ANCHOR = pl.duration(minutes=29)
    for K in ks:
        t_bucket = (
            (
                pl.col("ts_ny") - ANCHOR
            ).dt.truncate(  # shift so 09:29 becomes a “00” boundary
                f"{K}m"
            )  # floor to K-min grid
            + ANCHOR  # shift back → buckets start at 09:29 + n*K
        ).alias("t_bucket")
        out = (
            lf.with_columns([t_bucket])
            .group_by(["symbol", "t_bucket"])
            .agg(
                [
                    pl.col("close").sort_by(pl.col("ts_ny")).last().alias("close_k"),
                    pl.col("ts_ny").dt.date().first().alias("trade_date"),
                ]
            )
            .sort(["symbol", "trade_date", "t_bucket"])
            .with_columns(
                ((pl.col("close_k") / pl.col("close_k").shift(1)).log())
                .over(["symbol", "trade_date"])
                .alias("r")
            )
            .group_by(["symbol", "trade_date"])
            .agg(
                [
                    pl.len().alias("n_buckets"),
                    pl.col("r").count().alias("n_ret"),
                    (pl.col("r") ** 2).mean().alias("rv"),
                ]
            )
            .with_columns(
                [
                    pl.col("rv").sqrt().alias("sigma_daily"),
                    (pl.col("rv").sqrt() * math.sqrt(252.0 * 24.0 * 60.0 / K)).alias(
                        "sigma_annualized"
                    ),
                    # ( (pl.col("rv") / pl.col("n_ret")) ).sqrt().alias("sigma_daily_std"),
                    # ( (pl.col("rv") / pl.col("n_ret")).sqrt() * math.sqrt(252.0)).alias("sigma_daily_anualized"),
                    pl.lit(K).alias("K"),
                ]
            )
        )
        outs.append(out)

    # use streaming engine (new style)
    return pl.concat(outs).collect(engine="streaming")


if __name__ == "__main__":
    src = sys.argv[1]
    dst = sys.argv[2] if len(sys.argv) > 2 else src.replace(".parquet", "_rv.parquet")
    df = rv_daily_for_file(src)
    df.write_parquet(dst)
    print(df.shape, "->", dst)
