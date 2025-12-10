import polars as pl
from result import CheckResult
import numpy as np

def pnl_distribution(df: pl.DataFrame) -> CheckResult:
    pnl  = df['Pnl'].to_numpy()
    output = {
        "mean": f'Rs.{pnl.mean():.4f}',
        "max": f'Rs.{pnl.max():.4f}',
        "min": f'Rs.{pnl.min():.4f}'
    }
    return CheckResult("PnL ", "UNIVERSAL", "FETCHED INFO", "Pnl Distribution", output)


def trade_duration(df: pl.DataFrame) -> CheckResult:
    df2 = df.with_columns([
        pl.col("KeyEpoch").cast(pl.Int64),
        pl.col("ExitEpoch").cast(pl.Int64),

        pl.col("KeyEpoch").cast(pl.Datetime("us")).alias("EntryDT"),
        pl.col("ExitEpoch").cast(pl.Datetime("us")).alias("ExitDT"),
    ])

    df2 = df2.with_columns(
        (pl.col("ExitDT") - pl.col("EntryDT")).dt.total_seconds().alias("DurationSec")
    )
    durations = df2["DurationSec"].to_numpy() / 86400  
    output = {
        "mean": f'{durations.mean():.4f} DAYS',
        "max": f'{durations.max():.4f} DAYS',
        "min": f'{durations.min():.4f} DAYS'
    }

    return CheckResult("Trades duration (DAYS)", "UNIVERSAL", "FETCHED INFO", "Trade Duration", output)


def concurrent_positions(df: pl.DataFrame) -> CheckResult:
    entry_col = "KeyEpoch"
    exit_col = "ExitEpoch"
    df2 = df.with_columns([
        pl.col(entry_col).cast(pl.Int64),
        pl.col(exit_col).cast(pl.Int64),

        pl.col(entry_col).cast(pl.Datetime("us")).alias("EntryDT"),
        pl.col(exit_col).cast(pl.Datetime("us")).alias("ExitDT"),
        pl.lit(1).alias("delta")
    ])

    events = (
        df2.select([pl.col("EntryDT").alias("ts"), pl.col("delta")])
        .vstack(df2.select([pl.col("ExitDT").alias("ts"), -pl.col("delta")]))
        .sort("ts")
        .with_columns(pl.col("delta").cum_sum().alias("ConcurrentTrades"))
    )

    events = events.filter(pl.col("ConcurrentTrades") > 0)
    arr = events["ConcurrentTrades"].to_numpy()
    arr = arr[np.isfinite(arr)]
    output = {
        'min': int(arr.min()),
        'max': int(arr.max()),
        'mean': f'{arr.mean():.4f}'        
    }
    
    
    return CheckResult("Concurrent positions", "UNIVERSAL", "FETCHED INFO", "Concurrent Positions", output)
