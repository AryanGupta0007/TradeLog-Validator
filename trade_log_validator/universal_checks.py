import polars as pl
import pandas as pd
# from .result import CheckResult
from result import CheckResult


def no_nulls_check(df: pl.DataFrame) -> CheckResult:
    issues = {}
    result_name = f'Nulls'
    issues[result_name] = [
        ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
            'Quantity', 'PositionStatus', 'Pnl', 'ExitType')
    ]

    for col in df.columns:
        if col in ['KeyEpoch', 'ExitEpoch']:
            continue
        rows = df.filter(pl.col(col).is_null()).rows()
        for row in rows:
            issues[result_name].append(row)

    has_issues = any(len(v) > 1 for v in issues.values())
    if has_issues:
        severity = {result_name: "ERROR"}
        return CheckResult("No Nulls", "UNIVERSAL", "FAIL", "Nulls detected", issues, severity)

    return CheckResult("No Nulls", "UNIVERSAL", "PASS", "No nulls found")


def non_zero_check(df: pl.DataFrame) -> CheckResult:
    cols = ['PositionStatus', 'Quantity', 'EntryPrice', 'ExitPrice', 'Pnl']
    issues = {}
    result_name = "Zeros" 
    for col in cols:
        issues[result_name] = [
            ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
             'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')
        ]

    for c in cols:
        rows = df.filter(pl.col(c) == 0).rows()
        if rows:
            for row in rows:
                issues[result_name].append(row)

    has_issue = any(len(v) > 1 for v in issues.values())
    if has_issue:
        severity = {result_name: "ERROR"}
        return CheckResult("NON ZERO CHECKS", "UNIVERSAL", "FAIL", "Zero values detected", issues, severity)

    return CheckResult("NON ZERO CHECKS", "UNIVERSAL", "PASS", "No zeros detected")


def no_fractional_check(df: pl.DataFrame) -> CheckResult:
    cols = ['PositionStatus', 'Quantity']
    issues = {}
    result_name = 'Fractional Value'
    issues[result_name] = [
        ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
            'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')
    ]

    for c in cols:
        rows = df.filter((pl.col(c) - pl.col(c).floor()) > 0).rows()
        if rows:
            for row in rows:
                issues[result_name].append(row)

    has_issues = any(len(v) > 1 for v in issues.values())
    if has_issues:
        severity = {result_name: "ERROR"}
        return CheckResult("FRACTIONAL VALUES", "UNIVERSAL", "FAIL", "Fractional values detected", issues, severity)

    return CheckResult("FRACTIONAL VALUES", "UNIVERSAL", "PASS", "No fractional values detected")

def no_negatives_check(df: pl.DataFrame) -> CheckResult:
    cols=['Quantity','EntryPrice','ExitPrice']
    issues = {}
    result_name = 'Negatives'
    issues[result_name] = [
        ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
            'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')
    ]

    for c in cols:
        rows = df.filter(pl.col(c) < 0).rows()
        if rows:
            for row in rows:
                issues[result_name].append(row)

    has_issues = any(len(v) > 1 for v in issues.values())
    if has_issues:
        severity = {result_name: "ERROR"}
        return CheckResult("NEGATIVE VALUES", "UNIVERSAL", "FAIL", "Negative values detected", issues, severity)

    return CheckResult("NEGATIVE VALUES", "UNIVERSAL", "PASS", "No Negative values detected")


def exit_after_entry_check(df: pl.DataFrame) -> CheckResult:
    result_name = "Exit < Entry"
    issues = {
        result_name: [
            ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
             'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')
        ]
    }
    rows = df.filter(pl.col("ExitEpoch") < pl.col("KeyEpoch")).rows()

    if rows:
        for row in rows:
            issues[result_name].append(row)
        severity = {result_name: "ERROR"}
        return CheckResult("EXIT TI > ENTRY", "UNIVERSAL", "FAIL", "Exit before entry detected", issues, severity)

    return CheckResult("EXIT TI > ENTRY", "UNIVERSAL", "PASS", "All trades have valid entry/exit ordering")


def market_hours_check(df: pl.DataFrame) -> CheckResult:
    df2 = df.with_columns(
        pl.col("Key").str.to_datetime(strict=False),
        pl.col("ExitTime").str.to_datetime(strict=False)
    )
    issues = {}
    result_name = "OUTSIDE MARKET HOURS"
    # keys = ["Entry before Market Hours", "Exit after Market Hours", "Entry before Market Hours", "Exit after Market Hours"]
    # for key in keys:
    issues[result_name] = [
        ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
            'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')
    ]

    entry_early = df2.filter(pl.col("Key").dt.time() < pl.time(9, 15)).rows()
    entry_late = df2.filter(pl.col("Key").dt.time() > pl.time(15, 25)).rows()
    exit_early = df2.filter(pl.col("ExitTime").dt.time() < pl.time(9, 15)).rows()
    exit_late = df2.filter(pl.col("ExitTime").dt.time() > pl.time(15, 25)).rows()

    if entry_early:
        for row in entry_early:
            issues[result_name].append(row)
    if exit_early:
        for row in exit_early:
            issues[result_name].append(row)
    if entry_late:
        for row in entry_late:
            issues[result_name].append(row)
    if exit_late:
        for row in exit_late:
            issues[result_name].append(row)

    has_issues = any(len(v) > 1 for v in issues.values())
    if has_issues:
        severity = {result_name: "ERROR"}
        return CheckResult("TRADING OUTSIDE MARKET HOURS", "UNIVERSAL", "FAIL", "Market hour violations", issues, severity)

    return CheckResult("TRADING OUTSIDE MARKET HOURS", "UNIVERSAL", "PASS", "All trades within market hours")


def pnl_check(df: pl.DataFrame) -> CheckResult:
    result_name = ["PnL", "Pnl"]
    issues = {}
    for res in result_name:
        issues[res] = [('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice', 'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch', 'ExitTag', 'ExpectedPnl')]
            

    df2 = df.with_columns([
        pl.when(pl.col("ExitType").str.contains("Target")).then(pl.lit("+") )
        .when(pl.col("ExitType").str.contains("Stoploss")).then(pl.lit("-")).otherwise(pl.lit("")).alias("ExitTag"),
        ((pl.col('Quantity') * (pl.col('ExitPrice') - pl.col('EntryPrice'))) * pl.col('PositionStatus')).alias('ExpectedPnl')
    ])

    pnl_mismatch_rows = df2.filter(pl.col('ExpectedPnl') - pl.col('Pnl') > 1e-4).rows()
    reason_mismatch_rows = df2.filter(
        ((pl.col('ExitTag') == '+') & (pl.col('Pnl') < 0))
        | ((pl.col('ExitTag') == '-') & (pl.col('Pnl') > 0))
    ).rows()

    for row in pnl_mismatch_rows:
        issues['Pnl'].append(row)
    for row in reason_mismatch_rows:
        issues['PnL'].append(row)

    has_issues = any(len(v) > 1 for v in issues.values())
    if has_issues:
        severity = {
            'Pnl': "ERROR",
            'PnL': "WARNING"
        }
        return CheckResult("Pnl Validation", "UNIVERSAL", "FAIL", "PnL mismatches detected", issues, severity)
    
    return CheckResult("Pnl Validation", "UNIVERSAL", "PASS", "PnL validation passed")


def entry_exit_price_chain_check(df: pl.DataFrame, chain_df: pl.DataFrame) -> CheckResult:
    # chain_df expected to be polars; convert to pandas frame and index by ti,sym with column 'c'
    try:
        cdf = chain_df.to_pandas()
        chain = cdf.set_index(["ti", "sym"]).sort_index()
    except Exception:
        chain = None

    def _get_price(t, sym):
        try:
            row = chain.loc[(t, sym)]
            return row.iloc[-1]["c"]
        except Exception:
            return None

    pdf = df.to_pandas()
    result_name = "LTP"
    issues = {}
    issues[result_name] = [('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice', 'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')]
    # issues = {
    #     "Entry Price Not Found": [('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice', 'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')],
    #     "Exit Price Not Found": [('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice', 'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')],
    #     "Entry Price Mismatch": [("idx", "EntryTime", "EntryPrice", "ExpectedPrice", "Symbol", "ti", "EntryTimeEpoch")],
    #     "Exit Price Mismatch": [("idx", "ExitTime", "ExitPrice", "ExpectedPrice", "Symbol", "ti", "ExitTimeEpoch")]
    # }

    for idx, row in pdf.iterrows():
        # Handle NaN values in KeyEpoch and ExitEpoch
        try:
            key_epoch = row["KeyEpoch"]
            exit_epoch = row["ExitEpoch"]
            
            # Skip rows with NaN values
            if pd.isna(key_epoch) or pd.isna(exit_epoch):
                issues[result_name].append(row)
                continue
            
            entry_time = int((key_epoch / 1e6) - 60)
            exit_time = int((exit_epoch / 1e6) - 60)
        except (ValueError, TypeError) as e:
            # If we can't convert epoch times, mark as issue and skip
            issues[result_name].append(row)
            continue
        
        # print(entry_time, row['Symbol'], exit_time)
        # import sys    
        # sys.exit()
        
        entry = _get_price(entry_time, row["Symbol"]) if chain is not None else None
        exitp = _get_price(exit_time, row["Symbol"]) if chain is not None else None

        if entry is None or float(entry) != float(row['EntryPrice']) or exitp is None or float(exitp) != float(row['ExitPrice']):
            issues[result_name].append(row)
        
        
    has_issues = any(len(v) > 1 for v in issues.values())
    if has_issues:
        severity = {result_name: "ERROR"}
        return CheckResult("LTP VALIDATION", "UNIVERSAL", "FAIL", "Chain entry/exit mismatches detected", issues, severity)

    return CheckResult("LTP VALIDATION", "UNIVERSAL", "PASS", "Entry/Exit chain prices consistent")

def options_expiry_check(df: pl.DataFrame)->CheckResult:
    result_name = "Exit After Expiry"
    issues = {
        result_name: [
            ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
            'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')
        ]
    }
    df = df.with_columns(
        pl.col("Symbol").str.slice(5, 7).alias("Expiry"),
    )
    df = df.with_columns(
        pl.col("Expiry").str.strptime(pl.Date, "%d%b%y", strict=False).add(pl.duration(days=1)).alias("Expiry"),        
        pl.col("ExitEpoch").cast(pl.Datetime)
    )
    

    rows = df.filter(
        pl.col("ExitEpoch") > pl.col("Expiry")
    ).rows()
    for row in rows:
        issues[result_name].append(row) 
    has_issues = any(len(v) > 1 for k, v in issues.items())
    if has_issues:
        severity = {"Exit After Expiry": "ERROR"}
        return CheckResult(
            "option_expiry_check",
            "OPTIONS",
            "FAIL",
            "Exit After expiry",
            issues,
            severity
        )

    return CheckResult("option_expiry_check",
            "OPTIONS",
            "PASS", 
            "Exit After expiry"
            )

import re

def extract_symbol(sym):
    pattern = r"(.+?)(?=\d{1,2}[A-Z]{3}\d{2})"
    match = re.match(pattern, sym)
    return match.group(1) if match else None

def options_quantity_check(df: pl.DataFrame, lot_size_df)->CheckResult:
    result_name = ["QTY", "SYMBOL"]
    issues = {}
    for res in result_name:
        issues[res] = [
                ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
                'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')
            ]

    pdf = df.to_pandas()
    for idx, row in pdf.iterrows():
        try:
            sym = extract_symbol(row["Symbol"])
        except Exception as e:
            print(e)
            print(f"ERROR RETRIEVING SYMBOL {idx}, {sym}")
        # print(sym)
        else:
            size = lot_size_df.filter(pl.col('Symbol') == sym)["LotSize"]
            if len(size) > 0:
                size = size[0]
                if size != row["Quantity"]:
                    issues["QTY"].append(row)    
            else:
                issues["SYMBOL"].append(row)
                
    has_issues = any(len(v)  > 1 for k, v in issues.items())
    if has_issues:
        severity = {
            "QTY": "ERROR",
            "SYMBOL": "ERROR"
        }
        return CheckResult("option_quantity_check", "OPTIONS", "FAIL", "Wrong Quantity detected", issues, severity)
    return CheckResult("option_quantity_check", "OPTIONS", "PASS", "No Wrong Quantity detected")

def duplicate_rows_check(df: pl.DataFrame) -> CheckResult:
    result_name = "DUPLICATES"

    issues = {
        result_name: [
            ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
             'Quantity', 'PositionStatus', 'Pnl', 'ExitType', 'KeyEpoch', 'ExitEpoch')
        ]
    }

    # ---- CRITICAL FIX: remove idx column when checking ----
    dup_cols = [c for c in df.columns if c != "idx"]

    dup_df = (
        df
        .group_by(dup_cols)
        .agg(pl.count().alias("cnt"))
        .filter(pl.col("cnt") > 1)
        .drop("cnt")
    )

    # Join back to get original rows including idx
    dup_rows_df = df.join(dup_df, on=dup_cols, how="inner")

    for row in dup_rows_df.rows():
        issues[result_name].append(row)

    has_issues = len(issues[result_name]) > 1

    if has_issues:
        severity = { result_name: "ERROR" }
        return CheckResult(
            "duplicate_rows_check",
            "GENERAL",
            "FAIL",
            "Duplicate rows detected",
            issues,
            severity
        )

    return CheckResult(
        "duplicate_rows_check",
        "GENERAL",
        "PASS",
        "No duplicate rows detected"
    )
