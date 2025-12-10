from typing import Tuple, Dict, Any
import polars as pl
import sys
from io import StringIO
from datetime import datetime
import time
import os
from universal_checks import (
    no_nulls_check,
    non_zero_check,
    no_fractional_check,
    exit_after_entry_check,
    market_hours_check,
    pnl_check,
    entry_exit_price_chain_check,
    no_negatives_check,
    options_expiry_check,
    options_quantity_check,
    duplicate_rows_check
)
from universal_info_checks import (
    concurrent_positions,
)



class Logger:
    def __init__(self, algo_name, log_dir: str = "logs"):
        import os
        self.terminal = sys.stdout
        
        os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = os.path.join(log_dir, f"validation_{algo_name}_{timestamp}.log")
        self.log = open(self.log_file, 'a', encoding='utf-8')
        
        # Write separator for new run
        self.log.write(f"\n{'='*80}\n")
        self.log.write(f"Run started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.log.write(f"{'='*80}\n\n")
        self.log.flush()
    
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()
    
    def flush(self):
        self.terminal.flush()
        self.log.flush()
    
    def close(self):
        self.log.write(f"\n{'='*80}\n")
        self.log.write(f"Run completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.log.write(f"{'='*80}\n")
        self.log.close()


def load_df(path: str) -> pl.DataFrame:
    return pl.read_csv(path)


def build_and_run(trade_log: str, chain_file: str, segment: str, lot_size) -> Tuple[list, Dict[str, Any], Dict[str, Any], pl.DataFrame]:
    df = load_df(trade_log)

    # mimic main.py preprocessing
    df = df.with_row_index("idx")
    df = df.with_columns(
        pl.col("Key").alias("KeyEpoch"),
        pl.col("ExitTime").alias("ExitEpoch"),
    )
    
    try:
        df = df.with_columns(
            pl.col("KeyEpoch").str.to_datetime(strict=False).dt.epoch(),
            pl.col("ExitEpoch").str.to_datetime(strict=False).dt.epoch(),
        )
    except Exception as e:
        print(f"Warning: Error converting datetime for KeyEpoch/ExitEpoch: {e}")
        # Fill NaN values with 0 or handle gracefully
        df = df.with_columns([
            pl.col("KeyEpoch").str.to_datetime(strict=False).dt.epoch().fill_null(0),
            pl.col("ExitEpoch").str.to_datetime(strict=False).dt.epoch().fill_null(0)
        ])
    
    try:
        df = df.with_columns([
            (pl.col("KeyEpoch").cast(pl.Int64))
                .cast(pl.Datetime("us"))
                .dt.offset_by("-5h30m")
                .cast(pl.Int64)
                .alias("KeyEpoch"),

            (pl.col("ExitEpoch").cast(pl.Int64))
                .cast(pl.Datetime("us"))
                .dt.offset_by("-5h30m")
                .cast(pl.Int64)
                .alias("ExitEpoch")
        ])
    except Exception as e:
        print(f"Warning: Error processing epoch timestamps for KeyEpoch/ExitEpoch: {e}")
        # Handle NaN values in epoch columns
        df = df.with_columns([
            pl.when(pl.col("KeyEpoch").is_nan())
                .then(0)
                .otherwise(pl.col("KeyEpoch"))
                .alias("KeyEpoch"),
            pl.when(pl.col("ExitEpoch").is_nan())
                .then(0)
                .otherwise(pl.col("ExitEpoch"))
                .alias("ExitEpoch")
        ])
    
    # Additional validation to ensure no NaN values in KeyEpoch/ExitEpoch
    df = df.with_columns([
        pl.when(pl.col("KeyEpoch").is_nan())
            .then(0)
            .otherwise(pl.col("KeyEpoch"))
            .alias("KeyEpoch"),
        pl.when(pl.col("ExitEpoch").is_nan())
            .then(0)
            .otherwise(pl.col("ExitEpoch"))
            .alias("ExitEpoch")
    ])


    chain_df = None
    if chain_file:
        try:
            chain_df = load_df(chain_file)
        except Exception:
            chain_df = None

    results = []

    results.append(no_nulls_check(df))
    results.append(non_zero_check(df))
    results.append(no_fractional_check(df))
    results.append(exit_after_entry_check(df))
    results.append(market_hours_check(df))
    results.append(pnl_check(df))
    results.append(no_negatives_check(df))
    results.append(duplicate_rows_check(df))
    if chain_df is not None:
        results.append(entry_exit_price_chain_check(df, chain_df))
    if segment == "OPTIONS":
        results.append(options_expiry_check(df))
        results.append(options_quantity_check(df, lot_size))
        
    infos: Dict[str, Any] = {}
    # for fn in (pnl_distribution, trade_duration, concurrent_positions):
    r = concurrent_positions(df)
    infos[r.name] = r.details
    # print(results)
    
    violations = {r.name: r.details for r in results if r.status == "FAIL" and r.details}

    return results, violations, infos, df


def print_summary(results, violations, infos, skip_infos=False):
    # print("=== Validation Summary ===")
    for r in results:
        tag = "PASS" if r.status == "PASS" else ("FAIL" if r.status == "FAIL" else r.status)
        # print(f"[{tag}] {r.name}: {r.message}")

    # print("\nViolations:\n")
    # if not violations:
    #     print("No violations found.")
    # else:
    #     for name, detail in violations.items():
    #         print(f"- {name}: {type(detail)}")

    if not skip_infos:
        print("\nInfo Checks:")
        for k, v in infos.items():
            print(f"- {k}")
            for key, val in v.items():
                print(f'{key.upper()}: {val}')


def generate_violations_report(trade_log: str = "violations.csv", output_file: str = "violations_report.csv"):
    try:
        df = pl.read_csv(trade_log)
        
        # exclude_issues = [
        #     "ConcurrentTradesExceeded",  
        #     "TradesDuration",            
        #     "PnLDistribution"            
        # ]
        
        df_violations = df.filter(~pl.col("IssueType").is_in(exclude_issues))
        
        report_data = df_violations.select([
            pl.col("Key"),
            pl.col("ExitTime"),
            pl.col("Symbol"),
            pl.col("EntryPrice"),
            pl.col("ExitPrice"),
            pl.col("Pnl"),
            pl.col("Quantity"),
            pl.col("IssueType"),
            (pl.col("IssueType") + " - Entry: " + pl.col("Key") + ", Exit: " + pl.col("ExitTime")).alias("Description")
        ]).with_row_index("Trade_ID")
        
        report_df = report_data.select([
            pl.col("Trade_ID"),
            pl.col("Key"),
            pl.col("ExitTime"),
            pl.col("Symbol"),
            pl.col("EntryPrice"),
            pl.col("ExitPrice"),
            pl.col("Pnl"),
            pl.col("Quantity"),
            pl.col("IssueType").str.split(' ').list.get(0),
            pl.col("Description")
        ])
        
        report_df.write_csv(output_file)
        
        if len(df_violations) > 0:
            issue_counts = df_violations.group_by("IssueType").len().sort("len", descending=True)
            print(f"\n=== Violations Report Generated ===")
            print(f"Output file: {output_file}")
            # print(f"Total violations (excluding info checks): {len(df_violations)}")
            print("\nBreakdown by Issue Type:")
            for row in issue_counts.iter_rows(named=True):
                print(f"  {row['IssueType']}: {row['len']}")
        else:
            print(f"\n=== Violations Report Generated ===")
            print(f"Output file: {output_file}")
            print(f"Total violations (excluding info checks): 0")
            print("No violations found after excluding info check related issues.")
        
        return report_df
    except Exception as e:
        print(f"Error generating violations report: {e}")
        return None


def generate_violations_from_checks(results, df_original: pl.DataFrame, algo_name,  output_dir: str = "logs"):
    try:
        violations_indices = set()
        violations_issues = {}  # Maps idx to list of issue types
        violations_severity = {}  # Maps (idx, issue_type) to severity level
        info_check_violations = {}
        errors_count = 0
        warnings_count = 0
        # info_check_names = ["pnl distribution", "check trade duration", "check all concurrent positions"]
        info_check_names = ["check all concurrent positions"]
        for result in results:
            is_info_check = result.name.lower() in info_check_names
            
            if result.status == "FAIL" and isinstance(result.details, dict):
                for issue_type, rows in result.details.items():
                    if isinstance(rows, list) and len(rows) > 1:
                        header = rows[0]  # Get header
                        if 'idx' in header:
                            idx_pos = header.index('idx')
                            # Get severity for this issue type
                            severity = "ERROR"  # Default
                            if result.issue_severity and issue_type in result.issue_severity:
                                severity = result.issue_severity[issue_type]
                            
                            for row_data in rows[1:]:  # Skip header, get data
                                row_idx = row_data[idx_pos]
                                if is_info_check:
                                    info_check_violations[row_idx] = f"{result.name}: {issue_type}"
                                else:
                                    violations_indices.add(row_idx)
                                    # Store all issue types for each row (can have multiple)
                                    if row_idx not in violations_issues:
                                        violations_issues[row_idx] = []
                                    violations_issues[row_idx].append(issue_type)
                                    # Store severity level for this issue
                                    violations_severity[(row_idx, issue_type)] = severity
                                    if severity == "ERROR":
                                        errors_count += 1
                                    else:
                                        warnings_count += 1
        
        if info_check_violations:
            print(f"\n=== Info Check Violations (Console Only) ===")
            for idx, issue_detail in sorted(info_check_violations.items()):
                print(f"  Row {idx}: {issue_detail}")
        
        if violations_indices:
            df_violations = df_original.filter(pl.col("idx").is_in(list(violations_indices)))
            
            # Expand rows: duplicate each row for each issue type
            expanded_rows = []
            for row in df_violations.iter_rows(named=True):
                idx = row["idx"]
                issue_types = violations_issues.get(idx, ["Unknown"])
                for issue_type in issue_types:
                    row_copy = dict(row)
                    row_copy["IssueType"] = issue_type
                    # Get severity level for this issue
                    severity = violations_severity.get((idx, issue_type), "ERROR")
                    row_copy["IssueLevel"] = severity
                    expanded_rows.append(row_copy)
            
            # Create dataframe from expanded rows
            report_df = pl.DataFrame(expanded_rows)
            
            # Write CSV to timestamped file in logs directory
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"violations_report_{algo_name}_{timestamp}.csv")
            # output_file = os.path.join(f"violations_report.csv")
            report_df = report_df.drop(["KeyEpoch", "ExitEpoch"])
            report_df = report_df.unique().sort("idx")

            # import sys 
            # sys.exit()
            report_df.write_csv(output_file)
            # Count individual issue types (not combined ones)
            issue_type_counts = {}
            for idx, issue_list in violations_issues.items():
                for issue_type in issue_list:
                    issue_type = issue_type.upper()
                    if issue_type not in issue_type_counts:
                        issue_type_counts[issue_type] = 0
                    issue_type_counts[issue_type] += 1
            
            print(f"\n=== Violations Report Generated (Excluding Info Checks) ===")
            print(f"Output file: {output_file}")
            # print(f"Total violations found: {len(df_violations)}")
            print("\nBreakdown by Issue Type:")
            
            print(f'ERRORS: {errors_count}')
            print(f'WARNINGS: {warnings_count}')
            # Sort by count descending
            for issue_type, count in sorted(issue_type_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"  {issue_type}: {count}")
            
            return report_df
        else:
            print(f"\n=== Violations Report Generated (Excluding Info Checks) ===")
            print("No violations found (excluding info checks).")
            return None
    except Exception as e:
        print(f"Error generating violations report: {e}")
        import traceback
        traceback.print_exc()
        return None

def main(algo_name, trade_log, options_file, lot_size, segment="UNIVERSAL"):
    ALGO_NAME = algo_name
    TRADE_LOG = trade_log
    OPTION_FILE = options_file
    SEGMENT = segment.upper()
    LOT_SIZE = lot_size
    logger = Logger(algo_name=ALGO_NAME, log_dir="logs")
    sys.stdout = logger
    
    try:
        results, violations, infos, df = build_and_run(trade_log=TRADE_LOG, chain_file=OPTION_FILE, segment=SEGMENT, lot_size=LOT_SIZE)
        print_summary(results, violations, infos, skip_infos=False)
        print("\n" + "="*80)
        generate_violations_from_checks(results, df, algo_name=ALGO_NAME)
        print(f"\n[OK] Console output logged to: {logger.log_file}")
    finally:
        logger.close()
        sys.stdout = sys.__stdout__
        print("[OK] Validation complete. Log saved to: " + logger.log_file)

main("my_algo", "trade_log.csv", "sample_options.csv", 75, "options")