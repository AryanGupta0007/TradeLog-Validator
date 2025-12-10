# Low-Level Design — Validator Module

- **Repository:** Top-level project at `validator_final`
- **Primary runtime:** Python (Polars used for DataFrame processing)
- **Key files:**
  - `functional_main.py` — orchestrator: loads data, runs checks/info functions, generates reports.
  - `universal_checks.py` — validation checks (fail/pass + detailed per-row issues).
  - `universal_info_checks.py` — informational metrics (non-fail summaries).
  - `result.py` — `CheckResult` dataclass.
  - `logs/` — generated violation reports (CSV) and run logs.
  - `tests/` — unit tests for checks (see `tests/test_checks.py`).

## Design goals

- Centralized checks return structured results (`CheckResult`) so `functional_main.py` can uniformly process outcomes.
- Detailed issues attach headers + rows per check to enable per-trade violation reporting.
- Information-style checks (non-failing metrics) are returned separately and printed to console / included in the `infos` map.
- Severity classification (ERROR/WARNING) is included for issue types and flows through to the output CSV as `IssueLevel`.

## Core Data Structures

- `CheckResult` (in `result.py`):
  - Fields:
    - `name: str` — human-readable name of the check (e.g., "No Nulls", "Pnl Validation")
    - `segment: str` — domain or group (e.g., "UNIVERSAL")
    - `status: str` — "PASS", "FAIL", or informational statuses like "FETCHED INFO"
    - `message: str` — short message describing the result
    - `details: Any` — typically a dict mapping issue type → list (header row + data rows)
    - `issue_severity: dict` — mapping issue type (keys matching `details`) to severity value: `"ERROR"` or `"WARNING"`
  - Example: `CheckResult("Pnl Validation","UNIVERSAL","FAIL","...", details, {"Pnl":"ERROR","Pnl (Warning)":"WARNING"})`

- Polars DataFrame shape:
  - Input trade log is a CSV loaded with `pl.read_csv`.
  - The main processing augments the DataFrame with `idx` (row index), `KeyEpoch` and `ExitEpoch` (epoch datetime conversions), and uses other columns like `Key`, `ExitTime`, `Symbol`, `EntryPrice`, `ExitPrice`, `Quantity`, `PositionStatus`, `Pnl`, `ExitType`.

## Data flow / high-level steps (in `functional_main.build_and_run`)

1. Load trade log CSV (via `load_df`)
2. Add `idx` row index column
3. Copy `Key` → `KeyEpoch`, `ExitTime` → `ExitEpoch`, attempt to parse to epoch datetimes
4. Adjust timezone offsets and coerce missing epoch values to safe defaults
5. Optionally load chain file (`chain_df`) used by LTP validation
6. Assemble `results` list by calling check functions in `universal_checks.py` and info functions from `universal_info_checks.py`
7. Build `violations` mapping from `results` for `status == "FAIL"` and then write detailed report rows with `generate_violations_from_checks`
8. Generate per-run log and per-violation CSV with `IssueType` and `IssueLevel` columns

## Detailed checks (`universal_checks.py`)

Note: Each check returns a `CheckResult`. `details` is a dict: issue_type_name -> list, where the first element is a header tuple and subsequent elements are rows (tuples or pandas Series rows). `issue_severity` maps issue_type_name -> "ERROR"/"WARNING".

- `no_nulls_check(df)`
  # Low-Level Design — Validator Module

  Version: 1.2

  Repository root: `validator_final`

  Overview
  --------
  This document provides an in-depth description of the validator implemented in this repository. It covers the code structure, the data model, every error check and information metric (their algorithms and edge-cases), how results are represented and propagated, and precise, step-by-step instructions for adding new checks or info functions.

  Conventions and assumptions
  ---------------------------
  - Input is a tabular trade log (CSV) with columns such as `Key`, `ExitTime`, `Symbol`, `EntryPrice`, `ExitPrice`, `Quantity`, `PositionStatus`, `Pnl`, `ExitType`.
  - The orchestrator will add an `idx` column (row index). Many checks rely on `idx` to map back to original rows.
  - `KeyEpoch` and `ExitEpoch` are epoch timestamps derived from `Key` and `ExitTime`. The code attempts robust parsing but missing values should be reported by `no_nulls_check`.
  - `details` in `CheckResult` follows a strict format: dict(issue_type -> list) where list[0] is a header tuple including `'idx'`; list[1:] are data rows matching that header.

  Repository structure and purpose of files
  ----------------------------------------
  - `functional_main.py` — entrypoint and orchestrator. Responsibilities:
    - Load trade CSV and optional chain file.
    - Preprocess and normalize timestamps and index columns.
    - Execute checks (errors/warnings) and info functions.
    - Generate violation CSVs with `IssueType` and `IssueLevel` fields.
    - Log execution details in `logs/`.

  - `universal_checks.py` — contains deterministic, row-level validation functions that detect data quality or business-rule violations. Each check returns a `CheckResult`.

  - `universal_info_checks.py` — returns aggregated metrics (not considered violations) as `CheckResult` with `status='FETCHED INFO'`.

  - `result.py` — defines the `CheckResult` dataclass used for uniform results propagation.

  - `tests/` — tests for checks and helpers.

  Overview — How the system works
  --------------------------------
  1. `functional_main.build_and_run()` loads the trade CSV and creates a Polars DataFrame.
  2. It adds an integer row index column named `idx` and attempts to convert `Key` and `ExitTime` into epoch timestamps stored in `KeyEpoch` and `ExitEpoch`.
  3. Checks in `universal_checks.py` are run in sequence; each returns a `CheckResult` (PASS/FAIL or informational). Info functions in `universal_info_checks.py` are also called and their results stored in an `infos` dictionary.
  4. `generate_violations_from_checks()` consumes FAILing `CheckResult` objects with `details` dicts and produces a per-trade CSV report. Each violation row contains `IssueType` and `IssueLevel` (ERROR/WARNING).

  Core data representations
  --------------------------
  Polars DataFrame (trade log)
  - Required/expected columns: `Key`, `ExitTime`, `Symbol`, `EntryPrice`, `ExitPrice`, `Quantity`, `PositionStatus`, `Pnl`, `ExitType`.
  - After preprocessing: `idx` (row index), `KeyEpoch`, `ExitEpoch` (both int epoch values; may be converted back to datetimes as needed).

  CheckResult dataclass (in `result.py`)
  - Fields:
    - `name: str` — human-friendly check name
    - `segment: str` — logical category (eg. `UNIVERSAL`)
    - `status: str` — typically `PASS`, `FAIL`, or `FETCHED INFO`
    - `message: str` — short summary
    - `details: Any` — usually a dict mapping `issue_type` -> `list` (first element is header tuple, subsequent elements are rows)
    - `issue_severity: dict` — mapping `issue_type` -> severity string (`ERROR` or `WARNING`). If not present, the report generator defaults to `ERROR` for that issue.

  Standard `details` format for validation checks
  - `details` is a dictionary. Each key is a named issue type (string). The value is a list where:
    - index 0 is a header tuple that MUST include `'idx'` as the first or one of the header columns (this is required by `generate_violations_from_checks`).
    - subsequent entries are rows corresponding to the header columns; rows can be tuples or pandas Series but keeping them as tuples makes downstream processing robust.

  Example `details` shape:

  ```
  {
    "Pnl": [
     ("idx","Key","ExitTime","Symbol","EntryPrice","ExitPrice","Quantity","PositionStatus","Pnl","ExitType"),
     (0, "01-01-2021 09:17", "08-01-2021 14:15", ...),
     (5, ...)
    ],
    "Pnl (Warning)": [ header_tuple, row_tuple, ... ]
  }
  ```

  Design of `generate_violations_from_checks`
  - Iterates all `CheckResult` objects whose `status == 'FAIL'`.
  - For each issue type in `result.details` it finds the `idx` column index from the header and collects all offending `idx` values.
  - For each offending `idx` it duplicates the original trade row once per issue type and appends `IssueType` and `IssueLevel` columns.
  - `IssueLevel` is taken from `result.issue_severity[issue_type]` if present, otherwise defaults to `ERROR`.

  Detailed, in-depth checks (file: `universal_checks.py`)
  ------------------------------------------------------
  Below are the checks implemented, with reasoning, algorithm, expected inputs and outputs, edge cases, and examples.

  1) No Nulls — `no_nulls_check(df)`
    - Purpose: ensure no important columns are null/missing. Nulls often indicate parsing or data feed problems.
    - Algorithm: iterate DataFrame columns (skipping `KeyEpoch` and `ExitEpoch`), use Polars `is_null()` to find rows. For each null row append that row to `issues['Nulls']`.
    - `details` key: `"Nulls"`.
    - `issue_severity`: `{"Nulls":"ERROR"}`.
    - Edge cases: row-level nulls in non-critical columns could be acceptable — if you need column-level severity, split into multiple issue types.

  2) Non Zero Checks — `non_zero_check(df)`
    - Purpose: ensure critical numeric fields are not zero (indicates missing or invalid values).
    - Columns checked: `PositionStatus`, `Quantity`, `EntryPrice`, `ExitPrice`, `Pnl`.
    - Algorithm: filter rows where any of these columns == 0; append them to `issues['Zeros']`.
    - `issue_severity`: `{"Zeros":"ERROR"}`.
    - Notes: zero `Pnl` may be legitimate in some strategies — if so, reclassify as `WARNING` or add a rule exception.

  3) Fractional Values — `no_fractional_check(df)`
    - Purpose: ensure integer-only columns (e.g., `Quantity`, `PositionStatus`) do not have fractional components.
    - Algorithm: check `(col - col.floor()) > 0` for each column; append offending rows to `issues['Fractional Value']`.
    - `issue_severity`: `{"Fractional Value":"ERROR"}`.
     - Note: [This note has been removed for clarity.]

  4) Negatives — `no_negatives_check(df)`
    - Purpose: detect negative values where they are invalid (e.g., negative `EntryPrice`).
    - Columns: `Quantity`, `EntryPrice`, `ExitPrice`.
    - Algorithm: filter where col < 0.
    - `issue_severity`: `{"Negatives":"ERROR"}`.

  5) Exit After Entry — `exit_after_entry_check(df)`
    - Purpose: ensure trade exit timestamps occur after entry timestamps.
    - Algorithm: find rows where `ExitEpoch < KeyEpoch` and append to `issues['Exit < Entry']`.
    - `issue_severity`: `{"Exit < Entry":"ERROR"}`.
    - Edge cases: missing or zero epochs — such rows should be validated by `no_nulls_check` first. `build_and_run` coerces missing epochs to 0; the check treats those as invalid ordering.

  6) Market Hours — `market_hours_check(df)`
    - Purpose: flag trades entering or exiting outside expected market hours (configured in code as 09:15–15:25 local time).
    - Algorithm: convert `Key` and `ExitTime` to datetimes and compare `.dt.time()` against thresholds.
    - `issue_severity`: `{"OUTSIDE MARKET HOURS":"ERROR"}`.
    - Edge cases: overnight products, different exchange hours — make the hours configurable if supporting multiple instruments.

  7) PnL Validation — `pnl_check(df)`
    - Purpose: detect inconsistencies between reported `Pnl` and expected value from `Quantity`, `EntryPrice`, `ExitPrice` and `PositionStatus`. Also detect cases where `ExitType` suggests a reason (Target/Stoploss) but the `Pnl` sign contradicts it; these are surfaced as warnings.
    - Algorithm:
      1. Add an `ExitTag` column: `'+'` for exit types containing `Target`, `'-'` for exit types containing `Stoploss`, else `''`.
      2. Compute `ExpectedPnl = Quantity * (ExitPrice - EntryPrice) * PositionStatus`.
      3. `Pnl` mismatch: if `ExpectedPnl - Pnl > tolerance` (tolerance = 1e-4) then append row to `issues['Pnl']`.
      4. `Reason mismatch`: if `ExitTag == '+'` and `Pnl < 0` or `ExitTag == '-'` and `Pnl > 0` then append row to `issues['Pnl (Warning)']`.
    - Severity mapping:
      - `"Pnl": "ERROR"`
      - `"Pnl (Warning)": "WARNING"`
    - Notes: Floating point rounding may cause tiny diffs — keep the tolerance carefully chosen. If the system reports PnL using different conventions (fees, slippage, commissions), adjust the expected formula accordingly.

  8) LTP / Chain Price Validation — `entry_exit_price_chain_check(df, chain_df)`
    - Purpose: ensure the recorded `EntryPrice` and `ExitPrice` match an external reference price feed at or near the trade timestamps.
    - Inputs:
      - `chain_df` is expected to be a Polars DataFrame convertible to pandas with rows indexed by `(ti, sym)` and a price column `c`.
    - Algorithm:
      1. Build a pandas `chain` DataFrame indexed by `(ti, sym)`.
      2. For each trade, compute `entry_time = int((KeyEpoch/1e6) - 60)` and `exit_time` similarly (a 60-second offset is applied in the code).
      3. Fetch LTP for `entry_time` and `exit_time` and compare to `EntryPrice`/`ExitPrice`.
      4. If missing prices or mismatch, append the row to `issues['LTP']`.
    - `issue_severity`: `{"LTP":"ERROR"}`.
    - Notes: The code uses a fixed 60-second offset — change if your chain timestamps use different alignment.

  Info functions (`universal_info_checks.py`)
  -------------------------------------------------
  Info functions are not violations by default; they return `CheckResult` objects with `status='FETCHED INFO'` and `details` as a simple dict of computed statistics. These are stored in `infos` and printed to the console.

  1) `pnl_distribution(df)`
    - Purpose: basic PnL metrics (mean/max/min).
    - Input: `df['Pnl']` → numpy array.
    - Output: formatted strings in `details`.

  2) `trade_duration(df)`
    - Purpose: compute trade durations in days.
    - Algorithm: create `EntryDT` and `ExitDT` from scaled epoch columns, compute `(ExitDT - EntryDT).total_seconds()` and convert to days.
    - Output: mean, max, min in `details` (string representation in days).

  3) `concurrent_positions(df)`
    - Purpose: compute concurrent open positions over time.
    - Algorithm: create event stream with entry `+1` at `EntryDT` and exit `-1` at `ExitDT`, sort, cumulative sum to get concurrent counts; then compute min/max/mean.
    - Output: numeric min/max/mean in `details`.

  ## How to add a new check (detailed, checklist-style)
  1. Choose file: row-level checks -> `universal_checks.py`; info-only -> `universal_info_checks.py`.
  2. Follow function signature:

  ```python
  def my_new_check(df: pl.DataFrame) -> CheckResult:
     """One-line summary.

     Longer explanation: why this check exists, expected inputs, and known edge cases.
     """

     result_name = "My Issue Name"  # used as IssueType in reports
     issues = { result_name: [
        ('idx', 'Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice', 'Quantity', 'PositionStatus', 'Pnl', 'ExitType')
     ] }

     # compute offending rows using Polars
     rows = df.filter(<your condition here>).rows()
     for r in rows:
        issues[result_name].append(r)

     if any(len(v) > 1 for v in issues.values()):
        severity = {result_name: 'ERROR'}  # or 'WARNING'
        return CheckResult('My Check Friendly', 'UNIVERSAL', 'FAIL', 'Short description', issues, severity)

     return CheckResult('My Check Friendly', 'UNIVERSAL', 'PASS', 'All OK')
  ```

  3. Important conventions:
    - Header tuple must include `'idx'` so the report generator can map rows to `df_original`.
    - Keep the header order stable and consistent with the row tuples you append.
    - Use `issue_severity` to classify each issue type; the report generator will use it to set `IssueLevel`.

  4. Hook the check into the orchestrator:
    - Import the new function at the top of `functional_main.py`.
    - Add `results.append(my_new_check(df))` in the same sequence as other checks.

  5. Unit tests:
    - Add test cases to `tests/test_checks.py` or create a new test file under `tests/`.
    - Test both PASS and FAIL cases and assert the generated `details` structure and `issue_severity` mapping.

  Example: adding an `Abnormal PnL` check (detailed code with comments)

  ```python
  def abnormal_pnl_check(df: pl.DataFrame) -> CheckResult:
     """Flag trades where absolute PnL is above a threshold.

     Rationale: extremely large PnL values may indicate data corruption or missed adjustments.
     """
     threshold = 1_000_000
     name = 'Abnormal PnL'
     issues = { name: [('idx', 'Key', 'ExitTime', 'Symbol', 'Pnl')] }

     rows = df.filter(pl.col('Pnl').abs() > threshold).rows()
     for r in rows:
        issues[name].append(r)

     if any(len(v) > 1 for v in issues.values()):
        severity = { name: 'ERROR' }
        return CheckResult('Abnormal PnL', 'UNIVERSAL', 'FAIL', f'PnL > {threshold}', issues, severity)
     return CheckResult('Abnormal PnL', 'UNIVERSAL', 'PASS', 'No abnormal pnl')
  ```

  Report generation and the `IssueLevel` column
  ---------------------------------------------
  - `generate_violations_from_checks()` builds the output rows by joining `violations_issues` (collected from `CheckResult.details`) with the original `df_original` by `idx`.
  - Each row in the final CSV includes an `IssueType` column (string) and `IssueLevel` which is the severity string (`ERROR`/`WARNING`).


  Output generation and storage
  -----------------------------

  - **Logger / console output:** The tool creates a run-level log file in the `logs/` directory using the `Logger` helper in `functional_main.py`. The logger writes a timestamped file named `validation_<algo_name>_<YYYYMMDD_HHMMSS>.log` and also mirrors console output to this file for easy run review.

  - **Violations CSVs:** Violations are written by `generate_violations_from_checks(results, df_original, algo_name, output_dir='logs')`. The function:
    - Collects all failing `CheckResult` objects and expands each offending trade (row) with `IssueType` and `IssueLevel` columns.
    - Drops internal epoch columns (`KeyEpoch`, `ExitEpoch`) and de-duplicates rows.
    - Writes a timestamped CSV under `logs/` named `violations_report_<algo_name>_<YYYYMMDD_HHMMSS>.csv`.
    - Prints a summary to the console including total errors/warnings and a breakdown by issue type.

  - **Ad-hoc violations report generator:** There is also `generate_violations_report(trade_log, output_file)` which can post-process a standalone `violations.csv` into a human-readable `violations_report.csv` (or another chosen `output_file`). This function selects a useful subset of columns and adds a `Description` column for quick context.

  - **File locations & naming conventions:**
    - Log files: `logs/validation_<algo_name>_<timestamp>.log`
    - Violations CSVs: `logs/violations_report_<algo_name>_<timestamp>.csv`
    - Ad-hoc report output: as provided to `generate_violations_report`, e.g. `violations_report.csv`

  - **Format details:**
    - The violations CSV contains the original trade columns plus `IssueType` and `IssueLevel` (severity). `IssueType` values are taken from `CheckResult.details` keys and are upper-cased for summary counts.
    - `IssueLevel` is taken from `CheckResult.issue_severity` if provided, otherwise it defaults to `ERROR`.

  - **How to regenerate or inspect outputs:**
    - Run the validator (example):

  ```bash
  python functional_main.py
  ```

    - Look in `logs/` for the generated `validation_*.log` and `violations_report_*.csv` files.


  Testing and validation
  ----------------------
  - Unit tests should assert:
    - The correct set of rows are returned in `details` (use synthetic small DataFrames).
    - `issue_severity` contains expected mappings.
    - `generate_violations_from_checks` produces a DataFrame with `IssueType` and `IssueLevel` set correctly.

  Operational commands
  --------------------
  To run the validator and produce a violations report:

  ```bash
  cd "C:/Users/aryan/OneDrive/Desktop/WORK__/validator_final"
  python functional_main.py
  ```

  To run tests:

  ```bash
  python -m pytest tests -q
  ```

  To regenerate this PDF (requires `reportlab`):

  ```bash
  python -m pip install --user reportlab
  python scripts/generate_pdf.py
  ```

  

  Appendix: quick reference for developers
  --------------------------------------
  - If you implement a new row-level check, verify header contains `'idx'`.
  - Prefer Polars expressions for performance: avoid row-by-row Python loops when possible.
  - When in doubt, add an integration test that runs `build_and_run` with a small `trade_log.csv` fixture and asserts the generated `report_df` contains expected `IssueLevel` values.

  ---

  Generated: December 10, 2025 (revised)
