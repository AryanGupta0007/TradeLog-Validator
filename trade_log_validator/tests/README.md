# Test Suite Documentation

## Overview
This directory contains comprehensive test cases for all validation checks in the `universal_checks.py` module. The test suite includes 47 test cases covering all check functions with various scenarios including edge cases, boundary conditions, and error cases.

## Test Structure

### Test Files
- **test_checks.py** - Main test suite with all check function tests
- **conftest.py** - Pytest configuration and shared fixtures

## Test Coverage

### 1. No Nulls Check (`TestNoNullsCheck`) - 4 tests
- ✅ No nulls present (PASS case)
- ✅ Null in single column
- ✅ Multiple nulls across columns
- ✅ Null in critical PnL column

### 2. Non-Zero Check (`TestNonZeroCheck`) - 7 tests
- ✅ No zeros present (PASS case)
- ✅ Zero in Quantity column
- ✅ Zero in EntryPrice column
- ✅ Zero in ExitPrice column
- ✅ Zero in PositionStatus column
- ✅ Zero in Pnl column
- ✅ Multiple zeros in different columns

### 3. No Fractional Check (`TestNoFractionalCheck`) - 4 tests
- ✅ No fractional values (PASS case)
- ✅ Fractional quantity (10.5)
- ✅ Fractional position status (1.5)
- ✅ Multiple fractional values

### 4. No Negatives Check (`TestNoNegativesCheck`) - 5 tests
- ✅ No negative values (PASS case)
- ✅ Negative quantity
- ✅ Negative entry price
- ✅ Negative exit price
- ✅ Multiple negative values

### 5. Exit After Entry Check (`TestExitAfterEntryCheck`) - 4 tests
- ✅ Valid exit after entry (PASS case)
- ✅ Exit before entry
- ✅ Exit at same time as entry (valid)
- ✅ Multiple trades with exit before entry

### 6. Market Hours Check (`TestMarketHoursCheck`) - 5 tests
- ✅ Trades within market hours (PASS case)
- ✅ Entry before market hours (before 09:15)
- ✅ Entry after market hours (after 15:25)
- ✅ Exit before market hours
- ✅ Exit after market hours

### 7. PnL Check (`TestPnlCheck`) - 5 tests
- ✅ Valid PnL (PASS case)
- ✅ PnL mismatch detection
- ✅ Positive PnL with stoploss exit
- ✅ Negative PnL with target exit
- ✅ Short position PnL calculation

### 8. Entry/Exit Price Chain Check (`TestEntryExitPriceChainCheck`) - 5 tests
- ✅ Valid chain prices
- ✅ Entry price not found in chain
- ✅ Exit price not found in chain
- ✅ Entry price mismatch
- ✅ Exit price mismatch

### 9. Check Result Structure (`TestCheckResultStructure`) - 3 tests
- ✅ Required fields validation
- ✅ Status value validation (PASS/FAIL)
- ✅ Details format on failure

### 10. Edge Cases (`TestEdgeCases`) - 5 tests
- ✅ Empty DataFrame handling
- ✅ Single row DataFrame
- ✅ Large DataFrame (1000 rows)
- ✅ Very small floating point differences
- ✅ Mixed valid and invalid rows

## Running Tests

### Run all tests
```bash
python -m pytest tests/test_checks.py -v
```

### Run specific test class
```bash
python -m pytest tests/test_checks.py::TestNoNullsCheck -v
```

### Run specific test
```bash
python -m pytest tests/test_checks.py::TestNoNullsCheck::test_no_nulls_pass -v
```

### Run with coverage report
```bash
python -m pytest tests/test_checks.py --cov=universal_checks --cov-report=html
```

### Run with detailed output
```bash
python -m pytest tests/test_checks.py -vv --tb=long
```

## Test Fixtures

### Fixtures provided in conftest.py

#### `sample_trade_df`
A valid sample trade DataFrame with 3 rows containing:
- Valid entry/exit times
- Positive and negative trades (long and short positions)
- Valid PnL calculations

#### `empty_trade_df`
An empty trade DataFrame with all required columns but no data.

#### `sample_chain_df`
Sample chain data with matching timestamps and prices.

## Test Data Factory

The `TestDataFactory` class provides helper methods:

### `create_base_df(num_rows=5)`
Creates a valid baseline DataFrame with specified number of rows. All rows have:
- Valid timestamps (entry < exit)
- Within market hours
- Correct PnL calculations
- No null values
- No zeros in critical columns
- No fractional values

## Test Results Summary

```
Total Tests: 47
Passed: 47 ✅
Failed: 0 ✅
Skipped: 0
Time: ~1.0 second
```

## Coverage Areas

✅ **Happy Path Testing** - All checks with valid data
✅ **Error Detection** - Each violation type caught correctly
✅ **Boundary Testing** - Edge cases like empty DataFrames
✅ **Data Validation** - Null, zero, fractional, negative values
✅ **Business Logic** - PnL calculations, market hours, exit ordering
✅ **Integration** - Chain data validation with timestamps

## Key Test Scenarios

### 1. Data Integrity
- Null values in any column
- Zero values in critical columns
- Fractional values in integer columns
- Negative values where not allowed

### 2. Business Rules
- Exit must be after entry (or at same time)
- Trades must be within market hours (09:15 - 15:25)
- PnL must match calculated value
- Exit reason must match PnL direction

### 3. Price Validation
- Entry price must exist in chain data
- Exit price must exist in chain data
- Prices must match within tolerance

### 4. Data Scale
- Handles empty datasets
- Handles single row
- Handles 1000+ rows efficiently

## Maintenance

When adding new checks:
1. Create a new test class inheriting from check test pattern
2. Add PASS case test
3. Add FAIL case tests for each violation type
4. Add edge case tests if applicable
5. Update this README with new test class documentation

## Dependencies

- pytest >= 8.0
- pytest-cov >= 6.0
- polars >= latest
- pandas >= latest

## Notes

- Tests use Polars DataFrames to match production code
- All timestamps are in standard date format: DD-MM-YYYY HH:MM
- Epoch times are in microseconds (consistent with production data)
- Market hours: 09:15 - 15:25 IST
- All tolerance checks use 1e-4 for floating point comparisons
