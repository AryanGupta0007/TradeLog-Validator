"""
Pytest configuration and shared fixtures
"""
import pytest
import polars as pl
import os
import sys

# Add parent directory to path so tests can import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sample_trade_df():
    """Fixture providing a sample valid trade DataFrame"""
    return pl.DataFrame({
        "idx": [0, 1, 2],
        "Key": ["01-01-2021 09:30", "01-01-2021 10:00", "01-01-2021 11:00"],
        "ExitTime": ["01-01-2021 10:30", "01-01-2021 11:30", "01-01-2021 12:30"],
        "Symbol": ["NIFTY", "NIFTY", "BANKNIFTY"],
        "EntryPrice": [100.0, 110.0, 200.0],
        "ExitPrice": [105.0, 108.0, 205.0],
        "Quantity": [10.0, 20.0, 5.0],
        "PositionStatus": [1.0, 1.0, -1.0],
        "Pnl": [50.0, -40.0, 25.0],
        "ExitType": ["Target Hit", "Stoploss Hit", "Target Hit"],
        "KeyEpoch": [1609472400000000, 1609476000000000, 1609479600000000],
        "ExitEpoch": [1609476000000000, 1609479600000000, 1609483200000000],
        "ExitTag": ["+", "-", "+"],
        "ExpectedPnl": [50.0, -40.0, 25.0],
    })


@pytest.fixture
def empty_trade_df():
    """Fixture providing an empty trade DataFrame"""
    return pl.DataFrame({
        "idx": [],
        "Key": [],
        "ExitTime": [],
        "Symbol": [],
        "EntryPrice": [],
        "ExitPrice": [],
        "Quantity": [],
        "PositionStatus": [],
        "Pnl": [],
        "ExitType": [],
        "KeyEpoch": [],
        "ExitEpoch": [],
        "ExitTag": [],
        "ExpectedPnl": [],
    })


@pytest.fixture
def sample_chain_df():
    """Fixture providing sample chain data"""
    return pl.DataFrame({
        "ti": [1609472340, 1609475940, 1609479540],
        "sym": ["NIFTY", "NIFTY", "BANKNIFTY"],
        "c": [100.0, 105.0, 205.0],
    })
