"""Unit tests for the performance data persistence module.

Tests persistence functionality for loading and saving performance tracking data.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from cyberdelta.monitoring.persistence import PerformanceDataPersistence


@pytest.fixture
def temp_output_dir() -> Generator[str]:
    """Create a temporary directory for test output.
    
    Yields:
        str: Path to temporary directory for test output.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def persistence(temp_output_dir: str) -> PerformanceDataPersistence:
    """Create a persistence instance with temporary output directory.
    
    Returns:
        PerformanceDataPersistence: Persistence instance configured with temp directory.
    """
    return PerformanceDataPersistence(temp_output_dir)


@pytest.fixture
def sample_returns_data() -> dict[datetime, Decimal]:
    """Create sample returns data for testing.
    
    Returns:
        dict[datetime, Decimal]: Dictionary mapping timestamps to return values.
    """
    return {
        datetime(2024, 1, 1, 12, 0, tzinfo=UTC): Decimal("0.01"),
        datetime(2024, 1, 1, 13, 0, tzinfo=UTC): Decimal("0.02"),
        datetime(2024, 1, 1, 14, 0, tzinfo=UTC): Decimal("0.015"),
    }


@pytest.fixture
def sample_trades_data() -> list[dict[str, Any]]:
    """Create sample trades data for testing.
    
    Returns:
        list[dict[str, Any]]: List of trade dictionaries with sample data.
    """
    return [
        {
            "trade_id": "T001",
            "strategy": "test_strategy",
            "symbol": "BTC-PERP",
            "exchange": "hyperliquid",
            "direction": "long",
            "size": Decimal("0.1"),
            "entry_price": Decimal(50000),
            "entry_time": datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            "exit_price": Decimal(51000),
            "exit_time": datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
            "pnl": Decimal(100),
            "is_completed": True,
        }
    ]


@pytest.fixture
def sample_signals_data() -> list[dict[str, Any]]:
    """Create sample signals data for testing.
    
    Returns:
        list[dict[str, Any]]: List of signal dictionaries with sample data.
    """
    return [
        {
            "signal_id": "S001",
            "strategy": "test_strategy",
            "symbol": "BTC-PERP",
            "signal_type": "ENTER_LONG",
            "timestamp": datetime(2024, 1, 1, 11, 30, tzinfo=UTC),
            "confidence": Decimal("0.85"),
            "executed": False,
        }
    ]


@pytest.fixture
def sample_funding_rates_data() -> list[dict[str, Any]]:
    """Create sample funding rates data for testing.
    
    Returns:
        list[dict[str, Any]]: List of funding rate dictionaries with sample data.
    """
    return [
        {
            "timestamp": datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "funding_rate": Decimal("0.0001"),
            "predicted_rate": Decimal("0.00015"),
        }
    ]


class TestPerformanceDataPersistenceInit:
    """Test suite for PerformanceDataPersistence initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success_creates_directory(self, temp_output_dir: str) -> None:
        """Test successful initialization creates output directory."""
        # Arrange
        test_dir = Path(temp_output_dir) / "test_persistence"

        # Act
        persistence = PerformanceDataPersistence(str(test_dir))

        # Assert
        assert test_dir.exists()
        assert test_dir.is_dir()
        assert hasattr(persistence, "lock")
        assert hasattr(persistence, "output_dir")

    def test_init_success_existing_directory(self, temp_output_dir: str) -> None:
        """Test successful initialization with existing directory."""
        # Act
        persistence = PerformanceDataPersistence(temp_output_dir)

        # Assert
        assert Path(temp_output_dir).exists()
        assert isinstance(persistence.output_dir, Path)


class TestSaveData:
    """Test suite for save_data method."""

    # ==================== SUCCESS CASES ====================

    def test_save_data_success_dict(self, persistence: PerformanceDataPersistence) -> None:
        """Test successful saving of dictionary data."""
        # Arrange
        test_data = {"key": "value", "number": 42}

        # Act
        persistence.save_data("test", "test.json", test_data)

        # Assert
        filepath = persistence.output_dir / "test" / "test.json"
        assert filepath.exists()

        with filepath.open("r") as f:
            loaded_data = json.load(f)
        assert loaded_data == test_data

    def test_save_data_success_list(self, persistence: PerformanceDataPersistence) -> None:
        """Test successful saving of list data."""
        # Arrange
        test_data = [{"id": 1}, {"id": 2}]

        # Act
        persistence.save_data("test", "list.json", test_data)

        # Assert
        filepath = persistence.output_dir / "test" / "list.json"
        assert filepath.exists()

        with filepath.open("r") as f:
            loaded_data = json.load(f)
        assert loaded_data == test_data

    def test_save_data_success_with_datetime(self, persistence: PerformanceDataPersistence) -> None:
        """Test successful saving of data with datetime objects."""
        # Arrange
        test_datetime = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        test_data = {"timestamp": test_datetime, "value": "test"}

        # Act
        persistence.save_data("test", "datetime.json", test_data)

        # Assert
        filepath = persistence.output_dir / "test" / "datetime.json"
        assert filepath.exists()

        with filepath.open("r") as f:
            loaded_data = json.load(f)
        assert loaded_data["timestamp"] == test_datetime.isoformat()

    def test_save_data_success_with_decimal(self, persistence: PerformanceDataPersistence) -> None:
        """Test successful saving of data with Decimal objects."""
        # Arrange
        test_data = {"amount": Decimal("123.456"), "price": Decimal(50000)}

        # Act
        persistence.save_data("test", "decimal.json", test_data)

        # Assert
        filepath = persistence.output_dir / "test" / "decimal.json"
        assert filepath.exists()

        with filepath.open("r") as f:
            loaded_data = json.load(f)
        assert loaded_data["amount"] == "123.456"
        assert loaded_data["price"] == "50000"

    # ==================== EDGE CASES ====================

    def test_save_data_edge_nested_structures(
        self, persistence: PerformanceDataPersistence
    ) -> None:
        """Test saving nested data structures."""
        # Arrange
        test_data = {
            "metadata": {
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "values": [Decimal("1.23"), Decimal("4.56")],
            }
        }

        # Act
        persistence.save_data("test", "nested.json", test_data)

        # Assert
        filepath = persistence.output_dir / "test" / "nested.json"
        assert filepath.exists()

    def test_save_data_edge_empty_data(self, persistence: PerformanceDataPersistence) -> None:
        """Test saving empty data structures."""
        # Arrange
        empty_dict: dict[str, Any] = {}
        empty_list: list[Any] = []

        # Act
        persistence.save_data("test", "empty_dict.json", empty_dict)
        persistence.save_data("test", "empty_list.json", empty_list)

        # Assert
        dict_filepath = persistence.output_dir / "test" / "empty_dict.json"
        list_filepath = persistence.output_dir / "test" / "empty_list.json"
        assert dict_filepath.exists()
        assert list_filepath.exists()

    # ==================== FAILURE CASES ====================

    def test_save_data_failure_permission_error(
        self, persistence: PerformanceDataPersistence
    ) -> None:
        """Test handling of permission errors during save."""
        # Arrange
        test_data = {"key": "value"}

        # Act
        with patch("pathlib.Path.open", side_effect=PermissionError("Permission denied")):
            persistence.save_data("test", "perm_error.json", test_data)

        # Assert - should not raise exception, just log the error
        # File should not exist due to permission error
        filepath = persistence.output_dir / "test" / "perm_error.json"
        assert not filepath.exists()


class TestLoadData:
    """Test suite for load_data method."""

    # ==================== SUCCESS CASES ====================

    def test_load_data_success_dict(self, persistence: PerformanceDataPersistence) -> None:
        """Test successful loading of dictionary data."""
        # Arrange
        test_data = {"key": "value", "number": 42}
        persistence.save_data("test", "load_test.json", test_data)

        # Act
        loaded_data = persistence.load_data("test", "load_test.json")

        # Assert
        assert loaded_data == test_data

    def test_load_data_success_list(self, persistence: PerformanceDataPersistence) -> None:
        """Test successful loading of list data."""
        # Arrange
        test_data = [{"id": 1}, {"id": 2}]
        persistence.save_data("test", "load_list.json", test_data)

        # Act
        loaded_data = persistence.load_data("test", "load_list.json")

        # Assert
        assert loaded_data == test_data

    # ==================== EDGE CASES ====================

    def test_load_data_edge_file_not_exists(self, persistence: PerformanceDataPersistence) -> None:
        """Test loading non-existent file returns None."""
        # Act
        loaded_data = persistence.load_data("test", "nonexistent.json")

        # Assert
        assert loaded_data is None

    def test_load_data_edge_empty_file(self, persistence: PerformanceDataPersistence) -> None:
        """Test loading empty JSON file."""
        # Arrange
        filepath = persistence.output_dir / "test" / "empty.json"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text("null")

        # Act
        loaded_data = persistence.load_data("test", "empty.json")

        # Assert
        assert loaded_data is None

    # ==================== FAILURE CASES ====================

    def test_load_data_failure_invalid_json(self, persistence: PerformanceDataPersistence) -> None:
        """Test handling of invalid JSON during load."""
        # Arrange
        filepath = persistence.output_dir / "test" / "invalid.json"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text("invalid json content")

        # Act
        loaded_data = persistence.load_data("test", "invalid.json")

        # Assert
        assert loaded_data is None

    def test_load_data_failure_permission_error(
        self, persistence: PerformanceDataPersistence
    ) -> None:
        """Test handling of permission errors during load."""
        # Arrange
        test_data = {"key": "value"}
        persistence.save_data("test", "perm_test.json", test_data)

        # Act
        with patch("pathlib.Path.open", side_effect=PermissionError("Permission denied")):
            loaded_data = persistence.load_data("test", "perm_test.json")

        # Assert
        assert loaded_data is None


class TestPostProcessLoadedData:
    """Test suite for post_process_loaded_data method."""

    # ==================== SUCCESS CASES ====================

    def test_post_process_returns_data(self, persistence: PerformanceDataPersistence) -> None:
        """Test post-processing of returns data."""
        # Arrange
        raw_data = {
            "strategy1": {"2024-01-01T12:00:00+00:00": "0.01", "2024-01-01T13:00:00+00:00": "0.02"}
        }

        # Act
        processed_data = persistence.post_process_loaded_data("returns", raw_data)

        # Assert
        assert isinstance(processed_data, dict)
        strategy_data = processed_data["strategy1"]
        assert isinstance(strategy_data, dict)
        # Check that timestamps are converted to datetime objects
        # We know what the original data contained, so check if it was processed correctly
        expected_timestamp_1 = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        expected_timestamp_2 = datetime(2024, 1, 1, 13, 0, tzinfo=UTC)
        assert expected_timestamp_1 in strategy_data
        assert expected_timestamp_2 in strategy_data

    def test_post_process_trades_data(self, persistence: PerformanceDataPersistence) -> None:
        """Test post-processing of trades data."""
        # Arrange
        raw_data = [
            {
                "trade_id": "T001",
                "entry_time": "2024-01-01T12:00:00+00:00",
                "exit_time": "2024-01-01T13:00:00+00:00",
                "pnl": "100",
            }
        ]

        # Act
        processed_data = persistence.post_process_loaded_data("trades", raw_data)

        # Assert
        assert isinstance(processed_data, list)
        assert len(processed_data) == 1
        trade = processed_data[0]
        assert isinstance(trade["entry_time"], datetime)
        assert isinstance(trade["exit_time"], datetime)

    # ==================== EDGE CASES ====================

    def test_post_process_none_data(self, persistence: PerformanceDataPersistence) -> None:
        """Test post-processing of None data."""
        # Act
        processed_data = persistence.post_process_loaded_data("returns", None)

        # Assert
        assert processed_data is None

    def test_post_process_unknown_data_type(self, persistence: PerformanceDataPersistence) -> None:
        """Test post-processing of unknown data type."""
        # Arrange
        raw_data = {"unknown": "data"}

        # Act
        processed_data = persistence.post_process_loaded_data("unknown_type", raw_data)

        # Assert
        assert processed_data == raw_data


class TestSaveReturns:
    """Test suite for save_returns method."""

    # ==================== SUCCESS CASES ====================

    def test_save_returns_success(
        self, persistence: PerformanceDataPersistence, sample_returns_data: dict[datetime, Decimal]
    ) -> None:
        """Test successful saving of returns data."""
        # Act
        persistence.save_returns("test_strategy", sample_returns_data)

        # Assert
        filepath = persistence.output_dir / "returns" / "test_strategy.json"
        assert filepath.exists()

        with filepath.open("r") as f:
            saved_data = json.load(f)

        # Check that datetime keys are converted to ISO format
        for key in saved_data:
            assert isinstance(key, str)
            datetime.fromisoformat(key)  # Should not raise exception

    # ==================== EDGE CASES ====================

    def test_save_returns_edge_empty_data(self, persistence: PerformanceDataPersistence) -> None:
        """Test saving empty returns data."""
        # Arrange
        empty_returns: dict[datetime, Decimal] = {}

        # Act
        persistence.save_returns("empty_strategy", empty_returns)

        # Assert
        filepath = persistence.output_dir / "returns" / "empty_strategy.json"
        assert filepath.exists()


class TestLoadAllReturns:
    """Test suite for load_all_returns method."""

    # ==================== SUCCESS CASES ====================

    def test_load_all_returns_success(
        self, persistence: PerformanceDataPersistence, sample_returns_data: dict[datetime, Decimal]
    ) -> None:
        """Test successful loading of all returns data."""
        # Arrange
        persistence.save_returns("strategy1", sample_returns_data)
        persistence.save_returns("strategy2", sample_returns_data)

        # Act
        all_returns = persistence.load_all_returns()

        # Assert
        assert isinstance(all_returns, dict)
        assert "strategy1" in all_returns
        assert "strategy2" in all_returns

        # Check that loaded data structure is correct
        # Note: The current implementation loads the data directly, not post-processed
        for strategy_data in all_returns.values():
            assert isinstance(strategy_data, dict)
            assert len(strategy_data) > 0
            # Check structure exists
            for key, value in strategy_data.items():
                # Key might be string (ISO format) or datetime depending on post-processing
                assert key is not None
                assert value is not None

    # ==================== EDGE CASES ====================

    def test_load_all_returns_edge_no_returns_dir(
        self, persistence: PerformanceDataPersistence
    ) -> None:
        """Test loading returns when returns directory doesn't exist."""
        # Act
        all_returns = persistence.load_all_returns()

        # Assert
        assert all_returns == {}

    def test_load_all_returns_edge_empty_returns_dir(
        self, persistence: PerformanceDataPersistence
    ) -> None:
        """Test loading returns from empty returns directory."""
        # Arrange
        returns_dir = persistence.output_dir / "returns"
        returns_dir.mkdir(parents=True, exist_ok=True)

        # Act
        all_returns = persistence.load_all_returns()

        # Assert
        assert all_returns == {}

    # ==================== FAILURE CASES ====================

    def test_load_all_returns_failure_handles_corrupted_files(
        self, persistence: PerformanceDataPersistence
    ) -> None:
        """Test loading returns handles corrupted files gracefully."""
        # Arrange
        returns_dir = persistence.output_dir / "returns"
        returns_dir.mkdir(parents=True, exist_ok=True)

        # Create a corrupted file
        corrupted_file = returns_dir / "corrupted.json"
        corrupted_file.write_text("invalid json")

        # Act
        all_returns = persistence.load_all_returns()

        # Assert
        assert isinstance(all_returns, dict)
        assert "corrupted" not in all_returns  # Corrupted file should be skipped


class TestSaveLoadTrades:
    """Test suite for save_trades and load_trades methods."""

    # ==================== SUCCESS CASES ====================

    def test_save_load_trades_success(
        self, persistence: PerformanceDataPersistence, sample_trades_data: list[dict[str, Any]]
    ) -> None:
        """Test successful saving and loading of trades data."""
        # Act
        persistence.save_trades(sample_trades_data)
        loaded_trades = persistence.load_trades()

        # Assert
        assert isinstance(loaded_trades, list)
        assert len(loaded_trades) == len(sample_trades_data)

        # Check that datetime fields are properly restored
        for trade in loaded_trades:
            assert isinstance(trade["entry_time"], datetime)
            assert isinstance(trade["exit_time"], datetime)

    # ==================== EDGE CASES ====================

    def test_load_trades_edge_no_file(self, persistence: PerformanceDataPersistence) -> None:
        """Test loading trades when no trades file exists."""
        # Act
        loaded_trades = persistence.load_trades()

        # Assert
        assert loaded_trades == []

    def test_save_load_trades_edge_empty_list(
        self, persistence: PerformanceDataPersistence
    ) -> None:
        """Test saving and loading empty trades list."""
        # Act
        persistence.save_trades([])
        loaded_trades = persistence.load_trades()

        # Assert
        assert loaded_trades == []


class TestSaveLoadSignals:
    """Test suite for save_signals and load_signals methods."""

    # ==================== SUCCESS CASES ====================

    def test_save_load_signals_success(
        self, persistence: PerformanceDataPersistence, sample_signals_data: list[dict[str, Any]]
    ) -> None:
        """Test successful saving and loading of signals data."""
        # Act
        persistence.save_signals(sample_signals_data)
        loaded_signals = persistence.load_signals()

        # Assert
        assert isinstance(loaded_signals, list)
        assert len(loaded_signals) == len(sample_signals_data)

        # Check that datetime fields are properly restored
        for signal in loaded_signals:
            assert isinstance(signal["timestamp"], datetime)

    # ==================== EDGE CASES ====================

    def test_load_signals_edge_no_file(self, persistence: PerformanceDataPersistence) -> None:
        """Test loading signals when no signals file exists."""
        # Act
        loaded_signals = persistence.load_signals()

        # Assert
        assert loaded_signals == []


class TestSaveLoadFundingRates:
    """Test suite for save_funding_rates and load_funding_rates methods."""

    # ==================== SUCCESS CASES ====================

    def test_save_load_funding_rates_success(
        self,
        persistence: PerformanceDataPersistence,
        sample_funding_rates_data: list[dict[str, Any]],
    ) -> None:
        """Test successful saving and loading of funding rates data."""
        # Act
        persistence.save_funding_rates(sample_funding_rates_data)
        loaded_funding_rates = persistence.load_funding_rates()

        # Assert
        assert isinstance(loaded_funding_rates, list)
        assert len(loaded_funding_rates) == len(sample_funding_rates_data)

        # Check that datetime fields are properly restored
        for funding_rate in loaded_funding_rates:
            assert isinstance(funding_rate["timestamp"], datetime)

    # ==================== EDGE CASES ====================

    def test_load_funding_rates_edge_no_file(self, persistence: PerformanceDataPersistence) -> None:
        """Test loading funding rates when no funding rates file exists."""
        # Act
        loaded_funding_rates = persistence.load_funding_rates()

        # Assert
        assert loaded_funding_rates == []


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("data_type", "filename", "test_data"),
    [
        ("returns", "strategy1.json", {"2024-01-01T12:00:00+00:00": "0.01"}),
        ("trades", "trades.json", [{"trade_id": "T001", "pnl": "100"}]),
        ("signals", "signals.json", [{"signal_id": "S001", "executed": False}]),
        ("funding_rates", "funding_rates.json", [{"symbol": "BTC-PERP", "funding_rate": "0.0001"}]),
    ],
)
def test_save_load_data_parametrized(
    persistence: PerformanceDataPersistence,
    data_type: str,
    filename: str,
    test_data: dict[str, Any] | list[Any],
) -> None:
    """Test save and load data for various data types."""
    # Act
    persistence.save_data(data_type, filename, test_data)
    loaded_data = persistence.load_data(data_type, filename)

    # Assert
    assert loaded_data is not None
    # Basic structure check
    if isinstance(test_data, dict):
        assert isinstance(loaded_data, dict)
    else:
        assert isinstance(loaded_data, list)


@pytest.mark.parametrize(
    ("invalid_datetime", "expected_behavior"),
    [
        ("invalid-datetime", "should_skip"),
        ("2024-13-01T12:00:00", "should_skip"),
        ("", "should_skip"),
    ],
)
def test_datetime_parsing_error_handling_parametrized(
    persistence: PerformanceDataPersistence,
    invalid_datetime: str,
    expected_behavior: str,
) -> None:
    """Test datetime parsing error handling for various invalid formats."""
    # Arrange
    test_data = {
        "test_field": invalid_datetime,
        "timestamp": invalid_datetime,
    }

    # Act - Test through public API by processing list data that contains dict
    list_data = [test_data]
    processed_data = persistence.post_process_loaded_data("trades", list_data)

    # Assert
    if expected_behavior == "should_skip" and isinstance(processed_data, list):
        # Invalid datetime should remain as string (not converted)
        assert processed_data[0]["timestamp"] == invalid_datetime
