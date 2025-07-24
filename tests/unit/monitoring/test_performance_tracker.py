"""Unit tests for the performance tracker module.

Tests strategy performance tracking functionality including trades, signals, and returns.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from cyberdelta.monitoring.performance_tracker import PerformanceTracker


@pytest.fixture
def temp_output_dir() -> Generator[str]:
    """Create a temporary directory for test output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def performance_tracker(temp_output_dir: str) -> PerformanceTracker:
    """Create a performance tracker instance with temporary output directory."""
    return PerformanceTracker(output_dir=temp_output_dir)


@pytest.fixture
def sample_trade() -> dict[str, Any]:
    """Create sample trade data for testing."""
    return {
        "trade_id": "TEST001",
        "strategy_name": "test_strategy",
        "symbol": "BTC-PERP",
        "exchange": "hyperliquid",
        "direction": "long",
        "size": Decimal("0.1"),
        "entry_price": Decimal(50000),
        "entry_time": datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        "exit_price": Decimal(51000),
        "exit_time": datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
        "pnl": Decimal(100),
        "metadata": {"slippage": Decimal("0.01"), "fees": Decimal(10)},
    }


@pytest.fixture
def sample_signal() -> dict[str, Any]:
    """Create sample signal data for testing."""
    return {
        "signal_id": "SIG001",
        "strategy_name": "test_strategy",
        "symbol": "BTC-PERP",
        "signal_type": "ENTER_LONG",
        "timestamp": datetime(2024, 1, 1, 11, 30, tzinfo=UTC),
        "confidence": Decimal("0.85"),
        "metadata": {"indicator": "RSI"},
    }


class TestPerformanceTrackerInit:
    """Test suite for PerformanceTracker initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success_with_output_dir(self, temp_output_dir: str) -> None:
        """Test successful initialization with specified output directory."""
        # Act
        tracker = PerformanceTracker(output_dir=temp_output_dir)

        # Assert
        assert hasattr(tracker, "persistence")
        assert isinstance(tracker.returns, dict)
        assert isinstance(tracker.trades, list)
        assert isinstance(tracker.signals, list)
        assert isinstance(tracker.funding_rates, list)
        assert hasattr(tracker, "lock")

    def test_init_success_default_output_dir(self) -> None:
        """Test successful initialization with default output directory."""
        # Act
        with patch(
            "cyberdelta.monitoring.performance_tracker.PerformanceDataPersistence"
        ) as mock_persistence:
            # Configure the mock to return empty data structures
            mock_instance = mock_persistence.return_value
            mock_instance.load_all_returns.return_value = {}
            mock_instance.load_trades.return_value = []
            mock_instance.load_signals.return_value = []
            mock_instance.load_funding_rates.return_value = []

            tracker = PerformanceTracker()

        # Assert
        mock_persistence.assert_called_once_with("./performance_data")
        assert isinstance(tracker.returns, dict)

    # ==================== EDGE CASES ====================

    def test_init_edge_none_output_dir(self) -> None:
        """Test initialization with None output directory uses default."""
        # Act
        with patch(
            "cyberdelta.monitoring.performance_tracker.PerformanceDataPersistence"
        ) as mock_persistence:
            # Configure the mock to return empty data structures
            mock_instance = mock_persistence.return_value
            mock_instance.load_all_returns.return_value = {}
            mock_instance.load_trades.return_value = []
            mock_instance.load_signals.return_value = []
            mock_instance.load_funding_rates.return_value = []

            tracker = PerformanceTracker(output_dir=None)

        # Assert
        mock_persistence.assert_called_once_with("./performance_data")
        assert tracker is not None

    def test_init_edge_loads_existing_data(self, temp_output_dir: str) -> None:
        """Test initialization loads existing data files."""
        # Arrange - create existing data files
        returns_dir = Path(temp_output_dir) / "returns"
        returns_dir.mkdir(parents=True, exist_ok=True)

        # Create a returns file for a strategy
        returns_data = {datetime.now(UTC).isoformat(): str(Decimal("0.05"))}
        with (returns_dir / "test_strategy.json").open("w", encoding="utf-8") as f:
            json.dump(returns_data, f)

        # Act
        tracker = PerformanceTracker(output_dir=temp_output_dir)

        # Assert - data is loaded into memory
        assert "test_strategy" in tracker.returns
        assert len(tracker.returns["test_strategy"]) > 0


class TestTrackReturn:
    """Test suite for track_return method."""

    # ==================== SUCCESS CASES ====================

    def test_track_return_success_new_strategy(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test tracking return for a new strategy."""
        # Arrange
        strategy_name = "new_strategy"
        timestamp = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        return_value = Decimal("0.05")

        # Act
        with patch.object(performance_tracker.persistence, "save_returns") as mock_save:
            performance_tracker.track_return(strategy_name, timestamp, return_value)

        # Assert
        assert strategy_name in performance_tracker.returns
        assert performance_tracker.returns[strategy_name][timestamp] == return_value
        mock_save.assert_called_once_with(strategy_name, {timestamp: return_value})

    def test_track_return_success_existing_strategy(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test tracking return for an existing strategy."""
        # Arrange
        strategy_name = "existing_strategy"
        timestamp1 = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        timestamp2 = datetime(2024, 1, 1, 13, 0, tzinfo=UTC)
        return1 = Decimal("0.03")
        return2 = Decimal("0.04")

        # First return
        performance_tracker.track_return(strategy_name, timestamp1, return1)

        # Act - Second return
        with patch.object(performance_tracker.persistence, "save_returns") as mock_save:
            performance_tracker.track_return(strategy_name, timestamp2, return2)

        # Assert
        assert len(performance_tracker.returns[strategy_name]) == 2
        assert performance_tracker.returns[strategy_name][timestamp2] == return2
        expected_returns = {timestamp1: return1, timestamp2: return2}
        mock_save.assert_called_once_with(strategy_name, expected_returns)

    # ==================== EDGE CASES ====================

    def test_track_return_edge_zero_return(self, performance_tracker: PerformanceTracker) -> None:
        """Test tracking zero return value."""
        # Arrange
        strategy_name = "zero_return_strategy"
        timestamp = datetime.now(UTC)
        return_value = Decimal(0)

        # Act
        performance_tracker.track_return(strategy_name, timestamp, return_value)

        # Assert
        assert performance_tracker.returns[strategy_name][timestamp] == Decimal(0)

    def test_track_return_edge_negative_return(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test tracking negative return value."""
        # Arrange
        strategy_name = "loss_strategy"
        timestamp = datetime.now(UTC)
        return_value = Decimal("-0.05")

        # Act
        performance_tracker.track_return(strategy_name, timestamp, return_value)

        # Assert
        assert performance_tracker.returns[strategy_name][timestamp] == Decimal("-0.05")

    # ==================== FAILURE CASES ====================

    def test_track_return_failure_persistence_error(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test handling of persistence save error."""
        # Arrange
        strategy_name = "error_strategy"
        timestamp = datetime.now(UTC)
        return_value = Decimal("0.05")

        # Act
        with (
            patch.object(
                performance_tracker.persistence, "save_returns", side_effect=Exception("Save error")
            ),
            pytest.raises(Exception, match="Save error"),
        ):
            performance_tracker.track_return(strategy_name, timestamp, return_value)


class TestTrackTrade:
    """Test suite for track_trade method."""

    # ==================== SUCCESS CASES ====================

    def test_track_trade_success_complete_trade(
        self, performance_tracker: PerformanceTracker, sample_trade: dict[str, Any]
    ) -> None:
        """Test tracking a complete trade with all fields."""
        # Act
        with patch.object(performance_tracker.persistence, "save_trades") as mock_save:
            performance_tracker.track_trade(**sample_trade)

        # Assert
        assert len(performance_tracker.trades) == 1
        trade = performance_tracker.trades[0]
        assert trade["trade_id"] == sample_trade["trade_id"]
        assert trade["strategy"] == sample_trade["strategy_name"]  # Note: stored as "strategy"
        assert trade["pnl"] == sample_trade["pnl"]
        assert trade["is_completed"] is True
        mock_save.assert_called_once()

    def test_track_trade_success_open_trade(self, performance_tracker: PerformanceTracker) -> None:
        """Test tracking an open trade without exit details."""
        # Arrange
        open_trade: dict[str, Any] = {
            "trade_id": "OPEN001",
            "strategy_name": "test_strategy",
            "symbol": "ETH-PERP",
            "exchange": "hyperliquid",
            "direction": "short",
            "size": Decimal("1.0"),
            "entry_price": Decimal(3000),
            "entry_time": datetime.now(UTC),
        }

        # Act
        performance_tracker.track_trade(**open_trade)

        # Assert
        assert len(performance_tracker.trades) == 1
        trade = performance_tracker.trades[0]
        assert trade["exit_price"] is None
        assert trade["exit_time"] is None
        assert trade["pnl"] is None
        assert trade["is_completed"] is False
        assert trade["duration"] is None

    # ==================== EDGE CASES ====================

    def test_track_trade_edge_zero_pnl(self, performance_tracker: PerformanceTracker) -> None:
        """Test tracking trade with zero PnL."""
        # Arrange
        entry_time = datetime.now(UTC)
        exit_time = entry_time + timedelta(hours=1)
        breakeven_trade: dict[str, Any] = {
            "trade_id": "BREAK001",
            "strategy_name": "test_strategy",
            "symbol": "BTC-PERP",
            "exchange": "hyperliquid",
            "direction": "long",
            "size": Decimal("0.1"),
            "entry_price": Decimal(50000),
            "entry_time": entry_time,
            "exit_price": Decimal(50000),
            "exit_time": exit_time,
            "pnl": Decimal(0),
        }

        # Act
        performance_tracker.track_trade(**breakeven_trade)

        # Assert
        assert performance_tracker.trades[0]["pnl"] == Decimal(0)
        # Duration should be calculated from entry to exit time (1 hour = 60 minutes)
        expected_duration = (exit_time - entry_time).total_seconds() / 60
        assert performance_tracker.trades[0]["duration"] == expected_duration

    def test_track_trade_edge_update_existing(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test updating an existing trade."""
        # Arrange
        trade_id = "UPDATE001"
        initial_trade: dict[str, Any] = {
            "trade_id": trade_id,
            "strategy_name": "test_strategy",
            "symbol": "BTC-PERP",
            "exchange": "hyperliquid",
            "direction": "long",
            "size": Decimal("0.1"),
            "entry_price": Decimal(50000),
            "entry_time": datetime.now(UTC),
        }

        # Track initial trade
        performance_tracker.track_trade(**initial_trade)

        # Update with exit info
        updated_trade: dict[str, Any] = {
            **initial_trade,
            "exit_price": Decimal(51000),
            "exit_time": datetime.now(UTC) + timedelta(hours=2),
            "pnl": Decimal(100),
        }

        # Act
        performance_tracker.track_trade(**updated_trade)

        # Assert
        assert len(performance_tracker.trades) == 1  # Same trade, not duplicated
        assert performance_tracker.trades[0]["is_completed"] is True
        assert performance_tracker.trades[0]["pnl"] == Decimal(100)


class TestTrackTradeExit:
    """Test suite for track_trade_exit method."""

    # ==================== SUCCESS CASES ====================

    def test_track_trade_exit_success(self, performance_tracker: PerformanceTracker) -> None:
        """Test tracking trade exit successfully."""
        # Arrange - create an open trade
        trade_id = "EXIT001"
        entry_time = datetime.now(UTC)
        performance_tracker.track_trade(
            trade_id=trade_id,
            strategy_name="test_strategy",
            symbol="BTC-PERP",
            exchange="hyperliquid",
            direction="long",
            size=Decimal("0.1"),
            entry_price=Decimal(50000),
            entry_time=entry_time,
        )

        # Act
        exit_time = entry_time + timedelta(hours=1)
        with patch.object(performance_tracker, "track_return") as mock_track_return:
            performance_tracker.track_trade_exit(
                trade_id=trade_id,
                exit_price=Decimal(51000),
                exit_time=exit_time,
                pnl=Decimal(100),
            )

        # Assert
        trade = performance_tracker.trades[0]
        assert trade["is_completed"] is True
        assert trade["exit_price"] == Decimal(51000)
        assert trade["pnl"] == Decimal(100)
        assert trade["duration"] == 60.0  # 60 minutes
        # Check that return was tracked
        mock_track_return.assert_called_once()

    # ==================== EDGE CASES ====================

    def test_track_trade_exit_edge_with_metadata(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test tracking trade exit with metadata."""
        # Arrange
        trade_id = "META001"
        performance_tracker.track_trade(
            trade_id=trade_id,
            strategy_name="test_strategy",
            symbol="ETH-PERP",
            exchange="hyperliquid",
            direction="short",
            size=Decimal("1.0"),
            entry_price=Decimal(3000),
            entry_time=datetime.now(UTC),
        )

        # Act
        performance_tracker.track_trade_exit(
            trade_id=trade_id,
            exit_price=Decimal(2900),
            exit_time=datetime.now(UTC),
            pnl=Decimal(100),
            metadata={"slippage": Decimal(5), "exit_reason": "stop_loss"},
        )

        # Assert
        assert performance_tracker.trades[0]["metadata"]["exit_reason"] == "stop_loss"

    # ==================== FAILURE CASES ====================

    def test_track_trade_exit_failure_trade_not_found(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test tracking exit for non-existent trade."""
        # Act & Assert
        with patch("structlog.get_logger") as mock_logger:
            mock_logger.return_value.warning = MagicMock()
            performance_tracker.track_trade_exit(
                trade_id="NONEXISTENT",
                exit_price=Decimal(51000),
                exit_time=datetime.now(UTC),
                pnl=Decimal(100),
            )
            # Logger warning should be called but no exception raised


class TestTrackSignal:
    """Test suite for track_signal method."""

    # ==================== SUCCESS CASES ====================

    def test_track_signal_success_entry_signal(
        self, performance_tracker: PerformanceTracker, sample_signal: dict[str, Any]
    ) -> None:
        """Test tracking an entry signal."""
        # Act
        with patch.object(performance_tracker.persistence, "save_signals") as mock_save:
            performance_tracker.track_signal(**sample_signal)

        # Assert
        assert len(performance_tracker.signals) == 1
        signal = performance_tracker.signals[0]
        assert signal["signal_id"] == sample_signal["signal_id"]
        assert signal["strategy"] == sample_signal["strategy_name"]  # Stored as "strategy"
        assert signal["signal_type"] == "ENTER_LONG"
        assert signal["executed"] is False  # Default state
        mock_save.assert_called_once()

    def test_track_signal_success_exit_signal(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test tracking an exit signal."""
        # Arrange
        exit_signal: dict[str, Any] = {
            "signal_id": "EXIT001",
            "strategy_name": "test_strategy",
            "symbol": "BTC-PERP",
            "signal_type": "EXIT",
            "timestamp": datetime.now(UTC),
        }

        # Act
        performance_tracker.track_signal(**exit_signal)

        # Assert
        assert performance_tracker.signals[0]["signal_type"] == "EXIT"
        assert performance_tracker.signals[0]["confidence"] is None

    # ==================== EDGE CASES ====================

    def test_track_signal_edge_update_existing(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test updating an existing signal."""
        # Arrange
        signal_id = "UPDATE001"
        initial_signal: dict[str, Any] = {
            "signal_id": signal_id,
            "strategy_name": "test_strategy",
            "symbol": "BTC-PERP",
            "signal_type": "ENTER_LONG",
            "timestamp": datetime.now(UTC),
        }

        # Track initial signal
        performance_tracker.track_signal(**initial_signal)

        # Update signal
        updated_signal: dict[str, Any] = {
            **initial_signal,
            "confidence": Decimal("0.95"),
            "metadata": {"updated": True},
        }

        # Act
        performance_tracker.track_signal(**updated_signal)

        # Assert
        assert len(performance_tracker.signals) == 1  # Not duplicated
        assert performance_tracker.signals[0]["confidence"] == Decimal("0.95")


class TestTrackSignalExecution:
    """Test suite for track_signal_execution method."""

    # ==================== SUCCESS CASES ====================

    def test_track_signal_execution_success(self, performance_tracker: PerformanceTracker) -> None:
        """Test marking signal as executed."""
        # Arrange
        signal_id = "EXEC001"
        performance_tracker.track_signal(
            signal_id=signal_id,
            strategy_name="test_strategy",
            symbol="BTC-PERP",
            signal_type="ENTER_LONG",
            timestamp=datetime.now(UTC),
        )

        # Act
        performance_tracker.track_signal_execution(signal_id=signal_id, executed=True)

        # Assert
        assert performance_tracker.signals[0]["executed"] is True

    # ==================== EDGE CASES ====================

    def test_track_signal_execution_edge_with_metadata(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test signal execution with metadata."""
        # Arrange
        signal_id = "EXECMETA001"
        performance_tracker.track_signal(
            signal_id=signal_id,
            strategy_name="test_strategy",
            symbol="ETH-PERP",
            signal_type="ENTER_SHORT",
            timestamp=datetime.now(UTC),
        )

        # Act
        performance_tracker.track_signal_execution(
            signal_id=signal_id,
            executed=True,
            metadata={"execution_price": Decimal(2950), "slippage": Decimal("0.02")},
        )

        # Assert
        signal = performance_tracker.signals[0]
        assert signal["executed"] is True
        assert signal["metadata"]["execution_price"] == Decimal(2950)


class TestTrackFundingRate:
    """Test suite for track_funding_rate method."""

    # ==================== SUCCESS CASES ====================

    def test_track_funding_rate_success(self, performance_tracker: PerformanceTracker) -> None:
        """Test tracking funding rate data."""
        # Arrange
        funding_data: dict[str, Any] = {
            "timestamp": datetime.now(UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "funding_rate": Decimal("0.0001"),
            "predicted_rate": Decimal("0.00015"),
        }

        # Act
        with patch.object(performance_tracker.persistence, "save_funding_rates") as mock_save:
            performance_tracker.track_funding_rate(**funding_data)

        # Assert
        assert len(performance_tracker.funding_rates) == 1
        assert performance_tracker.funding_rates[0]["funding_rate"] == Decimal("0.0001")
        assert performance_tracker.funding_rates[0]["predicted_rate"] == Decimal("0.00015")
        mock_save.assert_called_once()

    # ==================== EDGE CASES ====================

    def test_track_funding_rate_edge_negative_rate(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test tracking negative funding rate."""
        # Arrange
        negative_funding: dict[str, Any] = {
            "timestamp": datetime.now(UTC),
            "exchange": "hyperliquid",
            "symbol": "ETH-PERP",
            "funding_rate": Decimal("-0.0002"),
        }

        # Act
        performance_tracker.track_funding_rate(**negative_funding)

        # Assert
        assert performance_tracker.funding_rates[0]["funding_rate"] == Decimal("-0.0002")
        assert performance_tracker.funding_rates[0]["predicted_rate"] is None


class TestGetStrategyNames:
    """Test suite for get_strategy_names method."""

    # ==================== SUCCESS CASES ====================

    def test_get_strategy_names_success(self, performance_tracker: PerformanceTracker) -> None:
        """Test getting all strategy names."""
        # Arrange
        performance_tracker.track_return("strategy1", datetime.now(UTC), Decimal("0.01"))
        performance_tracker.track_trade(
            trade_id="T1",
            strategy_name="strategy2",
            symbol="BTC-PERP",
            exchange="hyperliquid",
            direction="long",
            size=Decimal("0.1"),
            entry_price=Decimal(50000),
            entry_time=datetime.now(UTC),
        )
        performance_tracker.track_signal(
            signal_id="S1",
            strategy_name="strategy3",
            symbol="ETH-PERP",
            signal_type="ENTER_LONG",
            timestamp=datetime.now(UTC),
        )

        # Act
        strategies = performance_tracker.get_strategy_names()

        # Assert
        assert set(strategies) == {"strategy1", "strategy2", "strategy3"}

    # ==================== EDGE CASES ====================

    def test_get_strategy_names_edge_empty(self, performance_tracker: PerformanceTracker) -> None:
        """Test getting strategy names when no data exists."""
        # Act
        strategies = performance_tracker.get_strategy_names()

        # Assert
        assert strategies == []


class TestGetReturnsDataframe:
    """Test suite for get_returns_dataframe method."""

    # ==================== SUCCESS CASES ====================

    def test_get_returns_dataframe_success_single_strategy(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test getting returns for a single strategy."""
        # Arrange
        strategy_name = "test_strategy"
        t1 = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        t2 = datetime(2024, 1, 1, 13, 0, tzinfo=UTC)
        performance_tracker.track_return(strategy_name, t1, Decimal("0.01"))
        performance_tracker.track_return(strategy_name, t2, Decimal("0.02"))

        # Act
        df = performance_tracker.get_returns_dataframe(strategy_names=[strategy_name])

        # Assert
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == [strategy_name]
        # Verify DataFrame structure and content without iloc
        assert strategy_name in df.columns
        # Check that the sum matches expected values (0.01 + 0.02)
        total_returns = df[strategy_name].sum()
        assert abs(total_returns - Decimal("0.03")) < Decimal("1e-10")
        # Check that we have the expected number of entries
        assert not df.empty

    def test_get_returns_dataframe_success_all_strategies(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test getting returns for all strategies."""
        # Arrange
        t1 = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        performance_tracker.track_return("strategy1", t1, Decimal("0.01"))
        performance_tracker.track_return("strategy2", t1, Decimal("0.03"))

        # Act
        df = performance_tracker.get_returns_dataframe()

        # Assert
        assert isinstance(df, pd.DataFrame)
        assert set(df.columns) == {"strategy1", "strategy2"}

    # ==================== EDGE CASES ====================

    def test_get_returns_dataframe_edge_no_data(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test getting returns when no data exists."""
        # Act
        df = performance_tracker.get_returns_dataframe()

        # Assert
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestGetTradesDataframe:
    """Test suite for get_trades_dataframe method."""

    # ==================== SUCCESS CASES ====================

    def test_get_trades_dataframe_success_filter_by_strategy(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test getting trades filtered by strategy."""
        # Arrange
        performance_tracker.track_trade(
            trade_id="T1",
            strategy_name="strategy1",
            symbol="BTC-PERP",
            exchange="hyperliquid",
            direction="long",
            size=Decimal("0.1"),
            entry_price=Decimal(50000),
            entry_time=datetime.now(UTC),
        )
        performance_tracker.track_trade(
            trade_id="T2",
            strategy_name="strategy2",
            symbol="ETH-PERP",
            exchange="hyperliquid",
            direction="short",
            size=Decimal("1.0"),
            entry_price=Decimal(3000),
            entry_time=datetime.now(UTC),
        )

        # Act
        df = performance_tracker.get_trades_dataframe(strategy_names=["strategy1"])

        # Assert
        assert len(df) == 1
        assert df.iloc[0]["trade_id"] == "T1"

    # ==================== EDGE CASES ====================

    def test_get_trades_dataframe_edge_completed_only(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test filtering for completed trades only."""
        # Arrange
        entry_time = datetime.now(UTC)
        # Open trade
        performance_tracker.track_trade(
            trade_id="OPEN",
            strategy_name="test",
            symbol="BTC-PERP",
            exchange="hyperliquid",
            direction="long",
            size=Decimal("0.1"),
            entry_price=Decimal(50000),
            entry_time=entry_time,
        )
        # Completed trade
        performance_tracker.track_trade(
            trade_id="COMPLETE",
            strategy_name="test",
            symbol="ETH-PERP",
            exchange="hyperliquid",
            direction="short",
            size=Decimal("1.0"),
            entry_price=Decimal(3000),
            entry_time=entry_time,
            exit_price=Decimal(2900),
            exit_time=entry_time + timedelta(hours=1),
            pnl=Decimal(100),
        )

        # Act
        df = performance_tracker.get_trades_dataframe(completed_only=True)

        # Assert
        assert len(df) == 1
        assert df.iloc[0]["trade_id"] == "COMPLETE"


class TestGetSignalsDataframe:
    """Test suite for get_signals_dataframe method."""

    # ==================== SUCCESS CASES ====================

    def test_get_signals_dataframe_success_filter_by_executed(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test filtering signals by execution status."""
        # Arrange
        # Unexecuted signal
        performance_tracker.track_signal(
            signal_id="S1",
            strategy_name="test",
            symbol="BTC-PERP",
            signal_type="ENTER_LONG",
            timestamp=datetime.now(UTC),
        )
        # Executed signal
        performance_tracker.track_signal(
            signal_id="S2",
            strategy_name="test",
            symbol="ETH-PERP",
            signal_type="ENTER_SHORT",
            timestamp=datetime.now(UTC),
        )
        performance_tracker.track_signal_execution("S2", executed=True)

        # Act
        # Filter manually since get_signals_dataframe doesn't have executed_only parameter
        all_signals_df = performance_tracker.get_signals_dataframe()
        df_executed = all_signals_df[all_signals_df["executed"]]
        df_unexecuted = all_signals_df[~all_signals_df["executed"]]

        # Assert
        assert len(df_executed) == 1
        assert df_executed.iloc[0]["signal_id"] == "S2"
        assert len(df_unexecuted) == 1
        assert df_unexecuted.iloc[0]["signal_id"] == "S1"


class TestGetFundingRatesDataframe:
    """Test suite for get_funding_rates_dataframe method."""

    # ==================== SUCCESS CASES ====================

    def test_get_funding_rates_dataframe_success_basic(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test getting funding rates basic functionality."""
        # Arrange
        t1 = datetime.now(UTC)

        performance_tracker.track_funding_rate(
            timestamp=t1,
            exchange="hyperliquid",
            symbol="BTC-PERP",
            funding_rate=Decimal("0.0001"),
        )

        # Act
        df = performance_tracker.get_funding_rates_dataframe()

        # Assert
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "BTC-PERP"

    # ==================== EDGE CASES ====================

    def test_get_funding_rates_dataframe_edge_empty_data(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test getting funding rates when no data exists."""
        # Act
        df = performance_tracker.get_funding_rates_dataframe()

        # Assert
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


# Parametrized tests for comprehensive coverage
@pytest.mark.parametrize(
    ("strategy_filter", "expected_count"),
    [
        ("strategy1", 2),
        ("strategy2", 1),
        ("non_existent", 0),
        (None, 3),  # All trades
    ],
)
def test_get_trades_dataframe_parametrized(
    performance_tracker: PerformanceTracker, strategy_filter: str | None, expected_count: int
) -> None:
    """Test get_trades_dataframe with different strategy filters."""
    # Arrange
    base_trade_data: dict[str, Any] = {
        "symbol": "BTC-PERP",
        "exchange": "hyperliquid",
        "direction": "long",
        "size": Decimal("0.1"),
        "entry_price": Decimal(50000),
        "entry_time": datetime.now(UTC),
    }

    trade_configs = [
        {"trade_id": "T1", "strategy_name": "strategy1"},
        {"trade_id": "T2", "strategy_name": "strategy1"},
        {"trade_id": "T3", "strategy_name": "strategy2"},
    ]

    for config in trade_configs:
        performance_tracker.track_trade(
            trade_id=config["trade_id"],
            strategy_name=config["strategy_name"],
            **base_trade_data,
        )

    # Act
    df = performance_tracker.get_trades_dataframe(
        strategy_names=[strategy_filter] if strategy_filter else None
    )

    # Assert
    assert len(df) == expected_count
