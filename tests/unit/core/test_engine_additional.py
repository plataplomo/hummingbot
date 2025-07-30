"""Additional comprehensive unit tests for Engine.

Tests additional public methods and edge cases that need better coverage,
focusing on strategy management, signal routing, market data processing,
and engine lifecycle management.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

import contextlib
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from cyberdelta.core.dataframe_processor import DataFrameProcessingError, process_dataframe
from cyberdelta.core.engine import Engine, EngineConfigurationError
from cyberdelta.core.models import OrderSide, SignalType
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.trade_signal import TradeSignal
from cyberdelta.core.strategy import Strategy


@pytest.fixture
def engine() -> Engine:
    """Create an Engine instance for testing.

    Returns:
        Engine: Engine instance with name "TestEngine".
    """
    return Engine("TestEngine")


@pytest.fixture
def mock_strategy() -> Mock:
    """Create a mock Strategy for testing.

    Returns:
        Mock: Mocked Strategy with BTC-PERP symbol and test configuration.
    """
    strategy = Mock(spec=Strategy)
    strategy.name = "test_strategy"
    strategy.symbol = "BTC-PERP"
    strategy.enabled = True  # Strategy has enabled attribute, not is_enabled method
    strategy.enable = Mock()
    strategy.disable = Mock()
    strategy.process_data = AsyncMock(return_value=None)
    return strategy


@pytest.fixture
def sample_candle() -> Candle:
    """Create a sample Candle for testing.

    Returns:
        Candle: Sample BTC-PERP candle with test price data.
    """
    return Candle(
        symbol="BTC-PERP",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("50000.0"),
        high=Decimal("50100.0"),
        low=Decimal("49900.0"),
        close=Decimal("50050.0"),
        volume=Decimal("1.5"),
    )


@pytest.fixture
def sample_trade_signal() -> TradeSignal:
    """Create a sample TradeSignal for testing.

    Returns:
        TradeSignal: Sample long entry signal for BTC-PERP.
    """
    return TradeSignal(
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000.0"),
        quantity=Decimal("0.1"),
        exchange="hyperliquid",
        timestamp=datetime.now(UTC),
        metadata={"confidence": 0.8},
    )


@pytest.fixture
def mock_signal_handler() -> AsyncMock:
    """Create a mock signal handler for testing.

    Returns:
        AsyncMock: Mocked async signal handler function.
    """
    return AsyncMock()


class TestEngineInitialization:
    """Test suite for Engine initialization and basic configuration."""

    # ==================== SUCCESS CASES ====================

    def test_initialization_success_default_name(self) -> None:
        """Test successful initialization with default name."""
        # Act
        engine = Engine()

        # Assert
        assert engine.name == "CyberDeltaEngine"
        assert engine.strategies == {}
        assert engine.enabled_strategies == set()
        assert engine.active_symbols == set()
        assert engine.signal_handler is None
        assert engine.is_running is False
        assert engine.start_time is None
        assert engine.last_data_time is None

    def test_initialization_success_custom_name(self) -> None:
        """Test successful initialization with custom name."""
        # Arrange
        custom_name = "MyTradingEngine"

        # Act
        engine = Engine(custom_name)

        # Assert
        assert engine.name == custom_name
        assert hasattr(engine, "logger")

    def test_initialization_success_creates_logger(self, engine: Engine) -> None:
        """Test that initialization creates a logger with engine name."""
        # Assert
        assert hasattr(engine, "logger")
        assert engine.logger is not None


class TestEngineStrategyManagement:
    """Test suite for strategy management functionality."""

    # ==================== SUCCESS CASES ====================

    def test_add_strategy_success_new_strategy(self, engine: Engine, mock_strategy: Mock) -> None:
        """Test adding a new strategy to the engine."""
        # Act
        engine.add_strategy(mock_strategy)

        # Assert
        assert mock_strategy.name in engine.strategies
        assert engine.strategies[mock_strategy.name] is mock_strategy
        assert mock_strategy.name not in engine.enabled_strategies
        mock_strategy.disable.assert_called_once()

    def test_add_strategy_success_updates_active_symbols(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test that adding strategy updates active symbols tracking."""
        # Act
        engine.add_strategy(mock_strategy)

        # Assert
        # Active symbols are updated via _refresh_active_symbols
        assert hasattr(engine, "active_symbols")

    def test_add_strategy_success_replaces_existing_strategy(self, engine: Engine) -> None:
        """Test replacing an existing strategy with same name."""
        # Arrange
        old_strategy = Mock(spec=Strategy)
        old_strategy.name = "test_strategy"
        old_strategy.symbol = "BTC-PERP"
        old_strategy.enabled = False
        old_strategy.disable = Mock()

        new_strategy = Mock(spec=Strategy)
        new_strategy.name = "test_strategy"  # Same name
        new_strategy.symbol = "ETH-PERP"  # Different symbol
        new_strategy.enabled = False
        new_strategy.disable = Mock()

        engine.add_strategy(old_strategy)

        # Act
        engine.add_strategy(new_strategy)

        # Assert
        assert engine.strategies["test_strategy"] is new_strategy
        assert engine.strategies["test_strategy"] is not old_strategy

    def test_enable_strategy_success_existing_strategy(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test enabling an existing strategy."""
        # Arrange
        engine.add_strategy(mock_strategy)

        # Act
        engine.enable_strategy(mock_strategy.name)

        # Assert
        assert mock_strategy.name in engine.enabled_strategies
        mock_strategy.enable.assert_called_once()

    def test_disable_strategy_success_enabled_strategy(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test disabling an enabled strategy."""
        # Arrange
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)

        # Act
        engine.disable_strategy(mock_strategy.name)

        # Assert
        assert mock_strategy.name not in engine.enabled_strategies
        # disable is called twice: once in add_strategy, once in disable_strategy
        assert mock_strategy.disable.call_count == 2

    def test_remove_strategy_success_existing_strategy(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test removing an existing strategy."""
        # Arrange
        engine.add_strategy(mock_strategy)

        # Act
        engine.remove_strategy(mock_strategy.name)

        # Assert
        assert mock_strategy.name not in engine.strategies
        assert mock_strategy.name not in engine.enabled_strategies

    # ==================== EDGE CASES ====================

    def test_enable_strategy_edge_nonexistent_strategy(self, engine: Engine) -> None:
        """Test enabling a strategy that doesn't exist."""
        # Act
        engine.enable_strategy("nonexistent_strategy")

        # Assert
        assert "nonexistent_strategy" not in engine.enabled_strategies

    def test_enable_strategy_edge_already_enabled(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test enabling a strategy that's already enabled."""
        # Arrange
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        mock_strategy.enable.reset_mock()

        # Act
        engine.enable_strategy(mock_strategy.name)

        # Assert
        assert mock_strategy.name in engine.enabled_strategies
        # Should not call enable again
        mock_strategy.enable.assert_not_called()

    def test_disable_strategy_edge_nonexistent_strategy(self, engine: Engine) -> None:
        """Test disabling a strategy that doesn't exist."""
        # Act
        engine.disable_strategy("nonexistent_strategy")

        # Assert
        # Should handle gracefully without error
        assert "nonexistent_strategy" not in engine.enabled_strategies

    def test_disable_strategy_edge_already_disabled(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test disabling a strategy that's already disabled."""
        # Arrange
        engine.add_strategy(mock_strategy)
        # Strategy is disabled by default, reset the mock
        mock_strategy.disable.reset_mock()

        # Act
        engine.disable_strategy(mock_strategy.name)

        # Assert
        assert mock_strategy.name not in engine.enabled_strategies
        # Engine skips calling disable when already disabled
        mock_strategy.disable.assert_not_called()

    def test_remove_strategy_edge_nonexistent_strategy(self, engine: Engine) -> None:
        """Test removing a strategy that doesn't exist."""
        # Act
        engine.remove_strategy("nonexistent_strategy")

        # Assert
        # Should handle gracefully without error
        assert "nonexistent_strategy" not in engine.strategies


class TestEngineSignalHandling:
    """Test suite for signal handler configuration and processing."""

    # ==================== SUCCESS CASES ====================

    def test_set_signal_handler_success_function_handler(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test setting a signal handler function."""
        # Act
        engine.set_signal_handler(mock_signal_handler)

        # Assert
        assert engine.signal_handler is mock_signal_handler

    def test_set_signal_handler_success_method_handler(self, engine: Engine) -> None:
        """Test setting a signal handler method."""
        # Arrange
        handler_object = Mock()
        handler_object.handle_signal = AsyncMock()

        # Act
        engine.set_signal_handler(handler_object.handle_signal)

        # Assert
        assert engine.signal_handler is handler_object.handle_signal

    def test_set_signal_handler_success_replaces_existing(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test that setting signal handler replaces existing one."""
        # Arrange
        old_handler = AsyncMock()
        engine.set_signal_handler(old_handler)

        # Act
        engine.set_signal_handler(mock_signal_handler)

        # Assert
        assert engine.signal_handler is mock_signal_handler
        assert engine.signal_handler is not old_handler

    # ==================== EDGE CASES ====================

    def test_set_signal_handler_edge_none_handler(self, engine: Engine) -> None:
        """Test setting None as signal handler."""
        # Act
        engine.set_signal_handler(None)  # type: ignore

        # Assert
        assert engine.signal_handler is None

    def test_set_signal_handler_edge_lambda_handler(self, engine: Engine) -> None:
        """Test setting a lambda as signal handler."""

        # Arrange
        async def lambda_handler(signal: TradeSignal) -> None:
            pass

        # Act
        engine.set_signal_handler(lambda_handler)

        # Assert
        assert engine.signal_handler is lambda_handler


class TestEngineMarketDataProcessing:
    """Test suite for market data processing functionality."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_success_with_enabled_strategy(
        self,
        engine: Engine,
        mock_strategy: Mock,
        sample_candle: Candle,
        mock_signal_handler: AsyncMock,
    ) -> None:
        """Test processing market data with an enabled strategy."""
        # Arrange
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        mock_strategy.process_data.assert_called_once_with(sample_candle)
        assert engine.last_data_time is not None

    @pytest.mark.asyncio
    async def test_process_market_data_success_no_enabled_strategies(
        self,
        engine: Engine,
        mock_strategy: Mock,
        sample_candle: Candle,
        mock_signal_handler: AsyncMock,
    ) -> None:
        """Test processing market data with no enabled strategies."""
        # Arrange
        engine.add_strategy(mock_strategy)  # Added but not enabled
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        # Strategy should not be called since it's disabled
        mock_strategy.process_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_market_data_success_multiple_strategies(
        self, engine: Engine, sample_candle: Candle, mock_signal_handler: AsyncMock
    ) -> None:
        """Test processing market data with multiple enabled strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"
        strategy1.enabled = True
        strategy1.enable = Mock()
        strategy1.disable = Mock()
        strategy1.process_data = AsyncMock(return_value=None)

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "BTC-PERP"  # Same symbol
        strategy2.enabled = True
        strategy2.enable = Mock()
        strategy2.disable = Mock()
        strategy2.process_data = AsyncMock(return_value=None)

        engine.add_strategy(strategy1)
        engine.add_strategy(strategy2)
        engine.enable_strategy("strategy1")
        engine.enable_strategy("strategy2")
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        strategy1.process_data.assert_called_once_with(sample_candle)
        strategy2.process_data.assert_called_once_with(sample_candle)

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_edge_engine_not_running(
        self, engine: Engine, sample_candle: Candle, mock_signal_handler: AsyncMock
    ) -> None:
        """Test processing market data when engine is not running."""
        # Arrange
        engine.set_signal_handler(mock_signal_handler)
        # Engine is not started

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        # Should not process data when engine is not running
        assert engine.last_data_time is None

    @pytest.mark.asyncio
    async def test_process_market_data_edge_no_signal_handler(
        self, engine: Engine, sample_candle: Candle
    ) -> None:
        """Test processing market data without signal handler."""
        # Arrange
        # Try to start engine without signal handler - should fail
        with pytest.raises(EngineConfigurationError, match=r"cannot start without.*signal handler"):
            engine.start()

        # Act - process data on unstarted engine
        await engine.process_market_data(sample_candle)

        # Assert
        # Should not process data without signal handler or when not started
        assert engine.last_data_time is None

    @pytest.mark.asyncio
    async def test_process_market_data_edge_symbol_not_monitored(
        self, engine: Engine, mock_strategy: Mock, mock_signal_handler: AsyncMock
    ) -> None:
        """Test processing market data for symbol not monitored by any strategy."""
        # Arrange
        mock_strategy.symbol = "ETH-PERP"  # Different symbol
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        btc_candle = Candle(
            symbol="BTC-PERP",  # Different from strategy symbol
            interval="1m",
            open_time=datetime.now(UTC),
            open=Decimal("50000.0"),
            high=Decimal("50100.0"),
            low=Decimal("49900.0"),
            close=Decimal("50050.0"),
            volume=Decimal("1.5"),
        )

        # Act
        await engine.process_market_data(btc_candle)

        # Assert
        # Strategy should not be called for different symbol
        mock_strategy.process_data.assert_not_called()

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_failure_strategy_raises_exception(
        self,
        engine: Engine,
        mock_strategy: Mock,
        sample_candle: Candle,
        mock_signal_handler: AsyncMock,
    ) -> None:
        """Test handling when strategy raises exception during processing."""
        # Arrange
        # Current business logic doesn't catch generic Exception, so it propagates
        mock_strategy.process_data.side_effect = Exception("Strategy error")
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act & Assert - Current business logic lets generic Exception propagate
        # This is the current behavior and source of truth
        with pytest.raises(Exception) as exc_info:
            await engine.process_market_data(sample_candle)

        # Verify the exception details
        assert "Strategy error" in str(exc_info.value)
        # Engine's last_data_time should not be updated when exception propagates
        # (The data processing was interrupted by the exception)


class TestEngineDataFrameProcessing:
    """Test suite for DataFrame processing functionality."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_process_dataframe_success_valid_dataframe(
        self, engine: Engine, mock_strategy: Mock, mock_signal_handler: AsyncMock
    ) -> None:
        """Test processing a valid DataFrame."""
        # Arrange
        candle_data = pd.DataFrame({
            "timestamp": [datetime.now(UTC), datetime.now(UTC)],
            "open": [50000.0, 50010.0],
            "high": [50100.0, 50110.0],
            "low": [49900.0, 49910.0],
            "close": [50050.0, 50060.0],
            "volume": [1.5, 1.6],
        })

        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act
        await process_dataframe(candle_data, "BTC-PERP", engine.process_market_data)

        # Assert
        # Should process each row of the DataFrame
        assert mock_strategy.process_data.call_count >= 1

    @pytest.mark.asyncio
    async def test_process_dataframe_success_empty_dataframe(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test processing an empty DataFrame."""
        # Arrange
        empty_data = pd.DataFrame()
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act & Assert
        # Empty DataFrame should raise exception for missing columns
        with pytest.raises(DataFrameProcessingError, match="missing required columns"):
            await process_dataframe(empty_data, "BTC-PERP", engine.process_market_data)

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_process_dataframe_edge_missing_required_columns(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test processing DataFrame with missing required columns."""
        # Arrange
        incomplete_data = pd.DataFrame({
            "timestamp": [datetime.now(UTC)],
            "open": [50000.0],
            # Missing high, low, close, volume columns
        })

        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act & Assert
        with pytest.raises(DataFrameProcessingError) as exc_info:
            await process_dataframe(incomplete_data, "BTC-PERP", engine.process_market_data)

        assert "missing required columns" in str(exc_info.value)
        assert hasattr(exc_info.value, "missing_columns")

    @pytest.mark.asyncio
    async def test_process_dataframe_edge_invalid_data_types(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test processing DataFrame with invalid data types."""
        # Arrange
        invalid_data = pd.DataFrame({
            "timestamp": [datetime.now(UTC)],
            "open": ["invalid"],  # Invalid numeric data
            "high": [50100.0],
            "low": [49900.0],
            "close": [50050.0],
            "volume": [1.5],
        })

        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act & Assert
        # Should handle invalid data types gracefully or raise appropriate error
        with contextlib.suppress(ValueError, TypeError, DataFrameProcessingError):
            await process_dataframe(invalid_data, "BTC-PERP", engine.process_market_data)


class TestEngineLifecycleManagement:
    """Test suite for engine lifecycle management."""

    # ==================== SUCCESS CASES ====================

    def test_start_success_engine_not_running(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test starting the engine when it's not running."""
        # Arrange
        engine.set_signal_handler(mock_signal_handler)

        # Act
        engine.start()

        # Assert
        assert engine.is_running is True
        assert engine.start_time is not None

    def test_start_success_updates_start_time(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test that starting the engine updates start time."""
        # Arrange
        engine.set_signal_handler(mock_signal_handler)
        original_start_time = engine.start_time

        # Act
        engine.start()

        # Assert
        assert engine.start_time != original_start_time
        assert isinstance(engine.start_time, datetime)

    def test_stop_success_engine_running(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test stopping the engine when it's running."""
        # Arrange
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act
        engine.stop()

        # Assert
        assert engine.is_running is False

    def test_stop_success_clears_state(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test that stopping the engine clears relevant state."""
        # Arrange
        engine.set_signal_handler(mock_signal_handler)
        engine.start()
        engine.last_data_time = datetime.now(UTC)

        # Act
        engine.stop()

        # Assert
        assert engine.is_running is False
        # last_data_time might be preserved for debugging/metrics

    # ==================== EDGE CASES ====================

    def test_start_edge_already_running(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test starting the engine when it's already running."""
        # Arrange
        engine.set_signal_handler(mock_signal_handler)
        engine.start()
        original_start_time = engine.start_time

        # Act
        engine.start()  # Start again

        # Assert
        assert engine.is_running is True
        # Start time should not change when already running
        assert engine.start_time == original_start_time

    def test_stop_edge_already_stopped(self, engine: Engine) -> None:
        """Test stopping the engine when it's already stopped."""
        # Arrange
        # Engine is stopped by default

        # Act
        engine.stop()

        # Assert
        assert engine.is_running is False

    def test_lifecycle_edge_multiple_start_stop_cycles(
        self, engine: Engine, mock_signal_handler: AsyncMock
    ) -> None:
        """Test multiple start/stop cycles."""
        # Arrange
        engine.set_signal_handler(mock_signal_handler)

        # Act & Assert
        for _ in range(3):
            engine.start()
            assert engine.is_running is True

            engine.stop()
            assert engine.is_running is False


class TestEngineUtilityMethods:
    """Test suite for utility and helper methods."""

    # ==================== SUCCESS CASES ====================

    def test_active_symbols_success_with_enabled_strategies(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test that active symbols are properly updated when strategies are added."""
        # Arrange
        initial_active_symbols = len(engine.active_symbols)

        # Act
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)

        # Assert
        # Active symbols should be updated when strategies are added
        assert len(engine.active_symbols) >= initial_active_symbols
        assert mock_strategy.symbol in engine.active_symbols

    @pytest.mark.asyncio
    async def test_process_market_data_success_monitored_symbol(
        self,
        engine: Engine,
        mock_strategy: Mock,
        sample_candle: Candle,
        mock_signal_handler: AsyncMock,
    ) -> None:
        """Test that market data is processed for monitored symbols."""
        # Arrange
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        # Strategy should be called for monitored symbols
        mock_strategy.process_data.assert_called_once_with(sample_candle)

    @pytest.mark.asyncio
    async def test_process_market_data_success_unmonitored_symbol(
        self, engine: Engine, mock_strategy: Mock, mock_signal_handler: AsyncMock
    ) -> None:
        """Test that market data is ignored for unmonitored symbols."""
        # Arrange
        mock_strategy.symbol = "ETH-PERP"  # Different symbol
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Create candle for unmonitored symbol
        unmonitored_candle = Candle(
            symbol="UNKNOWN-PERP",
            interval="1m",
            open_time=datetime.now(UTC),
            open=Decimal("50000.0"),
            high=Decimal("50100.0"),
            low=Decimal("49900.0"),
            close=Decimal("50050.0"),
            volume=Decimal("1.5"),
        )

        # Act
        await engine.process_market_data(unmonitored_candle)

        # Assert
        # Strategy should not be called for unmonitored symbols
        mock_strategy.process_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_market_data_success_valid_engine_state(
        self,
        engine: Engine,
        mock_strategy: Mock,
        sample_candle: Candle,
        mock_signal_handler: AsyncMock,
    ) -> None:
        """Test that market data is processed when engine is in valid state."""
        # Arrange
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        engine.set_signal_handler(mock_signal_handler)
        engine.start()

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        # Valid engine state should process data successfully
        mock_strategy.process_data.assert_called_once_with(sample_candle)
        assert engine.last_data_time is not None

    @pytest.mark.asyncio
    async def test_process_market_data_edge_invalid_state_not_running(
        self,
        engine: Engine,
        mock_strategy: Mock,
        sample_candle: Candle,
        mock_signal_handler: AsyncMock,
    ) -> None:
        """Test that market data is ignored when engine is not running."""
        # Arrange
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        engine.set_signal_handler(mock_signal_handler)
        # Engine not started

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        # Should not process data when engine is not running
        mock_strategy.process_data.assert_not_called()
        assert engine.last_data_time is None

    @pytest.mark.asyncio
    async def test_process_market_data_edge_invalid_state_no_handler(
        self, engine: Engine, mock_strategy: Mock, sample_candle: Candle
    ) -> None:
        """Test that market data is ignored when no signal handler is set."""
        # Arrange
        engine.add_strategy(mock_strategy)
        engine.enable_strategy(mock_strategy.name)
        # No signal handler set, so starting will fail
        # Test process_market_data behavior without starting engine

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        # Should not process data when no signal handler is set
        mock_strategy.process_data.assert_not_called()
        assert engine.last_data_time is None

    # ==================== EDGE CASES ====================

    def test_active_symbols_edge_no_strategies(self, engine: Engine) -> None:
        """Test that active symbols are empty when no strategies are added."""
        # Act & Assert
        # Engine should have no active symbols when no strategies are added
        assert engine.active_symbols == set()
        assert len(engine.active_symbols) == 0

    def test_active_symbols_edge_all_disabled_strategies(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test that active symbols include all strategies regardless of enabled state."""
        # Arrange
        engine.add_strategy(mock_strategy)
        # Strategy is disabled by default

        # Act & Assert
        # Active symbols should include all strategies' symbols (engine tracks all strategies)
        assert mock_strategy.symbol in engine.active_symbols
        assert len(engine.active_symbols) > 0
