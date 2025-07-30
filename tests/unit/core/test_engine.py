"""Unit tests for the Engine component.

Tests the core trading engine's public interface and business logic.
Focuses on strategy management, signal routing, and lifecycle management.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from cyberdelta.core.engine import Engine, EngineConfigurationError
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.trade_signal import TradeSignal
from cyberdelta.core.strategy import Strategy
from cyberdelta.enums import OrderSide, SignalType


@pytest.fixture
def engine() -> Engine:
    """Create an Engine instance for testing.

    Returns:
        Engine: Trading engine instance configured for testing.
    """
    return Engine("test_engine")


@pytest.fixture
def mock_strategy() -> Mock:
    """Create a mock strategy that behaves like the real Strategy interface.

    Returns:
        Mock: Mock strategy object with proper interface methods.
    """
    strategy = Mock(spec=Strategy)
    strategy.name = "test_strategy"
    strategy.symbol = "BTC-PERP"  # Single symbol, not symbols list
    strategy.enabled = False

    # Mock the actual public methods that engine calls
    strategy.enable = Mock()
    strategy.disable = Mock()
    strategy.on_start = Mock()  # Synchronous, not async
    strategy.on_stop = Mock()  # Synchronous, not async
    strategy.process_data = AsyncMock(return_value=None)  # This is the async method

    return strategy


@pytest.fixture
def sample_candle() -> Candle:
    """Create a valid Candle for testing.

    Returns:
        Candle: Market candle data for BTC-PERP.
    """
    return Candle(
        symbol="BTC-PERP",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal(50000),
        high=Decimal(51000),
        low=Decimal(49000),
        close=Decimal(50500),
        volume=Decimal(100),
    )


@pytest.fixture
def sample_signal() -> TradeSignal:
    """Create a valid TradeSignal for testing.

    Returns:
        TradeSignal: Trade signal for entering a long position.
    """
    return TradeSignal(
        signal_id="test_signal",
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal(50000),
        exchange="test_exchange",
        timestamp=datetime.now(UTC),
    )


class TestEngineInitialization:
    """Test engine initialization and basic state."""

    def test_engine_initializes_with_default_name(self) -> None:
        """Test engine initializes with default name."""
        # Act
        engine = Engine()

        # Assert
        assert engine.name == "CyberDeltaEngine"
        assert engine.is_running is False
        assert len(engine.strategies) == 0

    def test_engine_initializes_with_custom_name(self) -> None:
        """Test engine initializes with custom name."""
        # Act
        engine = Engine("custom_engine")

        # Assert
        assert engine.name == "custom_engine"
        assert engine.is_running is False


class TestStrategyManagement:
    """Test strategy lifecycle management through public interface."""

    def test_add_strategy_adds_and_disables_by_default(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test that adding a strategy adds it but keeps it disabled."""
        # Act
        engine.add_strategy(mock_strategy)

        # Assert
        assert mock_strategy.name in engine.strategies
        mock_strategy.disable.assert_called_once()

    def test_enable_strategy_enables_registered_strategy(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test enabling a strategy that was previously added."""
        # Arrange
        engine.add_strategy(mock_strategy)

        # Act
        engine.enable_strategy("test_strategy")

        # Assert
        mock_strategy.enable.assert_called_once()

    def test_enable_nonexistent_strategy_logs_warning(self, engine: Engine) -> None:
        """Test that enabling non-existent strategy logs warning and returns without error."""
        # Act - should not raise exception
        engine.enable_strategy("nonexistent")

        # Assert - strategy not added to enabled list
        assert "nonexistent" not in engine.enabled_strategies

    def test_disable_strategy_disables_enabled_strategy(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test disabling an enabled strategy."""
        # Arrange
        engine.add_strategy(mock_strategy)
        engine.enable_strategy("test_strategy")

        # Act
        engine.disable_strategy("test_strategy")

        # Assert
        # disable should be called twice: once on add, once on disable
        assert mock_strategy.disable.call_count == 2

    def test_remove_strategy_removes_from_engine(self, engine: Engine, mock_strategy: Mock) -> None:
        """Test removing a strategy removes it from the engine."""
        # Arrange
        engine.add_strategy(mock_strategy)

        # Act
        engine.remove_strategy("test_strategy")

        # Assert
        assert "test_strategy" not in engine.strategies


class TestSignalHandling:
    """Test signal handler configuration."""

    def test_set_signal_handler_configures_handler(self, engine: Engine) -> None:
        """Test setting a signal handler stores it correctly."""
        # Arrange
        handler = AsyncMock()

        # Act
        engine.set_signal_handler(handler)

        # Assert
        assert engine.signal_handler == handler

    def test_start_without_signal_handler_raises_error(self, engine: Engine) -> None:
        """Test that starting without signal handler raises configuration error."""
        # Act & Assert
        with pytest.raises(EngineConfigurationError, match="signal handler"):
            engine.start()


class TestEngineLifecycle:
    """Test engine start/stop lifecycle."""

    def test_start_sets_running_state_and_calls_strategy_hooks(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test engine start calls strategy on_start hooks."""
        # Arrange
        handler = AsyncMock()
        engine.set_signal_handler(handler)
        engine.add_strategy(mock_strategy)
        engine.enable_strategy("test_strategy")

        # Act
        engine.start()

        # Assert
        assert engine.is_running is True
        assert engine.start_time is not None
        mock_strategy.on_start.assert_called_once()

    def test_stop_clears_running_state_and_calls_strategy_hooks(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test engine stop calls strategy on_stop hooks."""
        # Arrange
        handler = AsyncMock()
        engine.set_signal_handler(handler)
        engine.add_strategy(mock_strategy)
        engine.enable_strategy("test_strategy")
        engine.start()

        # Act
        engine.stop()

        # Assert
        assert engine.is_running is False
        mock_strategy.on_stop.assert_called_once()


class TestMarketDataProcessing:
    """Test market data processing through public interface."""

    @pytest.mark.asyncio
    async def test_process_market_data_routes_to_enabled_strategies(
        self, engine: Engine, mock_strategy: Mock, sample_candle: Candle, sample_signal: TradeSignal
    ) -> None:
        """Test that market data is routed to enabled strategies matching symbol."""
        # Arrange
        handler = AsyncMock()
        engine.set_signal_handler(handler)
        engine.add_strategy(mock_strategy)
        engine.enable_strategy("test_strategy")
        engine.start()
        mock_strategy.process_data.return_value = sample_signal

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        mock_strategy.process_data.assert_called_once_with(sample_candle)
        handler.assert_called_once_with(sample_signal)

    @pytest.mark.asyncio
    async def test_process_market_data_ignores_disabled_strategies(
        self, engine: Engine, mock_strategy: Mock, sample_candle: Candle
    ) -> None:
        """Test that disabled strategies don't receive market data."""
        # Arrange
        handler = AsyncMock()
        engine.set_signal_handler(handler)
        engine.add_strategy(mock_strategy)  # Strategy is disabled by default
        engine.start()

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        mock_strategy.process_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_market_data_handles_strategy_returning_none(
        self, engine: Engine, mock_strategy: Mock, sample_candle: Candle
    ) -> None:
        """Test that strategies returning None don't cause signals to be sent."""
        # Arrange
        handler = AsyncMock()
        engine.set_signal_handler(handler)
        engine.add_strategy(mock_strategy)
        engine.enable_strategy("test_strategy")
        engine.start()
        mock_strategy.process_data.return_value = None

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        mock_strategy.process_data.assert_called_once()
        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_market_data_when_engine_not_running_does_nothing(
        self, engine: Engine, mock_strategy: Mock, sample_candle: Candle
    ) -> None:
        """Test that market data processing is ignored when engine not running."""
        # Arrange
        handler = AsyncMock()
        engine.set_signal_handler(handler)
        engine.add_strategy(mock_strategy)
        engine.enable_strategy("test_strategy")
        # Note: not calling start()

        # Act
        await engine.process_market_data(sample_candle)

        # Assert
        mock_strategy.process_data.assert_not_called()
        handler.assert_not_called()


class TestEngineInformation:
    """Test engine information reporting."""

    def test_get_engine_info_returns_engine_state(self, engine: Engine) -> None:
        """Test that get_engine_info returns correct engine state information."""
        # Act
        info = engine.get_engine_info()

        # Assert
        assert info["name"] == "test_engine"
        assert info["running"] is False
        assert info["total_strategies"] == 0
        assert info["enabled_strategies"] == 0

    def test_get_engine_info_with_strategies_shows_counts(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test engine info reflects strategy counts correctly."""
        # Arrange
        engine.add_strategy(mock_strategy)
        engine.enable_strategy("test_strategy")

        # Act
        info = engine.get_engine_info()

        # Assert
        assert info["total_strategies"] == 1
        assert info["enabled_strategies"] == 1


class TestErrorHandling:
    """Test error handling in engine operations."""

    def test_strategy_exception_during_start_is_handled_gracefully(
        self, engine: Engine, mock_strategy: Mock
    ) -> None:
        """Test that strategy exceptions during start are caught and strategy is disabled."""
        # Arrange
        handler = AsyncMock()
        engine.set_signal_handler(handler)
        engine.add_strategy(mock_strategy)
        engine.enable_strategy("test_strategy")
        mock_strategy.on_start.side_effect = RuntimeError("Strategy start error")

        # Act - Engine catches RuntimeError and disables the strategy
        engine.start()

        # Assert - Engine started successfully but strategy was disabled
        assert engine.is_running is True
        assert "test_strategy" not in engine.enabled_strategies
        mock_strategy.disable.assert_called()  # Strategy was disabled due to error

    @pytest.mark.asyncio
    async def test_strategy_exception_during_data_processing_is_handled(
        self, engine: Engine, mock_strategy: Mock, sample_candle: Candle
    ) -> None:
        """Test that strategy exceptions during data processing are caught and logged."""
        # Arrange
        handler = AsyncMock()
        engine.set_signal_handler(handler)
        engine.add_strategy(mock_strategy)
        engine.enable_strategy("test_strategy")
        engine.start()
        mock_strategy.process_data.side_effect = ValueError("Strategy processing error")

        # Act - Engine catches ValueError and continues
        await engine.process_market_data(sample_candle)

        # Assert - Engine handled the error gracefully
        mock_strategy.process_data.assert_called_once_with(sample_candle)
        # Handler should not be called when strategy throws exception
        handler.assert_not_called()
        # Strategy remains enabled (errors during processing don't disable strategies)
        assert "test_strategy" in engine.enabled_strategies
