"""Unit tests for the StrategyManager component.

Tests strategy management functionality including lifecycle management and signal processing.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, PropertyMock

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import TradeSignal
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy
from cyberdelta.core.strategy_manager import StrategyManager
from cyberdelta.enums import OrderSide, SignalType


@pytest.fixture
def mock_config() -> Mock:
    """Create mock configuration for testing.

    Returns:
        Mock: A mock AppSettings instance for testing.
    """
    return Mock(spec=AppSettings)


@pytest.fixture
def mock_execution_handler() -> Mock:
    """Create mock execution handler.

    Returns:
        Mock: A mock ExecutionHandler instance for testing.
    """
    return Mock(spec=ExecutionHandler)


@pytest.fixture
def mock_portfolio_tracker() -> Mock:
    """Create mock portfolio tracker.

    Returns:
        Mock: A mock PortfolioTracker instance for testing.
    """
    return Mock(spec=PortfolioTracker)


@pytest.fixture
def mock_risk_manager() -> Mock:
    """Create mock risk manager.

    Returns:
        Mock: A mock RiskManager instance for testing.
    """
    return Mock(spec=RiskManager)


@pytest.fixture
def mock_signal_queue() -> Mock:
    """Create mock signal queue.

    Returns:
        Mock: A mock PrioritySignalQueue instance for testing.
    """
    queue = Mock(spec=PrioritySignalQueue)
    queue.add_signal = AsyncMock()
    return queue


@pytest.fixture
def strategy_manager(
    mock_config: Mock,
    mock_execution_handler: Mock,
    mock_portfolio_tracker: Mock,
    mock_risk_manager: Mock,
    mock_signal_queue: Mock,
) -> StrategyManager:
    """Create StrategyManager instance for testing.

    Returns:
        StrategyManager: A configured StrategyManager instance for testing.
    """
    return StrategyManager(
        config=mock_config,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )


class MockStrategy(Strategy):
    """Mock strategy implementation for testing."""

    def __init__(self) -> None:
        """Initialize mock strategy."""
        super().__init__(name="test_strategy", symbol="BTC-PERP")
        self._performance_metrics = {"win_rate": 0.75, "total_trades": 100}
        # Store mock callables as attributes that can be checked
        self.enable_called = False
        self.disable_called = False
        self.on_start_called = False
        self.on_stop_called = False
        self.update_historical_data_calls: list[Candle] = []
        self.process_data_calls: list[Candle] = []
        # Support side effects for testing error cases
        self.update_historical_data_side_effect: Exception | None = None
        self.process_data_side_effect: Exception | None = None
        self.on_start_side_effect: Exception | None = None
        self.on_stop_side_effect: Exception | None = None
        # Support configurable return value for process_data
        self.process_data_return_value: TradeSignal | list[TradeSignal] | None = None

    @property
    def performance_metrics(self) -> dict[str, float | int]:
        """Return performance metrics."""
        return self._performance_metrics

    def enable(self) -> None:
        """Enable the strategy."""
        super().enable()
        self.enable_called = True

    def disable(self) -> None:
        """Disable the strategy."""
        super().disable()
        self.disable_called = True

    def on_start(self) -> None:
        """Handle strategy start."""
        self.on_start_called = True
        if self.on_start_side_effect:
            raise self.on_start_side_effect
        super().on_start()

    def on_stop(self) -> None:
        """Handle strategy stop."""
        self.on_stop_called = True
        if self.on_stop_side_effect:
            raise self.on_stop_side_effect
        super().on_stop()

    def update_historical_data(self, data: Candle, max_bars: int = 100) -> None:
        """Update historical data."""
        if self.update_historical_data_side_effect:
            raise self.update_historical_data_side_effect
        super().update_historical_data(data, max_bars)
        self.update_historical_data_calls.append(data)

    async def process_data(self, data: Candle) -> TradeSignal | list[TradeSignal] | None:
        """Process market data.

        Returns:
            TradeSignal | list[TradeSignal] | None: The configured return value for testing.
        """
        self.process_data_calls.append(data)
        if self.process_data_side_effect:
            raise self.process_data_side_effect
        return self.process_data_return_value

    def get_required_history_size(self) -> int:
        """Get required history size.

        Returns:
            int: The required history size (always 0 for testing).
        """
        return 0


@pytest.fixture
def mock_strategy() -> MockStrategy:
    """Create mock strategy for testing.

    Returns:
        MockStrategy: A MockStrategy instance for testing.
    """
    return MockStrategy()


@pytest.fixture
def sample_candle() -> Candle:
    """Create sample candle for testing.

    Returns:
        Candle: A sample candle with BTC-PERP data for testing.
    """
    return Candle(
        symbol="BTC-PERP",
        open=Decimal("50000.0"),
        high=Decimal("50100.0"),
        low=Decimal("49900.0"),
        close=Decimal("50050.0"),
        volume=Decimal("100.0"),
        open_time=datetime.now(UTC),
        interval="1m",
    )


@pytest.fixture
def sample_trade_signal() -> TradeSignal:
    """Create sample trade signal for testing.

    Returns:
        TradeSignal: A sample ENTER_LONG trade signal for testing.
    """
    return TradeSignal(
        signal_id="test_signal_1",
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000.0"),
        quantity=Decimal("1.0"),
        exchange="test_exchange",
        timestamp=datetime.now(UTC),
        source_strategy="test_strategy",
        metadata={"reason": "test"},
    )


class TestStrategyManagerInit:
    """Test suite for StrategyManager initialization."""

    # ==================== SUCCESS CASES ====================

    def test_strategy_manager_init_success(
        self,
        mock_config: Mock,
        mock_execution_handler: Mock,
        mock_portfolio_tracker: Mock,
        mock_risk_manager: Mock,
        mock_signal_queue: Mock,
    ) -> None:
        """Test successful initialization of StrategyManager."""
        # Act
        manager = StrategyManager(
            config=mock_config,
            execution_handler=mock_execution_handler,
            portfolio_tracker=mock_portfolio_tracker,
            risk_manager=mock_risk_manager,
            signal_queue=mock_signal_queue,
        )

        # Assert
        assert manager.config == mock_config
        assert manager.execution_handler == mock_execution_handler
        assert manager.portfolio_tracker == mock_portfolio_tracker
        assert manager.risk_manager == mock_risk_manager
        assert manager.signal_queue == mock_signal_queue
        assert manager.strategies == {}
        assert manager.enabled_strategies == set()
        assert manager.active_symbols == set()
        assert manager.last_update_time is None


class TestRegisterStrategy:
    """Test suite for register_strategy method."""

    # ==================== SUCCESS CASES ====================

    def test_register_strategy_success(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test successful strategy registration."""
        # Act
        strategy_manager.register_strategy(mock_strategy)

        # Assert
        assert "test_strategy" in strategy_manager.strategies
        assert strategy_manager.strategies["test_strategy"] == mock_strategy
        assert "BTC-PERP" in strategy_manager.active_symbols

    def test_register_strategy_success_multiple_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test registration of multiple strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "ETH-PERP"

        # Act
        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)

        # Assert
        assert len(strategy_manager.strategies) == 2
        assert "strategy1" in strategy_manager.strategies
        assert "strategy2" in strategy_manager.strategies
        assert "BTC-PERP" in strategy_manager.active_symbols
        assert "ETH-PERP" in strategy_manager.active_symbols

    # ==================== EDGE CASES ====================

    def test_register_strategy_edge_replaces_existing(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test registering strategy with same name replaces existing."""
        # Arrange
        original_strategy = Mock(spec=Strategy)
        original_strategy.name = "test_strategy"
        original_strategy.symbol = "BTC-PERP"
        strategy_manager.register_strategy(original_strategy)

        new_strategy = Mock(spec=Strategy)
        new_strategy.name = "test_strategy"  # Same name
        new_strategy.symbol = "ETH-PERP"  # Different symbol

        # Act
        strategy_manager.register_strategy(new_strategy)

        # Assert
        assert strategy_manager.strategies["test_strategy"] == new_strategy
        assert strategy_manager.strategies["test_strategy"] != original_strategy
        assert "ETH-PERP" in strategy_manager.active_symbols

    def test_register_strategy_edge_same_symbol_different_names(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test registering multiple strategies for same symbol."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "BTC-PERP"  # Same symbol

        # Act
        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)

        # Assert
        assert len(strategy_manager.strategies) == 2
        assert len(strategy_manager.active_symbols) == 1  # Only one unique symbol
        assert "BTC-PERP" in strategy_manager.active_symbols


class TestUnregisterStrategy:
    """Test suite for unregister_strategy method."""

    # ==================== SUCCESS CASES ====================

    def test_unregister_strategy_success(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test successful strategy unregistration."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")

        # Act
        strategy_manager.unregister_strategy("test_strategy")

        # Assert
        assert "test_strategy" not in strategy_manager.strategies
        assert "test_strategy" not in strategy_manager.enabled_strategies

    def test_unregister_strategy_success_refreshes_symbols(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test unregistration refreshes active symbols."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "ETH-PERP"

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)

        # Act
        strategy_manager.unregister_strategy("strategy1")

        # Assert
        assert "BTC-PERP" not in strategy_manager.active_symbols
        assert "ETH-PERP" in strategy_manager.active_symbols

    # ==================== EDGE CASES ====================

    def test_unregister_strategy_edge_nonexistent_strategy(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test unregistering non-existent strategy."""
        # Act - Should not raise exception
        strategy_manager.unregister_strategy("nonexistent_strategy")

        # Assert - No changes to manager state
        assert len(strategy_manager.strategies) == 0
        assert len(strategy_manager.enabled_strategies) == 0

    def test_unregister_strategy_edge_multiple_same_symbol(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test unregistering one of multiple strategies with same symbol."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "BTC-PERP"  # Same symbol

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)

        # Act
        strategy_manager.unregister_strategy("strategy1")

        # Assert
        assert "BTC-PERP" in strategy_manager.active_symbols  # Still active due to strategy2
        assert "strategy1" not in strategy_manager.strategies
        assert "strategy2" in strategy_manager.strategies


class TestEnableStrategy:
    """Test suite for enable_strategy method."""

    # ==================== SUCCESS CASES ====================

    def test_enable_strategy_success(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test successful strategy enabling."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)

        # Act
        result = strategy_manager.enable_strategy("test_strategy")

        # Assert
        assert result is True
        assert "test_strategy" in strategy_manager.enabled_strategies
        assert mock_strategy.enable_called is True

    def test_enable_strategy_success_multiple_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test enabling multiple strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"
        strategy1.enable = Mock()

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "ETH-PERP"
        strategy2.enable = Mock()

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)

        # Act
        result1 = strategy_manager.enable_strategy("strategy1")
        result2 = strategy_manager.enable_strategy("strategy2")

        # Assert
        assert result1 is True
        assert result2 is True
        assert len(strategy_manager.enabled_strategies) == 2
        strategy1.enable.assert_called_once()
        strategy2.enable.assert_called_once()

    # ==================== EDGE CASES ====================

    def test_enable_strategy_edge_already_enabled(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test enabling already enabled strategy."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")
        mock_strategy.enable_called = False

        # Act
        result = strategy_manager.enable_strategy("test_strategy")

        # Assert
        assert result is True
        assert mock_strategy.enable_called is True  # Called again

    # ==================== FAILURE CASES ====================

    def test_enable_strategy_failure_nonexistent_strategy(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test enabling non-existent strategy returns False."""
        # Act
        result = strategy_manager.enable_strategy("nonexistent_strategy")

        # Assert
        assert result is False
        assert len(strategy_manager.enabled_strategies) == 0


class TestDisableStrategy:
    """Test suite for disable_strategy method."""

    # ==================== SUCCESS CASES ====================

    def test_disable_strategy_success(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test successful strategy disabling."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")

        # Act
        result = strategy_manager.disable_strategy("test_strategy")

        # Assert
        assert result is True
        assert "test_strategy" not in strategy_manager.enabled_strategies
        assert mock_strategy.disable_called is True

    def test_disable_strategy_success_not_enabled(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test disabling strategy that is not enabled."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        # Don't enable the strategy

        # Act
        result = strategy_manager.disable_strategy("test_strategy")

        # Assert
        assert result is True
        assert mock_strategy.disable_called is True

    # ==================== FAILURE CASES ====================

    def test_disable_strategy_failure_nonexistent_strategy(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test disabling non-existent strategy returns False."""
        # Act
        result = strategy_manager.disable_strategy("nonexistent_strategy")

        # Assert
        assert result is False


class TestProcessMarketData:
    """Test suite for process_market_data method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_success(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy, sample_candle: Candle
    ) -> None:
        """Test successful market data processing."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        assert strategy_manager.last_update_time is not None
        assert mock_strategy.update_historical_data_calls == [sample_candle]
        assert len(mock_strategy.process_data_calls) == 1
        assert mock_strategy.process_data_calls[0] == sample_candle

    @pytest.mark.asyncio
    async def test_process_market_data_success_with_signals(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_candle: Candle,
        sample_trade_signal: TradeSignal,
        mock_signal_queue: Mock,
    ) -> None:
        """Test market data processing that generates signals."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")
        mock_strategy.process_data_return_value = sample_trade_signal

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        mock_signal_queue.add_signal.assert_called_once_with(sample_trade_signal)

    @pytest.mark.asyncio
    async def test_process_market_data_success_multiple_signals(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_candle: Candle,
        mock_signal_queue: Mock,
    ) -> None:
        """Test processing market data that generates multiple signals."""
        # Arrange
        signal1 = TradeSignal(
            signal_id="signal1",
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("1.0"),
            exchange="test_exchange",
            timestamp=datetime.now(UTC),
            source_strategy="test_strategy",
            metadata={"reason": "test"},
        )
        signal2 = TradeSignal(
            signal_id="signal2",
            symbol="BTC-PERP",
            signal_type=SignalType.EXIT_LONG,
            side=OrderSide.SELL,
            price=Decimal("50100.0"),
            quantity=Decimal("1.0"),
            exchange="test_exchange",
            timestamp=datetime.now(UTC),
            source_strategy="test_strategy",
            metadata={"reason": "test"},
        )

        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")
        mock_strategy.process_data_return_value = [signal1, signal2]

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        assert mock_signal_queue.add_signal.call_count == 2

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_edge_symbol_not_active(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test processing data for symbol not in active symbols."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")

        # Create candle for different symbol
        candle = Candle(
            symbol="ETH-PERP",  # Different from strategy symbol
            open=Decimal("3000.0"),
            high=Decimal("3100.0"),
            low=Decimal("2900.0"),
            close=Decimal("3050.0"),
            volume=Decimal("50.0"),
            open_time=datetime.now(UTC),
            interval="1m",
        )

        # Act
        await strategy_manager.process_market_data(candle)

        # Assert
        assert len(mock_strategy.update_historical_data_calls) == 0
        assert len(mock_strategy.process_data_calls) == 0

    @pytest.mark.asyncio
    async def test_process_market_data_edge_no_enabled_strategies(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy, sample_candle: Candle
    ) -> None:
        """Test processing data when no strategies are enabled."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        # Don't enable the strategy

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        assert mock_strategy.update_historical_data_calls == [sample_candle]
        assert len(mock_strategy.process_data_calls) == 0  # Not called for disabled strategy

    @pytest.mark.asyncio
    async def test_process_market_data_edge_strategy_removed_during_processing(
        self, strategy_manager: StrategyManager, sample_candle: Candle
    ) -> None:
        """Test processing when strategy is removed during processing."""
        # Arrange
        strategy = Mock(spec=Strategy)
        strategy.name = "test_strategy"
        strategy.symbol = "BTC-PERP"
        strategy.update_historical_data = Mock()
        strategy.process_data = AsyncMock(return_value=None)

        strategy_manager.register_strategy(strategy)
        strategy_manager.enable_strategy("test_strategy")

        # Remove strategy from enabled_strategies during processing
        original_enabled = strategy_manager.enabled_strategies.copy()
        _ = original_enabled  # Used for context
        strategy_manager.enabled_strategies.clear()

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert - Should handle gracefully
        strategy.update_historical_data.assert_called_once()

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_failure_historical_data_error(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy, sample_candle: Candle
    ) -> None:
        """Test handling of historical data update errors."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")
        mock_strategy.update_historical_data_side_effect = ValueError("Historical data error")

        # Act & Assert
        with pytest.raises(ValueError, match="Historical data error"):
            await strategy_manager.process_market_data(sample_candle)

    @pytest.mark.asyncio
    async def test_process_market_data_failure_strategy_processing_error(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy, sample_candle: Candle
    ) -> None:
        """Test handling of strategy processing errors."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")
        mock_strategy.process_data_side_effect = RuntimeError("Processing error")

        # Act - Should not raise exception
        await strategy_manager.process_market_data(sample_candle)

        # Assert - Strategy was called but error was handled
        assert len(mock_strategy.process_data_calls) == 1

    """Test suite for get_strategies_for_symbol method."""

    # ==================== SUCCESS CASES ====================

    def test_get_strategies_for_symbol_success(self, strategy_manager: StrategyManager) -> None:
        """Test successful retrieval of strategies for symbol."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "BTC-PERP"

        strategy3 = Mock(spec=Strategy)
        strategy3.name = "strategy3"
        strategy3.symbol = "ETH-PERP"

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)
        strategy_manager.register_strategy(strategy3)
        strategy_manager.enable_strategy("strategy1")
        strategy_manager.enable_strategy("strategy2")
        strategy_manager.enable_strategy("strategy3")

        # Act
        btc_strategies = strategy_manager.get_strategies_for_symbol("BTC-PERP")

        # Assert
        assert len(btc_strategies) == 2
        assert strategy1 in btc_strategies
        assert strategy2 in btc_strategies
        assert strategy3 not in btc_strategies

    # ==================== EDGE CASES ====================

    def test_get_strategies_for_symbol_edge_no_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test retrieval when no strategies exist for symbol."""
        # Act
        strategies = strategy_manager.get_strategies_for_symbol("BTC-PERP")

        # Assert
        assert strategies == []

    def test_get_strategies_for_symbol_edge_disabled_strategies(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test retrieval excludes disabled strategies."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        # Don't enable the strategy

        # Act
        strategies = strategy_manager.get_strategies_for_symbol("BTC-PERP")

        # Assert
        assert strategies == []


class TestGetEnabledStrategies:
    """Test suite for get_enabled_strategies method."""

    # ==================== SUCCESS CASES ====================

    def test_get_enabled_strategies_success(self, strategy_manager: StrategyManager) -> None:
        """Test successful retrieval of enabled strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "ETH-PERP"

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)
        strategy_manager.enable_strategy("strategy1")

        # Act
        enabled_strategies = strategy_manager.get_enabled_strategies()

        # Assert
        assert len(enabled_strategies) == 1
        assert strategy1 in enabled_strategies
        assert strategy2 not in enabled_strategies

    # ==================== EDGE CASES ====================

    def test_get_enabled_strategies_edge_no_enabled(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test retrieval when no strategies are enabled."""
        # Act
        strategies = strategy_manager.get_enabled_strategies()

        # Assert
        assert strategies == []

    def test_get_enabled_strategies_edge_strategy_removed_but_enabled_list_not_updated(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test retrieval handles orphaned enabled strategy names."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")

        # Manually remove strategy but leave it in enabled list
        del strategy_manager.strategies["test_strategy"]

        # Act
        enabled_strategies = strategy_manager.get_enabled_strategies()

        # Assert
        assert enabled_strategies == []  # Should handle missing strategy gracefully


class TestGetStrategyPerformance:
    """Test suite for get_strategy_performance method."""

    # ==================== SUCCESS CASES ====================

    def test_get_strategy_performance_success(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test successful retrieval of strategy performance."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)

        # Act
        performance = strategy_manager.get_strategy_performance()

        # Assert
        assert "test_strategy" in performance
        assert performance["test_strategy"] == mock_strategy.performance_metrics

    def test_get_strategy_performance_success_multiple_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test performance retrieval for multiple strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"
        strategy1.performance_metrics = {"win_rate": 0.8, "trades": 50}

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "ETH-PERP"
        strategy2.performance_metrics = {"win_rate": 0.6, "trades": 30}

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)

        # Act
        performance = strategy_manager.get_strategy_performance()

        # Assert
        assert len(performance) == 2
        assert performance["strategy1"]["win_rate"] == 0.8
        assert performance["strategy2"]["win_rate"] == 0.6

    # ==================== EDGE CASES ====================

    def test_get_strategy_performance_edge_no_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test performance retrieval when no strategies exist."""
        # Act
        performance = strategy_manager.get_strategy_performance()

        # Assert
        assert performance == {}

    # ==================== FAILURE CASES ====================

    def test_get_strategy_performance_failure_metrics_error(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test handling of performance metrics access errors."""
        # Arrange
        strategy = Mock(spec=Strategy)
        strategy.name = "error_strategy"
        strategy.symbol = "BTC-PERP"
        # Mock the property to raise an exception when accessed
        type(strategy).performance_metrics = PropertyMock(side_effect=RuntimeError("Metrics error"))

        strategy_manager.register_strategy(strategy)

        # Act
        performance = strategy_manager.get_strategy_performance()

        # Assert
        assert "error_strategy" in performance
        assert "error" in performance["error_strategy"]


class TestStartAll:
    """Test suite for start_all method."""

    # ==================== SUCCESS CASES ====================

    def test_start_all_success(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test successful starting of all enabled strategies."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")

        # Act
        strategy_manager.start_all()

        # Assert
        assert mock_strategy.on_start_called is True

    def test_start_all_success_multiple_strategies(self, strategy_manager: StrategyManager) -> None:
        """Test starting multiple enabled strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"
        strategy1.on_start = Mock()

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "ETH-PERP"
        strategy2.on_start = Mock()

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)
        strategy_manager.enable_strategy("strategy1")
        strategy_manager.enable_strategy("strategy2")

        # Act
        strategy_manager.start_all()

        # Assert
        strategy1.on_start.assert_called_once()
        strategy2.on_start.assert_called_once()

    # ==================== EDGE CASES ====================

    def test_start_all_edge_no_enabled_strategies(self, strategy_manager: StrategyManager) -> None:
        """Test starting when no strategies are enabled."""
        # Act - Should not raise exception
        strategy_manager.start_all()

        # Assert - No exceptions should be raised

    def test_start_all_edge_enabled_strategy_removed(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test starting when enabled strategy is removed."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")
        del strategy_manager.strategies["test_strategy"]  # Remove but leave enabled

        # Act - Should handle gracefully
        strategy_manager.start_all()

        # Assert - No exceptions should be raised

    # ==================== FAILURE CASES ====================

    def test_start_all_failure_strategy_start_error(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test handling of strategy start errors."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")
        mock_strategy.on_start_side_effect = RuntimeError("Start error")

        # Act - Should not raise exception
        strategy_manager.start_all()

        # Assert - Error was handled gracefully
        assert mock_strategy.on_start_called is True


class TestStopAll:
    """Test suite for stop_all method."""

    # ==================== SUCCESS CASES ====================

    def test_stop_all_success(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test successful stopping of all strategies."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")

        # Act
        strategy_manager.stop_all()

        # Assert
        assert mock_strategy.on_stop_called is True
        assert mock_strategy.disable_called is True
        assert "test_strategy" not in strategy_manager.enabled_strategies

    # ==================== FAILURE CASES ====================

    def test_stop_all_failure_strategy_stop_error(
        self, strategy_manager: StrategyManager, mock_strategy: MockStrategy
    ) -> None:
        """Test handling of strategy stop errors."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy("test_strategy")
        mock_strategy.on_stop_side_effect = RuntimeError("Stop error")

        # Act - Should not raise exception
        strategy_manager.stop_all()

        # Assert - Error was handled gracefully
        assert mock_strategy.on_stop_called is True
