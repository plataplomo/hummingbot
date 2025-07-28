"""Comprehensive unit tests for the StrategyManager component.

Tests strategy management functionality including lifecycle management and signal processing.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.enums import SignalType
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import (
    TradeSignal,
)
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy
from cyberdelta.core.strategy_manager import StrategyManager
from cyberdelta.enums import OrderSide


@pytest.fixture
def mock_config() -> Mock:
    """Create mock configuration for testing.
    
    Returns:
        Mock: Mock AppSettings instance for testing.
    """
    return Mock(spec=AppSettings)


@pytest.fixture
def mock_execution_handler() -> Mock:
    """Create mock execution handler.
    
    Returns:
        Mock: Mock ExecutionHandler instance for testing.
    """
    return Mock(spec=ExecutionHandler)


@pytest.fixture
def mock_portfolio_tracker() -> Mock:
    """Create mock portfolio tracker.
    
    Returns:
        Mock: Mock PortfolioTracker instance for testing.
    """
    return Mock(spec=PortfolioTracker)


@pytest.fixture
def mock_risk_manager() -> Mock:
    """Create mock risk manager.
    
    Returns:
        Mock: Mock RiskManager instance for testing.
    """
    return Mock(spec=RiskManager)


@pytest.fixture
def mock_signal_queue() -> Mock:
    """Create mock signal queue.
    
    Returns:
        Mock: Mock PrioritySignalQueue instance with async add_signal method.
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
        StrategyManager: Configured StrategyManager instance with mocked dependencies.
    """
    return StrategyManager(
        config=mock_config,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )


@pytest.fixture
def mock_strategy() -> Mock:
    """Create mock strategy for testing.
    
    Returns:
        Mock: Mock Strategy instance with pre-configured attributes and methods.
    """
    strategy = Mock(spec=Strategy)
    strategy.name = "test_strategy"
    strategy.symbol = "BTC-PERP"
    strategy.enable = Mock()
    strategy.disable = Mock()
    strategy.on_start = Mock()
    strategy.on_stop = Mock()
    strategy.update_historical_data = Mock()
    strategy.process_data = AsyncMock(return_value=None)
    strategy.performance_metrics = {"win_rate": 0.75, "total_trades": 100}
    return strategy


@pytest.fixture
def sample_candle() -> Candle:
    """Create sample candle for testing.
    
    Returns:
        Candle: Sample Candle instance with test data for BTC-PERP.
    """
    return Candle(
        symbol="BTC-PERP",
        open=Decimal("50000.0"),
        high=Decimal("50100.0"),
        low=Decimal("49900.0"),
        close=Decimal("50050.0"),
        volume=Decimal("1000.0"),
        open_time=datetime.now(UTC),
        interval="1m",
    )


@pytest.fixture
def sample_trade_signal() -> TradeSignal:
    """Create sample trade signal for testing.
    
    Returns:
        TradeSignal: Sample TradeSignal instance with test data for entering a long position.
    """
    return TradeSignal(
        signal_id="test_signal_123",
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

    def test_init_success(
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

    def test_register_strategy_success_new_strategy(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test successful registration of a new strategy."""
        # Act
        strategy_manager.register_strategy(mock_strategy)

        # Assert
        assert mock_strategy.name in strategy_manager.strategies
        assert strategy_manager.strategies[mock_strategy.name] == mock_strategy
        assert mock_strategy.symbol in strategy_manager.active_symbols

    def test_register_strategy_success_replace_existing(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test successful replacement of existing strategy."""
        # Arrange
        old_strategy = Mock(spec=Strategy)
        old_strategy.name = mock_strategy.name
        old_strategy.symbol = "ETH-PERP"
        strategy_manager.strategies[old_strategy.name] = old_strategy

        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            strategy_manager.register_strategy(mock_strategy)

            # Assert
            assert strategy_manager.strategies[mock_strategy.name] == mock_strategy
            assert mock_strategy.symbol in strategy_manager.active_symbols
            mock_logger.warning.assert_called_once()

    # ==================== EDGE CASES ====================

    def test_register_strategy_edge_multiple_strategies_same_symbol(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test registering multiple strategies for the same symbol."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "BTC-PERP"

        # Act
        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)

        # Assert
        assert len(strategy_manager.strategies) == 2
        assert "BTC-PERP" in strategy_manager.active_symbols


class TestUnregisterStrategy:
    """Test suite for unregister_strategy method."""

    # ==================== SUCCESS CASES ====================

    def test_unregister_strategy_success(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test successful unregistration of strategy."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)
        strategy_manager.active_symbols.add(mock_strategy.symbol)

        # Act
        strategy_manager.unregister_strategy(mock_strategy.name)

        # Assert
        assert mock_strategy.name not in strategy_manager.strategies
        assert mock_strategy.name not in strategy_manager.enabled_strategies

    # ==================== EDGE CASES ====================

    def test_unregister_strategy_edge_not_found(self, strategy_manager: StrategyManager) -> None:
        """Test unregistering non-existent strategy."""
        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            strategy_manager.unregister_strategy("non_existent")

            # Assert
            mock_logger.warning.assert_called_once()

    def test_unregister_strategy_edge_last_strategy_for_symbol(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test unregistering last strategy for a symbol removes symbol from active."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.active_symbols.add(mock_strategy.symbol)

        # Act
        strategy_manager.unregister_strategy(mock_strategy.name)

        # Assert
        assert mock_strategy.symbol not in strategy_manager.active_symbols

    # ==================== FAILURE CASES ====================

    def test_unregister_strategy_failure_empty_name(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test unregistering with empty strategy name."""
        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            strategy_manager.unregister_strategy("")

            # Assert
            mock_logger.warning.assert_called_once()


class TestEnableDisableStrategy:
    """Test suite for enable_strategy and disable_strategy methods."""

    # ==================== SUCCESS CASES ====================

    def test_enable_strategy_success(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test successful enabling of strategy."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy

        # Act
        result = strategy_manager.enable_strategy(mock_strategy.name)

        # Assert
        assert result is True
        assert mock_strategy.name in strategy_manager.enabled_strategies
        mock_strategy.enable.assert_called_once()

    def test_disable_strategy_success(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test successful disabling of strategy."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)

        # Act
        result = strategy_manager.disable_strategy(mock_strategy.name)

        # Assert
        assert result is True
        assert mock_strategy.name not in strategy_manager.enabled_strategies
        mock_strategy.disable.assert_called_once()

    # ==================== EDGE CASES ====================

    def test_enable_strategy_edge_already_enabled(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test enabling already enabled strategy."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)

        # Act
        result = strategy_manager.enable_strategy(mock_strategy.name)

        # Assert
        assert result is True
        assert mock_strategy.name in strategy_manager.enabled_strategies

    def test_disable_strategy_edge_already_disabled(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test disabling already disabled strategy."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy

        # Act
        result = strategy_manager.disable_strategy(mock_strategy.name)

        # Assert
        assert result is True
        assert mock_strategy.name not in strategy_manager.enabled_strategies

    # ==================== FAILURE CASES ====================

    def test_enable_strategy_failure_not_found(self, strategy_manager: StrategyManager) -> None:
        """Test enabling non-existent strategy."""
        # Act
        result = strategy_manager.enable_strategy("non_existent")

        # Assert
        assert result is False

    def test_disable_strategy_failure_not_found(self, strategy_manager: StrategyManager) -> None:
        """Test disabling non-existent strategy."""
        # Act
        result = strategy_manager.disable_strategy("non_existent")

        # Assert
        assert result is False


class TestProcessMarketData:
    """Test suite for process_market_data method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_success_with_signal(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        sample_candle: Candle,
        sample_trade_signal: TradeSignal,
    ) -> None:
        """Test successful processing of market data with signal generation."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)
        strategy_manager.active_symbols.add(sample_candle.symbol)
        mock_strategy.process_data.return_value = sample_trade_signal

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        assert strategy_manager.last_update_time is not None
        mock_strategy.update_historical_data.assert_called_once_with(sample_candle)
        mock_strategy.process_data.assert_called_once_with(sample_candle)
        # Verify signal was added through the queue
        mock_signal_queue.add_signal.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_market_data_success_no_signal(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        sample_candle: Candle,
    ) -> None:
        """Test successful processing when strategy returns no signal."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)
        strategy_manager.active_symbols.add(sample_candle.symbol)
        mock_strategy.process_data.return_value = None

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        mock_strategy.update_historical_data.assert_called_once()
        mock_strategy.process_data.assert_called_once()
        # Verify no signal was added
        mock_signal_queue.add_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_market_data_success_multiple_signals(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        sample_candle: Candle,
        sample_trade_signal: TradeSignal,
    ) -> None:
        """Test successful processing when strategy returns multiple signals."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)
        strategy_manager.active_symbols.add(sample_candle.symbol)

        signal2 = TradeSignal(
            signal_id="test_signal_456",
            symbol="BTC-PERP",
            signal_type=SignalType.EXIT_LONG,
            side=OrderSide.SELL,
            price=Decimal("51000.0"),
            quantity=Decimal("1.0"),
            exchange="test_exchange",
            timestamp=datetime.now(UTC),
            source_strategy="test_strategy",
        )

        mock_strategy.process_data.return_value = [sample_trade_signal, signal2]

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        assert mock_signal_queue.add_signal.call_count == 2

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_edge_inactive_symbol(
        self,
        strategy_manager: StrategyManager,
        sample_candle: Candle,
    ) -> None:
        """Test processing data for inactive symbol."""
        # Arrange
        # Don't add symbol to active_symbols

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert - Should return early without processing
        assert strategy_manager.last_update_time is not None  # Still updates time

    @pytest.mark.asyncio
    async def test_process_market_data_edge_no_enabled_strategies(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_candle: Candle,
    ) -> None:
        """Test processing when no strategies are enabled."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        # Don't add to enabled_strategies
        strategy_manager.active_symbols.add(sample_candle.symbol)

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        mock_strategy.process_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_market_data_edge_wrong_symbol(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_candle: Candle,
    ) -> None:
        """Test processing data for wrong symbol."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)
        strategy_manager.active_symbols.add(sample_candle.symbol)
        mock_strategy.symbol = "ETH-PERP"  # Different symbol

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        mock_strategy.process_data.assert_not_called()

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_failure_historical_update_error(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_candle: Candle,
    ) -> None:
        """Test handling of error during historical data update."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)
        strategy_manager.active_symbols.add(sample_candle.symbol)
        mock_strategy.update_historical_data.side_effect = ValueError("Update error")

        # Act & Assert
        with pytest.raises(ValueError):
            await strategy_manager.process_market_data(sample_candle)

    @pytest.mark.asyncio
    async def test_process_market_data_failure_strategy_processing_error(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        sample_candle: Candle,
    ) -> None:
        """Test handling of error during strategy processing."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)
        strategy_manager.active_symbols.add(sample_candle.symbol)
        mock_strategy.process_data.side_effect = RuntimeError("Processing error")

        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            await strategy_manager.process_market_data(sample_candle)

            # Assert
            mock_logger.exception.assert_called_once()
            mock_signal_queue.add_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_market_data_failure_invalid_signal(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        sample_candle: Candle,
    ) -> None:
        """Test handling of invalid signal from strategy."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)
        strategy_manager.active_symbols.add(sample_candle.symbol)

        # Create invalid signal (missing required fields)
        invalid_signal = Mock()
        invalid_signal.symbol = None  # Missing required field
        mock_strategy.process_data.return_value = invalid_signal

        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            await strategy_manager.process_market_data(sample_candle)

            # Assert
            mock_logger.warning.assert_called()
            mock_signal_queue.add_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_market_data_failure_signal_queue_error(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        sample_candle: Candle,
        sample_trade_signal: TradeSignal,
    ) -> None:
        """Test handling of error when adding signal to queue."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)
        strategy_manager.active_symbols.add(sample_candle.symbol)
        mock_strategy.process_data.return_value = sample_trade_signal
        mock_signal_queue.add_signal.side_effect = RuntimeError("Queue error")

        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            await strategy_manager.process_market_data(sample_candle)

            # Assert
            mock_logger.exception.assert_called()


class TestGetStrategiesForSymbol:
    """Test suite for get_strategies_for_symbol method."""

    # ==================== SUCCESS CASES ====================

    def test_get_strategies_for_symbol_success_single_strategy(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test getting single strategy for symbol."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        strategy_manager.enabled_strategies.add(mock_strategy.name)

        # Act
        result = strategy_manager.get_strategies_for_symbol(mock_strategy.symbol)

        # Assert
        assert len(result) == 1
        assert result[0] == mock_strategy

    def test_get_strategies_for_symbol_success_multiple_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test getting multiple strategies for same symbol."""
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

        strategy_manager.strategies = {
            "strategy1": strategy1,
            "strategy2": strategy2,
            "strategy3": strategy3,
        }
        strategy_manager.enabled_strategies = {"strategy1", "strategy2", "strategy3"}

        # Act
        result = strategy_manager.get_strategies_for_symbol("BTC-PERP")

        # Assert
        assert len(result) == 2
        assert strategy1 in result
        assert strategy2 in result
        assert strategy3 not in result

    # ==================== EDGE CASES ====================

    def test_get_strategies_for_symbol_edge_no_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test getting strategies when none exist for symbol."""
        # Act
        result = strategy_manager.get_strategies_for_symbol("BTC-PERP")

        # Assert
        assert result == []

    def test_get_strategies_for_symbol_edge_disabled_strategies(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test that disabled strategies are not returned."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy
        # Don't add to enabled_strategies

        # Act
        result = strategy_manager.get_strategies_for_symbol(mock_strategy.symbol)

        # Assert
        assert result == []


class TestGetEnabledStrategies:
    """Test suite for get_enabled_strategies method."""

    # ==================== SUCCESS CASES ====================

    def test_get_enabled_strategies_success(self, strategy_manager: StrategyManager) -> None:
        """Test getting all enabled strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"

        strategy3 = Mock(spec=Strategy)
        strategy3.name = "strategy3"

        strategy_manager.strategies = {
            "strategy1": strategy1,
            "strategy2": strategy2,
            "strategy3": strategy3,
        }
        strategy_manager.enabled_strategies = {"strategy1", "strategy3"}

        # Act
        result = strategy_manager.get_enabled_strategies()

        # Assert
        assert len(result) == 2
        assert strategy1 in result
        assert strategy3 in result
        assert strategy2 not in result

    # ==================== EDGE CASES ====================

    def test_get_enabled_strategies_edge_no_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test getting enabled strategies when none exist."""
        # Act
        result = strategy_manager.get_enabled_strategies()

        # Assert
        assert result == []

    def test_get_enabled_strategies_edge_inconsistent_state(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test when enabled_strategies contains non-existent strategy."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"

        strategy_manager.strategies = {"strategy1": strategy1}
        strategy_manager.enabled_strategies = {"strategy1", "non_existent"}

        # Act
        result = strategy_manager.get_enabled_strategies()

        # Assert
        assert len(result) == 1
        assert strategy1 in result


class TestGetStrategyPerformance:
    """Test suite for get_strategy_performance method."""

    # ==================== SUCCESS CASES ====================

    def test_get_strategy_performance_success(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test successful retrieval of strategy performance metrics."""
        # Arrange
        strategy_manager.strategies[mock_strategy.name] = mock_strategy

        # Act
        result = strategy_manager.get_strategy_performance()

        # Assert
        assert mock_strategy.name in result
        assert result[mock_strategy.name] == mock_strategy.performance_metrics

    def test_get_strategy_performance_success_multiple_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test retrieving performance for multiple strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.performance_metrics = {"win_rate": 0.8, "total_trades": 50}

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.performance_metrics = {"win_rate": 0.6, "total_trades": 30}

        strategy_manager.strategies = {
            "strategy1": strategy1,
            "strategy2": strategy2,
        }

        # Act
        result = strategy_manager.get_strategy_performance()

        # Assert
        assert len(result) == 2
        assert result["strategy1"] == strategy1.performance_metrics
        assert result["strategy2"] == strategy2.performance_metrics

    # ==================== EDGE CASES ====================

    def test_get_strategy_performance_edge_no_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test performance retrieval when no strategies exist."""
        # Act
        result = strategy_manager.get_strategy_performance()

        # Assert
        assert result == {}

    # ==================== FAILURE CASES ====================

    def test_get_strategy_performance_failure_attribute_error(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test handling of strategy without performance_metrics attribute."""
        # Arrange
        bad_strategy = Mock(spec=Strategy)
        bad_strategy.name = "bad_strategy"
        del bad_strategy.performance_metrics  # Remove the attribute

        strategy_manager.strategies = {"bad_strategy": bad_strategy}

        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            result = strategy_manager.get_strategy_performance()

            # Assert
            assert "bad_strategy" in result
            assert result["bad_strategy"] == {"error": "Failed to retrieve metrics"}
            mock_logger.exception.assert_called_once()

    def test_get_strategy_performance_failure_property_error(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test handling of error when accessing performance_metrics property."""
        # Arrange
        error_strategy = Mock(spec=Strategy)
        error_strategy.name = "error_strategy"
        type(error_strategy).performance_metrics = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("Metrics error"))
        )

        strategy_manager.strategies = {"error_strategy": error_strategy}

        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            result = strategy_manager.get_strategy_performance()

            # Assert
            assert "error_strategy" in result
            assert result["error_strategy"] == {"error": "Failed to retrieve metrics"}
            mock_logger.exception.assert_called_once()


class TestStartStopMethods:
    """Test suite for start_all and stop_all methods."""

    # ==================== SUCCESS CASES ====================

    def test_start_all_success(self, strategy_manager: StrategyManager) -> None:
        """Test successful starting of all enabled strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.on_start = Mock()

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.on_start = Mock()

        strategy3 = Mock(spec=Strategy)
        strategy3.name = "strategy3"
        strategy3.on_start = Mock()

        strategy_manager.strategies = {
            "strategy1": strategy1,
            "strategy2": strategy2,
            "strategy3": strategy3,
        }
        strategy_manager.enabled_strategies = {"strategy1", "strategy3"}

        # Act
        strategy_manager.start_all()

        # Assert
        strategy1.on_start.assert_called_once()
        strategy2.on_start.assert_not_called()  # Not enabled
        strategy3.on_start.assert_called_once()

    def test_stop_all_success(self, strategy_manager: StrategyManager) -> None:
        """Test successful stopping of all strategies."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.on_stop = Mock()
        strategy1.disable = Mock()

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.on_stop = Mock()
        strategy2.disable = Mock()

        strategy_manager.strategies = {
            "strategy1": strategy1,
            "strategy2": strategy2,
        }
        strategy_manager.enabled_strategies = {"strategy1", "strategy2"}

        # Act
        strategy_manager.stop_all()

        # Assert
        strategy1.on_stop.assert_called_once()
        strategy2.on_stop.assert_called_once()
        strategy1.disable.assert_called_once()
        strategy2.disable.assert_called_once()
        assert len(strategy_manager.enabled_strategies) == 0

    # ==================== EDGE CASES ====================

    def test_start_all_edge_no_enabled_strategies(self, strategy_manager: StrategyManager) -> None:
        """Test starting when no strategies are enabled."""
        # Arrange
        strategy = Mock(spec=Strategy)
        strategy.name = "strategy"
        strategy.on_start = Mock()

        strategy_manager.strategies = {"strategy": strategy}
        # No enabled strategies

        # Act
        strategy_manager.start_all()

        # Assert
        strategy.on_start.assert_not_called()

    def test_stop_all_edge_no_strategies(self, strategy_manager: StrategyManager) -> None:
        """Test stopping when no strategies exist."""
        # Act
        strategy_manager.stop_all()

        # Assert - Should complete without error

    # ==================== FAILURE CASES ====================

    def test_start_all_failure_strategy_error(self, strategy_manager: StrategyManager) -> None:
        """Test handling of error when starting strategy."""
        # Arrange
        good_strategy = Mock(spec=Strategy)
        good_strategy.name = "good_strategy"
        good_strategy.on_start = Mock()

        bad_strategy = Mock(spec=Strategy)
        bad_strategy.name = "bad_strategy"
        bad_strategy.on_start = Mock(side_effect=RuntimeError("Start error"))

        strategy_manager.strategies = {
            "good_strategy": good_strategy,
            "bad_strategy": bad_strategy,
        }
        strategy_manager.enabled_strategies = {"good_strategy", "bad_strategy"}

        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            strategy_manager.start_all()

            # Assert
            good_strategy.on_start.assert_called_once()
            bad_strategy.on_start.assert_called_once()
            mock_logger.exception.assert_called_once()

    def test_stop_all_failure_strategy_error(self, strategy_manager: StrategyManager) -> None:
        """Test handling of error when stopping strategy."""
        # Arrange
        good_strategy = Mock(spec=Strategy)
        good_strategy.name = "good_strategy"
        good_strategy.on_stop = Mock()
        good_strategy.disable = Mock()

        bad_strategy = Mock(spec=Strategy)
        bad_strategy.name = "bad_strategy"
        bad_strategy.on_stop = Mock(side_effect=RuntimeError("Stop error"))
        bad_strategy.disable = Mock()

        strategy_manager.strategies = {
            "good_strategy": good_strategy,
            "bad_strategy": bad_strategy,
        }
        strategy_manager.enabled_strategies = {"good_strategy", "bad_strategy"}

        # Act
        with patch("cyberdelta.core.strategy_manager.logger") as mock_logger:
            strategy_manager.stop_all()

            # Assert
            good_strategy.on_stop.assert_called_once()
            bad_strategy.on_stop.assert_called_once()
            mock_logger.exception.assert_called_once()
            # Should still disable strategies even if stop fails
            good_strategy.disable.assert_called_once()


class TestOnMarketData:
    """Test suite for on_market_data method."""

    @pytest.mark.asyncio
    async def test_on_market_data_success(
        self,
        strategy_manager: StrategyManager,
        sample_candle: Candle,
    ) -> None:
        """Test on_market_data delegates to process_market_data."""
        # Arrange
        with patch.object(
            strategy_manager,
            "process_market_data",
            new_callable=AsyncMock,
        ) as mock_process:
            # Act
            await strategy_manager.on_market_data(sample_candle)

            # Assert
            mock_process.assert_called_once_with(sample_candle)


# ==================== INTEGRATION TESTS ====================


class TestStrategyManagerIntegration:
    """Integration tests for StrategyManager."""

    @pytest.mark.asyncio
    async def test_full_signal_processing_workflow(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        sample_candle: Candle,
        sample_trade_signal: TradeSignal,
    ) -> None:
        """Test complete workflow from market data to signal queue."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)
        mock_strategy.process_data.return_value = sample_trade_signal

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        # Verify complete flow
        assert mock_strategy.name in strategy_manager.strategies
        assert mock_strategy.name in strategy_manager.enabled_strategies
        assert sample_candle.symbol in strategy_manager.active_symbols
        mock_strategy.update_historical_data.assert_called_once_with(sample_candle)
        mock_strategy.process_data.assert_called_once_with(sample_candle)
        # Verify signal was added through the queue
        mock_signal_queue.add_signal.assert_called_once()

        # Verify signal was processed correctly
        call_args = mock_signal_queue.add_signal.call_args
        assert call_args[0][0] == sample_trade_signal

    @pytest.mark.asyncio
    async def test_multiple_strategies_coordination(
        self, strategy_manager: StrategyManager, mock_signal_queue: Mock, sample_candle: Candle
    ) -> None:
        """Test coordination of multiple strategies for same symbol."""
        # Arrange
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = "BTC-PERP"
        strategy1.enable = Mock()
        strategy1.update_historical_data = Mock()
        strategy1.process_data = AsyncMock(return_value=None)

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = "BTC-PERP"
        strategy2.enable = Mock()
        strategy2.update_historical_data = Mock()
        signal2 = Mock()
        signal2.symbol = "BTC-PERP"
        signal2.signal_type = SignalType.ENTER_LONG
        signal2.side = OrderSide.BUY
        signal2.price = Decimal(50000)
        signal2.quantity = Decimal("1.0")
        strategy2.process_data = AsyncMock(return_value=signal2)

        strategy3 = Mock(spec=Strategy)
        strategy3.name = "strategy3"
        strategy3.symbol = "ETH-PERP"  # Different symbol
        strategy3.enable = Mock()
        strategy3.update_historical_data = Mock()
        strategy3.process_data = AsyncMock()

        # Register and enable all strategies
        for strategy in [strategy1, strategy2, strategy3]:
            strategy_manager.register_strategy(strategy)
            strategy_manager.enable_strategy(strategy.name)

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        # Only BTC strategies should process
        strategy1.update_historical_data.assert_called_once()
        strategy1.process_data.assert_called_once()
        strategy2.update_historical_data.assert_called_once()
        strategy2.process_data.assert_called_once()
        strategy3.update_historical_data.assert_not_called()
        strategy3.process_data.assert_not_called()

        # Only strategy2's signal should be added to queue
        assert mock_signal_queue.add_signal.call_count == 1

    def test_lifecycle_management(self, strategy_manager: StrategyManager) -> None:
        """Test complete strategy lifecycle management."""
        # Arrange
        strategy = Mock(spec=Strategy)
        strategy.name = "lifecycle_strategy"
        strategy.symbol = "BTC-PERP"
        strategy.enable = Mock()
        strategy.disable = Mock()
        strategy.on_start = Mock()
        strategy.on_stop = Mock()
        strategy.performance_metrics = {"win_rate": 0.7}

        # Test registration
        strategy_manager.register_strategy(strategy)
        assert strategy.name in strategy_manager.strategies

        # Test enabling
        result = strategy_manager.enable_strategy(strategy.name)
        assert result is True
        assert strategy.name in strategy_manager.enabled_strategies
        strategy.enable.assert_called_once()

        # Test starting
        strategy_manager.start_all()
        strategy.on_start.assert_called_once()

        # Test performance retrieval
        perf = strategy_manager.get_strategy_performance()
        assert strategy.name in perf
        assert perf[strategy.name]["win_rate"] == 0.7

        # Test stopping
        strategy_manager.stop_all()
        strategy.on_stop.assert_called_once()
        strategy.disable.assert_called_once()
        assert strategy.name not in strategy_manager.enabled_strategies

        # Test unregistration
        strategy_manager.unregister_strategy(strategy.name)
        assert strategy.name not in strategy_manager.strategies
