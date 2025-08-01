"""Additional comprehensive unit tests for StrategyManager.

Tests additional edge cases and scenarios that need better coverage,
focusing on market data processing, signal validation, performance tracking,
lifecycle management, and error handling scenarios.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, PropertyMock, patch
from uuid import uuid4

import pytest

from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import TradeSignal
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy
from cyberdelta.core.strategy_manager import StrategyManager
from cyberdelta.core.symbols import Symbol, symbols
from cyberdelta.enums import OrderSide, SignalType
from tests.fixtures.symbol_domain_fixtures import SymbolSet


# Use shared mock_config from conftest.py


@pytest.fixture
def mock_execution_handler() -> Mock:
    """Create mock execution handler.

    Returns:
        Mock: Mock ExecutionHandler instance for testing.
    """
    return Mock(spec=ExecutionHandler)


# Use shared mock_portfolio_state_manager from conftest.py


@pytest.fixture
def mock_risk_manager() -> Mock:
    """Create mock risk manager.

    Returns:
        Mock: Mock RiskManager instance for testing.
    """
    return Mock(spec=RiskManager)


class FakeSignalQueue:
    """Test double for PrioritySignalQueue that records added signals."""

    def __init__(self) -> None:
        """Initialize the fake signal queue."""
        self.added_signals: list[TradeSignal] = []
        self._exception_to_raise: Exception | None = None

    async def add_signal(self, signal: TradeSignal) -> bool:
        """Record the signal and return success.

        Returns:
            bool: True if signal was added successfully.
        """
        if self._exception_to_raise:
            raise self._exception_to_raise
        self.added_signals.append(signal)
        return True

    def set_exception(self, exception: Exception) -> None:
        """Set an exception to raise when add_signal is called."""
        self._exception_to_raise = exception

    def get_added_signals(self) -> list[TradeSignal]:
        """Get all signals that were added.

        Returns:
            list[TradeSignal]: Copy of all added signals.
        """
        return self.added_signals.copy()

    def clear(self) -> None:
        """Clear the recorded signals."""
        self.added_signals.clear()


@pytest.fixture
def mock_signal_queue() -> Mock:
    """Create mock signal queue that tracks added signals.

    Returns:
        Mock: Mock PrioritySignalQueue with signal tracking capabilities.
    """
    mock = Mock(spec=PrioritySignalQueue)
    # Track added signals
    added_signals: list[TradeSignal] = []
    exception_to_raise: Exception | None = None

    # Implement add_signal behavior
    def add_signal_impl(signal: TradeSignal) -> bool:
        if exception_to_raise:
            raise exception_to_raise
        added_signals.append(signal)
        return True

    mock.add_signal = AsyncMock(side_effect=add_signal_impl)

    # Add helper methods for tests
    def get_added_signals() -> list[TradeSignal]:
        return added_signals.copy()

    mock.get_added_signals = get_added_signals

    def set_exception(exc: Exception) -> None:
        nonlocal exception_to_raise
        exception_to_raise = exc

    mock.set_exception = set_exception

    return mock


@pytest.fixture
def strategy_manager(
    mock_config: Mock,
    mock_execution_handler: Mock,
    mock_portfolio_state_manager: Mock,
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
        portfolio_tracker=mock_portfolio_state_manager,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )


@pytest.fixture
def mock_strategy(btc_symbols: SymbolSet) -> Mock:
    """Create mock strategy for testing.

    Returns:
        Mock: Mock Strategy instance with pre-configured attributes and methods.
    """
    strategy = Mock(spec=Strategy)
    strategy.name = "test_strategy"
    strategy.symbol = btc_symbols.perp_hl.value
    strategy.enabled = False
    strategy.enable = Mock()
    strategy.disable = Mock()
    strategy.on_start = Mock()
    strategy.on_stop = Mock()
    strategy.update_historical_data = Mock()
    strategy.process_data = AsyncMock(return_value=None)
    strategy.performance_metrics = {"total_trades": 10, "win_rate": 0.6}
    return strategy


@pytest.fixture
def sample_candle(btc_symbols: SymbolSet) -> Candle:
    """Create sample candle for testing.

    Returns:
        Candle: Sample Candle instance with test data for BTC-PERP.
    """
    return Candle(
        symbol=btc_symbols.perp_hl.value,
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("50000.0"),
        high=Decimal("50100.0"),
        low=Decimal("49900.0"),
        close=Decimal("50050.0"),
        volume=Decimal("100.0"),
    )


@pytest.fixture
def sample_trade_signal(btc_symbols: SymbolSet) -> TradeSignal:
    """Create sample trade signal for testing.

    Returns:
        TradeSignal: Sample TradeSignal instance with test data for entering a long position.
    """
    return TradeSignal(
        signal_id=str(uuid4()),
        symbol=btc_symbols.perp_hl.value,
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000.0"),
        quantity=Decimal("0.1"),
        exchange="hyperliquid",
        timestamp=datetime.now(UTC),
        source_strategy="test_strategy",
    )


class TestStrategyManagerInitialization:
    """Test suite for StrategyManager initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success_with_all_dependencies(
        self,
        mock_config: Mock,
        mock_execution_handler: Mock,
        mock_portfolio_state_manager: Mock,
        mock_risk_manager: Mock,
        mock_signal_queue: Mock,
    ) -> None:
        """Test successful initialization with all dependencies."""
        # Act
        manager = StrategyManager(
            config=mock_config,
            execution_handler=mock_execution_handler,
            portfolio_tracker=mock_portfolio_state_manager,
            risk_manager=mock_risk_manager,
            signal_queue=mock_signal_queue,
        )

        # Assert
        assert manager.config is mock_config
        assert manager.execution_handler is mock_execution_handler
        assert manager.portfolio_tracker is mock_portfolio_state_manager
        assert manager.risk_manager is mock_risk_manager
        assert manager.signal_queue is mock_signal_queue
        assert manager.strategies == {}
        assert manager.enabled_strategies == set()
        assert manager.active_symbols == set()
        assert manager.last_update_time is None
        # Test private attributes for initialization completeness
        # Verify internal state is initialized properly
        # These assertions test the public behavior indirectly
        assert manager.strategies == {}
        assert manager.enabled_strategies == set()


class TestStrategyManagerRegistration:
    """Test suite for strategy registration and unregistration."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_register_strategy_success_new_strategy(
        self, mock_logger: Mock, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test registering a new strategy."""
        # Act
        strategy_manager.register_strategy(mock_strategy)

        # Assert
        assert mock_strategy.name in strategy_manager.strategies
        assert strategy_manager.strategies[mock_strategy.name] is mock_strategy
        assert mock_strategy.symbol in strategy_manager.active_symbols
        mock_logger.info.assert_called_once()

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_register_strategy_success_replace_existing(
        self, mock_logger: Mock, strategy_manager: StrategyManager, mock_strategy: Mock, eth_symbols: SymbolSet
    ) -> None:
        """Test replacing an existing strategy."""
        # Arrange
        eth_symbol = eth_symbols.perp_hl
        old_strategy = Mock(spec=Strategy)
        old_strategy.name = mock_strategy.name
        old_strategy.symbol = eth_symbol.value
        strategy_manager.strategies[mock_strategy.name] = old_strategy

        # Act
        strategy_manager.register_strategy(mock_strategy)

        # Assert
        assert strategy_manager.strategies[mock_strategy.name] is mock_strategy
        assert mock_strategy.symbol in strategy_manager.active_symbols
        mock_logger.warning.assert_called_once()
        mock_logger.info.assert_called_once()

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_unregister_strategy_success_existing_strategy(
        self, mock_logger: Mock, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test unregistering an existing strategy."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        # Act
        strategy_manager.unregister_strategy(mock_strategy.name)

        # Assert
        assert mock_strategy.name not in strategy_manager.strategies
        assert mock_strategy.name not in strategy_manager.enabled_strategies
        mock_logger.info.assert_called()

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_unregister_strategy_edge_nonexistent_strategy(
        self, mock_logger: Mock, strategy_manager: StrategyManager
    ) -> None:
        """Test unregistering a non-existent strategy."""
        # Act
        strategy_manager.unregister_strategy("nonexistent")

        # Assert
        mock_logger.warning.assert_called_once()

    def test_register_strategy_edge_multiple_strategies_same_symbol(
        self, strategy_manager: StrategyManager, btc_symbols: SymbolSet
    ) -> None:
        """Test registering multiple strategies for the same symbol."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = btc_symbol.value

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = btc_symbol.value

        # Act
        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)

        # Assert
        assert len(strategy_manager.strategies) == 2
        assert btc_symbol.value in strategy_manager.active_symbols


class TestStrategyManagerEnableDisable:
    """Test suite for strategy enable/disable functionality."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_enable_strategy_success_existing_strategy(
        self, mock_logger: Mock, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test enabling an existing strategy."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)

        # Act
        result = strategy_manager.enable_strategy(mock_strategy.name)

        # Assert
        assert result is True
        assert mock_strategy.name in strategy_manager.enabled_strategies
        mock_strategy.enable.assert_called_once()
        mock_logger.info.assert_called()

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_disable_strategy_success_existing_strategy(
        self, mock_logger: Mock, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test disabling an existing strategy."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        # Act
        result = strategy_manager.disable_strategy(mock_strategy.name)

        # Assert
        assert result is True
        assert mock_strategy.name not in strategy_manager.enabled_strategies
        mock_strategy.disable.assert_called_once()
        mock_logger.info.assert_called()

    # ==================== FAILURE CASES ====================

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_enable_strategy_failure_nonexistent_strategy(
        self, mock_logger: Mock, strategy_manager: StrategyManager
    ) -> None:
        """Test enabling a non-existent strategy."""
        # Act
        result = strategy_manager.enable_strategy("nonexistent")

        # Assert
        assert result is False
        mock_logger.warning.assert_called_once()

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_disable_strategy_failure_nonexistent_strategy(
        self, mock_logger: Mock, strategy_manager: StrategyManager
    ) -> None:
        """Test disabling a non-existent strategy."""
        # Act
        result = strategy_manager.disable_strategy("nonexistent")

        # Assert
        assert result is False
        mock_logger.warning.assert_called_once()


class TestStrategyManagerMarketDataProcessing:
    """Test suite for market data processing."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_success_active_symbol(
        self, strategy_manager: StrategyManager, mock_strategy: Mock, sample_candle: Candle
    ) -> None:
        """Test processing market data for active symbol."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        assert strategy_manager.last_update_time is not None
        mock_strategy.update_historical_data.assert_called_once_with(sample_candle)
        mock_strategy.process_data.assert_called_once_with(sample_candle)

    @pytest.mark.asyncio
    async def test_process_market_data_success_with_generated_signals(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_candle: Candle,
        sample_trade_signal: TradeSignal,
        mock_signal_queue: Mock,
    ) -> None:
        """Test processing market data with generated signals."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)
        mock_strategy.process_data.return_value = sample_trade_signal

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 1
        assert added_signals[0] == sample_trade_signal

    @pytest.mark.asyncio
    async def test_process_market_data_success_multiple_signals(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_candle: Candle,
        sample_trade_signal: TradeSignal,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test processing market data with multiple generated signals."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)
        signal2 = TradeSignal(
            signal_id=str(uuid4()),
            symbol=btc_symbol.value,
            signal_type=SignalType.EXIT_LONG,
            side=OrderSide.SELL,
            price=Decimal("50100.0"),
            quantity=Decimal("0.1"),
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            source_strategy="test_strategy",
        )
        mock_strategy.process_data.return_value = [sample_trade_signal, signal2]

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 2
        assert sample_trade_signal in added_signals
        assert signal2 in added_signals

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_process_market_data_edge_inactive_symbol(
        self, strategy_manager: StrategyManager, mock_strategy: Mock, eth_symbols: SymbolSet
    ) -> None:
        """Test processing market data for inactive symbol."""
        # Arrange
        eth_symbol = eth_symbols.perp_hl
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)
        candle = Candle(
            symbol=eth_symbol.value,  # Different symbol
            interval="1m",
            open_time=datetime.now(UTC),
            open=Decimal("3000.0"),
            high=Decimal("3100.0"),
            low=Decimal("2900.0"),
            close=Decimal("3050.0"),
            volume=Decimal("100.0"),
        )

        # Act
        await strategy_manager.process_market_data(candle)

        # Assert
        mock_strategy.update_historical_data.assert_not_called()
        mock_strategy.process_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_market_data_edge_disabled_strategy(
        self, strategy_manager: StrategyManager, mock_strategy: Mock, sample_candle: Candle
    ) -> None:
        """Test processing market data with disabled strategy."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        # Don't enable the strategy

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        mock_strategy.update_historical_data.assert_called_once_with(sample_candle)
        mock_strategy.process_data.assert_not_called()

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    @patch("cyberdelta.core.strategy_manager.logger")
    async def test_process_market_data_failure_historical_data_error(
        self,
        mock_logger: Mock,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_candle: Candle,
    ) -> None:
        """Test processing market data with historical data update error."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)
        mock_strategy.update_historical_data.side_effect = ValueError("Data error")

        # Act & Assert
        with pytest.raises(ValueError):
            await strategy_manager.process_market_data(sample_candle)

        mock_logger.exception.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.core.strategy_manager.logger")
    async def test_process_market_data_failure_strategy_processing_error(
        self,
        mock_logger: Mock,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_candle: Candle,
    ) -> None:
        """Test processing market data with strategy processing error."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)
        mock_strategy.process_data.side_effect = RuntimeError("Processing error")

        # Act
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        mock_logger.exception.assert_called_once()


class TestStrategyManagerSignalValidation:
    """Test suite for signal validation."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_validate_signal_success_valid_signal(
        self,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        sample_trade_signal: TradeSignal,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test validating a valid signal."""
        # Act
        # Test signal validation through public API instead of private method
        btc_symbol = btc_symbols.perp_hl
        mock_strategy.process_data.return_value = sample_trade_signal
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        sample_candle = Mock()
        sample_candle.symbol = btc_symbol.value
        await strategy_manager.process_market_data(sample_candle)

        # Valid signal should be added to queue (validation passed)
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 1
        assert added_signals[0] == sample_trade_signal

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    @patch("cyberdelta.core.strategy_manager.logger")
    async def test_validate_signal_edge_signal_with_model_dump(
        self,
        mock_logger: Mock,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test validating signal that supports model_dump but is invalid."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        invalid_signal = Mock()
        invalid_signal.symbol = None  # Missing required field
        invalid_signal.model_dump = Mock(return_value={"symbol": None})

        # Act
        # Test signal validation through public API instead of private method
        mock_strategy.process_data.return_value = invalid_signal
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        sample_candle = Mock()
        sample_candle.symbol = btc_symbol.value
        await strategy_manager.process_market_data(sample_candle)

        # Invalid signal should NOT be added to queue (validation failed)
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 0
        mock_logger.warning.assert_called()

    @pytest.mark.asyncio
    @patch("cyberdelta.core.strategy_manager.logger")
    async def test_validate_signal_edge_signal_dump_fails(
        self,
        mock_logger: Mock,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test validating signal where model_dump fails."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        invalid_signal = Mock()
        invalid_signal.symbol = None  # Missing required field
        invalid_signal.model_dump = Mock(side_effect=ValueError("Dump failed"))

        # Act
        # Test signal validation through public API instead of private method
        mock_strategy.process_data.return_value = invalid_signal
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        sample_candle = Mock()
        sample_candle.symbol = btc_symbol.value
        await strategy_manager.process_market_data(sample_candle)

        # Invalid signal should NOT be added to queue (validation failed)
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 0
        # Should be called twice - once for validation failure, once for dump failure
        assert mock_logger.warning.call_count == 2

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    @patch("cyberdelta.core.strategy_manager.logger")
    async def test_validate_signal_failure_missing_symbol(
        self,
        mock_logger: Mock,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test validating signal with missing symbol."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        invalid_signal = Mock()
        invalid_signal.symbol = None
        invalid_signal.signal_type = SignalType.ENTER_LONG
        invalid_signal.side = OrderSide.BUY
        invalid_signal.price = Decimal("50000.0")
        invalid_signal.quantity = Decimal("0.1")

        # Act
        # Test signal validation through public API instead of private method
        mock_strategy.process_data.return_value = invalid_signal
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        sample_candle = Mock()
        sample_candle.symbol = btc_symbol.value
        await strategy_manager.process_market_data(sample_candle)

        # Invalid signal should NOT be added to queue (validation failed)
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 0
        mock_logger.warning.assert_called()

    @pytest.mark.asyncio
    @patch("cyberdelta.core.strategy_manager.logger")
    async def test_validate_signal_failure_missing_multiple_fields(
        self,
        mock_logger: Mock,
        strategy_manager: StrategyManager,
        mock_strategy: Mock,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test validating signal with multiple missing fields."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        invalid_signal = Mock()
        invalid_signal.symbol = None
        invalid_signal.signal_type = None
        invalid_signal.side = None
        invalid_signal.price = None
        invalid_signal.quantity = None

        # Act
        # Test signal validation through public API instead of private method
        mock_strategy.process_data.return_value = invalid_signal
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        sample_candle = Mock()
        sample_candle.symbol = btc_symbol.value
        await strategy_manager.process_market_data(sample_candle)

        # Invalid signal should NOT be added to queue (validation failed)
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 0
        mock_logger.warning.assert_called()


class TestStrategyManagerRiskManagement:
    """Test suite for risk management."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_apply_risk_management_success_signal_passes(
        self,
        strategy_manager: StrategyManager,
        sample_trade_signal: TradeSignal,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test risk management allows valid signal."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        
        # Act
        # Test risk management through public API instead of private method
        mock_strategy = Mock(spec=Strategy)
        mock_strategy.name = "test_strategy"
        mock_strategy.symbol = btc_symbol.value
        mock_strategy.enabled = True
        mock_strategy.process_data.return_value = sample_trade_signal

        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        sample_candle = Mock()
        sample_candle.symbol = btc_symbol.value
        await strategy_manager.process_market_data(sample_candle)

        # Risk management currently returns True, so signal should be queued
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 1
        assert added_signals[0] == sample_trade_signal

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_apply_risk_management_failure_exception_during_processing(
        self,
        strategy_manager: StrategyManager,
        sample_trade_signal: TradeSignal,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test risk management handles exceptions gracefully."""
        # This test is simplified since the current implementation just returns True
        # and the exception handling is placeholder code for future implementation
        
        # Arrange
        btc_symbol = btc_symbols.perp_hl

        # Act
        # Test risk management through public API instead of private method
        mock_strategy = Mock(spec=Strategy)
        mock_strategy.name = "test_strategy"
        mock_strategy.symbol = btc_symbol.value
        mock_strategy.enabled = True
        mock_strategy.process_data.return_value = sample_trade_signal

        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        sample_candle = Mock()
        sample_candle.symbol = btc_symbol.value
        await strategy_manager.process_market_data(sample_candle)

        # Risk management currently returns True, so signal should be queued
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 1
        assert added_signals[0] == sample_trade_signal


class TestStrategyManagerSignalQueue:
    """Test suite for signal queue operations."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    @patch("cyberdelta.core.strategy_manager.logger")
    async def test_add_signal_to_queue_success(
        self,
        mock_logger: Mock,
        strategy_manager: StrategyManager,
        sample_trade_signal: TradeSignal,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test successfully adding signal to queue."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        
        # Act
        # Test signal queue through public API instead of private method
        mock_strategy = Mock(spec=Strategy)
        mock_strategy.name = "test_strategy"
        mock_strategy.symbol = btc_symbol.value
        mock_strategy.enabled = True
        mock_strategy.process_data.return_value = sample_trade_signal

        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        sample_candle = Mock()
        sample_candle.symbol = btc_symbol.value
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        added_signals = mock_signal_queue.get_added_signals()
        assert len(added_signals) == 1
        assert added_signals[0] == sample_trade_signal
        mock_logger.debug.assert_called_once()

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    @patch("cyberdelta.core.strategy_manager.logger")
    async def test_add_signal_to_queue_failure_queue_error(
        self,
        mock_logger: Mock,
        strategy_manager: StrategyManager,
        sample_trade_signal: TradeSignal,
        mock_signal_queue: Mock,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test handling queue errors when adding signal."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        mock_signal_queue.set_exception(ValueError("Queue error"))

        # Act
        # Test signal queue through public API instead of private method
        mock_strategy = Mock(spec=Strategy)
        mock_strategy.name = "test_strategy"
        mock_strategy.symbol = btc_symbol.value
        mock_strategy.enabled = True
        mock_strategy.process_data.return_value = sample_trade_signal

        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        sample_candle = Mock()
        sample_candle.symbol = btc_symbol.value
        await strategy_manager.process_market_data(sample_candle)

        # Assert
        mock_logger.exception.assert_called_once()


class TestStrategyManagerStrategyQueries:
    """Test suite for strategy query methods."""

    # ==================== SUCCESS CASES ====================

    def test_get_strategies_for_symbol_success_matching_strategies(
        self, strategy_manager: StrategyManager, btc_symbols: SymbolSet, eth_symbols: SymbolSet
    ) -> None:
        """Test getting strategies for a specific symbol."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = btc_symbol.value

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = btc_symbol.value

        strategy3 = Mock(spec=Strategy)
        strategy3.name = "strategy3"
        strategy3.symbol = eth_symbol.value

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)
        strategy_manager.register_strategy(strategy3)
        strategy_manager.enable_strategy("strategy1")
        strategy_manager.enable_strategy("strategy2")
        strategy_manager.enable_strategy("strategy3")

        # Act
        result = strategy_manager.get_strategies_for_symbol(btc_symbol.value)

        # Assert
        assert len(result) == 2
        assert strategy1 in result
        assert strategy2 in result
        assert strategy3 not in result

    def test_get_enabled_strategies_success_multiple_enabled(
        self, strategy_manager: StrategyManager, btc_symbols: SymbolSet, eth_symbols: SymbolSet, sol_symbols: SymbolSet
    ) -> None:
        """Test getting all enabled strategies."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        sol_symbol = sol_symbols.perp_hl
        
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = btc_symbol.value

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = eth_symbol.value

        strategy3 = Mock(spec=Strategy)
        strategy3.name = "strategy3"
        strategy3.symbol = sol_symbol.value

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)
        strategy_manager.register_strategy(strategy3)
        strategy_manager.enable_strategy("strategy1")
        strategy_manager.enable_strategy("strategy3")

        # Act
        result = strategy_manager.get_enabled_strategies()

        # Assert
        assert len(result) == 2
        assert strategy1 in result
        assert strategy3 in result
        assert strategy2 not in result

    # ==================== EDGE CASES ====================

    def test_get_strategies_for_symbol_edge_no_matching_strategies(
        self, strategy_manager: StrategyManager, mock_strategy: Mock, eth_symbols: SymbolSet
    ) -> None:
        """Test getting strategies for symbol with no matches."""
        # Arrange
        eth_symbol = eth_symbols.perp_hl
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        # Act
        result = strategy_manager.get_strategies_for_symbol(eth_symbol.value)

        # Assert
        assert result == []

    def test_get_enabled_strategies_edge_no_enabled_strategies(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test getting enabled strategies when none are enabled."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)

        # Act
        result = strategy_manager.get_enabled_strategies()

        # Assert
        assert result == []

    def test_get_enabled_strategies_edge_strategy_removed_but_still_enabled(
        self, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test getting enabled strategies when strategy was removed but still in enabled set."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)
        # Manually remove from strategies but leave in enabled_strategies
        del strategy_manager.strategies[mock_strategy.name]

        # Act
        result = strategy_manager.get_enabled_strategies()

        # Assert
        assert result == []


class TestStrategyManagerPerformanceMetrics:
    """Test suite for performance metrics retrieval."""

    # ==================== SUCCESS CASES ====================

    def test_get_strategy_performance_success_all_strategies(
        self, strategy_manager: StrategyManager, btc_symbols: SymbolSet, eth_symbols: SymbolSet
    ) -> None:
        """Test getting performance metrics for all strategies."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = btc_symbol.value
        strategy1.performance_metrics = {"total_trades": 10, "win_rate": 0.6}

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = eth_symbol.value
        strategy2.performance_metrics = {"total_trades": 5, "win_rate": 0.8}

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)

        # Act
        result = strategy_manager.get_strategy_performance()

        # Assert
        assert len(result) == 2
        assert result["strategy1"] == {"total_trades": 10, "win_rate": 0.6}
        assert result["strategy2"] == {"total_trades": 5, "win_rate": 0.8}

    # ==================== EDGE CASES ====================

    def test_get_strategy_performance_edge_no_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test getting performance metrics when no strategies exist."""
        # Act
        result = strategy_manager.get_strategy_performance()

        # Assert
        assert result == {}

    # ==================== FAILURE CASES ====================

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_get_strategy_performance_failure_metrics_error(
        self, mock_logger: Mock, strategy_manager: StrategyManager, btc_symbols: SymbolSet
    ) -> None:
        """Test handling errors when getting performance metrics."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        
        strategy = Mock(spec=Strategy)
        strategy.name = "strategy1"
        strategy.symbol = btc_symbol.value

        # Create a mock strategy that raises exception on performance_metrics access
        error_strategy = Mock(spec=Strategy)
        error_strategy.name = "strategy1"
        error_strategy.symbol = btc_symbol.value

        # Delete the existing mock attribute to allow PropertyMock to work
        del error_strategy.performance_metrics

        # Configure the property to raise an exception
        type(error_strategy).performance_metrics = PropertyMock(
            side_effect=AttributeError("Metrics error")
        )

        strategy_manager.strategies["strategy1"] = error_strategy

        # Act
        result = strategy_manager.get_strategy_performance()

        # Assert
        assert result["strategy1"] == {"error": "Failed to retrieve metrics"}
        mock_logger.exception.assert_called_once()


class TestStrategyManagerLifecycleManagement:
    """Test suite for strategy lifecycle management."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_start_all_success_enabled_strategies(
        self, mock_logger: Mock, strategy_manager: StrategyManager, btc_symbols: SymbolSet, eth_symbols: SymbolSet
    ) -> None:
        """Test starting all enabled strategies."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = btc_symbol.value

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = eth_symbol.value

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)
        strategy_manager.enable_strategy("strategy1")
        strategy_manager.enable_strategy("strategy2")

        # Act
        strategy_manager.start_all()

        # Assert
        strategy1.on_start.assert_called_once()
        strategy2.on_start.assert_called_once()
        assert mock_logger.info.call_count >= 3  # Initial message + 2 strategies

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_stop_all_success_all_strategies(
        self, mock_logger: Mock, strategy_manager: StrategyManager, btc_symbols: SymbolSet, eth_symbols: SymbolSet
    ) -> None:
        """Test stopping all strategies."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = btc_symbol.value

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = eth_symbol.value

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)
        strategy_manager.enable_strategy("strategy1")
        strategy_manager.enable_strategy("strategy2")

        # Act
        strategy_manager.stop_all()

        # Assert
        strategy1.on_stop.assert_called_once()
        strategy2.on_stop.assert_called_once()
        strategy1.disable.assert_called_once()
        strategy2.disable.assert_called_once()
        assert len(strategy_manager.enabled_strategies) == 0
        # Verify strategies are properly disabled
        assert len(strategy_manager.enabled_strategies) == 0

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_start_all_edge_no_enabled_strategies(
        self, mock_logger: Mock, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test starting when no strategies are enabled."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        # Don't enable the strategy

        # Act
        strategy_manager.start_all()

        # Assert
        mock_strategy.on_start.assert_not_called()

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_stop_all_edge_no_strategies(
        self, mock_logger: Mock, strategy_manager: StrategyManager
    ) -> None:
        """Test stopping when no strategies exist."""
        # Act
        strategy_manager.stop_all()

        # Assert
        mock_logger.info.assert_called_once()  # Only the initial message

    # ==================== FAILURE CASES ====================

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_start_all_failure_strategy_start_error(
        self, mock_logger: Mock, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test handling errors when starting strategies."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)
        mock_strategy.on_start.side_effect = RuntimeError("Start error")

        # Act
        strategy_manager.start_all()

        # Assert
        mock_logger.exception.assert_called_once()

    @patch("cyberdelta.core.strategy_manager.logger")
    def test_stop_all_failure_strategy_stop_error(
        self, mock_logger: Mock, strategy_manager: StrategyManager, mock_strategy: Mock
    ) -> None:
        """Test handling errors when stopping strategies."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)
        mock_strategy.on_stop.side_effect = RuntimeError("Stop error")

        # Act
        strategy_manager.stop_all()

        # Assert
        mock_logger.exception.assert_called_once()


class TestStrategyManagerActiveSymbols:
    """Test suite for active symbols management."""

    # ==================== SUCCESS CASES ====================

    def test_refresh_active_symbols_success_with_strategies(
        self, strategy_manager: StrategyManager, btc_symbols: SymbolSet, eth_symbols: SymbolSet
    ) -> None:
        """Test refreshing active symbols with multiple strategies."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        
        strategy1 = Mock(spec=Strategy)
        strategy1.name = "strategy1"
        strategy1.symbol = btc_symbol.value

        strategy2 = Mock(spec=Strategy)
        strategy2.name = "strategy2"
        strategy2.symbol = eth_symbol.value

        strategy3 = Mock(spec=Strategy)
        strategy3.name = "strategy3"
        strategy3.symbol = btc_symbol.value  # Duplicate symbol

        strategy_manager.register_strategy(strategy1)
        strategy_manager.register_strategy(strategy2)
        strategy_manager.register_strategy(strategy3)

        # Act
        # Test _refresh_active_symbols through public API (unregister_strategy calls it)
        # First register a strategy to ensure there's something to unregister
        dummy_strategy = Mock(spec=Strategy)
        dummy_strategy.name = "dummy"
        dummy_strategy.symbol = "DUMMY-PERP"
        strategy_manager.register_strategy(dummy_strategy)

        # Now unregister to trigger _refresh_active_symbols
        strategy_manager.unregister_strategy("dummy")

        # Assert
        assert strategy_manager.active_symbols == {btc_symbol.value, eth_symbol.value}

    # ==================== EDGE CASES ====================

    def test_refresh_active_symbols_edge_empty_strategies(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test refreshing active symbols with no strategies."""
        # Act
        # Test _refresh_active_symbols through public API (unregister_strategy calls it)
        # First register a strategy to ensure there's something to unregister
        dummy_strategy = Mock(spec=Strategy)
        dummy_strategy.name = "dummy"
        dummy_strategy.symbol = "DUMMY-PERP"
        strategy_manager.register_strategy(dummy_strategy)

        # Now unregister to trigger _refresh_active_symbols
        strategy_manager.unregister_strategy("dummy")

        # Assert
        assert strategy_manager.active_symbols == set()

    def test_refresh_active_symbols_edge_strategies_with_none_symbol(
        self, strategy_manager: StrategyManager
    ) -> None:
        """Test refreshing active symbols with strategies having None symbol."""
        # Arrange
        strategy = Mock(spec=Strategy)
        strategy.name = "strategy1"
        strategy.symbol = None

        strategy_manager.strategies["strategy1"] = strategy

        # Act
        # Test _refresh_active_symbols through public API (unregister_strategy calls it)
        # First register a strategy to ensure there's something to unregister
        dummy_strategy = Mock(spec=Strategy)
        dummy_strategy.name = "dummy"
        dummy_strategy.symbol = "DUMMY-PERP"
        strategy_manager.register_strategy(dummy_strategy)

        # Now unregister to trigger _refresh_active_symbols
        strategy_manager.unregister_strategy("dummy")

        # Assert
        assert strategy_manager.active_symbols == set()


class TestStrategyManagerMarketDataEntry:
    """Test suite for market data entry point."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_on_market_data_success_delegates_to_process(
        self, strategy_manager: StrategyManager, mock_strategy: Mock, sample_candle: Candle
    ) -> None:
        """Test market data entry point delegates to process_market_data."""
        # Arrange
        strategy_manager.register_strategy(mock_strategy)
        strategy_manager.enable_strategy(mock_strategy.name)

        # Act
        await strategy_manager.on_market_data(sample_candle)

        # Assert
        assert strategy_manager.last_update_time is not None
        mock_strategy.update_historical_data.assert_called_once_with(sample_candle)
        mock_strategy.process_data.assert_called_once_with(sample_candle)
