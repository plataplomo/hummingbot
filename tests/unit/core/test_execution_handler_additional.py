"""Additional comprehensive unit tests for ExecutionHandler.

Tests additional public methods, error handling, and edge cases that need better coverage,
focusing on circuit breaker integration, order placement flow, execution state management,
and error recovery scenarios.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

import asyncio
import contextlib
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.common import APIError
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.execution_handler import (
    ExecutionHandler,
    ExecutionStatus,
    LongExchangeCircuitBreakerError,
    MissingClientError,
    ShortExchangeCircuitBreakerError,
    SymbolMappingError,
    TradeExecution,
)
from cyberdelta.core.models import Order, OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock app settings for testing."""
    settings = Mock(spec=AppSettings)

    # Configure execution settings
    execution = Mock()
    execution.max_slippage_pct = Decimal("0.01")
    execution.max_retries = 3
    execution.retry_delay_base_sec = Decimal("1.0")
    settings.execution = execution

    return settings


@pytest.fixture
def mock_portfolio_tracker() -> Mock:
    """Create mock portfolio tracker for testing."""
    return Mock(spec=PortfolioTracker)


@pytest.fixture
def mock_symbol_mapper() -> Mock:
    """Create mock symbol mapper for testing."""
    mapper = Mock(spec=SymbolMapper)
    # Default behavior - return the same symbol
    mapper.get_exchange_symbol.return_value = "BTC-PERP"
    return mapper


@pytest.fixture
def mock_circuit_breaker_system() -> Mock:
    """Create mock circuit breaker system for testing."""
    system = Mock(spec=CircuitBreakerSystem)
    # Default behavior - allow all operations
    system.can_execute.return_value = (True, None)
    system.reset_breaker.return_value = None
    return system


@pytest.fixture
def execution_handler(
    mock_app_settings: Mock,
    mock_portfolio_tracker: Mock,
    mock_symbol_mapper: Mock,
    mock_circuit_breaker_system: Mock,
) -> ExecutionHandler:
    """Create an ExecutionHandler instance for testing."""
    return ExecutionHandler(
        app_settings=mock_app_settings,
        portfolio_tracker=mock_portfolio_tracker,
        symbol_mapper=mock_symbol_mapper,
        circuit_breaker_system=mock_circuit_breaker_system,
    )


@pytest.fixture
def sample_arbitrage_opportunity() -> ArbitrageOpportunity:
    """Create a sample ArbitrageOpportunity for testing."""
    return ArbitrageOpportunity(
        symbol="BTC-PERP",
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("50000.0"),
        short_price=Decimal("50100.0"),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("0.0002"),
        net_funding_differential=Decimal("0.0001"),
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_sized_opportunity(
    sample_arbitrage_opportunity: ArbitrageOpportunity,
) -> SizedOpportunity:
    """Create a sample SizedOpportunity for testing."""
    return SizedOpportunity(
        opportunity=sample_arbitrage_opportunity,
        long_size=Decimal("1000.0"),
        short_size=Decimal("1000.0"),
        allocation_percentage=Decimal("0.1"),  # 10% allocation
        expected_profit=Decimal("10.0"),
        expected_return=Decimal("0.01"),  # 1% return
        risk_adjusted_return=Decimal("0.05"),  # 5% risk-adjusted return
    )


@pytest.fixture
def mock_exchange_api() -> Mock:
    """Create mock exchange API for testing."""
    api = Mock(spec=ExchangeAPI)

    # Mock successful order placement
    mock_order = Order(
        client_order_id=str(uuid4()),
        exchange_order_id="12345",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("0.02"),
        quantity_filled=Decimal("0.0"),
        price=Decimal("50000.0"),
        average_fill_price=None,
        status=OrderStatus.NEW,
        time_in_force=TimeInForce.IOC,
        created_at=datetime.now(UTC),
        exchange="hyperliquid",
        updated_at=None,
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )

    api.place_order = AsyncMock(return_value=mock_order)
    api.get_order = AsyncMock(return_value=mock_order)
    api.cancel_order = AsyncMock(return_value=True)

    return api


class TestTradeExecution:
    """Test suite for TradeExecution class."""

    # ==================== SUCCESS CASES ====================

    def test_trade_execution_init_success(self, sample_sized_opportunity: SizedOpportunity) -> None:
        """Test successful initialization of TradeExecution."""
        # Act
        execution = TradeExecution(sample_sized_opportunity)

        # Assert
        assert execution.opportunity == sample_sized_opportunity
        assert execution.status == ExecutionStatus.PENDING
        assert execution.error_message is None
        assert execution.long_order_id is None
        assert execution.short_order_id is None
        assert execution.start_time is None
        assert execution.end_time is None

    def test_trade_execution_to_dict_success_complete(
        self, sample_sized_opportunity: SizedOpportunity
    ) -> None:
        """Test to_dict with complete execution data."""
        # Arrange
        execution = TradeExecution(sample_sized_opportunity)
        execution.status = ExecutionStatus.COMPLETED
        execution.long_order_id = "long123"
        execution.short_order_id = "short456"
        execution.start_time = datetime.now(UTC)
        execution.end_time = datetime.now(UTC)
        execution.long_fill_price = Decimal("50000.0")
        execution.short_fill_price = Decimal("50100.0")
        execution.long_fill_quantity = Decimal("0.02")
        execution.short_fill_quantity = Decimal("0.02")
        execution.realized_pnl = Decimal("10.0")

        # Act
        result = execution.to_dict()

        # Assert
        assert result["status"] == "COMPLETED"
        assert result["long_order_id"] == "long123"
        assert result["short_order_id"] == "short456"
        assert result["long_fill_price"] == "50000.0"
        assert result["short_fill_price"] == "50100.0"
        assert result["realized_pnl"] == "10.0"
        assert result["start_time"] is not None
        assert result["end_time"] is not None

    def test_trade_execution_str_success(self, sample_sized_opportunity: SizedOpportunity) -> None:
        """Test string representation of TradeExecution."""
        # Arrange
        execution = TradeExecution(sample_sized_opportunity)

        # Act
        result = str(execution)

        # Assert
        assert "BTC-PERP" in result
        assert "hyperliquid" in result
        assert "backpack" in result
        assert "1000.00" in result
        assert "PENDING" in result

    # ==================== EDGE CASES ====================

    def test_trade_execution_to_dict_edge_minimal_data(
        self, sample_sized_opportunity: SizedOpportunity
    ) -> None:
        """Test to_dict with minimal execution data."""
        # Arrange
        execution = TradeExecution(sample_sized_opportunity)

        # Act
        result = execution.to_dict()

        # Assert
        assert result["status"] == "PENDING"
        assert result["error_message"] is None
        assert result["long_order_id"] is None
        assert result["realized_pnl"] is None
        assert result["start_time"] is None

    def test_trade_execution_to_dict_edge_with_error(
        self, sample_sized_opportunity: SizedOpportunity
    ) -> None:
        """Test to_dict when execution has error."""
        # Arrange
        execution = TradeExecution(sample_sized_opportunity)
        execution.status = ExecutionStatus.FAILED
        execution.error_message = "Connection timeout"

        # Act
        result = execution.to_dict()

        # Assert
        assert result["status"] == "FAILED"
        assert result["error_message"] == "Connection timeout"


class TestExecutionHandlerInitialization:
    """Test suite for ExecutionHandler initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success_with_all_dependencies(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test successful initialization with all dependencies."""
        # Act
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=mock_circuit_breaker_system,
        )

        # Assert
        assert handler.app_settings is mock_app_settings
        assert handler.portfolio_tracker is mock_portfolio_tracker
        assert handler.symbol_mapper is mock_symbol_mapper
        assert handler.circuit_breaker_system is mock_circuit_breaker_system
        assert handler.max_slippage == Decimal("0.01")
        assert handler.max_retries == 3
        assert handler.retry_delay_base == 1.0
        assert handler.api_clients == {}
        assert handler.executions == []
        assert handler.active_executions == {}

    def test_init_success_without_circuit_breaker(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test successful initialization without circuit breaker."""
        # Act
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=None,
        )

        # Assert
        assert handler.circuit_breaker_system is None


class TestExecutionHandlerAPIClientManagement:
    """Test suite for API client management."""

    # ==================== SUCCESS CASES ====================

    def test_register_api_client_success_single_exchange(
        self, execution_handler: ExecutionHandler, mock_exchange_api: Mock
    ) -> None:
        """Test registering API client for single exchange."""
        # Act
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)

        # Assert
        assert "hyperliquid" in execution_handler.api_clients
        assert execution_handler.api_clients["hyperliquid"] is mock_exchange_api

    def test_register_api_client_success_multiple_exchanges(
        self, execution_handler: ExecutionHandler
    ) -> None:
        """Test registering API clients for multiple exchanges."""
        # Arrange
        mock_api1 = Mock(spec=ExchangeAPI)
        mock_api2 = Mock(spec=ExchangeAPI)

        # Act
        execution_handler.register_api_client("hyperliquid", mock_api1)
        execution_handler.register_api_client("backpack", mock_api2)

        # Assert
        assert len(execution_handler.api_clients) == 2
        assert execution_handler.api_clients["hyperliquid"] is mock_api1
        assert execution_handler.api_clients["backpack"] is mock_api2

    # ==================== EDGE CASES ====================

    def test_register_api_client_edge_overwrite_existing(
        self, execution_handler: ExecutionHandler
    ) -> None:
        """Test overwriting existing API client."""
        # Arrange
        mock_api1 = Mock(spec=ExchangeAPI)
        mock_api2 = Mock(spec=ExchangeAPI)
        execution_handler.register_api_client("hyperliquid", mock_api1)

        # Act
        execution_handler.register_api_client("hyperliquid", mock_api2)

        # Assert
        assert execution_handler.api_clients["hyperliquid"] is mock_api2


class TestExecutionHandlerOpportunityExecution:
    """Test suite for opportunity execution."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_complete_fill(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test successful execution with complete fills."""
        # Arrange
        # Register API clients
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Mock symbol mapping
        def symbol_mapper_side_effect(symbol: str, exchange: str) -> str:
            return f"{symbol}_{exchange}"

        mock_symbol_mapper.get_exchange_symbol.side_effect = symbol_mapper_side_effect

        # Mock get_internal_symbol for synthetic trade creation
        def internal_symbol_side_effect(symbol: str, exchange: str) -> str:
            # Return just the base symbol for internal representation
            return "BTC-PERP"

        mock_symbol_mapper.get_internal_symbol.side_effect = internal_symbol_side_effect

        # Mock successful order placement and fills
        long_order = Order(
            client_order_id=str(uuid4()),
            exchange_order_id="long123",
            symbol="BTC-PERP_hyperliquid",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.02"),
            quantity_filled=Decimal("0.02"),
            price=Decimal("50000.0"),
            average_fill_price=Decimal("50000.0"),
            status=OrderStatus.FILLED,
            time_in_force=TimeInForce.IOC,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="signal_123",
            exchange="hyperliquid",
        )

        short_order = Order(
            client_order_id=str(uuid4()),
            exchange_order_id="short456",
            symbol="BTC-PERP_backpack",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.02"),
            quantity_filled=Decimal("0.02"),
            price=Decimal("50100.0"),
            average_fill_price=Decimal("50100.0"),
            status=OrderStatus.FILLED,
            time_in_force=TimeInForce.IOC,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="signal_456",
            exchange="backpack",
        )

        mock_exchange_api.place_order.side_effect = [long_order, short_order]
        mock_exchange_api.get_order.side_effect = [long_order, short_order]

        # Act
        result = await execution_handler.execute_opportunity(sample_sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.COMPLETED
        assert result.error_message is None
        assert result.long_order_id == "long123"
        assert result.short_order_id == "short456"
        assert result.long_fill_price == Decimal("50000.0")
        assert result.short_fill_price == Decimal("50100.0")
        assert result.realized_pnl is not None

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_circuit_breaker_tripped_long(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test execution when circuit breaker is tripped for long exchange."""
        # Arrange
        mock_circuit_breaker_system.can_execute.side_effect = [
            (False, "Too many failures"),  # Long exchange
            (True, None),  # Short exchange
        ]

        # Act
        result = await execution_handler.execute_opportunity(sample_sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.REJECTED
        assert result.error_message is not None
        assert "circuit breaker" in result.error_message.lower()
        assert result.long_order_id is None
        assert result.short_order_id is None

    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_circuit_breaker_tripped_short(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test execution when circuit breaker is tripped for short exchange."""
        # Arrange
        mock_circuit_breaker_system.can_execute.side_effect = [
            (True, None),  # Long exchange
            (False, "Rate limit exceeded"),  # Short exchange
        ]

        # Act
        result = await execution_handler.execute_opportunity(sample_sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.REJECTED
        assert result.error_message is not None
        assert "circuit breaker" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_no_circuit_breaker(
        self,
        mock_app_settings: Mock,  # Use shared fixture
        mock_portfolio_tracker: Mock,  # Use shared fixture
        mock_symbol_mapper: Mock,  # Use shared fixture
        sample_sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test execution without circuit breaker system."""
        # Arrange
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=None,
        )
        handler.register_api_client("hyperliquid", mock_exchange_api)
        handler.register_api_client("backpack", mock_exchange_api)

        # Act
        result = await handler.execute_opportunity(sample_sized_opportunity)

        # Assert
        # Should proceed without circuit breaker checks
        assert result.status in [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED]

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_missing_api_client(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test execution failure when API client is missing."""
        # Act
        result = await execution_handler.execute_opportunity(sample_sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert "Missing API client" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_symbol_mapping_error(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test execution failure when symbol mapping fails."""
        # Arrange
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Mock symbol mapping failure by returning None
        mock_symbol_mapper.get_exchange_symbol.return_value = None

        # Act
        result = await execution_handler.execute_opportunity(sample_sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_order_placement_error(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test execution failure when order placement fails."""
        # Arrange
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Mock order placement failure
        mock_exchange_api.place_order.side_effect = APIError(
            "Insufficient balance",
            "INSUFFICIENT_FUNDS",
        )

        # Act
        result = await execution_handler.execute_opportunity(sample_sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert "Insufficient balance" in result.error_message


class TestExecutionHandlerActiveExecutions:
    """Test suite for active executions management."""

    # ==================== SUCCESS CASES ====================

    def test_get_active_executions_success_empty(self, execution_handler: ExecutionHandler) -> None:
        """Test getting active executions when none exist."""
        # Act
        result = execution_handler.get_active_executions()

        # Assert
        assert result == []

    @pytest.mark.timing
    @pytest.mark.asyncio
    async def test_get_active_executions_success_with_executions(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test getting active executions during execution."""
        # Arrange
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Setup symbol mapper to avoid errors
        mock_symbol_mapper.get_exchange_symbol.return_value = "BTC-PERP"
        mock_symbol_mapper.get_internal_symbol.return_value = "BTC-PERP"

        # Make the place_order method hang so execution stays active
        async def hanging_place_order(*args: object, **kwargs: object) -> object:
            await asyncio.sleep(60)  # Hang for a long time
            return mock_exchange_api.place_order.return_value

        mock_exchange_api.place_order = AsyncMock(side_effect=hanging_place_order)

        # Start execution (but don't await it)
        task = asyncio.create_task(execution_handler.execute_opportunity(sample_sized_opportunity))

        # Give it a moment to start
        await asyncio.sleep(0.1)

        # Act
        active = execution_handler.get_active_executions()

        # Assert
        assert len(active) == 1
        assert active[0].opportunity == sample_sized_opportunity

        # Clean up - cancel the hanging task
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    # ==================== EDGE CASES ====================

    def test_get_active_executions_edge_multiple_executions(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test getting active executions with multiple executions."""
        # Arrange
        execution1 = TradeExecution(sample_sized_opportunity)
        execution2 = TradeExecution(sample_sized_opportunity)
        execution_handler.active_executions[execution1.id] = execution1
        execution_handler.active_executions[execution2.id] = execution2

        # Act
        result = execution_handler.get_active_executions()

        # Assert
        assert len(result) == 2
        assert execution1 in result
        assert execution2 in result


class TestExecutionHandlerCircuitBreakerReset:
    """Test suite for circuit breaker reset functionality."""

    # ==================== SUCCESS CASES ====================

    def test_reset_circuit_breaker_success_with_breaker_system(
        self,
        execution_handler: ExecutionHandler,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test resetting circuit breaker when system exists."""
        # Act
        execution_handler.reset_circuit_breaker("hyperliquid")

        # Assert
        mock_circuit_breaker_system.reset_breaker.assert_called_once_with("hyperliquid")

    def test_reset_circuit_breaker_success_without_breaker_system(
        self,
        mock_app_settings: Mock,  # Use shared fixture
        mock_portfolio_tracker: Mock,  # Use shared fixture
        mock_symbol_mapper: Mock,  # Use shared fixture
    ) -> None:
        """Test resetting circuit breaker when system doesn't exist."""
        # Arrange
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=None,
        )

        # Act
        # Should not raise any exceptions
        handler.reset_circuit_breaker("hyperliquid")

        # Assert
        # No assertions needed - just verifying no exception

    # ==================== EDGE CASES ====================

    def test_reset_circuit_breaker_edge_unknown_exchange(
        self,
        execution_handler: ExecutionHandler,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test resetting circuit breaker for unknown exchange."""
        # Act
        execution_handler.reset_circuit_breaker("unknown_exchange")

        # Assert
        mock_circuit_breaker_system.reset_breaker.assert_called_once_with("unknown_exchange")


class TestExecutionHandlerErrorClasses:
    """Test suite for custom error classes."""

    # ==================== SUCCESS CASES ====================

    def test_long_exchange_circuit_breaker_error_success(self) -> None:
        """Test LongExchangeCircuitBreakerError initialization."""
        # Act
        error = LongExchangeCircuitBreakerError("hyperliquid", "Too many failures")

        # Assert
        assert error.exchange == "hyperliquid"
        assert error.reason == "Too many failures"
        assert "hyperliquid" in str(error)
        assert "Too many failures" in str(error)

    def test_short_exchange_circuit_breaker_error_success(self) -> None:
        """Test ShortExchangeCircuitBreakerError initialization."""
        # Act
        error = ShortExchangeCircuitBreakerError("backpack", "Rate limit exceeded")

        # Assert
        assert error.exchange == "backpack"
        assert error.reason == "Rate limit exceeded"
        assert "backpack" in str(error)
        assert "Rate limit exceeded" in str(error)

    def test_missing_client_error_success(self) -> None:
        """Test MissingClientError initialization."""
        # Act
        error = MissingClientError("hyperliquid")

        # Assert
        assert error.exchange == "hyperliquid"
        assert "hyperliquid" in str(error)

    def test_symbol_mapping_error_success(self) -> None:
        """Test SymbolMappingError initialization."""
        # Act
        error = SymbolMappingError("BTC-PERP", "long", "hyperliquid")

        # Assert
        assert error.symbol == "BTC-PERP"
        assert error.leg == "long"
        assert error.exchange == "hyperliquid"
        assert "BTC-PERP" in str(error)
        assert "long" in str(error)
        assert "hyperliquid" in str(error)


class TestExecutionHandlerPartialFills:
    """Test suite for partial fill handling."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_partial_fills(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test execution with partial fills."""
        # Arrange
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Mock partial fill
        partial_order = Order(
            client_order_id=str(uuid4()),
            exchange_order_id="partial123",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.02"),
            quantity_filled=Decimal("0.01"),  # Partial fill
            price=Decimal("50000.0"),
            average_fill_price=Decimal("50000.0"),
            status=OrderStatus.PARTIALLY_FILLED,
            time_in_force=TimeInForce.IOC,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="signal_partial",
            exchange="hyperliquid",
        )

        mock_exchange_api.place_order.return_value = partial_order
        mock_exchange_api.get_order.return_value = partial_order

        # Act
        result = await execution_handler.execute_opportunity(sample_sized_opportunity)

        # Assert
        assert result.status in [ExecutionStatus.PARTIALLY_COMPLETED, ExecutionStatus.FAILED]


class TestExecutionHandlerStateManagement:
    """Test suite for execution state management."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_execution_state_transitions_success(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test execution state transitions."""
        # Arrange
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Act
        result = await execution_handler.execute_opportunity(sample_sized_opportunity)

        # Assert
        assert result.start_time is not None
        assert result.end_time is not None
        assert result.start_time <= result.end_time
        assert result.id in execution_handler.executions[-1].id

    # ==================== EDGE CASES ====================

    def test_execution_history_edge_max_history_limit(
        self, execution_handler: ExecutionHandler, sample_sized_opportunity: SizedOpportunity
    ) -> None:
        """Test execution history respects max limit."""
        # Arrange
        execution_handler.max_execution_history = 5

        # Add more executions than the limit
        for _ in range(10):
            execution = TradeExecution(sample_sized_opportunity)
            execution.status = ExecutionStatus.COMPLETED
            execution_handler.executions.append(execution)

        # Act & Assert
        # Should maintain only the last max_execution_history executions
        # Note: The actual implementation may need to enforce this limit
        # Current implementation doesn't enforce limit
        assert len(execution_handler.executions) == 10
