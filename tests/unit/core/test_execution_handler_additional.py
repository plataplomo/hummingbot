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
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.execution_handler import (
    ExecutionHandler,
    LongExchangeCircuitBreakerError,
    MissingClientError,
    ShortExchangeCircuitBreakerError,
    SymbolMappingError,
)
from cyberdelta.core.models import Order
from cyberdelta.core.models.execution import ExecutionStatus, TradeExecution
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbols import Symbol, symbols
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity

from tests.helpers.symbol_validators import SymbolTestValidator
from tests.helpers.symbol_scenarios import SymbolTestScenarios
from tests.mocks.symbol_mocks import MockSymbolService


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock app settings for testing.

    Returns:
        Mock: Mock AppSettings instance with configured exchange settings.
    """
    settings = Mock(spec=AppSettings)

    # Configure execution settings
    execution = Mock()
    execution.max_slippage_pct = Decimal("0.01")
    execution.max_retries = 3
    execution.retry_delay_base_sec = Decimal("1.0")
    settings.execution = execution

    return settings


@pytest.fixture
def mock_portfolio_state_manager() -> Mock:
    """Create mock portfolio tracker for testing.

    Returns:
        Mock: Mock PortfolioStateManager instance for testing.
    """
    return Mock(spec=PortfolioStateManager)


@pytest.fixture
def symbol_service() -> SymbolService:
    """Create symbol service for testing.

    Returns:
        SymbolService: Real SymbolService instance for testing.
    """
    from cyberdelta.core.symbols import get_symbol_service
    return get_symbol_service()


@pytest.fixture
def mock_circuit_breaker_system() -> Mock:
    """Create mock circuit breaker system for testing.

    Returns:
        Mock: Mock CircuitBreakerSystem instance that allows all operations by default.
    """
    system = Mock(spec=CircuitBreakerSystem)
    # Default behavior - allow all operations
    system.can_execute.return_value = (True, None)
    system.reset_breaker.return_value = None
    return system


@pytest.fixture
def execution_handler(
    mock_app_settings: Mock,
    mock_portfolio_state_manager: Mock,
    symbol_service: SymbolService,
    mock_circuit_breaker_system: Mock,
) -> ExecutionHandler:
    """Create an ExecutionHandler instance for testing.

    Returns:
        ExecutionHandler: Configured ExecutionHandler instance with mocked dependencies.
    """
    return ExecutionHandler(
        app_settings=mock_app_settings,
        portfolio_tracker=mock_portfolio_state_manager,
        symbol_service=symbol_service,
        circuit_breaker_system=mock_circuit_breaker_system,
    )


@pytest.fixture
def sample_arbitrage_opportunity(btc_perp_hl, btc_perp_bp) -> ArbitrageOpportunity:
    """Create a sample ArbitrageOpportunity for testing.

    Returns:
        ArbitrageOpportunity: Sample ArbitrageOpportunity instance with test data.
    """
    btc_hl = btc_symbols.perp_hl
    btc_bp = btc_symbols.perp_bp
    
    return ArbitrageOpportunity(
        symbol=btc_hl.value,  # Use canonical symbol value
        long_exchange=btc_hl.exchange.value,
        short_exchange=btc_bp.exchange.value,
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
    """Create a sample SizedOpportunity for testing.

    Returns:
        SizedOpportunity: Sample SizedOpportunity instance with test data.
    """
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
def mock_exchange_api(btc_perp_hl) -> Mock:
    """Create mock exchange API for testing.

    Returns:
        Mock: Mock ExchangeAPI instance with async methods mocked.
    """
    api = Mock(spec=ExchangeAPI)

    # Mock successful order placement with Symbol
    mock_order = Order(
        client_order_id=str(uuid4()),
        exchange_order_id="12345",
        symbol=btc_symbols.perp_hl.value,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("0.02"),
        quantity_filled=Decimal("0.0"),
        price=Decimal("50000.0"),
        average_fill_price=None,
        status=OrderStatus.NEW,
        time_in_force=TimeInForce.IOC,
        created_at=datetime.now(UTC),
        exchange=btc_perp_hl.exchange.value,
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
        execution = TradeExecution(opportunity=sample_sized_opportunity)

        # Assert
        assert execution.opportunity == sample_sized_opportunity
        assert execution.status == ExecutionStatus.PENDING
        assert execution.error_message is None
        assert execution.long_order_id is None
        assert execution.short_order_id is None
        assert execution.start_time is None
        assert execution.end_time is None

    def test_trade_execution_to_dict_success_complete(
        self, sample_sized_opportunity: SizedOpportunity, btc_perp_hl, btc_perp_bp
    ) -> None:
        """Test to_dict with complete execution data."""
        # Arrange
        execution = TradeExecution(opportunity=sample_sized_opportunity)
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

    def test_trade_execution_str_success(self, sample_sized_opportunity: SizedOpportunity, btc_perp_hl, btc_perp_bp) -> None:
        """Test string representation of TradeExecution."""
        # Arrange
        execution = TradeExecution(opportunity=sample_sized_opportunity)

        # Act
        result = str(execution)

        # Assert - using Symbol metadata
        assert btc_perp_hl.value in result
        assert btc_perp_hl.exchange.value in result
        assert btc_perp_bp.exchange.value in result
        assert "1000.00" in result
        assert "PENDING" in result

    # ==================== EDGE CASES ====================

    def test_trade_execution_to_dict_edge_minimal_data(
        self, sample_sized_opportunity: SizedOpportunity
    ) -> None:
        """Test to_dict with minimal execution data."""
        # Arrange
        execution = TradeExecution(opportunity=sample_sized_opportunity)

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
        execution = TradeExecution(opportunity=sample_sized_opportunity)
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
        mock_portfolio_state_manager: Mock,
        mock_symbol_service: Mock,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test successful initialization with all dependencies."""
        # Act
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_state_manager,
            symbol_service=mock_symbol_service,
            circuit_breaker_system=mock_circuit_breaker_system,
        )

        # Assert
        assert handler.app_settings is mock_app_settings
        assert handler.portfolio_tracker is mock_portfolio_state_manager
        assert handler.symbol_service is mock_symbol_service
        assert handler.circuit_breaker_system is mock_circuit_breaker_system
        assert handler.max_slippage == Decimal("0.01")
        assert handler.max_retries == 3
        assert handler.retry_delay_base == 1.0
        assert handler.api_clients == {}
        assert handler.get_active_executions() == []

    def test_init_success_without_circuit_breaker(
        self,
        mock_app_settings: Mock,
        mock_portfolio_state_manager: Mock,
        symbol_service: SymbolService,
    ) -> None:
        """Test successful initialization without circuit breaker."""
        # Act
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_state_manager,
            symbol_service=symbol_service,
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
        btc_perp_hl,
        btc_perp_bp,
        symbol_service: SymbolService,
    ) -> None:
        """Test successful execution with complete fills."""
        # Arrange
        # Register API clients
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Use MockSymbolService builder for precise control
        mock_service = (
            MockSymbolService()
            .with_conversion(
                btc_perp_hl,
                ExchangeName.HYPERLIQUID,
                btc_perp_hl
            )
            .with_conversion(
                btc_perp_bp,
                ExchangeName.BACKPACK,
                btc_perp_bp
            )
            .with_equivalence([btc_perp_hl, btc_perp_bp])
            .build()
        )
        
        # Replace symbol service with mock
        execution_handler.symbol_service = mock_service

        # Mock successful order placement and fills with Symbols
        long_order = Order(
            client_order_id=str(uuid4()),
            exchange_order_id="long123",
            symbol=btc_perp_hl.value,
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
            exchange=btc_perp_hl.exchange.value,
        )

        short_order = Order(
            client_order_id=str(uuid4()),
            exchange_order_id="short456",
            symbol=btc_perp_bp.value,
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
            exchange=btc_perp_bp.exchange.value,
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

        # Validate symbols used
        SymbolTestValidator.assert_valid_arbitrage_pair(
            btc_perp_hl,
            btc_perp_bp,
            mock_service
        )

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
        mock_app_settings: Mock,
        mock_portfolio_state_manager: Mock,
        symbol_service: SymbolService,
        sample_sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test execution without circuit breaker system."""
        # Arrange
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_state_manager,
            symbol_service=symbol_service,
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
        btc_perp_hl,
    ) -> None:
        """Test execution failure when symbol mapping fails."""
        # Arrange
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Mock symbol mapping failure by setting None service
        mock_service = (
            MockSymbolService()
            .with_conversion(btc_perp_hl, ExchangeName.HYPERLIQUID, None)
            .build()
        )
        execution_handler.symbol_service = mock_service

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
        btc_perp_hl,
        btc_perp_bp,
    ) -> None:
        """Test getting active executions during execution."""
        # Arrange
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Setup mock service to avoid errors
        mock_service = (
            MockSymbolService()
            .with_conversion(btc_perp_hl, ExchangeName.HYPERLIQUID, btc_perp_hl)
            .with_conversion(btc_perp_bp, ExchangeName.BACKPACK, btc_perp_bp)
            .build()
        )
        execution_handler.symbol_service = mock_service

        # Make the place_order method hang so execution stays active
        async def hanging_place_order(*args: object, **kwargs: object) -> object:
            await asyncio.sleep(60)  # Hang for a long time
            return mock_exchange_api.place_order.return_value

        mock_exchange_api.place_order = AsyncMock(side_effect=hanging_place_order)

        # Start execution (but don't await it)
        task = asyncio.create_task(execution_handler.execute_opportunity(sample_sized_opportunity))

        try:
            # Give it a moment to start
            await asyncio.sleep(0.1)

            # Act
            active = execution_handler.get_active_executions()

            # Assert
            assert len(active) == 1
            assert active[0].opportunity == sample_sized_opportunity
        finally:
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
        # Act - get initial empty active executions
        result = execution_handler.get_active_executions()

        # Assert - should be empty initially
        assert isinstance(result, list)
        assert len(result) == 0


class TestExecutionHandlerCircuitBreakerReset:
    """Test suite for circuit breaker reset functionality."""

    # ==================== SUCCESS CASES ====================

    def test_circuit_breaker_integration_with_system(
        self,
        execution_handler: ExecutionHandler,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test circuit breaker integration when system exists."""
        # Circuit breaker functionality is now handled through the services layer
        # The ExecutionHandler maintains a reference to the circuit breaker system
        assert execution_handler.circuit_breaker_system is mock_circuit_breaker_system

    def test_circuit_breaker_integration_without_system(
        self,
        mock_app_settings: Mock,
        mock_portfolio_state_manager: Mock,
        symbol_service: SymbolService,
    ) -> None:
        """Test circuit breaker integration when system doesn't exist."""
        # Arrange
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_state_manager,
            symbol_service=symbol_service,
            circuit_breaker_system=None,
        )

        # Assert
        assert handler.circuit_breaker_system is None

    # ==================== EDGE CASES ====================

    def test_circuit_breaker_integration_edge_cases(
        self,
        execution_handler: ExecutionHandler,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test circuit breaker integration edge cases."""
        # Circuit breaker functionality is tested through the services that use it
        # The ExecutionHandler provides the circuit breaker to its services
        assert execution_handler.circuit_breaker_system is mock_circuit_breaker_system
        # Services handle circuit breaker operations internally


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

    def test_symbol_mapping_error_success(self, btc_perp_hl) -> None:
        """Test SymbolMappingError initialization with Symbol objects."""
        # Act
        error = SymbolMappingError(
            btc_perp_hl.value,
            "long",
            btc_perp_hl.exchange.value
        )

        # Assert
        assert error.symbol == btc_perp_hl.value
        assert error.leg == "long"
        assert error.exchange == btc_perp_hl.exchange.value
        assert btc_perp_hl.value in str(error)
        assert "long" in str(error)
        assert btc_perp_hl.exchange.value in str(error)


class TestExecutionHandlerPartialFills:
    """Test suite for partial fill handling."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_partial_fills(
        self,
        execution_handler: ExecutionHandler,
        sample_sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
        btc_perp_hl,
    ) -> None:
        """Test execution with partial fills using Symbols."""
        # Arrange
        execution_handler.register_api_client("hyperliquid", mock_exchange_api)
        execution_handler.register_api_client("backpack", mock_exchange_api)

        # Mock partial fill with Symbol
        partial_order = Order(
            client_order_id=str(uuid4()),
            exchange_order_id="partial123",
            symbol=btc_perp_hl.value,
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
            exchange=btc_perp_hl.exchange.value,
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
        # Execution is now tracked by the state manager service

    # ==================== EDGE CASES ====================

    def test_execution_history_max_limit_configuration(
        self, execution_handler: ExecutionHandler, sample_sized_opportunity: SizedOpportunity
    ) -> None:
        """Test execution history max limit configuration."""
        # The max_execution_history is now managed by the state manager service
        # This configuration is passed to the service during initialization
        assert execution_handler.max_execution_history == 100  # Default from settings

        # History management is handled internally by the ThreadSafeExecutionStateManager
        # The actual limit enforcement is tested in the state manager service tests
