"""Unit tests for the ExecutionHandler component.

Tests execution handling functionality including order execution and position management.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.common import APIError
from cyberdelta.config.models.config_models import AppSettings, ExecutionSettings
from cyberdelta.core.execution_handler import (
    AverageFillPriceError,
    ExecutionHandler,
    ExecutionStatus,
    LongExchangeCircuitBreakerError,
    MissingClientError,
    ShortExchangeCircuitBreakerError,
    SymbolMappingError,
    TradeExecution,
)
from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.validation.circuit_breaker import (
    CircuitBreakerSystem,
    CircuitBreakerTrippedError,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity


pytestmark = pytest.mark.timing


def _get_can_execute_mock(execution_handler: ExecutionHandler) -> Mock:
    """Helper to get the can_execute mock with proper typing."""
    assert execution_handler.circuit_breaker_system is not None
    can_execute = execution_handler.circuit_breaker_system.can_execute
    assert isinstance(can_execute, Mock)
    return can_execute


def _get_symbol_mapper_mock(execution_handler: ExecutionHandler) -> Mock:
    """Helper to get the symbol mapper's get_exchange_symbol mock with proper typing."""
    get_exchange_symbol = execution_handler.symbol_mapper.get_exchange_symbol
    assert isinstance(get_exchange_symbol, Mock)
    return get_exchange_symbol


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock app settings for testing."""
    settings = Mock(spec=AppSettings)
    settings.execution = Mock(spec=ExecutionSettings)
    settings.execution.max_slippage_pct = Decimal("0.01")
    settings.execution.max_retries = 3
    settings.execution.retry_delay_base_sec = 1
    return settings


@pytest.fixture
def mock_portfolio_tracker() -> Mock:
    """Create mock portfolio tracker."""
    return Mock(spec=PortfolioTracker)


@pytest.fixture
def mock_symbol_mapper() -> Mock:
    """Create mock symbol mapper."""
    mapper = Mock(spec=SymbolMapper)
    mapper.get_exchange_symbol = Mock(return_value="BTC-PERP")
    return mapper


@pytest.fixture
def mock_circuit_breaker() -> Mock:
    """Create mock circuit breaker system."""
    mock = Mock(spec=CircuitBreakerSystem)
    # Set up the can_execute method to return a tuple by default
    mock.can_execute = Mock(return_value=(True, None))
    return mock


@pytest.fixture
def execution_handler(
    mock_app_settings: Mock,
    mock_portfolio_tracker: Mock,
    mock_symbol_mapper: Mock,
    mock_circuit_breaker: Mock,
) -> ExecutionHandler:
    """Create ExecutionHandler instance with mocked dependencies."""
    return ExecutionHandler(
        app_settings=mock_app_settings,
        portfolio_tracker=mock_portfolio_tracker,
        symbol_mapper=mock_symbol_mapper,
        circuit_breaker_system=mock_circuit_breaker,
    )


@pytest.fixture
def mock_exchange_api() -> Mock:
    """Create mock exchange API client."""
    api = Mock(spec=ExchangeAPI)
    api.exchange_id = "test_exchange"
    api.place_order = AsyncMock()
    api.get_order = AsyncMock()
    api.cancel_order = AsyncMock()
    return api


@pytest.fixture
def sample_opportunity() -> ArbitrageOpportunity:
    """Create sample arbitrage opportunity."""
    return ArbitrageOpportunity(
        symbol="BTC",
        long_exchange="exchange1",
        short_exchange="exchange2",
        long_price=Decimal(50000),
        short_price=Decimal(50100),
        long_funding_rate=Decimal("0.001"),
        short_funding_rate=Decimal("-0.002"),
        net_funding_differential=Decimal("0.003"),
        timestamp=datetime.now(UTC),
        expected_profit=Decimal(30),
    )


@pytest.fixture
def sized_opportunity(sample_opportunity: ArbitrageOpportunity) -> SizedOpportunity:
    """Create sample sized opportunity."""
    return SizedOpportunity(
        opportunity=sample_opportunity,
        long_size=Decimal("1.0"),
        short_size=Decimal("1.0"),
        allocation_percentage=Decimal("0.1"),
        expected_profit=Decimal(300),
        expected_return=Decimal("0.03"),
        risk_adjusted_return=Decimal("0.025"),
    )


class TestExecutionHandlerInitialization:
    """Test suite for ExecutionHandler initialization with success, edge, and failure cases."""

    # SUCCESS CASES
    def test_init_success_with_all_dependencies(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
        mock_circuit_breaker: Mock,
    ) -> None:
        """Test successful initialization with all dependencies provided."""
        # Arrange & Act
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=mock_circuit_breaker,
        )

        # Assert
        assert handler.app_settings == mock_app_settings
        assert handler.portfolio_tracker == mock_portfolio_tracker
        assert handler.symbol_mapper == mock_symbol_mapper
        assert handler.circuit_breaker_system == mock_circuit_breaker
        assert handler.max_slippage == Decimal("0.01")
        assert handler.max_retries == 3
        assert handler.retry_delay_base == 1.0
        assert handler.max_execution_history == 100
        assert isinstance(handler.executions, list)
        assert isinstance(handler.active_executions, dict)
        assert isinstance(handler.api_clients, dict)

    def test_init_success_without_circuit_breaker(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test successful initialization without circuit breaker (optional dependency)."""
        # Arrange & Act
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=None,
        )

        # Assert
        assert handler.circuit_breaker_system is None
        assert handler.app_settings == mock_app_settings
        assert handler.portfolio_tracker == mock_portfolio_tracker
        assert handler.symbol_mapper == mock_symbol_mapper

    # EDGE CASES
    def test_init_edge_zero_retry_config(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test initialization with zero retry configuration."""
        # Arrange
        mock_app_settings.execution.max_retries = 0
        mock_app_settings.execution.retry_delay_base_sec = 0

        # Act
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
        )

        # Assert
        assert handler.max_retries == 0
        assert handler.retry_delay_base == 0.0

    def test_init_edge_max_values_config(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test initialization with maximum configuration values."""
        # Arrange
        mock_app_settings.execution.max_slippage_pct = Decimal("1.0")  # 100%
        mock_app_settings.execution.max_retries = 100
        mock_app_settings.execution.retry_delay_base_sec = 3600  # 1 hour

        # Act
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
        )

        # Assert
        assert handler.max_slippage == Decimal("1.0")
        assert handler.max_retries == 100
        assert handler.retry_delay_base == 3600.0

    # FAILURE CASES
    def test_init_failure_none_app_settings(
        self,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test initialization fails with None app_settings."""
        # Arrange
        none_settings: Any = None

        # Act & Assert
        with pytest.raises(AttributeError):
            ExecutionHandler(
                app_settings=none_settings,
                portfolio_tracker=mock_portfolio_tracker,
                symbol_mapper=mock_symbol_mapper,
            )

    def test_init_failure_none_portfolio_tracker(
        self,
        mock_app_settings: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test initialization with None portfolio_tracker (should succeed but may fail later)."""
        # Arrange & Act
        # Testing with None portfolio_tracker
        none_tracker: Any = None
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=none_tracker,
            symbol_mapper=mock_symbol_mapper,
        )

        # Assert - initialization succeeds but portfolio_tracker is None
        assert handler.portfolio_tracker is None

    def test_init_failure_none_symbol_mapper(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test initialization with None symbol_mapper (should succeed but may fail later)."""
        # Arrange & Act
        # Testing with None symbol_mapper
        none_mapper: Any = None
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=none_mapper,
        )

        # Assert - initialization succeeds but symbol_mapper is None
        assert handler.symbol_mapper is None


class TestRegisterApiClient:
    """Test suite for register_api_client method with success, edge, and failure cases."""

    # SUCCESS CASES
    def test_register_api_client_success_single(
        self,
        execution_handler: ExecutionHandler,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful registration of a single API client."""
        # Arrange
        exchange_id = "test_exchange"

        # Act
        execution_handler.register_api_client(exchange_id, mock_exchange_api)

        # Assert
        assert exchange_id in execution_handler.api_clients
        assert execution_handler.api_clients[exchange_id] == mock_exchange_api

    def test_register_api_client_success_multiple(
        self, execution_handler: ExecutionHandler
    ) -> None:
        """Test successful registration of multiple API clients."""
        # Arrange
        clients = {
            "exchange1": Mock(spec=ExchangeAPI),
            "exchange2": Mock(spec=ExchangeAPI),
            "exchange3": Mock(spec=ExchangeAPI),
        }

        # Act
        for exchange_id, client in clients.items():
            execution_handler.register_api_client(exchange_id, client)

        # Assert
        assert len(execution_handler.api_clients) == 3
        for exchange_id, client in clients.items():
            assert execution_handler.api_clients[exchange_id] == client

    # EDGE CASES
    def test_register_api_client_edge_overwrite_existing(
        self,
        execution_handler: ExecutionHandler,
        mock_exchange_api: Mock,
    ) -> None:
        """Test registering a client overwrites existing client for same exchange."""
        # Arrange
        exchange_id = "test_exchange"
        old_client = Mock(spec=ExchangeAPI)
        new_client = mock_exchange_api

        # Act
        execution_handler.register_api_client(exchange_id, old_client)
        execution_handler.register_api_client(exchange_id, new_client)

        # Assert
        assert execution_handler.api_clients[exchange_id] == new_client
        assert execution_handler.api_clients[exchange_id] != old_client

    def test_register_api_client_edge_empty_exchange_id(
        self,
        execution_handler: ExecutionHandler,
        mock_exchange_api: Mock,
    ) -> None:
        """Test registering client with empty exchange ID."""
        # Arrange
        exchange_id = ""

        # Act
        execution_handler.register_api_client(exchange_id, mock_exchange_api)

        # Assert
        assert "" in execution_handler.api_clients
        assert execution_handler.api_clients[""] == mock_exchange_api

    def test_register_api_client_edge_unicode_exchange_id(
        self,
        execution_handler: ExecutionHandler,
        mock_exchange_api: Mock,
    ) -> None:
        """Test registering client with unicode exchange ID."""
        # Arrange
        exchange_id = "交易所_🚀"

        # Act
        execution_handler.register_api_client(exchange_id, mock_exchange_api)

        # Assert
        assert exchange_id in execution_handler.api_clients
        assert execution_handler.api_clients[exchange_id] == mock_exchange_api

    # FAILURE CASES
    def test_register_api_client_failure_none_exchange_id(
        self,
        execution_handler: ExecutionHandler,
        mock_exchange_api: Mock,
    ) -> None:
        """Test registering client with None exchange ID."""
        # Arrange
        none_key: Any = None  # Testing edge case with None as key

        # Act
        execution_handler.register_api_client(none_key, mock_exchange_api)

        # Assert - None is a valid dictionary key in Python
        assert none_key in execution_handler.api_clients
        assert execution_handler.api_clients[none_key] == mock_exchange_api

    def test_register_api_client_failure_none_client(
        self,
        execution_handler: ExecutionHandler,
    ) -> None:
        """Test registering None as client."""
        # Arrange
        exchange_id = "test_exchange"

        # Act
        none_client: Any = None  # Testing edge case with None as client
        execution_handler.register_api_client(exchange_id, none_client)

        # Assert - None is stored but will fail when used
        assert exchange_id in execution_handler.api_clients
        assert execution_handler.api_clients[exchange_id] is None

    def test_register_api_client_failure_invalid_client_type(
        self,
        execution_handler: ExecutionHandler,
    ) -> None:
        """Test registering invalid object as client."""
        # Arrange
        exchange_id = "test_exchange"
        invalid_client = "not_an_api_client"

        # Act
        invalid_client_any: Any = invalid_client  # Testing with invalid type
        execution_handler.register_api_client(exchange_id, invalid_client_any)

        # Assert - stores invalid client but will fail when used
        assert exchange_id in execution_handler.api_clients
        assert execution_handler.api_clients[exchange_id] == invalid_client_any


class TestExecuteOpportunity:
    """Test suite for execute_opportunity method with success, edge, and failure cases."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_success_full_execution(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful full execution of an opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)

        # Mock successful order placement
        mock_order = Mock(spec=Order)
        mock_order.id = "order123"
        mock_order.status = OrderStatus.FILLED
        mock_order.quantity_filled = Decimal("1.0")
        mock_exchange_api.place_order.return_value = mock_order
        mock_exchange_api.get_order.return_value = mock_order

        # Mock circuit breaker checks
        _get_can_execute_mock(execution_handler).return_value = (True, None)

        with (
            patch.object(execution_handler, "_check_circuit_breakers"),
            patch.object(
                execution_handler,
                "_setup_execution_prerequisites",
                return_value=(mock_exchange_api, mock_exchange_api, "BTC-PERP", "BTC-PERP"),
            ),
            patch.object(
                execution_handler,
                "_place_orders_for_opportunity",
                return_value=(mock_order, mock_order),
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert isinstance(result, TradeExecution)
        assert result.opportunity == sized_opportunity
        assert result.status == ExecutionStatus.PENDING
        assert result.id in [e.id for e in execution_handler.executions]

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_partial_fill(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful execution with partial fills."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)

        # Mock partial fill
        mock_order = Mock(spec=Order)
        mock_order.id = "order123"
        mock_order.status = OrderStatus.PARTIALLY_FILLED
        mock_order.quantity_filled = Decimal("0.5")
        mock_order.quantity_requested = Decimal("1.0")
        mock_exchange_api.place_order.return_value = mock_order
        mock_exchange_api.get_order.return_value = mock_order

        with (
            patch.object(execution_handler, "_check_circuit_breakers"),
            patch.object(
                execution_handler,
                "_setup_execution_prerequisites",
                return_value=(mock_exchange_api, mock_exchange_api, "BTC-PERP", "BTC-PERP"),
            ),
            patch.object(
                execution_handler,
                "_place_orders_for_opportunity",
                return_value=(mock_order, mock_order),
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert isinstance(result, TradeExecution)
        assert result.long_order_id is None
        assert result.short_order_id is None

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_zero_size(
        self,
        execution_handler: ExecutionHandler,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test execution with zero-sized opportunity."""
        # Arrange
        zero_sized = SizedOpportunity(
            opportunity=sample_opportunity,
            long_size=Decimal(0),
            short_size=Decimal(0),
            allocation_percentage=Decimal(0),
            expected_profit=Decimal(0),
            expected_return=Decimal(0),
            risk_adjusted_return=Decimal(0),
        )

        with patch.object(execution_handler, "_check_circuit_breakers"):
            # Act
            result = await execution_handler.execute_opportunity(zero_sized)

        # Assert
        assert isinstance(result, TradeExecution)
        # With zero size, division by price will result in zero quantity which should be handled
        assert result.status in [ExecutionStatus.FAILED, ExecutionStatus.COMPLETED]

    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_negative_profit(
        self,
        execution_handler: ExecutionHandler,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test execution with negative expected profit."""
        # Arrange
        negative_profit = SizedOpportunity(
            opportunity=sample_opportunity,
            long_size=Decimal("1.0"),
            short_size=Decimal("1.0"),
            allocation_percentage=Decimal("0.1"),
            expected_profit=Decimal(-100),
            expected_return=Decimal("-0.01"),
            risk_adjusted_return=Decimal("-0.008"),
        )

        with patch.object(execution_handler, "_check_circuit_breakers"):
            # Act
            result = await execution_handler.execute_opportunity(negative_profit)

        # Assert
        assert isinstance(result, TradeExecution)
        # Should still attempt execution if not blocked by other checks

    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_mismatched_sizes(
        self,
        execution_handler: ExecutionHandler,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test execution with mismatched long/short sizes."""
        # Arrange
        mismatched = SizedOpportunity(
            opportunity=sample_opportunity,
            long_size=Decimal("1.0"),
            short_size=Decimal("2.0"),
            allocation_percentage=Decimal("0.15"),
            expected_profit=Decimal(100),
            expected_return=Decimal("0.01"),
            risk_adjusted_return=Decimal("0.008"),
        )

        with (
            patch.object(execution_handler, "_check_circuit_breakers"),
            patch.object(
                execution_handler,
                "_setup_execution_prerequisites",
                side_effect=ValueError("Size mismatch"),
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(mismatched)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_circuit_breaker_tripped(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test execution fails when circuit breaker is tripped."""
        # Arrange
        with patch.object(
            execution_handler,
            "_check_circuit_breakers",
            side_effect=LongExchangeCircuitBreakerError("exchange1", "Test trip"),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.REJECTED
        assert result.error_message is not None
        assert result.error_message is not None
        assert "Circuit breaker tripped" in result.error_message
        assert isinstance(result.error_message, str)

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_missing_api_client(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test execution fails when API client is missing."""
        # Arrange - no clients registered
        with (
            patch.object(execution_handler, "_check_circuit_breakers"),
            patch.object(
                execution_handler,
                "_setup_execution_prerequisites",
                side_effect=MissingClientError("exchange1"),
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert result.error_message is not None
        assert "Missing API client" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_symbol_mapping_error(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test execution fails when symbol mapping fails."""
        # Arrange
        execution_handler.register_api_client("exchange1", Mock(spec=ExchangeAPI))
        execution_handler.register_api_client("exchange2", Mock(spec=ExchangeAPI))

        with (
            patch.object(execution_handler, "_check_circuit_breakers"),
            patch.object(
                execution_handler,
                "_setup_execution_prerequisites",
                side_effect=SymbolMappingError("BTC", "long", "exchange1"),
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert result.error_message is not None
        assert "Symbol mapping failed" in result.error_message


    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_order_placement_error(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test execution fails when order placement fails."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        mock_exchange_api.place_order.side_effect = APIError("Order rejected", "ORDER_REJECTED")

        with (
            patch.object(execution_handler, "_check_circuit_breakers"),
            patch.object(
                execution_handler,
                "_setup_execution_prerequisites",
                return_value=(mock_exchange_api, mock_exchange_api, "BTC-PERP", "BTC-PERP"),
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None


class TestCircuitBreakerIntegration:
    """Test suite for circuit breaker functionality through execute_opportunity."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_success_circuit_breakers_not_tripped(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test execution succeeds when circuit breakers are not tripped."""
        # Arrange
        _get_can_execute_mock(execution_handler).return_value = (True, None)

        # Mock successful order placement
        mock_order = Order(
            exchange="exchange1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal(50000),
            quantity_requested=Decimal("1.0"),
            quantity_filled=Decimal("1.0"),
            status=OrderStatus.FILLED,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=datetime.now(UTC),
            exchange_order_id="ex123",
            time_in_force=TimeInForce.GTC,
            average_fill_price=Decimal(50000),
            strategy_name="test_strategy",
            signal_id="signal123",
        )

        # Mock the API clients to return successful orders
        mock_clients = {"exchange1": AsyncMock(), "exchange2": AsyncMock()}
        with (
            patch.object(execution_handler, "api_clients", mock_clients),
            patch.object(
                execution_handler.api_clients["exchange1"], "place_order", return_value=mock_order
            ),
            patch.object(
                execution_handler.api_clients["exchange2"], "place_order", return_value=mock_order
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

            # Assert - execution should succeed
            assert result.status == ExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_no_breaker_system(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test execution succeeds when circuit breaker system is None."""
        # Arrange
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=None,
        )

        # Mock successful order placement
        mock_order = Order(
            exchange="exchange1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal(50000),
            quantity_requested=Decimal("1.0"),
            quantity_filled=Decimal("1.0"),
            status=OrderStatus.FILLED,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=datetime.now(UTC),
            exchange_order_id="ex123",
            time_in_force=TimeInForce.GTC,
            average_fill_price=Decimal(50000),
            strategy_name="test_strategy",
            signal_id="signal123",
        )

        # Mock the API clients
        mock_clients = {"exchange1": AsyncMock(), "exchange2": AsyncMock()}
        with (
            patch.object(handler, "api_clients", mock_clients),
            patch.object(handler.api_clients["exchange1"], "place_order", return_value=mock_order),
            patch.object(handler.api_clients["exchange2"], "place_order", return_value=mock_order),
        ):
            # Act
            result = await handler.execute_opportunity(sized_opportunity)

            # Assert - should succeed without circuit breaker system
            assert result.status == ExecutionStatus.COMPLETED

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_both_exchanges_checked(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test that both exchanges are checked for circuit breaker status."""
        # Arrange
        _get_can_execute_mock(execution_handler).return_value = (True, None)

        # Mock successful order placement
        mock_order = Order(
            exchange="exchange1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal(50000),
            quantity_requested=Decimal("1.0"),
            quantity_filled=Decimal("1.0"),
            status=OrderStatus.FILLED,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=datetime.now(UTC),
            exchange_order_id="ex123",
            time_in_force=TimeInForce.GTC,
            average_fill_price=Decimal(50000),
            strategy_name="test_strategy",
            signal_id="signal123",
        )

        # Mock the API clients
        api_clients = {"exchange1": AsyncMock(), "exchange2": AsyncMock()}
        with (
            patch.object(execution_handler, "api_clients", api_clients),
            patch.object(api_clients["exchange1"], "place_order", return_value=mock_order),
            patch.object(api_clients["exchange2"], "place_order", return_value=mock_order),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

            # Assert - both exchanges should be checked
            calls = _get_can_execute_mock(execution_handler).call_args_list
            assert len(calls) == 2
            # Check that both exchanges were passed to can_execute
            called_exchanges = [call[0][0] for call in calls]
            assert "exchange1" in called_exchanges
            assert "exchange2" in called_exchanges
            assert result.status == ExecutionStatus.COMPLETED

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_long_exchange_tripped(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test execution fails when long exchange circuit breaker is tripped."""
        # Arrange
        _get_can_execute_mock(execution_handler).side_effect = [(False, "Test trip"), (True, None)]

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert result.error_message is not None
        assert "exchange1" in result.error_message
        assert result.error_message is not None
        assert "Test trip" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_short_exchange_tripped(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test execution fails when short exchange circuit breaker is tripped."""
        # Arrange
        _get_can_execute_mock(execution_handler).side_effect = [(True, None), (False, "Test trip")]

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert result.error_message is not None
        assert "exchange2" in result.error_message
        assert result.error_message is not None
        assert "Test trip" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_both_exchanges_tripped(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test execution fails when both exchange circuit breakers are tripped."""
        # Arrange
        _get_can_execute_mock(execution_handler).return_value = (False, "Test trip")

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - should fail with long exchange error first
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert result.error_message is not None
        assert "exchange1" in result.error_message


class TestErrorClasses:
    """Test suite for custom error classes."""

    # SUCCESS CASES
    def test_long_exchange_circuit_breaker_error_success(self) -> None:
        """Test LongExchangeCircuitBreakerError creation."""
        # Arrange & Act
        error = LongExchangeCircuitBreakerError("binance", "Rate limit exceeded")

        # Assert
        assert error.exchange == "binance"
        assert error.reason == "Rate limit exceeded"
        assert "Circuit breaker tripped for long exchange binance" in str(error)
        assert isinstance(error, CircuitBreakerTrippedError)

    def test_short_exchange_circuit_breaker_error_success(self) -> None:
        """Test ShortExchangeCircuitBreakerError creation."""
        # Arrange & Act
        error = ShortExchangeCircuitBreakerError("okx", "Too many errors")

        # Assert
        assert error.exchange == "okx"
        assert error.reason == "Too many errors"
        assert "Circuit breaker tripped for short exchange okx" in str(error)
        assert isinstance(error, CircuitBreakerTrippedError)

    def test_missing_client_error_success(self) -> None:
        """Test MissingClientError creation."""
        # Arrange & Act
        error = MissingClientError("kraken")

        # Assert
        assert error.exchange == "kraken"
        assert "Missing API client for 'kraken'" in str(error)
        assert isinstance(error, APIError)

    def test_symbol_mapping_error_success(self) -> None:
        """Test SymbolMappingError creation."""
        # Arrange & Act
        error = SymbolMappingError("BTC-USD", "long", "coinbase")

        # Assert
        assert error.symbol == "BTC-USD"
        assert error.leg == "long"
        assert error.exchange == "coinbase"
        assert "Could not map symbol 'BTC-USD'" in str(error)
        assert isinstance(error, APIError)

    # EDGE CASES
    def test_error_classes_edge_empty_strings(self) -> None:
        """Test error classes with empty string parameters."""
        # Arrange & Act
        long_error = LongExchangeCircuitBreakerError("", "")
        short_error = ShortExchangeCircuitBreakerError("", "")
        missing_error = MissingClientError("")
        symbol_error = SymbolMappingError("", "", "")

        # Assert - should handle empty strings gracefully
        # Testing that empty strings are preserved, not converted to None
        assert isinstance(long_error.exchange, str)
        assert len(long_error.exchange) == 0
        assert isinstance(short_error.exchange, str)
        assert len(short_error.exchange) == 0
        assert isinstance(missing_error.exchange, str)
        assert len(missing_error.exchange) == 0
        assert isinstance(symbol_error.symbol, str)
        assert len(symbol_error.symbol) == 0

    def test_error_classes_edge_unicode_parameters(self) -> None:
        """Test error classes with unicode parameters."""
        # Arrange & Act
        error = SymbolMappingError("BTC-🚀", "long", "交易所")

        # Assert
        assert error.symbol == "BTC-🚀"
        assert error.exchange == "交易所"

    # FAILURE CASES
    def test_error_classes_failure_none_parameters(self) -> None:
        """Test error classes handle None parameters gracefully."""
        # These classes expect string parameters
        # Act & Assert
        error1 = LongExchangeCircuitBreakerError("", "")
        assert isinstance(error1.exchange, str)
        assert len(error1.exchange) == 0
        assert isinstance(error1.reason, str)
        assert len(error1.reason) == 0

        error2 = MissingClientError("")
        assert isinstance(error2.exchange, str)
        assert len(error2.exchange) == 0


class TestTradeExecution:
    """Test suite for TradeExecution class."""

    # SUCCESS CASES
    def test_trade_execution_creation_success(self, sized_opportunity: SizedOpportunity) -> None:
        """Test TradeExecution object creation."""
        # Arrange & Act
        execution = TradeExecution(sized_opportunity)

        # Assert
        assert execution.opportunity == sized_opportunity
        assert execution.status == ExecutionStatus.PENDING
        assert execution.id is not None
        assert len(execution.id) > 0
        assert execution.start_time is None
        assert execution.end_time is None
        assert execution.long_order_id is None
        assert execution.short_order_id is None
        assert execution.error_message is None

    def test_trade_execution_state_transitions_success(
        self, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test TradeExecution state transitions."""
        # Arrange
        execution = TradeExecution(sized_opportunity)

        # Act & Assert - PENDING -> EXECUTING
        execution.status = ExecutionStatus.EXECUTING
        assert execution.status == ExecutionStatus.EXECUTING

        # Act & Assert - EXECUTING -> COMPLETED
        execution.status = ExecutionStatus.COMPLETED
        assert execution.status == ExecutionStatus.COMPLETED

    # EDGE CASES
    def test_trade_execution_edge_with_timestamps(
        self, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test TradeExecution with start and end times."""
        # Arrange
        execution = TradeExecution(sized_opportunity)
        start = datetime.now(UTC)
        end = start + timedelta(seconds=5)

        # Act
        execution.start_time = start
        execution.end_time = end

        # Assert
        assert execution.start_time == start
        assert execution.end_time == end
        assert (execution.end_time - execution.start_time).total_seconds() == 5

    def test_trade_execution_edge_with_orders(self, sized_opportunity: SizedOpportunity) -> None:
        """Test TradeExecution with order attachments."""
        # Arrange
        execution = TradeExecution(sized_opportunity)
        long_order = Mock(spec=Order)
        long_order.id = "long123"
        short_order = Mock(spec=Order)
        short_order.id = "short123"

        # Act
        execution.long_order_id = long_order.id
        execution.short_order_id = short_order.id

        # Assert
        assert execution.long_order_id == long_order.id
        assert execution.short_order_id == short_order.id

    # FAILURE CASES
    def test_trade_execution_failure_with_error(self, sized_opportunity: SizedOpportunity) -> None:
        """Test TradeExecution in failed state with error message."""
        # Arrange
        execution = TradeExecution(sized_opportunity)

        # Act
        execution.status = ExecutionStatus.FAILED
        execution.error_message = "Connection timeout"

        # Assert
        assert execution.status == ExecutionStatus.FAILED
        assert execution.error_message == "Connection timeout"

    def test_trade_execution_failure_rejected_state(
        self, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test TradeExecution in rejected state."""
        # Arrange
        execution = TradeExecution(sized_opportunity)

        # Act
        execution.status = ExecutionStatus.REJECTED
        execution.error_message = "Order rejected by exchange"

        # Assert
        assert execution.status == ExecutionStatus.REJECTED
        assert execution.error_message == "Order rejected by exchange"


# Integration-style tests that test method interactions
class TestExecutionHandlerIntegration:
    """Integration tests for ExecutionHandler workflows."""

    @pytest.mark.asyncio
    async def test_full_execution_workflow_success(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test complete execution workflow from start to finish."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)

        # Mock successful execution
        mock_order = Mock(spec=Order)
        mock_order.id = "order123"
        mock_order.status = OrderStatus.FILLED
        mock_order.quantity_filled = sized_opportunity.long_size
        mock_exchange_api.place_order.return_value = mock_order

        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Act
        with patch.object(
            execution_handler,
            "_place_orders_for_opportunity",
            return_value=(mock_order, mock_order),
        ):
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.PENDING
        assert result.id not in execution_handler.active_executions
        assert result.long_order_id is None
        assert result.short_order_id is None

    @pytest.mark.asyncio
    async def test_execution_cleanup_on_failure(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test that execution is properly cleaned up on failure."""
        # Arrange
        with patch.object(
            execution_handler,
            "_check_circuit_breakers",
            side_effect=LongExchangeCircuitBreakerError("exchange1", "Test"),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.REJECTED
        assert result.id in [e.id for e in execution_handler.executions]
        assert result.end_time is not None


class TestOrderPlacementThroughExecuteOpportunity:
    """Test suite for order placement functionality through execute_opportunity."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_success_both_orders_filled(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful execution with both long and short orders filled."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)

        # Mock circuit breakers as passing
        _get_can_execute_mock(execution_handler).return_value = (True, None)

        # Mock successful orders
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.quantity_filled = Decimal("0.02")  # 1.0 / 50000
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.quantity_filled = Decimal("0.01996")  # 1.0 / 50100
        mock_short_order.average_fill_price = Decimal(50100)
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(side_effect=[mock_long_order, mock_short_order])

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch.object(
                _get_symbol_mapper_mock(execution_handler),
                "get_internal_symbol",
                return_value="BTC",
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

            # Assert
            assert result.status == ExecutionStatus.COMPLETED
            assert result.long_order_id == "long123"
            assert result.short_order_id == "short123"
            assert result.long_fill_price == Decimal(50000)
            assert result.short_fill_price == Decimal(50100)
            assert mock_exchange_api.place_order.call_count == 2

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_with_trades(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful order placement with individual trades through execute_opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)

        # Create mock trade
        mock_trade = Mock()
        mock_trade.id = "trade123"
        mock_trade.exchange = "exchange1"

        # Mock order with trades
        mock_order = Mock(spec=Order)
        mock_order.exchange_order_id = "order123"
        mock_order.status = OrderStatus.FILLED
        mock_order.quantity_filled = Decimal("0.02")
        mock_order.average_fill_price = Decimal(50000)
        mock_order.trades = [mock_trade]

        mock_exchange_api.place_order = AsyncMock(return_value=mock_order)

        # Mock circuit breakers as passing
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        with patch.object(
            execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()
        ) as mock_process:
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

            # Assert
            assert result.status == ExecutionStatus.COMPLETED
            mock_process.assert_called()

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_zero_price(
        self,
        execution_handler: ExecutionHandler,
        sample_opportunity: ArbitrageOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test handling of zero price in opportunity through execute_opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)

        sample_opportunity.long_price = Decimal(0)
        sized_opp = SizedOpportunity(
            opportunity=sample_opportunity,
            long_size=Decimal("1.0"),
            short_size=Decimal("1.0"),
            allocation_percentage=Decimal("0.1"),
            expected_profit=Decimal(100),
            expected_return=Decimal("0.01"),
            risk_adjusted_return=Decimal("0.008"),
        )

        # Mock circuit breakers as passing
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Act
        result = await execution_handler.execute_opportunity(sized_opp)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert result.error_message is not None
        assert "Cannot derive long base asset quantity" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_long_fills_short_fails(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test compensation when long order fills but short order fails.

        Tests execute_opportunity method handling of partial fills.
        """
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)

        # Mock app settings for compensation
        mock_compensation_config = Mock()
        mock_compensation_config.use_limit_orders = False
        mock_compensation_config.limit_price_offset_pct = Decimal("0.001")
        execution_handler.app_settings.execution.compensation = mock_compensation_config

        # Mock successful long order
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.quantity_filled = Decimal("0.02")
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.trades = []

        # Mock failed short order
        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.REJECTED

        # Mock compensation order
        mock_comp_order = Mock(spec=Order)
        mock_comp_order.exchange_order_id = "comp123"
        mock_comp_order.status = OrderStatus.FILLED

        mock_exchange_api.place_order = AsyncMock(
            side_effect=[mock_long_order, mock_short_order, mock_comp_order]
        )

        # Mock circuit breakers as passing
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch.object(
                _get_symbol_mapper_mock(execution_handler),
                "get_internal_symbol",
                return_value="BTC",
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.COMPENSATING
        assert result.error_message
        assert result.error_message is not None
        assert "Long leg compensated" in result.error_message
        assert mock_exchange_api.place_order.call_count == 3  # long, short, compensation

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_long_order_fails(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test failure when long order placement fails through execute_opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)

        # Mock failed order
        mock_exchange_api.place_order = AsyncMock(return_value=None)

        # Mock circuit breakers as passing
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message
        assert result.error_message is not None
        assert "Long order placement failed" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_api_error_on_short(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test API error during short order placement triggers compensation.

        Tests execute_opportunity method handling of API errors.
        """
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)

        # Mock app settings for compensation
        mock_compensation_config = Mock()
        mock_compensation_config.use_limit_orders = False
        mock_compensation_config.limit_price_offset_pct = Decimal("0.001")
        execution_handler.app_settings.execution.compensation = mock_compensation_config

        # Mock successful long order
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.quantity_filled = Decimal("0.02")
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.trades = []

        # Mock compensation order
        mock_comp_order = Mock(spec=Order)
        mock_comp_order.exchange_order_id = "comp123"
        mock_comp_order.status = OrderStatus.FILLED

        # First call succeeds, second raises APIError, third compensates
        mock_exchange_api.place_order = AsyncMock(
            side_effect=[
                mock_long_order,
                APIError("Connection failed", "NETWORK_ERROR"),
                mock_comp_order,
            ]
        )

        # Mock circuit breakers as passing
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch.object(
                _get_symbol_mapper_mock(execution_handler),
                "get_internal_symbol",
                return_value="BTC",
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.COMPENSATING
        assert result.error_message
        assert result.error_message is not None
        assert "Connection failed" in result.error_message
        assert result.error_message
        assert result.error_message is not None
        assert "Long leg compensated" in result.error_message


class TestOrderRetryBehaviorThroughExecuteOpportunity:
    """Test suite for order retry behavior through execute_opportunity."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_success_orders_first_attempt(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful execution with orders placed on first attempt."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        _get_can_execute_mock(execution_handler).return_value = (True, None)

        # Mock successful orders
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.quantity_filled = Decimal("0.02")
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.quantity_filled = Decimal("0.01996")
        mock_short_order.average_fill_price = Decimal(50100)
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(side_effect=[mock_long_order, mock_short_order])

        with patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

            # Assert
            assert result.status == ExecutionStatus.COMPLETED
            assert mock_exchange_api.place_order.call_count == 2
            assert result.error_message is None

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_after_retry(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful order placement after retry through execute_opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        execution_handler.retry_delay_base = 0.01  # Speed up test
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Create successful orders
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.quantity_filled = Decimal("0.02")
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.quantity_filled = Decimal("0.01996")
        mock_short_order.average_fill_price = Decimal(50100)
        mock_short_order.trades = []

        # First attempt fails with retryable error, second succeeds
        mock_exchange_api.place_order = AsyncMock(
            side_effect=[
                APIError("Temporary error", "TEMP_ERROR"),
                mock_long_order,
                mock_short_order,
            ]
        )

        with patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.COMPLETED
        assert mock_exchange_api.place_order.call_count == 3  # 1 retry + 2 successful

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_retries_both_orders(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test retry behavior when both orders need retries through execute_opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        execution_handler.retry_delay_base = 0.01  # Speed up test
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Create successful orders
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.quantity_filled = Decimal("0.02")
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.quantity_filled = Decimal("0.01996")
        mock_short_order.average_fill_price = Decimal(50100)
        mock_short_order.trades = []

        # Both orders fail first, then succeed
        mock_exchange_api.place_order = AsyncMock(
            side_effect=[
                APIError("Temporary error", "TEMP_ERROR"),  # Long retry
                mock_long_order,
                APIError("Rate limited", "RATE_LIMITED"),  # Short retry
                mock_short_order,
            ]
        )

        with patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.COMPLETED
        assert mock_exchange_api.place_order.call_count == 4  # 2 retries + 2 successful

    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_max_retries_on_long(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test max retries exhausted on long order through execute_opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        execution_handler.max_retries = 2
        execution_handler.retry_delay_base = 0.01  # Speed up test
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Always fail with retryable error
        api_error = APIError("Rate limited", "RATE_LIMITED")
        mock_exchange_api.place_order = AsyncMock(side_effect=api_error)

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert mock_exchange_api.place_order.call_count == 2  # max_retries = 2
        assert result.error_message
        assert result.error_message is not None
        assert "Rate limited" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_edge_no_client(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test execution with no API client registered through execute_opportunity."""
        # Arrange - don't register any API clients
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message
        assert result.error_message is not None
        assert "No API client for" in result.error_message

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_non_retryable_error(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test non-retryable API error stops retry attempts through execute_opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        api_error = APIError("Invalid API key", "AUTH_FAILED")
        mock_exchange_api.place_order = AsyncMock(side_effect=api_error)

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert mock_exchange_api.place_order.call_count == 1  # No retries for non-retryable
        assert result.error_message
        assert result.error_message is not None
        assert "Invalid API key" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_max_retries_exhausted(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test max retries exhausted with retryable errors through execute_opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        execution_handler.max_retries = 3
        execution_handler.retry_delay_base = 0.01  # Speed up test
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        api_error = APIError("Rate limited", "RATE_LIMITED")
        mock_exchange_api.place_order = AsyncMock(side_effect=api_error)

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert mock_exchange_api.place_order.call_count == 3
        assert result.error_message
        assert result.error_message is not None
        assert "Rate limited" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_failure_unexpected_error(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test unexpected error during order placement through execute_opportunity."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        mock_exchange_api.place_order = AsyncMock(side_effect=ValueError("Unexpected error"))

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message
        assert result.error_message is not None
        assert "Unexpected error" in result.error_message
        assert mock_exchange_api.place_order.call_count == 1


class TestOrderStatusCheckingThroughPublicInterface:
    """Test suite for order status checking behavior through execute_opportunity."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_order_status_checked_during_execution(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test that order status is checked during execution flow."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Create mock order that will be placed
        mock_order = Mock(spec=Order)
        mock_order.exchange_order_id = "order123"
        mock_order.status = OrderStatus.NEW
        mock_order.quantity_filled = Decimal(0)
        mock_order.average_fill_price = None
        mock_order.trades = []

        # Mock order status check returning filled order
        mock_filled_order = Mock(spec=Order)
        mock_filled_order.exchange_order_id = "order123"
        mock_filled_order.status = OrderStatus.FILLED
        mock_filled_order.quantity_filled = sized_opportunity.long_size
        mock_filled_order.average_fill_price = Decimal(50000)
        mock_filled_order.trades = []

        # Setup place_order to return NEW order, get_order_status to return FILLED
        mock_exchange_api.place_order = AsyncMock(return_value=mock_order)
        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_filled_order)

        with patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - verify order status was checked
        assert result.status == ExecutionStatus.COMPLETED
        assert mock_exchange_api.get_order_status.called
        # Order status should be checked for both orders
        assert mock_exchange_api.get_order_status.call_count >= 2

    @pytest.mark.asyncio
    async def test_order_status_retry_during_execution(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test that order status is retried on temporary errors during execution."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        execution_handler.retry_delay_base = 0.01  # Speed up test
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Mock order placement
        mock_order = Mock(spec=Order)
        mock_order.exchange_order_id = "order123"
        mock_order.status = OrderStatus.NEW
        mock_order.quantity_filled = Decimal(0)
        mock_order.average_fill_price = None
        mock_order.trades = []

        # Mock filled order that will be returned after retry
        mock_filled_order = Mock(spec=Order)
        mock_filled_order.exchange_order_id = "order123"
        mock_filled_order.status = OrderStatus.FILLED
        mock_filled_order.quantity_filled = sized_opportunity.long_size
        mock_filled_order.average_fill_price = Decimal(50000)
        mock_filled_order.trades = []

        # Setup: place_order succeeds, get_order_status fails then succeeds
        mock_exchange_api.place_order = AsyncMock(return_value=mock_order)
        mock_exchange_api.get_order_status = AsyncMock(
            side_effect=[APIError("Temporary error", "TEMP_ERROR"), mock_filled_order]
        )

        with patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - execution should succeed despite temporary error
        assert result.status == ExecutionStatus.COMPLETED
        # Order status should be called multiple times due to retry
        assert mock_exchange_api.get_order_status.call_count >= 2

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_execution_fails_with_no_api_client(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test execution fails when no API client is registered."""
        # Arrange - no API clients registered
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert "No API client for" in result.error_message

    @pytest.mark.asyncio
    async def test_execution_handles_order_not_found(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test handling when order is not found during status check."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        execution_handler.max_retries = 2
        execution_handler.retry_delay_base = 0.01
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Mock order placement
        mock_order = Mock(spec=Order)
        mock_order.exchange_order_id = "order123"
        mock_order.status = OrderStatus.NEW
        mock_order.quantity_filled = Decimal(0)
        mock_order.average_fill_price = None
        mock_order.trades = []

        # Setup: place_order succeeds, get_order_status returns ORDER_NOT_FOUND
        mock_exchange_api.place_order = AsyncMock(return_value=mock_order)
        mock_exchange_api.get_order_status = AsyncMock(
            side_effect=APIError("Order not found", "ORDER_NOT_FOUND")
        )

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - execution should handle the missing order appropriately
        assert result.status in [ExecutionStatus.FAILED, ExecutionStatus.REJECTED]
        # Should retry ORDER_NOT_FOUND errors
        assert mock_exchange_api.get_order_status.call_count >= 2

    @pytest.mark.asyncio
    async def test_execution_handles_rate_limiting(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test handling of rate limit errors during execution."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        execution_handler.retry_delay_base = 0.01
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Mock order
        mock_order = Mock(spec=Order)
        mock_order.exchange_order_id = "order123"
        mock_order.status = OrderStatus.NEW
        mock_order.quantity_filled = Decimal(0)
        mock_order.average_fill_price = None
        mock_order.trades = []

        # Mock filled order
        mock_filled_order = Mock(spec=Order)
        mock_filled_order.exchange_order_id = "order123"
        mock_filled_order.status = OrderStatus.FILLED
        mock_filled_order.quantity_filled = sized_opportunity.long_size
        mock_filled_order.average_fill_price = Decimal(50000)
        mock_filled_order.trades = []

        # Setup: place_order succeeds, get_order_status rate limited then succeeds
        mock_exchange_api.place_order = AsyncMock(return_value=mock_order)
        mock_exchange_api.get_order_status = AsyncMock(
            side_effect=[APIError("Rate limited", "RATE_LIMITED"), mock_filled_order]
        )

        with patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - should succeed after retry
        assert result.status == ExecutionStatus.COMPLETED

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_execution_fails_on_auth_error(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test authentication error causes execution failure without retries."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Mock order placement
        mock_order = Mock(spec=Order)
        mock_order.exchange_order_id = "order123"
        mock_order.status = OrderStatus.NEW
        mock_order.quantity_filled = Decimal(0)
        mock_order.average_fill_price = None
        mock_order.trades = []

        # Setup: place_order succeeds, get_order_status returns auth error
        mock_exchange_api.place_order = AsyncMock(return_value=mock_order)
        mock_exchange_api.get_order_status = AsyncMock(
            side_effect=APIError("Invalid API key", "AUTHENTICATION_FAILED")
        )

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - auth errors should not retry
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert "Invalid API key" in result.error_message
        # Should not retry authentication errors
        assert mock_exchange_api.get_order_status.call_count == 1

    @pytest.mark.asyncio
    async def test_execution_fails_on_invalid_request(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test invalid request error causes execution failure without retries."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Mock order placement
        mock_order = Mock(spec=Order)
        mock_order.exchange_order_id = "order123"
        mock_order.status = OrderStatus.NEW
        mock_order.quantity_filled = Decimal(0)
        mock_order.average_fill_price = None
        mock_order.trades = []

        # Setup: place_order succeeds, get_order_status returns invalid request
        mock_exchange_api.place_order = AsyncMock(return_value=mock_order)
        mock_exchange_api.get_order_status = AsyncMock(
            side_effect=APIError("Invalid parameters", "INVALID_REQUEST")
        )

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - invalid request errors should not retry
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert "Invalid parameters" in result.error_message
        # Should not retry invalid request errors
        assert mock_exchange_api.get_order_status.call_count == 1

    @pytest.mark.asyncio
    async def test_execution_handles_unexpected_order_status_error(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test unexpected error during order status check in execution."""
        # Arrange
        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api)
        _get_can_execute_mock(execution_handler).return_value = (True, None)
        _get_symbol_mapper_mock(execution_handler).return_value = "BTC-PERP"

        # Mock order placement
        mock_order = Mock(spec=Order)
        mock_order.exchange_order_id = "order123"
        mock_order.status = OrderStatus.NEW
        mock_order.quantity_filled = Decimal(0)
        mock_order.average_fill_price = None
        mock_order.trades = []

        # Setup: place_order succeeds, get_order_status throws unexpected error
        mock_exchange_api.place_order = AsyncMock(return_value=mock_order)
        mock_exchange_api.get_order_status = AsyncMock(side_effect=ValueError("Unexpected error"))

        # Act
        result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - unexpected errors should cause failure
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert "Unexpected error" in result.error_message
        # Should attempt status check once before failing
        assert mock_exchange_api.get_order_status.call_count >= 1


class TestCompensationBehaviorThroughPublicInterface:
    """Test suite for compensation behavior when one leg of arbitrage fails.

    Tests the internal compensation logic through the public execute_opportunity method.
    """

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_compensation_success_market_order_when_short_fails(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful compensation with market order when short leg fails."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock compensation config
        mock_compensation_config = Mock()
        mock_compensation_config.use_limit_orders = False
        mock_compensation_config.limit_price_offset_pct = Decimal("0.001")
        execution_handler.app_settings.execution.compensation = mock_compensation_config

        # Mock successful long order
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")

        # Mock failed short order
        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Insufficient margin", "INSUFFICIENT_MARGIN")
        )

        # Mock compensation order
        mock_comp_order = Mock(spec=Order)
        mock_comp_order.exchange_order_id = "comp123"
        mock_comp_order.status = OrderStatus.FILLED

        # Set up mock to return compensation order on second call to exchange1
        mock_exchange_api.place_order.side_effect = [mock_long_order, mock_comp_order]

        # Act
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.COMPENSATING
        assert mock_exchange_api.place_order.call_count == 2  # long order + compensation

        # Verify compensation order properties
        comp_call_args = mock_exchange_api.place_order.call_args_list[1][0][0]
        assert comp_call_args.order_type == OrderType.MARKET
        assert comp_call_args.reduce_only is True
        assert comp_call_args.side == OrderSide.SELL  # Compensating the long buy

    @pytest.mark.asyncio
    async def test_compensation_success_limit_order_when_short_fails(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful compensation with limit order when short leg fails."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock compensation config for limit orders
        mock_compensation_config = Mock()
        mock_compensation_config.use_limit_orders = True
        mock_compensation_config.limit_price_offset_pct = Decimal("0.001")
        execution_handler.app_settings.execution.compensation = mock_compensation_config

        # Mock ticker for limit price calculation
        mock_ticker = Mock()
        mock_ticker.bid = Decimal(50000)
        mock_ticker.ask = Decimal(50100)
        mock_exchange_api.get_ticker = AsyncMock(return_value=mock_ticker)

        # Mock successful long order
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")

        # Mock failed short order
        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Insufficient margin", "INSUFFICIENT_MARGIN")
        )

        # Mock compensation order
        mock_comp_order = Mock(spec=Order)
        mock_comp_order.exchange_order_id = "comp123"
        mock_comp_order.status = OrderStatus.FILLED

        # Set up mock to return compensation order on second call
        mock_exchange_api.place_order.side_effect = [mock_long_order, mock_comp_order]

        # Act
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.COMPENSATING
        assert mock_exchange_api.place_order.call_count == 2

        # Verify compensation order uses limit order
        comp_call_args = mock_exchange_api.place_order.call_args_list[1][0][0]
        assert comp_call_args.order_type == OrderType.LIMIT
        # For SELL, price should be ask * (1 - offset)
        expected_price = Decimal(50100) * (Decimal(1) - Decimal("0.001"))
        assert comp_call_args.price == expected_price

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_compensation_edge_ticker_unavailable_falls_back_to_market(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test compensation falls back to market order when ticker unavailable."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock compensation config for limit orders
        mock_compensation_config = Mock()
        mock_compensation_config.use_limit_orders = True
        mock_compensation_config.limit_price_offset_pct = Decimal("0.001")
        execution_handler.app_settings.execution.compensation = mock_compensation_config

        # Ticker returns None (unavailable)
        mock_exchange_api.get_ticker = AsyncMock(return_value=None)

        # Mock successful long order
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")

        # Mock failed short order
        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Insufficient margin", "INSUFFICIENT_MARGIN")
        )

        # Mock compensation order
        mock_comp_order = Mock(spec=Order)
        mock_comp_order.exchange_order_id = "comp123"
        mock_comp_order.status = OrderStatus.FILLED

        # Set up mock to return compensation order on second call
        mock_exchange_api.place_order.side_effect = [mock_long_order, mock_comp_order]

        # Act
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.COMPENSATING
        assert mock_exchange_api.place_order.call_count == 2

        # Verify compensation order falls back to market order
        comp_call_args = mock_exchange_api.place_order.call_args_list[1][0][0]
        assert comp_call_args.order_type == OrderType.MARKET  # Falls back to market

    @pytest.mark.asyncio
    async def test_compensation_edge_order_not_immediately_filled(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test compensation with order not immediately filled."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        mock_compensation_config = Mock()
        mock_compensation_config.use_limit_orders = False
        mock_compensation_config.limit_price_offset_pct = Decimal("0.001")
        execution_handler.app_settings.execution.compensation = mock_compensation_config

        # Mock successful long order
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")

        # Mock failed short order
        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Insufficient margin", "INSUFFICIENT_MARGIN")
        )

        # Mock compensation order that's not immediately filled
        mock_comp_order = Mock(spec=Order)
        mock_comp_order.exchange_order_id = "comp123"
        mock_comp_order.status = OrderStatus.NEW  # Not filled yet

        # Set up mock to return compensation order on second call
        mock_exchange_api.place_order.side_effect = [mock_long_order, mock_comp_order]

        # Act
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - still marks as compensated optimistically
        assert execution.status == ExecutionStatus.COMPENSATING

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_compensation_failure_when_compensation_order_fails(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test compensation failure when compensation order placement fails."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        mock_compensation_config = Mock()
        mock_compensation_config.use_limit_orders = False
        mock_compensation_config.limit_price_offset_pct = Decimal("0.001")
        execution_handler.app_settings.execution.compensation = mock_compensation_config

        # Mock successful long order
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")

        # First call returns long order, second call (compensation) returns None
        mock_exchange_api.place_order.side_effect = [mock_long_order, None]

        # Mock failed short order
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Insufficient margin", "INSUFFICIENT_MARGIN")
        )

        # Act
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.FAILED
        assert execution.error_message is not None
        assert "compensation failed" in execution.error_message.lower()

    @pytest.mark.asyncio
    async def test_compensation_failure_when_compensation_order_raises_exception(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test compensation failure when compensation order raises exception."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        mock_compensation_config = Mock()
        mock_compensation_config.use_limit_orders = True
        mock_compensation_config.limit_price_offset_pct = Decimal("0.001")
        execution_handler.app_settings.execution.compensation = mock_compensation_config

        # Ticker throws error
        mock_exchange_api.get_ticker = AsyncMock(
            side_effect=APIError("Ticker error", "TICKER_ERROR")
        )

        # Mock successful long order
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")

        # First call returns long order, second call (compensation) raises exception
        mock_exchange_api.place_order.side_effect = [
            mock_long_order,
            APIError("Failed to place compensation order", "ORDER_FAILED"),
        ]

        # Mock failed short order
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Insufficient margin", "INSUFFICIENT_MARGIN")
        )

        # Act
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.FAILED
        assert execution.error_message is not None
        assert "compensation order placement failed" in execution.error_message.lower()


class TestOrderMonitoringBehaviorThroughPublicInterface:
    """Test suite for order monitoring behavior through public execute_opportunity method.

    Tests how the system monitors order status progression during execution.
    """

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_order_monitoring_success_orders_filled_progressively(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test monitoring orders that progress from NEW to FILLED status."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock long order that starts NEW and progresses to FILLED
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.NEW
        mock_long_order.average_fill_price = None
        mock_long_order.filled_quantity = Decimal(0)

        # Mock short order
        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.NEW

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        # Mock order progression for long order: NEW -> PARTIALLY_FILLED -> FILLED
        long_order_states = [
            Mock(spec=Order, status=OrderStatus.NEW, exchange_order_id="long123"),
            Mock(
                spec=Order,
                status=OrderStatus.PARTIALLY_FILLED,
                quantity_filled=Decimal("0.5"),
                quantity_requested=Decimal("1.0"),
                exchange_order_id="long123",
            ),
            Mock(
                spec=Order,
                status=OrderStatus.FILLED,
                average_fill_price=Decimal(50000),
                quantity_filled=Decimal("1.0"),
                trades=[],
                exchange_order_id="long123",
            ),
        ]

        # Mock order progression for short order: NEW -> FILLED
        short_order_states = [
            Mock(spec=Order, status=OrderStatus.NEW, exchange_order_id="short123"),
            Mock(
                spec=Order,
                status=OrderStatus.FILLED,
                average_fill_price=Decimal(50100),
                quantity_filled=Decimal("1.0"),
                trades=[],
                exchange_order_id="short123",
            ),
        ]

        mock_exchange_api.get_order_status = AsyncMock(side_effect=long_order_states)
        mock_exchange_api2.get_order_status = AsyncMock(side_effect=short_order_states)

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch.object(
                _get_symbol_mapper_mock(execution_handler),
                "get_internal_symbol",
                return_value="BTC",
            ),
            patch("asyncio.sleep", new=AsyncMock()),  # Speed up the test
        ):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.COMPLETED
        assert mock_exchange_api.get_order_status.call_count >= 2  # At least checked twice
        assert mock_exchange_api2.get_order_status.call_count >= 1

    @pytest.mark.asyncio
    async def test_order_monitoring_detects_canceled_orders(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test monitoring detects when orders get canceled."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock orders
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.NEW

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Failed to place short", "ORDER_FAILED")
        )

        # Long order gets canceled during monitoring
        mock_exchange_api.get_order_status = AsyncMock(
            return_value=Mock(spec=Order, status=OrderStatus.CANCELED, exchange_order_id="long123")
        )

        with patch("asyncio.sleep", new=AsyncMock()):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status in [ExecutionStatus.FAILED, ExecutionStatus.COMPENSATING]
        canceled_msg = (
            execution.error_message is not None and "canceled" in execution.error_message.lower()
        )
        assert canceled_msg or execution.status == ExecutionStatus.COMPENSATING

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_order_monitoring_edge_with_none_order_id(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test execution when place_order returns None (no order ID)."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # place_order returns None
        mock_exchange_api.place_order = AsyncMock(return_value=None)
        mock_exchange_api2.place_order = AsyncMock(return_value=None)

        # Act
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.FAILED
        assert execution.error_message is not None
        assert "failed to place" in execution.error_message.lower()

    @pytest.mark.asyncio
    async def test_order_monitoring_edge_timeout_waiting_for_fill(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test monitoring timeout when orders don't fill in time."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock timeout configuration (using valid execution settings)
        execution_handler.app_settings.execution.retry_delay_base_sec = Decimal("0.1")
        execution_handler.app_settings.execution.max_retries = 1

        # Mock orders that stay in NEW status
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.NEW

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.NEW

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        # Orders stay in NEW status
        mock_exchange_api.get_order_status = AsyncMock(
            return_value=Mock(spec=Order, status=OrderStatus.NEW)
        )
        mock_exchange_api2.get_order_status = AsyncMock(
            return_value=Mock(spec=Order, status=OrderStatus.NEW)
        )

        with patch("asyncio.sleep", new=AsyncMock()):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.FAILED
        timeout_msg = (
            execution.error_message is not None and "timeout" in execution.error_message.lower()
        )
        not_filled_msg = (
            execution.error_message is not None and "not filled" in execution.error_message.lower()
        )
        assert timeout_msg or not_filled_msg
        assert mock_exchange_api.get_order_status.call_count >= 1

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_order_monitoring_failure_when_status_check_fails(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test monitoring when get_order_status fails."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock successful order placement
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.NEW

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.NEW

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        # get_order_status returns None (failure)
        mock_exchange_api.get_order_status = AsyncMock(return_value=None)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=None)

        with patch("asyncio.sleep", new=AsyncMock()):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - when order status check fails, execution should fail
        assert execution.status == ExecutionStatus.FAILED
        api1_called = mock_exchange_api.get_order_status.called
        api2_called = mock_exchange_api2.get_order_status.called
        assert api1_called or api2_called


class TestOrderStateVerificationThroughPublicInterface:
    """Test suite for order state verification behavior through public interface.

    Tests how the system verifies order states during execution.
    """

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_order_verification_success_both_orders_filled(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test successful execution when both orders are properly filled."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock successful order placement
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")
        mock_long_order.quantity_requested = Decimal("1.0")
        mock_long_order.quantity_filled = Decimal("1.0")
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.average_fill_price = Decimal(50100)
        mock_short_order.filled_quantity = Decimal("1.0")
        mock_short_order.quantity_requested = Decimal("1.0")
        mock_short_order.quantity_filled = Decimal("1.0")
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        # Order status returns filled orders
        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=mock_short_order)

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - execution successful when both orders verified as filled
        assert execution.status == ExecutionStatus.COMPLETED
        assert execution.long_order_id == mock_long_order.id
        assert execution.short_order_id == mock_short_order.id

    @pytest.mark.asyncio
    async def test_order_verification_handles_canceled_orders_properly(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test system handles canceled orders during verification."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock order placement
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.NEW

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.NEW

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        # Long order gets canceled during monitoring
        canceled_order = Mock(spec=Order)
        canceled_order.status = OrderStatus.CANCELED
        canceled_order.exchange_order_id = "long123"

        mock_exchange_api.get_order_status = AsyncMock(return_value=canceled_order)
        mock_exchange_api2.get_order_status = AsyncMock(
            return_value=Mock(
                spec=Order,
                status=OrderStatus.FILLED,
                average_fill_price=Decimal(50100),
                quantity_filled=Decimal("1.0"),
                trades=[],
            )
        )

        with patch("asyncio.sleep", new=AsyncMock()):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - execution should handle the canceled order
        assert execution.status in [ExecutionStatus.FAILED, ExecutionStatus.COMPENSATING]

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_order_verification_edge_partial_fill_detection(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test verification detects and handles partial fills."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock order placement
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("0.5")  # Only half filled
        mock_long_order.quantity_requested = Decimal("1.0")
        mock_long_order.quantity_filled = Decimal("0.5")  # Partial fill
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.average_fill_price = Decimal(50100)
        mock_short_order.filled_quantity = Decimal("1.0")
        mock_short_order.quantity_requested = Decimal("1.0")
        mock_short_order.quantity_filled = Decimal("1.0")
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        # Order status returns the partial fill
        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=mock_short_order)

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - execution should detect the partial fill issue
        assert execution.status == ExecutionStatus.FAILED
        partial_msg = (
            execution.error_message is not None and "partial" in execution.error_message.lower()
        )
        fill_msg = execution.error_message is not None and "fill" in execution.error_message.lower()
        assert partial_msg or fill_msg

    @pytest.mark.asyncio
    async def test_order_verification_edge_status_fetch_failure_handling(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test verification handles order status fetch failures."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock successful order placement
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.NEW

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.NEW

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        # Status fetch fails - returns None
        mock_exchange_api.get_order_status = AsyncMock(return_value=None)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=None)

        with patch("asyncio.sleep", new=AsyncMock()):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - execution should fail when unable to verify order states
        assert execution.status == ExecutionStatus.FAILED

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_order_verification_failure_unexpected_order_status(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test verification fails when orders have unexpected status."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock order placement
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.NEW

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.NEW

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        # Orders remain in NEW status (not filled)
        mock_exchange_api.get_order_status = AsyncMock(
            return_value=Mock(spec=Order, status=OrderStatus.NEW)
        )
        mock_exchange_api2.get_order_status = AsyncMock(
            return_value=Mock(spec=Order, status=OrderStatus.NEW)
        )

        with patch("asyncio.sleep", new=AsyncMock()):
            # Act - should timeout waiting for fill
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.FAILED

    @pytest.mark.asyncio
    async def test_order_verification_failure_rejected_orders(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test verification handles rejected orders properly."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock order placement
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.NEW

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Order rejected", "ORDER_REJECTED")
        )

        # Long order gets rejected during monitoring
        mock_exchange_api.get_order_status = AsyncMock(
            return_value=Mock(spec=Order, status=OrderStatus.REJECTED)
        )

        with patch("asyncio.sleep", new=AsyncMock()):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status in [ExecutionStatus.FAILED, ExecutionStatus.COMPENSATING]


class TestPnLCalculationThroughPublicInterface:
    """Test suite for PnL calculation behavior through public interface.

    Tests how the system calculates profit and loss after execution.
    """

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_pnl_calculation_success_profitable_trade(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test PnL calculation for profitable trade."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock filled orders with profitable spread
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)  # Buy at 50000
        mock_long_order.filled_quantity = Decimal("1.0")
        mock_long_order.quantity_filled = Decimal("1.0")
        mock_long_order.quantity_requested = Decimal("1.0")
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.average_fill_price = Decimal(50100)  # Sell at 50100
        mock_short_order.filled_quantity = Decimal("1.0")
        mock_short_order.quantity_filled = Decimal("1.0")
        mock_short_order.quantity_requested = Decimal("1.0")
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=mock_short_order)

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.COMPLETED
        # PnL = (50100 * 1.0) - (50000 * 1.0) = 100
        assert execution.realized_pnl == Decimal(100)

    @pytest.mark.asyncio
    async def test_pnl_calculation_success_losing_trade(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test PnL calculation for losing trade."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock filled orders with losing spread (slippage)
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50100)  # Buy at 50100 (higher)
        mock_long_order.filled_quantity = Decimal("1.0")
        mock_long_order.quantity_filled = Decimal("1.0")
        mock_long_order.quantity_requested = Decimal("1.0")
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.average_fill_price = Decimal(50000)  # Sell at 50000 (lower)
        mock_short_order.filled_quantity = Decimal("1.0")
        mock_short_order.quantity_filled = Decimal("1.0")
        mock_short_order.quantity_requested = Decimal("1.0")
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=mock_short_order)

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.COMPLETED
        # PnL = (50000 * 1.0) - (50100 * 1.0) = -100
        assert execution.realized_pnl == Decimal(-100)

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_pnl_calculation_edge_failed_execution_no_pnl(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test PnL not calculated for failed executions."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock failed order placement
        mock_exchange_api.place_order = AsyncMock(
            side_effect=APIError("Insufficient funds", "INSUFFICIENT_FUNDS")
        )
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Insufficient funds", "INSUFFICIENT_FUNDS")
        )

        # Act
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.FAILED
        assert execution.realized_pnl is None  # No PnL for failed trades

    @pytest.mark.asyncio
    async def test_pnl_calculation_edge_compensated_execution(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test PnL calculation for compensated execution."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock compensation config
        mock_compensation_config = Mock()
        mock_compensation_config.use_limit_orders = False
        execution_handler.app_settings.execution.compensation = mock_compensation_config

        # Mock successful long order
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")
        mock_long_order.quantity_filled = Decimal("1.0")
        mock_long_order.quantity_requested = Decimal("1.0")
        mock_long_order.trades = []

        # Mock failed short order
        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(
            side_effect=APIError("Insufficient margin", "INSUFFICIENT_MARGIN")
        )

        # Mock compensation order
        mock_comp_order = Mock(spec=Order)
        mock_comp_order.exchange_order_id = "comp123"
        mock_comp_order.status = OrderStatus.FILLED

        # Set up mock to return compensation order on second call
        mock_exchange_api.place_order.side_effect = [mock_long_order, mock_comp_order]

        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_long_order)

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.COMPENSATING
        # PnL should not be calculated for compensated trades
        assert execution.realized_pnl is None or execution.realized_pnl == Decimal(0)

    @pytest.mark.asyncio
    async def test_pnl_calculation_edge_different_fill_quantities(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test PnL calculation when fill quantities differ."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock orders with different fill quantities
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("0.8")  # Less than requested
        mock_long_order.quantity_filled = Decimal("0.8")
        mock_long_order.quantity_requested = Decimal("1.0")
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.average_fill_price = Decimal(50100)
        mock_short_order.filled_quantity = Decimal("1.0")  # Full fill
        mock_short_order.quantity_filled = Decimal("1.0")
        mock_short_order.quantity_requested = Decimal("1.0")
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=mock_short_order)

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - should fail due to quantity mismatch
        assert execution.status == ExecutionStatus.FAILED

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_pnl_calculation_failure_zero_fill_quantities(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test PnL calculation when orders have zero fill."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock orders that were placed but got zero fills
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.CANCELED  # Canceled with no fill
        mock_long_order.average_fill_price = None
        mock_long_order.filled_quantity = Decimal(0)
        mock_long_order.quantity_filled = Decimal(0)
        mock_long_order.quantity_requested = Decimal("1.0")
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.CANCELED
        mock_short_order.average_fill_price = None
        mock_short_order.filled_quantity = Decimal(0)
        mock_short_order.quantity_filled = Decimal(0)
        mock_short_order.quantity_requested = Decimal("1.0")
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=mock_short_order)

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert execution.status == ExecutionStatus.FAILED
        assert execution.realized_pnl is None  # No PnL for zero fills


class TestMiscellaneousMethods:
    """Test suite for miscellaneous ExecutionHandler methods."""

    # SUCCESS CASES
    def test_get_active_executions_success(self, execution_handler: ExecutionHandler) -> None:
        """Test getting list of active executions."""
        # Arrange
        execution1 = Mock(id="exec1")
        execution2 = Mock(id="exec2")
        execution_handler.active_executions = {"exec1": execution1, "exec2": execution2}

        # Act
        result = execution_handler.get_active_executions()

        # Assert
        assert len(result) == 2
        assert execution1 in result
        assert execution2 in result

    def test_reset_circuit_breaker_success(self, execution_handler: ExecutionHandler) -> None:
        """Test resetting circuit breaker for exchange."""
        # Arrange
        with patch.object(execution_handler.circuit_breaker_system, "reset_breaker") as mock_reset:
            # Act
            execution_handler.reset_circuit_breaker("exchange1")

            # Assert
            mock_reset.assert_called_once_with("exchange1")

    def test_reset_circuit_breaker_none_system(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
    ) -> None:
        """Test reset circuit breaker when system is None."""
        # Arrange
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=None,
        )

        # Act & Assert - should not raise
        handler.reset_circuit_breaker("exchange1")

    @pytest.mark.asyncio
    async def test_execution_history_management_success(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test execution is added to history after completion."""
        # Arrange
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock successful orders
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")
        mock_long_order.quantity_filled = Decimal("1.0")
        mock_long_order.quantity_requested = Decimal("1.0")
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.average_fill_price = Decimal(50100)
        mock_short_order.filled_quantity = Decimal("1.0")
        mock_short_order.quantity_filled = Decimal("1.0")
        mock_short_order.quantity_requested = Decimal("1.0")
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=mock_short_order)

        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            # Act
            execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert - execution should be in history
        assert execution.id not in execution_handler.active_executions
        assert execution in execution_handler.executions

    @pytest.mark.asyncio
    async def test_execution_history_max_size_maintained(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_exchange_api: Mock,
    ) -> None:
        """Test history maintains max size by removing old entries."""
        # Arrange
        execution_handler.max_execution_history = 3
        mock_exchange_api2 = Mock(spec=ExchangeAPI)
        mock_exchange_api2.exchange_id = "exchange2"

        execution_handler.register_api_client("exchange1", mock_exchange_api)
        execution_handler.register_api_client("exchange2", mock_exchange_api2)

        # Mock successful orders
        mock_long_order = Mock(spec=Order)
        mock_long_order.exchange_order_id = "long123"
        mock_long_order.status = OrderStatus.FILLED
        mock_long_order.average_fill_price = Decimal(50000)
        mock_long_order.filled_quantity = Decimal("1.0")
        mock_long_order.quantity_filled = Decimal("1.0")
        mock_long_order.quantity_requested = Decimal("1.0")
        mock_long_order.trades = []

        mock_short_order = Mock(spec=Order)
        mock_short_order.exchange_order_id = "short123"
        mock_short_order.status = OrderStatus.FILLED
        mock_short_order.average_fill_price = Decimal(50100)
        mock_short_order.filled_quantity = Decimal("1.0")
        mock_short_order.quantity_filled = Decimal("1.0")
        mock_short_order.quantity_requested = Decimal("1.0")
        mock_short_order.trades = []

        mock_exchange_api.place_order = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.place_order = AsyncMock(return_value=mock_short_order)

        mock_exchange_api.get_order_status = AsyncMock(return_value=mock_long_order)
        mock_exchange_api2.get_order_status = AsyncMock(return_value=mock_short_order)

        # Fill history with 3 executions
        for i in range(3):
            with (
                patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
                patch("asyncio.sleep", new=AsyncMock()),
            ):
                execution = await execution_handler.execute_opportunity(sized_opportunity)
                execution.id = f"exec{i}"  # Set predictable ID for testing

        # Act - Add one more execution (4th)
        with (
            patch.object(execution_handler.portfolio_tracker, "process_trade", new=AsyncMock()),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            new_execution = await execution_handler.execute_opportunity(sized_opportunity)
            new_execution.id = "exec3"

        # Assert
        assert len(execution_handler.executions) == 3
        # First execution should have been removed
        execution_ids = [e.id for e in execution_handler.executions]
        assert "exec0" not in execution_ids
        assert "exec3" in execution_ids

    def test_trade_execution_to_dict(self, sized_opportunity: SizedOpportunity) -> None:
        """Test TradeExecution to_dict method."""
        # Arrange
        execution = TradeExecution(sized_opportunity)
        execution.status = ExecutionStatus.COMPLETED
        execution.long_order_id = "long123"
        execution.short_order_id = "short123"
        execution.realized_pnl = Decimal("100.50")
        execution.start_time = datetime.now(UTC)
        execution.end_time = execution.start_time + timedelta(seconds=5)

        # Act
        result = execution.to_dict()

        # Assert
        assert result["status"] == "COMPLETED"
        assert result["long_order_id"] == "long123"
        assert result["short_order_id"] == "short123"
        assert result["realized_pnl"] == "100.50"
        assert result["start_time"] is not None
        assert result["end_time"] is not None
        assert "opportunity" in result

    def test_trade_execution_str(self, sized_opportunity: SizedOpportunity) -> None:
        """Test TradeExecution string representation."""
        # Arrange
        execution = TradeExecution(sized_opportunity)

        # Act
        result = str(execution)

        # Assert
        assert "TradeExecution: BTC" in result
        assert "Long: exchange1" in result
        assert "Short: exchange2" in result
        assert "Status: PENDING" in result


class TestAverageFillPriceError:
    """Test suite for AverageFillPriceError class."""

    def test_average_fill_price_error_creation(self) -> None:
        """Test AverageFillPriceError creation."""
        # Arrange & Act
        error = AverageFillPriceError()

        # Assert
        assert isinstance(error, ValueError)
        assert str(error) == "average_fill_price cannot be None for synthetic trade"
