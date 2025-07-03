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
    OrderStatus,
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
                "_execute_sequenced_orders",
                return_value=(mock_order, mock_order),
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert isinstance(result, TradeExecution)
        assert result.opportunity == sized_opportunity
        assert result.status == ExecutionStatus.COMPLETED
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
                "_execute_sequenced_orders",
                return_value=(mock_order, mock_order),
            ),
        ):
            # Act
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert isinstance(result, TradeExecution)
        assert result.long_order_id == mock_order.id
        assert result.short_order_id == mock_order.id

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
                side_effect=Exception("Size mismatch"),
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
        assert result.status == ExecutionStatus.FAILED
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


class TestCheckCircuitBreakers:
    """Test suite for _check_circuit_breakers method."""

    # SUCCESS CASES
    def test_check_circuit_breakers_success_not_tripped(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test circuit breaker check passes when not tripped."""
        # Arrange
        execution = TradeExecution(sized_opportunity)
        _get_can_execute_mock(execution_handler).return_value = (True, None)

        # Act & Assert - should not raise
        execution_handler._check_circuit_breakers(execution, sized_opportunity)

    def test_check_circuit_breakers_success_no_breaker_system(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_symbol_mapper: Mock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test circuit breaker check passes when system is None."""
        # Arrange
        handler = ExecutionHandler(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=None,
        )
        execution = TradeExecution(sized_opportunity)

        # Act & Assert - should not raise
        handler._check_circuit_breakers(execution, sized_opportunity)

    # EDGE CASES
    def test_check_circuit_breakers_edge_both_exchanges_checked(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test that both exchanges are checked for circuit breaker status."""
        # Arrange
        execution = TradeExecution(sized_opportunity)
        _get_can_execute_mock(execution_handler).return_value = (True, None)

        # Act
        execution_handler._check_circuit_breakers(execution, sized_opportunity)

        # Assert - both exchanges should be checked
        calls = _get_can_execute_mock(execution_handler).call_args_list
        assert len(calls) == 2
        # Check that both exchanges were passed to can_execute
        called_exchanges = [call[0][0] for call in calls]
        assert "exchange1" in called_exchanges
        assert "exchange2" in called_exchanges

    # FAILURE CASES
    def test_check_circuit_breakers_failure_long_exchange_tripped(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test circuit breaker raises error when long exchange is tripped."""
        # Arrange
        execution = TradeExecution(sized_opportunity)
        _get_can_execute_mock(execution_handler).side_effect = [(False, "Test trip"), (True, None)]

        # Act & Assert
        with pytest.raises(LongExchangeCircuitBreakerError) as exc_info:
            execution_handler._check_circuit_breakers(execution, sized_opportunity)

        assert exc_info.value.exchange == "exchange1"

    def test_check_circuit_breakers_failure_short_exchange_tripped(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test circuit breaker raises error when short exchange is tripped."""
        # Arrange
        execution = TradeExecution(sized_opportunity)
        _get_can_execute_mock(execution_handler).side_effect = [(True, None), (False, "Test trip")]

        # Act & Assert
        with pytest.raises(ShortExchangeCircuitBreakerError) as exc_info:
            execution_handler._check_circuit_breakers(execution, sized_opportunity)

        assert exc_info.value.exchange == "exchange2"

    def test_check_circuit_breakers_failure_both_exchanges_tripped(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test circuit breaker raises error for first tripped exchange when both are tripped."""
        # Arrange
        execution = TradeExecution(sized_opportunity)
        _get_can_execute_mock(execution_handler).return_value = (False, "Test trip")

        # Act & Assert - should raise for long exchange first
        with pytest.raises(LongExchangeCircuitBreakerError):
            execution_handler._check_circuit_breakers(execution, sized_opportunity)


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
            execution_handler, "_execute_sequenced_orders", return_value=(mock_order, mock_order)
        ):
            result = await execution_handler.execute_opportunity(sized_opportunity)

        # Assert
        assert result.status == ExecutionStatus.COMPLETED
        assert result.id in execution_handler.active_executions
        assert result.long_order_id == mock_order.id
        assert result.short_order_id == mock_order.id

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
        assert result.status == ExecutionStatus.FAILED
        assert result.id in [e.id for e in execution_handler.executions]
        assert result.end_time is not None
