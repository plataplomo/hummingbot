import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime
import time

from cyberdelta.core.execution_handler import (
    ExecutionHandler,
    CircuitBreaker,
    ExecutionStatus,
    TradeExecution,
)
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.apis.base import ExchangeAPI


class TestCircuitBreaker:
    """Test suite for CircuitBreaker component."""

    @pytest.fixture
    def circuit_breaker(self, mock_config):
        """Create a CircuitBreaker instance with mocked dependencies."""
        return CircuitBreaker(mock_config)

    def test_initial_state(self, circuit_breaker):
        """Test initial state of the circuit breaker."""
        assert circuit_breaker.failed_trades_count == 0
        assert circuit_breaker.consecutive_failures == 0
        assert circuit_breaker.total_loss == 0.0
        assert circuit_breaker.open is False
        assert circuit_breaker.last_failure_time is None

    def test_record_success(self, circuit_breaker):
        """Test recording a successful trade."""
        # Set up initial state with some failures
        circuit_breaker.consecutive_failures = 2

        # Record a success
        circuit_breaker.record_success()

        # Check that consecutive failures is reset
        assert circuit_breaker.consecutive_failures == 0

        # But failed trades count should remain unchanged
        assert circuit_breaker.failed_trades_count == 0

    def test_record_failure(self, circuit_breaker):
        """Test recording a failed trade."""
        # Record a failure
        circuit_breaker.record_failure(10.0)

        # Check that counters are incremented
        assert circuit_breaker.failed_trades_count == 1
        assert circuit_breaker.consecutive_failures == 1
        assert circuit_breaker.total_loss == 10.0
        assert circuit_breaker.last_failure_time is not None

        # Circuit should still be closed
        assert circuit_breaker.open is False

        # Record more failures to trigger opening
        circuit_breaker.record_failure(10.0)
        circuit_breaker.record_failure(10.0)

        # Now circuit should be open due to consecutive failures
        assert circuit_breaker.open is True
        assert circuit_breaker.consecutive_failures == 3
        assert circuit_breaker.total_loss == 30.0

    def test_reset(self, circuit_breaker):
        """Test resetting the circuit breaker."""
        # Set up initial state with failures
        circuit_breaker.failed_trades_count = 5
        circuit_breaker.consecutive_failures = 3
        circuit_breaker.total_loss = 50.0
        circuit_breaker.open = True
        circuit_breaker.last_failure_time = datetime.now()

        # Reset the circuit breaker
        circuit_breaker.reset()

        # Check that all state is reset
        assert circuit_breaker.failed_trades_count == 0
        assert circuit_breaker.consecutive_failures == 0
        assert circuit_breaker.total_loss == 0.0
        assert circuit_breaker.open is False

    def test_is_open(self, circuit_breaker):
        """Test checking if the circuit breaker is open."""
        # Initially closed
        assert circuit_breaker.is_open() is False

        # Set to open
        circuit_breaker.open = True
        assert circuit_breaker.is_open() is True


class TestTradeExecution:
    """Test suite for TradeExecution component."""

    @pytest.fixture
    def sized_opportunity(self, mock_arbitrage_opportunity):
        """Create a SizedOpportunity for testing."""
        return SizedOpportunity(
            opportunity=mock_arbitrage_opportunity,
            long_size=1000.0,
            short_size=1000.0,
            allocation_percentage=10.0,
            expected_profit=50.0,
            expected_return=5.0,
            risk_adjusted_return=500.0,
        )

    @pytest.fixture
    def trade_execution(self, sized_opportunity):
        """Create a TradeExecution instance for testing."""
        return TradeExecution(sized_opportunity)

    def test_initial_state(self, trade_execution):
        """Test initial state of the trade execution."""
        assert trade_execution.status == ExecutionStatus.PENDING
        assert trade_execution.error_message is None
        assert trade_execution.long_order_id is None
        assert trade_execution.short_order_id is None
        assert trade_execution.long_position_id is None
        assert trade_execution.short_position_id is None
        assert trade_execution.long_order_response is None
        assert trade_execution.short_order_response is None
        assert trade_execution.start_time is None
        assert trade_execution.end_time is None
        assert trade_execution.long_fill_price is None
        assert trade_execution.short_fill_price is None
        assert trade_execution.long_fill_quantity is None
        assert trade_execution.short_fill_quantity is None

    def test_to_dict(self, trade_execution):
        """Test conversion to dictionary."""
        # Set some properties
        trade_execution.status = ExecutionStatus.EXECUTING
        trade_execution.long_order_id = "order123"
        trade_execution.short_order_id = "order456"
        trade_execution.start_time = datetime.now()

        # Convert to dictionary
        execution_dict = trade_execution.to_dict()

        # Check structure
        assert "opportunity" in execution_dict
        assert "status" in execution_dict
        assert "long_order_id" in execution_dict
        assert "short_order_id" in execution_dict
        assert "start_time" in execution_dict

        # Check values
        assert execution_dict["status"] == "EXECUTING"
        assert execution_dict["long_order_id"] == "order123"
        assert execution_dict["short_order_id"] == "order456"
        assert isinstance(execution_dict["start_time"], str)  # ISO format string

    def test_str_representation(self, trade_execution):
        """Test string representation."""
        string_rep = str(trade_execution)

        # Check that it contains important info
        assert "TradeExecution" in string_rep
        assert trade_execution.opportunity.opportunity.symbol in string_rep
        assert trade_execution.opportunity.opportunity.long_exchange in string_rep
        assert trade_execution.opportunity.opportunity.short_exchange in string_rep
        assert "PENDING" in string_rep  # Initial status


class TestExecutionHandler:
    """Test suite for ExecutionHandler component."""

    @pytest.fixture
    def mock_config(self):
        cfg = MagicMock()
        # Set specific return values for keys used in ExecutionHandler
        cfg.get.side_effect = lambda key, default=None: {
            "execution.max_retries": 3,
            "execution.retry_delay_base_sec": 0.1,  # Use short delay for tests
        }.get(key, default)
        return cfg

    @pytest.fixture
    def mock_portfolio_tracker(self):
        return MagicMock()

    @pytest.fixture
    def mock_exchange_api(self):
        api = MagicMock()
        # Mock the place_order method to be awaitable
        api.place_order = AsyncMock()
        # Mock get_order and cancel_order as AsyncMock
        api.get_order = AsyncMock()
        api.cancel_order = AsyncMock()
        return api

    @pytest.fixture
    def mock_circuit_breaker_system(self):
        """Provides a mock CircuitBreakerSystem."""
        system = MagicMock()
        # Use MagicMock for synchronous can_execute
        system.can_execute = MagicMock(return_value=(True, None))
        system.record_api_error = MagicMock()
        system.reset_exchange_breaker = MagicMock()
        # Add mock methods needed by ExecutionHandler
        return system

    @pytest.fixture
    def execution_handler(
        self,
        mock_config,
        mock_portfolio_tracker,
        mock_exchange_api,
        mock_circuit_breaker_system,
    ):
        """Create an ExecutionHandler instance with correctly mocked dependencies."""
        # Pass the mock config and system directly
        handler = ExecutionHandler(
            config=mock_config,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=mock_circuit_breaker_system,  # Pass the mock system
        )

        # Register API clients (use fresh mocks here instead of the fixture mock_exchange_api)
        handler.register_api_client("hyperliquid", MagicMock(spec=ExchangeAPI))
        handler.register_api_client("backpack", MagicMock(spec=ExchangeAPI))

        return handler

    @pytest.fixture
    def sized_opportunity(self):
        """Create a SizedOpportunity for testing."""
        # Using a simpler mock for the opportunity itself might be easier
        mock_opp = MagicMock()
        mock_opp.symbol = "BTC"
        mock_opp.long_exchange = "hyperliquid"
        mock_opp.short_exchange = "backpack"
        mock_opp.long_funding_rate = 0.0001
        mock_opp.short_funding_rate = -0.0001
        mock_opp.net_funding_differential = 0.0002
        mock_opp.expected_profit = 5.0
        mock_opp.utility_score = 0.9
        mock_opp.basis_volatility = 0.001

        return SizedOpportunity(
            opportunity=mock_opp,  # Use the mock opportunity
            long_size=1000.0,  # Placeholder value, assuming size in USD
            short_size=1000.0,  # Placeholder value, assuming size in USD
            allocation_percentage=10.0,  # Placeholder value
            expected_profit=5.0,  # Placeholder value
            expected_return=0.5,  # Placeholder value (profit/size)
            risk_adjusted_return=50.0,  # Placeholder value
        )

    def test_register_api_client(self, execution_handler, mock_exchange_api):
        """Test that API clients can be registered."""
        # Register a new API client
        execution_handler.register_api_client("test_exchange", mock_exchange_api)

        # Verify the client was registered
        assert "test_exchange" in execution_handler.api_clients
        assert execution_handler.api_clients["test_exchange"] == mock_exchange_api

    @pytest.mark.asyncio
    async def test_circuit_breaker_open_rejection(
        self, execution_handler, sized_opportunity
    ):
        """Test that executions are rejected when the relevant circuit breaker is open."""
        # Mock the system check to return False (blocked)
        # Ensure the system is attached via the fixture
        assert execution_handler.circuit_breaker_system is not None
        execution_handler.circuit_breaker_system.can_execute.return_value = (
            False,
            "Test breaker tripped",
        )

        # Attempt to execute an opportunity
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Verify the execution was rejected
        assert execution.status == ExecutionStatus.FAILED
        assert "Circuit breaker is open" in execution.error_message

    @pytest.mark.asyncio
    async def test_place_order_with_retry(self, execution_handler, mock_exchange_api):
        """Test placing an order with retry logic."""
        from cyberdelta.apis.base import APIError, APIErrorCode
        from cyberdelta.core.models import OrderSide, OrderType

        # Set up mock order response
        mock_order_response = MagicMock()
        mock_exchange_api.place_order.return_value = mock_order_response

        # Test successful order placement
        order = await execution_handler._place_order_with_retry(
            mock_exchange_api,
            "hyperliquid",
            "BTC",
            OrderSide.BUY,
            OrderType.LIMIT,
            0.1,
            42000.0,
        )

        # Verify the API client was called with the correct parameters
        mock_exchange_api.place_order.assert_called_once_with(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=0.1,
            price=42000.0,
        )

        # Verify the result
        assert order == mock_order_response

        # --- Test retryable error ---
        mock_exchange_api.place_order.reset_mock()
        # Simulate transient API error then success
        mock_exchange_api.place_order.side_effect = [
            APIError("Timeout", code=APIErrorCode.TIMEOUT),
            mock_order_response,  # Success on retry
        ]

        order_retry = await execution_handler._place_order_with_retry(
            mock_exchange_api,
            "hyperliquid",
            "BTC",
            OrderSide.BUY,
            OrderType.LIMIT,
            0.1,
            42000.0,
        )

        # Verify it was called twice (initial + 1 retry)
        assert mock_exchange_api.place_order.call_count == 2
        assert order_retry == mock_order_response

        # --- Test non-retryable error ---
        mock_exchange_api.place_order.reset_mock()
        mock_exchange_api.place_order.side_effect = APIError(
            "Invalid Params", code=APIErrorCode.INVALID_REQUEST
        )

        with pytest.raises(APIError) as exc_info:
            await execution_handler._place_order_with_retry(
                mock_exchange_api,
                "hyperliquid",
                "BTC",
                OrderSide.SELL,
                OrderType.MARKET,
                0.05,
            )

        assert "Invalid Params" in str(exc_info.value)
        # Verify it was called only once (no retry on non-retryable error)
        assert mock_exchange_api.place_order.call_count == 1

        # --- Test max retries exceeded ---
        mock_exchange_api.place_order.reset_mock()
        # Always raise a retryable error
        mock_exchange_api.place_order.side_effect = APIError(
            "Server Error",
            code=APIErrorCode.SERVER_ERROR,
            http_status=500,  # Ensure it's retryable
        )
        # Get max_retries from the execution_handler instance's config attribute
        max_retries = execution_handler.config.get("execution.max_retries", 3)

        with pytest.raises(APIError) as exc_info_max_retry:
            await execution_handler._place_order_with_retry(
                mock_exchange_api,
                "hyperliquid",
                "ETH",
                OrderSide.BUY,
                OrderType.MARKET,
                1.0,
            )

        assert "Server Error" in str(exc_info_max_retry.value)
        # Verify it was called exactly max_retries times before raising
        # Corrected: Should be called max_retries times (e.g., 3 times if max_retries=3)
        assert mock_exchange_api.place_order.call_count == max_retries

    @pytest.mark.asyncio
    async def test_get_order_status(self, execution_handler, mock_exchange_api):
        """Test getting order status."""
        # Set up mock response
        mock_order = MagicMock()
        mock_exchange_api.get_order.return_value = mock_order

        # Get order status
        order = await execution_handler._get_order_status(
            mock_exchange_api, "hyperliquid", "order123"
        )

        # Verify the API client was called with the correct parameters (positional)
        mock_exchange_api.get_order.assert_called_once_with("order123")

        assert order == mock_order

        # Test error
        mock_exchange_api.get_order.reset_mock()
        mock_exchange_api.get_order.side_effect = Exception("API error")

        # Get order status (should handle the error and return None)
        order = await execution_handler._get_order_status(
            mock_exchange_api, "hyperliquid", "order123"
        )

        # Verify the API client was called
        mock_exchange_api.get_order.assert_called_once()

        # Verify the result is None
        assert order is None

    @pytest.mark.asyncio
    async def test_compensate_position(self, execution_handler, mock_exchange_api):
        """Test compensating a position."""
        from cyberdelta.core.models import OrderSide, OrderType, Order, OrderStatus

        # Set up mock response for a successful order
        mock_order = Order(
            id="order123",
            symbol="BTC",
            side=OrderSide.SELL,  # Compensating order is opposite side
            type=OrderType.MARKET,
            price=0.0,  # Market order
            quantity=0.1,
            filled_quantity=0.1,
            status=OrderStatus.FILLED,
            time=int(time.time() * 1000),
        )
        mock_exchange_api.place_order.return_value = mock_order

        # Compensate a position
        # Use a valid price (even if None for MARKET) and reduce_only
        result = await execution_handler._compensate_position(
            mock_exchange_api,
            "hyperliquid",
            "BTC",
            OrderSide.BUY,  # Original position side
            0.1,
            price=None,  # Explicitly pass price=None for market
            reduce_only=True,  # Pass reduce_only=True
        )

        # Verify the API client was called with the correct parameters
        mock_exchange_api.place_order.assert_called_once_with(
            symbol="BTC",
            side=OrderSide.SELL,  # Compensation places opposite order (SELL for BUY)
            order_type=OrderType.MARKET,
            quantity=0.1,
            price=None,  # Ensure price=None is checked
            reduce_only=True,  # Ensure reduce_only=True is checked
        )

        assert result == mock_order

    @pytest.mark.asyncio
    async def test_execute_opportunity(
        self, execution_handler, sized_opportunity, mock_exchange_api
    ):
        """Test executing an opportunity."""
        from cyberdelta.core.models import Order, OrderStatus, OrderSide, OrderType

        # Set up mock responses for successful orders
        long_order = Order(
            id="long_order",
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=41000.0,
            quantity=0.024,  # 1000 USD at 41000 USD/BTC
            filled_quantity=0.024,
            status=OrderStatus.FILLED,
            time=int(time.time() * 1000),
        )

        short_order = Order(
            id="short_order",
            symbol="BTCUSDC",
            side=OrderSide.SELL,
            type=OrderType.LIMIT,
            price=41100.0,
            quantity=0.024,  # 1000 USD at 41100 USD/BTC
            filled_quantity=0.024,
            status=OrderStatus.FILLED,
            time=int(time.time() * 1000),
        )

        # Mock the place_order_with_retry method
        with (
            patch.object(
                execution_handler, "_place_order_with_retry", AsyncMock()
            ) as mock_place_order,
            patch.object(
                execution_handler, "_get_order_status", AsyncMock()
            ) as mock_get_status,
        ):
            # Set up returns for the first and second calls
            mock_place_order.side_effect = [long_order, short_order]
            mock_get_status.side_effect = [long_order, short_order]

            # Execute the opportunity
            execution = await execution_handler.execute_opportunity(sized_opportunity)

            # Verify the execution completed successfully
            assert execution.status == ExecutionStatus.COMPLETED
            assert execution.long_order_id == "long_order"
            assert execution.short_order_id == "short_order"
            assert execution.start_time is not None
            assert execution.end_time is not None

            # Verify place_order_with_retry was called twice (for long and short)
            assert mock_place_order.call_count == 2

            # Verify get_order_status was called twice (for long and short)
            assert mock_get_status.call_count == 2

            # Verify the execution was added to history
            assert len(execution_handler.execution_history) == 1
            assert execution_handler.execution_history[0] == execution

            # Verify the circuit breaker success was recorded
            assert execution_handler.circuit_breaker.consecutive_failures == 0

            # Assert successful execution and state updates
            assert execution.status == ExecutionStatus.COMPLETED
            assert execution.long_order_id == "long_order"
            assert execution.short_order_id == "short_order"
            assert (
                execution_handler.portfolio_tracker.update_position.call_count == 2
            )  # Called for both legs

    @pytest.mark.asyncio
    async def test_execution_failure(
        self, execution_handler, sized_opportunity, mock_exchange_api
    ):
        """Test handling an execution failure."""
        # Mock the place_order_with_retry method to fail
        with patch.object(
            execution_handler, "_place_order_with_retry", AsyncMock(return_value=None)
        ) as mock_place_order:
            # Execute the opportunity (should fail on the long order)
            execution = await execution_handler.execute_opportunity(sized_opportunity)

            # Verify the execution failed
            assert execution.status == ExecutionStatus.FAILED
            assert execution.error_message is not None
            assert "Failed to place long order" in execution.error_message

            # Verify place_order_with_retry was called once (for long)
            assert mock_place_order.call_count == 1

            # Verify the execution was added to history
            assert len(execution_handler.execution_history) == 1

            # Verify the circuit breaker failure was recorded
            assert execution_handler.circuit_breaker.consecutive_failures == 1
            assert execution_handler.circuit_breaker.failed_trades_count == 1
            # Breaker should NOT be open after only one failure (threshold is likely > 1)
            assert execution_handler.circuit_breaker.open is False

    def test_get_execution_history(self, execution_handler):
        """Test getting execution history."""
        # Create some mock executions
        execution1 = MagicMock()
        execution2 = MagicMock()

        # Add them to history
        execution_handler.execution_history = [execution1, execution2]

        # Get history
        history = execution_handler.get_execution_history()

        # Verify the result
        assert len(history) == 2
        assert history == [execution1, execution2]

    def test_get_active_executions(self, execution_handler):
        """Test getting active executions."""
        # Create some mock executions
        execution1 = MagicMock()
        execution2 = MagicMock()

        # Add them to active executions
        execution_handler.active_executions = {"exec1": execution1, "exec2": execution2}

        # Get active executions
        active = execution_handler.get_active_executions()

        # Verify the result
        assert len(active) == 2
        assert execution1 in active
        assert execution2 in active

    def test_reset_circuit_breaker(self, execution_handler):
        """Test resetting the circuit breaker via the handler."""
        # Ensure the handler has the mock system attached
        assert hasattr(execution_handler, "circuit_breaker_system")
        assert execution_handler.circuit_breaker_system is not None, (
            "Circuit breaker system not attached via fixture"
        )

        breaker_name = "hyperliquid"  # Example exchange name

        # --- Call the handler method ---
        # Use asyncio.run() if the handler method is async, otherwise call directly.
        # Assuming reset_circuit_breaker is synchronous based on typical handler patterns.
        # If it were async: asyncio.run(execution_handler.reset_circuit_breaker(breaker_name))
        execution_handler.reset_circuit_breaker(
            breaker_name
        )  # Assuming synchronous call

        # --- Assertions ---
        # Verify the system's reset_exchange_breaker method was called directly
        execution_handler.circuit_breaker_system.reset_exchange_breaker.assert_called_once_with(
            breaker_name
        )
