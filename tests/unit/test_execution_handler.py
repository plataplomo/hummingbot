from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.core.execution_handler import (
    CircuitBreakerSystem,
    ExecutionHandler,
    ExecutionStatus,
    TradeExecution,
)
from cyberdelta.core.models import (
    ArbitrageOpportunity,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Ticker,
    TimeInForce,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config


@pytest.fixture
def mock_arbitrage_opportunity() -> ArbitrageOpportunity:
    """Provides a basic mock ArbitrageOpportunity."""
    return ArbitrageOpportunity(
        symbol="BTC",
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("41000.0"),
        short_price=Decimal("41100.0"),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("-0.0001"),
        net_funding_differential=Decimal("0.0002"),
        timestamp=datetime.now(UTC),
        expected_profit=Decimal("5.0"),
        utility_score=0.8,
        basis_volatility=0.001,
    )


class TestTradeExecution:
    """Test suite for TradeExecution component."""

    @pytest.fixture
    def sized_opportunity(
        self, mock_arbitrage_opportunity: ArbitrageOpportunity
    ) -> SizedOpportunity:
        """Create a SizedOpportunity for testing."""
        return SizedOpportunity(
            opportunity=mock_arbitrage_opportunity,
            long_size=Decimal("1000.0"),
            short_size=Decimal("1000.0"),
            allocation_percentage=Decimal("0.1"),
            expected_profit=Decimal("50.0"),
            expected_return=Decimal("0.02"),
            risk_adjusted_return=Decimal("0.015"),
        )

    @pytest.fixture
    def trade_execution(self, sized_opportunity: SizedOpportunity) -> TradeExecution:
        """Create a TradeExecution instance for testing."""
        return TradeExecution(sized_opportunity)

    def test_initial_state(self, trade_execution: TradeExecution) -> None:
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

    def test_to_dict(self, trade_execution: TradeExecution) -> None:
        """Test conversion to dictionary."""
        # Set some properties
        trade_execution.status = ExecutionStatus.EXECUTING
        trade_execution.long_order_id = "order123"
        trade_execution.short_order_id = "order456"
        trade_execution.start_time = datetime.now(UTC)

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

    def test_str_representation(self, trade_execution: TradeExecution) -> None:
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
    def mock_config(self) -> MagicMock:
        cfg = MagicMock(spec=Config)
        cfg.get.side_effect = lambda key, default=None: {
            "execution.max_retries": 3,
            "execution.retry_delay_base_sec": 0.01,  # Very short delay for tests
            "exchanges.hyperliquid.collateral_asset": "USD",
            "exchanges.backpack.collateral_asset": "USDC",
            "execution.compensation.use_limit_orders": True,
            "execution.compensation.limit_price_offset_pct": "0.05",  # Use string for Decimal init
            "execution.order_placement_type": "concurrent",  # Default for most tests
            "execution.use_market_orders": True,  # Default for most tests
            "execution.max_history": 100,
            "execution.settlement_delay": 0.01,  # Short delay for tests
        }.get(key, default)
        return cfg

    @pytest.fixture
    def mock_portfolio_tracker(self) -> MagicMock:
        tracker = MagicMock(spec=PortfolioTracker)
        tracker.update_order = MagicMock()
        tracker.update_position = MagicMock()
        tracker.update_realized_pnl = MagicMock()
        tracker.get_position = MagicMock(return_value=None)  # Default mock for get_position
        return tracker

    @pytest.fixture
    def mock_symbol_mapper(self) -> MagicMock:
        """Provides a mock SymbolMapper."""
        mapper = MagicMock(spec=SymbolMapper)
        # Configure default successful mappings
        mapper.get_exchange_symbol.side_effect = lambda internal, ex_id: {
            ("BTC", "hyperliquid"): "BTC-PERP",
            ("BTC", "backpack"): "BTC_USDC",
            ("ETH", "hyperliquid"): "ETH-PERP",
        }.get((internal, ex_id))
        mapper.get_internal_symbol.side_effect = lambda ex_sym, ex_id: {
            ("BTC-PERP", "hyperliquid"): "BTC",
            ("BTC_USDC", "backpack"): "BTC",
            ("ETH-PERP", "hyperliquid"): "ETH",
        }.get((ex_sym, ex_id))
        mapper.get_all_internal_symbols = MagicMock(return_value=["BTC", "ETH"])
        return mapper

    @pytest.fixture
    def mock_circuit_breaker_system(self) -> MagicMock:
        """Provides a mock CircuitBreakerSystem."""
        system = MagicMock(spec=CircuitBreakerSystem)
        system.can_execute = MagicMock(return_value=(True, None))
        system.check_all = MagicMock(return_value=(True, "All systems OK"))
        system.record_api_error = MagicMock()
        system.record_critical_failure = MagicMock()
        system.record_success = MagicMock()
        system.reset_exchange_breakers = MagicMock(return_value=1)
        return system

    @pytest.fixture
    def mock_hl_api(self) -> MagicMock:
        """Provides a mock API client for Hyperliquid."""
        api = MagicMock(spec=ExchangeAPI)
        api.place_order = AsyncMock()
        api.get_order_status = AsyncMock()
        api.cancel_order = AsyncMock()
        api.get_ticker = AsyncMock()
        api.exchange_id = "hyperliquid"
        return api

    @pytest.fixture
    def mock_bp_api(self) -> MagicMock:
        """Provides a mock API client for Backpack."""
        api = MagicMock(spec=ExchangeAPI)
        api.place_order = AsyncMock()
        api.get_order_status = AsyncMock()
        api.cancel_order = AsyncMock()
        api.get_ticker = AsyncMock()
        api.exchange_id = "backpack"
        return api

    @pytest.fixture
    def execution_handler(
        self,
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        mock_symbol_mapper: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_hl_api: MagicMock,
        mock_bp_api: MagicMock,
    ) -> ExecutionHandler:
        """Create an ExecutionHandler instance with correctly mocked dependencies."""
        handler = ExecutionHandler(
            config=mock_config,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=mock_circuit_breaker_system,
        )
        # Correctly register clients using the method signature
        handler.register_api_client(mock_hl_api.exchange_id, mock_hl_api)
        handler.register_api_client(mock_bp_api.exchange_id, mock_bp_api)
        return handler

    @pytest.fixture
    def sized_opportunity(
        self, mock_arbitrage_opportunity: ArbitrageOpportunity
    ) -> SizedOpportunity:
        """Provides a mock SizedOpportunity."""
        return SizedOpportunity(
            opportunity=mock_arbitrage_opportunity,
            long_size=Decimal("1.0"),
            short_size=Decimal("1.0"),
            allocation_percentage=Decimal("0.1"),
            expected_profit=Decimal("50.0"),
            expected_return=Decimal("0.02"),
            risk_adjusted_return=Decimal("0.015"),
        )

    def test_register_api_client(self, execution_handler: ExecutionHandler) -> None:
        """Test that API clients can be registered."""
        new_api = MagicMock(spec=ExchangeAPI)
        new_api.exchange_id = "new_exchange"
        # Correct call to register_api_client
        execution_handler.register_api_client(new_api.exchange_id, new_api)
        assert "new_exchange" in execution_handler.api_clients
        assert execution_handler.api_clients["new_exchange"] is new_api

    @pytest.mark.asyncio
    async def test_circuit_breaker_open_rejection(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_circuit_breaker_system: MagicMock,
    ) -> None:
        """Test that executions are rejected when the relevant circuit breaker is open."""
        # Configure circuit breaker to be open for one exchange
        mock_circuit_breaker_system.can_execute.return_value = (
            False,
            "Circuit breaker open for hyperliquid",
        )

        # Attempt to execute
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assertions
        mock_circuit_breaker_system.can_execute.assert_called_once_with(
            "hyperliquid"
        )  # Check if it checked the long exchange
        assert execution is not None
        assert execution.status == ExecutionStatus.REJECTED  # Use REJECTED status
        assert (
            execution.error_message is not None
            and "Circuit breaker open" in execution.error_message
        )

    @pytest.mark.asyncio
    async def test_execute_opportunity_mapping_failure(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_symbol_mapper: MagicMock,
        mock_hl_api: MagicMock,
        mock_bp_api: MagicMock,
    ) -> None:
        """Test execution failure when symbol mapping fails."""
        # Configure symbol mapper to fail for one symbol
        mock_symbol_mapper.get_exchange_symbol.side_effect = lambda internal, ex_id: {
            ("BTC", "hyperliquid"): "BTC-PERP",
            # ("BTC", "backpack"): "BTC_USDC", # Missing mapping
        }.get((internal, ex_id))

        # Attempt to execute
        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Assertions
        assert execution is not None
        assert execution.status == ExecutionStatus.FAILED
        assert (
            execution.error_message is not None
            and "Failed to map symbol" in execution.error_message
        )
        # Ensure no API calls were made if mapping failed early
        mock_hl_api.get_ticker.assert_not_called()
        mock_bp_api.get_ticker.assert_not_called()
        mock_hl_api.place_order.assert_not_called()
        mock_bp_api.place_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_place_order_with_retry_success(
        self, execution_handler: ExecutionHandler, mock_hl_api: MagicMock
    ) -> None:
        """Test successful order placement via retry helper."""
        mock_order = Order(
            symbol="BTC-PERP",
            order_id="order1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            status=OrderStatus.FILLED,
            timestamp=datetime.now(UTC),
        )
        mock_hl_api.place_order.return_value = mock_order

        # Correct call signature for _place_order_with_retry
        result_order = await execution_handler._place_order_with_retry(
            client=mock_hl_api,  # Use 'client' kwarg
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
            # execution_id is not a parameter
        )

        assert result_order == mock_order
        mock_hl_api.place_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_order_with_retry_failure(
        self, execution_handler: ExecutionHandler, mock_hl_api: MagicMock
    ) -> None:
        """Test order placement failure after retries via retry helper."""
        # Correct APIError instantiation and ErrorCode
        mock_hl_api.place_order.side_effect = APIError(
            code=APIErrorCode.RATE_LIMITED,  # Correct enum member
            message="Rate limited",
            exchange_code="RL01",  # Example exchange code
        )

        with pytest.raises(APIError):
            # Correct call signature for _place_order_with_retry
            await execution_handler._place_order_with_retry(
                client=mock_hl_api,  # Use 'client' kwarg
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
                # execution_id is not a parameter
            )

        assert mock_hl_api.place_order.call_count == 4  # Initial call + 3 retries

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self, execution_handler: ExecutionHandler, mock_hl_api: MagicMock
    ) -> None:
        """Test successful retrieval of order status."""
        mock_order = Order(
            symbol="BTC-PERP",
            order_id="order1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            status=OrderStatus.FILLED,
            timestamp=datetime.now(UTC),
        )
        mock_hl_api.get_order_status.return_value = mock_order

        # Correct method name and signature
        result_status = await execution_handler._get_order_status(
            client=mock_hl_api,  # Use 'client' kwarg
            order_id="order1",
            symbol="BTC-PERP",  # Symbol is optional but good to provide
            # execution_id is not a parameter
        )

        assert result_status == mock_order
        mock_hl_api.get_order_status.assert_called_once_with(order_id="order1", symbol="BTC-PERP")

    @pytest.mark.asyncio
    async def test_get_order_status_failure(
        self, execution_handler: ExecutionHandler, mock_hl_api: MagicMock
    ) -> None:
        """Test failure during order status retrieval."""
        # Correct APIError instantiation and ErrorCode
        mock_hl_api.get_order_status.side_effect = APIError(
            code=APIErrorCode.UNKNOWN,  # Correct enum member
            message="Server error",
            exchange_code="E500",  # Example exchange code
        )

        with pytest.raises(APIError):
            # Correct method name and signature
            await execution_handler._get_order_status(
                client=mock_hl_api,  # Use 'client' kwarg
                order_id="order1",
                symbol="BTC-PERP",
                # execution_id is not a parameter
            )

        assert mock_hl_api.get_order_status.call_count == 4  # Initial call + 3 retries

    @pytest.mark.asyncio
    async def test_compensate_position_success(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: MagicMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test successful compensation placement."""
        # Mock necessary methods
        mock_ticker = Ticker(
            symbol="BTC-PERP",
            price=Decimal("40050.0"),
            bid=Decimal("40040.0"),
            ask=Decimal("40060.0"),
            timestamp=int(datetime.now(UTC).timestamp() * 1000),
        )
        mock_hl_api.get_ticker.return_value = mock_ticker

        # Create a mock TradeExecution object to pass to _compensate_position
        mock_execution = TradeExecution(sized_opportunity)
        mock_execution.status = (
            ExecutionStatus.PARTIALLY_COMPLETED
        )  # Example status before compensation

        mock_comp_order = Order(
            symbol="BTC-PERP",
            order_id="comp1",
            side=OrderSide.SELL,  # Compensating a BUY
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),  # Quantity from sized_opportunity
            price=Decimal("40040.0") * (Decimal("1") - Decimal("0.05")),  # Bid - offset
            status=OrderStatus.FILLED,  # Simulate filled compensation
            timestamp=datetime.now(UTC),
        )
        # Patch the retry helper directly for compensation
        with patch.object(
            execution_handler, "_place_order_with_retry", return_value=mock_comp_order
        ) as mock_place_retry:
            # Correct call signature for _compensate_position
            result = await execution_handler._compensate_position(
                failed_leg_exchange="backpack",  # Example failed exchange
                failed_leg_symbol="BTC",  # Internal symbol
                filled_leg_exchange="hyperliquid",  # The exchange to compensate on
                filled_leg_symbol="BTC",  # Internal symbol
                filled_leg_side=OrderSide.BUY,  # The side that was filled and needs closing
                filled_leg_quantity=sized_opportunity.long_size,  # Quantity to close
                execution=mock_execution,  # Pass the execution object
            )

            # Check the result type and value
            assert isinstance(result, Order)
            assert result == mock_comp_order
            mock_hl_api.get_ticker.assert_called_once_with(
                "BTC-PERP"
            )  # Called to get price for limit order
            mock_place_retry.assert_called_once()
            # Check args passed to place_order_with_retry
            call_args = mock_place_retry.call_args[1]  # kwargs
            assert call_args["client"] == mock_hl_api
            assert call_args["symbol"] == "BTC-PERP"  # Exchange symbol
            assert call_args["side"] == OrderSide.SELL  # Opposite of filled leg
            assert (
                call_args["order_type"] == OrderType.LIMIT
            )  # Because use_limit_orders is True in config
            assert call_args["quantity"] == sized_opportunity.long_size
            assert call_args["price"] is not None  # Limit order needs price
            assert call_args["reduce_only"] is True

    @pytest.mark.asyncio
    async def test_compensate_position_mapping_failure(
        self,
        execution_handler: ExecutionHandler,
        mock_symbol_mapper: MagicMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test compensation failure due to symbol mapping error."""
        # Mock API client needed for the call
        mock_api = MagicMock(spec=ExchangeAPI)
        mock_api.exchange_id = "failing_exchange"
        execution_handler.register_api_client("failing_exchange", mock_api)  # Register the mock

        # Configure mapper to fail
        mock_symbol_mapper.get_exchange_symbol.return_value = None

        # Create a mock TradeExecution object
        mock_execution = TradeExecution(sized_opportunity)

        with pytest.raises(ValueError, match="Failed to map internal symbol"):
            # Correct call signature for _compensate_position
            await execution_handler._compensate_position(
                failed_leg_exchange="backpack",
                failed_leg_symbol="BTC",
                filled_leg_exchange="failing_exchange",  # Use the exchange that will fail mapping
                filled_leg_symbol="BTC",  # Internal symbol that will fail mapping
                filled_leg_side=OrderSide.BUY,
                filled_leg_quantity=sized_opportunity.long_size,
                execution=mock_execution,
            )
        mock_symbol_mapper.get_exchange_symbol.assert_called_once_with("BTC", "failing_exchange")

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_concurrent(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_hl_api: MagicMock,
        mock_bp_api: MagicMock,
        mock_portfolio_tracker: MagicMock,
    ) -> None:
        """Test successful concurrent execution of an opportunity."""
        # Mock tickers
        hl_ticker = Ticker(
            symbol="BTC-PERP",
            price=Decimal("41000"),
            bid=Decimal("40990"),
            ask=Decimal("41010"),
            timestamp=1,
        )
        bp_ticker = Ticker(
            symbol="BTC_USDC",
            price=Decimal("41100"),
            bid=Decimal("41090"),
            ask=Decimal("41110"),
            timestamp=1,
        )
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        # Mock successful order placements
        long_order = Order(
            symbol="BTC-PERP",
            order_id="hl_order_1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=sized_opportunity.long_size,
            status=OrderStatus.FILLED,
            filled_quantity=sized_opportunity.long_size,
            timestamp=datetime.now(UTC),
        )
        short_order = Order(
            symbol="BTC_USDC",
            order_id="bp_order_1",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=sized_opportunity.short_size,
            status=OrderStatus.FILLED,
            filled_quantity=sized_opportunity.short_size,
            timestamp=datetime.now(UTC),
        )

        # Side effect for place_order mocks
        async def place_retry_side_effect(*args: Any, **kwargs: Any) -> Order:
            client_arg: ExchangeAPI = kwargs["client"]
            if client_arg.exchange_id == "hyperliquid":
                return long_order
            elif client_arg.exchange_id == "backpack":
                return short_order
            else:
                raise ValueError(
                    f"Unexpected exchange_id in place_side_effect: {client_arg.exchange_id}"
                )

        # Side effect for get_order_status mocks
        async def get_status_side_effect(*args: Any, **kwargs: Any) -> Order | None:
            client_arg: ExchangeAPI = kwargs["client"]
            order_id_arg: str = kwargs["order_id"]
            if client_arg.exchange_id == "hyperliquid" and order_id_arg == "hl_order_1":
                return long_order
            elif client_arg.exchange_id == "backpack" and order_id_arg == "bp_order_1":
                return short_order
            return None

        # Use patch to mock the internal retry helper directly
        with (
            patch.object(
                execution_handler, "_place_order_with_retry", side_effect=place_retry_side_effect
            ) as mock_place_retry,
            patch.object(
                execution_handler, "_get_order_status", side_effect=get_status_side_effect
            ) as mock_get_status,
        ):
            # Execute
            execution = await execution_handler.execute_opportunity(sized_opportunity)

            # Assertions
            assert execution is not None
            assert execution.status == ExecutionStatus.COMPLETED
            assert execution.error_message is None
            assert execution.long_order_id == "hl_order_1"
            assert execution.short_order_id == "bp_order_1"
            # Check that the responses were stored (they are dicts from the Order.to_dict())
            assert execution.long_order_response is not None
            assert execution.long_order_response["order_id"] == "hl_order_1"
            assert execution.short_order_response is not None
            assert execution.short_order_response["order_id"] == "bp_order_1"

            # Verify API calls (via the patched methods)
            assert mock_place_retry.call_count == 2
            mock_place_retry.assert_any_call(
                client=mock_hl_api,
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=sized_opportunity.long_size,
                time_in_force=TimeInForce.IOC,
            )
            mock_place_retry.assert_any_call(
                client=mock_bp_api,
                symbol="BTC_USDC",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=sized_opportunity.short_size,
                time_in_force=TimeInForce.IOC,
            )

            assert mock_get_status.call_count == 2
            mock_get_status.assert_any_call(
                client=mock_hl_api, order_id="hl_order_1", symbol="BTC-PERP"
            )
            mock_get_status.assert_any_call(
                client=mock_bp_api, order_id="bp_order_1", symbol="BTC_USDC"
            )

            # Verify portfolio tracker updates
            mock_portfolio_tracker.update_order.assert_any_call(long_order)
            mock_portfolio_tracker.update_order.assert_any_call(short_order)

    @pytest.mark.asyncio
    async def test_execute_opportunity_compensation_needed(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_hl_api: MagicMock,
        mock_bp_api: MagicMock,
        mock_portfolio_tracker: MagicMock,
        mock_circuit_breaker_system: MagicMock,
    ) -> None:
        """Test execution flow when one leg fails and compensation is triggered."""

        # Mock get_position for portfolio tracker
        def mock_get_position(exchange_id: str, internal_symbol: str) -> Position | None:
            # Simulate the long position exists after the first leg filled
            if exchange_id == "hyperliquid" and internal_symbol == "BTC":
                return Position(
                    symbol="BTC",  # Internal symbol
                    side=OrderSide.BUY,
                    size=sized_opportunity.long_size,
                    entry_price=Decimal("41000"),  # Example entry price
                    timestamp=int(datetime.now(UTC).timestamp() * 1000),
                )
            return None

        mock_portfolio_tracker.get_position = MagicMock(side_effect=mock_get_position)

        # Mock tickers
        hl_ticker = Ticker(
            symbol="BTC-PERP",
            price=Decimal("41000"),
            bid=Decimal("40990"),
            ask=Decimal("41010"),
            timestamp=1,
        )
        bp_ticker = Ticker(
            symbol="BTC_USDC",
            price=Decimal("41100"),
            bid=Decimal("41090"),
            ask=Decimal("41110"),
            timestamp=1,
        )
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        # Mock order placements: success for long, failure for short, success for compensation
        long_order = Order(
            symbol="BTC-PERP",
            order_id="hl_order_filled",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=sized_opportunity.long_size,
            status=OrderStatus.FILLED,
            filled_quantity=sized_opportunity.long_size,
            timestamp=datetime.now(UTC),
        )
        short_order_failure = APIError(
            code=APIErrorCode.ORDER_REJECTED,  # Correct enum
            message="Insufficient funds",
            exchange_code="BPX-1001",  # Example code
        )
        comp_order = Order(
            symbol="BTC-PERP",
            order_id="hl_comp_1",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=sized_opportunity.long_size,
            price=Decimal("40990") * (Decimal("1") - Decimal("0.05")),  # Bid - offset for selling
            status=OrderStatus.FILLED,
            filled_quantity=sized_opportunity.long_size,
            timestamp=datetime.now(UTC),
        )

        call_count = 0
        compensation_triggered = False

        # Side effect for _place_order_with_retry mock
        async def place_retry_side_effect(*args: Any, **kwargs: Any) -> Order:
            nonlocal call_count, compensation_triggered
            call_count += 1
            client_arg: ExchangeAPI = kwargs["client"]
            side_arg: OrderSide = kwargs["side"]

            if call_count == 1:  # First call (initial placement)
                if client_arg.exchange_id == "hyperliquid":
                    return long_order
                elif client_arg.exchange_id == "backpack":
                    raise short_order_failure  # Raise error for failure
                else:
                    raise ValueError("Unexpected exchange in place_retry_side_effect call 1")
            elif call_count == 2:  # Second call (compensation)
                if client_arg.exchange_id == "hyperliquid" and side_arg == OrderSide.SELL:
                    compensation_triggered = True
                    return comp_order
                else:
                    raise ValueError("Unexpected call during compensation")
            else:
                raise RuntimeError("Should not be called more than twice")

        # Side effect for _get_order_status mock
        async def get_status_side_effect(*args: Any, **kwargs: Any) -> Order | None:
            client_arg: ExchangeAPI = kwargs["client"]
            order_id_arg: str = kwargs["order_id"]

            if client_arg.exchange_id == "hyperliquid":
                if order_id_arg == "hl_order_filled":
                    return long_order
                elif order_id_arg == "hl_comp_1":
                    return comp_order  # Return compensation order status if checked
            # Backpack order status won't be checked as placement failed
            return None  # Default

        # Patch the internal helper methods
        with (
            patch.object(
                execution_handler, "_place_order_with_retry", side_effect=place_retry_side_effect
            ) as mock_place_retry,
            patch.object(
                execution_handler, "_get_order_status", side_effect=get_status_side_effect
            ) as mock_get_status,
            patch.object(
                execution_handler,
                "_compensate_position",
                wraps=execution_handler._compensate_position,
            ) as mock_compensate,
        ):  # Wrap to call real logic but allow assertion
            # Execute
            execution = await execution_handler.execute_opportunity(sized_opportunity)

            # Assertions
            assert execution is not None
            # If compensation is successful, the final status should be COMPLETED,
            assert (
                execution.status == ExecutionStatus.COMPLETED
            )  # Expect COMPLETED if compensation worked
            assert execution.error_message is None  # No error if compensation succeeded
            assert execution.long_order_id == "hl_order_filled"
            assert execution.short_order_id is None  # Short leg failed
            # Check if compensation details were recorded (e.g., in metadata or a dedicated field if added)
            # assert execution.compensation_order_id == "hl_comp_1" # This attribute doesn't exist
            assert compensation_triggered is True  # Ensure compensation logic was hit in mock

            # Verify API calls (via patched methods)
            assert (
                mock_place_retry.call_count == 2
            )  # Initial (HL success, BP fail) + Compensation (HL success)
            assert mock_get_status.call_count >= 1  # Status check for long order is expected
            mock_compensate.assert_called_once()  # Verify _compensate_position was called

            # Verify portfolio tracker updates
            mock_portfolio_tracker.update_order.assert_any_call(long_order)  # Initial order
            mock_portfolio_tracker.update_order.assert_any_call(comp_order)  # Compensation order

    def test_get_execution_history(self, execution_handler: ExecutionHandler) -> None:
        """Test retrieving execution history."""
        # Add dummy executions to history for testing
        exec1 = MagicMock(spec=TradeExecution)
        exec2 = MagicMock(spec=TradeExecution)
        # Append to the correct attribute 'executions'
        execution_handler.executions.append(exec1)
        execution_handler.executions.append(exec2)

        history = execution_handler.get_execution_history()
        assert len(history) == 2
        assert exec1 in history
        assert exec2 in history
        # Check it's a copy
        assert history is not execution_handler.executions

    def test_get_active_executions(self, execution_handler: ExecutionHandler) -> None:
        """Test retrieving active executions."""
        # Add dummy active executions
        exec1 = MagicMock(spec=TradeExecution, status=ExecutionStatus.EXECUTING)
        exec2 = MagicMock(spec=TradeExecution, status=ExecutionStatus.PENDING)
        exec3 = MagicMock(spec=TradeExecution, status=ExecutionStatus.COMPLETED)  # Not active
        execution_handler.active_executions["exec1"] = exec1
        execution_handler.active_executions["exec2"] = exec2
        execution_handler.active_executions["exec3"] = exec3

        active = execution_handler.get_active_executions()
        assert len(active) == 2
        assert exec1 in active
        assert exec2 in active

    def test_reset_circuit_breaker(
        self, execution_handler: ExecutionHandler, mock_circuit_breaker_system: MagicMock
    ) -> None:
        """Test resetting circuit breaker via the handler."""
        execution_handler.reset_circuit_breaker("hyperliquid")
        mock_circuit_breaker_system.reset_exchange_breakers.assert_called_once_with("hyperliquid")
