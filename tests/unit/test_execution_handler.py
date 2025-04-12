import time
from datetime import UTC, datetime
from decimal import Decimal
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
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config


@pytest.fixture
def mock_arbitrage_opportunity():
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
    def sized_opportunity(self, mock_arbitrage_opportunity):
        """Create a SizedOpportunity for testing."""
        return SizedOpportunity(
            opportunity=mock_arbitrage_opportunity,
            long_size=Decimal("1000.0"),
            short_size=Decimal("1000.0"),
            allocation_percentage=0.1,
            expected_profit=Decimal("50.0"),
            expected_return=0.02,
            risk_adjusted_return=0.015,
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
        cfg = MagicMock(spec=Config)
        cfg.get.side_effect = lambda key, default=None: {
            "execution.max_retries": 3,
            "execution.retry_delay_base_sec": 0.01,  # Very short delay for tests
            "exchanges.hyperliquid.collateral_asset": "USD",
            "exchanges.backpack.collateral_asset": "USDC",
            "execution.compensation.use_limit_orders": True,
            "execution.compensation.limit_price_offset_pct": 0.05,
            "execution.order_placement_type": "concurrent",  # Default for most tests
            "execution.use_market_orders": True,  # Default for most tests
            "execution.max_history": 100,
            "execution.settlement_delay": 0.01,  # Short delay for tests
        }.get(key, default)
        return cfg

    @pytest.fixture
    def mock_portfolio_tracker(self):
        tracker = MagicMock(spec=PortfolioTracker)
        tracker.update_order = MagicMock()
        tracker.update_position = MagicMock()
        tracker.update_realized_pnl = MagicMock()
        return tracker

    @pytest.fixture
    def mock_symbol_mapper(self):
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
    def mock_circuit_breaker_system(self):
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
    def mock_hl_api(self):
        """Provides a mock API client for Hyperliquid."""
        api = MagicMock(spec=ExchangeAPI)
        api.place_order = AsyncMock()
        api.get_order_status = AsyncMock()
        api.cancel_order = AsyncMock()
        api.get_ticker = AsyncMock()
        api.exchange_id = "hyperliquid"
        return api

    @pytest.fixture
    def mock_bp_api(self):
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
        mock_config,
        mock_portfolio_tracker,
        mock_symbol_mapper,
        mock_circuit_breaker_system,
        mock_hl_api,
        mock_bp_api,
    ):
        """Create an ExecutionHandler instance with correctly mocked dependencies."""
        handler = ExecutionHandler(
            config=mock_config,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=mock_circuit_breaker_system,
        )
        handler.register_api_client("hyperliquid", mock_hl_api)
        handler.register_api_client("backpack", mock_bp_api)
        return handler

    @pytest.fixture
    def sized_opportunity(self, mock_arbitrage_opportunity):
        """Create a SizedOpportunity for testing."""
        return SizedOpportunity(
            opportunity=mock_arbitrage_opportunity,
            long_size=Decimal("1000.0"),
            short_size=Decimal("1000.0"),
            allocation_percentage=Decimal("0.1"),
            expected_profit=Decimal("5.0"),
            expected_return=Decimal("0.01"),
            risk_adjusted_return=Decimal("0.008"),
        )

    def test_register_api_client(self, execution_handler):
        """Test that API clients can be registered."""
        # Register a new API client
        new_api = MagicMock(spec=ExchangeAPI)
        execution_handler.register_api_client("test_exchange", new_api)

        # Verify the client was registered
        assert "test_exchange" in execution_handler.api_clients
        assert execution_handler.api_clients["test_exchange"] == new_api

    @pytest.mark.asyncio
    async def test_circuit_breaker_open_rejection(
        self, execution_handler, sized_opportunity, mock_circuit_breaker_system
    ):
        """Test that executions are rejected when the relevant circuit breaker is open."""
        # Mock the system check to return False (blocked) for the long exchange
        breaker_message = "Test breaker tripped for hyperliquid"
        mock_circuit_breaker_system.can_execute.side_effect = (
            lambda ex, sym: (False, breaker_message) if ex == "hyperliquid" else (True, None)
        )

        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Verify the execution was rejected
        assert execution.status == ExecutionStatus.REJECTED
        assert breaker_message in execution.error_message
        # Verify CB was checked with internal symbol for the specific exchange
        mock_circuit_breaker_system.can_execute.assert_any_call("hyperliquid", "BTC")

    @pytest.mark.asyncio
    async def test_execute_opportunity_mapping_failure(
        self, execution_handler, sized_opportunity, mock_symbol_mapper
    ):
        """Test execution fails if symbol mapping returns None."""
        # Configure mock mapper to fail for the short symbol lookup
        mock_symbol_mapper.get_exchange_symbol.side_effect = lambda internal, ex_id: (
            "BTC-PERP" if ex_id == "hyperliquid" else None
        )

        execution = await execution_handler.execute_opportunity(sized_opportunity)

        # Verify failure status and error message
        assert execution.status == ExecutionStatus.FAILED
        assert "Cannot map internal symbol 'BTC'" in execution.error_message
        assert "Short (backpack): 'None'" in execution.error_message

        # Verify APIs were NOT called for tickers or orders
        execution_handler.api_clients["hyperliquid"].get_ticker.assert_not_called()
        execution_handler.api_clients["backpack"].get_ticker.assert_not_called()
        # _place_order_with_retry is patched in other tests, direct calls won't happen here
        # Need to check if the api_client's place_order was called if not patching
        execution_handler.api_clients["hyperliquid"].place_order.assert_not_called()
        execution_handler.api_clients["backpack"].place_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_place_order_with_retry_success(
        self, execution_handler, mock_hl_api, mock_portfolio_tracker
    ):
        """Test successful order placement via retry helper."""
        mock_order = Order(
            id="order1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            status=OrderStatus.FILLED,
        )
        mock_hl_api.place_order.return_value = mock_order

        result_order = await execution_handler._place_order_with_retry(
            client=mock_hl_api,
            exchange_id="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
        )

        assert result_order == mock_order
        mock_hl_api.place_order.assert_called_once_with(
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            price=None,
            time_in_force="GTC",
            reduce_only=False,
        )
        mock_portfolio_tracker.update_order.assert_called_once_with("hyperliquid", mock_order)

    @pytest.mark.asyncio
    async def test_place_order_with_retry_failure(
        self, execution_handler, mock_hl_api, mock_circuit_breaker_system
    ):
        """Test order placement failure after retries via retry helper."""
        api_error = APIError("Place failed", code=APIErrorCode.UNKNOWN, exchange_code="hyperliquid")
        mock_hl_api.place_order.side_effect = api_error

        result_order = await execution_handler._place_order_with_retry(
            client=mock_hl_api,
            exchange_id="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
        )

        assert result_order is None
        assert mock_hl_api.place_order.call_count == execution_handler.max_retries

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self, execution_handler, mock_hl_api, mock_portfolio_tracker
    ):
        """Test successful retrieval of order status."""
        mock_order = Order(
            id="order1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=Decimal("1"),
            status=OrderStatus.FILLED,
        )
        mock_hl_api.get_order_status.return_value = mock_order

        result = await execution_handler._get_order_status(
            mock_hl_api, "hyperliquid", "order1", "BTC-PERP"
        )

        assert result == mock_order
        mock_hl_api.get_order_status.assert_called_once_with(order_id="order1", symbol="BTC-PERP")
        mock_portfolio_tracker.update_order.assert_called_once_with("hyperliquid", mock_order)

    @pytest.mark.asyncio
    async def test_get_order_status_failure(
        self, execution_handler, mock_hl_api, mock_circuit_breaker_system
    ):
        """Test failure when retrieving order status."""
        api_error = APIError(
            "Not found", code=APIErrorCode.ORDER_NOT_FOUND, exchange_code="hyperliquid"
        )
        mock_hl_api.get_order_status.side_effect = api_error

        result = await execution_handler._get_order_status(
            mock_hl_api, "hyperliquid", "order1", "BTC-PERP"
        )

        assert result is None
        mock_hl_api.get_order_status.assert_called_once_with(order_id="order1", symbol="BTC-PERP")
        mock_circuit_breaker_system.record_api_error.assert_called_once_with(
            "hyperliquid", f"Get order status failed: {api_error}"
        )

    @pytest.mark.asyncio
    async def test_compensate_position_success(
        self, execution_handler, mock_hl_api, mock_symbol_mapper
    ):
        """Test successful compensation placement."""
        # --- MOCK Portfolio Tracker ---
        mock_position = MagicMock(spec=Position)
        mock_position.size = Decimal("0.6")  # Example size
        execution_handler.portfolio_tracker.get_position.return_value = mock_position
        # -----------------------------

        # Mock ticker needed for limit price calculation
        mock_ticker = Ticker(
            symbol="BTC-PERP",
            timestamp=time.time() * 1000,
            bid=Decimal("40000"),
            ask=Decimal("40010"),
            price=Decimal("40005"),
        )
        mock_hl_api.get_ticker.return_value = mock_ticker

        # Mock the order that will be returned by the mocked _place_order_with_retry
        mock_comp_order = Order(
            id="comp1",
            symbol="BTC-PERP",
            side=OrderSide.SELL,
            type=OrderType.LIMIT,
            quantity=Decimal("0.5"),
            status=OrderStatus.FILLED,
        )

        # Patch the internal _place_order_with_retry method
        with patch.object(
            execution_handler, "_place_order_with_retry", new_callable=AsyncMock
        ) as mock_place_retry:
            mock_place_retry.return_value = mock_comp_order

            result = await execution_handler._compensate_position(
                client=mock_hl_api,
                exchange_id="hyperliquid",
                internal_symbol="BTC",
                original_failed_side=OrderSide.SELL,
                quantity=Decimal("0.5"),
            )

            assert result is True
            mock_symbol_mapper.get_exchange_symbol.assert_called_once_with("BTC", "hyperliquid")
            mock_hl_api.get_ticker.assert_called_once_with("BTC-PERP")
            mock_place_retry.assert_called_once()
            call_args = mock_place_retry.call_args.kwargs
            assert call_args["symbol"] == "BTC-PERP"
            assert call_args["side"] == OrderSide.SELL
            assert call_args["quantity"] == Decimal("0.5")
            assert call_args["reduce_only"] is True
            assert call_args["order_type"] == OrderType.LIMIT
            assert call_args["price"] is not None
            assert call_args["price"] < mock_ticker.bid

    @pytest.mark.asyncio
    async def test_compensate_position_mapping_failure(
        self, execution_handler, mock_hl_api, mock_symbol_mapper, mock_circuit_breaker_system
    ):
        """Test compensation fails if symbol mapping returns None."""
        # --- MOCK Portfolio Tracker (to avoid TypeError even if mapping fails early) ---
        mock_position = MagicMock(spec=Position)
        mock_position.size = Decimal("1.0")
        execution_handler.portfolio_tracker.get_position.return_value = mock_position
        # -------------------------------------------------------------------------------

        # Configure mapper to fail
        mock_symbol_mapper.get_exchange_symbol.return_value = None

        # Patch the internal _place_order_with_retry to ensure it's not called
        with patch.object(
            execution_handler, "_place_order_with_retry", new_callable=AsyncMock
        ) as mock_place_retry:
            result = await execution_handler._compensate_position(
                client=mock_hl_api,
                exchange_id="hyperliquid",
                internal_symbol="UNKNOWN",
                original_failed_side=OrderSide.SELL,
                quantity=Decimal("0.5"),
            )

            assert result is False
            # Verify mapper was called
            mock_symbol_mapper.get_exchange_symbol.assert_called_once_with("UNKNOWN", "hyperliquid")
            # Verify API calls were not made
            mock_hl_api.get_ticker.assert_not_called()
            mock_place_retry.assert_not_called()
            # Verify circuit breaker recorded critical failure
            mock_circuit_breaker_system.record_critical_failure.assert_called_once_with(
                "hyperliquid", "Symbol mapping failed during compensation for UNKNOWN"
            )

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_concurrent(
        self,
        execution_handler,
        sized_opportunity,
        mock_symbol_mapper,
        mock_hl_api,
        mock_bp_api,
        mock_circuit_breaker_system,
    ):
        """Test successful concurrent execution of an opportunity."""
        # Mock Tickers
        hl_ticker = Ticker(
            symbol="BTC-PERP",
            timestamp=time.time() * 1000,
            bid=Decimal("40000"),
            ask=Decimal("40010"),
            price=Decimal("40005"),
        )
        bp_ticker = Ticker(
            symbol="BTC_USDC",
            timestamp=time.time() * 1000,
            bid=Decimal("40020"),
            ask=Decimal("40030"),
            price=Decimal("40025"),
        )
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        # Mock Orders returned by _place_order_with_retry
        # Quantities calculated based on $1000 size and mock prices
        long_qty = (Decimal("1000") / hl_ticker.price).quantize(Decimal("1e-6"))
        short_qty = (Decimal("1000") / bp_ticker.price).quantize(Decimal("1e-6"))
        long_order = Order(
            id="long1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=long_qty,
            status=OrderStatus.FILLED,
            filled_quantity=long_qty,
            price=hl_ticker.price,
            avg_fill_price=hl_ticker.price,
        )
        short_order = Order(
            id="short1",
            symbol="BTC_USDC",
            side=OrderSide.SELL,
            type=OrderType.MARKET,
            quantity=short_qty,
            status=OrderStatus.FILLED,
            filled_quantity=short_qty,
            price=bp_ticker.price,
            avg_fill_price=bp_ticker.price,
        )

        # Patch the retry helper directly for this test
        with patch.object(
            execution_handler, "_place_order_with_retry", new_callable=AsyncMock
        ) as mock_place_retry:
            # Define side effect based on the exchange_id or symbol
            def place_side_effect(*args, **kwargs):
                if (
                    kwargs.get("exchange_id") == "hyperliquid"
                    and kwargs.get("symbol") == "BTC-PERP"
                ):
                    return long_order
                elif kwargs.get("exchange_id") == "backpack" and kwargs.get("symbol") == "BTC_USDC":
                    return short_order
                print(f"UNEXPECTED PLACE ORDER CALL: {kwargs}")  # Debug print
                return None  # Should not happen in this test

            mock_place_retry.side_effect = place_side_effect

            execution = await execution_handler.execute_opportunity(sized_opportunity)

            # Verify overall status
            assert execution.status == ExecutionStatus.COMPLETED
            assert execution.error_message is None

            # Verify mapper calls
            mock_symbol_mapper.get_exchange_symbol.assert_any_call("BTC", "hyperliquid")
            mock_symbol_mapper.get_exchange_symbol.assert_any_call("BTC", "backpack")

            # Verify ticker calls
            mock_hl_api.get_ticker.assert_called_once_with("BTC-PERP")
            mock_bp_api.get_ticker.assert_called_once_with("BTC_USDC")

            # Verify order placement calls (via patched helper)
            assert mock_place_retry.call_count == 2
            # Check arguments passed to the *mocked* _place_order_with_retry
            mock_place_retry.assert_any_call(
                client=mock_hl_api,
                exchange_id="hyperliquid",
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=long_qty,
                price=None,
            )
            mock_place_retry.assert_any_call(
                client=mock_bp_api,
                exchange_id="backpack",
                symbol="BTC_USDC",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=short_qty,
                price=None,
            )

            # Verify execution record details
            assert execution.long_order_id == "long1"
            assert execution.short_order_id == "short1"
            assert execution.long_fill_quantity == long_qty
            assert execution.short_fill_quantity == short_qty
            # Verify CB success recorded
            mock_circuit_breaker_system.record_success.assert_any_call("hyperliquid", "BTC")
            mock_circuit_breaker_system.record_success.assert_any_call("backpack", "BTC")

    @pytest.mark.asyncio
    async def test_execute_opportunity_compensation_needed(
        self,
        execution_handler,
        sized_opportunity,
        mock_symbol_mapper,
        mock_hl_api,
        mock_bp_api,
        mock_circuit_breaker_system,
    ):
        """Test execution flow when one leg fails and compensation is triggered."""
        # --- MOCK Portfolio Tracker for compensation check ---
        mock_position_hl = MagicMock(spec=Position)
        # Simulate the long position existing before compensation
        mock_position_hl.size = (Decimal("1000") / Decimal("40005")).quantize(Decimal("1e-6"))

        def mock_get_position(exchange_id, internal_symbol):
            if exchange_id == "hyperliquid" and internal_symbol == "BTC":
                return mock_position_hl
            return None

        execution_handler.portfolio_tracker.get_position = mock_get_position
        # -----------------------------------------------------

        # Mock Tickers
        hl_ticker = Ticker(
            symbol="BTC-PERP",
            timestamp=time.time() * 1000,
            bid=Decimal("40000"),
            ask=Decimal("40010"),
            price=Decimal("40005"),
        )
        bp_ticker = Ticker(
            symbol="BTC_USDC",
            timestamp=time.time() * 1000,
            bid=Decimal("40020"),
            ask=Decimal("40030"),
            price=Decimal("40025"),
        )
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        # Mock successful long order, failed short order
        long_qty = (Decimal("1000") / hl_ticker.price).quantize(Decimal("1e-6"))
        long_order = Order(
            id="long1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=long_qty,
            status=OrderStatus.FILLED,
            filled_quantity=long_qty,
            price=hl_ticker.price,
            avg_fill_price=hl_ticker.price,
        )
        short_order_failure_exception = APIError(
            "Short order failed", code=APIErrorCode.UNKNOWN, exchange_code="backpack"
        )

        # Mock successful compensation order (placed on hyperliquid to close the long)
        comp_order = Order(
            id="comp1",
            symbol="BTC-PERP",
            side=OrderSide.SELL,
            type=OrderType.MARKET,
            quantity=long_qty,
            status=OrderStatus.FILLED,
        )

        # Patch _place_order_with_retry
        with patch.object(
            execution_handler, "_place_order_with_retry", new_callable=AsyncMock
        ) as mock_place_retry:
            # Side effect: Success for long, failure for short, success for compensation
            call_count = 0
            compensation_triggered = False

            def place_side_effect(*args, **kwargs):
                nonlocal call_count, compensation_triggered
                call_count += 1
                symbol = kwargs.get("symbol")
                side = kwargs.get("side")
                exchange = kwargs.get("exchange_id")

                print(
                    f"Mock place_order call {call_count}: {exchange} {symbol} {side}"
                )  # Debug print

                if (
                    exchange == "hyperliquid"
                    and symbol == "BTC-PERP"
                    and side == OrderSide.BUY
                    and call_count <= 2
                ):
                    print("  Returning long_order")
                    return long_order
                elif (
                    exchange == "backpack"
                    and symbol == "BTC_USDC"
                    and side == OrderSide.SELL
                    and call_count <= 2
                ):
                    print("  Raising short order failure")
                    raise short_order_failure_exception
                elif (
                    exchange == "hyperliquid" and symbol == "BTC-PERP" and side == OrderSide.SELL
                ):  # Compensation call
                    print("  Returning compensation order")
                    compensation_triggered = True
                    return comp_order
                else:
                    pytest.fail(f"Unexpected call to _place_order_with_retry: {kwargs}")
                return None  # Should not be reached normally

            mock_place_retry.side_effect = place_side_effect

            execution = await execution_handler.execute_opportunity(sized_opportunity)

            # Verify final status is FAILED (even if compensation worked)
            assert execution.status == ExecutionStatus.FAILED
            assert "One or both order placements failed." in execution.error_message
            assert (
                "Compensation successful." in execution.error_message
            )  # Check compensation message
            assert execution.long_order_id == "long1"
            assert execution.short_order_id is None  # Short order placement failed

            # Verify mapper calls
            mock_symbol_mapper.get_exchange_symbol.assert_any_call("BTC", "hyperliquid")
            mock_symbol_mapper.get_exchange_symbol.assert_any_call("BTC", "backpack")

            # Verify _place_order_with_retry calls (initial long, initial short, compensation long)
            assert mock_place_retry.call_count == 3
            assert compensation_triggered is True

            # Check compensation call details (it should be the third call)
            compensation_call = mock_place_retry.call_args_list[2]  # Third call
            comp_kwargs = compensation_call.kwargs
            assert comp_kwargs["client"] == mock_hl_api
            assert comp_kwargs["exchange_id"] == "hyperliquid"
            assert comp_kwargs["symbol"] == "BTC-PERP"  # Mapped symbol for compensation
            assert comp_kwargs["side"] == OrderSide.SELL  # Selling to compensate the long
            assert comp_kwargs["quantity"] == long_order.filled_quantity
            assert comp_kwargs["reduce_only"] is True

            # Verify CB recorded API errors for the failed leg
            mock_circuit_breaker_system.record_api_error.assert_any_call(
                "backpack", f"Concurrent order placement failed: {short_order_failure_exception}"
            )

    def test_get_execution_history(self, execution_handler):
        """Test retrieving execution history."""
        # Add dummy executions to history for testing
        exec1 = MagicMock(spec=TradeExecution)
        exec2 = MagicMock(spec=TradeExecution)
        execution_handler.execution_history = [exec1, exec2]

        history = execution_handler.get_execution_history()
        assert history == [exec1, exec2]
        # Ensure it's a copy
        assert history is not execution_handler.execution_history

    def test_get_active_executions(self, execution_handler):
        """Test retrieving active executions."""
        # Add dummy active executions
        exec1 = MagicMock(spec=TradeExecution)
        exec2 = MagicMock(spec=TradeExecution)
        execution_handler.active_executions = {"id1": exec1, "id2": exec2}

        active = execution_handler.get_active_executions()
        assert len(active) == 2
        assert exec1 in active
        assert exec2 in active

    def test_reset_circuit_breaker(self, execution_handler, mock_circuit_breaker_system):
        """Test resetting circuit breaker via the handler."""
        execution_handler.reset_circuit_breaker("hyperliquid")
        mock_circuit_breaker_system.reset_exchange_breakers.assert_called_once_with("hyperliquid")
