from datetime import UTC, datetime
from decimal import Decimal

# Removed unused import: from typing import Any
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
            "execution.retry_delay_base_sec": 0.01,
            "exchanges.hyperliquid.collateral_asset": "USD",
            "exchanges.backpack.collateral_asset": "USDC",
            "execution.compensation.use_limit_orders": True,
            "execution.compensation.limit_price_offset_pct": "0.05",
            "execution.order_placement_type": "concurrent",
            "execution.use_market_orders": True,
            "execution.max_history": 100,
            "execution.settlement_delay": 0.01,
        }.get(key, default)
        return cfg

    @pytest.fixture
    def mock_portfolio_tracker(self) -> MagicMock:
        tracker = MagicMock(spec=PortfolioTracker)
        tracker.update_order = MagicMock()
        tracker.update_position = MagicMock()
        tracker.update_realized_pnl = MagicMock()
        tracker.get_position = MagicMock(return_value=None)
        return tracker

    @pytest.fixture
    def mock_symbol_mapper(self) -> MagicMock:
        """Provides a mock SymbolMapper."""
        mapper = MagicMock(spec=SymbolMapper)
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
        api.exchange_name = "hyperliquid"  # Corrected: Use exchange_name
        api.place_order = AsyncMock()
        api.get_order_status = AsyncMock()
        api.cancel_order = AsyncMock()
        api.get_ticker = AsyncMock()
        return api

    @pytest.fixture
    def mock_bp_api(self) -> MagicMock:
        """Provides a mock API client for Backpack."""
        api = MagicMock(spec=ExchangeAPI)
        api.exchange_name = "backpack"  # Corrected: Use exchange_name
        api.place_order = AsyncMock()
        api.get_order_status = AsyncMock()
        api.cancel_order = AsyncMock()
        api.get_ticker = AsyncMock()
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
        handler.register_api_client(
            mock_hl_api.exchange_name, mock_hl_api
        )  # Corrected: Use exchange_name
        handler.register_api_client(
            mock_bp_api.exchange_name, mock_bp_api
        )  # Corrected: Use exchange_name
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
        new_api.exchange_name = "new_exchange"  # Corrected: Use exchange_name
        execution_handler.register_api_client(
            new_api.exchange_name, new_api
        )  # Corrected: Use exchange_name
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
        mock_circuit_breaker_system.can_execute.return_value = (
            False,
            "Circuit breaker open for hyperliquid",
        )
        execution = await execution_handler.execute_opportunity(sized_opportunity)
        mock_circuit_breaker_system.can_execute.assert_called_once_with("hyperliquid")
        assert execution is not None
        assert execution.status == ExecutionStatus.REJECTED
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
        mock_symbol_mapper.get_exchange_symbol.side_effect = lambda internal, ex_id: {
            ("BTC", "hyperliquid"): "BTC-PERP",
        }.get((internal, ex_id))
        execution = await execution_handler.execute_opportunity(sized_opportunity)
        assert execution is not None
        assert execution.status == ExecutionStatus.FAILED
        assert (
            execution.error_message is not None
            and "Failed to map symbol" in execution.error_message
        )
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
            id="hl_order_123",
            client_order_id="test_client_id",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("40000"),
            status=OrderStatus.FILLED,
            time=datetime.now(UTC),
        )
        mock_hl_api.place_order.return_value = mock_order
        result_order = await execution_handler._place_order_with_retry(
            exchange_id="hyperliquid",
            client=mock_hl_api,
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,  # Use 'order_type'
            quantity=Decimal("1.0"),
            price=Decimal("40000"),
            client_order_id="test_client_id",
            time_in_force=TimeInForce.IOC,
        )
        assert result_order == mock_order
        mock_hl_api.place_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_order_with_retry_failure(
        self, execution_handler: ExecutionHandler, mock_hl_api: MagicMock
    ) -> None:
        """Test order placement failure after retries via retry helper."""
        mock_hl_api.place_order.side_effect = APIError(
            code=APIErrorCode.RATE_LIMITED,
            message="Rate limited",
            exchange_code="RL01",
        )
        with pytest.raises(APIError):
            await execution_handler._place_order_with_retry(
                exchange_id="hyperliquid",
                client=mock_hl_api,
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,  # Use 'order_type'
                quantity=Decimal("1.0"),
                price=Decimal("40000"),
                client_order_id="test_client_id",
                time_in_force=TimeInForce.IOC,
            )
        assert mock_hl_api.place_order.call_count == 4

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self, execution_handler: ExecutionHandler, mock_hl_api: MagicMock
    ) -> None:
        """Test successful retrieval of order status."""
        mock_order = Order(
            symbol="BTC-PERP",
            id="hl_order_123",
            client_order_id="test_client_id",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("40000"),
            status=OrderStatus.FILLED,
            time=datetime.now(UTC),
        )
        mock_hl_api.get_order_status.return_value = mock_order
        result_status = await execution_handler._get_order_status(
            exchange_id="hyperliquid",
            client=mock_hl_api,
            order_id="hl_order_123",
            symbol="BTC-PERP",
            client_order_id="test_client_id",
        )
        assert result_status == mock_order
        mock_hl_api.get_order_status.assert_called_once_with(
            order_id="hl_order_123", symbol="BTC-PERP", client_order_id="test_client_id"
        )

    @pytest.mark.asyncio
    async def test_get_order_status_failure(
        self, execution_handler: ExecutionHandler, mock_hl_api: MagicMock
    ) -> None:
        """Test failure during order status retrieval."""
        mock_hl_api.get_order_status.side_effect = APIError(
            code=APIErrorCode.UNKNOWN,
            message="Server error",
            exchange_code="E500",
        )
        with pytest.raises(APIError):
            await execution_handler._get_order_status(
                exchange_id="hyperliquid",
                client=mock_hl_api,
                order_id="hl_order_123",
                symbol="BTC-PERP",
                client_order_id="test_client_id",
            )
        assert mock_hl_api.get_order_status.call_count == 4

    @pytest.mark.asyncio
    async def test_compensate_position_success(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: MagicMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test successful compensation placement."""
        mock_ticker = Ticker(
            symbol="BTC-PERP",
            price=Decimal("40050.0"),
            bid=Decimal("40040.0"),
            ask=Decimal("40060.0"),
            timestamp=1,
        )
        mock_hl_api.get_ticker.return_value = mock_ticker
        mock_execution = TradeExecution(sized_opportunity)
        mock_execution.status = ExecutionStatus.PARTIALLY_COMPLETED

        # Create a mock original order for context
        mock_original_order = Order(
            symbol="BTC-PERP",
            id="original_hl_order",
            client_order_id="orig_cid",
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=Decimal("0.5"),  # Match quantity to be compensated
            status=OrderStatus.FILLED,
            time=datetime.now(UTC),
        )

        mock_comp_order = Order(
            symbol="BTC-PERP",
            id="hl_comp_1",
            client_order_id="comp_client_id",
            side=OrderSide.SELL,
            type=OrderType.LIMIT,
            quantity=Decimal("0.5"),
            price=Decimal("40040.0") * (Decimal("1") - Decimal("0.05")),
            status=OrderStatus.FILLED,
            time=datetime.now(UTC),
        )
        with patch.object(
            execution_handler, "_place_order_with_retry", return_value=mock_comp_order
        ) as mock_place_retry:
            # Correct call signature for _compensate_position
            result = await execution_handler._compensate_position(
                client=mock_hl_api,
                exchange_id="hyperliquid",
                internal_symbol="BTC",
                quantity=Decimal("0.5"),
                original_order=mock_original_order,
            )

            # The function returns bool, not the order object
            assert result is True  # Correct assertion for bool return type
            mock_hl_api.get_ticker.assert_called_once_with("BTC-PERP")
            mock_place_retry.assert_called_once()
            call_args = mock_place_retry.call_args[1]
            assert call_args["client"] == mock_hl_api
            assert call_args["symbol"] == "BTC-PERP"
            assert call_args["side"] == OrderSide.SELL
            assert call_args["order_type"] == OrderType.LIMIT
            assert call_args["quantity"] == Decimal("0.5")
            assert call_args["price"] is not None
            assert call_args["reduce_only"] is True

    @pytest.mark.asyncio
    async def test_compensate_position_mapping_failure(
        self,
        execution_handler: ExecutionHandler,
        mock_symbol_mapper: MagicMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test compensation failure due to symbol mapping error."""
        mock_api = MagicMock(spec=ExchangeAPI)
        mock_api.exchange_name = "failing_exchange"  # Corrected: Use exchange_name
        execution_handler.register_api_client("failing_exchange", mock_api)
        mock_symbol_mapper.get_exchange_symbol.return_value = None
        # mock_execution = TradeExecution(sized_opportunity) # Removed unused variable
        # Create a mock original order for context
        mock_original_order = Order(
            symbol="BTC-PERP",
            id="orig_fail",
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=sized_opportunity.long_size,
            status=OrderStatus.FILLED,
            time=datetime.now(UTC),
        )

        with pytest.raises(ValueError, match="Failed to map internal symbol"):
            # Correct call signature for _compensate_position
            await execution_handler._compensate_position(
                client=mock_api,
                exchange_id="failing_exchange",
                internal_symbol="BTC",
                quantity=sized_opportunity.long_size,
                original_order=mock_original_order,
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

        long_order = Order(
            symbol="BTC-PERP",
            id="hl_order_filled",
            client_order_id="long_cid_success",
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=sized_opportunity.long_size,
            price=None,
            status=OrderStatus.FILLED,
            filled_quantity=sized_opportunity.long_size,
            time=datetime.now(UTC),
        )
        short_order = Order(
            symbol="BTC_USDC",
            id="bp_order_filled",
            client_order_id="short_cid_success",
            side=OrderSide.SELL,
            type=OrderType.MARKET,
            quantity=sized_opportunity.short_size,
            price=None,
            status=OrderStatus.FILLED,
            filled_quantity=sized_opportunity.short_size,
            time=datetime.now(UTC),
        )

        async def place_retry_side_effect(client: ExchangeAPI) -> Order:
            if client.exchange_name == "hyperliquid":  # Corrected: Use exchange_name
                return long_order
            elif client.exchange_name == "backpack":  # Corrected: Use exchange_name
                return short_order
            raise ValueError(
                f"Unexpected client: {client.exchange_name}"
            )  # Corrected: Use exchange_name

        async def get_status_side_effect(client: ExchangeAPI, order_id: str) -> Order | None:
            if (
                client.exchange_name == "hyperliquid" and order_id == "hl_order_filled"
            ):  # Corrected: Use exchange_name
                return long_order
            elif (
                client.exchange_name == "backpack" and order_id == "bp_order_filled"
            ):  # Corrected: Use exchange_name
                return short_order
            return None

        with (
            patch.object(
                execution_handler, "_place_order_with_retry", side_effect=place_retry_side_effect
            ) as mock_place_retry,
            patch.object(
                execution_handler, "_get_order_status", side_effect=get_status_side_effect
            ) as mock_get_status,
        ):
            execution = await execution_handler.execute_opportunity(sized_opportunity)

            assert execution is not None
            assert execution.status == ExecutionStatus.COMPLETED
            assert execution.error_message is None
            assert execution.long_order_id == "hl_order_filled"
            assert execution.short_order_id == "bp_order_filled"
            assert execution.long_order_response is not None
            assert execution.long_order_response["id"] == "hl_order_filled"
            assert execution.short_order_response is not None
            assert execution.short_order_response["id"] == "bp_order_filled"

            assert mock_place_retry.call_count == 2
            mock_place_retry.assert_any_call(
                exchange_id="hyperliquid",
                client=mock_hl_api,
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,  # Use 'order_type'
                quantity=sized_opportunity.long_size,
                time_in_force=TimeInForce.IOC,
                price=None,
                client_order_id=None,
            )
            mock_place_retry.assert_any_call(
                exchange_id="backpack",
                client=mock_bp_api,
                symbol="BTC_USDC",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,  # Use 'order_type'
                quantity=sized_opportunity.short_size,
                time_in_force=TimeInForce.IOC,
                price=None,
                client_order_id=None,
            )

            assert mock_get_status.call_count == 2
            mock_get_status.assert_any_call(
                exchange_id="hyperliquid",
                client=mock_hl_api,
                order_id="hl_order_filled",
                symbol="BTC-PERP",
                client_order_id=None,
            )
            mock_get_status.assert_any_call(
                exchange_id="backpack",
                client=mock_bp_api,
                order_id="bp_order_filled",
                symbol="BTC_USDC",
                client_order_id=None,
            )

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

        def mock_get_position(exchange_id: str, internal_symbol: str) -> Position | None:
            if exchange_id == "hyperliquid" and internal_symbol == "BTC":
                return Position(
                    symbol="BTC",
                    side=OrderSide.BUY,
                    size=sized_opportunity.long_size,
                    entry_price=Decimal("41000"),
                    timestamp=1,
                )
            return None

        mock_portfolio_tracker.get_position = MagicMock(side_effect=mock_get_position)

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

        long_order = Order(
            symbol="BTC-PERP",
            id="hl_order_filled",
            client_order_id="long_cid_comp",
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=sized_opportunity.long_size,
            price=None,
            status=OrderStatus.FILLED,
            filled_quantity=sized_opportunity.long_size,
            time=datetime.now(UTC),
        )
        short_order_failure = APIError(
            code=APIErrorCode.ORDER_REJECTED,
            message="Insufficient funds",
            exchange_code="BPX-1001",
        )
        comp_order = Order(
            symbol="BTC-PERP",
            id="hl_comp_1",
            client_order_id="comp_cid_1",
            side=OrderSide.SELL,
            type=OrderType.LIMIT,
            quantity=sized_opportunity.long_size,
            price=Decimal("40990") * (Decimal("1") - Decimal("0.05")),
            status=OrderStatus.FILLED,
            filled_quantity=sized_opportunity.long_size,
            time=datetime.now(UTC),
        )

        call_count = 0
        compensation_triggered = False

        async def place_retry_side_effect(client: ExchangeAPI, side: OrderSide) -> Order:
            nonlocal call_count, compensation_triggered
            call_count += 1
            if call_count == 1:
                if client.exchange_name == "hyperliquid":  # Corrected: Use exchange_name
                    return long_order
                elif client.exchange_name == "backpack":  # Corrected: Use exchange_name
                    raise short_order_failure
                else:
                    raise ValueError("Unexpected exchange in place_retry_side_effect call 1")
            elif call_count == 2:
                if (
                    client.exchange_name == "hyperliquid" and side == OrderSide.SELL
                ):  # Corrected: Use exchange_name
                    compensation_triggered = True
                    return comp_order
                else:
                    raise ValueError("Unexpected call during compensation")
            else:
                raise RuntimeError("Should not be called more than twice")

        async def get_status_side_effect(client: ExchangeAPI, order_id: str) -> Order | None:
            if client.exchange_name == "hyperliquid":  # Corrected: Use exchange_name
                if order_id == "hl_order_filled":
                    return long_order
                elif order_id == "hl_comp_1":
                    return comp_order
            return None

        # Use the 'long_order' object as it represents the filled order for compensation
        mock_original_order_for_comp = long_order

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
        ):
            execution = await execution_handler.execute_opportunity(sized_opportunity)

            assert execution is not None
            assert execution.status == ExecutionStatus.COMPLETED
            assert execution.error_message is None
            assert execution.long_order_id == "hl_order_filled"
            assert execution.short_order_id is None
            assert compensation_triggered is True

            assert mock_place_retry.call_count == 2
            assert mock_get_status.call_count >= 1
            mock_compensate.assert_called_once_with(
                client=mock_hl_api,
                exchange_id="hyperliquid",
                internal_symbol="BTC",
                quantity=sized_opportunity.long_size,
                original_order=mock_original_order_for_comp,
            )

            mock_portfolio_tracker.update_order.assert_any_call(long_order)
            mock_portfolio_tracker.update_order.assert_any_call(comp_order)

    def test_get_execution_history(self, execution_handler: ExecutionHandler) -> None:
        """Test retrieving execution history."""
        exec1 = MagicMock(spec=TradeExecution)
        exec2 = MagicMock(spec=TradeExecution)
        execution_handler.executions.append(exec1)
        execution_handler.executions.append(exec2)
        history = execution_handler.get_execution_history()
        assert len(history) == 2
        assert exec1 in history
        assert exec2 in history
        assert history is not execution_handler.executions

    def test_get_active_executions(self, execution_handler: ExecutionHandler) -> None:
        """Test retrieving active executions."""
        exec1 = MagicMock(spec=TradeExecution, status=ExecutionStatus.EXECUTING)
        exec2 = MagicMock(spec=TradeExecution, status=ExecutionStatus.PENDING)
        exec3 = MagicMock(spec=TradeExecution, status=ExecutionStatus.COMPLETED)
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
