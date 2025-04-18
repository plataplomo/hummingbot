import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any  # Removed Coroutine
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
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Ticker,
    TimeInForce,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config
from cyberdelta.validation.funding_data import ArbitrageOpportunity


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
        expected_profit=Decimal("50.0"),
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
        assert trade_execution.status == ExecutionStatus.PENDING
        assert trade_execution.realized_pnl is None

    def test_to_dict(self, trade_execution: TradeExecution) -> None:
        trade_execution.status = ExecutionStatus.EXECUTING
        trade_execution.long_order_id = "order123"
        trade_execution.start_time = datetime.now(UTC)
        trade_execution.realized_pnl = Decimal("10.50")
        execution_dict = trade_execution.to_dict()
        assert execution_dict["status"] == "EXECUTING"
        assert execution_dict["realized_pnl"] == "10.50"

    def test_str_representation(self, trade_execution: TradeExecution) -> None:
        string_rep = str(trade_execution)
        assert "TradeExecution" in string_rep
        assert "PENDING" in string_rep


class TestExecutionHandler:
    """Test suite for ExecutionHandler component."""

    @pytest.fixture
    def mock_config_dict(self) -> dict[str, Any]:
        """Provides a dictionary for simple config mocking."""
        return {
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
        }

    @pytest.fixture
    def mock_config(self, mock_config_dict: dict[str, Any]) -> MagicMock:
        """Provides a mock Config object using the dictionary."""
        cfg = MagicMock(spec=Config)

        def config_get_side_effect(key: str, default: Any = None) -> Any:
            return mock_config_dict.get(key, default)

        cfg.get.side_effect = config_get_side_effect
        return cfg

    @pytest.fixture
    def mock_portfolio_tracker(self) -> MagicMock:
        tracker = MagicMock(spec=PortfolioTracker)
        tracker.update_order = MagicMock()
        tracker.process_trade = MagicMock()
        tracker.get_position = MagicMock(return_value=None)
        return tracker

    @pytest.fixture
    def mock_symbol_mapper(self) -> MagicMock:
        mapper = MagicMock(spec=SymbolMapper)

        def get_exchange_symbol_side_effect(internal_symbol: str, ex_id: str) -> str | None:
            mapping = {
                ("BTC", "hyperliquid"): "BTC-PERP",
                ("BTC", "backpack"): "BTC_USDC",
                ("ETH", "hyperliquid"): "ETH-PERP",
                ("INVALID_SYMBOL", "hyperliquid"): None,
            }
            return mapping.get((internal_symbol, ex_id))

        def get_internal_symbol_side_effect(ex_sym: str, ex_id: str) -> str | None:
            mapping = {
                ("BTC-PERP", "hyperliquid"): "BTC",
                ("BTC_USDC", "backpack"): "BTC",
                ("ETH-PERP", "hyperliquid"): "ETH",
            }
            return mapping.get((ex_sym, ex_id))

        mapper.get_exchange_symbol.side_effect = get_exchange_symbol_side_effect
        mapper.get_internal_symbol.side_effect = get_internal_symbol_side_effect
        mapper.get_all_internal_symbols = MagicMock(return_value=["BTC", "ETH"])
        return mapper

    @pytest.fixture
    def mock_circuit_breaker_system(self) -> MagicMock:
        system = MagicMock(spec=CircuitBreakerSystem)
        system.can_execute = MagicMock(return_value=(True, None))
        system.record_api_error = MagicMock()
        system.record_success = MagicMock()
        system.reset_breaker = MagicMock()
        return system

    @pytest.fixture
    def mock_hl_api(self) -> AsyncMock:
        api = AsyncMock(spec=ExchangeAPI)
        api.exchange_name = "hyperliquid"
        return api

    @pytest.fixture
    def mock_bp_api(self) -> AsyncMock:
        api = AsyncMock(spec=ExchangeAPI)
        api.exchange_name = "backpack"
        return api

    @pytest.fixture
    def execution_handler(
        self,
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        mock_symbol_mapper: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_hl_api: AsyncMock,
        mock_bp_api: AsyncMock,
    ) -> ExecutionHandler:
        handler = ExecutionHandler(
            config=mock_config,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=mock_circuit_breaker_system,
        )
        handler.register_api_client(mock_hl_api.exchange_name, mock_hl_api)
        handler.register_api_client(mock_bp_api.exchange_name, mock_bp_api)
        return handler

    @pytest.fixture
    def sized_opportunity(
        self, mock_arbitrage_opportunity: ArbitrageOpportunity
    ) -> SizedOpportunity:
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
        new_api = AsyncMock(spec=ExchangeAPI)
        new_api.exchange_name = "new_exchange"
        execution_handler.register_api_client(new_api.exchange_name, new_api)
        assert "new_exchange" in execution_handler.api_clients
        assert execution_handler.api_clients["new_exchange"] is new_api

    @pytest.mark.asyncio
    async def test_circuit_breaker_open_rejection(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_circuit_breaker_system: MagicMock,
    ) -> None:
        mock_circuit_breaker_system.can_execute.return_value = (False, "CB open")
        execution = await execution_handler.execute_opportunity(sized_opportunity)
        mock_circuit_breaker_system.can_execute.assert_called_once_with("hyperliquid")
        assert execution is not None and execution.status == ExecutionStatus.REJECTED
        assert execution.error_message is not None and "CB open" in execution.error_message

    @pytest.mark.asyncio
    async def test_execute_opportunity_mapping_failure(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_symbol_mapper: MagicMock,
        mock_hl_api: AsyncMock,
        mock_bp_api: AsyncMock,
    ) -> None:
        def get_symbol_side_effect(internal_symbol: str, ex_id: str) -> str | None:
            return "BTC-PERP" if ex_id == "hyperliquid" else None

        mock_symbol_mapper.get_exchange_symbol.side_effect = get_symbol_side_effect
        execution = await execution_handler.execute_opportunity(sized_opportunity)
        assert execution is not None and execution.status == ExecutionStatus.FAILED
        assert (
            execution.error_message is not None
            and "Failed to map symbol" in execution.error_message
        )
        mock_hl_api.place_order.assert_not_called()
        mock_bp_api.place_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_place_order_with_retry_success(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        mock_order = Order(
            client_order_id="HL-Success",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=Decimal("0.1"),
            status=OrderStatus.NEW,
        )
        mock_hl_api.place_order.return_value = mock_order
        execution = TradeExecution(sized_opportunity)
        result_order = await execution_handler._place_order_with_retry(  # noqa: SLF001  # White-box test: protected member access required for state validation; no public getter exists
            execution=execution,
            exchange_id="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.IOC,
        )
        assert result_order == mock_order
        mock_hl_api.place_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_order_with_retry_failure(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        mock_hl_api.place_order.side_effect = APIError("Timeout", APIErrorCode.TIMEOUT)
        execution = TradeExecution(sized_opportunity)
        with pytest.raises(APIError):
            await execution_handler._place_order_with_retry(  # noqa: SLF001  # White-box test: protected member access required for state validation; no public getter exists
                execution=execution,
                exchange_id="hyperliquid",
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.1"),
                time_in_force=TimeInForce.IOC,
            )
        assert mock_hl_api.place_order.call_count == execution_handler.max_retries + 1

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        mock_order = Order(
            client_order_id="HL-Status",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=Decimal("0.1"),
            status=OrderStatus.FILLED,
        )
        mock_hl_api.get_order_status.return_value = mock_order
        execution = TradeExecution(sized_opportunity)
        result_status = await execution_handler._get_order_status(  # noqa: SLF001  # White-box test: protected member access required for state validation; no public getter exists
            execution=execution, exchange_id="hyperliquid", order_id="HL-Status"
        )
        assert result_status == mock_order
        mock_hl_api.get_order_status.assert_called_once_with(order_id="HL-Status")

    @pytest.mark.asyncio
    async def test_get_order_status_failure(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        mock_hl_api.get_order_status.side_effect = APIError(
            "Not Found", APIErrorCode.ORDER_NOT_FOUND
        )
        execution = TradeExecution(sized_opportunity)
        result_status = await execution_handler._get_order_status(  # noqa: SLF001  # White-box test: protected member access required for state validation; no public getter exists
            execution=execution, exchange_id="hyperliquid", order_id="HL-NotFound"
        )
        assert result_status is None
        mock_hl_api.get_order_status.assert_called_once_with(order_id="HL-NotFound")

    @pytest.mark.asyncio
    async def test_compensate_position_success(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: AsyncMock,
        mock_portfolio_tracker: MagicMock,
        sized_opportunity: SizedOpportunity,
        mock_symbol_mapper: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test successful compensation placement."""
        mock_ticker = Ticker(symbol="BTC-PERP", bid=Decimal("40900"), ask=Decimal("40950"))
        mock_hl_api.get_ticker.return_value = mock_ticker

        limit_price_offset_pct_str = mock_config.get(
            "execution.compensation.limit_price_offset_pct", "0.05"
        )
        comp_price = None
        assert mock_ticker.ask is not None, "Ticker ask price must be available"
        if limit_price_offset_pct_str is not None:
            limit_price_offset_pct = Decimal(limit_price_offset_pct_str)
            comp_price = mock_ticker.ask * (Decimal(1) - limit_price_offset_pct)

        mock_comp_order = Order(
            client_order_id="COMP-HL",
            symbol="BTC-PERP",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.1"),
            status=OrderStatus.NEW,
            price=comp_price,
        )
        execution = TradeExecution(sized_opportunity)

        mock_symbol_mapper.get_exchange_symbol.return_value = "BTC-PERP"

        with patch.object(
            execution_handler, "_place_order_with_retry", return_value=mock_comp_order
        ) as mock_place_comp:
            result = await execution_handler._compensate_position(  # noqa: SLF001  # White-box test: protected member access required for state validation; no public getter exists
                execution=execution,
                exchange_id="hyperliquid",
                symbol="BTC-PERP",
                side=OrderSide.SELL,
                quantity=Decimal("0.1"),
            )
            assert result is True
            mock_hl_api.get_ticker.assert_called_once_with("BTC-PERP")
            mock_place_comp.assert_called_once()
            # Unpack without unused 'args'
            _, kwargs = mock_place_comp.call_args
            assert kwargs["side"] == OrderSide.SELL
            assert kwargs["order_type"] == OrderType.LIMIT
            assert kwargs["price"] == comp_price

    @pytest.mark.asyncio
    async def test_compensate_position_mapping_failure(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: AsyncMock,
        mock_symbol_mapper: MagicMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test compensation failure due to symbol mapping error."""

        # Define side effect as nested function with type hints
        def get_symbol_fail_side_effect(internal_symbol: str, ex_id: str) -> str | None:
            return None  # Simulate failure

        mock_symbol_mapper.get_exchange_symbol.side_effect = get_symbol_fail_side_effect
        execution = TradeExecution(sized_opportunity)

        # Call with the *exchange specific symbol* that _compensate_position receives
        result = await execution_handler._compensate_position(  # noqa: SLF001  # White-box test: protected member access required for state validation; no public getter exists
            execution=execution,
            exchange_id="hyperliquid",
            symbol="INVALID_SYMBOL",
            side=OrderSide.SELL,
            quantity=Decimal("0.1"),
        )
        assert result is False
        # The internal get_exchange_symbol is no longer called inside _compensate_position
        # Instead, the failure happens earlier or is handled differently.
        # We verify that place_order was not called.
        mock_hl_api.place_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_opportunity_success_concurrent(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_hl_api: AsyncMock,
        mock_bp_api: AsyncMock,
        mock_portfolio_tracker: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test successful concurrent execution of an opportunity."""

        # Define side effect with type hints
        def config_get_side_effect_conc(key: str, default: Any = None) -> Any:
            values = {
                "execution.order_placement_type": "concurrent",
                "execution.max_retries": 3,
                "execution.retry_delay_base_sec": 0.01,
                "execution.settlement_delay": 0.01,
            }
            return values.get(key, default)

        mock_config.get.side_effect = config_get_side_effect_conc

        hl_ticker = Ticker(symbol="BTC-PERP", bid=Decimal("41000"), ask=Decimal("41050"))
        bp_ticker = Ticker(symbol="BTC_USDC", bid=Decimal("41100"), ask=Decimal("41150"))
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        long_order = Order(
            client_order_id="HL-1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=sized_opportunity.long_size,
            status=OrderStatus.NEW,
        )
        short_order = Order(
            client_order_id="BP-1",
            symbol="BTC_USDC",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity_requested=sized_opportunity.short_size,
            status=OrderStatus.NEW,
        )

        # Added type hints and correct return type
        async def place_retry_side_effect(
            execution: TradeExecution,
            exchange_id: str,
            symbol: str,
            side: OrderSide,
            order_type: OrderType,
            quantity: Decimal,
            time_in_force: TimeInForce,
            price: Decimal | None = None,
            client_order_id: str | None = None,
            reduce_only: bool = False,
            post_only: bool = False,
            **kwargs: Any,
        ) -> Order:
            await asyncio.sleep(0.01)
            if exchange_id == "hyperliquid":
                return long_order
            if exchange_id == "backpack":
                return short_order
            raise ValueError("Unexpected exchange")

        # Added type hints and correct return type
        async def get_status_side_effect(
            execution: TradeExecution, exchange_id: str, order_id: str
        ) -> Order | None:
            await asyncio.sleep(0.01)
            if order_id == "HL-1":
                filled_long = Order(**long_order.to_dict())
                filled_long.status = OrderStatus.FILLED
                filled_long.quantity_filled = long_order.quantity_requested
                filled_long.average_fill_price = hl_ticker.ask
                return filled_long
            if order_id == "BP-1":
                filled_short = Order(**short_order.to_dict())
                filled_short.status = OrderStatus.FILLED
                filled_short.quantity_filled = short_order.quantity_requested
                filled_short.average_fill_price = bp_ticker.bid
                return filled_short
            return None

        with (
            patch.object(
                execution_handler, "_place_order_with_retry", side_effect=place_retry_side_effect
            ) as mock_place_retry,
            patch.object(
                execution_handler, "_get_order_status", side_effect=get_status_side_effect
            ) as mock_get_status,
            patch.object(execution_handler, "_monitor_order_status", AsyncMock(return_value=True)),
        ):
            execution_result = await execution_handler.execute_opportunity(sized_opportunity)

            assert execution_result.status == ExecutionStatus.COMPLETED
            assert execution_result.long_order_id == "HL-1"
            assert execution_result.short_order_id == "BP-1"
            assert execution_result.long_fill_price == hl_ticker.ask
            assert execution_result.short_fill_price == bp_ticker.bid
            assert execution_result.error_message is None
            assert mock_place_retry.call_count == 2
            mock_circuit_breaker_system.record_success.assert_called()
            assert mock_portfolio_tracker.process_trade.call_count >= 2

    @pytest.mark.asyncio
    async def test_execute_opportunity_compensation_needed(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_hl_api: AsyncMock,
        mock_bp_api: AsyncMock,
        mock_portfolio_tracker: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_symbol_mapper: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test execution flow when one leg fails and compensation is triggered."""

        # Define side effect with type hints
        def config_get_side_effect_comp(key: str, default: Any = None) -> Any:
            values = {
                "execution.order_placement_type": "concurrent",
                "execution.compensation.use_limit_orders": True,
                "execution.compensation.limit_price_offset_pct": "0.05",
                "execution.max_retries": 3,
                "execution.retry_delay_base_sec": 0.01,
                "execution.settlement_delay": 0.01,
            }
            return values.get(key, default)

        mock_config.get.side_effect = config_get_side_effect_comp

        mock_portfolio_tracker.get_position.return_value = None

        hl_ticker = Ticker(symbol="BTC-PERP", bid=Decimal("41000"), ask=Decimal("41050"))
        bp_ticker = Ticker(symbol="BTC_USDC", bid=Decimal("41100"), ask=Decimal("41150"))
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        long_order = Order(
            client_order_id="HL-COMP-L",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=sized_opportunity.long_size,
            status=OrderStatus.NEW,
        )
        short_order_failure = APIError("Insufficient funds", APIErrorCode.INSUFFICIENT_FUNDS)

        # Calculate comp_price with None checks
        comp_price: Decimal | None = None
        limit_price_offset_pct_str = mock_config.get(
            "execution.compensation.limit_price_offset_pct"
        )
        assert hl_ticker.ask is not None
        if limit_price_offset_pct_str is not None:
            limit_price_offset_pct = Decimal(limit_price_offset_pct_str)
            comp_price = hl_ticker.ask * (Decimal(1) - limit_price_offset_pct)
        comp_order_type = (
            OrderType.LIMIT
            if mock_config.get("execution.compensation.use_limit_orders") and comp_price is not None
            else OrderType.MARKET
        )
        comp_order = Order(
            client_order_id="HL-COMP-C",
            symbol="BTC-PERP",
            side=OrderSide.SELL,
            order_type=comp_order_type,
            price=comp_price,
            quantity_requested=sized_opportunity.long_size,
            status=OrderStatus.NEW,
        )

        # Add type hints and correct return type
        async def place_retry_side_effect(
            execution: TradeExecution,
            exchange_id: str,
            symbol: str,
            side: OrderSide,
            order_type: OrderType,
            quantity: Decimal,
            time_in_force: TimeInForce,
            price: Decimal | None = None,
            client_order_id: str | None = None,
            reduce_only: bool = False,
            post_only: bool = False,
            **kwargs: Any,
        ) -> Order:
            nonlocal comp_order
            await asyncio.sleep(0.01)
            if exchange_id == "hyperliquid" and side == OrderSide.BUY:
                return long_order
            elif exchange_id == "backpack" and side == OrderSide.SELL:
                raise short_order_failure
            elif (
                exchange_id == "hyperliquid"
                and side == OrderSide.SELL
                and client_order_id
                and client_order_id.startswith("COMP_")
            ):
                comp_order.client_order_id = (
                    client_order_id if client_order_id else comp_order.client_order_id
                )
                return comp_order
            raise ValueError(f"Unexpected place call: {exchange_id} {side}")

        # Add type hints and correct return type
        async def get_status_side_effect(
            execution: TradeExecution, exchange_id: str, order_id: str
        ) -> Order | None:
            nonlocal comp_order
            await asyncio.sleep(0.01)
            if order_id == "HL-COMP-L" and exchange_id == "hyperliquid":
                filled_long = Order(**long_order.to_dict())
                filled_long.status = OrderStatus.FILLED
                filled_long.quantity_filled = long_order.quantity_requested
                filled_long.average_fill_price = hl_ticker.ask
                return filled_long
            if order_id == comp_order.client_order_id and exchange_id == "hyperliquid":
                filled_comp = Order(**comp_order.to_dict())
                filled_comp.status = OrderStatus.FILLED
                filled_comp.quantity_filled = comp_order.quantity_requested
                filled_comp.average_fill_price = hl_ticker.bid
                return filled_comp
            return None

        # Add type hints and correct return type
        async def get_original_status_for_comp(
            execution_arg: TradeExecution, exchange_id: str, order_id: str
        ) -> Order | None:
            if order_id == "HL-COMP-L":
                filled_long_for_comp = Order(**long_order.to_dict())
                filled_long_for_comp.status = OrderStatus.FILLED
                filled_long_for_comp.quantity_filled = long_order.quantity_requested
                filled_long_for_comp.average_fill_price = hl_ticker.ask
                return filled_long_for_comp
            # Correct delegation using await
            result: Order | None = await get_status_side_effect(
                execution_arg, exchange_id, order_id
            )
            return result

        with (
            patch.object(
                execution_handler, "_place_order_with_retry", side_effect=place_retry_side_effect
            ) as mock_place_retry,
            patch.object(
                execution_handler, "_get_order_status", side_effect=get_original_status_for_comp
            ) as mock_get_status,
            patch.object(execution_handler, "_monitor_order_status", AsyncMock(return_value=True)),
            patch.object(
                execution_handler,
                "_compensate_position",
                wraps=execution_handler._compensate_position,
            ) as mock_compensate,
        ):
            execution_result = await execution_handler.execute_opportunity(sized_opportunity)

            assert execution_result.status == ExecutionStatus.FAILED
            assert (
                execution_result.error_message is not None
                and "Insufficient funds" in execution_result.error_message
            )
            assert execution_result.long_order_id == "HL-COMP-L"
            assert execution_result.short_order_id is None

            # Assert _compensate_position was called with correct args
            mock_compensate.assert_called_once()
            # Unpack carefully, avoiding unused 'args'
            _, call_kwargs = mock_compensate.call_args
            assert call_kwargs["execution"] == execution_result
            assert call_kwargs["exchange_id"] == "hyperliquid"
            assert call_kwargs["symbol"] == "BTC-PERP"
            assert call_kwargs["side"] == OrderSide.SELL
            assert call_kwargs["quantity"] == sized_opportunity.long_size

            # Verify compensation placement call via the mock
            compensation_call_found = False
            for call in mock_place_retry.call_args_list:
                # Unpack carefully, avoiding unused 'args_call'
                _, kwargs_call = call
                if (
                    kwargs_call.get("exchange_id") == "hyperliquid"
                    and kwargs_call.get("side") == OrderSide.SELL
                    and kwargs_call.get("client_order_id", "").startswith("COMP_")
                ):
                    compensation_call_found = True
                    break
            assert compensation_call_found, (
                "Compensation order placement not called via _place_order_with_retry"
            )

            mock_circuit_breaker_system.record_api_error.assert_called_with(
                "backpack", short_order_failure
            )
            mock_circuit_breaker_system.record_success.assert_not_called()

    def test_get_execution_history(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test retrieving the execution history."""
        exec1 = TradeExecution(sized_opportunity)
        exec1.id = "exec1"
        exec1.status = ExecutionStatus.COMPLETED
        exec2 = TradeExecution(sized_opportunity)
        exec2.id = "exec2"
        exec2.status = ExecutionStatus.FAILED
        execution_handler.executions = []
        execution_handler._add_to_history(exec1)
        execution_handler._add_to_history(exec2)
        history = execution_handler.executions
        assert isinstance(history, list) and len(history) == 2
        assert history[0].id == "exec1" and history[1].id == "exec2"

    def test_get_active_executions(
        self, execution_handler: ExecutionHandler, sized_opportunity: SizedOpportunity
    ) -> None:
        """Test retrieving active executions."""
        exec1 = TradeExecution(sized_opportunity)
        exec1.id = "active_exec1"
        exec1.status = ExecutionStatus.EXECUTING
        exec2 = TradeExecution(sized_opportunity)
        exec2.id = "active_exec2"
        exec2.status = ExecutionStatus.PENDING
        exec3 = TradeExecution(sized_opportunity)
        exec3.id = "completed_exec"
        exec3.status = ExecutionStatus.COMPLETED
        execution_handler.active_executions = {
            "active_exec1": exec1,
            "active_exec2": exec2,
            "completed_exec": exec3,
        }
        active = list(execution_handler.active_executions.values())
        active_filtered = [
            ex
            for ex in active
            if ex.status
            not in (ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.REJECTED)
        ]
        assert isinstance(active_filtered, list) and len(active_filtered) == 2
        active_ids = {ex.id for ex in active_filtered}
        assert "active_exec1" in active_ids and "active_exec2" in active_ids

    def test_reset_circuit_breaker(
        self, execution_handler: ExecutionHandler, mock_circuit_breaker_system: MagicMock
    ) -> None:
        """Test resetting the circuit breaker for an exchange."""
        execution_handler.reset_circuit_breaker("hyperliquid")
        mock_circuit_breaker_system.reset_breaker.assert_called_once_with("hyperliquid")
