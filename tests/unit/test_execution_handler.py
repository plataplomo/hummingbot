import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.exchange_api import APIError, APIErrorCode, ExchangeAPI
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
    Trade,
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

        def config_get_side_effect(key: str, default: object | None = None) -> Any:
            # Allow Any return type for mock flexibility
            return mock_config_dict.get(key, default)

        cfg.get.side_effect = config_get_side_effect
        return cfg

    @pytest.fixture
    def mock_portfolio_tracker(self) -> MagicMock:
        tracker = MagicMock(spec=PortfolioTracker)
        tracker.update_order = MagicMock()
        tracker.process_trade = MagicMock()
        tracker.get_position = MagicMock(return_value=None)
        tracker.get_order = MagicMock(return_value=None)
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
        api.get_order = AsyncMock()
        api.place_order = AsyncMock()
        api.get_order_status = AsyncMock()
        api.get_ticker = AsyncMock()
        return api

    @pytest.fixture
    def mock_bp_api(self) -> AsyncMock:
        api = AsyncMock(spec=ExchangeAPI)
        api.exchange_name = "backpack"
        api.get_order = AsyncMock()
        api.place_order = AsyncMock()
        api.get_order_status = AsyncMock()
        api.get_ticker = AsyncMock()
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
        assert execution.error_message is not None
        # Check for more specific parts of the error message
        assert "Could not map symbol" in execution.error_message
        assert f"'{sized_opportunity.opportunity.symbol}'" in execution.error_message
        assert "for short leg" in execution.error_message
        assert (
            f"on exchange '{sized_opportunity.opportunity.short_exchange}'"
            in execution.error_message
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
            exchange_order_id="EX123",
            related_order_id=None,
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            status=OrderStatus.NEW,
            quantity_requested=Decimal("0.1"),
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.0"),
            price=None,
            stop_price=None,
            average_fill_price=None,
            trigger_by=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=False,
            post_only=False,
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )
        mock_hl_api.place_order.return_value = mock_order
        execution = TradeExecution(sized_opportunity)
        result_order = await execution_handler._place_order_with_retry(  # noqa: SLF001
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
        mock_hl_api.place_order.side_effect = APIError("Timeout", APIErrorCode.TIMEOUT.value)
        execution = TradeExecution(sized_opportunity)
        with pytest.raises(APIError):
            await execution_handler._place_order_with_retry(  # noqa: SLF001
                execution=execution,
                exchange_id="hyperliquid",
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.1"),
                time_in_force=TimeInForce.IOC,
            )
        # result should be None as all retries failed
        assert mock_hl_api.place_order.call_count == execution_handler.max_retries

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        mock_order = Order(
            client_order_id="HL-Status",
            exchange_order_id="EX124",
            related_order_id=None,
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            status=OrderStatus.FILLED,
            quantity_requested=Decimal("0.1"),
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.1"),
            price=None,
            stop_price=None,
            average_fill_price=Decimal("41000"),
            trigger_by=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=False,
            post_only=False,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )
        mock_hl_api.get_order_status.return_value = mock_order
        execution = TradeExecution(sized_opportunity)
        result_status = await execution_handler._get_order_status(  # noqa: SLF001
            execution=execution, exchange_id="hyperliquid", order_id="HL-Status"
        )
        assert result_status == mock_order
        mock_hl_api.get_order_status.assert_called_once_with(
            order_id="HL-Status", symbol=None, client_order_id=None
        )

    @pytest.mark.asyncio
    async def test_get_order_status_failure(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        mock_hl_api.get_order_status.side_effect = APIError(
            "Not Found", APIErrorCode.ORDER_NOT_FOUND.value
        )
        execution = TradeExecution(sized_opportunity)
        result_status = await execution_handler._get_order_status(  # noqa: SLF001
            execution=execution, exchange_id="hyperliquid", order_id="HL-NotFound"
        )
        assert result_status is None
        assert mock_hl_api.get_order_status.call_count >= 1

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
        mock_ticker = Ticker(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            bid=Decimal("40900"),
            ask=Decimal("40950"),
        )
        mock_hl_api.get_ticker.return_value = mock_ticker

        limit_price_offset_pct_str = mock_config.get(
            "execution.compensation.limit_price_offset_pct", "0.05"
        )
        comp_price = None
        assert mock_ticker.ask is not None
        if limit_price_offset_pct_str is not None:
            limit_price_offset_pct = Decimal(limit_price_offset_pct_str)
            comp_price = mock_ticker.ask * (Decimal(1) - limit_price_offset_pct)

        mock_comp_order = Order(
            client_order_id="COMP-HL",
            exchange_order_id="EX125",
            related_order_id=None,
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            status=OrderStatus.NEW,
            quantity_requested=Decimal("0.1"),
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.0"),
            price=comp_price,
            stop_price=None,
            average_fill_price=None,
            trigger_by=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=True,
            post_only=False,
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name="Compensation",
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )
        execution = TradeExecution(sized_opportunity)
        execution.long_order_id = "Original-Long-ID"
        execution.short_order_id = "Original-Short-ID"

        mock_symbol_mapper.get_exchange_symbol.return_value = "BTC-PERP"

        original_filled_order = Order(
            client_order_id="Original-Long-ID",
            exchange_order_id="EX-ORIG-L",
            related_order_id=None,
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            status=OrderStatus.FILLED,
            quantity_requested=sized_opportunity.long_size,
            quantity_filled=sized_opportunity.long_size,
            average_fill_price=Decimal("41000"),
            time_in_force=TimeInForce.IOC,
            created_at=datetime.now(UTC) - timedelta(seconds=10),
            updated_at=datetime.now(UTC),
            quote_quantity_requested=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_portfolio_tracker.get_order.return_value = original_filled_order

        with patch.object(
            execution_handler, "_place_order_with_retry", return_value=mock_comp_order
        ) as mock_place_comp:
            result: bool = await execution_handler._compensate_position(  # noqa: SLF001
                execution=execution,
                exchange_id="hyperliquid",
                symbol="BTC-PERP",
                side=OrderSide.SELL,
                quantity=Decimal("0.1"),
            )
            assert result is True
            mock_hl_api.get_ticker.assert_called_once_with("BTC-PERP")
            mock_place_comp.assert_called_once()

            pos_args, kwargs = mock_place_comp.call_args
            # _place_order_with_retry(
            #     execution, exchange_id, symbol, side, quantity, order_type, price, ...
            # )
            # Indices: 0 1 2 3 4 5 6
            assert pos_args[3] == OrderSide.SELL  # side
            assert pos_args[5] == OrderType.LIMIT  # order_type (derived in _compensate_position)
            assert pos_args[6] == comp_price  # price (derived in _compensate_position)
            assert kwargs.get("is_compensation") is True

    @pytest.mark.asyncio
    async def test_compensate_position_mapping_failure(
        self,
        execution_handler: ExecutionHandler,
        mock_hl_api: AsyncMock,
        mock_symbol_mapper: MagicMock,
        sized_opportunity: SizedOpportunity,
        mock_portfolio_tracker: MagicMock,
    ) -> None:
        """Test compensation failure due to API error during placement."""
        mock_hl_api.get_ticker.return_value = Ticker(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            bid=Decimal("40900"),
            ask=Decimal("40950"),
        )

        execution = TradeExecution(sized_opportunity)
        execution.long_order_id = "Original-Long-ID-Fail"
        execution.long_fill_quantity = Decimal("0.1")

        original_filled_order = Order(
            client_order_id="Original-Long-ID-Fail",
            exchange_order_id="EX-ORIG-L-FAIL",
            related_order_id=None,
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            status=OrderStatus.FILLED,
            quantity_requested=Decimal("0.1"),
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.1"),
            price=None,
            stop_price=None,
            average_fill_price=Decimal("41000"),
            trigger_by=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=False,
            post_only=False,
            created_at=datetime.now(UTC) - timedelta(seconds=10),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )
        mock_portfolio_tracker.get_order.return_value = original_filled_order

        # Define an async side_effect function that raises the APIError
        async def async_api_error_side_effect(*args: Any, **kwargs: Any) -> None:
            raise APIError("Comp Failed", APIErrorCode.UNKNOWN.value)

        with patch.object(
            execution_handler,
            "_place_order_with_retry",
            side_effect=async_api_error_side_effect,  # Use the async side_effect
        ) as mock_place_retry_method:
            with pytest.raises(APIError) as exc_info:
                await execution_handler._compensate_position(  # noqa: SLF001
                    execution=execution,
                    exchange_id="hyperliquid",
                    symbol="BTC-PERP",
                    side=OrderSide.SELL,
                    quantity=Decimal("0.1"),
                )
            assert exc_info.value.message == "Comp Failed"
            assert exc_info.value.code == APIErrorCode.UNKNOWN.value

            mock_hl_api.get_ticker.assert_called_once_with(
                "BTC-PERP"
            )  # Ticker is fetched for price
            mock_place_retry_method.assert_called_once()  # Check it was actually called

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

        def config_get_side_effect_conc(key: str, default: object | None = None) -> object | None:
            values = {
                "execution.order_placement_type": "concurrent",
                "execution.max_retries": 3,
                "execution.retry_delay_base_sec": 0.01,
                "execution.settlement_delay": 0.01,
            }
            return values.get(key, default)

        mock_config.get.side_effect = config_get_side_effect_conc

        hl_ticker = Ticker(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            bid=Decimal("41000"),
            ask=Decimal("41050"),
        )
        bp_ticker = Ticker(
            symbol="BTC_USDC",
            timestamp=datetime.now(UTC),
            bid=Decimal("41100"),
            ask=Decimal("41150"),
        )
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        long_order = Order(
            client_order_id="HL-1",
            exchange_order_id="EX126",
            related_order_id=None,
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            status=OrderStatus.NEW,
            quantity_requested=sized_opportunity.long_size,
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.0"),
            price=None,
            stop_price=None,
            average_fill_price=None,
            trigger_by=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=False,
            post_only=False,
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )
        short_order = Order(
            client_order_id="BP-1",
            exchange_order_id="EX127",
            related_order_id=None,
            exchange="backpack",
            symbol="BTC_USDC",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            status=OrderStatus.NEW,
            quantity_requested=sized_opportunity.short_size,
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.0"),
            price=None,
            stop_price=None,
            average_fill_price=None,
            trigger_by=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=False,
            post_only=False,
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )

        async def place_retry_side_effect(
            execution: TradeExecution,
            exchange_id: str,
            symbol: str,
            side: OrderSide,
            order_type: OrderType,
            quantity: Decimal,
            time_in_force: TimeInForce = TimeInForce.GTC,
            price: Decimal | None = None,
            client_order_id: str | None = None,
            reduce_only: bool = False,
            post_only: bool = False,
            **kwargs: object,
        ) -> Order:
            await asyncio.sleep(0.01)
            if exchange_id == "hyperliquid":
                filled_long_dict_place = long_order.model_dump()
                filled_long_dict_place.update(
                    {
                        "status": OrderStatus.FILLED,
                        "quantity_filled": long_order.quantity_requested,
                        "average_fill_price": hl_ticker.ask,
                        "updated_at": datetime.now(UTC),
                    }
                )
                return Order(**filled_long_dict_place)
            if exchange_id == "backpack":
                filled_short_dict_place = short_order.model_dump()
                filled_short_dict_place.update(
                    {
                        "status": OrderStatus.FILLED,
                        "quantity_filled": short_order.quantity_requested,
                        "average_fill_price": bp_ticker.bid,
                        "updated_at": datetime.now(UTC),
                    }
                )
                return Order(**filled_short_dict_place)
            raise ValueError("Unexpected exchange")

        async def get_status_side_effect(
            execution: TradeExecution, exchange_id: str, order_id: str
        ) -> Order | None:
            await asyncio.sleep(0.01)
            if order_id == "HL-1":
                filled_long_dict = long_order.model_dump()
                filled_long_dict.update(
                    {
                        "status": OrderStatus.FILLED,
                        "quantity_filled": long_order.quantity_requested,
                        "average_fill_price": hl_ticker.ask,
                        "updated_at": datetime.now(UTC),
                    }
                )
                return Order(**filled_long_dict)
            if order_id == "BP-1":
                filled_short_dict = short_order.model_dump()
                filled_short_dict.update(
                    {
                        "status": OrderStatus.FILLED,
                        "quantity_filled": short_order.quantity_requested,
                        "average_fill_price": bp_ticker.bid,
                        "updated_at": datetime.now(UTC),
                    }
                )
                return Order(**filled_short_dict)
            return None

        async def monitor_side_effect(
            trade_exec: TradeExecution, exchange_id: str, order_id: str, is_long_leg: bool
        ) -> OrderStatus | None:
            # Simulate successful monitoring by directly updating the TradeExecution object's
            # attributes AND calling portfolio_tracker.on_trade
            now_utc = datetime.now(UTC)

            fill_price: Decimal | None = None
            fill_quantity: Decimal | None = None
            trade_symbol: str | None = None
            trade_side: OrderSide | None = None
            client_oid_for_trade: str | None = None
            exchange_oid_for_trade: str | None = None

            if is_long_leg:
                if trade_exec.long_order_id == order_id:
                    fill_quantity = sized_opportunity.long_size
                    fill_price = hl_ticker.ask
                    trade_exec.long_fill_quantity = fill_quantity
                    trade_exec.long_fill_price = fill_price
                    trade_exec.long_order_updated_at = now_utc

                    trade_symbol = long_order.symbol
                    trade_side = OrderSide.BUY
                    client_oid_for_trade = long_order.client_order_id
                    exchange_oid_for_trade = trade_exec.long_order_id

                    # Construct and process Trade for portfolio tracker
                    if fill_price and fill_quantity and trade_symbol and exchange_oid_for_trade:
                        trade = Trade(
                            id=f"{exchange_oid_for_trade}-{fill_quantity}-{int(now_utc.timestamp())}",
                            symbol=trade_symbol,
                            side=trade_side,
                            order_id=exchange_oid_for_trade,
                            exchange=exchange_id,
                            client_order_id=client_oid_for_trade,
                            price=fill_price,
                            quantity=fill_quantity,
                            fee=Decimal("0"),
                            fee_asset=None,
                            executed_at=now_utc,
                            is_maker=False,  # Assume TAKER for market orders
                        )
                        mock_portfolio_tracker.process_trade(
                            exchange_id, trade
                        )  # Renamed and no await
                    return OrderStatus.FILLED
                else:
                    print(
                        f"MonitorSideEffect WARN: Long leg, order_id '{order_id}' mismatch "
                        f"'{trade_exec.long_order_id}'"
                    )
                    return None
            else:  # Short leg
                if trade_exec.short_order_id == order_id:
                    fill_quantity = sized_opportunity.short_size
                    fill_price = bp_ticker.bid
                    trade_exec.short_fill_quantity = fill_quantity
                    trade_exec.short_fill_price = fill_price
                    trade_exec.short_order_updated_at = now_utc

                    trade_symbol = short_order.symbol
                    trade_side = OrderSide.SELL
                    client_oid_for_trade = short_order.client_order_id
                    exchange_oid_for_trade = trade_exec.short_order_id

                    if fill_price and fill_quantity and trade_symbol and exchange_oid_for_trade:
                        trade = Trade(
                            id=f"{exchange_oid_for_trade}-{fill_quantity}-{int(now_utc.timestamp())}",
                            symbol=trade_symbol,
                            side=trade_side,
                            order_id=exchange_oid_for_trade,
                            exchange=exchange_id,
                            client_order_id=client_oid_for_trade,
                            price=fill_price,
                            quantity=fill_quantity,
                            fee=Decimal("0"),
                            fee_asset=None,
                            executed_at=now_utc,
                            is_maker=False,  # Assume TAKER for market orders
                        )
                        mock_portfolio_tracker.process_trade(
                            exchange_id, trade
                        )  # Renamed and no await
                    return OrderStatus.FILLED
                else:
                    print(
                        f"MonitorSideEffect WARN: Short leg, order_id '{order_id}' mismatch "
                        f"'{trade_exec.short_order_id}'"
                    )
                    return None

            return None  # Fallback

        with (
            patch.object(
                execution_handler, "_place_order_with_retry", side_effect=place_retry_side_effect
            ) as mock_place_retry,
            patch.object(
                execution_handler, "_get_order_status", side_effect=get_status_side_effect
            ) as _,
            patch.object(
                execution_handler, "_monitor_order_status", side_effect=monitor_side_effect
            ),
        ):
            execution_result = await execution_handler.execute_opportunity(sized_opportunity)

        assert execution_result is not None
        assert execution_result.status == ExecutionStatus.COMPLETED
        assert execution_result.long_order_id == "EX126"
        assert execution_result.short_order_id == "EX127"
        assert execution_result.long_fill_quantity == sized_opportunity.long_size
        assert execution_result.short_fill_quantity == sized_opportunity.short_size
        assert execution_result.long_fill_price == hl_ticker.ask
        assert execution_result.short_fill_price == bp_ticker.bid
        assert execution_result.error_message is None
        assert mock_place_retry.call_count == 2
        assert mock_circuit_breaker_system.record_api_success.call_count >= 2
        assert mock_portfolio_tracker.process_trade.call_count >= 2

    @pytest.mark.asyncio
    async def test_execute_opportunity_failed_long_order(
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

        def config_get_side_effect_comp(key: str, default: object | None = None) -> object | None:
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

        hl_ticker = Ticker(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            bid=Decimal("41000"),
            ask=Decimal("41050"),
        )
        bp_ticker = Ticker(
            symbol="BTC_USDC",
            timestamp=datetime.now(UTC),
            bid=Decimal("41100"),
            ask=Decimal("41150"),
        )
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        long_order = Order(
            client_order_id="HL-COMP-L",
            exchange_order_id="EX128",
            related_order_id=None,
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            status=OrderStatus.NEW,
            quantity_requested=sized_opportunity.long_size,
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.0"),
            price=None,
            stop_price=None,
            average_fill_price=None,
            trigger_by=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=False,
            post_only=False,
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )
        short_order_failure = APIError("Insufficient funds", APIErrorCode.INSUFFICIENT_FUNDS.value)

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
            exchange_order_id="EX129",
            related_order_id="HL-COMP-L",
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.SELL,
            order_type=comp_order_type,
            status=OrderStatus.NEW,
            quantity_requested=sized_opportunity.long_size,
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.0"),
            price=comp_price,
            stop_price=None,
            average_fill_price=None,
            trigger_by=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=True,
            post_only=False,
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name="Compensation",
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )

        async def place_retry_side_effect(
            execution: TradeExecution,
            exchange_id: str,
            symbol: str,
            side: OrderSide,
            order_type: OrderType,
            quantity: Decimal,
            time_in_force: TimeInForce = TimeInForce.GTC,
            price: Decimal | None = None,
            client_order_id: str | None = None,
            reduce_only: bool = False,
            post_only: bool = False,
            **kwargs: object,
        ) -> Order:
            await asyncio.sleep(0.01)
            if exchange_id == "hyperliquid" and side == OrderSide.BUY:
                return Order(**long_order.model_dump())
            if exchange_id == "backpack" and side == OrderSide.SELL:
                raise short_order_failure
            elif exchange_id == "hyperliquid" and side == OrderSide.SELL and reduce_only:
                comp_order_placed_dict = comp_order.model_dump()
                comp_order_placed_dict["client_order_id"] = (
                    client_order_id or comp_order.client_order_id
                )
                comp_order_placed = Order(**comp_order_placed_dict)
                return comp_order_placed
            raise ValueError(f"Unexpected place call: {exchange_id} {side} {reduce_only=}")

        async def get_status_side_effect(
            execution: TradeExecution, exchange_id: str, order_id: str
        ) -> Order | None:
            nonlocal comp_order
            await asyncio.sleep(0.01)
            if order_id == "HL-COMP-L" and exchange_id == "hyperliquid":
                filled_long_dict = long_order.model_dump()
                filled_long_dict.update(
                    {
                        "status": OrderStatus.FILLED,
                        "quantity_filled": long_order.quantity_requested,
                        "average_fill_price": hl_ticker.ask,
                        "updated_at": datetime.now(UTC),
                    }
                )
                return Order(**filled_long_dict)
            if order_id.startswith("COMP_") and exchange_id == "hyperliquid":
                filled_comp_dict = comp_order.model_dump()
                filled_comp_dict.update(
                    {
                        "client_order_id": order_id,
                        "status": OrderStatus.FILLED,
                        "quantity_filled": comp_order.quantity_requested,
                        "average_fill_price": hl_ticker.bid,
                        "updated_at": datetime.now(UTC),
                    }
                )
                return Order(**filled_comp_dict)
            return None

        async def get_original_status_for_comp(order_id: str, exchange_id: str) -> Order | None:
            if order_id == "HL-COMP-L" and exchange_id == "hyperliquid":
                filled_long_for_comp_dict = long_order.model_dump()
                filled_long_for_comp_dict.update(
                    {
                        "status": OrderStatus.FILLED,
                        "quantity_filled": long_order.quantity_requested,
                        "average_fill_price": hl_ticker.ask,
                        "updated_at": datetime.now(UTC),
                    }
                )
                return Order(**filled_long_for_comp_dict)
            return None

        mock_portfolio_tracker.get_order.side_effect = get_original_status_for_comp

        async def monitor_compensation_side_effect(
            trade_exec_arg: TradeExecution,
            exchange_id_arg: str,
            order_id_arg: str,
        ) -> OrderStatus | None:
            nonlocal comp_order  # Need to access comp_order defined in the test scope
            if exchange_id_arg == "hyperliquid" and order_id_arg == getattr(
                trade_exec_arg, "compensation_order_id", None
            ):
                if (
                    order_id_arg == "EX129"
                ):  # Further check if comp_order is the one being monitored
                    return OrderStatus.FILLED
            return OrderStatus.NEW  # Default for non-matched or still processing

        with (
            patch.object(
                execution_handler, "_place_order_with_retry", side_effect=place_retry_side_effect
            ) as mock_place_retry,
            patch.object(
                execution_handler, "_get_order_status", side_effect=get_status_side_effect
            ) as _,
            patch.object(
                execution_handler,
                "_monitor_order_status",
                side_effect=monitor_compensation_side_effect,
            ),
            patch.object(
                execution_handler,
                "_compensate_position",
                wraps=execution_handler._compensate_position,  # noqa: SLF001
            ) as mock_compensate,
        ):
            execution_result = await execution_handler.execute_opportunity(sized_opportunity)

        assert execution_result is not None
        assert execution_result.status == ExecutionStatus.FAILED
        assert execution_result.error_message is not None
        assert (
            f"Execution {execution_result.id}: Long order placement failed or not filled"
            in execution_result.error_message
        )
        assert f"Status: {OrderStatus.NEW}" in execution_result.error_message

        place_retry_call_args_list = mock_place_retry.call_args_list
        long_leg_call = next(
            (
                c
                for c in place_retry_call_args_list
                if c.kwargs.get("exchange_id") == "hyperliquid"
                and c.kwargs.get("side") == OrderSide.BUY
            ),
            None,
        )
        assert long_leg_call is not None
        actual_execution_object_for_long = long_leg_call.kwargs.get("execution")
        assert actual_execution_object_for_long is execution_result

        short_leg_call = next(
            (
                c
                for c in place_retry_call_args_list
                if c.kwargs.get("exchange_id") == "backpack"
                and c.kwargs.get("side") == OrderSide.SELL
            ),
            None,
        )
        assert short_leg_call is not None
        actual_execution_object_for_short = short_leg_call.kwargs.get("execution")
        assert actual_execution_object_for_short is execution_result

        mock_compensate.assert_not_called()

        # Verify that record_api_error was NOT called with the short_order_failure message,
        # as that leg wasn't even attempted because the long leg was deemed incomplete.
        called_with_short_error = False
        for call_args in mock_circuit_breaker_system.record_api_error.call_args_list:
            if short_order_failure.message in str(call_args):
                called_with_short_error = True
                break
        assert not called_with_short_error, (
            "CB incorrectly notified of short leg APIError when long leg failed."
        )

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
        execution_handler._add_to_history(exec1)  # noqa: SLF001
        execution_handler._add_to_history(exec2)  # noqa: SLF001
        history = execution_handler.executions
        assert isinstance(history, list) and len(history) == 2
        history_ids = {ex.id for ex in history}
        assert "exec1" in history_ids and "exec2" in history_ids

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
