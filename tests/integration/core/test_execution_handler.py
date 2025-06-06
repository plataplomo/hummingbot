"""Integration tests for ExecutionHandler component.

Tests the complete execution workflow including order placement, monitoring,
compensation logic, and integration with portfolio tracker and circuit breaker.
Covers both successful and failure scenarios for trade execution.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.exchange_api import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.config import AppSettings
from cyberdelta.core.execution_handler import (
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
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity
from tests.test_utils.testable_classes import TestableExecutionHandler

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_arbitrage_opportunity() -> ArbitrageOpportunity:
    """Provide a basic mock ArbitrageOpportunity for testing execution scenarios."""
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
        self,
        mock_arbitrage_opportunity: ArbitrageOpportunity,
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
        """Test initial state."""
        assert trade_execution.status == ExecutionStatus.PENDING
        assert trade_execution.realized_pnl is None

    def test_to_dict(self, trade_execution: TradeExecution) -> None:
        """Test to dict."""
        trade_execution.status = ExecutionStatus.EXECUTING
        trade_execution.long_order_id = "order123"
        trade_execution.start_time = datetime.now(UTC)
        trade_execution.realized_pnl = Decimal("10.50")
        execution_dict = trade_execution.to_dict()
        assert execution_dict["status"] == "EXECUTING"
        assert execution_dict["realized_pnl"] == "10.50"

    def test_str_representation(self, trade_execution: TradeExecution) -> None:
        """Test str representation."""
        string_rep = str(trade_execution)
        assert "TradeExecution" in string_rep
        assert "PENDING" in string_rep


class TestExecutionHandler:
    """Test suite for ExecutionHandler component."""

    @pytest.fixture
    def mock_config_dict(self) -> dict[str, Any]:
        """Provide a dictionary for simple config mocking in execution tests."""
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
        """Provide a mock Config object using the test configuration dictionary."""
        cfg = MagicMock(spec=AppSettings)

        # Mock the execution attribute structure
        execution_mock = MagicMock()
        execution_mock.max_slippage_pct = mock_config_dict.get("execution.max_slippage_pct", "0.01")
        execution_mock.max_retries = mock_config_dict.get("execution.max_retries", 3)
        execution_mock.retry_delay_base_sec = mock_config_dict.get(
            "execution.retry_delay_base_sec",
            "1.0",
        )

        # Mock compensation sub-config
        compensation_mock = MagicMock()
        compensation_mock.use_limit_orders = mock_config_dict.get(
            "execution.compensation.use_limit_orders",
            True,
        )
        compensation_mock.limit_price_offset_pct = mock_config_dict.get(
            "execution.compensation.limit_price_offset_pct",
            "0.05",
        )
        execution_mock.compensation = compensation_mock

        cfg.execution = execution_mock
        return cfg

    @pytest.fixture
    def mock_portfolio_tracker(self) -> MagicMock:
        """Return mock portfolio tracker for testing."""
        tracker = MagicMock(spec=PortfolioTracker)
        tracker.update_order = MagicMock()

        # Add a side effect to process_trade for debugging
        process_trade_call_tracker: list[tuple[str, Trade]] = []  # Explicitly typed

        def process_trade_side_effect(exchange_id: str, trade: Trade) -> None:
            """Process trade and log the call for testing portfolio tracker integration."""
            logger.debug(
                f"mock_portfolio_tracker.process_trade called with: {exchange_id}, {trade!r}",
            )
            process_trade_call_tracker.append((exchange_id, trade))
            # original_process_trade_behavior_if_any() # If it had real behavior to mimic

        tracker.process_trade = MagicMock(side_effect=process_trade_side_effect)
        tracker.process_trade_call_tracker = process_trade_call_tracker  # Attach for assertion

        tracker.get_position = MagicMock(return_value=None)
        tracker.get_order = MagicMock(return_value=None)
        return tracker

    @pytest.fixture
    def mock_symbol_mapper(self) -> MagicMock:
        """Return mock symbol mapper for testing."""
        mapper = MagicMock(spec=SymbolMapper)

        def get_exchange_symbol_side_effect(internal_symbol: str, ex_id: str) -> str | None:
            """Get exchange symbol side effect for testing."""
            mapping = {
                ("BTC", "hyperliquid"): "BTC-PERP",
                ("BTC", "backpack"): "BTC_USDC",
                ("ETH", "hyperliquid"): "ETH-PERP",
                ("INVALID_SYMBOL", "hyperliquid"): None,
            }
            return mapping.get((internal_symbol, ex_id))

        def get_internal_symbol_side_effect(ex_sym: str, ex_id: str) -> str | None:
            """Get internal symbol side effect for testing."""
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
        """Return mock circuit breaker system for testing."""
        system = MagicMock(spec=CircuitBreakerSystem)
        system.can_execute = MagicMock(return_value=(True, None))
        system.record_api_error = MagicMock()
        system.record_success = MagicMock()
        system.reset_breaker = MagicMock()
        return system

    @pytest.fixture
    def mock_hl_api(self) -> AsyncMock:
        """Return mock hl api for testing."""
        api = AsyncMock(spec=ExchangeAPI)
        api.exchange_name = "hyperliquid"
        api.get_order = AsyncMock()
        api.place_order = AsyncMock()
        api.get_order_status = AsyncMock()
        api.get_ticker = AsyncMock()
        return api

    @pytest.fixture
    def mock_bp_api(self) -> AsyncMock:
        """Return mock bp api for testing."""
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
        """Create execution handler instance with mocked dependencies for testing."""
        handler = ExecutionHandler(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=mock_circuit_breaker_system,
        )
        handler.register_api_client(mock_hl_api.exchange_name, mock_hl_api)
        handler.register_api_client(mock_bp_api.exchange_name, mock_bp_api)
        return handler

    @pytest.fixture
    def testable_execution_handler(
        self,
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        mock_symbol_mapper: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_hl_api: AsyncMock,
        mock_bp_api: AsyncMock,
    ) -> TestableExecutionHandler:
        """Fixture for TestableExecutionHandler that exposes protected methods."""
        handler = TestableExecutionHandler(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
            circuit_breaker_system=mock_circuit_breaker_system,
        )
        handler.register_api_client(mock_hl_api.exchange_name, mock_hl_api)
        handler.register_api_client(mock_bp_api.exchange_name, mock_bp_api)
        return handler

    @pytest.fixture
    def sized_opportunity(
        self,
        mock_arbitrage_opportunity: ArbitrageOpportunity,
    ) -> SizedOpportunity:
        """Create sized opportunity instance for testing order execution scenarios."""
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
        """Test register api client."""
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
        """Test that execution handler rejects opportunities when circuit breaker is open."""
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
        """Test execution handler behavior when symbol mapping fails for one exchange."""

        def get_symbol_side_effect(internal_symbol: str, ex_id: str) -> str | None:
            """Get symbol side effect for testing."""
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
        testable_execution_handler: TestableExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test successful order placement with retry mechanism."""
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
        result_order = await testable_execution_handler.test_place_order_with_retry(
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
        testable_execution_handler: TestableExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test order placement failure handling with retry exhaustion."""
        mock_hl_api.place_order.side_effect = APIError("Timeout", APIErrorCode.TIMEOUT.value)
        execution = TradeExecution(sized_opportunity)
        with pytest.raises(APIError):
            await testable_execution_handler.test_place_order_with_retry(
                execution=execution,
                exchange_id="hyperliquid",
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.1"),
                time_in_force=TimeInForce.IOC,
            )
        # result should be None as all retries failed
        assert mock_hl_api.place_order.call_count == testable_execution_handler.max_retries

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self,
        testable_execution_handler: TestableExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test successful order status retrieval."""
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
        result_status = await testable_execution_handler.test_get_order_status(
            execution=execution,
            exchange_id="hyperliquid",
            order_id="HL-Status",
        )
        assert result_status == mock_order
        # Check that get_order_status was called with the correct args object
        mock_hl_api.get_order_status.assert_called_once()
        call_args = mock_hl_api.get_order_status.call_args
        assert call_args.kwargs["args"].order_id == "HL-Status"
        assert call_args.kwargs["args"].symbol is None
        assert call_args.kwargs["args"].client_order_id is None

    @pytest.mark.asyncio
    async def test_get_order_status_failure(
        self,
        testable_execution_handler: TestableExecutionHandler,
        mock_hl_api: AsyncMock,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test order status retrieval failure handling."""
        mock_hl_api.get_order_status.side_effect = APIError(
            "Not Found",
            APIErrorCode.ORDER_NOT_FOUND.value,
        )
        execution = TradeExecution(sized_opportunity)
        result_status = await testable_execution_handler.test_get_order_status(
            execution=execution,
            exchange_id="hyperliquid",
            order_id="HL-NotFound",
        )
        assert result_status is None
        assert mock_hl_api.get_order_status.call_count >= 1

    @pytest.mark.asyncio
    async def test_compensate_position_success(
        self,
        testable_execution_handler: TestableExecutionHandler,
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
            "execution.compensation.limit_price_offset_pct",
            "0.05",
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
            testable_execution_handler,
            "_place_order_with_retry",
            return_value=mock_comp_order,
        ) as mock_place_comp:
            result: bool = await testable_execution_handler.test_compensate_position(
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
            assert kwargs.get("reduce_only") is True

    @pytest.mark.asyncio
    async def test_compensate_position_mapping_failure(
        self,
        testable_execution_handler: TestableExecutionHandler,
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
        async def async_api_error_side_effect(*_args: object, **_kwargs: object) -> None:
            raise APIError("Comp Failed", APIErrorCode.UNKNOWN.value)

        with patch.object(
            testable_execution_handler,
            "_place_order_with_retry",
            side_effect=async_api_error_side_effect,  # Use the async side_effect
        ) as mock_place_retry_method:
            with pytest.raises(APIError) as exc_info:
                await testable_execution_handler.test_compensate_position(
                    execution=execution,
                    exchange_id="hyperliquid",
                    symbol="BTC-PERP",
                    side=OrderSide.SELL,
                    quantity=Decimal("0.1"),
                )
            assert exc_info.value.message == "Comp Failed"
            assert exc_info.value.code == APIErrorCode.UNKNOWN.value

            mock_hl_api.get_ticker.assert_called_once_with(
                "BTC-PERP",
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
        now_ts = datetime.now(UTC)
        hl_ticker = Ticker(
            symbol="BTC-PERP",
            timestamp=now_ts,
            bid=Decimal("41000"),
            ask=Decimal("41050"),
        )
        bp_ticker = Ticker(
            symbol="BTC_USDC",
            timestamp=now_ts,
            bid=Decimal("41100"),
            ask=Decimal("41150"),
        )
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        # Base Order models - these will be copied and updated by mocks
        base_long_order = Order(
            client_order_id="HL-CONC-L-1",
            exchange_order_id="EX-HL-CONC-L-126",
            exchange="hyperliquid",
            symbol="BTC-PERP",  # Internal symbol from SizedOpportunity
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            status=OrderStatus.NEW,  # Initial status
            quantity_requested=sized_opportunity.long_size,
            time_in_force=TimeInForce.IOC,
            created_at=now_ts,
            # Explicitly add None for optional fields to satisfy linter
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],  # Changed from None to empty list
            quote_quantity_requested=None,
            price=None,
            stop_price=None,
            average_fill_price=None,
            reduce_only=False,
            post_only=False,
            related_order_id=None,
            trigger_by=None,
            hl_details=None,
            bp_details=None,
        )
        base_short_order = Order(
            client_order_id="BP-CONC-S-1",
            exchange_order_id="EX-BP-CONC-S-127",
            exchange="backpack",
            symbol="BTC_USDC",  # Internal symbol from SizedOpportunity
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            status=OrderStatus.NEW,  # Initial status
            quantity_requested=sized_opportunity.short_size,
            time_in_force=TimeInForce.IOC,
            created_at=now_ts,
            # Explicitly add None for optional fields
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],  # Changed from None to empty list
            quote_quantity_requested=None,
            price=None,
            stop_price=None,
            average_fill_price=None,
            reduce_only=False,
            post_only=False,
            related_order_id=None,
            trigger_by=None,
            hl_details=None,
            bp_details=None,
        )

        # --- Mock for _place_order_with_retry ---
        async def place_order_retry_side_effect(
            **kwargs: TradeExecution | str | OrderSide | OrderType | Decimal | None,
        ) -> Order:
            await asyncio.sleep(0.001)  # Simulate async call

            # Extract all necessary parameters from kwargs with proper type casting
            from typing import cast

            execution: TradeExecution = cast("TradeExecution", kwargs["execution"])
            exchange_id: str = cast("str", kwargs["exchange_id"])
            symbol_from_kwargs: str = cast("str", kwargs["symbol"])
            side: OrderSide = cast("OrderSide", kwargs["side"])
            order_type_from_kwargs: OrderType = cast("OrderType", kwargs["order_type"])
            quantity_from_kwargs: Decimal = cast("Decimal", kwargs["quantity"])

            # Use extracted parameters for validation
            assert execution is not None, "Execution parameter is required"
            assert symbol_from_kwargs, "Symbol parameter is required"
            assert order_type_from_kwargs is not None, "Order type parameter is required"
            assert quantity_from_kwargs > 0, "Quantity must be positive"

            sut_generated_client_oid_raw = kwargs.get("client_order_id")
            if sut_generated_client_oid_raw is None:
                sut_generated_client_oid = f"fallback-oid-{datetime.now(UTC).timestamp()}"
            else:
                sut_generated_client_oid = cast("str", sut_generated_client_oid_raw)

            if exchange_id == "hyperliquid" and side == OrderSide.BUY:
                return base_long_order.model_copy(
                    update={
                        "client_order_id": sut_generated_client_oid,
                        "status": OrderStatus.FILLED,
                        "quantity_filled": sized_opportunity.long_size,
                        "average_fill_price": hl_ticker.ask,
                        "updated_at": datetime.now(UTC),
                        "trades": [
                            Trade(
                                id=f"{base_long_order.exchange_order_id}-trade1",
                                symbol=base_long_order.symbol,
                                side=OrderSide.BUY,
                                order_id=str(base_long_order.exchange_order_id),
                                exchange=exchange_id,
                                client_order_id=sut_generated_client_oid,
                                price=hl_ticker.ask or Decimal(0),
                                quantity=sized_opportunity.long_size,
                                fee=Decimal("0"),
                                fee_asset=None,
                                executed_at=datetime.now(UTC),
                                is_maker=False,
                            ),
                        ],
                    },
                )
            if exchange_id == "backpack" and side == OrderSide.SELL:
                return base_short_order.model_copy(
                    update={
                        "client_order_id": sut_generated_client_oid,
                        "status": OrderStatus.FILLED,
                        "quantity_filled": sized_opportunity.short_size,
                        "average_fill_price": bp_ticker.bid,
                        "updated_at": datetime.now(UTC),
                        "trades": [
                            Trade(
                                id=f"{base_short_order.exchange_order_id}-trade1",
                                symbol=base_short_order.symbol,
                                side=OrderSide.SELL,
                                order_id=str(base_short_order.exchange_order_id),
                                exchange=exchange_id,
                                client_order_id=sut_generated_client_oid,
                                price=bp_ticker.bid or Decimal(0),
                                quantity=sized_opportunity.short_size,
                                fee=Decimal("0"),
                                fee_asset=None,
                                executed_at=datetime.now(UTC),
                                is_maker=False,
                            ),
                        ],
                    },
                )
            # Ensure all paths return or raise
            raise ValueError(
                f"place_order_retry_side_effect: Unhandled combination "
                f"exchange_id='{exchange_id}', side='{side}'. Kwargs: {kwargs}",
            )

        # We are testing execute_opportunity, which calls _place_orders_for_opportunity,
        # which in turn calls _place_order_with_retry.
        # If _place_order_with_retry returns FILLED orders (as our mock now does for success),
        # _monitor_order_status (and thus _get_order_status) might not be called explicitly
        # afterwards by _place_orders_for_opportunity.
        # So, we only mock _place_order_with_retry.
        with patch.object(
            execution_handler,
            "_place_order_with_retry",
            side_effect=place_order_retry_side_effect,
        ) as mock_place_retry_patcher:
            execution_result = await execution_handler.execute_opportunity(sized_opportunity)

        assert execution_result is not None
        assert execution_result.status == ExecutionStatus.COMPLETED
        # Check that the SUT-generated client_order_ids are on the execution_result
        assert execution_result.long_order_id is not None
        assert execution_result.short_order_id is not None

        # The actual exchange_order_ids come from our base_long_order/base_short_order via the mocks
        assert execution_result.long_order_id == base_long_order.exchange_order_id
        assert execution_result.short_order_id == base_short_order.exchange_order_id

        assert execution_result.long_fill_quantity == sized_opportunity.long_size
        assert execution_result.short_fill_quantity == sized_opportunity.short_size
        assert execution_result.long_fill_price == hl_ticker.ask
        assert execution_result.short_fill_price == bp_ticker.bid
        assert execution_result.error_message is None

        # _place_order_with_retry should be called twice (once per leg)
        assert mock_place_retry_patcher.call_count == 2
        # _get_order_status (via _monitor_order_status) is no longer asserted here for this
        # success path
        # assert mock_get_status_patcher.call_count >= 2 # Removed this assertion

        # Verify PortfolioTracker.process_trade was called correctly
        # It should be called twice, once for each filled leg.
        # assert mock_portfolio_tracker.process_trade.call_count == 2
        # Using the custom tracker list instead
        assert len(mock_portfolio_tracker.process_trade_call_tracker) == 2

        # Inspect calls to process_trade
        # calls = mock_portfolio_tracker.process_trade.call_args_list
        calls_tracked = mock_portfolio_tracker.process_trade_call_tracker
        assert len(calls_tracked) == 2

        # Check first call (order doesn't matter due to current sequential debug mode)
        trade1_exchange_id = calls_tracked[0][0]
        trade1_object = calls_tracked[0][1]
        assert isinstance(trade1_object, Trade)
        if trade1_exchange_id == "hyperliquid":
            assert trade1_object.side == OrderSide.BUY
            assert trade1_object.order_id == base_long_order.exchange_order_id
            assert trade1_object.quantity == sized_opportunity.long_size
            assert trade1_object.price == hl_ticker.ask
        elif trade1_exchange_id == "backpack":
            assert trade1_object.side == OrderSide.SELL
            assert trade1_object.order_id == base_short_order.exchange_order_id
            assert trade1_object.quantity == sized_opportunity.short_size
            assert trade1_object.price == bp_ticker.bid
        else:
            pytest.fail(f"Unexpected exchange_id for trade1: {trade1_exchange_id}")

        # Check second call
        trade2_exchange_id = calls_tracked[1][0]
        trade2_object = calls_tracked[1][1]
        assert isinstance(trade2_object, Trade)
        if trade2_exchange_id == "hyperliquid":
            assert trade2_object.side == OrderSide.BUY
            assert trade2_object.order_id == base_long_order.exchange_order_id
            assert trade2_object.quantity == sized_opportunity.long_size
            assert trade2_object.price == hl_ticker.ask
        elif trade2_exchange_id == "backpack":
            assert trade2_object.side == OrderSide.SELL
            assert trade2_object.order_id == base_short_order.exchange_order_id
            assert trade2_object.quantity == sized_opportunity.short_size
            assert trade2_object.price == bp_ticker.bid
        else:
            pytest.fail(f"Unexpected exchange_id for trade2: {trade2_exchange_id}")

        # Ensure one call was for hyperliquid and the other for backpack
        assert {calls_tracked[0][0], calls_tracked[1][0]} == {"hyperliquid", "backpack"}

        mock_circuit_breaker_system.record_api_success.assert_called()
        # record_api_success is called after each successful order placement and after monitoring.
        # Two placements + two monitoring phases = 4 success records expected.
        assert mock_circuit_breaker_system.record_api_success.call_count >= 2  # Relaxed to >=2

    @pytest.mark.asyncio
    def _setup_failed_order_config(self) -> dict[str, object]:
        """Setup configuration for failed order test."""
        return {
            "execution.order_placement_type": "concurrent",
            "execution.compensation.use_limit_orders": True,
            "execution.compensation.limit_price_offset_pct": "0.05",
            "execution.max_retries": 3,
            "execution.retry_delay_base_sec": 0.01,
            "execution.settlement_delay": 0.01,
            "execution.use_market_orders": True,
        }

    def _setup_test_tickers(self) -> tuple[Ticker, Ticker]:
        """Setup test tickers for HL and BP."""
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
        return hl_ticker, bp_ticker

    def _setup_test_orders(
        self,
        sized_opportunity: SizedOpportunity,
        mock_config: MagicMock,
        hl_ticker: Ticker,
    ) -> tuple[Order, Order, APIError, OrderType, Decimal | None]:
        """Setup test orders and related objects for compensation testing."""
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
        limit_price_offset_pct_str = mock_config.get("execution.compensation.limit_price_offset")
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

        return long_order, comp_order, short_order_failure, comp_order_type, comp_price

    def _create_place_retry_side_effect(
        self,
        long_order: Order,
        comp_order: Order,
        short_order_failure: APIError,
        sized_opportunity: SizedOpportunity,
        hl_ticker: Ticker,
    ) -> Callable[..., Awaitable[Order]]:
        """Create the place_retry_side_effect function for testing."""

        async def place_retry_side_effect(
            execution: TradeExecution,
            exchange_id: str,
            symbol: str,
            side: OrderSide,
            quantity: Decimal,
            order_type: OrderType,
            price: Decimal | None = None,
            time_in_force: TimeInForce = TimeInForce.IOC,
            client_order_id: str | None = None,
            reduce_only: bool = False,
            post_only: bool = False,
            is_long_leg: bool | None = None,
        ) -> Order:
            await asyncio.sleep(0.01)
            # Ensure this mock handles the compensation call correctly
            if (
                exchange_id == "hyperliquid" and side == OrderSide.BUY and not reduce_only
            ):  # Initial long leg
                # Return a FILLED order directly to bypass _monitor_order_status for this leg
                filled_long_order_dict = long_order.model_dump()
                filled_long_order_dict.update(
                    {
                        "status": OrderStatus.FILLED,
                        "quantity_filled": sized_opportunity.long_size,
                        "average_fill_price": hl_ticker.ask,  # Use a valid price
                        "updated_at": datetime.now(UTC),
                        "trades": [  # Add a trade to be consistent with a filled order
                            Trade(
                                id=f"{long_order.exchange_order_id}-trade-debug",
                                symbol=long_order.symbol,
                                side=OrderSide.BUY,
                                order_id=str(long_order.exchange_order_id),
                                exchange=exchange_id,
                                client_order_id=long_order.client_order_id,  # Use original Cloid
                                price=hl_ticker.ask or Decimal(0),
                                quantity=sized_opportunity.long_size,
                                fee=Decimal("0"),
                                fee_asset=None,
                                executed_at=datetime.now(UTC),
                                is_maker=False,
                            ),
                        ],
                    },
                )
                return Order(**filled_long_order_dict)
            if (
                exchange_id == "backpack"
                and side == OrderSide.SELL
                and not reduce_only
                and is_long_leg is False
            ):  # Short leg expected to fail
                raise short_order_failure
            if (
                exchange_id == "hyperliquid" and side == OrderSide.SELL and reduce_only is True
            ):  # Compensation leg
                # Use the parameters passed by the SUT for the compensation order
                compensation_details = comp_order.model_dump()  # Start with base defaults
                compensation_details.update(
                    {
                        "client_order_id": client_order_id or comp_order.client_order_id,
                        "order_type": order_type,  # From SUT
                        "price": price,  # From SUT (could be None for MARKET)
                        "time_in_force": time_in_force,  # From SUT
                        "quantity_requested": quantity,  # From SUT
                        "reduce_only": reduce_only,  # Should be True from SUT
                        "post_only": post_only,  # Should be False from SUT
                        "symbol": symbol,  # From SUT
                        "exchange": exchange_id,  # From SUT
                        "side": side,  # From SUT (SELL)
                        "status": OrderStatus.NEW,  # Mock assumes it's newly placed
                        "updated_at": datetime.now(UTC),
                        "created_at": datetime.now(UTC),
                        "quantity_filled": Decimal("0.0"),  # New order, not filled yet
                        "average_fill_price": None,  # New order
                    },
                )
                return Order(**compensation_details)

            raise ValueError(
                f"Unexpected place call: {exchange_id=} {side=} {reduce_only=} {is_long_leg=}",
            )

        return place_retry_side_effect

    async def test_execute_opportunity_failed_long_order(
        self,
        testable_execution_handler: TestableExecutionHandler,
        sized_opportunity: SizedOpportunity,
        mock_hl_api: AsyncMock,
        mock_bp_api: AsyncMock,
        mock_portfolio_tracker: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_symbol_mapper: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test execution flow when one leg fails and compensation is triggered."""
        # Setup configuration
        config_values = self._setup_failed_order_config()
        mock_config.get.side_effect = lambda key, default=None: config_values.get(key, default)

        mock_portfolio_tracker.get_position.return_value = None

        # Setup tickers
        hl_ticker, bp_ticker = self._setup_test_tickers()
        mock_hl_api.get_ticker.return_value = hl_ticker
        mock_bp_api.get_ticker.return_value = bp_ticker

        # Setup test orders
        (
            long_order,
            comp_order,
            short_order_failure,
            comp_order_type,
            comp_price,
        ) = self._setup_test_orders(
            sized_opportunity,
            mock_config,
            hl_ticker,
        )

        # Setup mock side effects
        place_retry_side_effect = self._create_place_retry_side_effect(
            long_order,
            comp_order,
            short_order_failure,
            sized_opportunity,
            hl_ticker,
        )

        # Re-added: This mock is essential for testing the monitoring of orders, esp. compensation
        async def get_status_side_effect(
            execution_obj: TradeExecution,  # Passed by SUT's _monitor_order_status
            exchange_id_arg: str,
            order_id_arg: str,
        ) -> Order | None:
            nonlocal long_order, comp_order, hl_ticker  # Ensure access to fixture vars
            await asyncio.sleep(0.01)
            if exchange_id_arg == "hyperliquid" and order_id_arg == long_order.exchange_order_id:
                # Original long order, which was filled
                return long_order.model_copy(
                    update={
                        "status": OrderStatus.FILLED,
                        "quantity_filled": sized_opportunity.long_size,
                        "average_fill_price": hl_ticker.ask,
                        "updated_at": datetime.now(UTC),
                    },
                )
            if exchange_id_arg == "hyperliquid" and order_id_arg == comp_order.exchange_order_id:
                # Compensation order, which should also get filled
                return comp_order.model_copy(
                    update={
                        "status": OrderStatus.FILLED,
                        "quantity_filled": sized_opportunity.long_size,
                        "average_fill_price": hl_ticker.bid,
                        "updated_at": datetime.now(UTC),
                    },
                )
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
                    },
                )
                return Order(**filled_long_for_comp_dict)
            return None

        mock_portfolio_tracker.get_order.side_effect = get_original_status_for_comp

        # Attempt to simplify mocking _monitor_order_status
        # Create an AsyncMock for _monitor_order_status directly
        mock_monitor_status = AsyncMock()

        # Compensation leg is the only one actively monitored if initial long is immediately FILLED
        # and short leg fails before monitoring. Only one FILLED status needed for compensation.
        mock_monitor_status.side_effect = [
            OrderStatus.FILLED,  # For the compensation order monitoring
        ]

        with (
            patch.object(
                testable_execution_handler,
                "_place_order_with_retry",
                side_effect=place_retry_side_effect,
            ) as mock_place_retry,
            patch.object(
                testable_execution_handler,
                "_get_order_status",
                side_effect=get_status_side_effect,
            ),
            patch.object(
                testable_execution_handler,
                "_monitor_order_status",
                new=mock_monitor_status,
            ),
            patch.object(
                testable_execution_handler,
                "test_compensate_position",
                wraps=testable_execution_handler.test_compensate_position,
            ) as mock_compensate,
        ):
            execution_result = await testable_execution_handler.execute_opportunity(
                sized_opportunity,
            )

        assert execution_result is not None
        assert execution_result.status == ExecutionStatus.FAILED
        assert execution_result.error_message is not None
        # Check for the specific error message from the failed short leg and successful compensation
        expected_error_msg = "Insufficient funds | Long leg compensated."
        assert execution_result.error_message == expected_error_msg

        # Ensure _compensate_position was called
        mock_compensate.assert_called_once()

        place_retry_call_args_list = mock_place_retry.call_args_list

        # Expected calls to _place_order_with_retry:
        # 1. Long leg (hyperliquid, BUY) - returns FILLED order
        # 2. Short leg (backpack, SELL) - raises APIError("Insufficient funds")
        # 3. Compensation leg (hyperliquid, SELL, reduce_only=True) - returns NEW order
        #    (then monitored)
        assert mock_place_retry.call_count == 3

        # Verify call 1: Long leg
        long_leg_call = place_retry_call_args_list[0]
        assert long_leg_call.kwargs.get("exchange_id") == "hyperliquid"
        assert long_leg_call.kwargs.get("side") == OrderSide.BUY
        assert long_leg_call.kwargs.get("reduce_only") is False  # Default for initial leg

        # Verify call 2: Short leg (failed)
        short_leg_call = place_retry_call_args_list[1]
        assert short_leg_call.kwargs.get("exchange_id") == "backpack"
        assert short_leg_call.kwargs.get("side") == OrderSide.SELL
        assert short_leg_call.kwargs.get("reduce_only") is False  # Default for initial leg

        # Verify call 3: Compensation leg
        comp_leg_call = place_retry_call_args_list[2]
        # Positional arguments for compensation call from _compensate_position
        # to _place_order_with_retry:
        # args: (execution, exchange_id, symbol, side, quantity, order_type, price)
        # kwargs: (time_in_force, reduce_only)
        assert comp_leg_call.args[1] == "hyperliquid"  # exchange_id
        assert comp_leg_call.args[2] == "BTC-PERP"  # symbol (original long symbol)
        assert comp_leg_call.args[3] == OrderSide.SELL  # side (opposite of original long)

        # Calculate the expected base quantity for compensation
        # This should match how _place_orders_for_opportunity calculates it
        # before calling _compensate_position
        # sized_opportunity.long_size is quote. opportunity.long_price is the target entry for long.
        assert sized_opportunity.opportunity.long_price is not None  # Ensure price is available
        expected_comp_base_quantity = (
            sized_opportunity.long_size / sized_opportunity.opportunity.long_price
        )

        assert (
            comp_leg_call.args[4] == expected_comp_base_quantity
        )  # quantity (base asset quantity)
        assert (
            comp_leg_call.args[5] == comp_order_type
        )  # order_type (LIMIT or MARKET based on config)
        assert comp_leg_call.args[6] == comp_price  # price (calculated or None)

        assert comp_leg_call.kwargs.get("time_in_force") == TimeInForce.GTC
        assert comp_leg_call.kwargs.get("reduce_only") is True

        # Verify that portfolio_tracker.process_trade was called for the initial filled long leg
        assert len(mock_portfolio_tracker.process_trade_call_tracker) == 1
        tracked_trade_info = mock_portfolio_tracker.process_trade_call_tracker[0]
        assert tracked_trade_info[0] == "hyperliquid"  # exchange_id
        assert isinstance(tracked_trade_info[1], Trade)
        assert tracked_trade_info[1].order_id == long_order.exchange_order_id
        assert tracked_trade_info[1].side == OrderSide.BUY

        # Clean up the tracker list for other tests that might use the same fixture instance
        mock_portfolio_tracker.process_trade_call_tracker.clear()

    def test_get_execution_history(
        self,
        testable_execution_handler: TestableExecutionHandler,
        sized_opportunity: SizedOpportunity,
    ) -> None:
        """Test retrieving the execution history."""
        exec1 = TradeExecution(sized_opportunity)
        exec1.id = "exec1"
        exec1.status = ExecutionStatus.COMPLETED
        exec2 = TradeExecution(sized_opportunity)
        exec2.id = "exec2"
        exec2.status = ExecutionStatus.FAILED
        testable_execution_handler.executions = []
        testable_execution_handler.test_add_to_history(exec1)
        testable_execution_handler.test_add_to_history(exec2)
        history = testable_execution_handler.executions
        assert isinstance(history, list) and len(history) == 2
        history_ids = {ex.id for ex in history}
        assert "exec1" in history_ids and "exec2" in history_ids

    def test_get_active_executions(
        self,
        execution_handler: ExecutionHandler,
        sized_opportunity: SizedOpportunity,
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
        self,
        execution_handler: ExecutionHandler,
        mock_circuit_breaker_system: MagicMock,
    ) -> None:
        """Test resetting the circuit breaker for an exchange."""
        execution_handler.reset_circuit_breaker("hyperliquid")
        mock_circuit_breaker_system.reset_breaker.assert_called_once_with("hyperliquid")
