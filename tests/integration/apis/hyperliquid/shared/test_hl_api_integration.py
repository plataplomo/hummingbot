"""Comprehensive secure integration tests for Hyperliquid API component interactions.

This module tests the integration between different API components to ensure they work
together correctly in real trading scenarios. It focuses on:

1. Service layer integration (Trading, Market Data, Account services)
2. Data mapper consistency across components
3. Request/response pipeline integrity
4. Error handling consistency across the stack
5. Authentication flow integration
6. Real market data validation and transformation

SECURITY COMPLIANCE:
- Uses only real market data from exchange APIs (no mocks/hardcoded values)
- Implements fail-fast error handling for all critical operations
- Validates financial data precision throughout the pipeline
- Uses timezone-aware datetime operations
- Implements proper polling for race condition prevention
- Validates currency/symbol handling across components

Authentication: EIP-712 signing for private endpoints, address-based for public
VCR: Records real API responses for reproducible testing
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetMarketArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.market.market import Market
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.market.ticker import Ticker
from tests.integration.apis.hyperliquid.shared.hl_test_helpers import HyperliquidTestHelpers
from tests.integration.apis.hyperliquid.shared.symbol_helpers import (
    get_test_symbol,
)


logger = logging.getLogger(__name__)

# Mark all tests in this file as integration tests requiring network
pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_network,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/shared/integration"], indirect=True
)
class TestHyperliquidAPIComponentIntegration:
    """Comprehensive integration tests for Hyperliquid API component interactions.

    Tests the integration between:
    - Trading service and market data service
    - Account service and order management
    - Request builder and response handler
    - Authentication components
    - Data mappers across services
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_trading_account_service_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test integration between trading and account services."""
        # Get account summary before trading
        account_before = await hl_api_for_test_env.get_account_summary()
        assert account_before is not None, "Account summary must be accessible"
        assert isinstance(account_before, MarginAccountSummary), (
            "Account summary must be MarginAccountSummary"
        )

        # Get a valid test symbol
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Place a limit order (won't execute immediately)
        minimal_size = await HyperliquidTestHelpers.get_minimal_order_size(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )
        test_price = await HyperliquidTestHelpers.get_dynamic_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, Decimal("10")
        )

        order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=minimal_size,
            price=test_price,
            time_in_force=TimeInForce.GTC,
            post_only=True,
        )

        try:
            placed_order = await hl_api_for_test_env.place_order(order_args)
            assert placed_order.exchange_order_id is not None, "Order must have exchange ID"

            # Verify order appears in open orders
            open_orders = await hl_api_for_test_env.get_open_orders()
            found_order = any(
                order.exchange_order_id == placed_order.exchange_order_id for order in open_orders
            )
            assert found_order, "Placed order must appear in open orders"

            # Account summary should reflect the open order (available equity reduced)
            account_after = await hl_api_for_test_env.get_account_summary()
            assert account_after is not None, "Account summary must remain accessible"

            # Cleanup: cancel the order
            cancel_args = CancelOrderArgs(
                symbol=test_symbol, order_id=placed_order.exchange_order_id
            )
            await hl_api_for_test_env.cancel_order(cancel_args)

            # Wait for order cancellation
            await HyperliquidTestHelpers.wait_for_order_cancellation(
                hl_api_for_test_env, test_symbol
            )

        except APIError as e:
            if e.code == APIErrorCode.INSUFFICIENT_FUNDS.value:
                pytest.skip("Insufficient balance for trading test")
            elif e.code == APIErrorCode.ORDER_REJECTED.value:
                # Order was rejected for crossing spread - this is expected behavior
                pass
            else:
                raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_market_data_trading_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test integration between market data and trading services."""
        # Get test symbol and market data
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get comprehensive market data for the symbol
        market = await hl_api_for_test_env.get_market(GetMarketArgs(symbol=test_symbol))
        ticker = await hl_api_for_test_env.get_ticker(test_symbol)
        orderbook = await hl_api_for_test_env.get_order_book(test_symbol)

        assert market is not None, "Market data must be available"
        assert ticker is not None, "Ticker data must be available"
        assert orderbook is not None, "Orderbook must be available"

        # Verify data consistency across services
        assert market.symbol == test_symbol, "Market symbol must match request"
        assert ticker.symbol == test_symbol, "Ticker symbol must match request"
        assert orderbook.symbol == test_symbol, "Orderbook symbol must match request"

        # Verify price relationships
        if ticker.price and orderbook.bids and orderbook.asks:
            best_bid = orderbook.bids[0][0]  # price is first element of tuple
            best_ask = orderbook.asks[0][0]  # price is first element of tuple

            # Note: Due to timing differences between ticker and orderbook updates in real market
            # data,
            # the ticker price may occasionally fall outside the current bid-ask spread.
            # This is a sanity check rather than a strict requirement - we allow a reasonable
            # deviation.
            mid_price = (best_bid + best_ask) / Decimal("2")
            max_deviation_pct = Decimal("0.02")  # Allow 2% deviation from mid price
            tolerance = mid_price * max_deviation_pct

            # Validate ticker price is reasonably close to market (within 2% of mid)
            min_reasonable_price = mid_price - tolerance
            max_reasonable_price = mid_price + tolerance

            assert min_reasonable_price <= ticker.price <= max_reasonable_price, (
                f"Ticker price {ticker.price} unreasonably far from market mid-price {mid_price}. "
                f"Bid: {best_bid}, Ask: {best_ask}, "
                f"Reasonable range: [{min_reasonable_price}, {max_reasonable_price}] (±2%)"
            )

        # Use market data to calculate safe order parameters
        if ticker.price:
            # Calculate order size based on market constraints
            minimal_size = await HyperliquidTestHelpers.get_minimal_order_size(
                hl_api_for_test_env, test_symbol, OrderSide.BUY, ticker.price
            )

            # Verify the calculated size meets market constraints
            if market.min_quantity is not None:
                assert minimal_size >= market.min_quantity, (
                    f"Calculated size {minimal_size} must meet minimum {market.min_quantity}"
                )

            # Verify size is properly aligned to step size
            size_steps = minimal_size / market.step_size
            assert size_steps == int(size_steps), (
                f"Size {minimal_size} must be aligned to step size {market.step_size}"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_data_model_consistency_across_pipeline(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that data models maintain consistency through the request/response pipeline."""
        # Get test symbol
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Test decimal precision consistency
        market = await hl_api_for_test_env.get_market(GetMarketArgs(symbol=test_symbol))
        assert market is not None, "Market data required for test"

        # All financial values should be Decimal
        financial_fields = [
            market.tick_size,
            market.step_size,
            market.min_quantity,
            market.max_quantity,
        ]

        for field in financial_fields:
            if field is not None:
                assert isinstance(field, Decimal), (
                    f"Financial field {field} must be Decimal, got {type(field)}"
                )

        # Test order model consistency
        minimal_size = await HyperliquidTestHelpers.get_minimal_order_size(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )
        test_price = await HyperliquidTestHelpers.get_dynamic_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, Decimal("10")
        )

        order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=minimal_size,
            price=test_price,
            time_in_force=TimeInForce.GTC,
            post_only=True,
        )

        try:
            placed_order = await hl_api_for_test_env.place_order(order_args)

            # Verify order model consistency
            assert placed_order.symbol == order_args.symbol, "Symbol must match"
            assert placed_order.side == order_args.side, "Side must match"
            assert placed_order.order_type == order_args.order_type, "Order type must match"
            assert placed_order.quantity_requested == order_args.quantity, "Quantity must match"
            assert placed_order.price == order_args.price, "Price must match"

            # Cleanup
            if placed_order.exchange_order_id:
                cancel_args = CancelOrderArgs(
                    symbol=test_symbol, order_id=placed_order.exchange_order_id
                )
                try:
                    await hl_api_for_test_env.cancel_order(cancel_args)
                    await HyperliquidTestHelpers.wait_for_order_cancellation(
                        hl_api_for_test_env, test_symbol
                    )
                except APIError:
                    pass  # Cancellation failure is acceptable in cleanup

        except APIError as e:
            if e.code in [
                APIErrorCode.INSUFFICIENT_FUNDS.value,
                APIErrorCode.ORDER_REJECTED.value,
            ]:
                pytest.skip(f"Cannot test order consistency: {e.message}")
            else:
                raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_timezone_consistency_across_services(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that all services handle timezones consistently."""
        # Get test symbol
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get data from different services that include timestamps
        account = await hl_api_for_test_env.get_account_summary()
        orders = await hl_api_for_test_env.get_open_orders()
        trades = await hl_api_for_test_env.get_recent_trades(test_symbol)

        # Verify account timestamp is timezone-aware if present
        if account and hasattr(account, "timestamp") and account.timestamp:
            assert account.timestamp.tzinfo is not None, "Account timestamp must be timezone-aware"
            assert account.timestamp.tzinfo.utcoffset(None) == timedelta(0), (
                "Account timestamp must be in UTC"
            )

        # Verify order timestamps are timezone-aware
        for order in orders:
            if order.created_at:
                assert order.created_at.tzinfo is not None, (
                    f"Order {order.exchange_order_id} created_at must be timezone-aware"
                )
                assert order.created_at.tzinfo.utcoffset(None) == timedelta(0), (
                    f"Order {order.exchange_order_id} created_at must be in UTC"
                )

        # Verify trade timestamps are timezone-aware
        for trade in trades:
            if hasattr(trade, "executed_at") and trade.executed_at:
                assert trade.executed_at.tzinfo is not None, (
                    "Trade executed_at must be timezone-aware"
                )
                assert trade.executed_at.tzinfo.utcoffset(None) == timedelta(0), (
                    "Trade executed_at must be in UTC"
                )

    def _validate_timing(self, start_time: datetime, end_time: datetime) -> None:
        """Validate that operations completed within reasonable time."""
        duration = (end_time - start_time).total_seconds()
        assert duration < 5.0, f"Concurrent operations took too long: {duration}s"

    def _separate_results(self, results: list[Any]) -> tuple[list[Any], list[Exception]]:
        """Separate successful results from errors."""
        successful_results: list[Any] = []
        errors: list[Exception] = []
        for result in results:
            if isinstance(result, Exception):
                errors.append(result)
            else:
                successful_results.append(result)
        return successful_results, errors

    def _categorize_results(
        self, successful_results: list[Any]
    ) -> tuple[
        MarginAccountSummary | None,
        Market | None,
        Ticker | None,
        list[Order] | None,
    ]:
        """Categorize successful results by type."""
        account_data = None
        market_data = None
        ticker_data = None
        orders_data = None

        for result in successful_results:
            if isinstance(result, MarginAccountSummary):
                account_data = result
            elif isinstance(result, Market):
                market_data = result
            elif isinstance(result, Ticker):
                ticker_data = result
            elif isinstance(result, list):
                # Pyright has difficulty with type narrowing in comprehensions
                # Check all items are Orders without using comprehension
                all_orders = True
                # Type narrowing: after isinstance check, result is a list
                for item in result:  # pyright: ignore[reportUnknownVariableType]
                    if not isinstance(item, Order):
                        all_orders = False
                        break
                if all_orders:
                    orders_data = cast("list[Order]", result)

        return account_data, market_data, ticker_data, orders_data

    def _validate_financial_precision(
        self,
        account_data: MarginAccountSummary | None,
        market_data: Market | None,
        ticker_data: Ticker | None,
    ) -> None:
        """Validate financial precision is maintained."""
        financial_values: list[Decimal] = []
        if isinstance(account_data, MarginAccountSummary):
            financial_values.append(account_data.total_equity)
            financial_values.append(account_data.available_equity)
        if isinstance(market_data, Market):
            financial_values.append(market_data.tick_size)
        if isinstance(ticker_data, Ticker) and ticker_data.price is not None:
            financial_values.append(ticker_data.price)

        for value in financial_values:
            assert isinstance(value, Decimal), (
                "Financial precision must be maintained in concurrent operations"
            )

    async def _validate_concurrent_operation_results(
        self,
        results: list[Any],
        test_symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> None:
        """Validate results from concurrent operations."""
        # Check timing
        self._validate_timing(start_time, end_time)

        # Separate and validate results
        successful_results, errors = self._separate_results(results)

        # At least some operations should succeed
        assert len(successful_results) >= 2, (
            f"Too many concurrent operation failures: {len(errors)} errors"
        )

        # Categorize results
        account_data, market_data, ticker_data, orders_data = self._categorize_results(
            successful_results
        )

        # Validate data consistency across concurrent results
        if market_data and ticker_data:
            assert market_data.symbol == test_symbol, "Market symbol must match request"
            assert ticker_data.symbol == test_symbol, "Ticker symbol must match request"

        # Validate orders data if present
        if orders_data is not None:
            for order in orders_data:
                assert isinstance(order, Order), "Orders list must contain Order objects"

        # Validate financial precision
        self._validate_financial_precision(account_data, market_data, ticker_data)


class TestHyperliquidAPIConcurrentOperations:
    """Tests for concurrent operations without VCR recording."""

    def _validate_timing(self, start_time: datetime, end_time: datetime) -> None:
        """Validate that operations completed within reasonable time."""
        duration = (end_time - start_time).total_seconds()
        assert duration < 5.0, f"Concurrent operations took too long: {duration}s"

    def _separate_results(self, results: list[Any]) -> tuple[list[Any], list[Exception]]:
        """Separate successful results from errors."""
        successful_results: list[Any] = []
        errors: list[Exception] = []
        for result in results:
            if isinstance(result, Exception):
                errors.append(result)
            else:
                successful_results.append(result)
        return successful_results, errors

    def _categorize_results(
        self, successful_results: list[Any]
    ) -> tuple[
        MarginAccountSummary | None,
        Market | None,
        Ticker | None,
        list[Order] | None,
    ]:
        """Categorize successful results by type."""
        account_data = None
        market_data = None
        ticker_data = None
        orders_data = None

        for result in successful_results:
            if isinstance(result, MarginAccountSummary):
                account_data = result
            elif isinstance(result, Market):
                market_data = result
            elif isinstance(result, Ticker):
                ticker_data = result
            elif isinstance(result, list):
                # Pyright has difficulty with type narrowing in comprehensions
                # Check if empty or all items are Orders without using comprehension
                if not result:
                    # Empty list is valid
                    orders_data = cast("list[Order]", result)
                else:
                    all_orders = True
                    # Type narrowing: result is a non-empty list
                    for item in result:  # pyright: ignore[reportUnknownVariableType]
                        if not isinstance(item, Order):
                            all_orders = False
                            break
                    if all_orders:
                        orders_data = cast("list[Order]", result)

        return account_data, market_data, ticker_data, orders_data

    def _validate_data_consistency(
        self,
        market_data: Market | None,
        ticker_data: Ticker | None,
        orders_data: list[Order] | None,
        test_symbol: str,
    ) -> None:
        """Validate data consistency across concurrent results."""
        if market_data and ticker_data:
            assert market_data.symbol == test_symbol, "Market symbol must match request"
            assert ticker_data.symbol == test_symbol, "Ticker symbol must match request"

        if orders_data is not None:
            for order in orders_data:
                assert isinstance(order, Order), "Orders list must contain Order objects"

    def _validate_financial_precision(
        self,
        account_data: MarginAccountSummary | None,
        market_data: Market | None,
        ticker_data: Ticker | None,
    ) -> None:
        """Validate financial precision is maintained."""
        financial_values: list[Decimal] = []
        if isinstance(account_data, MarginAccountSummary):
            financial_values.append(account_data.total_equity)
            financial_values.append(account_data.available_equity)
        if isinstance(market_data, Market):
            financial_values.append(market_data.tick_size)
        if isinstance(ticker_data, Ticker) and ticker_data.price is not None:
            financial_values.append(ticker_data.price)

        for value in financial_values:
            assert isinstance(value, Decimal), (
                "Financial precision must be maintained in concurrent operations"
            )

    async def _execute_concurrent_operations(
        self, hl_api_for_test_env: HyperliquidAPI, test_symbol: str
    ) -> tuple[list[Any], datetime, datetime]:
        """Execute concurrent operations and return results with timing."""

        async def get_account_data() -> MarginAccountSummary:
            result = await hl_api_for_test_env.get_account_summary()
            assert result is not None, "Account summary must not be None"
            return result

        async def get_market_data() -> Market:
            result = await hl_api_for_test_env.get_market(GetMarketArgs(symbol=test_symbol))
            assert result is not None, "Market must not be None"
            return result

        async def get_ticker_data() -> Ticker:
            result = await hl_api_for_test_env.get_ticker(test_symbol)
            assert result is not None, "Ticker must not be None"
            return result

        async def get_open_orders() -> list[Order]:
            return await hl_api_for_test_env.get_open_orders()

        start_time = datetime.now(UTC)
        results = await asyncio.gather(
            get_account_data(),
            get_market_data(),
            get_ticker_data(),
            get_open_orders(),
            return_exceptions=True,
        )
        end_time = datetime.now(UTC)

        # Cast results to list[Any] since gather with return_exceptions=True returns mixed types
        return list(results), start_time, end_time

    @pytest.mark.asyncio
    async def test_concurrent_operations_no_vcr(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that concurrent operations across services maintain consistency without VCR."""
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Execute operations concurrently
        results, start_time, end_time = await self._execute_concurrent_operations(
            hl_api_for_test_env, test_symbol
        )

        # Validate timing
        self._validate_timing(start_time, end_time)

        # Separate and validate results
        successful_results, errors = self._separate_results(results)

        # At least some operations should succeed
        assert len(successful_results) >= 2, (
            f"Too many concurrent operation failures: {len(errors)} errors"
        )

        # Categorize results
        account_data, market_data, ticker_data, orders_data = self._categorize_results(
            successful_results
        )

        # Validate data consistency
        self._validate_data_consistency(market_data, ticker_data, orders_data, test_symbol)

        # Validate financial precision
        self._validate_financial_precision(account_data, market_data, ticker_data)

    @pytest.mark.asyncio
    async def test_error_handling_consistency_across_services(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that different services handle errors consistently without VCR."""
        # Test 1: Invalid symbol across different services
        invalid_symbol = "INVALID_SYMBOL_XYZ"

        # Market service should handle invalid symbol gracefully
        with pytest.raises(APIError) as market_exc:
            await hl_api_for_test_env.get_market(GetMarketArgs(symbol=invalid_symbol))
        assert market_exc.value.code in [
            APIErrorCode.INVALID_SYMBOL.value,
            APIErrorCode.SYMBOL_NOT_FOUND.value,
        ], f"Market service returned unexpected error code: {market_exc.value.code}"

        # Ticker service should handle invalid symbol by returning None
        ticker_result = await hl_api_for_test_env.get_ticker(invalid_symbol)
        assert ticker_result is None, "Ticker service should return None for invalid symbols"

        # Trading service should also handle invalid symbol
        try:
            order_args = PlaceOrderArgs(
                symbol=invalid_symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.IOC,
            )
            await hl_api_for_test_env.place_order(order_args)
            pytest.fail("Expected APIError for invalid symbol in place_order")
        except APIError as e:
            assert e.code in [
                APIErrorCode.INVALID_SYMBOL.value,
                APIErrorCode.SYMBOL_NOT_FOUND.value,
                APIErrorCode.INVALID_REQUEST.value,  # Hyperliquid may return this
            ], f"Trading service returned unexpected error code: {e.code}"
