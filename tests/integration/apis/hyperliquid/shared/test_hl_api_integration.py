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
from typing import Any

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
from tests.integration.apis.hyperliquid.shared.symbol_helpers import (
    get_available_symbols,
    get_test_symbol,
)
from tests.integration.apis.hyperliquid.shared.test_helpers import HyperliquidTestHelpers

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/shared/integration"], indirect=True
)
class TestHyperliquidAPIComponentIntegration:
    """Comprehensive integration tests for Hyperliquid API component interactions.

    Tests the integration between:
    - HyperliquidTradingService ↔ HyperliquidAccountService
    - HyperliquidMarketDataService ↔ Trading components
    - Data mappers consistency across the pipeline
    - Error handling across service boundaries
    - Authentication integration across endpoints
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_trading_account_service_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test integration between trading and account services with real data.

        This validates that placing orders correctly affects account summaries
        and that the data transformation pipeline maintains consistency.
        """
        # Get dynamic test symbol from exchange (no hardcoded symbols)
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Step 1: Get baseline account state (Account Service)
        initial_account = await hl_api_for_test_env.get_account_summary()
        assert isinstance(initial_account, MarginAccountSummary), (
            "Account service must return MarginAccountSummary model"
        )
        assert initial_account.exchange == "hyperliquid", (
            "Account data must specify correct exchange"
        )

        # Validate financial precision in account data
        assert isinstance(initial_account.total_equity, Decimal), (
            "Account equity must be Decimal for financial precision"
        )
        assert isinstance(initial_account.available_equity, Decimal), (
            "Available equity must be Decimal for financial precision"
        )

        # Step 2: Get market constraints (Market Data Service)
        await HyperliquidTestHelpers.get_market_constraints(hl_api_for_test_env, test_symbol)

        # Step 3: Calculate safe order parameters using real market data
        await HyperliquidTestHelpers.get_current_market_price(hl_api_for_test_env, test_symbol)

        # Use 10% below market price to avoid immediate fills
        safe_price = await HyperliquidTestHelpers.get_dynamic_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, Decimal("10.0")
        )

        # Get minimal viable order size based on account
        safe_quantity = await HyperliquidTestHelpers.get_minimal_order_size(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, safe_price
        )

        # Step 4: Place order (Trading Service)
        order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=safe_quantity,
            price=safe_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await hl_api_for_test_env.place_order(order_args)

        # Validate order model consistency
        assert isinstance(placed_order, Order), "Trading service must return Order model"
        assert placed_order.exchange_order_id is not None, (
            "Placed order must have exchange order ID"
        )
        assert placed_order.symbol == test_symbol, "Order symbol must match requested symbol"
        assert isinstance(placed_order.quantity_requested, Decimal), (
            "Order quantity must be Decimal for financial precision"
        )
        assert isinstance(placed_order.price, Decimal), (
            "Order price must be Decimal for financial precision"
        )

        # Step 5: Wait for order placement to be reflected (avoid race conditions)
        await HyperliquidTestHelpers.wait_for_order_placement(
            hl_api_for_test_env, placed_order.exchange_order_id
        )

        # Step 6: Verify account state changes (Account Service Integration)
        updated_account = await hl_api_for_test_env.get_account_summary()

        # Validate that services maintain data consistency
        assert isinstance(updated_account, MarginAccountSummary), (
            "Updated account must maintain model type consistency"
        )
        assert updated_account.exchange == initial_account.exchange, (
            "Exchange specification must be consistent across services"
        )

        # Check if margin usage increased (order should reserve margin)
        if (
            updated_account.total_initial_margin_required is not None
            and initial_account.total_initial_margin_required is not None
            and updated_account.total_initial_margin_required
            > initial_account.total_initial_margin_required
        ):
            margin_increase = (
                updated_account.total_initial_margin_required
                - initial_account.total_initial_margin_required
            )
            # Validate margin increase is reasonable for order size
            expected_margin = safe_quantity * safe_price * Decimal("0.1")  # ~10% margin
            assert margin_increase <= expected_margin * 2, (
                f"Margin increase {margin_increase} seems excessive for order size"
            )

        # Step 7: Test service integration for order cancellation
        cancel_args = CancelOrderArgs(
            order_id=placed_order.exchange_order_id,
            symbol=test_symbol,
        )

        cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)
        if not cancel_result:
            pytest.fail(
                "Order cancellation failed. This is a critical trading operation "
                "that must work reliably across service boundaries."
            )

        # Step 8: Wait for cancellation to be reflected and validate final state
        await HyperliquidTestHelpers._wait_for_order_cancellation(hl_api_for_test_env, test_symbol)

        # Final account state should reflect order removal
        final_account = await hl_api_for_test_env.get_account_summary()
        assert final_account is not None, "Final account summary must not be None"

        # Account equity should be unchanged (no fills occurred)
        equity_change = abs(final_account.total_equity - initial_account.total_equity)
        assert equity_change < Decimal("1.0"), (
            f"Total equity changed by {equity_change} after cancelled order. "
            "Cancelled orders should not affect total equity."
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_market_data_trading_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test integration between market data and trading services.

        Validates that market data from MarketDataService is correctly
        used by TradingService for order validation and execution.
        """
        # Get real available symbols (no hardcoding)
        available_symbols = await get_available_symbols(hl_api_for_test_env, "perp")
        assert len(available_symbols) > 0, (
            "Market data service must return available trading symbols"
        )

        test_symbol = available_symbols[0]

        # Step 1: Get market data (Market Data Service)
        market_args = GetMarketArgs(symbol=test_symbol)
        market_data = await hl_api_for_test_env.get_market(market_args)

        assert isinstance(market_data, Market), "Market data service must return Market model"
        assert market_data.symbol == test_symbol, "Market data symbol must match requested symbol"

        # Validate market data precision (critical for trading)
        assert isinstance(market_data.tick_size, Decimal), (
            "Tick size must be Decimal for precise order pricing"
        )
        assert isinstance(market_data.step_size, Decimal), (
            "Step size must be Decimal for precise order sizing"
        )
        assert isinstance(market_data.min_quantity, Decimal), (
            "Min quantity must be Decimal for precise order validation"
        )

        # Step 2: Get current ticker (Market Data Service)
        ticker = await hl_api_for_test_env.get_ticker(test_symbol)

        assert isinstance(ticker, Ticker), "Market data service must return Ticker model"
        assert ticker.symbol == test_symbol, "Ticker symbol must match requested symbol"
        assert isinstance(ticker.price, Decimal), (
            "Ticker price must be Decimal for financial precision"
        )

        # Validate data freshness (prevent stale data trading disasters)
        data_age = datetime.now(UTC) - ticker.timestamp
        if data_age > timedelta(minutes=5):
            pytest.fail(
                f"Ticker data is {data_age} old. Trading requires fresh market data "
                "to prevent execution at stale prices."
            )

        # Step 3: Test that trading service respects market constraints
        # Use market data to create a valid order that respects constraints

        # Price must respect tick size
        market_price = ticker.price
        tick_size = market_data.tick_size
        aligned_price = (market_price / tick_size).quantize(Decimal("1")) * tick_size

        # Quantity must respect step size and minimums
        step_size = market_data.step_size
        min_quantity = market_data.min_quantity
        test_quantity = max(min_quantity, step_size)

        # Step 4: Validate trading service uses market data correctly
        order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=aligned_price * Decimal("0.9"),  # 10% below market to avoid fills
            time_in_force=TimeInForce.GTC,
        )

        # Trading service should accept order that respects market constraints
        placed_order = await hl_api_for_test_env.place_order(order_args)
        assert placed_order.exchange_order_id is not None, (
            "Order respecting market constraints should be accepted"
        )

        # Step 5: Test constraint violation handling
        invalid_quantity = min_quantity / Decimal("2")  # Below minimum
        invalid_order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=invalid_quantity,
            price=aligned_price * Decimal("0.9"),
            time_in_force=TimeInForce.GTC,
        )

        # Trading service should reject orders violating market constraints
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.place_order(invalid_order_args)

        # Validate error mapping consistency
        api_error = exc_info.value
        assert api_error.code in [
            APIErrorCode.INVALID_ORDER_SIZE.value,
            APIErrorCode.MIN_QUANTITY_NOT_MET.value,
        ], f"Expected quantity violation error, got {api_error.code}"

        # Cleanup: Cancel the valid order
        if placed_order.exchange_order_id:
            cancel_args = CancelOrderArgs(
                order_id=placed_order.exchange_order_id,
                symbol=test_symbol,
            )
            cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)
            if not cancel_result:
                pytest.fail("Failed to cleanup test order - critical trading operation")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_error_handling_consistency_across_services(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that error handling is consistent across all API services.

        Validates that errors are properly mapped and propagated consistently
        across the Trading, Account, and Market Data services.
        """
        # Test 1: Invalid symbol handling across services
        invalid_symbol = "INVALID_SYMBOL_XYZ_123"

        # Market Data Service error handling
        with pytest.raises(APIError) as market_exc:
            await hl_api_for_test_env.get_ticker(invalid_symbol)

        market_error = market_exc.value
        assert market_error.code == APIErrorCode.SYMBOL_NOT_FOUND.value, (
            f"Market data service should map invalid symbol to SYMBOL_NOT_FOUND, "
            f"got {market_error.code}"
        )

        # Trading Service error handling
        invalid_order_args = PlaceOrderArgs(
            symbol=invalid_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("100.0"),
            time_in_force=TimeInForce.GTC,
        )

        with pytest.raises(APIError) as trading_exc:
            await hl_api_for_test_env.place_order(invalid_order_args)

        trading_error = trading_exc.value
        assert trading_error.code == APIErrorCode.SYMBOL_NOT_FOUND.value, (
            f"Trading service should map invalid symbol to SYMBOL_NOT_FOUND, "
            f"got {trading_error.code}"
        )

        # Error messages should be informative and consistent
        assert invalid_symbol in market_error.message, (
            "Error message should include the invalid symbol for debugging"
        )
        assert invalid_symbol in trading_error.message, (
            "Error message should include the invalid symbol for debugging"
        )

        # Test 2: Authentication error consistency
        # This would require testing with invalid credentials, which we skip
        # in integration tests to avoid API lockouts

        # Test 3: Network error handling consistency
        # We don't test network failures directly as they're environmental

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_data_model_consistency_across_pipeline(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that data models maintain consistency across the entire pipeline.

        Validates that the same symbol/market data is consistently represented
        across different services and operations.
        """
        # Get a real test symbol
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Step 1: Get the same symbol data from different services
        market_data = await hl_api_for_test_env.get_market(GetMarketArgs(symbol=test_symbol))
        ticker_data = await hl_api_for_test_env.get_ticker(test_symbol)
        assert ticker_data is not None, "Ticker data must not be None"

        # Step 2: Validate symbol consistency
        assert market_data.symbol == ticker_data.symbol == test_symbol, (
            "Symbol representation must be consistent across all services"
        )

        # Step 3: Validate financial data types are consistent
        assert ticker_data.price is not None, "Ticker price must not be None"
        financial_fields = [
            (market_data.tick_size, "market tick_size"),
            (market_data.step_size, "market step_size"),
            (market_data.min_quantity, "market min_quantity"),
            (ticker_data.price, "ticker price"),
        ]

        for field_value, field_name in financial_fields:
            assert isinstance(field_value, Decimal), (
                f"{field_name} must be Decimal for financial precision"
            )
            # Validate no negative values for positive-definite quantities
            if "price" in field_name or "size" in field_name or "quantity" in field_name:
                assert field_value > Decimal("0"), f"{field_name} must be positive: {field_value}"

        # Step 4: Validate timestamp consistency (timezone-aware)
        if hasattr(ticker_data, "timestamp") and ticker_data.timestamp:
            assert ticker_data.timestamp.tzinfo is not None, (
                "All financial timestamps must be timezone-aware"
            )

            # Validate timestamp is recent (not stale data)
            time_diff = datetime.now(UTC) - ticker_data.timestamp
            assert time_diff < timedelta(hours=1), (
                f"Ticker timestamp {ticker_data.timestamp} is too old: {time_diff}"
            )

        # Step 5: Test order creation maintains model consistency
        safe_price = await HyperliquidTestHelpers.get_dynamic_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, Decimal("10.0")
        )
        safe_quantity = market_data.min_quantity
        assert safe_quantity is not None, "Safe quantity must not be None"

        order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=safe_quantity,
            price=safe_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await hl_api_for_test_env.place_order(order_args)

        # Validate order maintains symbol consistency
        assert placed_order.symbol == test_symbol, "Order symbol must be consistent with input"

        # Validate order maintains precision consistency
        assert isinstance(placed_order.quantity_requested, Decimal), (
            "Order quantity must maintain Decimal precision"
        )
        assert isinstance(placed_order.price, Decimal), (
            "Order price must maintain Decimal precision"
        )
        assert placed_order.price is not None, "Order price must not be None"

        # Validate order respects market constraints
        assert market_data.min_quantity is not None, "Market min quantity must not be None"
        assert placed_order.quantity_requested >= market_data.min_quantity, (
            "Order quantity must respect market minimum"
        )

        # Price should respect tick size (within rounding tolerance)
        price_remainder = placed_order.price % market_data.tick_size
        assert price_remainder == Decimal("0"), (
            f"Order price {placed_order.price} must align with tick size {market_data.tick_size}"
        )

        # Cleanup
        if placed_order.exchange_order_id:
            cancel_args = CancelOrderArgs(
                order_id=placed_order.exchange_order_id,
                symbol=test_symbol,
            )
            await hl_api_for_test_env.cancel_order(cancel_args)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_concurrent_service_operations(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that concurrent operations across services maintain consistency.

        Validates that the API can handle concurrent operations across different
        services without data corruption or race conditions.
        """
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Define concurrent operations across different services
        async def get_account_data() -> MarginAccountSummary:
            result = await hl_api_for_test_env.get_account_summary()
            assert result is not None, "Account summary must not be None"
            return result

        async def get_market_data() -> Market:
            return await hl_api_for_test_env.get_market(GetMarketArgs(symbol=test_symbol))

        async def get_ticker_data() -> Ticker:
            result = await hl_api_for_test_env.get_ticker(test_symbol)
            assert result is not None, "Ticker must not be None"
            return result

        async def get_open_orders() -> list[Order]:
            return await hl_api_for_test_env.get_open_orders()

        # Execute operations concurrently
        start_time = datetime.now(UTC)
        results = await asyncio.gather(
            get_account_data(),
            get_market_data(),
            get_ticker_data(),
            get_open_orders(),
            return_exceptions=True,
        )
        end_time = datetime.now(UTC)

        # Validate all operations completed successfully
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                pytest.fail(f"Concurrent operation {i} failed: {result}")

        # Type cast results after confirming they're not exceptions
        account_data = results[0]
        market_data = results[1]
        ticker_data = results[2]
        open_orders = results[3]

        # Validate data consistency despite concurrent execution
        assert hasattr(account_data, "exchange") and account_data.exchange == "hyperliquid", (
            "Account data must maintain exchange consistency"
        )
        assert hasattr(market_data, "symbol") and market_data.symbol == test_symbol, (
            "Market data must maintain symbol consistency"
        )
        assert hasattr(ticker_data, "symbol") and ticker_data.symbol == test_symbol, (
            "Ticker data must maintain symbol consistency"
        )
        assert isinstance(open_orders, list), "Open orders must return list consistently"

        # Validate timing consistency (operations should complete reasonably quickly)
        total_time = end_time - start_time
        if total_time > timedelta(seconds=10):
            pytest.fail(
                f"Concurrent operations took {total_time}, may indicate "
                "performance issues or rate limiting problems"
            )

        # Validate financial data precision maintained across concurrent calls
        financial_values = []
        if hasattr(account_data, "total_equity"):
            financial_values.append(account_data.total_equity)
        if hasattr(account_data, "available_equity"):
            financial_values.append(account_data.available_equity)
        if hasattr(market_data, "tick_size"):
            financial_values.append(market_data.tick_size)
        if hasattr(ticker_data, "price") and ticker_data.price is not None:
            financial_values.append(ticker_data.price)

        for value in financial_values:
            assert isinstance(value, Decimal), (
                "Financial precision must be maintained in concurrent operations"
            )
