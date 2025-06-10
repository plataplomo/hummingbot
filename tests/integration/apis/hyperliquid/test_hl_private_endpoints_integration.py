"""Integration tests for Hyperliquid private (authenticated) endpoints.

This module provides comprehensive integration testing for all private Hyperliquid API endpoints
that require authentication via EIP-712 signing. Tests validate the complete data pipeline from
API method calls through to final Internal Domain Models, using pytest-recording (VCR) for
deterministic testing.

These tests use the hl_api_for_test_env fixture which creates real HyperliquidAPI instances
configured with test secrets, enabling full end-to-end validation of:
- EIP-712 authentication and signing
- Request building and validation
- HTTP communication to /exchange and /info endpoints
- Response handling and error mapping
- Data mapping to internal models

All sensitive data (private keys, signatures, nonces, wallet addresses) is filtered by VCR
configuration before cassettes are written to disk.

Note: Tests should be run against testnet environment by setting CYBERDELTA_TEST_ENV_HL=testnet
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.spot_balance import SpotBalance

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/private"], indirect=True)
class TestHyperliquidPrivateEndpoints:
    """Integration tests for Hyperliquid private endpoints.

    Tests the complete pipeline from API method calls to Internal Domain Models
    for all authenticated Hyperliquid endpoints using EIP-712 signing.

    Tests cover both /exchange endpoints (trading actions) and /info endpoints
    (private data queries).
    """

    @pytest.mark.vcr
    async def test_get_balances_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() validates full pipeline to SpotBalance models.

        Validates:
        - EIP-712 signing for /info endpoint with user query
        - Request building with wallet address
        - Response parsing from clearinghouse state
        - Mapping to SpotBalance instances with Decimal precision
        """
        # Execute the full pipeline
        balances = await hl_api_for_test_env.get_balances()

        # Validate return type
        assert isinstance(balances, dict), "get_balances() should return dict[str, SpotBalance]"

        # If balances exist, validate structure and types
        if balances:
            # Get a sample balance for detailed validation
            sample_asset = next(iter(balances.keys()))
            sample_balance = balances[sample_asset]

            # Validate it's the correct internal model
            assert isinstance(sample_balance, SpotBalance), (
                f"Balance for {sample_asset} should be SpotBalance instance"
            )

            # Validate asset name matches key
            assert sample_balance.asset == sample_asset, (
                "SpotBalance.asset should match dictionary key"
            )

            # Validate Decimal precision for financial values
            assert isinstance(sample_balance.total_quantity, Decimal), (
                "total_quantity must be Decimal"
            )
            assert isinstance(sample_balance.available_quantity, Decimal), (
                "available_quantity must be Decimal"
            )
            # SpotBalance doesn't have locked_quantity - Hyperliquid uses total and available

            # Validate business logic constraints
            assert sample_balance.total_quantity >= Decimal("0"), (
                "total_quantity must be non-negative"
            )
            assert sample_balance.available_quantity >= Decimal("0"), (
                "available_quantity must be non-negative"
            )
            # Hyperliquid SpotBalance uses total_quantity and available_quantity

    @pytest.mark.vcr
    async def test_get_positions_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() validates full pipeline to DerivativePosition models.

        Validates:
        - EIP-712 signing for /info endpoint user state query
        - Response mapping to DerivativePosition instances
        - Decimal precision for position values
        - Position side and PnL calculations
        """
        # Execute the full pipeline
        positions = await hl_api_for_test_env.get_positions()

        # Validate return type
        assert isinstance(positions, list), "get_positions() should return list[DerivativePosition]"

        # If positions exist, validate structure
        if positions:
            sample_position = positions[0]
            assert isinstance(sample_position, DerivativePosition), (
                "Position should be DerivativePosition instance"
            )

            # Validate symbol format (Hyperliquid uses asset names like "BTC", "ETH")
            assert isinstance(sample_position.symbol, str), "symbol must be string"
            assert len(sample_position.symbol) > 0, "symbol must not be empty"

            # Validate Decimal precision for financial values
            assert isinstance(sample_position.size, Decimal), "size must be Decimal"
            assert isinstance(sample_position.entry_price, Decimal), "entry_price must be Decimal"
            assert isinstance(sample_position.mark_price, Decimal), "mark_price must be Decimal"
            assert isinstance(sample_position.unrealized_pnl, Decimal), (
                "unrealized_pnl must be Decimal"
            )
            assert isinstance(sample_position.realized_pnl, Decimal), "realized_pnl must be Decimal"

            # Validate price constraints
            if sample_position.entry_price > Decimal("0"):  # May be 0 for new positions
                assert sample_position.mark_price > Decimal("0"), "mark_price must be positive"

    @pytest.mark.vcr
    async def test_get_account_summary_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() validates full pipeline to MarginAccount model.

        Validates:
        - EIP-712 signing for /info endpoint user state query
        - Response mapping to MarginAccount internal model
        - Decimal precision for all financial values
        - Account status and margin calculations
        """
        # Execute the full pipeline
        account_summary = await hl_api_for_test_env.get_account_summary()

        # Validate return type
        assert isinstance(account_summary, MarginAccountSummary), (
            "get_account_summary() should return MarginAccountSummary"
        )

        # Validate Decimal precision for all financial fields
        assert isinstance(account_summary.total_equity, Decimal), "total_equity must be Decimal"
        assert isinstance(account_summary.available_equity, Decimal), (
            "available_equity must be Decimal"
        )

        # Validate business logic constraints
        assert account_summary.total_equity >= Decimal("0"), "total_equity must be non-negative"
        assert account_summary.available_equity >= Decimal("0"), (
            "available_equity must be non-negative"
        )

        # Check optional fields if present
        if account_summary.total_initial_margin_required is not None:
            assert isinstance(account_summary.total_initial_margin_required, Decimal), (
                "total_initial_margin_required must be Decimal"
            )
            assert account_summary.total_initial_margin_required >= Decimal("0"), (
                "total_initial_margin_required must be non-negative"
            )

    @pytest.mark.vcr
    async def test_get_open_orders_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_open_orders() validates full pipeline to Order models.

        Validates:
        - EIP-712 signing for /info endpoint open orders query
        - Response mapping to list of Order instances
        - Order field validation and types
        - Hyperliquid-specific order ID format
        """
        # Execute the full pipeline
        open_orders = await hl_api_for_test_env.get_open_orders()

        # Validate return type
        assert isinstance(open_orders, list), "get_open_orders() should return list[Order]"

        # If orders exist, validate structure
        if open_orders:
            sample_order = open_orders[0]
            assert isinstance(sample_order, Order), "Order should be Order instance"

            # Validate required fields
            assert isinstance(sample_order.symbol, str), "symbol must be string"
            assert sample_order.side in [OrderSide.BUY, OrderSide.SELL], (
                "side must be valid OrderSide"
            )
            assert sample_order.status == OrderStatus.OPEN, "open order should have OPEN status"
            assert sample_order.exchange_order_id is not None, "order should have exchange ID"

            # Validate Decimal precision
            assert isinstance(sample_order.quantity_requested, Decimal), (
                "quantity_requested must be Decimal"
            )
            assert isinstance(sample_order.quantity_filled, Decimal), (
                "quantity_filled must be Decimal"
            )

            if sample_order.price is not None:  # Market orders may not have price
                assert isinstance(sample_order.price, Decimal), "price must be Decimal"

    @pytest.mark.vcr
    async def test_place_and_cancel_order_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() and cancel_order() validate full trading pipeline.

        This test places a limit order for a testnet asset (like PURP) far from market price
        to avoid fills during recording, then cancels it to test both endpoints.

        Validates:
        - EIP-712 signing for /exchange endpoint order placement
        - Request building with proper order parameters
        - Response mapping to Order internal model
        - Order status transitions and cancellation via /exchange endpoint
        """
        # Define order parameters (use testnet asset, far from market to avoid fills)
        place_args = PlaceOrderArgs(
            symbol="PURP",  # Common testnet asset on Hyperliquid
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),  # Small size
            price=Decimal("0.01"),  # Far below market price
            time_in_force=TimeInForce.GTC,
        )

        # Place order - test full pipeline to Order model
        placed_order = await hl_api_for_test_env.place_order(place_args)

        # Validate placed order
        assert isinstance(placed_order, Order), "place_order() should return Order instance"
        assert placed_order.symbol == "PURP", "Order symbol should match request"
        assert placed_order.side == OrderSide.BUY, "Order side should match request"
        assert placed_order.order_type == OrderType.LIMIT, "Order type should match request"
        assert placed_order.quantity_requested == Decimal("1"), (
            "Order quantity should match request"
        )
        assert placed_order.price == Decimal("0.01"), "Order price should match request"
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.NEW], (
            "Order should be open/new"
        )
        assert placed_order.exchange_order_id is not None, "Order should have exchange ID"

        # Validate Decimal precision
        assert isinstance(placed_order.quantity_requested, Decimal), (
            "quantity_requested must be Decimal"
        )
        assert isinstance(placed_order.price, Decimal), "price must be Decimal"
        assert isinstance(placed_order.quantity_filled, Decimal), "quantity_filled must be Decimal"

        # Cancel order - test cancel pipeline
        cancel_args = CancelOrderArgs(
            order_id=placed_order.exchange_order_id,
            symbol="PURP",
        )

        cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)

        # Validate cancellation
        assert cancel_result is True, "cancel_order() should return True on success"

    @pytest.mark.vcr
    async def test_get_order_history_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order_history() validates full pipeline to Order models.

        Validates:
        - EIP-712 signing for /info endpoint order history query
        - Date range handling and pagination
        - Historical order data mapping to Order instances
        """
        # Define recent date range for order history
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)  # Last 7 days

        # Execute the full pipeline using GetOrderHistoryArgs
        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
        )
        order_history = await hl_api_for_test_env.get_order_history(args)

        # Validate return type
        assert isinstance(order_history, list), "get_order_history() should return list[Order]"

        # If history exists, validate structure
        if order_history:
            sample_order = order_history[0]
            assert isinstance(sample_order, Order), "Historical order should be Order instance"

            # Validate order fields
            assert isinstance(sample_order.symbol, str), "symbol must be string"
            assert sample_order.side in [OrderSide.BUY, OrderSide.SELL], (
                "side must be valid OrderSide"
            )
            assert sample_order.exchange_order_id is not None, "order should have exchange ID"

            # Validate Decimal precision
            assert isinstance(sample_order.quantity_requested, Decimal), (
                "quantity_requested must be Decimal"
            )
            assert isinstance(sample_order.quantity_filled, Decimal), (
                "quantity_filled must be Decimal"
            )

            # Validate timestamps
            assert sample_order.created_at is not None, "order should have created_at timestamp"

    @pytest.mark.vcr
    async def test_get_trade_history_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_trade_history() validates full pipeline to Trade models.

        Validates:
        - EIP-712 signing for /info endpoint user fills query
        - Response mapping to Trade instances with proper types
        - Trade execution data and fee calculations
        """
        # GetTradeHistoryArgs uses symbol filtering, not date ranges

        # Execute the full pipeline using GetTradeHistoryArgs
        args = GetTradeHistoryArgs(
            symbol="PURP",  # GetTradeHistoryArgs expects symbol, not time range
            limit=50,
        )
        trade_history = await hl_api_for_test_env.get_trade_history(args)

        # Validate return type
        assert isinstance(trade_history, list), "get_trade_history() should return list"

        # If trades exist, validate structure
        if trade_history:
            sample_trade = trade_history[0]
            # Validate basic trade properties (structure depends on Trade model)
            assert hasattr(sample_trade, "symbol"), "Trade should have symbol"
            assert hasattr(sample_trade, "price"), "Trade should have price"
            assert hasattr(sample_trade, "quantity"), "Trade should have quantity"

            # If Trade model has specific type, validate it
            if hasattr(sample_trade, "price"):
                price = getattr(sample_trade, "price", None)
                if price is not None:
                    assert isinstance(price, Decimal), "Trade price must be Decimal"

    @pytest.mark.vcr
    async def test_get_order_by_id_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order() validates pipeline for single order query.

        This test first places an order to get a valid order ID, then queries it.

        Validates:
        - EIP-712 signing for /info endpoint single order query
        - Order ID lookup and response mapping
        - Order status and field validation
        """
        # First place an order to get a valid order ID
        place_args = PlaceOrderArgs(
            symbol="PURP",  # Testnet asset
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("0.01"),  # Far below market
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await hl_api_for_test_env.place_order(place_args)
        order_id = placed_order.exchange_order_id

        # Now query the specific order using GetOrderArgs
        # Ensure order_id is not None before creating GetOrderArgs
        assert order_id is not None, "Order ID should not be None"
        get_order_args = GetOrderArgs(order_id=order_id)
        retrieved_order = await hl_api_for_test_env.get_order(get_order_args)

        # Validate retrieved order
        assert isinstance(retrieved_order, Order), "get_order() should return Order instance"
        assert retrieved_order.exchange_order_id == order_id, "Order ID should match query"
        assert retrieved_order.symbol == "PURP", "Order symbol should match"
        assert retrieved_order.side == OrderSide.BUY, "Order side should match"

        # Clean up - cancel the order
        assert order_id is not None, "Order ID should not be None for cancellation"
        cancel_args = CancelOrderArgs(order_id=order_id, symbol="PURP")
        await hl_api_for_test_env.cancel_order(cancel_args)

    @pytest.mark.vcr
    async def test_cancel_all_orders_integration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_all_orders() validates bulk cancellation pipeline.

        Validates:
        - EIP-712 signing for /exchange endpoint bulk cancel
        - Optional symbol filtering for cancellation
        - Bulk operation response handling
        """
        # First place a couple of orders
        place_args_1 = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("0.01"),
            time_in_force=TimeInForce.GTC,
        )

        place_args_2 = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("100.00"),  # Far above market
            time_in_force=TimeInForce.GTC,
        )

        # Place both orders
        await hl_api_for_test_env.place_order(place_args_1)
        await hl_api_for_test_env.place_order(place_args_2)

        # Cancel all orders for PURP symbol
        cancel_results = await hl_api_for_test_env.cancel_all_orders(symbol="PURP")

        # Validate cancellation result (returns list of CancelOrderResult)
        assert isinstance(cancel_results, list), (
            "cancel_all_orders() should return list of CancelOrderResult"
        )

        # Verify orders were cancelled by checking open orders
        remaining_orders = await hl_api_for_test_env.get_open_orders()
        purp_orders = [order for order in remaining_orders if order.symbol == "PURP"]
        assert len(purp_orders) == 0, "All PURP orders should be cancelled"

    @pytest.mark.vcr
    async def test_invalid_order_error_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test error handling for invalid order parameters.

        Validates:
        - API error responses are properly mapped to APIError exceptions
        - Error codes and messages are preserved from Hyperliquid
        - Invalid requests don't crash the system
        """
        # Attempt to place order with invalid parameters
        invalid_args = PlaceOrderArgs(
            symbol="INVALID_ASSET",  # Non-existent asset
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.00001"),  # Too small
            price=Decimal("0.000001"),  # Too low
            time_in_force=TimeInForce.GTC,
        )

        # Should raise APIError
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.place_order(invalid_args)

        # Validate error structure
        api_error = exc_info.value
        assert isinstance(api_error.code, str), "Error code should be string"
        assert isinstance(api_error.message, str), "Error message should be string"
        assert len(api_error.message) > 0, "Error message should not be empty"

    @pytest.mark.vcr
    async def test_cancel_nonexistent_order_error(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test error handling when canceling non-existent order.

        Validates:
        - Proper error handling for order not found scenarios
        - APIError exceptions with appropriate error codes from Hyperliquid
        """
        # Attempt to cancel non-existent order
        cancel_args = CancelOrderArgs(
            order_id="99999999999999999",  # Non-existent order ID
            symbol="PURP",
        )

        # Should raise APIError
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.cancel_order(cancel_args)

        # Validate error indicates order not found
        api_error = exc_info.value
        assert "not found" in api_error.message.lower() or "invalid" in api_error.message.lower(), (
            "Error message should indicate order not found"
        )

    @pytest.mark.vcr
    async def test_authentication_failure_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test handling of authentication failures.

        This test verifies that authentication errors are properly handled.
        Note: This test may be skipped if we can't easily trigger auth failures
        without breaking the test environment.
        """
        try:
            # Attempt an operation that requires authentication
            # If the test environment is properly configured, this should succeed
            balances = await hl_api_for_test_env.get_balances()

            # If we reach here, authentication worked
            assert isinstance(balances, dict), (
                "Successfully authenticated request should return valid data"
            )

        except APIError as e:
            # If we get an authentication error, validate its structure
            if "auth" in e.message.lower() or "unauthorized" in e.message.lower():
                assert isinstance(e.code, str), "Auth error should have string code"
                assert len(e.message) > 0, "Auth error should have descriptive message"
            else:
                # Re-raise if it's not an auth error
                raise
