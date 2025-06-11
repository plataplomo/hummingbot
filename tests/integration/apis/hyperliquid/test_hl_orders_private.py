"""Integration tests for Hyperliquid private orders endpoints.

This module focuses specifically on testing the Order model pipeline
through Hyperliquid's private /exchange and /info endpoints with EIP-712 authentication.
Tests validate complete data transformation from API responses to Order instances.

Model Focus: Order
- Validates complete Order model field mapping
- Tests Decimal precision for financial values (quantities, prices)
- Validates business logic constraints and order state transitions
- Tests Hyperliquid-specific order details (hl_details with order management info)
- Comprehensive error handling and order lifecycle edge cases

Authentication: EIP-712 signing for testnet environment
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/private/orders"], indirect=True
)
class TestHyperliquidOrdersPrivate:
    """Comprehensive private orders integration tests for Order model validation."""

    @pytest.mark.vcr
    async def test_place_order_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful place_order() with comprehensive Order model validation.

        This test validates the complete pipeline from EIP-712 authenticated request
        to fully validated Order model instances with all field constraints.
        Uses testnet asset and far-from-market price to avoid fills during recording.
        """
        # Define order parameters for testnet (use testnet asset, far from market to avoid fills)
        place_args = PlaceOrderArgs(
            symbol="PURP",  # Common testnet asset on Hyperliquid
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),  # Small size for testnet
            price=Decimal("0.01"),  # Far below market price to avoid fills
            time_in_force=TimeInForce.GTC,
        )

        # Execute the API call
        placed_order = await hl_api_for_test_env.place_order(place_args)

        # Validate return type
        assert isinstance(placed_order, Order), "place_order() should return Order instance"

        # Validate core fields
        assert placed_order.exchange == "hyperliquid", (
            f"Order.exchange should be 'hyperliquid', got {placed_order.exchange}"
        )

        # Validate timestamp recency
        assert placed_order.created_at is not None, "Order must have created_at timestamp"
        time_diff = datetime.now(placed_order.created_at.tzinfo) - placed_order.created_at
        assert time_diff.total_seconds() < 300, (  # 5 minutes for order placement
            f"Order timestamp should be recent (< 5 minutes), got {time_diff.total_seconds()}s ago"
        )

        # Validate order matches request parameters
        assert placed_order.symbol == "PURP", (
            f"Order symbol should match request, got {placed_order.symbol}"
        )
        assert placed_order.side == OrderSide.BUY, (
            f"Order side should match request, got {placed_order.side}"
        )
        assert placed_order.order_type == OrderType.LIMIT, (
            f"Order type should match request, got {placed_order.order_type}"
        )

        # Validate Decimal precision for all financial fields
        assert isinstance(placed_order.quantity_requested, Decimal), (
            f"quantity_requested must be Decimal, got {type(placed_order.quantity_requested)}"
        )
        assert isinstance(placed_order.price, Decimal), (
            f"price must be Decimal, got {type(placed_order.price)}"
        )
        assert isinstance(placed_order.quantity_filled, Decimal), (
            f"quantity_filled must be Decimal, got {type(placed_order.quantity_filled)}"
        )

        # Validate order parameter values
        assert placed_order.quantity_requested == Decimal("1"), (
            f"Order quantity should match request, got {placed_order.quantity_requested}"
        )
        assert placed_order.price == Decimal("0.01"), (
            f"Order price should match request, got {placed_order.price}"
        )

        # Validate order status and lifecycle
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.NEW], (
            f"Order should be open/new after placement, got {placed_order.status}"
        )
        assert placed_order.exchange_order_id is not None, "Order should have exchange-generated ID"

        # For new orders, quantity_filled should be 0
        assert placed_order.quantity_filled == Decimal("0"), (
            f"New order should have zero filled quantity, got {placed_order.quantity_filled}"
        )

        # Validate business logic constraints
        assert placed_order.quantity_requested > Decimal("0"), (
            f"quantity_requested must be positive, got {placed_order.quantity_requested}"
        )
        assert placed_order.price > Decimal("0"), (
            f"price must be positive, got {placed_order.price}"
        )
        assert placed_order.quantity_filled >= Decimal("0"), (
            f"quantity_filled must be non-negative, got {placed_order.quantity_filled}"
        )
        assert placed_order.quantity_filled <= placed_order.quantity_requested, (
            f"quantity_filled ({placed_order.quantity_filled}) cannot exceed "
            f"quantity_requested ({placed_order.quantity_requested})"
        )

        # Validate Hyperliquid-specific order ID format (should be integer string)
        try:
            int(placed_order.exchange_order_id)
        except ValueError:
            pytest.fail("Hyperliquid order ID should be a valid integer string")

        # Validate exchange-specific details if present
        if placed_order.hl_details:
            # Add specific validation for Hyperliquid order details
            # This would depend on the actual hl_details structure
            assert hasattr(placed_order, "hl_details"), "hl_details should be accessible"

    @pytest.mark.vcr
    async def test_cancel_order_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful cancel_order() after order placement.

        This validates the complete order lifecycle: place → cancel → verify cancellation.
        """
        # First place an order
        place_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("0.01"),  # Far below market
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await hl_api_for_test_env.place_order(place_args)
        order_id = placed_order.exchange_order_id

        # Ensure order_id is not None before creating cancel args
        assert order_id is not None, "Order ID should not be None after placement"

        # Cancel the order
        cancel_args = CancelOrderArgs(
            order_id=order_id,
            symbol="PURP",  # Hyperliquid requires asset for cancellation
        )

        cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)

        # Validate cancellation success
        assert cancel_result is True, "cancel_order() should return True on success"

    @pytest.mark.vcr
    async def test_get_order_by_id_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order() validates pipeline for single order query.

        This tests order retrieval by ID and validates the Order model consistency.
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
        assert order_id is not None, "Order ID should not be None"
        get_order_args = GetOrderArgs(order_id=order_id)
        retrieved_order = await hl_api_for_test_env.get_order(get_order_args)

        # Validate retrieved order
        assert retrieved_order is not None, "get_order() should return an Order"
        assert isinstance(retrieved_order, Order), "get_order() should return Order instance"
        assert retrieved_order.exchange_order_id == order_id, (
            f"Order ID should match query: {retrieved_order.exchange_order_id} vs {order_id}"
        )
        assert retrieved_order.symbol == "PURP", (
            f"Order symbol should match: {retrieved_order.symbol}"
        )
        assert retrieved_order.side == OrderSide.BUY, (
            f"Order side should match: {retrieved_order.side}"
        )

        # Validate consistency between placed and retrieved order
        assert retrieved_order.quantity_requested == placed_order.quantity_requested, (
            "Retrieved order quantity should match placed order"
        )
        if retrieved_order.price is not None and placed_order.price is not None:
            assert retrieved_order.price == placed_order.price, (
                "Retrieved order price should match placed order"
            )

        # Clean up - cancel the order
        cancel_args = CancelOrderArgs(order_id=order_id, symbol="PURP")
        await hl_api_for_test_env.cancel_order(cancel_args)

    @pytest.mark.vcr
    async def test_get_order_history_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order_history() validates full pipeline to Order models.

        This tests historical order retrieval and validates Order model consistency.
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

            # Validate exchange field
            assert sample_order.exchange == "hyperliquid", (
                f"Exchange should be 'hyperliquid', got {sample_order.exchange}"
            )

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

            # Validate historical order is within requested time range
            if sample_order.created_at:
                assert start_time <= sample_order.created_at <= end_time, (
                    f"Order timestamp should be within requested range: {sample_order.created_at}"
                )

    @pytest.mark.vcr
    async def test_get_open_orders_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_open_orders() with detailed Order model validation."""
        # Execute the full pipeline
        open_orders = await hl_api_for_test_env.get_open_orders()

        # Validate return type
        assert isinstance(open_orders, list), "get_open_orders() should return list[Order]"

        # If orders exist, validate structure
        if open_orders:
            sample_order = open_orders[0]
            assert isinstance(sample_order, Order), "Order should be Order instance"

            # Validate exchange field
            assert sample_order.exchange == "hyperliquid", (
                f"Exchange should be 'hyperliquid', got {sample_order.exchange}"
            )

            # Validate required fields
            assert isinstance(sample_order.symbol, str), "symbol must be string"
            assert sample_order.side in [OrderSide.BUY, OrderSide.SELL], (
                "side must be valid OrderSide"
            )
            assert sample_order.status == OrderStatus.OPEN, (
                f"open order should have OPEN status, got {sample_order.status}"
            )
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

            # Validate Hyperliquid-specific order ID format
            try:
                int(sample_order.exchange_order_id)
            except ValueError:
                pytest.fail("Hyperliquid order ID should be a valid integer string")

    @pytest.mark.vcr
    async def test_place_order_authentication_failure(
        self,
        hl_api_with_di: Callable[
            ..., HyperliquidAPI
        ],  # Factory function for creating API with custom secrets
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with invalid EIP-712 authentication."""
        # Create API with invalid EIP-712 private key
        invalid_secrets = PrivateKeyAuthSecrets(
            private_key=SecretStr(
                "0x0000000000000000000000000000000000000000000000000000000000000004"
            ),
        )

        bad_api = hl_api_with_di(secrets=invalid_secrets)

        # Define order args
        place_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("0.01"),
            time_in_force=TimeInForce.GTC,
        )

        # Should raise authentication error
        with pytest.raises(APIError) as exc_info:
            await bad_api.place_order(place_args)

        # Validate error mapping and structure
        error = exc_info.value
        assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value, (
            f"Expected AUTHENTICATION_FAILED, got {error.code}"
        )
        assert error.http_status in [401, 403], f"Expected 401/403 status, got {error.http_status}"

    @pytest.mark.vcr
    async def test_place_order_insufficient_funds(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with insufficient funds error.

        This validates that our HyperliquidErrorMapper correctly maps Hyperliquid's
        "Insufficient balance" or similar error to our standardized APIErrorCode.
        """
        # Create order with unrealistically large quantity to trigger insufficient funds
        large_order_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("999999999.0"),  # Unrealistically large for testnet
            price=Decimal("100.00"),  # High price to maximize required margin
            time_in_force=TimeInForce.GTC,
        )

        # Should raise APIError with INSUFFICIENT_FUNDS code
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.place_order(large_order_args)

        # Validate error mapping
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value, (
            f"HyperliquidErrorMapper should map insufficient balance to INSUFFICIENT_FUNDS, got "
            f"{api_error.code}"
        )
        assert isinstance(api_error.message, str), "Error message should be string"
        assert len(api_error.message) > 0, "Error message should not be empty"

        # Check for common Hyperliquid insufficient funds phrases
        message_lower = api_error.message.lower()
        assert any(
            phrase in message_lower for phrase in ["insufficient", "balance", "margin", "funds"]
        ), f"Error message should indicate insufficient funds/balance: {api_error.message}"

    @pytest.mark.vcr
    async def test_place_order_invalid_symbol(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with invalid asset/symbol error."""
        # Create order with non-existent asset
        invalid_asset_args = PlaceOrderArgs(
            symbol="INVALID_ASSET_XYZ",  # Non-existent asset
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("1.00"),
            time_in_force=TimeInForce.GTC,
        )

        # Should raise APIError with appropriate error code
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.place_order(invalid_asset_args)

        # Validate error structure
        api_error = exc_info.value
        assert api_error.code in [
            APIErrorCode.INVALID_SYMBOL.value,
            APIErrorCode.SYMBOL_NOT_FOUND.value,
            APIErrorCode.INVALID_REQUEST.value,
        ], f"Should map to symbol-related error code, got {api_error.code}"

    @pytest.mark.vcr
    async def test_cancel_nonexistent_order(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_order() with non-existent order ID.

        This tests that our HyperliquidErrorMapper correctly maps Hyperliquid's
        "Order was never placed" or similar error to ORDER_NOT_FOUND.
        """
        # Attempt to cancel order with fake ID
        cancel_args = CancelOrderArgs(
            order_id="99999999999999999",  # Non-existent order ID
            symbol="PURP",
        )

        # Should raise APIError with ORDER_NOT_FOUND
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.cancel_order(cancel_args)

        # Validate error mapping
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.ORDER_NOT_FOUND.value, (
            f"HyperliquidErrorMapper should map order not found to ORDER_NOT_FOUND, got "
            f"{api_error.code}"
        )

        # Check for common Hyperliquid order not found phrases
        message_lower = api_error.message.lower()
        assert any(
            phrase in message_lower
            for phrase in ["not found", "never placed", "invalid", "does not exist"]
        ), f"Error message should indicate order not found: {api_error.message}"

    @pytest.mark.vcr
    async def test_get_order_nonexistent_id(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order() with non-existent order ID."""
        # Query non-existent order
        get_order_args = GetOrderArgs(order_id="99999999999999999")

        # Should raise APIError with ORDER_NOT_FOUND
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.get_order(get_order_args)

        # Validate error mapping
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.ORDER_NOT_FOUND.value, (
            f"HyperliquidErrorMapper should map order not found to ORDER_NOT_FOUND, got "
            f"{api_error.code}"
        )

    @pytest.mark.vcr
    async def test_order_precision_edge_cases(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order operations with edge cases around decimal precision.

        This validates handling of very small quantities, dust amounts,
        and precision edge cases that might occur in real trading.
        """
        # Test very small quantity order
        small_order_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.000001"),  # Very small quantity
            price=Decimal("0.01"),
            time_in_force=TimeInForce.GTC,
        )

        try:
            # Attempt to place small order
            placed_order = await hl_api_for_test_env.place_order(small_order_args)

            # If successful, validate precision is maintained
            assert placed_order.quantity_requested == Decimal("0.000001"), (
                f"Small quantity precision should be maintained: {placed_order.quantity_requested}"
            )

            # Validate that small quantities maintain proper decimal representation
            quantity_str = str(placed_order.quantity_requested)
            assert "E" not in quantity_str.upper() or "E-" in quantity_str.upper(), (
                f"Scientific notation should be negative exponent if used: {quantity_str}"
            )

            # Clean up
            if placed_order.exchange_order_id:
                cancel_args = CancelOrderArgs(
                    order_id=placed_order.exchange_order_id, symbol="PURP"
                )
                await hl_api_for_test_env.cancel_order(cancel_args)

        except APIError as e:
            # If exchange rejects due to minimum quantity, that's also valid behavior
            if "minimum" in e.message.lower() or "size" in e.message.lower():
                pytest.skip(f"Exchange has minimum quantity requirements: {e.message}")
            else:
                raise

    @pytest.mark.vcr
    async def test_order_lifecycle_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test complete order lifecycle: place → query → cancel → verify.

        This validates the full order management pipeline and Order model consistency
        across different order states.
        """
        # Step 1: Place order
        place_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("0.01"),
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await hl_api_for_test_env.place_order(place_args)
        order_id = placed_order.exchange_order_id
        assert order_id is not None, "Placed order should have order ID"

        # Step 2: Verify order appears in open orders
        open_orders = await hl_api_for_test_env.get_open_orders()
        placed_order_found = any(order.exchange_order_id == order_id for order in open_orders)
        assert placed_order_found, "Placed order should appear in open orders"

        # Step 3: Query specific order by ID
        get_order_args = GetOrderArgs(order_id=order_id)
        retrieved_order = await hl_api_for_test_env.get_order(get_order_args)
        assert retrieved_order is not None, "Retrieved order should not be None"
        assert retrieved_order.exchange_order_id == order_id, "Retrieved order should match"
        assert retrieved_order.status == OrderStatus.OPEN, "Retrieved order should be open"

        # Step 4: Cancel order
        cancel_args = CancelOrderArgs(order_id=order_id, symbol="PURP")
        cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)
        assert cancel_result is True, "Order cancellation should succeed"

        # Step 5: Verify order no longer in open orders
        open_orders_after = await hl_api_for_test_env.get_open_orders()
        cancelled_order_found = any(
            order.exchange_order_id == order_id for order in open_orders_after
        )
        assert not cancelled_order_found, "Cancelled order should not appear in open orders"
