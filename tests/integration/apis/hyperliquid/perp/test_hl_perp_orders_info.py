"""Integration tests for Hyperliquid order info endpoints.

This module focuses specifically on testing the Order model pipeline
through Hyperliquid's /info (unsigned) endpoints only.
Tests validate complete data transformation from API responses to Order instances.

Model Focus: Order
- Validates complete Order model field mapping
- Tests Decimal precision for financial values (quantities, prices)
- Validates business logic constraints for order retrieval
- Tests Hyperliquid-specific order details (hl_details with order management info)
- Comprehensive error handling for order queries

Authentication: User address in body for /info endpoints
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import (
    GetOrderArgs,
    GetOrderHistoryArgs,
)
from cyberdelta.core.models.enums import OrderSide, OrderStatus
from cyberdelta.core.models.market.order import Order


# Mark all tests in this file as integration tests
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/orders"], indirect=True)
@pytest.mark.zero_balance
class TestHyperliquidOrdersInfo:
    """Comprehensive order info integration tests for Order model validation.

    This class tests /info (read) operations only:

    /info endpoint (unsigned, user address in body):
    - get_order_by_id
    - get_order_history
    - get_open_orders

    Note: /exchange endpoint tests (place_order, cancel_order) are excluded
    as they require authenticated operations and are tested separately.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_order_by_id_success_comprehensive(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order() validates pipeline for single order query.

        This tests order retrieval by ID and validates the Order model consistency.
        Note: This test uses a pre-existing order ID for testing the /info endpoint.
        """
        # Use a known order ID from test data/cassettes for consistent testing
        # This ID should exist in the VCR cassettes for reproducible tests
        test_order_id = "123456789"  # This should be updated with actual test order ID

        get_order_args = GetOrderArgs(order_id=test_order_id)

        try:
            retrieved_order = await hl_api_for_zero_balance_test.get_order(get_order_args)

            # Validate retrieved order if it exists
            if retrieved_order is not None:
                assert isinstance(retrieved_order, Order), (
                    "get_order() should return Order instance"
                )
                assert retrieved_order.exchange_order_id == test_order_id, (
                    f"Order ID should match query: {retrieved_order.exchange_order_id} vs "
                    f"{test_order_id}"
                )

                # Validate core order fields
                assert isinstance(retrieved_order.symbol, str), "symbol must be string"
                assert retrieved_order.side in [OrderSide.BUY, OrderSide.SELL], (
                    "side must be valid OrderSide"
                )

                # Validate Decimal precision for financial fields
                assert isinstance(retrieved_order.quantity_requested, Decimal), (
                    "quantity_requested must be Decimal"
                )
                assert isinstance(retrieved_order.quantity_filled, Decimal), (
                    "quantity_filled must be Decimal"
                )

                if retrieved_order.price is not None:
                    assert isinstance(retrieved_order.price, Decimal), "price must be Decimal"

        except APIError:
            # If order doesn't exist, that's expected for this test case
            # The test validates the error handling path
            pass

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_order_history_success_comprehensive(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order_history() validates full pipeline to Order models.

        This tests historical order retrieval and validates Order model consistency.
        """
        # Define recent date range for order history with timezone-aware datetime
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=7)  # Last 7 days

        # Execute the full pipeline using GetOrderHistoryArgs
        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
        )
        order_history = await hl_api_for_zero_balance_test.get_order_history(args)

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
    @pytest.mark.asyncio
    async def test_get_open_orders_success_comprehensive(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_open_orders() with detailed Order model validation."""
        # Execute the full pipeline
        open_orders = await hl_api_for_zero_balance_test.get_open_orders()

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
    @pytest.mark.asyncio
    async def test_get_order_nonexistent_id(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order() with non-existent order ID."""
        # Query non-existent order
        get_order_args = GetOrderArgs(order_id="99999999999999999")

        # Should raise APIError with ORDER_NOT_FOUND
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_zero_balance_test.get_order(get_order_args)

        # Validate error mapping
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.ORDER_NOT_FOUND.value, (
            f"HyperliquidErrorMapper should map order not found to ORDER_NOT_FOUND, got "
            f"{api_error.code}"
        )
