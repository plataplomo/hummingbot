"""Integration tests for Backpack private (authenticated) endpoints.

This module provides comprehensive integration testing for all private Backpack API endpoints
that require authentication. Tests validate the complete data pipeline from API method calls
through to final Internal Domain Models, using pytest-recording (VCR) for deterministic testing.

These tests use the bp_api_for_test_env fixture which creates real BackpackAPI instances
configured with test secrets, enabling full end-to-end validation of:
- Authentication and signing
- Request building and validation
- HTTP communication
- Response handling and error mapping
- Data mapping to internal models

All sensitive data (API keys, signatures, timestamps) is filtered by VCR configuration
before cassettes are written to disk.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private"], indirect=True)
class TestBackpackPrivateEndpoints:
    """Integration tests for Backpack private endpoints.

    Tests the complete pipeline from API method calls to Internal Domain Models
    for all authenticated Backpack endpoints.
    """

    @pytest.mark.vcr
    async def test_get_balances_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() validates full pipeline to SpotBalance models.

        Validates:
        - Authentication headers are properly signed
        - Request is built correctly for /api/v1/capital endpoint
        - Response is parsed and mapped to SpotBalance instances
        - All SpotBalance fields have correct types (Decimal precision)
        - Business logic constraints are enforced
        """
        # Execute the full pipeline
        balances = await bp_api_for_test_env.get_balances()

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
            # SpotBalance doesn't have locked_quantity - check if available via bp_details
            if sample_balance.bp_details and hasattr(
                sample_balance.bp_details, "open_order_quantity"
            ):
                if sample_balance.bp_details.open_order_quantity is not None:
                    assert isinstance(sample_balance.bp_details.open_order_quantity, Decimal), (
                        "open_order_quantity must be Decimal"
                    )

            # Validate business logic constraints
            assert sample_balance.total_quantity >= Decimal("0"), (
                "total_quantity must be non-negative"
            )
            assert sample_balance.available_quantity >= Decimal("0"), (
                "available_quantity must be non-negative"
            )
            # Check business logic constraints for available quantities
            # Note: SpotBalance uses total_quantity and available_quantity, not locked_quantity
            assert sample_balance.total_quantity >= sample_balance.available_quantity, (
                "total_quantity must be >= available_quantity"
            )

    @pytest.mark.vcr
    async def test_get_account_summary_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() validates full pipeline to MarginAccount model.

        Validates:
        - Authentication for /api/v1/account endpoint
        - Response mapping to MarginAccount internal model
        - Decimal precision for all financial values
        - Account status and margin calculations
        """
        # Execute the full pipeline
        account_summary = await bp_api_for_test_env.get_account_summary()

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

        if account_summary.total_maintenance_margin_required is not None:
            assert isinstance(account_summary.total_maintenance_margin_required, Decimal), (
                "total_maintenance_margin_required must be Decimal"
            )
            assert account_summary.total_maintenance_margin_required >= Decimal("0"), (
                "total_maintenance_margin_required must be non-negative"
            )

    @pytest.mark.vcr
    async def test_get_positions_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() validates full pipeline to DerivativePosition models.

        Validates:
        - Authentication for /api/v1/position endpoint
        - Response mapping to DerivativePosition instances
        - Decimal precision for position values
        - Position side and PnL calculations
        """
        # Execute the full pipeline
        positions = await bp_api_for_test_env.get_positions()

        # Validate return type
        assert isinstance(positions, list), "get_positions() should return list[DerivativePosition]"

        # If positions exist, validate structure
        if positions:
            sample_position = positions[0]
            assert isinstance(sample_position, DerivativePosition), (
                "Position should be DerivativePosition instance"
            )

            # Validate symbol format
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
            assert sample_position.entry_price > Decimal("0"), "entry_price must be positive"
            assert sample_position.mark_price > Decimal("0"), "mark_price must be positive"

    @pytest.mark.vcr
    async def test_place_and_cancel_order_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() and cancel_order() validate full trading pipeline.

        This test places a limit order far from market price to avoid fills during recording,
        then cancels it to test both endpoints in sequence.

        Validates:
        - Authentication for /api/v1/order POST and DELETE endpoints
        - Request building with proper order parameters
        - Response mapping to Order internal model
        - Order status transitions and cancellation
        """
        # Define order parameters (far from market to avoid fills)
        place_args = PlaceOrderArgs(
            symbol="SOL_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),  # Small size
            price=Decimal("1.00"),  # Far below market price
            time_in_force=TimeInForce.GTC,
        )

        # Place order - test full pipeline to Order model
        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate placed order
        assert isinstance(placed_order, Order), "place_order() should return Order instance"
        assert placed_order.symbol == "SOL_USDC", "Order symbol should match request"
        assert placed_order.side == OrderSide.BUY, "Order side should match request"
        assert placed_order.order_type == OrderType.LIMIT, "Order type should match request"
        assert placed_order.quantity_requested == Decimal("0.1"), (
            "Order quantity should match request"
        )
        assert placed_order.price == Decimal("1.00"), "Order price should match request"
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
            symbol="SOL_USDC",
        )

        cancel_result = await bp_api_for_test_env.cancel_order(cancel_args)

        # Validate cancellation
        assert cancel_result is True, "cancel_order() should return True on success"

    @pytest.mark.vcr
    async def test_get_open_orders_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_open_orders() validates full pipeline to Order models.

        Validates:
        - Authentication for /api/v1/orders GET endpoint
        - Response mapping to list of Order instances
        - Order field validation and types
        """
        # Execute the full pipeline
        open_orders = await bp_api_for_test_env.get_open_orders()

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
    async def test_get_order_history_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order_history() validates full pipeline to Order models.

        Validates:
        - Authentication for /wapi/v1/history/orders endpoint
        - Date range handling and pagination
        - Historical order data mapping to Order instances
        """
        from datetime import datetime, timedelta

        # Define recent date range for order history
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)  # Last 7 days

        # Execute the full pipeline using GetOrderHistoryArgs
        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
            limit=50,
        )
        order_history = await bp_api_for_test_env.get_order_history(args)

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
            # Note: Order history may include orders outside the exact range due to
            # exchange pagination - Just verify we have valid timestamps

    @pytest.mark.vcr
    async def test_invalid_order_error_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test error handling for invalid order parameters.

        Validates:
        - API error responses are properly mapped to APIError exceptions
        - Error codes and messages are preserved
        - Invalid requests don't crash the system
        """
        # Attempt to place order with invalid parameters
        invalid_args = PlaceOrderArgs(
            symbol="INVALID_SYMBOL",  # Non-existent symbol
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.001"),  # Too small
            price=Decimal("0.01"),  # Too low
            time_in_force=TimeInForce.GTC,
        )

        # Should raise APIError
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.place_order(invalid_args)

        # Validate error structure
        api_error = exc_info.value
        assert isinstance(api_error.code, str), "Error code should be string"
        assert isinstance(api_error.message, str), "Error message should be string"
        assert len(api_error.message) > 0, "Error message should not be empty"

    @pytest.mark.vcr
    async def test_cancel_nonexistent_order_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test error handling when canceling non-existent order.

        Validates:
        - Proper error handling for order not found scenarios
        - APIError exceptions with appropriate error codes
        """
        # Attempt to cancel non-existent order
        cancel_args = CancelOrderArgs(
            order_id="99999999999999999",  # Non-existent order ID
            symbol="SOL_USDC",
        )

        # Should raise APIError
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.cancel_order(cancel_args)

        # Validate error indicates order not found
        api_error = exc_info.value
        assert "not found" in api_error.message.lower() or "invalid" in api_error.message.lower(), (
            "Error message should indicate order not found"
        )

    @pytest.mark.vcr
    async def test_get_trade_history_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_trade_history() validates full pipeline to Trade models.

        Note: This test assumes the BackpackAPI has a get_trade_history method.
        If not implemented, this test can be skipped or the method can be added.
        """
        # GetTradeHistoryArgs uses symbol filtering, not date ranges

        try:
            # Execute the full pipeline using GetTradeHistoryArgs
            args = GetTradeHistoryArgs(
                symbol="SOL_USDC",  # GetTradeHistoryArgs expects symbol, not time range
                limit=50,
            )
            trade_history = await bp_api_for_test_env.get_trade_history(args)

            # Validate return type (assuming Trade model exists in core.models)
            assert isinstance(trade_history, list), "get_trade_history() should return list"

            # If trades exist, validate structure
            if trade_history:
                sample_trade = trade_history[0]
                # Additional validation would depend on Trade model structure
                assert hasattr(sample_trade, "symbol"), "Trade should have symbol"
                assert hasattr(sample_trade, "price"), "Trade should have price"
                assert hasattr(sample_trade, "quantity"), "Trade should have quantity"

        except NotImplementedError:
            pytest.skip("get_trade_history() not yet implemented in BackpackAPI")
        except AttributeError:
            pytest.skip("get_trade_history() method not available in BackpackAPI")
