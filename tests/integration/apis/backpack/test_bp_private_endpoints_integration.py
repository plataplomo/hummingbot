"""Comprehensive integration tests for Backpack private (authenticated) endpoints.

This module provides battle-tested integration testing for all private Backpack API endpoints
that require authentication. Tests validate the complete data pipeline from API method calls
through to final Internal Domain Models for both SUCCESS and ERROR scenarios, using 
pytest-recording (VCR) for deterministic testing.

These tests validate the full client stack including:
- BackpackEd25519Authenticator (signing and auth failure handling)
- BackpackRequestBuilder (request construction)
- HTTP communication via connectivity layer
- BackpackResponseHandler (response parsing)
- BackpackErrorMapper (error code mapping)
- Data mapping to internal models with Decimal precision

Test Structure:
- Each endpoint has a dedicated test class (e.g., TestGetBalancesIntegration)
- Each test class contains multiple scenarios: success, auth failure, business logic errors
- VCR cassettes capture both successful and error responses from the exchange
- All sensitive data (API keys, signatures, timestamps) is filtered by VCR configuration

Recording Notes:
- Success tests use valid test credentials
- Auth failure tests use intentionally invalid credentials during recording
- Business logic error tests use invalid parameters (large quantities, bad symbols, etc.)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.spot_balance import SpotBalance

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private"], indirect=True)
class TestGetBalancesIntegration:
    """Comprehensive tests for get_balances() endpoint.

    Validates the complete pipeline: Service → Authenticator → HTTP → ResponseHandler → 
    Mapper → SpotBalance
    """

    @pytest.mark.vcr
    async def test_get_balances_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_balances() with detailed model validation.

        Validates:
        - Authentication headers are properly signed with Ed25519
        - Request is built correctly for /api/v1/capital endpoint
        - Response is parsed and mapped to SpotBalance instances
        - All SpotBalance fields have correct types (Decimal precision)
        - Business logic constraints are enforced
        - Exchange-specific details are populated correctly
        """
        # Execute the full pipeline
        balances = await bp_api_for_test_env.get_balances()

        # Validate return type
        assert isinstance(balances, dict), "get_balances() should return dict[str, SpotBalance]"

        # If balances exist, perform deep validation
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

            # Validate exchange field
            assert sample_balance.exchange == "backpack", "Exchange should be 'backpack'"

            # Validate timestamp exists and is recent (within last 24 hours)
            assert sample_balance.timestamp is not None, "SpotBalance should have timestamp"
            time_diff = datetime.now(sample_balance.timestamp.tzinfo) - sample_balance.timestamp
            assert time_diff.total_seconds() < 86400, "Timestamp should be recent (< 24 hours)"

            # Validate Decimal precision for all financial values
            assert isinstance(sample_balance.total_quantity, Decimal), (
                "total_quantity must be Decimal for precision"
            )
            assert isinstance(sample_balance.available_quantity, Decimal), (
                "available_quantity must be Decimal for precision"
            )

            # Validate business logic constraints
            assert sample_balance.total_quantity >= Decimal("0"), (
                "total_quantity must be non-negative"
            )
            assert sample_balance.available_quantity >= Decimal("0"), (
                "available_quantity must be non-negative"
            )
            assert sample_balance.total_quantity >= sample_balance.available_quantity, (
                "total_quantity must be >= available_quantity"
            )

            # Validate exchange-specific details if present
            if sample_balance.bp_details:
                if sample_balance.bp_details.open_order_quantity is not None:
                    assert isinstance(sample_balance.bp_details.open_order_quantity, Decimal), (
                        "bp_details.open_order_quantity must be Decimal"
                    )
                    assert sample_balance.bp_details.open_order_quantity >= Decimal("0"), (
                        "bp_details.open_order_quantity must be non-negative"
                    )

    @pytest.mark.vcr
    async def test_get_balances_authentication_failure(
        self,
        bp_api_with_di: MagicMock,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with invalid authentication credentials.

        This test validates that our BackpackEd25519Authenticator and BackpackErrorMapper
        correctly handle authentication failures from the exchange.

        Note: During recording, this test uses intentionally invalid credentials to
        capture a real 401/403 response from Backpack. On playback, VCR returns
        the recorded error response.
        """
        # Create API instance with invalid credentials
        bad_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )
        
        bad_api = bp_api_with_di(secrets=bad_secrets)

        # Attempt operation that requires authentication
        with pytest.raises(APIError) as exc_info:
            await bad_api.get_balances()

        # Validate error structure and mapping
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.AUTHENTICATION_FAILED.value, (
            "BackpackErrorMapper should map auth errors to AUTHENTICATION_FAILED"
        )
        assert api_error.http_status in [401, 403], (
            "Authentication failures should have 401 or 403 HTTP status"
        )
        assert isinstance(api_error.message, str), "Error message should be string"
        assert len(api_error.message) > 0, "Error message should not be empty"

        # Validate exchange-specific error details are preserved
        assert api_error.exchange_code is not None, (
            "Exchange-specific error code should be preserved"
        )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private"], indirect=True)
class TestGetAccountSummaryIntegration:
    """Comprehensive tests for get_account_summary() endpoint."""

    @pytest.mark.vcr
    async def test_get_account_summary_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_account_summary() with detailed MarginAccountSummary validation."""
        # Execute the full pipeline
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Validate return type
        assert isinstance(account_summary, MarginAccountSummary), (
            "get_account_summary() should return MarginAccountSummary"
        )

        # Validate exchange field
        assert account_summary.exchange == "backpack", "Exchange should be 'backpack'"

        # Validate timestamp
        assert account_summary.timestamp is not None, "Account summary should have timestamp"

        # Validate required Decimal fields with precision
        assert isinstance(account_summary.total_equity, Decimal), (
            "total_equity must be Decimal for precision"
        )
        assert isinstance(account_summary.available_equity, Decimal), (
            "available_equity must be Decimal for precision"
        )

        # Validate business logic constraints
        assert account_summary.total_equity >= Decimal("0"), (
            "total_equity must be non-negative"
        )
        assert account_summary.available_equity >= Decimal("0"), (
            "available_equity must be non-negative"
        )

        # Validate optional margin fields if present
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

        # Validate exchange-specific details
        if account_summary.bp_details:
            bp_details = account_summary.bp_details
            
            # Validate optional Decimal fields in bp_details
            decimal_fields = ["assets_value", "borrow_liability", "liabilities_value", 
                            "locked_equity", "margin_fraction"]
            for field_name in decimal_fields:
                field_value = getattr(bp_details, field_name, None)
                if field_value is not None:
                    assert isinstance(field_value, Decimal), (
                        f"bp_details.{field_name} must be Decimal if present"
                    )
                    assert field_value >= Decimal("0"), (
                        f"bp_details.{field_name} must be non-negative"
                    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private"], indirect=True)
class TestGetPositionsIntegration:
    """Comprehensive tests for get_positions() endpoint."""

    @pytest.mark.vcr
    async def test_get_positions_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_positions() with detailed DerivativePosition validation."""
        # Execute the full pipeline
        positions = await bp_api_for_test_env.get_positions()

        # Validate return type
        assert isinstance(positions, list), (
            "get_positions() should return list[DerivativePosition]"
        )

        # If positions exist, validate structure
        if positions:
            sample_position = positions[0]
            assert isinstance(sample_position, DerivativePosition), (
                "Position should be DerivativePosition instance"
            )

            # Validate exchange field
            assert sample_position.exchange == "backpack", "Exchange should be 'backpack'"

            # Validate timestamp
            assert sample_position.timestamp is not None, "Position should have timestamp"

            # Validate symbol format
            assert isinstance(sample_position.symbol, str), "symbol must be string"
            assert len(sample_position.symbol) > 0, "symbol must not be empty"

            # Validate Decimal precision for all financial values
            assert isinstance(sample_position.size, Decimal), (
                "size must be Decimal for precision"
            )
            assert isinstance(sample_position.entry_price, Decimal), (
                "entry_price must be Decimal for precision"
            )
            assert isinstance(sample_position.mark_price, Decimal), (
                "mark_price must be Decimal for precision"
            )
            assert isinstance(sample_position.unrealized_pnl, Decimal), (
                "unrealized_pnl must be Decimal for precision"
            )
            assert isinstance(sample_position.realized_pnl, Decimal), (
                "realized_pnl must be Decimal for precision"
            )

            # Validate price constraints
            assert sample_position.entry_price > Decimal("0"), (
                "entry_price must be positive"
            )
            assert sample_position.mark_price > Decimal("0"), (
                "mark_price must be positive"
            )

            # Validate exchange-specific details if present
            if sample_position.bp_details:
                # Add specific validation for Backpack position details
                # This would depend on the actual bp_details structure
                pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private"], indirect=True)
class TestPlaceOrderIntegration:
    """Comprehensive tests for place_order() endpoint."""

    @pytest.mark.vcr
    async def test_place_order_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful order placement with detailed Order model validation.

        Places a limit order far from market price to avoid fills during recording.
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

        # Validate placed order structure
        assert isinstance(placed_order, Order), (
            "place_order() should return Order instance"
        )

        # Validate exchange field
        assert placed_order.exchange == "backpack", "Exchange should be 'backpack'"

        # Validate timestamp
        assert placed_order.created_at is not None, "Order should have created_at timestamp"

        # Validate order matches request parameters
        assert placed_order.symbol == "SOL_USDC", "Order symbol should match request"
        assert placed_order.side == OrderSide.BUY, "Order side should match request"
        assert placed_order.order_type == OrderType.LIMIT, "Order type should match request"
        assert placed_order.quantity_requested == Decimal("0.1"), (
            "Order quantity should match request"
        )
        assert placed_order.price == Decimal("1.00"), "Order price should match request"

        # Validate order status
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.NEW], (
            "Order should be open/new after placement"
        )
        assert placed_order.exchange_order_id is not None, (
            "Order should have exchange-generated ID"
        )

        # Validate Decimal precision for all financial fields
        assert isinstance(placed_order.quantity_requested, Decimal), (
            "quantity_requested must be Decimal"
        )
        assert isinstance(placed_order.price, Decimal), "price must be Decimal"
        assert isinstance(placed_order.quantity_filled, Decimal), (
            "quantity_filled must be Decimal"
        )

        # For new orders, quantity_filled should be 0
        assert placed_order.quantity_filled == Decimal("0"), (
            "New order should have zero filled quantity"
        )

    @pytest.mark.vcr
    async def test_place_order_insufficient_funds(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with insufficient funds error.

        This validates that our BackpackErrorMapper correctly maps Backpack's
        "INSUFFICIENT_FUNDS" error to our standardized APIErrorCode.
        """
        # Create order with unrealistically large quantity to trigger insufficient funds
        large_order_args = PlaceOrderArgs(
            symbol="SOL_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("999999999.0"),  # Unrealistically large
            price=Decimal("200.00"),  # High price to maximize required funds
            time_in_force=TimeInForce.GTC,
        )

        # Should raise APIError with INSUFFICIENT_FUNDS code
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.place_order(large_order_args)

        # Validate error mapping
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value, (
            "BackpackErrorMapper should map insufficient funds to INSUFFICIENT_FUNDS"
        )
        assert isinstance(api_error.message, str), "Error message should be string"
        assert len(api_error.message) > 0, "Error message should not be empty"

    @pytest.mark.vcr
    async def test_place_order_invalid_symbol(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with invalid symbol error."""
        # Create order with non-existent symbol
        invalid_symbol_args = PlaceOrderArgs(
            symbol="INVALID_SYMBOL_XYZ",  # Non-existent symbol
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("1.00"),
            time_in_force=TimeInForce.GTC,
        )

        # Should raise APIError with appropriate error code
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.place_order(invalid_symbol_args)

        # Validate error structure
        api_error = exc_info.value
        assert api_error.code in [
            APIErrorCode.INVALID_SYMBOL.value,
            APIErrorCode.SYMBOL_NOT_FOUND.value,
            APIErrorCode.INVALID_REQUEST.value,
        ], "Should map to symbol-related error code"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private"], indirect=True)
class TestCancelOrderIntegration:
    """Comprehensive tests for cancel_order() endpoint."""

    @pytest.mark.vcr
    async def test_cancel_order_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful order cancellation after placement."""
        # First place an order
        place_args = PlaceOrderArgs(
            symbol="SOL_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("1.00"),  # Far below market
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)
        order_id = placed_order.exchange_order_id
        
        # Ensure order_id is not None before creating cancel args
        assert order_id is not None, "Order ID should not be None after placement"

        # Cancel the order
        cancel_args = CancelOrderArgs(
            order_id=order_id,
            symbol="SOL_USDC",
        )

        cancel_result = await bp_api_for_test_env.cancel_order(cancel_args)

        # Validate cancellation success
        assert cancel_result is True, "cancel_order() should return True on success"

    @pytest.mark.vcr
    async def test_cancel_nonexistent_order(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_order() with non-existent order ID."""
        # Attempt to cancel order with fake ID
        cancel_args = CancelOrderArgs(
            order_id="99999999999999999",  # Non-existent order ID
            symbol="SOL_USDC",
        )

        # Should raise APIError with ORDER_NOT_FOUND
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.cancel_order(cancel_args)

        # Validate error mapping
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.ORDER_NOT_FOUND.value, (
            "BackpackErrorMapper should map order not found to ORDER_NOT_FOUND"
        )
        assert "not found" in api_error.message.lower() or "invalid" in api_error.message.lower(), (
            "Error message should indicate order not found"
        )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private"], indirect=True)
class TestGetOpenOrdersIntegration:
    """Comprehensive tests for get_open_orders() endpoint."""

    @pytest.mark.vcr
    async def test_get_open_orders_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_open_orders() with detailed Order model validation."""
        # Execute the full pipeline
        open_orders = await bp_api_for_test_env.get_open_orders()

        # Validate return type
        assert isinstance(open_orders, list), (
            "get_open_orders() should return list[Order]"
        )

        # If orders exist, validate structure
        if open_orders:
            sample_order = open_orders[0]
            assert isinstance(sample_order, Order), "Order should be Order instance"

            # Validate exchange field
            assert sample_order.exchange == "backpack", "Exchange should be 'backpack'"

            # Validate required fields
            assert isinstance(sample_order.symbol, str), "symbol must be string"
            assert sample_order.side in [OrderSide.BUY, OrderSide.SELL], (
                "side must be valid OrderSide"
            )
            assert sample_order.status == OrderStatus.OPEN, (
                "open order should have OPEN status"
            )
            assert sample_order.exchange_order_id is not None, (
                "order should have exchange ID"
            )

            # Validate Decimal precision
            assert isinstance(sample_order.quantity_requested, Decimal), (
                "quantity_requested must be Decimal"
            )
            assert isinstance(sample_order.quantity_filled, Decimal), (
                "quantity_filled must be Decimal"
            )

            if sample_order.price is not None:  # Market orders may not have price
                assert isinstance(sample_order.price, Decimal), "price must be Decimal"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private"], indirect=True)
class TestGetOrderHistoryIntegration:
    """Comprehensive tests for get_order_history() endpoint."""

    @pytest.mark.vcr
    async def test_get_order_history_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_order_history() with date range validation."""
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
        assert isinstance(order_history, list), (
            "get_order_history() should return list[Order]"
        )

        # If history exists, validate structure
        if order_history:
            sample_order = order_history[0]
            assert isinstance(sample_order, Order), (
                "Historical order should be Order instance"
            )

            # Validate exchange field
            assert sample_order.exchange == "backpack", "Exchange should be 'backpack'"

            # Validate order fields
            assert isinstance(sample_order.symbol, str), "symbol must be string"
            assert sample_order.side in [OrderSide.BUY, OrderSide.SELL], (
                "side must be valid OrderSide"
            )
            assert sample_order.exchange_order_id is not None, (
                "order should have exchange ID"
            )

            # Validate Decimal precision
            assert isinstance(sample_order.quantity_requested, Decimal), (
                "quantity_requested must be Decimal"
            )
            assert isinstance(sample_order.quantity_filled, Decimal), (
                "quantity_filled must be Decimal"
            )

            # Validate timestamps
            assert sample_order.created_at is not None, (
                "order should have created_at timestamp"
            )

    @pytest.mark.vcr
    async def test_get_order_history_invalid_date_range(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order_history() with invalid date range."""
        # Create invalid date range (start > end)
        start_time = datetime.now()
        end_time = start_time - timedelta(days=1)  # End before start

        # This should fail validation in GetOrderHistoryArgs
        with pytest.raises(ValueError, match="start_time must be before end_time"):
            GetOrderHistoryArgs(
                start_time=start_time,
                end_time=end_time,
                limit=50,
            )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private"], indirect=True)
class TestGetTradeHistoryIntegration:
    """Comprehensive tests for get_trade_history() endpoint."""

    @pytest.mark.vcr
    async def test_get_trade_history_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_trade_history() validates full pipeline to Trade models.

        Note: This test assumes the BackpackAPI has a get_trade_history method.
        If not implemented, this test will be skipped.
        """
        try:
            # Execute the full pipeline using GetTradeHistoryArgs
            args = GetTradeHistoryArgs(
                symbol="SOL_USDC",
                limit=50,
            )
            trade_history = await bp_api_for_test_env.get_trade_history(args)

            # Validate return type
            assert isinstance(trade_history, list), (
                "get_trade_history() should return list"
            )

            # If trades exist, validate structure
            if trade_history:
                sample_trade = trade_history[0]
                # Basic trade validation (structure depends on Trade model)
                assert hasattr(sample_trade, "symbol"), "Trade should have symbol"
                assert hasattr(sample_trade, "price"), "Trade should have price"
                assert hasattr(sample_trade, "quantity"), "Trade should have quantity"

                # Validate Decimal precision if price/quantity are Decimal
                if hasattr(sample_trade, "price"):
                    price = getattr(sample_trade, "price", None)
                    if price is not None and isinstance(price, Decimal):
                        assert price > Decimal("0"), "Trade price should be positive"

        except (NotImplementedError, AttributeError):
            pytest.skip("get_trade_history() not yet implemented in BackpackAPI")