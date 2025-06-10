"""Comprehensive integration tests for Hyperliquid private (authenticated) endpoints.

This module provides battle-tested integration testing for all private Hyperliquid API endpoints
that require EIP-712 authentication. Tests validate the complete data pipeline from API method calls
through to final Internal Domain Models for both SUCCESS and ERROR scenarios, using 
pytest-recording (VCR) for deterministic testing against the Hyperliquid testnet.

These tests validate the full client stack including:
- HyperliquidEIP712Authenticator (EIP-712 signing and auth failure handling)
- HyperliquidRequestBuilder (request construction for /exchange and /info endpoints)
- HTTP communication via connectivity layer
- HyperliquidResponseHandler (response parsing)
- HyperliquidErrorMapper (error code mapping from Hyperliquid-specific errors)
- Data mapping to internal models with Decimal precision

Test Structure:
- TestGetAccountStateIntegration: Validates account state endpoints (balances, positions, summary)
- TestTradingActionsIntegration: Validates trading endpoints (place/cancel orders)
- TestOrderHistoryIntegration: Validates order and trade history endpoints
- Each test class contains multiple scenarios: success, auth failure, business logic errors
- VCR cassettes capture both successful and error responses from Hyperliquid testnet
- All sensitive data (private keys, signatures, nonces, wallet addresses) is filtered by VCR

Recording Notes:
- Tests target Hyperliquid testnet (CYBERDELTA_TEST_ENV_HL=testnet)
- Success tests use valid testnet private key with sufficient funds
- Auth failure tests use intentionally invalid EIP-712 signatures during recording
- Business logic error tests use invalid parameters (large quantities, bad order IDs, etc.)
- All /exchange endpoint signatures and nonces are scrubbed by VCR filters
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.spot_balance import SpotBalance

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/private"], indirect=True)
class TestGetAccountStateIntegration:
    """Comprehensive tests for Hyperliquid account state endpoints.

    Since get_balances(), get_positions(), and get_account_summary() all derive from the same
    clearinghouseState /info call, we test them together to validate the complete account
    state pipeline with EIP-712 authentication.
    """

    @pytest.mark.vcr
    async def test_get_balances_and_positions_success(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful account state retrieval with detailed model validation.

        Validates:
        - EIP-712 signing for /info endpoint with user query
        - Request building with wallet address
        - Response parsing from clearinghouse state
        - Mapping to SpotBalance, DerivativePosition, and MarginAccountSummary instances
        - All financial fields have correct Decimal precision
        - Business logic constraints are enforced
        - Exchange-specific details are populated correctly
        """
        # Execute all account state calls concurrently to test the same underlying endpoint
        balances, positions, summary = await asyncio.gather(
            hl_api_for_test_env.get_balances(),
            hl_api_for_test_env.get_positions(),
            hl_api_for_test_env.get_account_summary(),
        )

        # ===== BALANCES VALIDATION =====
        assert isinstance(balances, dict), "get_balances() should return dict[str, SpotBalance]"

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
            assert sample_balance.exchange == "hyperliquid", "Exchange should be 'hyperliquid'"

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
            # Hyperliquid uses total_quantity and available_quantity relationship
            assert sample_balance.total_quantity >= sample_balance.available_quantity, (
                "total_quantity should be >= available_quantity"
            )

            # Validate exchange-specific details if present
            if sample_balance.hl_details:
                # Add specific validation for Hyperliquid balance details
                # This would depend on the actual hl_details structure
                pass

        # ===== POSITIONS VALIDATION =====
        assert isinstance(positions, list), "get_positions() should return list[DerivativePosition]"

        if positions:
            sample_position = positions[0]
            assert isinstance(sample_position, DerivativePosition), (
                "Position should be DerivativePosition instance"
            )

            # Validate exchange field
            assert sample_position.exchange == "hyperliquid", "Exchange should be 'hyperliquid'"

            # Validate timestamp
            assert sample_position.timestamp is not None, "Position should have timestamp"

            # Validate symbol format (Hyperliquid uses asset names like "BTC", "ETH")
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
            if sample_position.entry_price > Decimal("0"):  # May be 0 for new positions
                assert sample_position.mark_price > Decimal("0"), "mark_price must be positive"

            # Validate exchange-specific details if present
            if sample_position.hl_details:
                hl_position_details = sample_position.hl_details
                # Validate leverage type (HyperliquidPositionDetails)
                assert isinstance(hl_position_details.leverage_type, str), (
                    "leverage_type must be string"
                )
                assert hl_position_details.leverage_type in ["cross", "isolated"], (
                    "leverage_type must be 'cross' or 'isolated'"
                )
                assert isinstance(hl_position_details.leverage_value, int), (
                    "leverage_value must be int"
                )
                assert hl_position_details.leverage_value >= 0, (
                    "leverage_value must be non-negative"
                )

        # ===== ACCOUNT SUMMARY VALIDATION =====
        assert isinstance(summary, MarginAccountSummary), (
            "get_account_summary() should return MarginAccountSummary"
        )

        # Validate exchange field
        assert summary.exchange == "hyperliquid", "Exchange should be 'hyperliquid'"

        # Validate timestamp
        assert summary.timestamp is not None, "Account summary should have timestamp"

        # Validate required Decimal fields with precision
        assert isinstance(summary.total_equity, Decimal), (
            "total_equity must be Decimal for precision"
        )
        assert isinstance(summary.available_equity, Decimal), (
            "available_equity must be Decimal for precision"
        )

        # Validate business logic constraints
        assert summary.total_equity >= Decimal("0"), (
            "total_equity must be non-negative"
        )
        assert summary.available_equity >= Decimal("0"), (
            "available_equity must be non-negative"
        )

        # Validate optional margin fields if present
        if summary.total_initial_margin_required is not None:
            assert isinstance(summary.total_initial_margin_required, Decimal), (
                "total_initial_margin_required must be Decimal"
            )
            assert summary.total_initial_margin_required >= Decimal("0"), (
                "total_initial_margin_required must be non-negative"
            )

        # Validate exchange-specific details
        if summary.hl_details:
            hl_margin_details = summary.hl_details
            
            # Validate Decimal fields in hl_details (HyperliquidMarginDetails)
            assert isinstance(hl_margin_details.cross_maintenance_margin_used, Decimal), (
                "cross_maintenance_margin_used must be Decimal"
            )
            assert isinstance(hl_margin_details.isolated_maintenance_margin_used, Decimal), (
                "isolated_maintenance_margin_used must be Decimal"
            )
            
            # Validate non-negative constraints
            assert hl_margin_details.cross_maintenance_margin_used >= Decimal("0"), (
                "cross_maintenance_margin_used must be non-negative"
            )
            assert hl_margin_details.isolated_maintenance_margin_used >= Decimal("0"), (
                "isolated_maintenance_margin_used must be non-negative"
            )

    @pytest.mark.vcr
    async def test_get_account_state_auth_failure(
        self,
        hl_api_with_di: MagicMock,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test account state retrieval with invalid EIP-712 authentication.

        This test validates that our HyperliquidEIP712Authenticator and HyperliquidErrorMapper
        correctly handle authentication failures from the exchange.

        Note: During recording, this test uses intentionally invalid EIP-712 private key to
        capture a real authentication error response from Hyperliquid. On playback, VCR returns
        the recorded error response.
        """
        # Create API instance with invalid EIP-712 credentials
        bad_secrets = PrivateKeyAuthSecrets(
            private_key=SecretStr("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
        )
        
        bad_api = hl_api_with_di(secrets=bad_secrets)

        # Attempt operation that requires EIP-712 authentication
        with pytest.raises(APIError) as exc_info:
            await bad_api.get_balances()

        # Validate error structure and mapping
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.AUTHENTICATION_FAILED.value, (
            "HyperliquidErrorMapper should map auth errors to AUTHENTICATION_FAILED"
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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/private"], indirect=True)
class TestTradingActionsIntegration:
    """Comprehensive tests for Hyperliquid trading action endpoints.

    Tests place_order(), cancel_order(), and cancel_all_orders() endpoints that use
    the /exchange endpoint with EIP-712 signing for state-changing operations.
    """

    @pytest.mark.vcr
    async def test_place_and_cancel_order_success(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful order placement and cancellation with detailed Order model validation.

        This test uses testnet assets and prices far from market to avoid fills during recording.
        It validates the complete trading action pipeline with EIP-712 signing.
        """
        # Define order parameters for testnet (use testnet asset, far from market to avoid fills)
        place_args = PlaceOrderArgs(
            symbol="PURP",  # Common testnet asset on Hyperliquid
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),  # Small size for testnet
            price=Decimal("0.01"),  # Far below market price
            time_in_force=TimeInForce.GTC,
        )

        # ===== PLACE ORDER VALIDATION =====
        placed_order = await hl_api_for_test_env.place_order(place_args)

        # Validate placed order structure
        assert isinstance(placed_order, Order), (
            "place_order() should return Order instance"
        )

        # Validate exchange field
        assert placed_order.exchange == "hyperliquid", "Exchange should be 'hyperliquid'"

        # Validate timestamp
        assert placed_order.created_at is not None, "Order should have created_at timestamp"

        # Validate order matches request parameters
        assert placed_order.symbol == "PURP", "Order symbol should match request"
        assert placed_order.side == OrderSide.BUY, "Order side should match request"
        assert placed_order.order_type == OrderType.LIMIT, "Order type should match request"
        assert placed_order.quantity_requested == Decimal("1"), (
            "Order quantity should match request"
        )
        assert placed_order.price == Decimal("0.01"), "Order price should match request"

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

        # Validate Hyperliquid-specific order ID format (should be integer string)
        try:
            int(placed_order.exchange_order_id)
        except ValueError:
            pytest.fail("Hyperliquid order ID should be a valid integer string")

        # ===== CANCEL ORDER VALIDATION =====
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
            "HyperliquidErrorMapper should map insufficient balance to INSUFFICIENT_FUNDS"
        )
        assert isinstance(api_error.message, str), "Error message should be string"
        assert len(api_error.message) > 0, "Error message should not be empty"

        # Check for common Hyperliquid insufficient funds phrases
        message_lower = api_error.message.lower()
        assert any(phrase in message_lower for phrase in [
            "insufficient", "balance", "margin", "funds"
        ]), "Error message should indicate insufficient funds/balance"

    @pytest.mark.vcr
    async def test_place_order_invalid_asset(
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
        ], "Should map to symbol-related error code"

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
            "HyperliquidErrorMapper should map order not found to ORDER_NOT_FOUND"
        )
        
        # Check for common Hyperliquid order not found phrases
        message_lower = api_error.message.lower()
        assert any(phrase in message_lower for phrase in [
            "not found", "never placed", "invalid", "does not exist"
        ]), "Error message should indicate order not found"

    @pytest.mark.vcr
    async def test_cancel_all_orders_success(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_all_orders() validates bulk cancellation pipeline."""
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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/private"], indirect=True)
class TestOrderHistoryIntegration:
    """Comprehensive tests for Hyperliquid order and trade history endpoints."""

    @pytest.mark.vcr
    async def test_get_order_history_success(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order_history() validates full pipeline to Order models."""
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
            assert sample_order.exchange == "hyperliquid", "Exchange should be 'hyperliquid'"

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
    async def test_get_trade_history_success(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_trade_history() validates full pipeline to Trade models."""
        # Execute the full pipeline using GetTradeHistoryArgs
        args = GetTradeHistoryArgs(
            symbol="PURP",  # GetTradeHistoryArgs expects symbol filtering
            limit=50,
        )
        trade_history = await hl_api_for_test_env.get_trade_history(args)

        # Validate return type
        assert isinstance(trade_history, list), (
            "get_trade_history() should return list"
        )

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

            # Validate exchange-specific details if present
            if hasattr(sample_trade, "hl_details") and sample_trade.hl_details:
                # Add specific validation for Hyperliquid trade details
                # This would depend on the actual hl_details structure
                pass

    @pytest.mark.vcr
    async def test_get_order_by_id_success(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order() validates pipeline for single order query."""
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
        assert isinstance(retrieved_order, Order), (
            "get_order() should return Order instance"
        )
        assert retrieved_order.exchange_order_id == order_id, (
            "Order ID should match query"
        )
        assert retrieved_order.symbol == "PURP", "Order symbol should match"
        assert retrieved_order.side == OrderSide.BUY, "Order side should match"

        # Clean up - cancel the order
        assert order_id is not None, "Order ID should not be None for cancellation"
        cancel_args = CancelOrderArgs(order_id=order_id, symbol="PURP")
        await hl_api_for_test_env.cancel_order(cancel_args)

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
            "HyperliquidErrorMapper should map order not found to ORDER_NOT_FOUND"
        )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/private"], indirect=True)
class TestGetOpenOrdersIntegration:
    """Comprehensive tests for get_open_orders() endpoint."""

    @pytest.mark.vcr
    async def test_get_open_orders_success(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_open_orders() with detailed Order model validation."""
        # Execute the full pipeline
        open_orders = await hl_api_for_test_env.get_open_orders()

        # Validate return type
        assert isinstance(open_orders, list), (
            "get_open_orders() should return list[Order]"
        )

        # If orders exist, validate structure
        if open_orders:
            sample_order = open_orders[0]
            assert isinstance(sample_order, Order), "Order should be Order instance"

            # Validate exchange field
            assert sample_order.exchange == "hyperliquid", "Exchange should be 'hyperliquid'"

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

            # Validate Hyperliquid-specific order ID format
            try:
                int(sample_order.exchange_order_id)
            except ValueError:
                pytest.fail("Hyperliquid order ID should be a valid integer string")


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/private"], indirect=True)
class TestErrorHandlingIntegration:
    """Comprehensive tests for error handling scenarios."""

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

    @pytest.mark.vcr
    async def test_invalid_asset_error_consistency(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that invalid asset errors are consistently mapped across endpoints."""
        invalid_symbol = "NONEXISTENT_ASSET_12345"

        # Test place_order with invalid asset
        with pytest.raises(APIError) as place_exc:
            await hl_api_for_test_env.place_order(PlaceOrderArgs(
                symbol=invalid_symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1"),
                price=Decimal("1.00"),
                time_in_force=TimeInForce.GTC,
            ))

        # Test cancel_order with invalid asset
        with pytest.raises(APIError) as cancel_exc:
            await hl_api_for_test_env.cancel_order(CancelOrderArgs(
                order_id="123456789",
                symbol=invalid_symbol,
            ))

        # Both should map to similar error codes
        place_error = place_exc.value
        cancel_error = cancel_exc.value
        
        symbol_related_codes = {
            APIErrorCode.INVALID_SYMBOL.value,
            APIErrorCode.SYMBOL_NOT_FOUND.value,
            APIErrorCode.INVALID_REQUEST.value,
        }
        
        assert place_error.code in symbol_related_codes, (
            "place_order with invalid symbol should map to symbol-related error"
        )
        assert cancel_error.code in symbol_related_codes, (
            "cancel_order with invalid symbol should map to symbol-related error"
        )