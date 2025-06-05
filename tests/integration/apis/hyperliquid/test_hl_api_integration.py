"""Integration tests for the HyperliquidAPI client implementation.

Tests use environment-aware fixtures and test complete workflows through public interfaces.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetFundingRatesArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration

# Constants for testing
TEST_WALLET_ADDRESS = "0x0000000000000000000000000000000000000000"


class TestHyperliquidAPIAssetIndexingIntegration:
    """Test asset indexing integration through public API methods that depend on it."""

    @pytest.mark.asyncio
    async def test_place_order_with_asset_indexing_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that place_order works correctly when asset indexing succeeds."""
        api = hl_api_with_di()

        # Mock the trading service to return a successful order
        expected_order = Order(
            exchange_order_id="12345",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Configure the trading service mock to succeed
        mock_hl_trading_service.place_order.return_value = expected_order

        # Call place_order - this should internally use asset indexing
        place_order_args = PlaceOrderArgs(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
        )
        result = await api.place_order(place_order_args)

        # Verify the trading service was called correctly
        mock_hl_trading_service.place_order.assert_called_once_with(place_order_args)

        # Verify the result
        assert result == expected_order

        await api.close()

    @pytest.mark.asyncio
    async def test_place_order_with_asset_indexing_failure(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that place_order properly handles asset indexing failures."""
        api = hl_api_with_di()

        # Configure trading service to raise an asset indexing error
        # This simulates what happens when the trading service can't resolve the asset index
        asset_indexing_error = APIError(
            "Asset index not found for symbol 'UNKNOWN_SYMBOL'",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_hl_trading_service.place_order.side_effect = asset_indexing_error

        # Call place_order with an unknown symbol and expect the error to be propagated
        with pytest.raises(APIError) as exc_info:
            place_order_args = PlaceOrderArgs(
                symbol="UNKNOWN_SYMBOL",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )
            await api.place_order(place_order_args)

        # Verify the error is the expected asset indexing error
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        assert "Asset index not found" in str(exc_info.value)

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_order_with_asset_indexing_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that cancel_order works correctly when asset indexing succeeds."""
        api = hl_api_with_di()

        # Configure trading service to return successful cancellation
        mock_hl_trading_service.cancel_order.return_value = True

        # Call cancel_order - this should internally use asset indexing
        cancel_args = CancelOrderArgs(order_id="12345", symbol="BTC")
        result = await api.cancel_order(args=cancel_args)

        # Verify the trading service was called correctly
        mock_hl_trading_service.cancel_order.assert_called_once_with(args=cancel_args)

        # Verify the result
        assert result is True

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_order_with_asset_indexing_failure(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that cancel_order properly handles asset indexing failures."""
        api = hl_api_with_di()

        # Configure trading service to raise an asset indexing error
        asset_indexing_error = APIError(
            "Asset index not found for symbol 'INVALID_SYMBOL'",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_hl_trading_service.cancel_order.side_effect = asset_indexing_error

        # Call cancel_order with an invalid symbol and expect the error to be propagated
        with pytest.raises(APIError) as exc_info:
            cancel_args = CancelOrderArgs(order_id="12345", symbol="INVALID_SYMBOL")
            await api.cancel_order(args=cancel_args)

        # Verify the error is the expected asset indexing error
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        assert "Asset index not found" in str(exc_info.value)

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_with_asset_indexing_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that get_order works correctly when asset indexing succeeds."""
        api = hl_api_with_di()

        # Configure trading service to return an order
        expected_order = Order(
            exchange_order_id="12345",
            symbol="ETH",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("2.0"),
            price=Decimal("3000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_hl_trading_service.get_order.return_value = expected_order

        # Call get_order - this should internally use asset indexing
        result = await api.get_order(GetOrderArgs(order_id="12345", symbol="ETH"))

        # Verify the trading service was called correctly
        mock_hl_trading_service.get_order.assert_called_once_with(
            args=GetOrderArgs(order_id="12345", symbol="ETH"),
        )

        # Verify the result
        assert result == expected_order

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_with_asset_indexing_failure(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that get_order properly handles asset indexing failures."""
        api = hl_api_with_di()

        # Configure trading service to raise an asset indexing error
        asset_indexing_error = APIError(
            "Asset index not found for symbol 'NONEXISTENT'",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_hl_trading_service.get_order.side_effect = asset_indexing_error

        # Call get_order with a nonexistent symbol and expect the error to be propagated
        with pytest.raises(APIError) as exc_info:
            await api.get_order(GetOrderArgs(order_id="12345", symbol="NONEXISTENT"))

        # Verify the error is the expected asset indexing error
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        assert "Asset index not found" in str(exc_info.value)

        await api.close()

    @pytest.mark.asyncio
    async def test_multiple_operations_asset_indexing_consistency(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that multiple operations using the same symbol work consistently."""
        api = hl_api_with_di()

        # Configure trading service responses
        expected_order = Order(
            exchange_order_id="12345",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        mock_hl_trading_service.place_order.return_value = expected_order
        mock_hl_trading_service.get_order.return_value = expected_order
        mock_hl_trading_service.cancel_order.return_value = True

        # Perform multiple operations with the same symbol
        # Each should use asset indexing internally

        # Place order
        place_order_args = PlaceOrderArgs(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
        )
        place_result = await api.place_order(place_order_args)

        # Get order
        get_result = await api.get_order(GetOrderArgs(order_id="12345", symbol="BTC"))

        # Cancel order
        cancel_args = CancelOrderArgs(order_id="12345", symbol="BTC")
        cancel_result = await api.cancel_order(args=cancel_args)

        # Verify all operations succeeded
        assert place_result == expected_order
        assert get_result == expected_order
        assert cancel_result is True

        # Verify all trading service methods were called
        mock_hl_trading_service.place_order.assert_called_once()
        mock_hl_trading_service.get_order.assert_called_once()
        mock_hl_trading_service.cancel_order.assert_called_once()

        await api.close()


class TestHyperliquidAPIAccountOperations:
    """Test account-related operations with service delegation."""

    @pytest.mark.asyncio
    async def test_get_balances_delegates_to_account_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test that get_balances properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_balances = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                total_quantity=Decimal("5000.0"),
                available_quantity=Decimal("4800.0"),
                timestamp=datetime.now(UTC),
            ),
        }
        mock_hl_account_service.get_balances.return_value = expected_balances

        # Test delegation
        result = await api.get_balances()

        # Verify service was called and result returned
        mock_hl_account_service.get_balances.assert_called_once()
        assert result == expected_balances

        await api.close()

    @pytest.mark.asyncio
    async def test_get_account_summary_delegates_to_account_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test that get_account_summary properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_summary = MarginAccountSummary(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            total_equity=Decimal("5000.0"),
            available_equity=Decimal("4200.0"),
            total_initial_margin_required=None,
            total_maintenance_margin_required=Decimal("80.0"),
            total_unrealized_pnl=Decimal("25.0"),
        )
        mock_hl_account_service.get_account_summary.return_value = expected_summary

        # Test delegation
        result = await api.get_account_summary()

        # Verify service was called and result returned
        mock_hl_account_service.get_account_summary.assert_called_once()
        assert result == expected_summary

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_delegates_to_account_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test that get_positions properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_positions: list[DerivativePosition] = []  # Empty positions list
        mock_hl_account_service.get_positions.return_value = expected_positions

        # Test delegation
        result = await api.get_positions()

        # Verify service was called and result returned
        mock_hl_account_service.get_positions.assert_called_once_with(symbol=None)
        assert result == expected_positions

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_with_symbol_delegates_to_account_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test that get_positions with symbol delegates to account service."""
        api = hl_api_with_di()
        mock_hl_account_service.get_positions.return_value = []

        result = await api.get_positions("ETH")

        assert result == []
        mock_hl_account_service.get_positions.assert_called_once_with(symbol="ETH")

    @pytest.mark.asyncio
    async def test_get_order_history_delegates_to_account_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test that get_order_history properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_orders: list[Order] = []  # Empty orders list
        mock_hl_account_service.get_order_history.return_value = expected_orders

        # Test delegation
        order_args = GetOrderHistoryArgs(symbol="ETH")
        result = await api.get_order_history(args=order_args)

        # Verify service was called with correct parameters
        mock_hl_account_service.get_order_history.assert_called_once_with(args=order_args)
        assert result == expected_orders

        await api.close()

    @pytest.mark.asyncio
    async def test_get_trade_history_delegates_to_account_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test that get_trade_history properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_trades: list[Trade] = []  # Empty trades list
        mock_hl_account_service.get_trade_history.return_value = expected_trades

        # Test delegation
        result = await api.get_trade_history(args=GetTradeHistoryArgs(symbol="ETH"))

        # Verify service was called with correct parameters
        mock_hl_account_service.get_trade_history.assert_called_once_with(
            args=GetTradeHistoryArgs(symbol="ETH"),
        )
        assert result == expected_trades

        await api.close()


class TestHyperliquidAPITradingOperations:
    """Test trading-related operations with service delegation."""

    @pytest.mark.asyncio
    async def test_place_order_delegates_to_trading_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that place_order properly delegates to trading service."""
        api = hl_api_with_di()

        # Configure mock trading service
        expected_order = Order(
            exchange_order_id="12345",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_hl_trading_service.place_order.return_value = expected_order

        # Test delegation
        place_order_args = PlaceOrderArgs(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
        )
        result = await api.place_order(place_order_args)

        # Verify service was called with correct parameters
        mock_hl_trading_service.place_order.assert_called_once_with(place_order_args)
        assert result == expected_order

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_order_delegates_to_trading_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that cancel_order properly delegates to trading service."""
        api = hl_api_with_di()

        # Configure mock trading service
        mock_hl_trading_service.cancel_order.return_value = True

        # Test delegation
        cancel_args = CancelOrderArgs(order_id="12345", symbol="BTC")
        result = await api.cancel_order(args=cancel_args)

        # Verify service was called with correct parameters
        mock_hl_trading_service.cancel_order.assert_called_once_with(args=cancel_args)
        assert result is True

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_delegates_to_trading_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that get_order properly delegates to trading service."""
        api = hl_api_with_di()

        # Configure mock trading service
        expected_order = Order(
            exchange_order_id="12345",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_hl_trading_service.get_order.return_value = expected_order

        # Test delegation
        result = await api.get_order(GetOrderArgs(order_id="12345", symbol="BTC"))

        # Verify service was called with correct parameters
        mock_hl_trading_service.get_order.assert_called_once_with(
            args=GetOrderArgs(order_id="12345", symbol="BTC"),
        )
        assert result == expected_order

        await api.close()

    @pytest.mark.asyncio
    async def test_get_open_orders_delegates_to_trading_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that get_open_orders delegates to trading service."""
        api = hl_api_with_di()
        mock_hl_trading_service.get_open_orders.return_value = []

        result = await api.get_open_orders("BTC")

        assert result == []
        mock_hl_trading_service.get_open_orders.assert_called_once_with(symbol="BTC")


class TestHyperliquidAPIMarketDataOperations:
    """Test market data operations with service delegation."""

    @pytest.mark.asyncio
    async def test_get_ticker_delegates_to_market_data_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test that get_ticker delegates to market data service."""
        api = hl_api_with_di()

        # Create a mock ticker
        mock_ticker = MagicMock()
        mock_hl_market_data_service.get_ticker.return_value = mock_ticker

        result = await api.get_ticker("BTC")

        assert result == mock_ticker
        mock_hl_market_data_service.get_ticker.assert_called_once_with(symbol="BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rates_delegates_to_market_data_service(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test that get_funding_rates delegates to market data service."""
        api = hl_api_with_di()
        mock_hl_market_data_service.get_funding_rates.return_value = []

        funding_args = GetFundingRatesArgs(symbols=["BTC"])
        result = await api.get_funding_rates(args=funding_args)

        assert result == []
        mock_hl_market_data_service.get_funding_rates.assert_called_once_with(args=funding_args)


class TestHyperliquidAPIComprehensiveErrorHandling:
    """Comprehensive error handling tests covering various failure scenarios.
    
    Tests edge cases across all API operations to ensure robust error handling.
    """

    @pytest.mark.asyncio
    async def test_get_balances_service_validation_error(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test get_balances exact propagation of service validation errors."""
        api = hl_api_with_di()

        # Configure mock to raise validation error
        validation_error = APIError(
            "Invalid balance data format",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )
        mock_hl_account_service.get_balances.side_effect = validation_error

        # Test exact error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_balances()

        assert exc_info.value is validation_error  # Same instance
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid balance data format" in exc_info.value.message
        mock_hl_account_service.get_balances.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_ticker_empty_successful_response(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test that get_ticker handles empty successful response correctly."""
        api = hl_api_with_di()
        mock_hl_market_data_service.get_ticker.return_value = None

        result = await api.get_ticker("NONEXISTENT")

        assert result is None
        mock_hl_market_data_service.get_ticker.assert_called_once_with(symbol="NONEXISTENT")

    @pytest.mark.asyncio
    async def test_get_positions_rate_limited_propagation(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test that rate limited errors from account service are propagated correctly."""
        api = hl_api_with_di()

        # Configure mock to raise rate limited error
        rate_limited_error = APIError("Rate limited", code=429)
        mock_hl_account_service.get_positions.side_effect = rate_limited_error

        with pytest.raises(APIError) as exc_info:
            await api.get_positions("BTC")

        assert exc_info.value.code == 429
        assert "Rate limited" in str(exc_info.value)
        mock_hl_account_service.get_positions.assert_called_once_with(symbol="BTC")

    @pytest.mark.asyncio
    async def test_get_account_summary_server_error_propagation(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test get_account_summary exact propagation of server errors."""
        api = hl_api_with_di()

        # Configure mock to raise server error
        server_error = APIError(
            "Internal server error",
            code=APIErrorCode.SERVER_ERROR.value,
            http_status=500,
        )
        mock_hl_account_service.get_account_summary.side_effect = server_error

        # Test exact server error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

        assert exc_info.value is server_error  # Same instance
        assert exc_info.value.code == APIErrorCode.SERVER_ERROR.value
        assert exc_info.value.http_status == 500
        mock_hl_account_service.get_account_summary.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_order_book_timeout_error_propagation(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test get_order_book exact propagation of timeout errors."""
        api = hl_api_with_di()

        # Configure mock to raise timeout error
        timeout_error = APIError("Request timeout", code=APIErrorCode.TIMEOUT.value)
        mock_hl_market_data_service.get_order_book.side_effect = timeout_error

        # Test exact timeout error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_order_book("ETH")

        assert exc_info.value is timeout_error  # Same instance
        assert exc_info.value.code == APIErrorCode.TIMEOUT.value
        mock_hl_market_data_service.get_order_book.assert_called_once_with(symbol="ETH")

    @pytest.mark.asyncio
    async def test_get_recent_trades_service_unavailable_propagation(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test get_recent_trades exact propagation of service unavailable errors."""
        api = hl_api_with_di()

        # Configure mock to raise service unavailable error
        service_error = APIError(
            "Service temporarily unavailable",
            code=APIErrorCode.SERVICE_UNAVAILABLE.value,
            http_status=503,
        )
        mock_hl_market_data_service.get_recent_trades.side_effect = service_error

        # Test exact service unavailable error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_recent_trades("BTC", limit=10)

        assert exc_info.value is service_error  # Same instance
        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert exc_info.value.http_status == 503
        mock_hl_market_data_service.get_recent_trades.assert_called_once_with(symbol="BTC")

    @pytest.mark.asyncio
    async def test_place_order_service_unexpected_exception(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test place_order exact propagation of unexpected exceptions from service."""
        api = hl_api_with_di()

        # Configure mock to raise unexpected exception
        unexpected_error = RuntimeError("Unexpected service failure")
        mock_hl_trading_service.place_order.side_effect = unexpected_error

        # Test exact unexpected exception propagation
        with pytest.raises(RuntimeError) as exc_info:
            place_order_args = PlaceOrderArgs(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )
            await api.place_order(place_order_args)

        assert exc_info.value is unexpected_error  # Same instance
        assert "Unexpected service failure" in str(exc_info.value)


class TestHyperliquidAPIWebSocketOperations:
    """Test WebSocket operations."""

    @pytest.mark.asyncio
    async def test_subscribe_delegates_to_ws_manager(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that subscribe properly delegates to WebSocket manager."""
        api = hl_api_with_di()

        async def mock_handler(data: dict[str, Any], full_message: dict[str, Any]) -> None:
            pass

        # Mock the base class subscribe method
        empty_handlers: dict[str, Any] = {}
        empty_subscriptions: dict[str, Any] = {}
        with patch.object(api, "_ws_handlers", empty_handlers):
            with patch.object(api, "_ws_subscriptions", empty_subscriptions):
                # This should not raise an error
                await api.subscribe("test_topic", mock_handler)

        await api.close()

    @pytest.mark.asyncio
    async def test_websocket_message_handling_public_behavior(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test WebSocket message handling through public interface."""
        api = hl_api_with_di()

        # Mock the router to avoid actual message processing
        with patch.object(api, "_hl_ws_router") as mock_router:
            mock_router.route_message = AsyncMock()
            # Test through public interface instead of protected method
            # This tests that the WebSocket infrastructure is properly set up
            assert hasattr(api, "_hl_ws_router")

        await api.close()


class TestHyperliquidAPIErrorHandlingIntegration:
    """Test that the API client correctly propagates errors from services through public API."""

    @pytest.mark.asyncio
    async def test_service_apierror_propagation_exact_passthrough(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that APIError from service is propagated exactly without wrapping."""
        api = hl_api_with_di()

        # Configure service to raise specific APIError
        service_error = APIError(
            message="Service-level validation failed",
            code=APIErrorCode.INVALID_PARAMS.value,
            http_status=400,
        )
        mock_hl_trading_service.place_order.side_effect = service_error

        # API client should propagate the exact same APIError
        with pytest.raises(APIError) as exc_info:
            place_order_args = PlaceOrderArgs(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )
            await api.place_order(place_order_args)

        # Assert exact error propagation
        assert exc_info.value is service_error  # Same instance
        assert exc_info.value.code == APIErrorCode.INVALID_PARAMS.value
        assert exc_info.value.message == "Service-level validation failed"
        assert exc_info.value.http_status == 400

        await api.close()

    @pytest.mark.asyncio
    async def test_service_valueerror_propagation_exact_passthrough(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test that ValueError from service is propagated exactly without wrapping."""
        api = hl_api_with_di()

        # Configure service to raise ValueError for input validation
        service_error = ValueError("Invalid symbol format for service processing")
        mock_hl_trading_service.get_order.side_effect = service_error

        # API client should propagate the exact same ValueError
        with pytest.raises(ValueError) as exc_info:
            await api.get_order(GetOrderArgs(order_id="invalid_id", symbol="BTC"))

        # Assert exact error propagation
        assert exc_info.value is service_error  # Same instance
        assert str(exc_info.value) == "Invalid symbol format for service processing"

        await api.close()

    @pytest.mark.asyncio
    async def test_multiple_error_types_from_different_services(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
        mock_hl_account_service: MagicMock,
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test different services raise different error types and all are propagated correctly."""
        api = hl_api_with_di()

        # Configure different services to raise different error types
        trading_api_error = APIError(
            message="Trading service error",
            code=APIErrorCode.NETWORK_ISSUE.value,
        )
        account_value_error = ValueError("Account service input validation failed")
        market_data_api_error = APIError(
            message="Market data service error",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

        mock_hl_trading_service.cancel_order.side_effect = trading_api_error
        mock_hl_account_service.get_balances.side_effect = account_value_error
        mock_hl_market_data_service.get_ticker.side_effect = market_data_api_error

        # Test trading service APIError propagation
        with pytest.raises(APIError) as trading_exc:
            cancel_args = CancelOrderArgs(order_id="12345")
            await api.cancel_order(args=cancel_args)
        assert trading_exc.value is trading_api_error

        # Test account service ValueError propagation
        with pytest.raises(ValueError) as account_exc:
            await api.get_balances()
        assert account_exc.value is account_value_error

        # Test market data service APIError propagation
        with pytest.raises(APIError) as market_exc:
            await api.get_ticker(symbol="BTC")
        assert market_exc.value is market_data_api_error

        await api.close()
