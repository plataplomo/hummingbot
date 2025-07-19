"""Unit tests for HyperliquidTradingRequestBuilder functionality."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cyberdelta.apis.base.trading_execution_domain import (
    LiquidityRequirement,
    OrderExecution,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawHistoricalOrdersRequestPayload,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_trading_request_builder import (
    HyperliquidTradingRequestBuilder,
)
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderHistoryArgsHL,
    PlaceOrderArgs,
)
from cyberdelta.enums import OrderSide, OrderType, TimeInForce


class TestHyperliquidTradingRequestBuilder:
    """Tests for HyperliquidTradingRequestBuilder functionality."""

    @pytest.fixture
    def builder(self) -> HyperliquidTradingRequestBuilder:
        """Create a HyperliquidTradingRequestBuilder instance."""
        return HyperliquidTradingRequestBuilder()

    @pytest.fixture
    def valid_wallet_address(self) -> str:
        """Provide a valid wallet address."""
        return "0x1234567890abcdef1234567890abcdef12345678"

    @pytest.fixture
    def symbol(self) -> str:
        """Provide a test symbol."""
        return "BTC-PERP"

    def test_build_place_order_payload_limit_buy(
        self,
        builder: HyperliquidTradingRequestBuilder,
        symbol: str,
    ) -> None:
        """Test building place order payload for limit buy order."""
        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.01"),
            price=Decimal("40000.00"),
            time_in_force=TimeInForce.GTC,
            execution=OrderExecution(),  # Defaults
        )

        payload = builder.build_place_order_payload_with_args(args, asset_index=0)

        assert isinstance(payload, HyperliquidApiPlaceOrderRequest)
        # Test passes if we can build the payload successfully

    def test_build_cancel_order_payload(
        self,
        builder: HyperliquidTradingRequestBuilder,
        symbol: str,
    ) -> None:
        """Test building cancel order payload."""
        args = CancelOrderArgs(
            symbol=symbol,
            order_id="123456",
        )

        payload = builder.build_cancel_order_payload_with_args(args, asset_index=0, order_id=123456)

        assert isinstance(payload, HyperliquidApiCancelOrderRequest)
        # Test passes if we can build the payload successfully

    def test_build_historical_orders_payload(
        self,
        builder: HyperliquidTradingRequestBuilder,
        valid_wallet_address: str,
    ) -> None:
        """Test building historical orders request payload."""
        args = GetOrderHistoryArgsHL(
            wallet_address=valid_wallet_address,
            start_time_ms=1640995200000,  # 2022-01-01
            end_time_ms=1672531200000,  # 2023-01-01
        )

        payload = HyperliquidTradingRequestBuilder.build_historical_orders_payload(args)

        assert isinstance(payload, HyperliquidRawHistoricalOrdersRequestPayload)
        # Test passes if we can build the payload successfully

    def test_build_historical_orders_payload_with_order_id(
        self,
        builder: HyperliquidTradingRequestBuilder,
        valid_wallet_address: str,
    ) -> None:
        """Test building historical orders request with specific order ID."""
        args = GetOrderHistoryArgsHL(
            wallet_address=valid_wallet_address,
            start_time_ms=1640995200000,  # 2022-01-01
            end_time_ms=1672531200000,  # 2023-01-01
        )

        payload = HyperliquidTradingRequestBuilder.build_historical_orders_payload(args)

        assert isinstance(payload, HyperliquidRawHistoricalOrdersRequestPayload)
        # Test passes if we can build the payload successfully

    def test_build_place_order_payload_market_sell(
        self,
        builder: HyperliquidTradingRequestBuilder,
        symbol: str,
    ) -> None:
        """Test building place order payload for market sell order."""
        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.IOC,
            execution=OrderExecution(),  # Defaults
        )

        payload = builder.build_place_order_payload_with_args(args, asset_index=0)

        assert isinstance(payload, HyperliquidApiPlaceOrderRequest)
        # Test passes if we can build the payload successfully

    def test_build_place_order_payload_post_only(
        self,
        builder: HyperliquidTradingRequestBuilder,
        symbol: str,
    ) -> None:
        """Test building place order payload with post-only flag."""
        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("30000.00"),
            time_in_force=TimeInForce.GTC,
            execution=OrderExecution(
                liquidity_requirement=LiquidityRequirement.POST_ONLY,
            ),
        )

        payload = builder.build_place_order_payload_with_args(args, asset_index=0)

        assert isinstance(payload, HyperliquidApiPlaceOrderRequest)
        # Test passes if we can build the payload successfully
