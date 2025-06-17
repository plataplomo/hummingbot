"""Unit tests for HyperliquidRequestBuilder trading operation functionality."""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawCancelOrderAction,
    HyperliquidRawOrderItemSpec,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderStatusArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.conftest_request_builder"]


class TestHyperliquidRequestBuilderTrading:
    """Tests for HyperliquidRequestBuilder trading operation functionality."""

    def test_build_place_order_payload_limit_gtc(self, asset_index: int) -> None:
        """Test build_place_order_payload for a GTC LIMIT order."""
        args = PlaceOrderArgs(
            symbol="BTC_USDC",  # Symbol will be mapped to asset_index by service layer
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.5"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2000.50"),
            client_order_id="clOrd123",
            reduce_only=False,
            post_only=False,
        )
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            args=args,
            asset_index=asset_index,
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawOrderItemSpec)
        assert action.a == asset_index
        assert action.b is True
        assert action.s == "1.5"
        assert action.p == "2000.5"
        assert action.t.limit is not None
        assert action.t.limit.tif == "Gtc"
        assert action.r is False
        assert action.c == "clOrd123"

    def test_build_place_order_payload_market(self, asset_index: int) -> None:
        """Test build_place_order_payload for a MARKET order."""
        args = PlaceOrderArgs(
            symbol="ETH_USDC",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            time_in_force=TimeInForce.IOC,
            reduce_only=True,
            post_only=False,
        )
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            args=args,
            asset_index=asset_index + 1,
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawOrderItemSpec)
        assert action.a == asset_index + 1
        assert action.b is False
        assert action.s == "10"
        assert action.p == "0"
        assert action.t.market is not None
        assert action.r is True

    def test_build_place_order_payload_limit_alo_post_only(self, asset_index: int) -> None:
        """Test build_place_order_payload for ALO LIMIT order (post_only=True)."""
        args = PlaceOrderArgs(
            symbol="BTC_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2100"),
            post_only=True,
            reduce_only=False,
        )
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            args=args,
            asset_index=asset_index,
            tif_str="Alo",  # ALO for post_only orders
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawOrderItemSpec)
        assert action.a == asset_index
        assert action.b is True
        assert action.s == "1"
        assert action.p == "2100"
        assert action.t.limit is not None
        assert action.t.limit.tif == "Alo"
        assert action.r is False

    def test_build_place_order_payload_stop_market(self, asset_index: int) -> None:
        """Test build_place_order_payload for a STOP_MARKET order."""
        args = PlaceOrderArgs(
            symbol="BTC_USDC",
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            quantity=Decimal("0.5"),
            time_in_force=TimeInForce.GTC,
            stop_price=Decimal("1900"),
            reduce_only=False,
            post_only=False,
        )
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            args=args,
            asset_index=asset_index + 2,
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawOrderItemSpec)
        assert action.a == asset_index + 2
        assert action.b is False
        assert action.s == "0.5"
        assert action.p == "0"
        assert action.t.trigger is not None
        assert action.t.trigger.trigger_px == "1900"
        assert action.t.trigger.is_market is True
        assert action.t.trigger.tpsl == "sl"
        # STOP_MARKET orders don't have a limit field, only trigger
        assert action.r is False

    def test_build_place_order_payload_stop_limit(self, asset_index: int) -> None:
        """Test build_place_order_payload for a STOP_LIMIT order."""
        args = PlaceOrderArgs(
            symbol="BTC_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal("2"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2200"),
            stop_price=Decimal("2150"),
            reduce_only=False,
            post_only=False,
        )
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            args=args,
            asset_index=asset_index + 3,
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawOrderItemSpec)
        assert action.a == asset_index + 3
        assert action.b is True
        assert action.s == "2"
        assert action.p == "2200"
        assert action.t.trigger is not None
        assert action.t.trigger.trigger_px == "2150"
        assert action.t.trigger.is_market is False
        assert action.t.trigger.tpsl == "sl"
        # STOP_LIMIT orders also use trigger field, not limit
        assert action.r is False

    def test_build_place_order_with_edge_tif_values(self, asset_index: int) -> None:
        """Test build_place_order_payload with edge TIF values.

        Tests the builder's handling of different time-in-force values.
        """
        # Test with custom TIF string override
        args = PlaceOrderArgs(
            symbol="BTC_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2000"),
            post_only=False,
            reduce_only=False,
        )
        # Override TIF with custom value
        payload = HyperliquidRequestBuilder.build_place_order_payload(
            args=args,
            asset_index=asset_index,
            tif_str="Ioc",  # Immediate or Cancel override
        )
        assert payload.orders[0].t.limit.tif == "Ioc"  # Custom TIF applied

        # Test with no TIF override - should use default
        payload_default = HyperliquidRequestBuilder.build_place_order_payload(
            args=args,
            asset_index=asset_index,
        )
        assert payload_default.orders[0].t.limit.tif == "Gtc"  # Default TIF

        # Test with post_only which should result in ALO
        args_alo = PlaceOrderArgs(
            symbol="BTC_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2000"),
            post_only=True,  # This should trigger ALO
            reduce_only=False,
        )
        payload_alo = HyperliquidRequestBuilder.build_place_order_payload(
            args=args_alo,
            asset_index=asset_index,
            tif_str="Alo",
        )
        assert payload_alo.orders[0].t.limit.tif == "Alo"  # ALO TIF for post_only

    def test_build_cancel_order_payload(self, asset_index: int) -> None:
        """Test build_cancel_order_payload with valid inputs."""
        args = CancelOrderArgs(
            symbol="BTC_USDC",
            order_id="12345",
        )
        request_model = HyperliquidRequestBuilder.build_cancel_order_payload(
            args=args,
            asset_index=asset_index + 1,
            order_id=12345,
        )
        assert isinstance(request_model, HyperliquidApiCancelOrderRequest)
        assert request_model.type == "cancel"
        action = request_model.action
        assert isinstance(action, HyperliquidRawCancelOrderAction)
        assert action.asset == asset_index + 1
        assert action.oid == 12345

    def test_build_order_status_payload(self, valid_wallet_address: str) -> None:
        """Test build_order_status_payload with valid inputs."""
        args = GetOrderStatusArgs(
            order_id=67890,
            wallet_address=valid_wallet_address,
        )
        request_model = HyperliquidRequestBuilder.build_order_status_payload(
            args=args,
        )
        assert isinstance(request_model, HyperliquidRawOrderStatusRequestPayload)
        assert request_model.type == "orderStatus"
        assert request_model.user == valid_wallet_address
        assert request_model.oid == args.order_id

    def test_build_place_order_payload_different_time_in_force(self, asset_index: int) -> None:
        """Test build_place_order_payload with different TimeInForce values."""
        # Test IOC (Immediate or Cancel)
        args_ioc = PlaceOrderArgs(
            symbol="BTC_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
            price=Decimal("2000"),
            reduce_only=False,
            post_only=False,
        )
        request_ioc = HyperliquidRequestBuilder.build_place_order_payload(
            args=args_ioc,
            asset_index=asset_index,
            tif_str="Ioc",
        )
        action_ioc = request_ioc.orders[0]
        assert action_ioc.t.limit is not None
        assert action_ioc.t.limit.tif == "Ioc"

        # Test GTC (Good Till Cancel) - default case
        args_gtc = PlaceOrderArgs(
            symbol="BTC_USDC",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("2.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2100"),
            reduce_only=False,
            post_only=False,
        )
        request_gtc = HyperliquidRequestBuilder.build_place_order_payload(
            args=args_gtc,
            asset_index=asset_index,
            tif_str="Gtc",
        )
        action_gtc = request_gtc.orders[0]
        assert action_gtc.t.limit is not None
        assert action_gtc.t.limit.tif == "Gtc"

    def test_build_place_order_payload_edge_cases(self, asset_index: int) -> None:
        """Test build_place_order_payload with edge case values."""
        # Test with very small quantity
        args_small = PlaceOrderArgs(
            symbol="BTC_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.00001"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("1.12345678"),  # 8 decimal places max
            reduce_only=False,
            post_only=False,
        )
        request_small = HyperliquidRequestBuilder.build_place_order_payload(
            args=args_small,
            asset_index=asset_index,
        )
        action_small = request_small.orders[0]
        assert action_small.s == "0.00001"
        assert action_small.p == "1.12345678"

        # Test with very large values
        args_large = PlaceOrderArgs(
            symbol="BTC_USDC",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("999999.999999"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("100000.12345678"),  # 8 decimal places max
            reduce_only=True,
            post_only=True,
        )
        request_large = HyperliquidRequestBuilder.build_place_order_payload(
            args=args_large,
            asset_index=asset_index,
            tif_str="Alo",  # ALO for post_only orders
        )
        action_large = request_large.orders[0]
        assert action_large.s == "999999.999999"
        assert action_large.p == "100000.12345678"
        assert action_large.r is True
        assert action_large.t.limit is not None
        assert action_large.t.limit.tif == "Alo"  # ALO due to post_only=True

    def test_build_cancel_order_payload_different_asset_indices(self) -> None:
        """Test build_cancel_order_payload with various asset indices."""
        # Test with asset index 0
        args_0 = CancelOrderArgs(
            symbol="BTC_USDC",
            order_id="1001",
        )
        request_0 = HyperliquidRequestBuilder.build_cancel_order_payload(
            args=args_0,
            asset_index=0,
            order_id=1001,
        )
        assert request_0.action.asset == 0
        assert request_0.action.oid == 1001

        # Test with larger asset index
        args_high = CancelOrderArgs(
            symbol="BTC_USDC",
            order_id="9999",
        )
        request_high = HyperliquidRequestBuilder.build_cancel_order_payload(
            args=args_high,
            asset_index=99,
            order_id=9999,
        )
        assert request_high.action.asset == 99
        assert request_high.action.oid == 9999

    def test_build_order_status_payload_edge_cases(self, valid_wallet_address: str) -> None:
        """Test build_order_status_payload with edge case order IDs."""
        # Test with small order ID
        args_small = GetOrderStatusArgs(
            order_id=1,
            wallet_address=valid_wallet_address,
        )
        request_small = HyperliquidRequestBuilder.build_order_status_payload(
            args=args_small,
        )
        assert request_small.oid == args_small.order_id

        # Test with large order ID
        args_large = GetOrderStatusArgs(
            order_id=999999999,
            wallet_address=valid_wallet_address,
        )
        request_large = HyperliquidRequestBuilder.build_order_status_payload(
            args=args_large,
        )
        assert request_large.oid == args_large.order_id
