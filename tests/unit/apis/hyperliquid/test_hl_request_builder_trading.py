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
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawPlaceOrderAction,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.conftest_request_builder"]


class TestHyperliquidRequestBuilderTrading:
    """Tests for HyperliquidRequestBuilder trading operation functionality."""

    def test_build_place_order_payload_limit_gtc(self, asset_index: int) -> None:
        """Test build_place_order_payload for a GTC LIMIT order."""
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.5"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2000.50"),
            client_order_id="clOrd123",
            reduce_only=False,
            post_only=False,
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawPlaceOrderAction)
        assert action.asset == asset_index
        assert action.is_buy is True
        assert action.sz == "1.5"
        assert action.limit_px == "2000.50"
        assert action.order_type.limit is not None
        assert action.order_type.limit.tif == "Gtc"
        assert action.reduce_only is False
        assert action.cloid == "clOrd123"

    def test_build_place_order_payload_market(self, asset_index: int) -> None:
        """Test build_place_order_payload for a MARKET order."""
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index + 1,
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            time_in_force=TimeInForce.IOC,
            reduce_only=True,
            post_only=False,
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawPlaceOrderAction)
        assert action.asset == asset_index + 1
        assert action.is_buy is False
        assert action.sz == "10"
        assert action.limit_px == "0"
        assert action.order_type.market is not None
        assert action.reduce_only is True

    def test_build_place_order_payload_limit_alo_post_only(self, asset_index: int) -> None:
        """Test build_place_order_payload for ALO LIMIT order (post_only=True)."""
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2100"),
            post_only=True,
            reduce_only=False,
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawPlaceOrderAction)
        assert action.asset == asset_index
        assert action.is_buy is True
        assert action.sz == "1.0"
        assert action.limit_px == "2100"
        assert action.order_type.limit is not None
        assert action.order_type.limit.tif == "Alo"
        assert action.reduce_only is False

    def test_build_place_order_payload_stop_market(self, asset_index: int) -> None:
        """Test build_place_order_payload for a STOP_MARKET order."""
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index + 2,
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            quantity=Decimal("0.5"),
            time_in_force=TimeInForce.GTC,
            stop_price=Decimal("1900"),
            reduce_only=False,
            post_only=False,
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawPlaceOrderAction)
        assert action.asset == asset_index + 2
        assert action.is_buy is False
        assert action.sz == "0.5"
        assert action.limit_px == "0"
        assert action.trigger is not None
        assert action.trigger.trigger_px == "1900"
        assert action.trigger.is_market is True
        assert action.trigger.tpsl == "sl"
        assert action.order_type.limit is not None
        assert action.order_type.limit.tif == "Gtc"
        assert action.reduce_only is False

    def test_build_place_order_payload_stop_limit(self, asset_index: int) -> None:
        """Test build_place_order_payload for a STOP_LIMIT order."""
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index + 3,
            side=OrderSide.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal("2"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2200"),
            stop_price=Decimal("2150"),
            reduce_only=False,
            post_only=False,
        )
        assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
        assert request_model.type == "order"
        assert len(request_model.orders) == 1
        action = request_model.orders[0]
        assert isinstance(action, HyperliquidRawPlaceOrderAction)
        assert action.asset == asset_index + 3
        assert action.is_buy is True
        assert action.sz == "2"
        assert action.limit_px == "2200"
        assert action.trigger is not None
        assert action.trigger.trigger_px == "2150"
        assert action.trigger.is_market is False
        assert action.trigger.tpsl == "sl"
        assert action.order_type.limit is not None
        assert action.order_type.limit.tif == "Gtc"
        assert action.reduce_only is False

    def test_build_place_order_with_none_values(self, asset_index: int) -> None:
        """Test build_place_order_payload handles None values gracefully.

        Request builder should not perform validation - that's done in the service layer.
        """
        # Request builder should accept None price for LIMIT order (validation happens in service)
        payload = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            price=None,  # Builder should handle this
            post_only=False,
            reduce_only=False,
        )
        assert payload.orders[0].limit_px == "0"  # Builder defaults to "0"

        # Request builder should accept None stop_price for STOP_MARKET order
        payload = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index + 1,
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.GTC,
            stop_price=None,  # Builder should handle this
            post_only=False,
            reduce_only=False,
        )
        assert payload.orders[0].trigger is None  # No trigger created without stop_price

        # Request builder should accept None stop_price for STOP_LIMIT order
        payload = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index + 2,
            side=OrderSide.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal("2"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2200"),
            stop_price=None,  # Builder should handle this
            post_only=False,
            reduce_only=False,
        )
        assert payload.orders[0].trigger is None  # No trigger created without stop_price

    def test_build_cancel_order_payload(self, asset_index: int) -> None:
        """Test build_cancel_order_payload with valid inputs."""
        request_model = HyperliquidRequestBuilder.build_cancel_order_payload(
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
        request_model = HyperliquidRequestBuilder.build_order_status_payload(
            wallet_address=valid_wallet_address,
            order_id=67890,
        )
        assert isinstance(request_model, HyperliquidRawOrderStatusRequestPayload)
        assert request_model.type == "orderStatus"
        assert request_model.user == valid_wallet_address
        assert request_model.oid == 67890

    def test_build_place_order_payload_different_time_in_force(self, asset_index: int) -> None:
        """Test build_place_order_payload with different TimeInForce values."""
        # Test IOC (Immediate or Cancel)
        request_ioc = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
            price=Decimal("2000"),
            reduce_only=False,
            post_only=False,
        )
        action_ioc = request_ioc.orders[0]
        assert action_ioc.order_type.limit is not None
        assert action_ioc.order_type.limit.tif == "Ioc"

        # Test GTC (Good Till Cancel) - default case
        request_gtc = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("2.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2100"),
            reduce_only=False,
            post_only=False,
        )
        action_gtc = request_gtc.orders[0]
        assert action_gtc.order_type.limit is not None
        assert action_gtc.order_type.limit.tif == "Gtc"

    def test_build_place_order_payload_edge_cases(self, asset_index: int) -> None:
        """Test build_place_order_payload with edge case values."""
        # Test with very small quantity
        request_small = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.00001"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("1.123456789"),
            reduce_only=False,
            post_only=False,
        )
        action_small = request_small.orders[0]
        assert action_small.sz == "0.00001"
        assert action_small.limit_px == "1.123456789"

        # Test with very large values
        request_large = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("999999.999999"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("100000.123456"),
            reduce_only=True,
            post_only=True,
        )
        action_large = request_large.orders[0]
        assert action_large.sz == "999999.999999"
        assert action_large.limit_px == "100000.123456"
        assert action_large.reduce_only is True
        assert action_large.order_type.limit is not None
        assert action_large.order_type.limit.tif == "Alo"  # ALO due to post_only=True

    def test_build_cancel_order_payload_different_asset_indices(self) -> None:
        """Test build_cancel_order_payload with various asset indices."""
        # Test with asset index 0
        request_0 = HyperliquidRequestBuilder.build_cancel_order_payload(
            asset_index=0,
            order_id=1001,
        )
        assert request_0.action.asset == 0
        assert request_0.action.oid == 1001

        # Test with larger asset index
        request_high = HyperliquidRequestBuilder.build_cancel_order_payload(
            asset_index=99,
            order_id=9999,
        )
        assert request_high.action.asset == 99
        assert request_high.action.oid == 9999

    def test_build_order_status_payload_edge_cases(self, valid_wallet_address: str) -> None:
        """Test build_order_status_payload with edge case order IDs."""
        # Test with small order ID
        request_small = HyperliquidRequestBuilder.build_order_status_payload(
            wallet_address=valid_wallet_address,
            order_id=1,
        )
        assert request_small.oid == 1

        # Test with large order ID
        request_large = HyperliquidRequestBuilder.build_order_status_payload(
            wallet_address=valid_wallet_address,
            order_id=999999999,
        )
        assert request_large.oid == 999999999
