from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
    HyperliquidApiEthWithdrawalRequest,
    HyperliquidApiL2UsdTransferRequest,
    HyperliquidApiPlaceOrderRequest,
    HyperliquidApiTokenWithdrawalRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleRequestDetails,
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawCancelOrderAction,
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawPlaceOrderAction,
    HyperliquidRawQueryOrderHistoryRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
    HyperliquidRawWithdrawalToL1ActionPayload,
)
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce

# Define VALID_ADDRESS at the module level or use a fixture
VALID_ADDRESS = "0xAbCDeF0123456789AbCDeF0123456789AbCDeF01"


def test_build_info_request_payload() -> None:
    """Test build_info_request_payload."""
    payload = HyperliquidRequestBuilder.build_info_request_payload()
    assert payload is None


def test_build_l2_usd_transfer_payload() -> None:
    """Test build_l2_usd_transfer_payload with valid inputs."""
    request_model = HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
        destination_address=VALID_ADDRESS, amount=Decimal("100.50")
    )
    assert isinstance(request_model, HyperliquidApiL2UsdTransferRequest)
    assert request_model.type == "usdTransfer"
    action = request_model.action
    assert isinstance(action, HyperliquidRawL2UsdTransferActionDetails)
    assert action.chain == "L2"
    assert isinstance(action.payload, HyperliquidRawL2UsdTransferPayload)
    assert action.payload.destination == VALID_ADDRESS
    assert action.payload.token == "USDC"
    assert action.payload.amount == "100.50"


def test_build_l2_usd_transfer_payload_invalid_input() -> None:
    """Test build_l2_usd_transfer_payload with invalid (empty) address."""
    with pytest.raises(
        ValueError, match="Destination address .* required for Hyperliquid L2 transfer"
    ):
        HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
            destination_address="", amount=Decimal("100")
        )


def test_build_withdrawal_payload_eth() -> None:
    """Test build_withdrawal_payload for ETH."""
    request_model = HyperliquidRequestBuilder.build_withdrawal_payload(
        asset="ETH", amount=Decimal("1.23"), destination_address=VALID_ADDRESS
    )
    assert isinstance(request_model, HyperliquidApiEthWithdrawalRequest)
    assert request_model.type == "withdrawEth"
    action = request_model.action
    assert isinstance(action, HyperliquidRawEthWithdrawalActionPayload)
    assert action.destination == VALID_ADDRESS
    assert action.amount == "1.23"


def test_build_withdrawal_payload_token() -> None:
    """Test build_withdrawal_payload for a generic token (USDC)."""
    request_model = HyperliquidRequestBuilder.build_withdrawal_payload(
        asset="USDC", amount=Decimal("500"), destination_address=VALID_ADDRESS
    )
    assert isinstance(request_model, HyperliquidApiTokenWithdrawalRequest)
    assert request_model.type == "withdraw"
    action = request_model.action
    assert isinstance(action, HyperliquidRawWithdrawalToL1ActionPayload)
    assert action.token == "USDC"
    assert action.amount == "500"
    assert action.destination == VALID_ADDRESS


def test_build_withdrawal_payload_invalid_input() -> None:
    """Test build_withdrawal_payload with invalid (empty) address."""
    with pytest.raises(ValueError, match="Destination address is required for withdrawal"):
        HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="USDC", amount=Decimal("100"), destination_address=""
        )


def test_build_order_history_payload() -> None:
    """Test build_order_history_payload."""
    start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_time_ms = int(datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
    request_model = HyperliquidRequestBuilder.build_order_history_payload(
        wallet_address=VALID_ADDRESS,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
    )
    assert isinstance(request_model, HyperliquidRawQueryOrderHistoryRequestPayload)
    assert request_model.type == "queryOrderHistory"
    assert request_model.user == VALID_ADDRESS
    assert request_model.start_time == start_time_ms
    assert request_model.end_time == end_time_ms


def test_build_candle_snapshot_payload() -> None:
    """Test build_candle_snapshot_payload."""
    start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_time_ms = int(datetime(2023, 1, 1, 1, 0, 0, tzinfo=UTC).timestamp() * 1000)
    payload = HyperliquidRequestBuilder.build_candle_snapshot_payload(
        symbol="ETH-PERP",
        timeframe="1h",
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
    )
    assert isinstance(payload, HyperliquidRawCandleSnapshotRequestPayload)
    assert payload.type == "candleSnapshot"
    assert isinstance(payload.req, HyperliquidRawCandleRequestDetails)
    assert payload.req.coin == "ETH-PERP"
    assert payload.req.interval == "1h"
    assert payload.req.start_time == start_time_ms
    assert payload.req.end_time == end_time_ms


def test_build_place_order_payload_limit_gtc() -> None:
    """Test build_place_order_payload for a GTC LIMIT order."""
    request_model = HyperliquidRequestBuilder.build_place_order_payload(
        asset_index=0,
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
    assert len(request_model.actions) == 1
    action = request_model.actions[0]
    assert isinstance(action, HyperliquidRawPlaceOrderAction)
    assert action.asset == 0
    assert action.is_buy is True
    assert action.sz == "1.5"
    assert action.limit_px == "2000.50"
    assert action.order_type.limit is not None
    assert action.order_type.limit.tif == "Gtc"
    assert action.reduce_only is False
    assert action.cloid == "clOrd123"


def test_build_place_order_payload_market() -> None:
    """Test build_place_order_payload for a MARKET order."""
    request_model = HyperliquidRequestBuilder.build_place_order_payload(
        asset_index=1,
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=Decimal("10"),
        time_in_force=TimeInForce.IOC,
        reduce_only=True,
        post_only=False,
    )
    assert isinstance(request_model, HyperliquidApiPlaceOrderRequest)
    assert request_model.type == "order"
    assert len(request_model.actions) == 1
    action = request_model.actions[0]
    assert isinstance(action, HyperliquidRawPlaceOrderAction)
    assert action.asset == 1
    assert action.is_buy is False
    assert action.sz == "10"
    assert action.limit_px == "0"
    assert action.order_type.market is not None
    assert action.reduce_only is True


def test_build_place_order_payload_limit_alo_post_only() -> None:
    """Test build_place_order_payload for ALO LIMIT order (post_only=True)."""
    request_model = HyperliquidRequestBuilder.build_place_order_payload(
        asset_index=0,
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
    assert len(request_model.actions) == 1
    action = request_model.actions[0]
    assert isinstance(action, HyperliquidRawPlaceOrderAction)
    assert action.asset == 0
    assert action.is_buy is True
    assert action.sz == "1.0"
    assert action.limit_px == "2100"
    assert action.order_type.limit is not None
    assert action.order_type.limit.tif == "Alo"
    assert action.reduce_only is False


def test_build_place_order_payload_stop_market() -> None:
    """Test build_place_order_payload for a STOP_MARKET order."""
    request_model = HyperliquidRequestBuilder.build_place_order_payload(
        asset_index=2,
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
    assert len(request_model.actions) == 1
    action = request_model.actions[0]
    assert isinstance(action, HyperliquidRawPlaceOrderAction)
    assert action.asset == 2
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


def test_build_place_order_payload_stop_limit() -> None:
    """Test build_place_order_payload for a STOP_LIMIT order."""
    request_model = HyperliquidRequestBuilder.build_place_order_payload(
        asset_index=3,
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
    assert len(request_model.actions) == 1
    action = request_model.actions[0]
    assert isinstance(action, HyperliquidRawPlaceOrderAction)
    assert action.asset == 3
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


def test_build_place_order_invalid_params() -> None:
    """Test build_place_order_payload with missing price for LIMIT order."""
    with pytest.raises(ValueError, match="Price is required for LIMIT orders"):
        HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=0,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            post_only=False,
            reduce_only=False,
        )
    with pytest.raises(ValueError, match="stop_price is required for STOP_MARKET orders"):
        HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=1,
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.GTC,
            post_only=False,
            reduce_only=False,
        )
    with pytest.raises(ValueError, match="stop_price is required for STOP_LIMIT orders"):
        HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=2,
            side=OrderSide.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal("2"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2200"),
            post_only=False,
            reduce_only=False,
        )
    with pytest.raises(
        ValueError, match=r"price \(for triggered limit\) is required for STOP_LIMIT\."
    ):
        HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=3,
            side=OrderSide.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal("2"),
            time_in_force=TimeInForce.GTC,
            stop_price=Decimal("2150"),
            post_only=False,
            reduce_only=False,
        )


def test_build_cancel_order_payload() -> None:
    """Test build_cancel_order_payload."""
    request_model = HyperliquidRequestBuilder.build_cancel_order_payload(
        asset_index=1, order_id=12345
    )
    assert isinstance(request_model, HyperliquidApiCancelOrderRequest)
    assert request_model.type == "cancel"
    action = request_model.action
    assert isinstance(action, HyperliquidRawCancelOrderAction)
    assert action.asset == 1
    assert action.oid == 12345


def test_build_order_status_payload() -> None:
    """Test build_order_status_payload."""
    request_model = HyperliquidRequestBuilder.build_order_status_payload(
        wallet_address=VALID_ADDRESS, order_id=67890
    )
    assert isinstance(request_model, HyperliquidRawOrderStatusRequestPayload)
    assert request_model.type == "orderStatus"
    assert request_model.user == VALID_ADDRESS
    assert request_model.oid == 67890
