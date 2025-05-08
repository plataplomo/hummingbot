"""Unit tests for HyperliquidResponseHandler."""

from typing import Any, cast
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_candle_snapshot import (
    HyperliquidRawCandle,
    HyperliquidRawCandleSnapshotResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrder,
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFill,
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


def test_handle_exchange_response_valid() -> None:
    """Test handling a valid raw exchange response (e.g., order placement)."""
    raw_data = {
        "status": "ok",
        "data": {"type": "order", "statuses": [{"resting": {"oid": 12345}}, "canceled"]},
    }
    response: HyperliquidRawExchangeResponse = HyperliquidResponseHandler.handle_exchange_response(
        cast(Any, raw_data), action_type="order"
    )
    assert isinstance(response, HyperliquidRawExchangeResponse)
    assert response.status == "ok"
    assert response.data is not None
    assert response.data.type == "order"
    assert len(response.data.statuses) == 2
    # Check first status (object)
    status1 = response.data.statuses[0]
    assert isinstance(status1, HyperliquidRawExchangeStatusObject)
    assert status1.resting is not None
    assert status1.resting.oid == 12345
    # Check second status (string)
    status2 = response.data.statuses[1]
    assert isinstance(status2, str)
    assert status2 == "canceled"


def test_handle_exchange_response_validation_error() -> None:
    """Test handling exchange response dict failing validation (e.g., missing status)."""
    raw_data = {"data": {"type": "order", "statuses": [{"resting": {"oid": 12345}}]}}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_exchange_response(
            cast(Any, raw_data), action_type="order"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Check correct message prefix and that the original exception mentions the missing field
    assert "Invalid exchange (order) response from exchange:" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)
    assert "status" in str(exc_info.value.original_exception)


def test_handle_exchange_response_top_level_status_error() -> None:
    """Test handling an exchange response where top-level status is 'error'.
    This should fail validation against HyperliquidRawExchangeResponse model which expects status='ok'.
    """
    raw_data = {
        "status": "error",
        "error": "Invalid order size",
    }
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_exchange_response(
            cast(Any, raw_data), action_type="order"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Check correct message prefix and that the original exception mentions the literal error
    assert "Invalid exchange (order) response from exchange:" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)
    assert "status" in str(exc_info.value.original_exception)
    assert "Input should be 'ok'" in str(exc_info.value.original_exception)
    assert "error" in str(exc_info.value.original_exception)  # Check the extra field error too


def test_handle_exchange_response_invalid_type() -> None:
    """Test handling an exchange response with an invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_exchange_response(cast(Any, raw_data), "order")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message


def test_handle_exchange_response_validation_error_ok_missing_data() -> None:
    """Test handling exchange response with status='ok' but invalid/missing 'data' field."""
    raw_data = {"status": "ok", "data": "not a valid data structure"}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_exchange_response(
            cast(Any, raw_data), action_type="order"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Check correct message prefix and that the original exception mentions the data field error
    assert "Invalid exchange (order) response from exchange:" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)
    assert "data" in str(exc_info.value.original_exception)
    assert "Input should be a valid dictionary" in str(exc_info.value.original_exception)


def test_handle_info_meta_and_asset_ctxs_response_valid() -> None:
    """Test handling a valid raw meta and asset ctxs response."""
    # Structure: [meta_object, list_of_asset_ctx_objects]
    raw_data = [
        {  # Meta Object
            "universe": [
                {
                    "name": "BTC",
                    "szDecimals": 5,
                    "maxLeverage": 100,
                    "onlyIsolated": False,
                },
                {
                    "name": "ETH",
                    "szDecimals": 4,
                    "maxLeverage": 80,
                    "onlyIsolated": False,
                },
            ]
        },
        [  # List of Asset Context Objects
            {
                "name": "BTC",
                "funding": "0.0001",
                "markPx": "55000.0",
                "prevDayPx": "54000.0",
                "dayNtlVlm": "1000000000.0",
                "impactPx": "55010.0",
            },
            {
                "name": "ETH",
                "funding": "0.0002",
                "markPx": "3000.0",
                "prevDayPx": "2950.0",
                "dayNtlVlm": "500000000.0",
                "impactPx": "3005.0",
            },
        ],
    ]

    meta_and_ctxs = HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
        cast(Any, raw_data)
    )
    assert isinstance(meta_and_ctxs, HyperliquidRawMetaAndAssetCtxsResponse)
    assert isinstance(meta_and_ctxs.meta, HyperliquidRawMetaResponse)
    assert len(meta_and_ctxs.meta.universe) == 2
    assert meta_and_ctxs.meta.universe[0].name == "BTC"
    assert isinstance(meta_and_ctxs.asset_ctxs, list)
    assert len(meta_and_ctxs.asset_ctxs) == 2
    assert isinstance(meta_and_ctxs.asset_ctxs[0], HyperliquidRawAssetCtx)
    assert meta_and_ctxs.asset_ctxs[1].name == "ETH"


def test_handle_info_meta_and_asset_ctxs_response_invalid_type() -> None:
    """Test handling meta/ctxs response with invalid type (dict instead of list)."""
    raw_data = {"invalid": "data"}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message


def test_handle_info_meta_and_asset_ctxs_response_validation_error() -> None:
    """Test handling meta/asset ctxs response with invalid data within the structure."""
    # Invalid data: missing 'szDecimals' in the first universe item
    raw_data = [
        {
            "universe": [
                {
                    "name": "BTC",
                    # "szDecimals": 5, # Missing required field
                    "maxLeverage": 100,
                    "onlyIsolated": False,
                }
            ]
        },
        [
            {
                "name": "BTC",
                "funding": "0.0001",
                "markPx": "55000.0",
                "prevDayPx": "54000.0",
                "dayNtlVlm": "1000000000.0",
                "impactPx": "55010.0",
            }
        ],
    ]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Check correct message prefix and that the original exception mentions the missing field
    assert "Invalid info (MetaAndAssetCtxs) response from exchange:" in exc_info.value.message
    assert isinstance(
        exc_info.value.original_exception, ValueError
    )  # Custom validator raises ValueError
    assert "szDecimals" in str(exc_info.value.original_exception)
    assert "Field required" in str(
        exc_info.value.original_exception
    )  # Ensure it's about missing field


def test_handle_info_user_state_response_valid() -> None:
    """Test handling a valid raw user state response."""
    # Corrected structure based on HyperliquidRawClearinghouseState
    raw_data = {
        "assetPositions": [
            {
                "asset": "BTC",
                "position": {
                    "coin": "BTC",
                    "szi": "1.0",
                    "entryPx": "50000.0",
                    "leverage": {"type": "cross", "value": 10},
                    "liquidationPx": "45000.0",
                    "marginUsed": "5000.0",
                    "maxLeverage": 50,
                    "positionValue": "50000.0",
                    "returnOnEquity": "0.0",
                    "unrealizedPnl": "0.0",
                },
            }
        ],
        "crossMaintenanceMarginUsed": "500.0",
        "crossMarginSummary": {
            "accountValue": "10000.0",
            "totalMarginUsed": "5000.0",
            "totalNtlPos": "50000.0",
            "totalRawUsd": "9500.0",
        },
        "marginSummary": {
            "accountValue": "10000.0",
            "totalMarginUsed": "5000.0",
            "totalNtlPos": "50000.0",
            "totalRawUsd": "9500.0",
        },
        "isolatedMaintenanceMarginUsed": "0.0",
        "isolatedMarginSummary": {
            "accountValue": "0.0",
            "totalMarginUsed": "0.0",
            "totalNtlPos": "0.0",
            "totalRawUsd": "0.0",
        },
        "withdrawable": "9500.0",
    }
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    user_state: HyperliquidRawClearinghouseState = (
        HyperliquidResponseHandler.handle_info_user_state_response(
            cast(Any, raw_data), user_address=user_address_placeholder
        )
    )
    assert isinstance(user_state, HyperliquidRawClearinghouseState)
    assert len(user_state.asset_positions) == 1
    assert user_state.asset_positions[0].asset == "BTC"
    assert user_state.asset_positions[0].position.coin == "BTC"
    assert user_state.margin_summary.account_value == "10000.0"
    assert user_state.cross_margin_summary.total_raw_usd == "9500.0"


def test_handle_info_user_state_response_invalid_type() -> None:
    """Test handling user state response with invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_user_state_response(
            cast(Any, raw_data), user_address=user_address_placeholder
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "user state" in exc_info.value.message


def test_handle_info_user_state_response_validation_error() -> None:
    """Test handling user state response dict failing validation."""
    raw_data = {
        "crossMaintenanceMarginUsed": "500.0",
        "crossMarginSummary": {
            "accountValue": "10000.0",
            "totalMarginUsed": "5000.0",
            "totalNtlPos": "50000.0",
            "totalRawUsd": "9500.0",
        },
    }
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_user_state_response(
            cast(Any, raw_data), user_address=user_address_placeholder
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for user state" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_info_open_orders_response_valid() -> None:
    """Test handling a valid raw open orders response."""
    # Adjusted data to match HyperliquidRawOrder model and only 'open' status
    raw_data = [
        {
            "order": {
                "asset": "ETH",
                "limitPx": "3000.0",
                "oid": 6001,
                "reduceOnly": False,
                "side": "B",
                "sz": "0.5",
                "timestamp": 1678889600000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "0.5",
                "status": "open",
                "statusTimestamp": 1678889601000,
                "cloid": "clientOpen1",
            },
            "trigger": None,
        },
        {
            "order": {
                "asset": "BTC",
                "limitPx": "55000.0",
                "oid": 6002,
                "reduceOnly": True,
                "side": "A",
                "sz": "0.1",
                "timestamp": 1678889700000,
                "orderType": {"limit": {"tif": "Alo"}},
                "remainingSz": "0.1",
                "status": "open",
                "statusTimestamp": 1678889701000,
                "cloid": None,
            },
            "trigger": {
                "triggerPx": "56000.0",
                "isMarket": True,
                "tpsl": "tp",
            },
        },
    ]
    open_orders_response = HyperliquidResponseHandler.handle_info_open_orders_response(
        cast(Any, raw_data), user_address="0x1234567890abcdef1234567890abcdef12345678"
    )
    # The response is the RootModel wrapping the list
    assert isinstance(open_orders_response, HyperliquidRawOpenOrdersResponse)
    open_orders = open_orders_response.root
    assert isinstance(open_orders, list)
    assert len(open_orders) == 2
    assert isinstance(open_orders[0], HyperliquidRawOpenOrder)
    assert open_orders[0].order.oid == 6001
    assert open_orders[0].order.asset == "ETH"
    assert open_orders[0].order.status == "open"
    assert open_orders[1].order.asset == "BTC"
    assert open_orders[1].order.status == "open"
    assert open_orders[1].trigger is not None
    assert open_orders[1].trigger.tpsl == "tp"


def test_handle_info_open_orders_response_invalid_type() -> None:
    """Test handling open orders response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_open_orders_response(
            cast(Any, raw_data), user_address="0x1234567890abcdef1234567890abcdef12345678"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Check exact error message for invalid top-level type
    assert (
        f"Unexpected info (OpenOrders for 0x1234567890abcdef1234567890abcdef12345678) response format: expected list, got {type(raw_data).__name__}"
        in exc_info.value.message
    )


def test_handle_info_open_orders_response_invalid_item_type() -> None:
    """Test handling open orders list containing a non-dict item."""
    raw_data = [
        {
            "order": {
                "asset": "ETH",
                "limitPx": "3000.0",
                "oid": 6001,
                "reduceOnly": False,
                "side": "B",
                "sz": "0.5",
                "timestamp": 1678889600000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "0.5",
                "status": "open",
                "statusTimestamp": 1678889601000,
                "cloid": "clientOpen1",
            },
            "trigger": None,
        },
        "not_an_order_dict",
    ]
    with patch("cyberdelta.apis.hyperliquid.hl_response_handler.logger.warning") as mock_log:
        # Expect the handler to skip the invalid item and return only the valid one
        open_orders_response = HyperliquidResponseHandler.handle_info_open_orders_response(
            cast(Any, raw_data), user_address="0x1234567890abcdef1234567890abcdef12345678"
        )
        open_orders = open_orders_response.root
        assert len(open_orders) == 1
        assert open_orders[0].order.oid == 6001
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_info_open_orders_response_item_validation_error() -> None:
    """Test handling open orders list with an item failing validation."""
    raw_data = [
        {
            "order": {
                "asset": "ETH",
                "limitPx": "3000.0",
                "oid": 6001,
                "reduceOnly": False,
                "side": "B",
                "sz": "0.5",
                "timestamp": 1678889600000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "0.5",
                "status": "open",
                "statusTimestamp": 1678889601000,
                "cloid": "clientOpen1",
            },
            "trigger": None,
        },
        {
            "order": {
                "asset": "BTC",
                "limitPx": "55000.0",
                "reduceOnly": True,
                "side": "A",
                "sz": "0.1",
                "timestamp": 1678889700000,
                "orderType": {"limit": {"tif": "Alo"}},
                "remainingSz": "0.1",
                "status": "open",
                "statusTimestamp": 1678889701000,
                "cloid": None,
            },
            "trigger": None,
        },
    ]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_open_orders_response(
            cast(Any, raw_data), user_address="0x1234567890abcdef1234567890abcdef12345678"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Check correct message prefix and that the original exception mentions the missing field
    assert (
        "Invalid single open order item in info (OpenOrders for 0x1234567890abcdef1234567890abcdef12345678) response from exchange:"
        in exc_info.value.message
    )
    assert isinstance(exc_info.value.original_exception, ValidationError)
    assert "order.oid" in str(exc_info.value.original_exception)
    assert "Field required" in str(exc_info.value.original_exception)


def test_handle_info_user_fills_response_valid() -> None:
    """Test handling a valid raw user fills response."""
    raw_data = [
        {
            "tid": 1001,
            "coin": "ETH",
            "px": "3000.1",
            "sz": "0.5",
            "time": 1678889800000,
            "side": "B",
            "oid": 6001,
            "startPosition": "0.0",
            "dir": "Open Long",
            "hash": "0xfillhash1",
            "fee": "1.5",
            "isMaker": False,
            "liquidationMarkPx": None,
            "cloid": "clientFill1",
        },
        {
            "tid": 1002,
            "coin": "BTC",
            "px": "55000.5",
            "sz": "0.1",
            "time": 1678889900000,
            "side": "A",
            "oid": 6002,
            "startPosition": "0.1",
            "dir": "Close Short",
            "hash": "0xfillhash2",
            "fee": "5.5",
            "isMaker": True,
            "liquidationMarkPx": "50000.0",
            "cloid": None,
        },
    ]
    user_fills_response: HyperliquidRawUserFillsResponse = (
        HyperliquidResponseHandler.handle_info_user_fills_response(
            cast(Any, raw_data), user_address="0x1234567890abcdef1234567890abcdef12345678"
        )
    )
    # The handler returns the RootModel wrapping the list
    assert isinstance(user_fills_response, HyperliquidRawUserFillsResponse)
    user_fills = user_fills_response.root  # Access the list via .root
    assert isinstance(user_fills, list)
    assert len(user_fills) == 2
    assert isinstance(user_fills[0], HyperliquidRawUserFill)
    assert user_fills[0].tid == 1001
    assert user_fills[1].coin == "BTC"
    assert user_fills[1].is_maker is True


def test_handle_info_user_fills_response_invalid_type() -> None:
    """Test handling user fills response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_user_fills_response(
            cast(Any, raw_data), user_address="0x1234567890abcdef1234567890abcdef12345678"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Check exact error message for invalid top-level type
    assert (
        f"Unexpected info (UserFills for 0x1234567890abcdef1234567890abcdef12345678) response format: expected list, got {type(raw_data).__name__}"
        in exc_info.value.message
    )


def test_handle_info_user_fills_response_invalid_item_type() -> None:
    """Test handling user fills list containing a non-dict item."""
    raw_data = [
        {  # Valid fill
            "tid": 1001,
            "coin": "ETH",
            "px": "3000.1",
            "sz": "0.5",
            "time": 1678889800000,
            "side": "B",
            "oid": 6001,
            "startPosition": "0.0",
            "dir": "Open Long",
            "hash": "0xfillhash1",
            "fee": "1.5",
            "isMaker": False,
            "liquidationMarkPx": None,
            "cloid": "clientFill1",
        },
        "not_a_fill_dict",  # Invalid item
    ]
    with patch("cyberdelta.apis.hyperliquid.hl_response_handler.logger.warning") as mock_log:
        # Expect the handler to skip the invalid item
        user_fills_response = HyperliquidResponseHandler.handle_info_user_fills_response(
            cast(Any, raw_data), user_address="0x1234567890abcdef1234567890abcdef12345678"
        )
        user_fills = user_fills_response.root
        assert len(user_fills) == 1
        assert user_fills[0].tid == 1001
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_info_user_fills_response_item_validation_error() -> None:
    """Test handling user fills list with an item failing validation."""
    raw_data = [
        {  # Valid fill
            "tid": 1001,
            "coin": "ETH",
            "px": "3000.1",
            "sz": "0.5",
            "time": 1678889800000,
            "side": "B",
            "oid": 6001,
            "startPosition": "0.0",
            "dir": "Open Long",
            "hash": "0xfillhash1",
            "fee": "1.5",
            "isMaker": False,
            "liquidationMarkPx": None,
            "cloid": "clientFill1",
        },
        {  # Invalid fill - missing 'tid'
            "coin": "BTC",
            "px": "55000.5",
            "sz": "0.1",
            "time": 1678889900000,
            "side": "A",
            "oid": 6002,
            "startPosition": "0.1",
            "dir": "Close Short",
            "hash": "0xfillhash2",
            "fee": "5.5",
            "isMaker": True,
            "liquidationMarkPx": "50000.0",
            "cloid": None,
        },
    ]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_user_fills_response(
            cast(Any, raw_data), user_address="0x1234567890abcdef1234567890abcdef12345678"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Check correct message prefix and that the original exception mentions the missing field
    assert (
        "Invalid single user fill item in info (UserFills for 0x1234567890abcdef1234567890abcdef12345678) response from exchange:"
        in exc_info.value.message
    )
    assert isinstance(exc_info.value.original_exception, ValidationError)
    assert "tid" in str(exc_info.value.original_exception)
    assert "Field required" in str(exc_info.value.original_exception)


def test_handle_info_funding_rate_response_valid() -> None:
    """Test handling a valid raw funding rate (AssetCtx) response."""
    # Corrected data matching HyperliquidRawAssetCtx
    raw_data = {
        "name": "BTC",
        # Removed szDecimals, maxLeverage, onlyIsolated
        "oraclePx": "56000.50",  # Included as it is often present, though not required by model
        "markPx": "56010.00",
        "midPx": "56005.00",  # Included as it is often present, though not required by model
        "impactPxs": ["55900.0", "56100.0"],  # Included as it is often present, though optional
        "funding": "0.000015",
        "prevDayPx": "55500.00",
        "dayNtlVlm": "100000000.0",
        "dayAvgPx": "55800.00",  # Included as it is often present, though not required by model
        "dayVol": "1792.1147",  # Included as it is often present, though not required by model
        "dayHigh": "56500.00",  # Included as it is often present, though not required by model
        "dayLow": "55000.00",  # Included as it is often present, though not required by model
    }
    asset_ctx: HyperliquidRawAssetCtx = (
        HyperliquidResponseHandler.handle_info_funding_rate_response(
            cast(Any, raw_data), symbol="BTC"
        )
    )
    assert isinstance(asset_ctx, HyperliquidRawAssetCtx)
    assert asset_ctx.name == "BTC"
    assert asset_ctx.funding == "0.000015"
    # Removed assertion for max_leverage


def test_handle_info_funding_rate_response_invalid_type() -> None:
    """Test handling funding rate response with invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_funding_rate_response(
            cast(Any, raw_data), symbol="BTC"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "FundingRate for BTC" in exc_info.value.message


def test_handle_info_funding_rate_response_validation_error() -> None:
    """Test handling funding rate response dict failing validation."""
    raw_data = {
        "name": "BTC",
        # Missing required 'funding' field
        "markPx": "56010.00",
        "prevDayPx": "55500.00",
        "dayNtlVlm": "100000000.0",
    }
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_funding_rate_response(
            cast(Any, raw_data), symbol="BTC"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for info (FundingRate for BTC)" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_info_l2_book_response_valid() -> None:
    """Test handling a valid raw L2 order book response."""
    raw_data = {
        "coin": "ETH",
        "levels": [
            [  # Bid levels
                {"px": "2999.0", "sz": "10.5", "n": 5},
                {"px": "2998.0", "sz": "20.0", "n": 8},
            ],
            [  # Ask levels
                {"px": "3001.0", "sz": "5.2", "n": 3},
                {"px": "3002.0", "sz": "15.8", "n": 6},
            ],
        ],
        "time": 1678889300000,
    }
    order_book: HyperliquidRawL2Book = HyperliquidResponseHandler.handle_info_l2_book_response(
        cast(Any, raw_data), symbol="ETH"
    )
    assert isinstance(order_book, HyperliquidRawL2Book)
    assert order_book.coin == "ETH"
    assert len(order_book.levels) == 2
    assert len(order_book.levels[0]) == 2  # Bids
    assert order_book.levels[0][0].px == "2999.0"
    assert len(order_book.levels[1]) == 2  # Asks
    assert order_book.levels[1][0].n == 3


def test_handle_info_l2_book_response_invalid_type() -> None:
    """Test handling L2 book response with invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_l2_book_response(cast(Any, raw_data), symbol="ETH")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "L2Book for ETH" in exc_info.value.message


def test_handle_info_l2_book_response_validation_error() -> None:
    """Test handling L2 book response dict failing validation."""
    raw_data = {
        "coin": "ETH",
        "levels": [
            [{"px": "2999.0", "sz": "10.5", "n": 5}],
            ["not_a_level_dict"],  # Invalid item in asks
        ],
        "time": 1678889300000,
    }
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_l2_book_response(cast(Any, raw_data), symbol="ETH")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for info (L2Book for ETH)" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_info_recent_trades_response_valid() -> None:
    """Test handling a valid raw recent trades response (list of trade dicts)."""
    raw_data = [
        {
            "coin": "BTC",
            "side": "B",
            "px": "56100.0",
            "sz": "0.05",
            "time": 1678889400000,
            "hash": "0xtradeHash1",
        },
        {
            "coin": "BTC",
            "side": "A",
            "px": "56105.0",
            "sz": "0.02",
            "time": 1678889401000,
            "hash": "0xtradeHash2",
        },
    ]
    # Corrected type hint for the returned list
    trades: list[HyperliquidRawPublicTrade] = (
        HyperliquidResponseHandler.handle_info_recent_trades_response(
            cast(Any, raw_data), symbol="BTC"
        )
    )
    assert isinstance(trades, list)
    assert len(trades) == 2
    assert isinstance(trades[0], HyperliquidRawPublicTrade)
    assert trades[0].coin == "BTC"
    assert trades[0].side == "B"
    assert trades[1].px == "56105.0"


def test_handle_info_recent_trades_response_invalid_type() -> None:
    """Test handling recent trades response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_recent_trades_response(
            cast(Any, raw_data), symbol="BTC"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "RecentTrades for BTC" in exc_info.value.message


def test_handle_info_recent_trades_response_invalid_item_type() -> None:
    """Test handling recent trades list containing a non-dict item."""
    raw_data = [
        {
            "coin": "BTC",
            "side": "B",
            "px": "56100.0",
            "sz": "0.05",
            "time": 1678889400000,
            "hash": "0xtradeHash1",
        },
        "not_a_trade_dict",
    ]
    with patch("cyberdelta.apis.hyperliquid.hl_response_handler.logger.warning") as mock_log:
        # Corrected type hint for the returned list
        trades: list[HyperliquidRawPublicTrade] = (
            HyperliquidResponseHandler.handle_info_recent_trades_response(
                cast(Any, raw_data), symbol="BTC"
            )
        )
        assert len(trades) == 1
        assert trades[0].hash == "0xtradeHash1"
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_info_recent_trades_response_item_validation_error() -> None:
    """Test handling recent trades list with an item failing validation."""
    raw_data = [
        {
            "coin": "BTC",
            "side": "B",
            "px": "56100.0",
            "sz": "0.05",
            "time": 1678889400000,
            "hash": "0xtradeHash1",
        },
        {
            "coin": "BTC",
            "side": "A",
            # Missing required 'px' field
            "sz": "0.02",
            "time": 1678889401000,
            "hash": "0xtradeHash2",
        },
    ]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_recent_trades_response(
            cast(Any, raw_data), symbol="BTC"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Check correct message prefix and that the original exception mentions the missing field
    assert (
        "Invalid single recent trade item in info (RecentTrades for BTC) response from exchange:"
        in exc_info.value.message
    )
    assert isinstance(exc_info.value.original_exception, ValidationError)
    assert "px" in str(exc_info.value.original_exception)
    assert "Field required" in str(exc_info.value.original_exception)


def test_handle_info_candle_snapshot_response_valid() -> None:
    """Test handling a valid raw candle snapshot response."""
    # Mock data for individual candles, aligning with HyperliquidRawCandle fields
    raw_data = [
        {
            "t": 1678889500000,
            "o": "3000.0",
            "h": "3015.0",
            "l": "2995.0",  # Uses alias 'l' for low_price
            "c": "3010.0",
            "v": "100.5",
            "n": 50,
            # Removed T, s, i as they are not in HyperliquidRawCandle
        },
        {
            "t": 1678889560000,
            "o": "3010.0",
            "h": "3012.0",
            "l": "3003.0",
            "c": "3005.0",
            "v": "80.2",
            "n": 45,
        },
    ]
    snapshot_response: HyperliquidRawCandleSnapshotResponse = (
        HyperliquidResponseHandler.handle_info_candle_snapshot_response(
            cast(Any, raw_data), symbol="ETH", interval="1m"
        )
    )
    assert isinstance(snapshot_response, HyperliquidRawCandleSnapshotResponse)
    candles = snapshot_response.candles  # Access list via .candles field
    assert isinstance(candles, list)
    assert len(candles) == 2
    assert isinstance(candles[0], HyperliquidRawCandle)
    assert candles[0].t == 1678889500000
    assert candles[0].low_price == "2995.0"  # Access via model field name
    assert candles[1].h == "3012.0"


def test_handle_info_candle_snapshot_response_invalid_type() -> None:
    """Test handling candle snapshot response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_candle_snapshot_response(
            cast(Any, raw_data), symbol="ETH", interval="1m"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "CandleSnapshot for ETH 1m" in exc_info.value.message


def test_handle_info_candle_snapshot_response_validation_error() -> None:
    """Test handling candle snapshot list failing validation."""
    raw_data = [
        {
            "t": 1678889500000,
            "o": "3000.0",
            "h": "3015.0",
            "l": "2995.0",
            "c": "3010.0",
            "v": "100.5",
            "n": 50,
        },
        {
            "t": 1678889560000,
            # Missing required 'o' (open price) field
            "h": "3012.0",
            "l": "3003.0",
            "c": "3005.0",
            "v": "80.2",
            "n": 45,
        },
    ]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_candle_snapshot_response(
            cast(Any, raw_data), symbol="ETH", interval="1m"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for info (CandleSnapshot for ETH 1m)" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


class TestHandleQueryOrderHistoryResponse:
    """Tests for handle_query_order_history_response."""

    def test_handle_query_order_history_response_valid(self) -> None:
        """Test handling a valid raw query order history response."""
        raw_data = [
            {
                "order": {
                    "asset": "ETH",
                    "limitPx": "2900.0",
                    "oid": 7001,
                    "reduceOnly": False,
                    "side": "B",
                    "sz": "1.0",
                    "timestamp": 1678890000000,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "remainingSz": "0.0",
                    "status": "filled",
                    "statusTimestamp": 1678890001000,
                    "cloid": "histClient1",
                }
            },
            {
                "order": {
                    "asset": "BTC",
                    "limitPx": "53000.0",
                    "oid": 7002,
                    "reduceOnly": True,
                    "side": "A",
                    "sz": "0.0",
                    "timestamp": 1678891000000,
                    "orderType": {"limit": {"tif": "Alo"}},
                    "remainingSz": "0.2",
                    "status": "canceled",
                    "statusTimestamp": 1678891001000,
                    "cloid": None,
                }
            },
            {
                "order": {
                    "asset": "SOL",
                    "limitPx": "90.0",
                    "oid": 7003,
                    "reduceOnly": False,
                    "side": "B",
                    "sz": "10.0",
                    "timestamp": 1678892000000,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "remainingSz": "5.0",
                    "status": "open",
                    "statusTimestamp": 1678892001000,
                    "cloid": "histClientOpen",
                }
            },
        ]
        order_history: list[HyperliquidRawHistoricalOrderResponse] = (
            HyperliquidResponseHandler.handle_query_order_history_response(
                cast(Any, raw_data), user_address="0xHistoryUser"
            )
        )
        assert isinstance(order_history, list)
        assert len(order_history) == 3
        assert isinstance(order_history[0], HyperliquidRawHistoricalOrderResponse)
        assert order_history[0].order.oid == 7001
        assert order_history[0].order.status == "filled"
        assert order_history[1].order.status == "canceled"
        assert order_history[2].order.status == "open"

    def test_handle_query_order_history_response_invalid_type(self) -> None:
        """Test handling query order history response with invalid type (dict instead of list)."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_query_order_history_response(
                cast(Any, raw_data), user_address="0xHistoryUser"
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Unexpected query_order_history (for 0xHistoryUser) response format: expected list, got {type(raw_data).__name__}"
            in exc_info.value.message
        )

    def test_handle_query_order_history_response_invalid_item_type(self) -> None:
        """Test handling query order history list containing a non-dict item."""
        raw_data: list[Any] = [
            {
                "order": {
                    "asset": "ETH",
                    "limitPx": "2900.0",
                    "oid": 7001,
                    "reduceOnly": False,
                    "side": "B",
                    "sz": "1.0",
                    "timestamp": 1678890000000,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "remainingSz": "0.0",
                    "status": "filled",
                    "statusTimestamp": 1678890001000,
                    "cloid": "histClient1",
                }
            },
            "not_an_order_status_dict",
        ]
        with patch("cyberdelta.apis.hyperliquid.hl_response_handler.logger.warning") as mock_log:
            order_history: list[HyperliquidRawHistoricalOrderResponse] = (
                HyperliquidResponseHandler.handle_query_order_history_response(
                    cast(Any, raw_data), user_address="0xHistoryUser"
                )
            )
            assert len(order_history) == 1
            assert order_history[0].order.oid == 7001
            mock_log.assert_called_once()
            assert "Skipping non-dict item" in mock_log.call_args[0][0]

    def test_handle_query_order_history_response_item_validation_error(self) -> None:
        """Test handling query order history list with an item failing validation."""
        raw_data = [
            {
                "order": {
                    "asset": "ETH",
                    "limitPx": "2900.0",
                    "oid": 7001,
                    "reduceOnly": False,
                    "side": "B",
                    "sz": "1.0",
                    "timestamp": 1678890000000,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "remainingSz": "0.0",
                    "status": "filled",
                    "statusTimestamp": 1678890001000,
                    "cloid": "histClient1",
                }
            },
            {
                "order": {  # Nested order is missing required 'oid'
                    "asset": "BTC",
                    "limitPx": "53000.0",
                    "reduceOnly": True,
                    "side": "A",
                    "sz": "0.0",
                    "timestamp": 1678891000000,
                    "orderType": {"limit": {"tif": "Alo"}},
                    "remainingSz": "0.2",
                    "status": "canceled",
                    "statusTimestamp": 1678891001000,
                }
            },
        ]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_query_order_history_response(
                cast(Any, raw_data), user_address="0xHistoryUser"
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Invalid single order history item (index 1) in query_order_history (for 0xHistoryUser) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "order.oid" in str(exc_info.value.original_exception)
        assert "Field required" in str(exc_info.value.original_exception)


class TestHandleInfoOrderStatusResponse:
    """Tests for handle_info_order_status_response."""

    def test_handle_info_order_status_response_valid_open(self) -> None:
        """Test valid order status response for an OPEN order."""
        raw_data = {
            "order": {
                "asset": "ETH",
                "limitPx": "3000.0",
                "oid": 12345,
                "reduceOnly": False,
                "side": "B",
                "sz": "0.5",
                "timestamp": 1678889600000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "0.5",
                "status": "open",
                "statusTimestamp": 1678889601000,
                "cloid": "clientOpen1",
            }
        }
        # API usually returns a list containing the dict
        response = HyperliquidResponseHandler.handle_info_order_status_response(
            cast(Any, [raw_data]), user_address="0xTestUser", order_id=12345
        )
        assert isinstance(response, HyperliquidRawHistoricalOrderResponse)
        assert response.order.status == "open"
        assert response.order.oid == 12345

    def test_handle_info_order_status_response_valid_filled(self) -> None:
        """Test valid order status response for a FILLED order."""
        raw_data = {
            "order": {
                "asset": "BTC",
                "limitPx": "50000.0",
                "oid": 54321,
                "reduceOnly": True,
                "side": "A",
                "sz": "0.0",
                "timestamp": 1678889700000,
                "orderType": {"limit": {"tif": "Ioc"}},
                "remainingSz": "0.0",
                "status": "filled",
                "statusTimestamp": 1678889701000,
                "cloid": "clientFilled1",
            }
        }
        response = HyperliquidResponseHandler.handle_info_order_status_response(
            cast(Any, [raw_data]), user_address="0xTestUser", order_id=54321
        )
        assert isinstance(response, HyperliquidRawHistoricalOrderResponse)
        assert response.order.status == "filled"
        assert response.order.oid == 54321

    def test_handle_info_order_status_response_valid_canceled(self) -> None:
        """Test valid order status response for a CANCELED order."""
        raw_data = {
            "order": {
                "asset": "SOL",
                "limitPx": "100.0",
                "oid": 67890,
                "reduceOnly": False,
                "side": "B",
                "sz": "0.0",
                "timestamp": 1678889800000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "10.0",
                "status": "canceled",
                "statusTimestamp": 1678889801000,
                "cloid": "clientCanceled1",
            }
        }
        response = HyperliquidResponseHandler.handle_info_order_status_response(
            cast(Any, [raw_data]), user_address="0xTestUser", order_id=67890
        )
        assert isinstance(response, HyperliquidRawHistoricalOrderResponse)
        assert response.order.status == "canceled"
        assert response.order.oid == 67890

    def test_handle_info_order_status_response_invalid_status_value(self) -> None:
        """Test order status response with an invalid status string."""
        raw_data = {
            "order": {
                "asset": "ETH",
                "limitPx": "3000.0",
                "oid": 12345,
                "reduceOnly": False,
                "side": "B",
                "sz": "0.5",
                "timestamp": 1678889600000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "0.5",
                "status": "unknown_status",
                "statusTimestamp": 1678889601000,
                "cloid": "clientInvalid1",
            }
        }
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(Any, [raw_data]), user_address="0xTestUser", order_id=12345
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "order status object in info (OrderStatus for user 0xTestUser, oid 12345)"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "status" in str(exc_info.value.original_exception)
        assert "Input 'unknown_status' is not a valid literal" in str(
            exc_info.value.original_exception
        )

    def test_handle_info_order_status_response_invalid_type_in_list(self) -> None:
        """Test handling order status list containing invalid item type (str instead of dict)."""
        raw_data = ["unexpected_string"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(Any, raw_data), user_address="0xTestUser", order_id=12345
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Unexpected string content in info (OrderStatus for user 0xTestUser, oid 12345) response: {raw_data[0]}"
            in exc_info.value.message
        )

    def test_handle_info_order_status_response_validation_error_missing_order_key(self) -> None:
        """Test handling order status response dict missing the 'order' key."""
        raw_data = {"not_the_order_key": "data"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(Any, [raw_data]), user_address="0xTestUser", order_id=12345
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "order status object in info (OrderStatus for user 0xTestUser, oid 12345)"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "order" in str(exc_info.value.original_exception)
        assert "Field required" in str(exc_info.value.original_exception)

    def test_handle_info_order_status_response_validation_error_invalid_nested_order(self) -> None:
        """Test handling order status response with an invalid nested order dict (e.g. missing oid)."""
        raw_data = {
            "order": {
                "asset": "ETH",
                "limitPx": "3000.0",
                "reduceOnly": False,
                "side": "B",
                "sz": "0.5",
                "timestamp": 1678889600000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "0.5",
                "status": "open",
                "statusTimestamp": 1678889601000,
            }
        }
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(Any, [raw_data]), user_address="0xTestUser", order_id=12345
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "order status object in info (OrderStatus for user 0xTestUser, oid 12345)"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "order.oid" in str(exc_info.value.original_exception)
        assert "Field required" in str(exc_info.value.original_exception)

    def test_handle_info_order_status_response_order_not_found_string(self) -> None:
        """Test handling 'Order not found' string response from API (within a list)."""
        raw_data = ["Order not found"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(Any, raw_data), user_address="0xTestUser", order_id=99999
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert (
            "Order 99999 for user 0xTestUser not found (string response: 'Order not found')"
            in exc_info.value.message
        )

    def test_handle_info_order_status_response_empty_list(self) -> None:
        """Test handling empty list response (order not found)."""
        raw_data: list[Any] = []
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(Any, raw_data), user_address="0xTestUser", order_id=99999
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert (
            "Order 99999 for user 0xTestUser not found (empty list response)"
            in exc_info.value.message
        )

    def test_handle_info_order_status_response_unexpected_string_in_list(self) -> None:
        """Test handling unexpected string in list response."""
        raw_data = ["Some other error string"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(Any, raw_data), user_address="0xTestUser", order_id=88888
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Unexpected string content in info (OrderStatus for user 0xTestUser, oid 88888) response: Some other error string"
            in exc_info.value.message
        )

    def test_handle_info_order_status_response_unexpected_item_type_in_list(self) -> None:
        """Test handling unexpected item type (not dict/str) in list response."""
        raw_data = [12345]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(Any, raw_data), user_address="0xTestUser", order_id=77777
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Unexpected item type in info (OrderStatus for user 0xTestUser, oid 77777) response list: expected dict, got int"
            in exc_info.value.message
        )

    def test_handle_info_order_status_response_invalid_top_level_type(self) -> None:
        """Test handler expecting list or dict, gets something else (e.g. int)."""
        raw_data = 12345
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(Any, raw_data), user_address="0xTestUser", order_id=11111
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Unexpected info (OrderStatus for user 0xTestUser, oid 11111) response format: expected list or dict, got int"
            in exc_info.value.message
        )
