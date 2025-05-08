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
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrder,
    HyperliquidRawOpenOrdersResponse,
    HyperliquidRawOrder,
    HyperliquidRawOrderStatusResponse,
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
from cyberdelta.apis.hyperliquid.models.hl_raw_vault_details import (
    HyperliquidRawVaultDetailsResponse,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


def test_handle_exchange_response_valid() -> None:
    """Test handling a valid raw exchange response."""
    raw_data = {
        "status": "ok",
        "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 12345}}]}},
    }
    exchange_response: HyperliquidRawExchangeResponse = (
        HyperliquidResponseHandler.handle_exchange_response(cast(Any, raw_data), "order")
    )
    assert isinstance(exchange_response, HyperliquidRawExchangeResponse)
    assert exchange_response.status == "ok"
    assert exchange_response.data is not None  # Check that data field is present
    assert exchange_response.data.type == "order"
    assert isinstance(exchange_response.data.statuses, list)

    # Access the status object within the list
    status_obj = exchange_response.data.statuses[0]
    # Assert it's the expected status object type and access attributes safely
    assert isinstance(status_obj, HyperliquidRawExchangeStatusObject)
    assert status_obj.resting is not None
    assert status_obj.resting.oid == 12345


def test_handle_exchange_response_invalid_type() -> None:
    """Test handling an exchange response with an invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_exchange_response(cast(Any, raw_data), "order")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message


def test_handle_exchange_response_validation_error() -> None:
    """Test handling an exchange response with missing required fields."""
    raw_data = {"status": "ok"}  # Missing 'response' field
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_exchange_response(cast(Any, raw_data), "order")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for exchange (order)" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_info_meta_and_asset_ctxs_response_valid() -> None:
    """Test handling a valid raw meta and asset contexts response."""
    raw_data = [
        {"universe": [{"name": "BTC", "szDecimals": 5}, {"name": "ETH", "szDecimals": 4}]},
        [
            {"name": "BTC", "maxLeverage": 50, "onlyIsolated": False},
            {"name": "ETH", "maxLeverage": 40, "onlyIsolated": False},
        ],
    ]
    response: HyperliquidRawMetaAndAssetCtxsResponse = (
        HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(cast(Any, raw_data))
    )
    assert isinstance(response, HyperliquidRawMetaAndAssetCtxsResponse)
    assert len(response.meta.universe) == 2
    assert response.meta.universe[0].name == "BTC"
    assert len(response.asset_ctxs) == 2
    assert response.asset_ctxs[1].name == "ETH"


def test_handle_info_meta_and_asset_ctxs_response_invalid_type() -> None:
    """Test handling meta/ctxs response with invalid type (dict instead of list)."""
    raw_data = {"invalid": "data"}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message


def test_handle_info_meta_and_asset_ctxs_response_validation_error() -> None:
    """Test handling meta/ctxs response with invalid structure."""
    # Missing the outer list structure
    raw_data = {"universe": [{"name": "BTC"}]}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(cast(Any, raw_data))
    # The error will be about expecting a list, not a dict
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    # Test with correct outer list but invalid inner structure
    raw_data_inner = [
        {"universe": [{"name": "BTC"}]},
        ["invalid_asset_ctx_item"],  # AssetCtx should be a dict
    ]
    with pytest.raises(APIError) as exc_info_inner:
        HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
            cast(Any, raw_data_inner)
        )
    assert exc_info_inner.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed" in exc_info_inner.value.message
    assert isinstance(exc_info_inner.value.original_exception, ValidationError)


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
    """Test handling a valid raw open orders response (list of order dicts)."""
    raw_data = [
        {
            "coin": "ETH",
            "limitPx": "3000.0",
            "oid": 98765,
            "origSz": "1.5",
            "reduceOnly": False,
            "side": "A",
            "sz": "1.5",
            "timestamp": 1678889100000,
            "cloid": None,
        },
        {
            "coin": "BTC",
            "limitPx": "54000.0",
            "oid": 98766,
            "origSz": "0.1",
            "reduceOnly": True,
            "side": "B",
            "sz": "0.1",
            "timestamp": 1678889110000,
            "cloid": "myClientOrderId123",
        },
    ]
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    response_wrapper: HyperliquidRawOpenOrdersResponse = (
        HyperliquidResponseHandler.handle_info_open_orders_response(
            cast(Any, raw_data), user_address=user_address_placeholder
        )
    )
    assert isinstance(response_wrapper, HyperliquidRawOpenOrdersResponse)
    orders = response_wrapper.root
    assert isinstance(orders, list)
    assert len(orders) == 2
    assert isinstance(orders[0], HyperliquidRawOpenOrder)
    assert orders[0].order.oid == 98765
    assert orders[0].order.asset == "ETH"
    assert orders[1].order.side == "B"
    assert orders[1].order.cloid == "myClientOrderId123"


def test_handle_info_open_orders_response_invalid_type() -> None:
    """Test handling open orders response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_open_orders_response(
            cast(Any, raw_data), user_address=user_address_placeholder
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "open orders" in exc_info.value.message


def test_handle_info_open_orders_response_invalid_item_type() -> None:
    """Test handling open orders list containing a non-dict item."""
    raw_data = [
        {
            "order": {
                "coin": "ETH",
                "limitPx": "3000.0",
                "oid": 98765,
                "origSz": "1.5",
                "reduceOnly": False,
                "side": "A",
                "sz": "1.5",
                "timestamp": 1678889100000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "1.5",
                "status": "open",
                "statusTimestamp": 1678889100000,
            },
            "trigger": None,
        },
        "not_an_order_dict",
    ]
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    with patch("cyberdelta.apis.hyperliquid.hl_response_handler.logger.warning") as mock_log:
        response_wrapper: HyperliquidRawOpenOrdersResponse = (
            HyperliquidResponseHandler.handle_info_open_orders_response(
                cast(Any, raw_data), user_address=user_address_placeholder
            )
        )
        orders = response_wrapper.root
        assert len(orders) == 1
        assert orders[0].order.oid == 98765
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_info_open_orders_response_item_validation_error() -> None:
    """Test handling open orders list with an item failing validation."""
    raw_data = [
        {
            "order": {
                "coin": "ETH",
                "limitPx": "3000.0",
                "oid": 98765,
                "origSz": "1.5",
                "reduceOnly": False,
                "side": "A",
                "sz": "1.5",
                "timestamp": 1678889100000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "1.5",
                "status": "open",
                "statusTimestamp": 1678889100000,
            },
            "trigger": None,
        },
        {
            "order": {
                "coin": "BTC",
                "oid": 98766,
                "origSz": "0.1",
                "reduceOnly": True,
                "side": "B",
                "sz": "0.1",
                "timestamp": 1678889110000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "remainingSz": "0.1",
                "status": "open",
                "statusTimestamp": 1678889110000,
            },
            "trigger": None,
        },
    ]
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_open_orders_response(
            cast(Any, raw_data), user_address=user_address_placeholder
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for open orders" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_info_user_fills_response_valid() -> None:
    """Test handling a valid raw user fills response (list of fill dicts)."""
    raw_data = [
        {
            "coin": "BTC",
            "px": "55000.0",
            "sz": "0.01",
            "side": "B",
            "time": 1678889200000,
            "startPosition": "0.0",
            "dir": "Open Long",
            "closedPnl": "0.0",
            "hash": "fillHash1",
            "oid": 10001,
            "crossed": True,
            "fee": "0.55",
            "liquidationMarkPx": None,
            "tid": "tradeId1",
            "cloid": None,
        },
        {
            "coin": "ETH",
            "px": "3000.0",
            "sz": "0.5",
            "side": "A",
            "time": 1678889210000,
            "startPosition": "1.0",
            "dir": "Close Short",
            "closedPnl": "50.0",
            "hash": "fillHash2",
            "oid": 10002,
            "crossed": False,
            "fee": "1.50",
            "liquidationMarkPx": "2800.0",
            "tid": "tradeId2",
            "cloid": "clientFill002",
        },
    ]
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    # Expect a list of wrapper models
    validated_fills_list: list[HyperliquidRawUserFillsResponse] = (
        HyperliquidResponseHandler.handle_info_user_fills_response(
            cast(Any, raw_data), user_address=user_address_placeholder
        )
    )
    assert isinstance(validated_fills_list, list)
    assert len(validated_fills_list) == 2
    # Check the first wrapper and its root list/item
    assert isinstance(validated_fills_list[0], HyperliquidRawUserFillsResponse)
    assert isinstance(validated_fills_list[0].root, list)
    assert len(validated_fills_list[0].root) == 1
    fill_item_0 = validated_fills_list[0].root[0]  # Access item within the root list
    assert isinstance(fill_item_0, HyperliquidRawUserFill)
    assert fill_item_0.coin == "BTC"
    assert fill_item_0.oid == 10001
    # Check the second wrapper and its root list/item
    assert isinstance(validated_fills_list[1], HyperliquidRawUserFillsResponse)
    assert isinstance(validated_fills_list[1].root, list)
    assert len(validated_fills_list[1].root) == 1
    fill_item_1 = validated_fills_list[1].root[0]  # Access item within the root list
    assert isinstance(fill_item_1, HyperliquidRawUserFill)
    assert fill_item_1.side == "A"
    assert fill_item_1.cloid == "clientFill002"


def test_handle_info_user_fills_response_invalid_type() -> None:
    """Test handling user fills response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_user_fills_response(
            cast(Any, raw_data), user_address=user_address_placeholder
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "user fills" in exc_info.value.message


def test_handle_info_user_fills_response_invalid_item_type() -> None:
    """Test handling user fills list containing a non-dict item."""
    raw_data = [
        {
            "coin": "BTC",
            "px": "55000.0",
            "sz": "0.01",
            "side": "B",
            "time": 1678889200000,
            "startPosition": "0.0",
            "dir": "Open Long",
            "closedPnl": "0.0",
            "hash": "fillHash1",
            "oid": 10001,
            "crossed": True,
            "fee": "0.55",
            "tid": "tradeId1",
        },
        "not_a_fill_dict",
    ]
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    with patch("cyberdelta.apis.hyperliquid.hl_response_handler.logger.warning") as mock_log:
        validated_fills_list: list[HyperliquidRawUserFillsResponse] = (
            HyperliquidResponseHandler.handle_info_user_fills_response(
                cast(Any, raw_data), user_address=user_address_placeholder
            )
        )
        assert len(validated_fills_list) == 1
        assert isinstance(validated_fills_list[0].root, list)
        assert len(validated_fills_list[0].root) == 1
        assert validated_fills_list[0].root[0].oid == 10001  # Correct access
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_info_user_fills_response_item_validation_error() -> None:
    """Test handling user fills list with an item failing validation."""
    raw_data = [
        {
            "coin": "BTC",
            "px": "55000.0",
            "sz": "0.01",
            "side": "B",
            "time": 1678889200000,
            "startPosition": "0.0",
            "dir": "Open Long",
            "closedPnl": "0.0",
            "hash": "fillHash1",
            "oid": 10001,
            "crossed": True,
            "fee": "0.55",
            "tid": "tradeId1",
        },
        {
            "coin": "ETH",
            # Missing required 'px' field
            "sz": "0.5",
            "side": "A",
            "time": 1678889210000,
            "startPosition": "1.0",
            "dir": "Close Short",
            "closedPnl": "50.0",
            "hash": "fillHash2",
            "oid": 10002,
            "crossed": False,
            "fee": "1.50",
            "tid": "tradeId2",
        },
    ]
    user_address_placeholder = "0x1234567890abcdef1234567890abcdef12345678"
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_user_fills_response(
            cast(Any, raw_data), user_address=user_address_placeholder
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # The error now occurs when validating a single item inside the loop
    assert "validation failed for single user fill item" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


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
            "hash": "tradeHash1",
            "tid": "trade1",
        },
        {
            "coin": "BTC",
            "side": "A",
            "px": "56105.0",
            "sz": "0.02",
            "time": 1678889401000,
            "hash": "tradeHash2",
            "tid": "trade2",
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
            "hash": "tradeHash1",
            "tid": "trade1",
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
        assert trades[0].hash == "tradeHash1"
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
            "hash": "tradeHash1",
            "tid": "trade1",
        },
        {
            "coin": "BTC",
            "side": "A",
            # Missing required 'px' field
            "sz": "0.02",
            "time": 1678889401000,
            "hash": "tradeHash2",
            "tid": "trade2",
        },
    ]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_recent_trades_response(
            cast(Any, raw_data), symbol="BTC"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for single recent trade item" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


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


def test_handle_info_order_status_response_valid() -> None:
    """Test handling a valid raw order status response."""
    raw_data = {
        "order": {
            "coin": "ETH",
            "limitPx": "3000.0",
            "oid": 12345,
            "origSz": "1.0",
            "reduceOnly": False,
            "side": "B",
            "sz": "0.5",  # Partially filled
            "timestamp": 1678889600000,
            "orderType": {"limit": {"tif": "Gtc"}},
            "remainingSz": "0.5",
            "status": "open",
            "statusTimestamp": 1678889601000,
            "cloid": "myOrderStatusClient1",
        }
    }
    order_status_response: HyperliquidRawOrderStatusResponse = (
        HyperliquidResponseHandler.handle_info_order_status_response(
            cast(Any, raw_data), user_address="0xTestUser", order_id=12345
        )
    )
    assert isinstance(order_status_response, HyperliquidRawOrderStatusResponse)
    assert isinstance(order_status_response.order, HyperliquidRawOrder)
    assert order_status_response.order.oid == 12345
    assert order_status_response.order.asset == "ETH"
    assert order_status_response.order.status == "open"


def test_handle_info_order_status_response_invalid_type() -> None:
    """Test handling order status response with invalid type (list instead of dict)."""
    raw_data = [{"order": "invalid"}]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_order_status_response(
            cast(Any, raw_data), user_address="0xTestUser", order_id=12345
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "Order Status for user 0xTestUser, order 12345" in exc_info.value.message


def test_handle_info_order_status_response_validation_error_missing_order_key() -> None:
    """Test handling order status response dict missing the 'order' key."""
    raw_data = {"not_order": "data"}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_order_status_response(
            cast(Any, raw_data), user_address="0xTestUser", order_id=12345
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for info (Order Status" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_info_order_status_response_validation_error_invalid_nested_order() -> None:
    """Test handling order status response with an invalid nested order dict."""
    raw_data = {
        "order": {
            "coin": "ETH",
            "limitPx": "3000.0",
            # oid is missing, which is required for HyperliquidRawOrder
            "origSz": "1.0",
            "reduceOnly": False,
            "side": "B",
            "sz": "0.5",
            "timestamp": 1678889600000,
        }
    }
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_order_status_response(
            cast(Any, raw_data), user_address="0xTestUser", order_id=12345
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for info (Order Status" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_info_vault_details_response_valid() -> None:
    """Test handling a valid raw vault details response."""
    # Corrected mock data structure based on actual models
    raw_data = {
        "name": "Test Vault",
        "description": "A vault for testing purposes.",
        "allowDeposits": True,  # Corrected field
        "alwaysCloseOnWithdraw": False,
        "creator": "0xCreatorAddress",
        "vaultAddress": "0xThisVaultAddress",
        "maxBalance": "100000.0",
        "currBalance": "10000.0",
        "totalPnl": "500.0",
        "allTimePnl": "1000.0",
        "performanceHistory": [
            {"time": 1678880000000, "pnl": "-500.0"},  # Corrected fields
            {"time": 1678881000000, "pnl": "500.0"},
        ],
        "userEquities": [
            {
                "user": "0xUser1",
                "equity": "5000.0",
                "allTimePnl": "200.0",
                "daysFollowing": 10,
                "vaultEntryTime": 1678800000000,
                "lockupUntil": 1679880000000,
            }
        ],
        "maxDistributable": "500.0",
        "maxWithdrawable": "4500.0",
        "isClosed": False,
        "relationship": {
            "type": "master_slave",
            "data": {  # Nested data field
                "master": "0xMasterVaultAddress",
                "childAddresses": ["0xSlave1", "0xSlave2"],
            },
        },
        # Removed fields not directly in VaultDetailsResponse or its direct children for clarity
        # e.g. maxUserLossLimit, minUserDepositLimit, totalValueLocked, totalUserEquity,
        # onlyVaultDepositors, vaultFee, rawVaultFee, manager, requireWhitelisting
        # These might be part of a more complex actual response or a different endpoint.
        # For this handler, we test against the defined HyperliquidRawVaultDetailsResponse.
    }
    vault_details: HyperliquidRawVaultDetailsResponse = (
        HyperliquidResponseHandler.handle_info_vault_details_response(
            cast(Any, raw_data), user_address="0xRequestingUser"
        )
    )
    assert isinstance(vault_details, HyperliquidRawVaultDetailsResponse)
    assert vault_details.name == "Test Vault"
    assert vault_details.allow_deposits is True  # Corrected assertion
    assert len(vault_details.performance_history) == 2
    assert vault_details.performance_history[0].pnl == "-500.0"  # Corrected assertion
    assert vault_details.relationship.data.master == "0xMasterVaultAddress"  # Corrected assertion


def test_handle_info_vault_details_response_invalid_type() -> None:
    """Test handling vault details response with invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_vault_details_response(
            cast(Any, raw_data), user_address="0xRequestingUser"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "Vault Details for 0xRequestingUser" in exc_info.value.message


def test_handle_info_vault_details_response_validation_error() -> None:
    """Test handling vault details response dict failing validation."""
    raw_data: dict[str, Any] = {
        "name": "Test Vault Error Case",
        "description": "A vault for testing validation errors.",
        "currBalance": "100.0",
        "totalPnl": "10.0",
        "allTimePnl": "20.0",
        "performanceHistory": [
            {"time": 1678880000000, "pnl": "-5.0"},
            {"time": 1678881000000},
        ],
        "relationship": {"type": "master_only", "data": {"master": "0xMasterOnly"}},
    }
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_info_vault_details_response(
            cast(Any, raw_data), user_address="0xRequestingUser"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for info (Vault Details" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_exchange_response_valid_success() -> None:
    """Test handling a valid, successful raw exchange action response."""
    raw_data = {
        "status": "ok",
        "data": {"type": "order", "statuses": [{"resting": {"oid": 12345}}]},
    }
    exchange_response: HyperliquidRawExchangeResponse = (
        HyperliquidResponseHandler.handle_exchange_response(
            cast(Any, raw_data), action_type="order"
        )
    )
    assert isinstance(exchange_response, HyperliquidRawExchangeResponse)
    assert exchange_response.status == "ok"
    assert exchange_response.data is not None
    assert exchange_response.data.type == "order"
    assert isinstance(exchange_response.data.statuses, list)
    assert len(exchange_response.data.statuses) == 1
    status_obj = exchange_response.data.statuses[0]
    assert isinstance(status_obj, HyperliquidRawExchangeStatusObject)
    assert status_obj.resting is not None
    assert status_obj.resting.oid == 12345


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
    assert "validation failed for Exchange Action (order)" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)
    # Check that the Pydantic error mentions the status field constraint
    assert "Input 'error' is not a valid literal" in str(exc_info.value.original_exception)


def test_handle_exchange_response_invalid_top_level_type() -> None:
    """Test handling exchange response with invalid top-level type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_exchange_response(
            cast(Any, raw_data), action_type="order"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "Exchange Action (order)" in exc_info.value.message


def test_handle_exchange_response_validation_error_ok_missing_data() -> None:
    """Test handling exchange response with status='ok' but missing 'data' field."""
    raw_data = {
        "status": "ok",
        # Missing 'data' field which is expected when status is "ok" by the model
    }
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_exchange_response(
            cast(Any, raw_data), action_type="order"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for Exchange Action (order)" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_query_order_history_response_valid() -> None:
    """Test handling a valid raw query order history response."""
    raw_data = [
        {
            "order": {
                "coin": "ETH",
                "limitPx": "2900.0",
                "oid": 7001,
                "origSz": "1.0",
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
                "coin": "BTC",
                "limitPx": "53000.0",
                "oid": 7002,
                "origSz": "0.5",
                "reduceOnly": True,
                "side": "A",
                "sz": "0.0",
                "timestamp": 1678891000000,
                "orderType": {"limit": {"tif": "Alo"}},
                "remainingSz": "0.0",
                "status": "canceled",
                "statusTimestamp": 1678891001000,
                "cloid": None,
            }
        },
    ]
    # The handler returns a list of validated HyperliquidRawOrderStatusResponse objects
    order_history: list[HyperliquidRawOrderStatusResponse] = (
        HyperliquidResponseHandler.handle_query_order_history_response(
            cast(Any, raw_data), user_address="0xHistoryUser"
        )
    )
    assert isinstance(order_history, list)
    assert len(order_history) == 2
    assert isinstance(order_history[0], HyperliquidRawOrderStatusResponse)
    assert order_history[0].order.oid == 7001
    assert order_history[0].order.status == "filled"
    assert isinstance(order_history[1], HyperliquidRawOrderStatusResponse)
    assert order_history[1].order.asset == "BTC"
    assert order_history[1].order.cloid is None


def test_handle_query_order_history_response_invalid_type() -> None:
    """Test handling query order history response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_query_order_history_response(
            cast(Any, raw_data), user_address="0xHistoryUser"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "Order History for 0xHistoryUser" in exc_info.value.message


def test_handle_query_order_history_response_invalid_item_type() -> None:
    """Test handling query order history list containing a non-dict item."""
    raw_data = [
        {
            "order": {
                "coin": "ETH",
                "limitPx": "2900.0",
                "oid": 7001,
                "origSz": "1.0",
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
        order_history: list[HyperliquidRawOrderStatusResponse] = (
            HyperliquidResponseHandler.handle_query_order_history_response(
                cast(Any, raw_data), user_address="0xHistoryUser"
            )
        )
        assert len(order_history) == 1
        assert order_history[0].order.oid == 7001
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_query_order_history_response_item_validation_error() -> None:
    """Test handling query order history list with an item failing validation."""
    raw_data = [
        {
            "order": {
                "coin": "ETH",
                "limitPx": "2900.0",
                "oid": 7001,
                "origSz": "1.0",
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
                "coin": "BTC",
                "limitPx": "53000.0",
                "origSz": "0.5",
                "reduceOnly": True,
                "side": "A",
                "sz": "0.0",
                "timestamp": 1678891000000,
            }
        },
    ]
    with pytest.raises(APIError) as exc_info:
        HyperliquidResponseHandler.handle_query_order_history_response(
            cast(Any, raw_data), user_address="0xHistoryUser"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for single order history item" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)
