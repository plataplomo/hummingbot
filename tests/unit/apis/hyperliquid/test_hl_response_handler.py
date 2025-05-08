"""Unit tests for HyperliquidResponseHandler."""

from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
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


# TODO: Add tests for other handler methods:
# - handle_info_user_state_response
# - handle_info_open_orders_response
# - handle_info_user_fills_response
# - handle_info_funding_rate_response (should take dict input)
# - handle_info_l2_book_response
# - handle_info_recent_trades_response
# - handle_info_candle_snapshot_response
# - handle_info_order_status_response (handles list, string, dict cases)
# - handle_info_spot_asset_contexts_response
# - handle_info_vault_details_response
# - handle_query_order_history_response
