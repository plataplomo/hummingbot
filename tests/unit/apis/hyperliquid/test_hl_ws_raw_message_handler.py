"""
Unit tests for HyperliquidWsRawMessageHandler.
"""

from typing import Any

import pytest

# Removed: from decimal import Decimal
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler import HyperliquidWsRawMessageHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
)  # For user order data
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsOrderUpdate,
    HyperliquidRawWsPositionUpdateEvent,
    HyperliquidRawWsTradeEvent,
    # Removed: HyperliquidRawBookLevel
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


# Test functions are now standalone, not part of a class
def test_handle_l2book_payload_valid() -> None:
    """Test handle_l2book_payload with valid data."""
    valid_payload = {
        "coin": "ETH",
        "levels": [
            [{"px": "1800.0", "sz": "10.5", "n": 2}],  # Bids
            [{"px": "1800.5", "sz": "5.2", "n": 1}],  # Asks
        ],
        "time": 1678886400000,
    }
    expected_model = HyperliquidRawWsBookUpdate.model_validate(valid_payload)
    result = HyperliquidWsRawMessageHandler.handle_l2book_payload(valid_payload)
    assert result == expected_model


def test_handle_l2book_payload_invalid() -> None:
    """Test handle_l2book_payload with invalid data (missing 'coin')."""
    invalid_payload: dict[str, Any] = {"levels": [[], []], "time": 1678886400000}
    with pytest.raises(APIError) as excinfo:
        HyperliquidWsRawMessageHandler.handle_l2book_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)


def test_handle_public_trades_payload_valid() -> None:
    """Test handle_public_trades_payload with a list of valid trade data."""
    valid_payload_list = [
        {
            "coin": "BTC",
            "px": "28000.0",
            "sz": "0.5",
            "side": "B",
            "time": 1678886400100,
            "hash": "0xabc",
        },
        {
            "coin": "BTC",
            "px": "28000.5",
            "sz": "0.2",
            "side": "A",
            "time": 1678886400200,
            "hash": "0xdef",
        },
    ]
    expected_models = [HyperliquidRawWsTradeEvent.model_validate(p) for p in valid_payload_list]
    result = HyperliquidWsRawMessageHandler.handle_public_trades_payload(valid_payload_list)
    assert result == expected_models


def test_handle_public_trades_payload_empty() -> None:
    """Test handle_public_trades_payload with an empty list."""
    result = HyperliquidWsRawMessageHandler.handle_public_trades_payload([])
    assert result == []


def test_handle_public_trades_payload_invalid_item() -> None:
    """Test handle_public_trades_payload with a list containing one invalid trade."""
    invalid_payload_list = [
        {
            "coin": "BTC",
            "px": "28000.0",
            "sz": "0.5",
            "side": "B",
            "time": 1678886400100,
            "hash": "0xabc",
        },
        {
            "coin": "BTC",
            "sz": "0.2",
            "side": "A",
            "time": 1678886400200,
            "hash": "0xdef",
        },  # Missing 'px'
    ]
    with pytest.raises(APIError) as excinfo:
        HyperliquidWsRawMessageHandler.handle_public_trades_payload(invalid_payload_list)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "Error in public trades list at index 1" in excinfo.value.message
    assert isinstance(excinfo.value.original_exception, ValidationError)


# --- User Fill Event ---
def test_handle_user_fill_event_payload_valid() -> None:
    """Test handle_user_fill_event_payload with valid data."""
    valid_payload = {
        "coin": "ETH",
        "px": "1800.0",
        "sz": "1.0",
        "side": "B",
        "time": 1678886400000,
        "hash": "0x123",
        "oid": 12345,
        "cloid": "clientOrder1",
        "isMaker": True,
    }
    expected_model = HyperliquidRawWsFillEvent.model_validate(valid_payload)
    result = HyperliquidWsRawMessageHandler.handle_user_fill_event_payload(valid_payload)
    assert result == expected_model


def test_handle_user_fill_event_payload_invalid() -> None:
    """Test handle_user_fill_event_payload with invalid data (px not decimal string)."""
    invalid_payload = {
        "coin": "ETH",
        "px": 1800.0,
        "sz": "1.0",
        "side": "B",  # px is float
        "time": 1678886400000,
        "hash": "0x123",
        "oid": 12345,
        "isMaker": False,
    }
    with pytest.raises(APIError) as excinfo:
        HyperliquidWsRawMessageHandler.handle_user_fill_event_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)


# --- User Order Event (Inner Detail) --- using HyperliquidRawOrder
def test_handle_user_order_event_payload_valid() -> None:
    """Test handle_user_order_event_payload with valid inner order data."""
    # This is the structure HyperliquidRawOrder expects
    valid_payload = {
        "order": {
            "coin": "ETH",
            "side": "A",
            "limitPx": "1900.0",
            "sz": "0.5",
            "origSz": "0.5",
            "reduceOnly": False,
            "timestamp": 1678886400000,
            "tif": "Gtc",
            "cloid": "myOrder123",
        },
        "status": "filled",
        "statusTimestamp": 1678886400100,
        "oid": 54321,
        "totalPx": "1899.5",
        "totalSz": "0.5",
    }
    expected_model = HyperliquidRawOrder.model_validate(valid_payload)
    result = HyperliquidWsRawMessageHandler.handle_user_order_event_payload(valid_payload)
    assert result == expected_model


def test_handle_user_order_event_payload_invalid() -> None:
    """Test handle_user_order_event_payload with invalid inner order data."""
    invalid_payload = {
        "order": {"coin": "ETH", "side": "A", "limitPx": "1900.0"},  # Missing sz, etc.
        "status": "open",
    }
    with pytest.raises(APIError) as excinfo:
        HyperliquidWsRawMessageHandler.handle_user_order_event_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)


# --- User Order Update Wrapper --- using HyperliquidRawWsOrderUpdate
def test_handle_user_order_update_wrapper_payload_valid() -> None:
    """Test handle_user_order_update_wrapper_payload with valid wrapper data."""
    valid_payload = {
        "eventType": "order",  # HyperliquidRawWsOrderUpdate expects 'eventType'
        "data": {  # The inner data for the order
            "order": {
                "coin": "ETH",
                "side": "B",
                "limitPx": "1800",
                "sz": "1",
                "origSz": "1",
                "reduceOnly": False,
                "timestamp": 1678886400000,
                "tif": "Gtc",
            },
            "status": "open",
            "oid": 123,
            "statusTimestamp": 1678886400100,
        },
    }
    expected_model = HyperliquidRawWsOrderUpdate.model_validate(valid_payload)
    result = HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload(valid_payload)
    assert result == expected_model


def test_handle_user_order_update_wrapper_payload_invalid() -> None:
    """Test handle_user_order_update_wrapper_payload with invalid wrapper (missing data)."""
    invalid_payload = {"eventType": "order"}
    with pytest.raises(APIError) as excinfo:
        HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)


# --- User Position Update Event ---
def test_handle_user_position_update_event_payload_valid() -> None:
    """Test handle_user_position_update_event_payload with valid data."""
    valid_payload = {
        "asset": "ETH",
        "position": {
            "type": "isolated",
            "amount": "2.5",
            "entryPx": "1750.0",
            "unrealizedPnl": "125.0",
            "liquidationPx": "1600.0",
            "marginUsed": "500.0",
        },
        "time": 1678886400000,
    }
    expected_model = HyperliquidRawWsPositionUpdateEvent.model_validate(valid_payload)
    result = HyperliquidWsRawMessageHandler.handle_user_position_update_event_payload(valid_payload)
    assert result == expected_model


def test_handle_user_position_update_event_payload_invalid() -> None:
    """Test handle_user_position_update_event_payload with invalid data (missing position)."""
    invalid_payload = {"asset": "ETH", "time": 1678886400000}
    with pytest.raises(APIError) as excinfo:
        HyperliquidWsRawMessageHandler.handle_user_position_update_event_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)


# --- All Mids Event ---
def test_handle_all_mids_payload_valid() -> None:
    """Test handle_all_mids_payload with valid data."""
    valid_payload = {
        "BTC": "60000.123",
        "ETH": "3000.45",
        "SOL_PERP": "150.99",  # Example with a perp symbol if allowed by RawAssetString64HL
    }
    expected_model = HyperliquidRawAllMids.model_validate(valid_payload)
    result = HyperliquidWsRawMessageHandler.handle_all_mids_payload(valid_payload)
    assert result.model_dump() == expected_model.model_dump()  # Compare dicts from RootModel


def test_handle_all_mids_payload_invalid_not_dict() -> None:
    """Test handle_all_mids_payload with non-dictionary payload."""
    invalid_payload_list: list[str] = ["not_a_dict"]
    with pytest.raises(APIError) as excinfo:
        # Typing ignored as the function expects a dict, but we are testing invalid input.
        HyperliquidWsRawMessageHandler.handle_all_mids_payload(invalid_payload_list)  # type: ignore
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)
    # Check that the Pydantic error message indicates it expected a dictionary/mapping
    assert "Input should be a valid dictionary" in str(
        excinfo.value.original_exception
    ) or "value is not a valid dict" in str(excinfo.value.original_exception)  # Pydantic v1/v2 diff


def test_handle_all_mids_payload_invalid_value_type() -> None:
    """Test handle_all_mids_payload with invalid value type (not string decimal)."""
    invalid_payload = {
        "BTC": "60000.0",
        "ETH": 3000,  # Should be string "3000"
    }
    with pytest.raises(APIError) as excinfo:
        HyperliquidWsRawMessageHandler.handle_all_mids_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)
    # Check for specific error related to the value for "ETH"
    error_details = excinfo.value.original_exception.errors(include_input=False)
    assert any(
        err["loc"] == ("ETH",) and "Input should be a valid string" in err["msg"]
        for err in error_details
    )


def test_handle_all_mids_payload_invalid_key_type() -> None:
    """Test handle_all_mids_payload with invalid key type (e.g. too long)."""
    # RawAssetString64HL implies max length 64 for asset names
    long_asset_name = "A" * 65
    invalid_payload = {
        long_asset_name: "60000.0",
    }
    with pytest.raises(APIError) as excinfo:
        HyperliquidWsRawMessageHandler.handle_all_mids_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)
    error_details = excinfo.value.original_exception.errors(include_input=False)
    assert any(
        err["loc"] == (long_asset_name,)
        and "ensure this value has at most 64 characters" in err["msg"]
        for err in error_details
    )
