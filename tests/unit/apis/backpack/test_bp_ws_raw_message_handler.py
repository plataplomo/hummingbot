"""Unit tests for BackpackWsRawMessageHandler.
"""

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_ws_raw_message_handler import BackpackWsRawMessageHandler
from cyberdelta.apis.backpack.models import (
    BackpackRawOrderUpdate,
    BackpackRawPositionUpdate,
    BackpackRawTradeEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


# Test functions are now standalone
def test_handle_depth_payload_valid() -> None:
    """Test handle_depth_payload with valid data."""
    # Payload for BackpackRawDepthUpdateEvent
    valid_payload = {
        "e": "depthUpdate",
        "E": 1678886400000,
        "s": "SOL_USDC",
        "lastUpdateId": "123456789",
        "b": [["100.0", "1.0"]],  # Bids
        "a": [["101.0", "2.0"]],  # Asks
    }
    expected_model = BackpackRawDepthUpdateEvent.model_validate(valid_payload)
    result = BackpackWsRawMessageHandler.handle_depth_payload(valid_payload)
    assert result == expected_model


def test_handle_depth_payload_invalid() -> None:
    """Test handle_depth_payload with invalid data (missing field)."""
    invalid_payload = {
        "e": "depthUpdate",
        "E": 1678886400000,
        "s": "SOL_USDC",
        "lastUpdateId": "123456789",
        "a": [["101.0", "2.0"]],
    }  # Missing 'b' (bids)
    with pytest.raises(APIError) as excinfo:
        BackpackWsRawMessageHandler.handle_depth_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)


# --- Ticker --- (Using BackpackRawTickerEvent)
def test_handle_ticker_payload_valid() -> None:
    """Test handle_ticker_payload with valid data."""
    # Payload for BackpackRawTickerEvent
    valid_payload = {
        "e": "ticker",
        "E": 1678886400000,
        "s": "SOL_USDC",
        "lastPrice": "150.55",
        "high": "151.00",
        "low": "148.50",
        "o": "149.00",
        "volume": "10000.0",
        "quoteVolume": "1500000.0",
        "priceChangePercent": "0.12",
    }
    expected_model = BackpackRawTickerEvent.model_validate(valid_payload)
    result = BackpackWsRawMessageHandler.handle_ticker_payload(valid_payload)
    assert result == expected_model


def test_handle_ticker_payload_invalid() -> None:
    """Test handle_ticker_payload with invalid data (wrong type for 'p')."""
    invalid_payload = {
        "e": "ticker",
        "E": 1678886400000,
        "s": "SOL_USDC",
        "lastPrice": 150.55,
        "high": "151.00",
        "low": "148.50",
        "o": "149.00",
        "volume": "10000.0",
        "quoteVolume": "1500000.0",
        "priceChangePercent": "0.12",
    }
    with pytest.raises(APIError) as excinfo:
        BackpackWsRawMessageHandler.handle_ticker_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)


# --- Trade Event --- (BackpackRawTradeEvent)
def test_handle_trade_event_payload_valid() -> None:
    """Test handle_trade_event_payload with valid data."""
    # Based on BackpackRawTradeEvent in bp_raw_trade.py
    valid_payload = {
        "e": "trade",  # event_type
        "E": 1678886400000,  # event_time
        "s": "SOL_USDC",  # symbol
        "p": "150.00",  # price
        "q": "1.5",  # quantity
        "b": "buyerOrderId123",  # buyer_order_id
        "a": "sellerOrderId456",  # seller_order_id
        "t": "tradeId789",  # trade_id
        "T": 1678886400001,  # engine_timestamp
        "m": True,  # is_buyer_the_maker
    }
    expected_model = BackpackRawTradeEvent.model_validate(valid_payload)
    result = BackpackWsRawMessageHandler.handle_trade_event_payload(valid_payload)
    assert result == expected_model


def test_handle_trade_event_payload_invalid() -> None:
    """Test handle_trade_event_payload with invalid data (missing 'p' price)."""
    invalid_payload = {
        "e": "trade",
        "E": 1678886400000,
        "s": "SOL_USDC",
        "q": "1.5",
        "b": "buyerOrderId123",
        "a": "sellerOrderId456",
        "t": "tradeId789",
        "T": 1678886400001,
        "m": True,
    }
    with pytest.raises(APIError) as excinfo:
        BackpackWsRawMessageHandler.handle_trade_event_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)


# --- Order Update --- (BackpackRawOrderUpdate)
def test_handle_order_update_payload_valid() -> None:
    """Test handle_order_update_payload with valid data."""
    # Based on BackpackRawOrderUpdate in bp_raw_order.py
    valid_payload = {
        "e": "orderAccepted",  # event_type
        "E": 1678886400000,  # event_time
        "s": "SOL_USDC",  # symbol
        "c": "clientOrderId123",  # client_order_id
        "S": "Bid",  # side
        "o": "LIMIT",  # order_type
        "f": "GTC",  # time_in_force
        "q": "10.0",  # quantity
        "p": "149.00",  # price
        "X": "NEW",  # order_status
    }
    expected_model = BackpackRawOrderUpdate.model_validate(valid_payload)
    result = BackpackWsRawMessageHandler.handle_order_update_payload(valid_payload)
    assert result == expected_model


def test_handle_order_update_payload_invalid() -> None:
    """Test handle_order_update_payload with invalid data (status missing)."""
    invalid_payload = {
        "e": "orderAccepted",
        "E": 1678886400000,
        "s": "SOL_USDC",
        "c": "clientOrderId123",
        "S": "Bid",
        "o": "LIMIT",
        "f": "GTC",
        "q": "10.0",
        "p": "149.00",
    }
    with pytest.raises(APIError) as excinfo:
        BackpackWsRawMessageHandler.handle_order_update_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)


# --- Position Update --- (BackpackRawPositionUpdate)
def test_handle_position_update_payload_valid() -> None:
    """Test handle_position_update_payload with valid data."""
    # Based on BackpackRawPositionUpdate in bp_raw_position.py
    # This model expects: event_type, event_time, symbol, break_event_price, entry_price etc.
    # Payload example requires specific aliases like 'e', 'E', 's'
    valid_payload = {
        "e": "positionUpdate",
        "E": 1678886400000,
        "s": "SOL_USDC",
        "b": "150.0",  # break_event_price
        "B": "149.5",  # entry_price
        "l": "140.0",  # liquidation_price
        "f": "0.01",  # initial_margin_fraction
        "M": "150.1",  # mark_price
        "m": "0.005",  # maintenance_margin_fraction
        "q": "5.0",  # net_quantity
        "Q": "5.0",  # net_exposure_quantity
        "n": "750.5",  # net_exposure_notional
    }
    expected_model = BackpackRawPositionUpdate.model_validate(valid_payload)
    result = BackpackWsRawMessageHandler.handle_position_update_payload(valid_payload)
    assert result == expected_model


def test_handle_position_update_payload_invalid() -> None:
    """Test handle_position_update_payload with invalid data (symbol missing)."""
    invalid_payload = {
        "e": "positionUpdate",
        "E": 1678886400000,
        "b": "150.0",
        "B": "149.5",
        "l": "140.0",
        "f": "0.01",
        "M": "150.1",
        "m": "0.005",
        "q": "5.0",
        "Q": "5.0",
        "n": "750.5",
    }
    with pytest.raises(APIError) as excinfo:
        BackpackWsRawMessageHandler.handle_position_update_payload(invalid_payload)
    assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert isinstance(excinfo.value.original_exception, ValidationError)
