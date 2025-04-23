import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawBookLevel
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawPositionInfo
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsOrderUpdate,
    HyperliquidRawWsPositionUpdateEvent,
    HyperliquidRawWsTradeEvent,
)


def test_ws_fill_event_happy_path() -> None:
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": 1234567890,
        "hash": "abc123",
        "oid": 42,
        "cloid": "cl123",
        "isMaker": True,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.coin == "ETH"
    assert model.px == "3000.0"
    assert model.is_maker is True


def test_ws_fill_event_missing_required() -> None:
    obj: dict[str, object] = {"coin": "ETH", "px": "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_type_errors() -> None:
    obj: dict[str, object] = {
        "coin": 123,
        "px": 3000.0,
        "sz": 1.5,
        "side": 1,
        "time": "now",
        "hash": 123,
        "oid": "oid",
        "cloid": 123,
        "isMaker": "yes",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_constraint_errors() -> None:
    obj: dict[str, object] = {
        "coin": "E" * 65,
        "px": "NaN",
        "sz": "inf",
        "side": "X",
        "time": -1,
        "hash": "h" * 65,
        "oid": -1,
        "cloid": "c" * 65,
        "isMaker": False,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_optional_cloid() -> None:
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "A",
        "time": 1234567890,
        "hash": "abc123",
        "oid": 42,
        "isMaker": False,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.cloid is None


def test_ws_fill_event_extra_field() -> None:
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": 1234567890,
        "hash": "abc123",
        "oid": 42,
        "cloid": "cl123",
        "isMaker": True,
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_adversarial_strings() -> None:
    obj: dict[str, object] = {
        "coin": "DROP TABLE users;",
        "px": "123.456",
        "sz": "789.012",
        "side": "A",
        "time": 1,
        "hash": "abc123",
        "oid": 1,
        "cloid": "cl123",
        "isMaker": False,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.coin == "DROP TABLE users;"


def test_ws_book_update_happy_path() -> None:
    obj: dict[str, object] = {
        "coin": "BTC",
        "levels": [
            [
                {"px": "30000.0", "sz": "1.0", "n": 2},
            ],
            [
                {"px": "30010.0", "sz": "0.5", "n": 1},
            ],
        ],
        "time": 1234567890,
    }
    model = HyperliquidRawWsBookUpdate.model_validate(obj)
    assert model.coin == "BTC"
    assert isinstance(model.levels[0][0], HyperliquidRawBookLevel)


def test_ws_book_update_invalid_levels() -> None:
    obj: dict[str, object] = {
        "coin": "BTC",
        "levels": [[], []],
        "time": 1234567890,
    }
    model = HyperliquidRawWsBookUpdate.model_validate(obj)
    assert model.levels == [[], []]
    # Now test wrong structure
    obj2: dict[str, object] = {
        "coin": "BTC",
        "levels": [[], [], []],
        "time": 1234567890,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsBookUpdate.model_validate(obj2)


def test_ws_book_update_type_errors() -> None:
    obj: dict[str, object] = {
        "coin": 123,
        "levels": "notalist",
        "time": "now",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsBookUpdate.model_validate(obj)


def test_ws_book_update_extra_field() -> None:
    obj: dict[str, object] = {
        "coin": "BTC",
        "levels": [[], []],
        "time": 1234567890,
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsBookUpdate.model_validate(obj)


def test_ws_book_update_only_bids_or_asks() -> None:
    # Only bids (should be rejected)
    obj: dict[str, object] = {
        "coin": "BTC",
        "levels": [[{"px": "30000.0", "sz": "1.0", "n": 2}]],
        "time": 1234567890,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsBookUpdate.model_validate(obj)
    # Only asks (should be rejected)
    obj2: dict[str, object] = {
        "coin": "BTC",
        "levels": [[], [{"px": "30010.0", "sz": "0.5", "n": 1}]],
        "time": 1234567890,
    }
    model = HyperliquidRawWsBookUpdate.model_validate(obj2)
    assert model.levels[0] == []
    assert isinstance(model.levels[1][0], HyperliquidRawBookLevel)


def test_ws_book_update_empty_lists() -> None:
    # Both bids and asks empty
    obj: dict[str, object] = {"coin": "BTC", "levels": [[], []], "time": 1234567890}
    model = HyperliquidRawWsBookUpdate.model_validate(obj)
    assert model.levels == [[], []]


def test_ws_trade_event_happy_path() -> None:
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "A",
        "time": 1234567890,
        "hash": "abc123",
    }
    model = HyperliquidRawWsTradeEvent.model_validate(obj)
    assert model.coin == "ETH"
    assert model.side == "A"


def test_ws_trade_event_missing_required() -> None:
    obj: dict[str, object] = {"coin": "ETH", "px": "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawWsTradeEvent.model_validate(obj)


def test_ws_trade_event_type_errors() -> None:
    obj: dict[str, object] = {
        "coin": 123,
        "px": 3000.0,
        "sz": 1.5,
        "side": 1,
        "time": "now",
        "hash": 123,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsTradeEvent.model_validate(obj)


def test_ws_trade_event_constraint_errors() -> None:
    obj: dict[str, object] = {
        "coin": "E" * 65,
        "px": "NaN",
        "sz": "inf",
        "side": "X",
        "time": -1,
        "hash": "h" * 65,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsTradeEvent.model_validate(obj)


def test_ws_trade_event_extra_field() -> None:
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": 1234567890,
        "hash": "abc123",
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsTradeEvent.model_validate(obj)


def test_ws_trade_event_side_lowercase_invalid() -> None:
    # side as lowercase or invalid
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "b",
        "time": 1234567890,
        "hash": "abc123",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsTradeEvent.model_validate(obj)
    obj2: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "X",
        "time": 1234567890,
        "hash": "abc123",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsTradeEvent.model_validate(obj2)
    obj3: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": " ",
        "time": 1234567890,
        "hash": "abc123",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsTradeEvent.model_validate(obj3)


def test_ws_order_update_happy_path() -> None:
    obj: dict[str, object] = {
        "eventType": "orderUpdate",
        "data": {"foo": "bar"},
    }
    model = HyperliquidRawWsOrderUpdate.model_validate(obj)
    assert model.event_type == "orderUpdate"
    assert model.data == {"foo": "bar"}


def test_ws_order_update_missing_required() -> None:
    obj: dict[str, object] = {"eventType": "orderUpdate"}
    with pytest.raises(ValidationError):
        HyperliquidRawWsOrderUpdate.model_validate(obj)


def test_ws_order_update_type_errors() -> None:
    obj: dict[str, object] = {"eventType": 123, "data": "notadict"}
    with pytest.raises(ValidationError):
        HyperliquidRawWsOrderUpdate.model_validate(obj)


def test_ws_order_update_empty_data() -> None:
    obj: dict[str, object] = {"eventType": "orderUpdate", "data": {}}
    with pytest.raises(ValidationError):
        HyperliquidRawWsOrderUpdate.model_validate(obj)


def test_ws_order_update_extra_field() -> None:
    obj: dict[str, object] = {"eventType": "orderUpdate", "data": {"foo": "bar"}, "foo": 1}
    with pytest.raises(ValidationError):
        HyperliquidRawWsOrderUpdate.model_validate(obj)


def test_ws_order_update_unknown_event_type() -> None:
    # eventType as unknown string
    obj: dict[str, object] = {"eventType": "unknownType", "data": {"foo": "bar"}}
    model = HyperliquidRawWsOrderUpdate.model_validate(obj)
    assert model.event_type == "unknownType"


def test_ws_position_update_event_happy_path() -> None:
    obj: dict[str, object] = {
        "asset": "ETH",
        "position": {
            "coin": "ETH",
            "entryPx": "3000.0",
            "leverage": {"type": "cross", "value": 10},
            "liquidationPx": "2900.0",
            "marginUsed": "100.0",
            "maxLeverage": 50,
            "positionValue": "150.0",
            "returnOnEquity": "0.1",
            "szi": "1.5",
            "unrealizedPnl": "10.0",
        },
        "time": 1234567890,
    }
    model = HyperliquidRawWsPositionUpdateEvent.model_validate(obj)
    assert model.asset == "ETH"
    assert isinstance(model.position, HyperliquidRawPositionInfo)


def test_ws_position_update_event_missing_required() -> None:
    obj: dict[str, object] = {"asset": "ETH", "position": {}}
    with pytest.raises(ValidationError):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj)


def test_ws_position_update_event_type_errors() -> None:
    obj: dict[str, object] = {"asset": 123, "position": "notadict", "time": "now"}
    with pytest.raises(ValidationError):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj)


def test_ws_position_update_event_extra_field() -> None:
    obj: dict[str, object] = {
        "asset": "ETH",
        "position": {
            "coin": "ETH",
            "entryPx": "3000.0",
            "leverage": {"type": "cross", "value": 10},
            "liquidationPx": "2900.0",
            "marginUsed": "100.0",
            "maxLeverage": 50,
            "positionValue": "150.0",
            "returnOnEquity": "0.1",
            "szi": "1.5",
            "unrealizedPnl": "10.0",
        },
        "time": 1234567890,
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj)


def test_ws_position_update_missing_optional_fields() -> None:
    # position with only required fields
    obj: dict[str, object] = {
        "asset": "ETH",
        "position": {
            "coin": "ETH",
            "leverage": {"type": "cross", "value": 10},
            "marginUsed": "100.0",
            "maxLeverage": 50,
            "positionValue": "150.0",
            "returnOnEquity": "0.1",
            "szi": "1.5",
            "unrealizedPnl": "10.0",
        },
        "time": 1234567890,
    }
    model = HyperliquidRawWsPositionUpdateEvent.model_validate(obj)
    assert model.asset == "ETH"


def test_ws_position_update_all_zero_negative_large() -> None:
    # All fields as zero, negative, or large
    obj: dict[str, object] = {
        "asset": "BTC",
        "position": {
            "coin": "BTC",
            "entryPx": "0",
            "leverage": {"type": "cross", "value": 1},
            "liquidationPx": "0",
            "marginUsed": "0",
            "maxLeverage": 1,
            "positionValue": "0",
            "returnOnEquity": "0",
            "szi": "0",
            "unrealizedPnl": "0",
        },
        "time": 0,
    }
    model = HyperliquidRawWsPositionUpdateEvent.model_validate(obj)
    assert model.asset == "BTC"
    obj2: dict[str, object] = {
        "asset": "BTC",
        "position": {
            "coin": "BTC",
            "entryPx": "-1",
            "leverage": {"type": "cross", "value": -1},
            "liquidationPx": "-1",
            "marginUsed": "-1",
            "maxLeverage": -1,
            "positionValue": "-1",
            "returnOnEquity": "-1",
            "szi": "-1",
            "unrealizedPnl": "-1",
        },
        "time": -1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj2)
    obj3: dict[str, object] = {
        "asset": "BTC",
        "position": {
            "coin": "BTC",
            "entryPx": "1e1000",
            "leverage": {"type": "cross", "value": 1e1000},
            "liquidationPx": "1e1000",
            "marginUsed": "1e1000",
            "maxLeverage": 1e1000,
            "positionValue": "1e1000",
            "returnOnEquity": "1e1000",
            "szi": "1e1000",
            "unrealizedPnl": "1e1000",
        },
        "time": 2**63 - 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj3)


def test_ws_fill_event_cloid_empty_string() -> None:
    # cloid as empty string (should be rejected)
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": 1234567890,
        "hash": "abc123",
        "oid": 42,
        "cloid": "",
        "isMaker": True,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_cloid_omitted() -> None:
    # cloid omitted
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": 1234567890,
        "hash": "abc123",
        "oid": 42,
        "isMaker": True,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.cloid is None


def test_ws_fill_event_cloid_very_long() -> None:
    # cloid very long (should be rejected)
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": 1234567890,
        "hash": "abc123",
        "oid": 42,
        "cloid": "c" * 100,
        "isMaker": True,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_hash_unicode_control() -> None:
    # hash with unicode or control characters
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": 1234567890,
        "hash": "abc\n123",
        "oid": 42,
        "cloid": "cl123",
        "isMaker": True,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.hash == "abc\n123"


def test_ws_event_fields_set_to_none() -> None:
    # Fields present but set to None (should be rejected if not optional)
    obj: dict[str, object] = {
        "coin": None,
        "px": None,
        "sz": None,
        "side": None,
        "time": None,
        "hash": None,
        "oid": None,
        "cloid": None,
        "isMaker": None,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_event_extra_fields_everywhere() -> None:
    # Extra fields at every level (should be rejected)
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": 1234567890,
        "hash": "abc123",
        "oid": 42,
        "cloid": "cl123",
        "isMaker": True,
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)
    obj2: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": 1234567890,
        "hash": "abc123",
        "oid": 42,
        "cloid": "cl123",
        "isMaker": True,
        "position": {"coin": "ETH", "foo": 1},
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj2)
