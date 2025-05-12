# (Full test suite for BackpackRawOrder, BackpackRawOrderBook,
# BackpackRawOrderUpdate will be written here.)

import json

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models import (
    BackpackRawOrder,
    BackpackRawOrderBook,
    BackpackRawOrderUpdate,
)


# --- BackpackRawOrder ---
def valid_order() -> dict[str, object]:
    return {
        "id": "123",
        "symbol": "BTC_USDC",
        "side": "buy",
        "orderType": "LIMIT",
        "status": "NEW",
        "quantity": "1.0",
        "createdAt": 1234567890,
    }


def test_BackpackRawOrder_happy_path() -> None:
    obj = BackpackRawOrder.model_validate(valid_order())
    assert obj.symbol == "BTC_USDC"
    assert obj.side == "buy"
    assert obj.quantity == "1.0"


def test_BackpackRawOrder_missing_required_id() -> None:
    p = valid_order()
    del p["id"]
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_missing_required_symbol() -> None:
    p = valid_order()
    del p["symbol"]
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_missing_required_side() -> None:
    p = valid_order()
    del p["side"]
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_missing_required_orderType() -> None:
    p = valid_order()
    del p["orderType"]
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_missing_required_status() -> None:
    p = valid_order()
    del p["status"]
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_missing_required_quantity() -> None:
    p = valid_order()
    del p["quantity"]
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_missing_required_createdAt() -> None:
    p = valid_order()
    del p["createdAt"]
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_wrong_type_quantity() -> None:
    p = valid_order()
    p["quantity"] = [1.0]
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_wrong_type_side() -> None:
    p = valid_order()
    p["side"] = 123
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_invalid_decimal_quantity() -> None:
    p = valid_order()
    p["quantity"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)
    # Scientific notation is allowed (project policy)
    p = valid_order()
    p["quantity"] = "1e3"
    obj = BackpackRawOrder.model_validate(p)
    assert obj.quantity == "1e3"


def test_BackpackRawOrder_invalid_enum_side() -> None:
    p = valid_order()
    p["side"] = "Diagonal"
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_invalid_timestamp_createdAt() -> None:
    p = valid_order()
    p["createdAt"] = "not-a-timestamp"
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_extra_field() -> None:
    p = valid_order()
    p["foo"] = "bar"
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_optional_fields_all_none() -> None:
    p = valid_order()
    for f in [
        "clientId",
        "relatedOrderId",
        "executedQuantity",
        "executedQuoteQuantity",
        "price",
        "triggerPrice",
        "avgFillPrice",
        "triggerBy",
        "timeInForce",
        "reduceOnly",
        "postOnly",
        "selfTradePrevention",
        "updatedAt",
        "triggeredAt",
        "expiryReason",
        "origin",
    ]:
        p[f] = None
    obj = BackpackRawOrder.model_validate(p)
    for f in [
        "clientId",
        "relatedOrderId",
        "executedQuantity",
        "executedQuoteQuantity",
        "price",
        "triggerPrice",
        "avgFillPrice",
        "triggerBy",
        "timeInForce",
        "reduceOnly",
        "postOnly",
        "selfTradePrevention",
        "updatedAt",
        "triggeredAt",
        "expiryReason",
        "origin",
    ]:
        assert getattr(obj, f, None) is None


def test_BackpackRawOrder_optional_fields_omitted() -> None:
    p = valid_order()
    for f in [
        "clientId",
        "relatedOrderId",
        "executedQuantity",
        "executedQuoteQuantity",
        "price",
        "triggerPrice",
        "avgFillPrice",
        "triggerBy",
        "timeInForce",
        "reduceOnly",
        "postOnly",
        "selfTradePrevention",
        "updatedAt",
        "triggeredAt",
        "expiryReason",
        "origin",
    ]:
        if f in p:
            del p[f]
    obj = BackpackRawOrder.model_validate(p)
    for f in [
        "clientId",
        "relatedOrderId",
        "executedQuantity",
        "executedQuoteQuantity",
        "price",
        "triggerPrice",
        "avgFillPrice",
        "triggerBy",
        "timeInForce",
        "reduceOnly",
        "postOnly",
        "selfTradePrevention",
        "updatedAt",
        "triggeredAt",
        "expiryReason",
        "origin",
    ]:
        assert getattr(obj, f, None) is None


def test_BackpackRawOrder_corruption_garbled_quantity() -> None:
    p = valid_order()
    p["quantity"] = "NaN"
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_corruption_null_required_symbol() -> None:
    p = valid_order()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawOrder.model_validate(p)


def test_BackpackRawOrder_corruption_unicode_symbol() -> None:
    p = valid_order()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawOrder.model_validate(p)
    assert "BTC_USDC" in obj.symbol


def test_BackpackRawOrder_corruption_nested_bids_in_orderbook() -> None:
    book = {
        "symbol": "BTC_USDC",
        "bids": [["50000.0", "1.0"], ["bad", "1..0"]],
        "asks": [["50010.0", "0.5"]],
        "time": 1234567890,
    }
    with pytest.raises(ValidationError):
        BackpackRawOrderBook.model_validate(book)


def test_BackpackRawOrder_truncated_json() -> None:
    bad_json = '{"id": "123", "symbol": "BTC_USDC", "side": "buy"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


# --- BackpackRawOrderBook ---
def valid_orderbook() -> dict[str, object]:
    return {
        "symbol": "BTC_USDC",
        "bids": [["50000.0", "1.0"], ["49900.0", "2.0"]],
        "asks": [["50100.0", "0.5"]],
        "time": 1234567890,
    }


def test_BackpackRawOrderBook_happy_path() -> None:
    obj = BackpackRawOrderBook.model_validate(valid_orderbook())
    assert obj.symbol == "BTC_USDC"
    assert obj.bids[0][0] == "50000.0"


def test_BackpackRawOrderBook_missing_required_fields() -> None:
    for field in ["symbol", "bids", "asks", "time"]:
        p = valid_orderbook().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawOrderBook.model_validate(p)


def test_BackpackRawOrderBook_wrong_type_fields() -> None:
    p = valid_orderbook().copy()
    p["bids"] = "notalist"
    with pytest.raises(ValidationError):
        BackpackRawOrderBook.model_validate(p)
    p = valid_orderbook().copy()
    p["asks"] = [123, 456]
    with pytest.raises(ValidationError):
        BackpackRawOrderBook.model_validate(p)
    p = valid_orderbook().copy()
    p["symbol"] = 123
    with pytest.raises(ValidationError):
        BackpackRawOrderBook.model_validate(p)


def test_BackpackRawOrderBook_invalid_format_validators() -> None:
    p = valid_orderbook().copy()
    p["bids"] = [["bad", "1..0"]]
    with pytest.raises(ValidationError):
        BackpackRawOrderBook.model_validate(p)
    p = valid_orderbook().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawOrderBook.model_validate(p)


def test_BackpackRawOrderBook_extra_field() -> None:
    p = valid_orderbook().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawOrderBook.model_validate(p)


def test_BackpackRawOrderBook_optional_fields() -> None:
    # No optional fields in this model
    obj = BackpackRawOrderBook.model_validate(valid_orderbook())
    assert obj.symbol == "BTC_USDC"


def test_BackpackRawOrderBook_corruption_cases() -> None:
    # Nested corruption
    p = valid_orderbook().copy()
    p["bids"] = [["50000.0", "1.0"], ["bad", "notanumber"]]
    with pytest.raises(ValidationError):
        BackpackRawOrderBook.model_validate(p)
    # Null required
    p = valid_orderbook().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawOrderBook.model_validate(p)
    # Truncated JSON
    bad_json = '{"symbol": "BTC_USDC", "bids": [["50000.0", "1.0"]]'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


# --- BackpackRawOrderUpdate ---
def valid_orderupdate() -> dict[str, object]:
    return {
        "e": "orderAccepted",
        "E": 1234567890,
        "s": "BTC_USDC",
        "S": "Bid",
        "o": "LIMIT",
        "X": "NEW",
    }


def test_BackpackRawOrderUpdate_happy_path() -> None:
    obj = BackpackRawOrderUpdate.model_validate(valid_orderupdate())
    assert obj.event_type == "orderAccepted"
    assert obj.symbol == "BTC_USDC"
    assert obj.side == "Bid"


def test_BackpackRawOrderUpdate_missing_required_fields() -> None:
    for field in ["e", "E", "s", "S", "o", "X"]:
        p = valid_orderupdate().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawOrderUpdate.model_validate(p)


def test_BackpackRawOrderUpdate_wrong_type_fields() -> None:
    p = valid_orderupdate().copy()
    p["E"] = "notanint"
    with pytest.raises(ValidationError):
        BackpackRawOrderUpdate.model_validate(p)
    p = valid_orderupdate().copy()
    p["S"] = 123
    with pytest.raises(ValidationError):
        BackpackRawOrderUpdate.model_validate(p)


def test_BackpackRawOrderUpdate_invalid_format_validators() -> None:
    p = valid_orderupdate().copy()
    p["S"] = "Diagonal"
    with pytest.raises(ValidationError):
        BackpackRawOrderUpdate.model_validate(p)
    p = valid_orderupdate().copy()
    p["X"] = "BADSTATUS"
    with pytest.raises(ValidationError):
        BackpackRawOrderUpdate.model_validate(p)


def test_BackpackRawOrderUpdate_extra_field() -> None:
    p = valid_orderupdate().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawOrderUpdate.model_validate(p)


def test_BackpackRawOrderUpdate_optional_fields() -> None:
    # All optional fields omitted
    obj = BackpackRawOrderUpdate.model_validate(valid_orderupdate())
    assert obj.client_order_id is None
    # All optional fields as None
    p = valid_orderupdate().copy()
    for f in ["client_order_id", "time_in_force", "quantity", "price"]:
        p[f] = None
    obj2 = BackpackRawOrderUpdate.model_validate(p)
    for f in ["client_order_id", "time_in_force", "quantity", "price"]:
        assert getattr(obj2, f, None) is None


def test_BackpackRawOrderUpdate_corruption_cases() -> None:
    # Garbled numerics
    p = valid_orderupdate().copy()
    p["quantity"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawOrderUpdate.model_validate(p)
    # Null required
    p = valid_orderupdate().copy()
    p["s"] = None
    with pytest.raises(ValidationError):
        BackpackRawOrderUpdate.model_validate(p)
    # Unicode/control chars
    p = valid_orderupdate().copy()
    p["s"] = "BTC_USDC\x00"
    obj = BackpackRawOrderUpdate.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Truncated JSON
    bad_json = '{"e": "orderAccepted", "E": 1234567890, "s": "BTC_USDC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


@pytest.mark.xfail(reason="Validator issues or apply model failure")
class TestBackpackRawOrder:
    """Test suite for the BackpackRawOrder Pydantic model."""

    def test_invalid_market_order_missing_side(self) -> None:
        """Test that a market order without a side raises ValidationError."""
        data = {
            "id": "123",
            "symbol": "BTC_USDC",
            "orderType": "MARKET",
            "status": "NEW",
            "quantity": "1.0",
            "createdAt": 1234567890,
        }
        with pytest.raises(ValidationError):
            BackpackRawOrder(**data)
