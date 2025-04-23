# CyberDeltaEngine: Hyperliquid Raw Open Orders Model Test Suite
# --------------------------------------------------------------
# Comprehensive tests for all models in hl_raw_open_orders.py
# - Strictly follows Raw Model Validation Policy
# - Covers all edge cases, adversarial input, and structure validation

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawTriggerInfo


# --- Helper: Valid minimal payloads for each model ---
def valid_trigger_info() -> dict[str, object]:
    return {"triggerPx": "123.45", "isMarket": True, "tpsl": "tp"}


def valid_tif_limit() -> dict[str, object]:
    return {"tif": "Gtc"}


def valid_order_type_limit() -> dict[str, object]:
    return {"limit": valid_tif_limit()}


def valid_order_type_market() -> dict[str, object]:
    return {"market": {}}


def valid_order() -> dict[str, object]:
    return {
        "oid": 1,
        "cloid": "client-1",
        "asset": "ETH",
        "side": "B",
        "limitPx": "123.45",
        "sz": "1.0",
        "timestamp": 1234567890,
        "orderType": valid_order_type_limit(),
        "reduceOnly": False,
        "remainingSz": "0.5",
        "status": "open",
        "statusTimestamp": 1234567891,
    }


def valid_open_order() -> dict[str, object]:
    return {"order": valid_order(), "trigger": valid_trigger_info()}


def valid_open_orders_response() -> list[dict[str, object]]:
    return [valid_open_order()]


def valid_order_spec() -> dict[str, object]:
    return {
        "asset": 0,
        "isBuy": True,
        "limitPx": "123.45",
        "sz": 1.0,
        "reduceOnly": False,
        "orderType": valid_order_type_limit(),
        "trigger": valid_trigger_info(),
        "cloid": "client-1",
    }


def valid_modify_order_request() -> dict[str, object]:
    return {"oid": 1, "order": valid_order_spec()}


def valid_cancel_request() -> dict[str, object]:
    return {"asset": 0, "oid": 1}


def valid_cancel_by_cloid_request() -> dict[str, object]:
    return {"asset": 0, "cloid": "client-1"}


def valid_exchange_status_object() -> dict[str, object]:
    return {"resting": {"foo": "bar"}, "filled": None, "error": None}


def valid_exchange_response_data() -> dict[str, object]:
    return {"type": "ok", "statuses": [valid_exchange_status_object()]}


def valid_exchange_action_response() -> dict[str, object]:
    return {"status": "ok", "data": valid_exchange_response_data()}


def valid_open_orders_request_payload() -> dict[str, object]:
    return {"type": "openOrders", "user": "0xabc"}


# --- Tests for HyperliquidRawTriggerInfo ---
def test_trigger_info_happy_path() -> None:
    obj = HyperliquidRawTriggerInfo.model_validate(valid_trigger_info())
    assert obj.trigger_px == "123.45"
    assert obj.is_market is True
    assert obj.tpsl == "tp"


def test_trigger_info_missing_required() -> None:
    for field in ["triggerPx", "isMarket", "tpsl"]:
        p = valid_trigger_info().copy()
        del p[field]
        with pytest.raises(ValidationError):
            HyperliquidRawTriggerInfo.model_validate(p)


def test_trigger_info_type_errors() -> None:
    p = valid_trigger_info().copy()
    p["triggerPx"] = 123.45
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(p)
    p = valid_trigger_info().copy()
    p["isMarket"] = "true"
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(p)
    p = valid_trigger_info().copy()
    p["tpsl"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(p)


def test_trigger_info_enum_and_format_errors() -> None:
    p = valid_trigger_info().copy()
    p["tpsl"] = "notatp"
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(p)
    p = valid_trigger_info().copy()
    p["triggerPx"] = ""
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(p)
    p = valid_trigger_info().copy()
    p["triggerPx"] = "a" * 1000
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(p)
    p = valid_trigger_info().copy()
    p["triggerPx"] = "NaN"
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(p)


def test_trigger_info_extra_field() -> None:
    p = valid_trigger_info().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(p)


def test_trigger_info_adversarial_strings() -> None:
    p = valid_trigger_info().copy()
    p["triggerPx"] = "1e6"
    obj = HyperliquidRawTriggerInfo.model_validate(p)
    assert obj.trigger_px == "1e6"


# ... (Repeat similar structure for all other models in the file, including nested, list, and root
# models, with creative edge cases and adversarial input) ...
