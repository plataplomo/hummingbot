# CyberDeltaEngine: Hyperliquid Raw User Fills Model Test Suite
# -------------------------------------------------------------
# Comprehensive tests for all models in hl_raw_user_fills.py
# - Strictly follows Raw Model Validation Policy
# - Covers all edge cases, adversarial input, and structure validation

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFill,
    HyperliquidRawUserFillsRequestPayload,
    HyperliquidRawUserFillsResponse,
)


# --- Helper: Valid minimal payloads for each model ---
def valid_user_fill() -> dict[str, object]:
    return {
        "tid": 1,
        "coin": "ETH",
        "px": "123.45",
        "sz": "1.0",
        "time": 1234567890,
        "side": "B",
        "oid": 2,
        "startPosition": "0.0",
        "dir": "long",
        "hash": "abc123",
        "fee": "0.01",
        "isMaker": True,
        "liquidationMarkPx": None,
        "cloid": None,
    }


def valid_user_fills_response() -> list[dict[str, object]]:
    return [valid_user_fill()]


def valid_user_fills_request_payload() -> dict[str, object]:
    # Use a valid Ethereum address (0x + 40 hex chars) for strict validation
    # Example: 0xabcdefabcdefabcdefabcdefabcdefabcdefabcd (40 hex chars)
    return {"type": "userFills", "user": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"}


# --- Tests for HyperliquidRawUserFill ---
def test_user_fill_happy_path() -> None:
    obj = HyperliquidRawUserFill.model_validate(valid_user_fill())
    assert obj.coin == "ETH"
    assert obj.px == "123.45"
    assert obj.is_maker is True
    assert obj.time == 1234567890
    assert obj.oid == 2
    assert obj.start_position == "0.0"


def test_user_fill_missing_required() -> None:
    required = [
        "tid",
        "coin",
        "px",
        "sz",
        "time",
        "side",
        "oid",
        "startPosition",
        "dir",
        "hash",
        "fee",
        "isMaker",
    ]
    for field in required:
        d = valid_user_fill().copy()
        del d[field]
        with pytest.raises(ValidationError):
            HyperliquidRawUserFill.model_validate(d)


def test_user_fill_optional_fields() -> None:
    d = valid_user_fill().copy()
    d["liquidationMarkPx"] = "123.45"
    d["cloid"] = "client-1"
    obj = HyperliquidRawUserFill.model_validate(d)
    assert obj.liquidation_mark_px == "123.45"
    assert obj.cloid == "client-1"
    d2 = valid_user_fill().copy()
    del d2["liquidationMarkPx"]
    del d2["cloid"]
    obj2 = HyperliquidRawUserFill.model_validate(d2)
    assert obj2.liquidation_mark_px is None
    assert obj2.cloid is None


def test_user_fill_type_errors() -> None:
    d = valid_user_fill().copy()
    d["tid"] = "notanint"
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["coin"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["isMaker"] = "true"
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["time"] = 123.45  # Float instead of int
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["oid"] = "id-string"  # String instead of int
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)


def test_user_fill_format_errors() -> None:
    for field in ["coin", "px", "sz", "fee", "startPosition", "dir", "hash"]:
        d = valid_user_fill().copy()
        d[field] = ""
        with pytest.raises(ValidationError):
            HyperliquidRawUserFill.model_validate(d)
        d[field] = "a" * 1000
        with pytest.raises(ValidationError):
            HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["px"] = "NaN"
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["side"] = "notaside"
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["hash"] = "0x" + "a" * 65  # Too long (max 66)
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["cloid"] = "a" * 129  # Too long (max 128)
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    # Test non-finite for start_position
    d = valid_user_fill().copy()
    d["startPosition"] = "inf"
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    # Test negative integers
    d = valid_user_fill().copy()
    d["tid"] = -1
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["oid"] = -1
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d = valid_user_fill().copy()
    d["time"] = -1
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)


def test_user_fill_extra_field() -> None:
    d = valid_user_fill().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)


def test_user_fill_adversarial_strings() -> None:
    d = valid_user_fill().copy()
    d["coin"] = "💣"
    obj = HyperliquidRawUserFill.model_validate(d)
    assert obj.coin == "💣"
    d["px"] = "1e6"
    obj = HyperliquidRawUserFill.model_validate(d)
    assert obj.px == "1e6"


# --- Additional edge case tests (OpenAPI/SDK/real-world) ---
def test_user_fill_coin_edge_cases() -> None:
    # Emoji, whitespace, symbols, bidi text
    for coin in ["ETH 💎", "   BTC   ", "COIN-123!@#", "\u202eABC\u202c"]:
        d = valid_user_fill().copy()
        d["coin"] = coin
        obj = HyperliquidRawUserFill.model_validate(d)
        assert obj.coin == coin


def test_user_fill_numeric_string_edge_cases() -> None:
    # px, sz, fee, startPosition: leading/trailing zeros, scientific notation, negative, overlong
    for field in ["px", "sz", "fee", "startPosition"]:
        d = valid_user_fill().copy()
        d[field] = "000123.4500"
        obj = HyperliquidRawUserFill.model_validate(d)
        # Use by_alias=True to check original field names as in input dict
        assert obj.model_dump(by_alias=True)[field] == "000123.4500"
        d[field] = "1.23e2"
        obj = HyperliquidRawUserFill.model_validate(d)
        assert obj.model_dump(by_alias=True)[field] == "1.23e2"
        d[field] = "-123.45"
        obj = HyperliquidRawUserFill.model_validate(d)
        assert obj.model_dump(by_alias=True)[field] == "-123.45"
        d[field] = "1" * 65
        with pytest.raises(ValidationError):
            HyperliquidRawUserFill.model_validate(d)


def test_user_fill_side_enum_edge_cases() -> None:
    # Lower/upper case, invalid value, whitespace
    d = valid_user_fill().copy()
    d["side"] = "b"
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d["side"] = "A "
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d["side"] = " "
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)


def test_user_fill_cloid_and_hash_edge_cases() -> None:
    # Overlong, empty, Unicode, control chars
    for field in ["cloid", "hash"]:
        d = valid_user_fill().copy()
        d[field] = "a" * 65
        with pytest.raises(ValidationError):
            HyperliquidRawUserFill.model_validate(d)
        d[field] = ""
        with pytest.raises(ValidationError):
            HyperliquidRawUserFill.model_validate(d)
        d[field] = "💣"
        obj = HyperliquidRawUserFill.model_validate(d)
        assert obj.model_dump()[field] == "💣"
        d[field] = "\x00"
        with pytest.raises(ValidationError):
            HyperliquidRawUserFill.model_validate(d)


def test_user_fill_liquidation_mark_px_edge_cases() -> None:
    # null, empty, overlong, non-decimal
    d = valid_user_fill().copy()
    d["liquidationMarkPx"] = None
    obj = HyperliquidRawUserFill.model_validate(d)
    assert obj.liquidation_mark_px is None
    d["liquidationMarkPx"] = ""
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d["liquidationMarkPx"] = "a" * 65
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    d["liquidationMarkPx"] = "notanumber"
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)


def test_user_fills_response_array_edge_cases() -> None:
    # Empty fills, excessive fills, non-list root
    obj = HyperliquidRawUserFillsResponse.model_validate([])
    assert obj.root == []
    obj = HyperliquidRawUserFillsResponse.model_validate([valid_user_fill()] * 1000)
    assert len(obj.root) == 1000
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsResponse.model_validate({"not": "alist"})


def test_user_fills_request_payload_user_edge_cases() -> None:
    # Invalid hex, too short/long, mixed case, non-hex
    d = valid_user_fills_request_payload().copy()
    d["user"] = "0x123"
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsRequestPayload.model_validate(d)
    d["user"] = "0x" + "a" * 41
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsRequestPayload.model_validate(d)
    # Use a valid Ethereum address for the valid case (exactly 40 hex chars)
    d["user"] = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
    obj = HyperliquidRawUserFillsRequestPayload.model_validate(d)
    assert obj.user == d["user"]
    d["user"] = "0xGHIJKL1234567890abcdefABCDEF1234567890"
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsRequestPayload.model_validate(d)


def test_user_fill_extra_fields() -> None:
    # Extra fields at all levels
    d = valid_user_fill().copy()
    d["extra"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawUserFill.model_validate(d)
    arr = [valid_user_fill() for _ in range(2)]
    arr[0]["extra"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsResponse.model_validate(arr)
    d = valid_user_fills_request_payload().copy()
    d["extra"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsRequestPayload.model_validate(d)


# --- Tests for HyperliquidRawUserFillsResponse (RootModel) ---
def test_user_fills_response_happy_path() -> None:
    obj = HyperliquidRawUserFillsResponse.model_validate(valid_user_fills_response())
    assert isinstance(obj.root, list)
    assert obj.root[0].coin == "ETH"


def test_user_fills_response_type_errors() -> None:
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsResponse.model_validate({"not": "alist"})
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsResponse.model_validate([{"tid": 1}])


def test_user_fills_response_extra_field() -> None:
    data = valid_user_fills_response()
    data[0]["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsResponse.model_validate(data)


def test_user_fills_request_payload_happy_path() -> None:
    # Use a valid Ethereum address for the happy path (exactly 40 hex chars)
    obj = HyperliquidRawUserFillsRequestPayload.model_validate(valid_user_fills_request_payload())
    assert obj.type == "userFills"
    assert obj.user == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"


def test_user_fills_request_payload_type_errors() -> None:
    d = valid_user_fills_request_payload().copy()
    d["user"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsRequestPayload.model_validate(d)
    d = valid_user_fills_request_payload().copy()
    d["type"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsRequestPayload.model_validate(d)


def test_user_fills_request_payload_format_errors() -> None:
    d = valid_user_fills_request_payload().copy()
    d["user"] = ""
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsRequestPayload.model_validate(d)
    d["user"] = "a" * 1000
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsRequestPayload.model_validate(d)


def test_user_fills_request_payload_extra_field() -> None:
    d = valid_user_fills_request_payload().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawUserFillsRequestPayload.model_validate(d)


def test_user_fill_extra_field_forbidden() -> None:
    """Test explicit check for extra='forbid'."""
    d = valid_user_fill().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawUserFill.model_validate(d)


def test_user_fill_frozen() -> None:
    """Test explicit check for frozen=True."""
    obj = HyperliquidRawUserFill.model_validate(valid_user_fill())
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.tid = 999
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.px = "999.99"
