import json
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_position import (
    BackpackRawPosition,
    BackpackRawPositionUpdate,
    PositionImfFunction,
    SqrtFunction,
)

"""
Unit tests for BackpackRawPosition and related Raw models.

**Boundary Validation Pattern (Project Standard):**
- All string fields in Raw models are strictly validated for:
    - Type: must be `str` (not bytes, int, list, etc.)
    - Non-emptiness (unless explicitly allowed)
    - Max length (per OpenAPI spec)
    - Valid UTF-8 encoding (no lone surrogates or invalid unicode)
- **Invalid unicode or broken types are always rejected** with `ValidationError` (if caught by the validator) or `UnicodeEncodeError` (if Python or Pydantic internals hit the error first).
- This test suite includes adversarial/hostile input cases to ensure the Raw model boundary is robust and spec-aligned.

This pattern is enforced for all Raw models in the CyberDeltaEngine project.
"""


# --- SqrtFunction ---
def valid_sqrt_function() -> dict[str, Any]:
    return {"base": "1.0", "factor": "0.5"}


def test_SqrtFunction_happy_path() -> None:
    obj = SqrtFunction.model_validate(valid_sqrt_function())
    assert obj.base == "1.0"
    assert obj.factor == "0.5"


def test_SqrtFunction_missing_required_fields() -> None:
    for field in ["base", "factor"]:
        p: dict[str, Any] = valid_sqrt_function().copy()
        del p[field]
        with pytest.raises(ValidationError):
            SqrtFunction.model_validate(p)


def test_SqrtFunction_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_sqrt_function().copy()
    p["base"] = 1.0
    with pytest.raises(ValidationError):
        SqrtFunction.model_validate(p)


def test_SqrtFunction_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_sqrt_function().copy()
    p["base"] = "notanumber"
    with pytest.raises(ValidationError):
        SqrtFunction.model_validate(p)


def test_SqrtFunction_extra_field() -> None:
    p: dict[str, Any] = valid_sqrt_function().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        SqrtFunction.model_validate(p)


def test_SqrtFunction_corruption_cases() -> None:
    # Null required
    p: dict[str, Any] = valid_sqrt_function().copy()
    p["base"] = None
    with pytest.raises(ValidationError):
        SqrtFunction.model_validate(p)
    # Unicode/control chars
    p = valid_sqrt_function().copy()
    p["base"] = "1.0\x00"
    obj = SqrtFunction.model_validate(p)
    assert "1.0" in obj.base
    # Truncated JSON
    bad_json = '{"base": "1.0"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


# --- SqrtFunction: Real JSON and Corruption Cases ---
def test_SqrtFunction_real_json_example() -> None:
    """Validate SqrtFunction using a real JSON payload."""
    payload = {"base": "1.00000001", "factor": "-0.99999999"}
    obj = SqrtFunction.model_validate(payload)
    assert obj.base == "1.00000001"
    assert obj.factor == "-0.99999999"


def test_SqrtFunction_corruption_null_base() -> None:
    """Should fail: null value for required 'base'."""
    p = valid_sqrt_function().copy()
    p["base"] = None
    with pytest.raises(ValidationError):
        SqrtFunction.model_validate(p)


def test_SqrtFunction_corruption_binary_factor() -> None:
    """Should fail: binary data for 'factor'."""
    p = valid_sqrt_function().copy()
    p["factor"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        SqrtFunction.model_validate(p)


def test_SqrtFunction_corruption_nested_base() -> None:
    """Should fail: nested object for 'base'."""
    p = valid_sqrt_function().copy()
    p["base"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        SqrtFunction.model_validate(p)


def test_SqrtFunction_corruption_list_factor() -> None:
    """Should fail: list for 'factor'."""
    p = valid_sqrt_function().copy()
    p["factor"] = ["0.5"]
    with pytest.raises(ValidationError):
        SqrtFunction.model_validate(p)


def test_SqrtFunction_corruption_garbled_unicode_base() -> None:
    """Should fail: garbled unicode in 'base'."""
    p = valid_sqrt_function().copy()
    p["base"] = "1.0\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        SqrtFunction.model_validate(p)


# --- PositionImfFunction ---
def valid_imf_function() -> dict[str, Any]:
    return {"type": "sqrt", "base": "1.0", "factor": "0.5"}


def test_PositionImfFunction_happy_path() -> None:
    obj = PositionImfFunction.model_validate(valid_imf_function())
    assert obj.type == "sqrt"
    assert obj.base == "1.0"
    assert obj.factor == "0.5"


def test_PositionImfFunction_missing_required_fields() -> None:
    for field in ["type", "base", "factor"]:
        p: dict[str, Any] = valid_imf_function().copy()
        del p[field]
        with pytest.raises(ValidationError):
            PositionImfFunction.model_validate(p)


def test_PositionImfFunction_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_imf_function().copy()
    p["type"] = 123
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)


def test_PositionImfFunction_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_imf_function().copy()
    p["type"] = "notasupportedtype"
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)
    p = valid_imf_function().copy()
    p["base"] = "notanumber"
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)


def test_PositionImfFunction_extra_field() -> None:
    p: dict[str, Any] = valid_imf_function().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)


def test_PositionImfFunction_corruption_cases() -> None:
    # Null required
    p: dict[str, Any] = valid_imf_function().copy()
    p["type"] = None
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)
    # Unicode/control chars
    p = valid_imf_function().copy()
    p["type"] = "sqrt\x00"
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)
    # Nested corruption
    p = valid_imf_function().copy()
    p["base"] = "1..0"
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)
    # Truncated JSON
    bad_json = '{"type": "sqrt", "base": "1.0"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


# --- PositionImfFunction: Real JSON and Corruption Cases ---
def test_PositionImfFunction_real_json_example() -> None:
    """Validate PositionImfFunction using a real JSON payload."""
    payload = {"type": "sqrt", "base": "0.00000001", "factor": "99999999.99999999"}
    obj = PositionImfFunction.model_validate(payload)
    assert obj.type == "sqrt"
    assert obj.base == "0.00000001"
    assert obj.factor == "99999999.99999999"


def test_PositionImfFunction_corruption_null_type() -> None:
    """Should fail: null value for required 'type'."""
    p = valid_imf_function().copy()
    p["type"] = None
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)


def test_PositionImfFunction_corruption_binary_base() -> None:
    """Should fail: binary data for 'base'."""
    p = valid_imf_function().copy()
    p["base"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)


def test_PositionImfFunction_corruption_nested_factor() -> None:
    """Should fail: nested object for 'factor'."""
    p = valid_imf_function().copy()
    p["factor"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)


def test_PositionImfFunction_corruption_list_base() -> None:
    """Should fail: list for 'base'."""
    p = valid_imf_function().copy()
    p["base"] = ["1.0"]
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)


def test_PositionImfFunction_corruption_garbled_unicode_type() -> None:
    """Should fail: garbled unicode in 'type'."""
    p = valid_imf_function().copy()
    p["type"] = "sqrt\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        PositionImfFunction.model_validate(p)


# --- BackpackRawPosition ---
def valid_position() -> dict[str, Any]:
    return {
        "breakEvenPrice": "50000.0",
        "entryPrice": "49900.0",
        "estLiquidationPrice": "45000.0",
        "imf": "0.05",
        "imfFunction": valid_imf_function(),
        "markPrice": "50010.0",
        "mmf": "0.01",
        "mmfFunction": valid_imf_function(),
        "netCost": "100.0",
        "netQuantity": "0.01",
        "netExposureQuantity": "0.01",
        "netExposureNotional": "500.0",
        "pnlRealized": "10.0",
        "pnlUnrealized": "5.0",
        "cumulativeFundingPayment": "-1.0",
        "symbol": "BTC_USDC",
        "userId": 12345,
        "positionId": "pos_abc123",
        "cumulativeInterest": "0.1",
    }


def test_BackpackRawPosition_happy_path() -> None:
    obj = BackpackRawPosition.model_validate(valid_position())
    assert obj.symbol == "BTC_USDC"
    assert obj.user_id == 12345
    assert obj.imf_function.type == "sqrt"


def test_BackpackRawPosition_missing_required_fields() -> None:
    for field in [
        "breakEvenPrice",
        "entryPrice",
        "estLiquidationPrice",
        "imf",
        "imfFunction",
        "markPrice",
        "mmf",
        "mmfFunction",
        "netCost",
        "netQuantity",
        "netExposureQuantity",
        "netExposureNotional",
        "pnlRealized",
        "pnlUnrealized",
        "cumulativeFundingPayment",
        "symbol",
        "userId",
        "positionId",
        "cumulativeInterest",
    ]:
        p: dict[str, Any] = valid_position().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawPosition.model_validate(p)


def test_BackpackRawPosition_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_position().copy()
    p["userId"] = "notanint"
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)
    p = valid_position().copy()
    p["imfFunction"] = "notadict"
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)


def test_BackpackRawPosition_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_position().copy()
    p["breakEvenPrice"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)
    p = valid_position().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)
    p = valid_position().copy()
    p["imfFunction"] = {"type": "notasupportedtype", "base": "1.0", "factor": "0.5"}
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)


def test_BackpackRawPosition_extra_field() -> None:
    p: dict[str, Any] = valid_position().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)


def test_BackpackRawPosition_corruption_cases() -> None:
    # Garbled numerics
    p: dict[str, Any] = valid_position().copy()
    p["netCost"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)
    # Null required
    p = valid_position().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)
    # Unicode/control chars
    p = valid_position().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawPosition.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Nested corruption (imfFunction)
    p = valid_position().copy()
    p["imfFunction"] = {"type": "sqrt", "base": "notanumber", "factor": "0.5"}
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)
    # Truncated JSON
    bad_json = '{"breakEvenPrice": "50000.0", "entryPrice": "49900.0"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


# --- BackpackRawPosition: Real JSON and Corruption Cases ---
def test_BackpackRawPosition_real_json_example() -> None:
    """Validate BackpackRawPosition using a real JSON payload."""
    payload = {
        "breakEvenPrice": "0.00000001",
        "entryPrice": "99999999.99999999",
        "estLiquidationPrice": "-0.00000001",
        "imf": "0.05",
        "imfFunction": {"type": "sqrt", "base": "1.0", "factor": "0.5"},
        "markPrice": "50010.0",
        "mmf": "0.01",
        "mmfFunction": {"type": "sqrt", "base": "1.0", "factor": "0.5"},
        "netCost": "100.0",
        "netQuantity": "0.01",
        "netExposureQuantity": "0.01",
        "netExposureNotional": "500.0",
        "pnlRealized": "10.0",
        "pnlUnrealized": "5.0",
        "cumulativeFundingPayment": "-1.0",
        "symbol": "BTC_USDC",
        "userId": 12345,
        "positionId": "pos_abc123",
        "cumulativeInterest": "0.1",
    }
    obj = BackpackRawPosition.model_validate(payload)
    assert obj.symbol == "BTC_USDC"
    assert obj.user_id == 12345
    assert obj.imf_function.type == "sqrt"


def test_BackpackRawPosition_corruption_null_symbol() -> None:
    """Should fail: null value for required 'symbol'."""
    p = valid_position().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)


def test_BackpackRawPosition_corruption_binary_breakEvenPrice() -> None:
    """Should fail: binary data for 'breakEvenPrice'."""
    p = valid_position().copy()
    p["breakEvenPrice"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)


def test_BackpackRawPosition_corruption_nested_imfFunction() -> None:
    """Should fail: nested object for 'imfFunction' with wrong type."""
    p = valid_position().copy()
    p["imfFunction"] = {"type": "notasupportedtype", "base": "1.0", "factor": "0.5"}
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)


def test_BackpackRawPosition_corruption_list_positionId() -> None:
    """Should fail: list for 'positionId'."""
    p = valid_position().copy()
    p["positionId"] = ["pos_abc123"]
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)


def test_BackpackRawPosition_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 'symbol'."""
    p = valid_position().copy()
    p["symbol"] = "BTC_USDC\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawPosition.model_validate(p)


# --- BackpackRawPositionUpdate ---
def valid_position_update() -> dict[str, Any]:
    return {
        "e": "positionOpened",
        "E": 1234567890,
        "s": "BTC_USDC",
        "b": "50000.0",
        "B": "49900.0",
        "l": "45000.0",
        "f": "0.05",
        "M": "50010.0",
        "m": "0.01",
        "q": "0.01",
        "Q": "0.01",
        "n": "500.0",
    }


def test_BackpackRawPositionUpdate_happy_path() -> None:
    obj = BackpackRawPositionUpdate.model_validate(valid_position_update())
    assert obj.event_type == "positionOpened"
    assert obj.symbol == "BTC_USDC"
    assert obj.event_time == 1234567890


def test_BackpackRawPositionUpdate_missing_required_fields() -> None:
    for field in ["e", "E", "s"]:
        p: dict[str, Any] = valid_position_update().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawPositionUpdate.model_validate(p)


def test_BackpackRawPositionUpdate_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_position_update().copy()
    p["E"] = "notanint"
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)
    p = valid_position_update().copy()
    p["b"] = ["50000.0"]
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)


def test_BackpackRawPositionUpdate_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_position_update().copy()
    p["b"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)
    p = valid_position_update().copy()
    p["s"] = ""
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)


def test_BackpackRawPositionUpdate_extra_field() -> None:
    p: dict[str, Any] = valid_position_update().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)


def test_BackpackRawPositionUpdate_optional_fields_all_none() -> None:
    p: dict[str, Any] = valid_position_update().copy()
    for f in ["b", "B", "l", "f", "M", "m", "q", "Q", "n"]:
        p[f] = None
    obj = BackpackRawPositionUpdate.model_validate(p)
    for f in ["b", "B", "l", "f", "M", "m", "q", "Q", "n"]:
        assert getattr(obj, f, None) is None


def test_BackpackRawPositionUpdate_optional_fields_omitted() -> None:
    p: dict[str, Any] = valid_position_update().copy()
    for f in ["b", "B", "l", "f", "M", "m", "q", "Q", "n"]:
        if f in p:
            del p[f]
    obj = BackpackRawPositionUpdate.model_validate(p)
    for f in ["b", "B", "l", "f", "M", "m", "q", "Q", "n"]:
        assert getattr(obj, f, None) is None


def test_BackpackRawPositionUpdate_corruption_cases() -> None:
    # Garbled numerics
    p: dict[str, Any] = valid_position_update().copy()
    p["b"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)
    # Null required
    p = valid_position_update().copy()
    p["s"] = None
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)
    # Unicode/control chars
    p = valid_position_update().copy()
    p["s"] = "BTC_USDC\x00"
    obj = BackpackRawPositionUpdate.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Truncated JSON
    bad_json = '{"e": "positionOpened", "E": 1234567890, "s": "BTC_USDC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


# --- BackpackRawPositionUpdate: Real JSON and Corruption Cases ---
def test_BackpackRawPositionUpdate_real_json_example() -> None:
    """Validate BackpackRawPositionUpdate using a real JSON payload."""
    payload = {
        "e": "positionClosed",
        "E": 9223372036854775807,
        "s": "ETH_USDC",
        "b": "0.00000001",
        "B": "99999999.99999999",
        "l": "-0.00000001",
        "f": "0.05",
        "M": "50010.0",
        "m": "0.01",
        "q": "0.01",
        "Q": "0.01",
        "n": "500.0",
    }
    obj = BackpackRawPositionUpdate.model_validate(payload)
    assert obj.event_type == "positionClosed"
    assert obj.symbol == "ETH_USDC"
    assert obj.event_time == 9223372036854775807


def test_BackpackRawPositionUpdate_corruption_null_symbol() -> None:
    """Should fail: null value for required 's' (symbol)."""
    p = valid_position_update().copy()
    p["s"] = None
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)


def test_BackpackRawPositionUpdate_corruption_binary_b() -> None:
    """Should fail: binary data for 'b'."""
    p = valid_position_update().copy()
    p["b"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)


def test_BackpackRawPositionUpdate_corruption_nested_f() -> None:
    """Should fail: nested object for 'f'."""
    p = valid_position_update().copy()
    p["f"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)


def test_BackpackRawPositionUpdate_corruption_list_n() -> None:
    """Should fail: list for 'n'."""
    p = valid_position_update().copy()
    p["n"] = ["500.0"]
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)


def test_BackpackRawPositionUpdate_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 's' (symbol)."""
    p = valid_position_update().copy()
    p["s"] = "ETH_USDC\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawPositionUpdate.model_validate(p)
