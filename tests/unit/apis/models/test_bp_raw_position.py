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
