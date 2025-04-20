import json
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_error import BackpackRawApiError


def valid_api_error() -> dict[str, Any]:
    return {
        "code": "INVALID_SIGNATURE",
        "message": "Signature is invalid or expired.",
    }


def test_BackpackRawApiError_happy_path() -> None:
    obj = BackpackRawApiError.model_validate(valid_api_error())
    assert obj.code == "INVALID_SIGNATURE"
    assert obj.message.startswith("Signature")


def test_BackpackRawApiError_missing_required_fields() -> None:
    for field in ["code", "message"]:
        p: dict[str, Any] = valid_api_error().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawApiError.model_validate(p)


def test_BackpackRawApiError_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_api_error().copy()
    p["code"] = 123
    with pytest.raises(ValidationError):
        BackpackRawApiError.model_validate(p)
    p = valid_api_error().copy()
    p["message"] = ["notastring"]
    with pytest.raises(ValidationError):
        BackpackRawApiError.model_validate(p)


def test_BackpackRawApiError_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_api_error().copy()
    p["code"] = ""
    with pytest.raises(ValidationError):
        BackpackRawApiError.model_validate(p)
    p = valid_api_error().copy()
    p["message"] = ""
    with pytest.raises(ValidationError):
        BackpackRawApiError.model_validate(p)
    p = valid_api_error().copy()
    p["code"] = "NOT_A_REAL_CODE"
    with pytest.raises(ValidationError):
        BackpackRawApiError.model_validate(p)


def test_BackpackRawApiError_extra_field() -> None:
    p: dict[str, Any] = valid_api_error().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawApiError.model_validate(p)


def test_BackpackRawApiError_corruption_cases() -> None:
    # Null required
    p: dict[str, Any] = valid_api_error().copy()
    p["code"] = None
    with pytest.raises(ValidationError):
        BackpackRawApiError.model_validate(p)
    # Unicode/control chars
    p = valid_api_error().copy()
    p["message"] = "Signature\x00invalid"
    obj = BackpackRawApiError.model_validate(p)
    assert "Signature" in obj.message
    # Excessive length
    p = valid_api_error().copy()
    p["message"] = "A" * 10000
    obj = BackpackRawApiError.model_validate(p)
    assert obj.message.startswith("A")
    # Truncated JSON
    bad_json = '{"code": "INVALID_SIGNATURE"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)
