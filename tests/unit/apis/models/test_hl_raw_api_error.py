import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_api_error import HyperliquidRawApiError


# --- Helpers ---
def valid_error() -> dict[str, object]:
    return {"error": "Something went wrong"}


# --- Tests ---
def test_happy_path() -> None:
    obj = HyperliquidRawApiError.model_validate(valid_error())
    assert obj.error == "Something went wrong"


def test_missing_error_field() -> None:
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate({})


def test_error_type_errors() -> None:
    for bad in [123, 1.5, True, None, ["err"], {"msg": "err"}]:
        with pytest.raises(ValidationError):
            HyperliquidRawApiError.model_validate({"error": bad})


def test_error_empty_and_whitespace() -> None:
    for bad in ["", "   "]:
        with pytest.raises(ValidationError):
            HyperliquidRawApiError.model_validate({"error": bad})


def test_error_too_long() -> None:
    msg = "a" * 1025
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate({"error": msg})


def test_error_invalid_utf8() -> None:
    # Simulate a string with invalid UTF-8 by using surrogates (which are not valid in UTF-8)
    bad = "bad\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate({"error": bad})


def test_extra_field() -> None:
    d = valid_error().copy()
    d["foo"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate(d)


def test_adversarial_strings_should_pass() -> None:
    # These are structurally valid and must be accepted
    for s in [
        "' OR 1=1 --",
        "<script>alert(1)</script>",
        "💣",
        "error: DROP TABLE users;",
        "SELECT * FROM errors WHERE msg='fail'",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s
