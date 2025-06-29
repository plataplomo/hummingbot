"""Unit tests for Hyperliquid Raw Orderbook Models.

CyberDeltaEngine: Hyperliquid Raw Orderbook Model Test Suite
-----------------------------------------------------------
Comprehensive tests for all models in hl_raw_orderbook.py
- Strictly follows Raw Model Validation Policy
- Covers all edge cases, adversarial input, and structure validation
"""

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawBookLevel,
    HyperliquidRawL2Book,
    HyperliquidRawL2BookRequestPayload,
)


# --- Helper: Valid minimal payloads for each model ---
def valid_book_level() -> dict[str, object]:
    """Return valid book level for testing."""
    return {"px": "123.45", "sz": "1.0", "n": 1}


def valid_l2book() -> dict[str, object]:
    """Return valid l2book for testing."""
    return {
        "coin": "ETH",
        "levels": [[valid_book_level()], [valid_book_level()]],
        "time": 1234567890,
    }


def valid_l2book_request_payload() -> dict[str, object]:
    """Return valid l2book request payload for testing."""
    return {"type": "l2Book", "coin": "ETH"}


# --- Tests for HyperliquidRawBookLevel ---
def test_book_level_happy_path() -> None:
    """Test book level happy path."""
    obj = HyperliquidRawBookLevel.model_validate(valid_book_level())
    assert obj.px == "123.45"
    assert obj.sz == "1.0"
    assert obj.n == 1


def test_book_level_missing_required() -> None:
    """Test book level missing required."""
    for field in ["px", "sz", "n"]:
        d = valid_book_level().copy()
        del d[field]
        with pytest.raises(ValidationError):
            HyperliquidRawBookLevel.model_validate(d)


def test_book_level_type_errors() -> None:
    """Test book level type errors."""
    d = valid_book_level().copy()
    d["px"] = 123.45
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawBookLevel.model_validate(d)
    d = valid_book_level().copy()
    d["n"] = "notanint"
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawBookLevel.model_validate(d)


def test_book_level_format_errors() -> None:
    """Test book level format errors."""
    for field in ["px", "sz"]:
        d = valid_book_level().copy()
        d[field] = ""
        with pytest.raises(ValidationError):
            HyperliquidRawBookLevel.model_validate(d)
        d[field] = "a" * 1000
        with pytest.raises(ValidationError):
            HyperliquidRawBookLevel.model_validate(d)
    d = valid_book_level().copy()
    d["px"] = "NaN"
    with pytest.raises(ValidationError):
        HyperliquidRawBookLevel.model_validate(d)


def test_book_level_extra_field() -> None:
    """Test book level extra field."""
    d = valid_book_level().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawBookLevel.model_validate(d)


def test_book_level_adversarial_strings() -> None:
    """Test book level adversarial strings."""
    d = valid_book_level().copy()
    d["px"] = "1e6"
    obj = HyperliquidRawBookLevel.model_validate(d)
    assert obj.px == "1000000"  # Business logic normalizes decimal strings


# --- Tests for HyperliquidRawL2Book ---
def test_l2book_happy_path() -> None:
    """Test l2book happy path."""
    obj = HyperliquidRawL2Book.model_validate(valid_l2book())
    assert obj.coin == "ETH"
    assert isinstance(obj.levels, list)
    assert obj.time == 1234567890


def test_l2book_missing_required() -> None:
    """Test l2book missing required."""
    for field in ["coin", "levels", "time"]:
        d = valid_l2book().copy()
        del d[field]
        with pytest.raises(ValidationError):
            HyperliquidRawL2Book.model_validate(d)


def test_l2book_type_errors() -> None:
    """Test l2book type errors."""
    d = valid_l2book().copy()
    d["coin"] = 123
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawL2Book.model_validate(d)
    d = valid_l2book().copy()
    d["levels"] = "notalist"
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawL2Book.model_validate(d)
    d = valid_l2book().copy()
    d["time"] = "notanint"
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawL2Book.model_validate(d)


def test_l2book_levels_structure() -> None:
    """Test l2book levels structure."""
    d = valid_l2book().copy()
    d["levels"] = []
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)
    d["levels"] = [[valid_book_level()]]
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)
    d["levels"] = [[valid_book_level()], [valid_book_level()], [valid_book_level()]]
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)


def test_l2book_nested_model_error() -> None:
    """Test l2book nested model error."""
    d = valid_l2book().copy()
    # mypy: ignore-next-line (we know the structure is correct for this test)
    d["levels"][0][0]["px"] = "notanumber"  # type: ignore[index]
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)


def test_l2book_extra_field() -> None:
    """Test l2book extra field."""
    d = valid_l2book().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)


def test_l2book_adversarial_strings() -> None:
    """Test l2book adversarial strings."""
    d = valid_l2book().copy()
    d["coin"] = "💣"
    obj = HyperliquidRawL2Book.model_validate(d)
    assert obj.coin == "💣"


# --- Tests for HyperliquidRawL2BookRequestPayload ---
def test_l2book_request_payload_happy_path() -> None:
    """Test l2book request payload happy path."""
    obj = HyperliquidRawL2BookRequestPayload.model_validate(valid_l2book_request_payload())
    assert obj.type == "l2Book"
    assert obj.coin == "ETH"


def test_l2book_request_payload_type_errors() -> None:
    """Test l2book request payload type errors."""
    d = valid_l2book_request_payload().copy()
    d["coin"] = 123
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawL2BookRequestPayload.model_validate(d)
    d = valid_l2book_request_payload().copy()
    d["type"] = 123
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawL2BookRequestPayload.model_validate(d)


def test_l2book_request_payload_format_errors() -> None:
    """Test l2book request payload format errors."""
    d = valid_l2book_request_payload().copy()
    d["coin"] = ""
    with pytest.raises(ValidationError):
        HyperliquidRawL2BookRequestPayload.model_validate(d)
    d["coin"] = "a" * 1000
    with pytest.raises(ValidationError):
        HyperliquidRawL2BookRequestPayload.model_validate(d)


def test_l2book_request_payload_extra_field() -> None:
    """Test l2book request payload extra field."""
    d = valid_l2book_request_payload().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawL2BookRequestPayload.model_validate(d)


# --- Additional edge case tests (OpenAPI/SDK/real-world) ---
def test_l2book_coin_edge_cases() -> None:
    """Test l2book coin edge cases."""
    # Emoji, whitespace, symbols, bidi text
    for coin in ["ETH 💎", "   BTC   ", "COIN-123!@#", "\u202eABC\u202c"]:
        d = valid_l2book().copy()
        d["coin"] = coin
        obj = HyperliquidRawL2Book.model_validate(d)
        assert obj.coin == coin


def test_l2book_levels_structure_edge_cases() -> None:
    """Test l2book levels structure edge cases."""
    # Not a list, wrong number of sublists, empty sublists, wrong type, excessive levels
    d = valid_l2book().copy()
    d["levels"] = "notalist"
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)
    d["levels"] = [[valid_book_level()]]
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)
    d["levels"] = [[], []]
    obj = HyperliquidRawL2Book.model_validate(d)
    assert obj.levels == [[], []]
    d["levels"] = [[valid_book_level()] for _ in range(3)]
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)
    d["levels"] = [[valid_book_level()] * 1000, [valid_book_level()] * 1000]
    obj = HyperliquidRawL2Book.model_validate(d)
    assert len(obj.levels[0]) == 1000


def test_book_level_field_edge_cases() -> None:
    """Test book level field edge cases."""
    # Missing fields, wrong types, negative/zero/large n, px/sz as above
    for field in ["px", "sz", "n"]:
        d = valid_book_level().copy()
        del d[field]
        with pytest.raises(ValidationError):
            HyperliquidRawBookLevel.model_validate(d)
    d = valid_book_level().copy()
    d["n"] = -1
    with pytest.raises(ValidationError):
        HyperliquidRawBookLevel.model_validate(d)
    d["n"] = 0
    obj = HyperliquidRawBookLevel.model_validate(d)
    assert obj.n == 0
    d["n"] = 2**31
    obj = HyperliquidRawBookLevel.model_validate(d)
    assert obj.n == 2**31
    d = valid_book_level().copy()
    d["px"] = "000123.4500"
    obj = HyperliquidRawBookLevel.model_validate(d)
    assert obj.px == "123.45"  # Business logic normalizes decimal strings
    d["px"] = "1.23e2"
    obj = HyperliquidRawBookLevel.model_validate(d)
    assert obj.px == "123"  # Business logic normalizes decimal strings
    d["px"] = "-123.45"
    obj = HyperliquidRawBookLevel.model_validate(d)
    assert obj.px == "-123.45"  # This one doesn't change
    d["px"] = "1" * 65
    with pytest.raises(ValidationError):
        HyperliquidRawBookLevel.model_validate(d)


def test_l2book_time_field_edge_cases() -> None:
    """Test l2book time field edge cases."""
    # Negative, zero, very large, string instead of int
    d = valid_l2book().copy()
    d["time"] = -1
    obj = HyperliquidRawL2Book.model_validate(d)
    assert obj.time == -1
    d["time"] = 0
    obj = HyperliquidRawL2Book.model_validate(d)
    assert obj.time == 0
    d["time"] = 2**63 - 1
    obj = HyperliquidRawL2Book.model_validate(d)
    assert obj.time == 2**63 - 1
    d["time"] = "notanint"
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawL2Book.model_validate(d)


def test_l2book_extra_fields() -> None:
    """Test l2book extra fields."""
    # Extra fields at all levels
    d = valid_l2book().copy()
    d["extra"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)
    d = valid_l2book().copy()
    # Fix: Only add extra field if levels[0] is non-empty and a list of dicts
    if (
        isinstance(d["levels"], list)
        and d["levels"]
        and isinstance(d["levels"][0], list)
        and d["levels"][0]
    ):
        pass  # d["levels"] is already correct
    else:
        # fallback: ensure at least one book level exists
        d["levels"] = [[valid_book_level()]]
    # At this point, d["levels"][0] is always a list[dict[str, Any]]
    levels0 = d["levels"][0]  # type: ignore[index]
    levels0[0]["extra"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawL2Book.model_validate(d)
    d = valid_l2book_request_payload().copy()
    d["extra"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawL2BookRequestPayload.model_validate(d)
