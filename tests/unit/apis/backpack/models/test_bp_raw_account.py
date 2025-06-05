"""Unit tests for Backpack raw account models.

Tests validation, serialization, and error handling for Backpack account-related
data models including balance and account summary structures.
"""
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawAccount, BackpackRawBalance


def valid_account() -> dict[str, Any]:
    """Return valid account for testing."""
    # Example based on OpenAPI required fields and types
    return {
        "id": "user_123",
        "email": "user@example.com",
        "status": "active",
    }


def test_BackpackRawAccount_happy_path() -> None:
    """Test BackpackRawAccount happy path."""
    obj = BackpackRawAccount.model_validate(valid_account())
    assert obj.id == "user_123"
    assert obj.email == "user@example.com"
    assert obj.status == "active"


# Schema-driven: Required fields
@pytest.mark.parametrize("missing_field", ["id", "email", "status"])
def test_BackpackRawAccount_missing_required_fields(missing_field: str) -> None:
    """Test BackpackRawAccount missing required fields."""
    p: dict[str, Any] = valid_account().copy()
    del p[missing_field]
    with pytest.raises(ValidationError):
        BackpackRawAccount.model_validate(p)


# Schema-driven: Enum values for status
@pytest.mark.parametrize("status", ["active", "suspended", "pending"])
def test_BackpackRawAccount_status_enum_valid(status: str) -> None:
    """Test BackpackRawAccount status enum valid."""
    p = valid_account().copy()
    p["status"] = status
    obj = BackpackRawAccount.model_validate(p)
    assert obj.status == status


# Adversarial: Invalid enum, case, whitespace, etc.
@pytest.mark.parametrize(
    "status",
    [
        "Active",
        "ACTIVE",
        "pending ",
        "",
        "notastatus",
        "😀",
        "<script>",
        "active; DROP TABLE users;",
    ],
)
def test_BackpackRawAccount_status_enum_invalid(status: str) -> None:
    """Test BackpackRawAccount status enum invalid."""
    p = valid_account().copy()
    p["status"] = status
    with pytest.raises(ValidationError):
        BackpackRawAccount.model_validate(p)


# Adversarial: Wrong types
@pytest.mark.parametrize(
    "field,value",
    [
        ("id", 123),
        ("email", ["user@example.com"]),
        ("status", ["active"]),
    ],
)
def test_BackpackRawAccount_wrong_type_fields(field: str, value: object) -> None:
    """Test BackpackRawAccount wrong type fields."""
    p = valid_account().copy()
    p[field] = value
    with pytest.raises(ValidationError):
        BackpackRawAccount.model_validate(p)


# Adversarial: Empty, whitespace, overlong, unicode, XSS, SQLi
@pytest.mark.parametrize(
    "field,value",
    [
        ("id", ""),
        ("id", "   "),
        ("id", "user_😀"),
        ("id", "user_123; DROP TABLE users;"),
        ("email", "<script>alert(1)</script>@example.com"),
        ("email", "user\x00@example.com"),
        ("email", "a" * 10000 + "@example.com"),
    ],
)
def test_BackpackRawAccount_adversarial_strings(field: str, value: object) -> None:
    """Test BackpackRawAccount adversarial strings."""
    p = valid_account().copy()
    p[field] = value
    if (
        (isinstance(value, str) and value.strip() == "")
        or not isinstance(value, str)
        or (field == "email" and len(value) > 254)
    ):
        with pytest.raises(ValidationError):
            BackpackRawAccount.model_validate(p)
    else:
        obj = BackpackRawAccount.model_validate(p)
        assert isinstance(getattr(obj, field), str)


# Schema-driven: Extra field
def test_BackpackRawAccount_extra_field() -> None:
    """Test BackpackRawAccount extra field."""
    p: dict[str, Any] = valid_account().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawAccount.model_validate(p)


def valid_balance() -> dict[str, Any]:
    """Return valid balance for testing."""
    return {
        "asset": "USDC",
        "available": "1000.0",
        "total": "1000.0",
    }


def test_BackpackRawBalance_happy_path() -> None:
    """Test BackpackRawBalance happy path."""
    obj = BackpackRawBalance.model_validate(valid_balance())
    assert obj.asset == "USDC"
    assert obj.available == "1000.0"
    assert obj.total == "1000.0"


# Schema-driven: Required fields
@pytest.mark.parametrize("missing_field", ["asset", "available", "total"])
def test_BackpackRawBalance_missing_required_fields(missing_field: str) -> None:
    """Test BackpackRawBalance missing required fields."""
    p: dict[str, Any] = valid_balance().copy()
    del p[missing_field]
    with pytest.raises(ValidationError):
        BackpackRawBalance.model_validate(p)


# Schema-driven: Decimal edge cases
@pytest.mark.parametrize(
    "field,value,should_pass",
    [
        ("available", "0", True),
        ("available", "-1.0", True),
        ("available", "NaN", False),
        ("available", "inf", False),
        ("available", "1e6", True),  # Scientific notation is allowed
        ("available", "1..0", False),
        ("total", "0", True),
        ("total", "-1.0", True),
        ("total", "NaN", False),
        ("total", "inf", False),
        ("total", "1e6", True),  # Scientific notation is allowed
        ("total", "1..0", False),
    ],
)
def test_BackpackRawBalance_decimal_edge_cases(field: str, value: str, should_pass: bool) -> None:
    """Test BackpackRawBalance decimal edge cases."""
    # Scientific notation is allowed for decimal fields (project policy)
    p = valid_balance().copy()
    p[field] = value
    if should_pass:
        obj = BackpackRawBalance.model_validate(p)
        assert getattr(obj, field) == value
    else:
        with pytest.raises(ValidationError):
            BackpackRawBalance.model_validate(p)


# Adversarial: Empty, whitespace, overlong, unicode, emoji, XSS, SQLi
@pytest.mark.parametrize(
    "field,value",
    [
        ("asset", ""),
        ("asset", "   "),
        ("asset", "USDC😀"),
        ("asset", "USDC; DROP TABLE balances;"),
        ("asset", "<img src=x onerror=alert(1)>"),
    ],
)
def test_BackpackRawBalance_adversarial_strings(field: str, value: object) -> None:
    """Test BackpackRawBalance adversarial strings."""
    p = valid_balance().copy()
    p[field] = value
    if (isinstance(value, str) and value.strip() == "") or not isinstance(value, str):
        with pytest.raises(ValidationError):
            BackpackRawBalance.model_validate(p)
    else:
        obj = BackpackRawBalance.model_validate(p)
        assert isinstance(getattr(obj, field), str)


# Schema-driven: Extra field
def test_BackpackRawBalance_extra_field() -> None:
    """Test BackpackRawBalance extra field."""
    p: dict[str, Any] = valid_balance().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawBalance.model_validate(p)


def test_BackpackRawBalance_corruption_cases() -> None:
    """Test BackpackRawBalance with creative corruption cases to ensure robust validation.
    
    Each case simulates a different form of data corruption or hostile input.
    """
    base = valid_balance()
    corruption_cases = [
        # 1. Null value for required field
        ("available", None, "null value for required field"),
        # 2. Binary data instead of string
        ("total", b"\x00\x01", "binary data instead of string"),
        # 3. Nested object instead of string
        ("asset", {"foo": "bar"}, "nested object instead of string"),
        # 4. List instead of string
        ("available", ["1000.0"], "list instead of string"),
        # 5. Integer instead of string for decimal field
        ("total", 1000, "integer instead of string for decimal field"),
        # 6. Float instead of string for decimal field
        ("available", 1000.0, "float instead of string for decimal field"),
        # 7. Garbled unicode string
        ("asset", "\udce2\udc28\udc00", "garbled unicode string"),
        # 8. Overly large string (potential DoS)
        ("asset", "A" * 10**7, "overly large string (potential DoS)"),
        # 9. Truncated JSON-like string
        ("available", '{"incomplete": ', "truncated JSON-like string"),
        # 10. SQL injection attempt
        ("asset", "USDC'; DROP TABLE balances;--", "SQL injection attempt"),
    ]
    for field, value, description in corruption_cases:
        p = base.copy()
        p[field] = value
        if description == "SQL injection attempt":
            # For raw models, SQLi content should be accepted as a valid string
            BackpackRawBalance.model_validate(p)
        else:
            try:
                BackpackRawBalance.model_validate(p)
            except ValidationError:
                pass  # Expected
            else:
                pytest.fail(
                    f"Failed corruption case: {description} ("
                    f"{field}={value!r}) - ValidationError not raised",
                )


def test_BackpackRawAccount_real_json_example() -> None:
    """Validate BackpackRawAccount using a real JSON payload with edge values."""
    payload = {
        "id": "user_Ωmega",
        "email": "user_😀@example.com",
        "status": "active",
    }
    obj = BackpackRawAccount.model_validate(payload)
    assert obj.id == "user_Ωmega"
    assert obj.email == "user_😀@example.com"
    assert obj.status == "active"


def test_BackpackRawAccount_corruption_null_id() -> None:
    """Should fail: null value for required 'id'."""
    p = valid_account().copy()
    p["id"] = None
    with pytest.raises(ValidationError):
        BackpackRawAccount.model_validate(p)


def test_BackpackRawAccount_corruption_binary_email() -> None:
    """Should fail: binary data for 'email'."""
    p = valid_account().copy()
    p["email"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawAccount.model_validate(p)


def test_BackpackRawAccount_corruption_nested_status() -> None:
    """Should fail: nested object for 'status'."""
    p = valid_account().copy()
    p["status"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawAccount.model_validate(p)


def test_BackpackRawAccount_corruption_list_id() -> None:
    """Should fail: list for 'id'."""
    p = valid_account().copy()
    p["id"] = ["user_123"]
    with pytest.raises(ValidationError):
        BackpackRawAccount.model_validate(p)


def test_BackpackRawAccount_corruption_garbled_unicode_email() -> None:
    """Should fail: garbled unicode in 'email'."""
    p = valid_account().copy()
    p["email"] = "user\udce2\udc28\udc00@example.com"
    with pytest.raises(ValidationError):
        BackpackRawAccount.model_validate(p)


def test_BackpackRawBalance_real_json_example() -> None:
    """Validate BackpackRawBalance using a real JSON payload with edge values."""
    payload = {
        "asset": "USDC_😀",
        "available": "0.00000001",
        "total": "99999999.99999999",
    }
    obj = BackpackRawBalance.model_validate(payload)
    assert obj.asset == "USDC_😀"
    assert obj.available == "0.00000001"
    assert obj.total == "99999999.99999999"


def test_BackpackRawBalance_corruption_null_asset() -> None:
    """Should fail: null value for required 'asset'."""
    p = valid_balance().copy()
    p["asset"] = None
    with pytest.raises(ValidationError):
        BackpackRawBalance.model_validate(p)


def test_BackpackRawBalance_corruption_binary_available() -> None:
    """Should fail: binary data for 'available'."""
    p = valid_balance().copy()
    p["available"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawBalance.model_validate(p)


def test_BackpackRawBalance_corruption_nested_total() -> None:
    """Should fail: nested object for 'total'."""
    p = valid_balance().copy()
    p["total"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawBalance.model_validate(p)


def test_BackpackRawBalance_corruption_list_asset() -> None:
    """Should fail: list for 'asset'."""
    p = valid_balance().copy()
    p["asset"] = ["USDC"]
    with pytest.raises(ValidationError):
        BackpackRawBalance.model_validate(p)


def test_BackpackRawBalance_corruption_garbled_unicode_asset() -> None:
    """Should fail: garbled unicode in 'asset'."""
    p = valid_balance().copy()
    p["asset"] = "USDC\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawBalance.model_validate(p)
