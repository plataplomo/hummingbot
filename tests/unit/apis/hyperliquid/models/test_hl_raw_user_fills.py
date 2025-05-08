# CyberDeltaEngine: Hyperliquid Raw User Fills Model Test Suite
# -------------------------------------------------------------
# Comprehensive tests for all models in hl_raw_user_fills.py
# - Strictly follows Raw Model Validation Policy
# - Covers all edge cases, adversarial input, and structure validation

from typing import Any

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


# --- Fixtures ---
@pytest.fixture
def valid_user_fill_data() -> dict[str, Any]:
    """Provides a dictionary with valid raw user fill data."""
    return {
        "tid": 123456789,
        "coin": "ETH",
        "px": "2000.50",
        "sz": "0.1",
        "time": 1678886400123,  # Example epoch ms
        "side": "B",
        "oid": 987654321,
        "startPosition": "1.0",
        "dir": "Buy",
        "hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef123456",
        "fee": "0.002",
        "isMaker": False,
        "liquidationMarkPx": None,
        "cloid": None,
    }


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
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawUserFill.model_validate(d)
        # Simpler assertion for missing field, ensuring field name is present and "Field required"
        error_str = str(exc_info.value)
        # Pydantic v2 often puts the field name on its own line above the error
        assert f"\n{field}\n" in error_str or f" {field}\n" in error_str
        assert "Field required" in error_str


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
    # Expect ValidationError because RawNonNegativeInt uses a validator that expects int
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawUserFill.model_validate(d)
    assert "Must be an integer" in str(exc_info.value)

    d = valid_user_fill().copy()
    d["coin"] = 123
    with pytest.raises(ValidationError) as exc_info_coin:
        HyperliquidRawUserFill.model_validate(d)
    assert "Expected string" in str(exc_info_coin.value)

    d = valid_user_fill().copy()
    d["isMaker"] = "true"
    with pytest.raises(ValidationError) as exc_info_maker:
        HyperliquidRawUserFill.model_validate(d)
    # RawStrictBool enforces bool type
    assert "Must be a boolean, got str" in str(exc_info_maker.value)

    d = valid_user_fill().copy()
    d["time"] = 123.45  # Float instead of int
    with pytest.raises(ValidationError) as exc_info_time:
        HyperliquidRawUserFill.model_validate(d)
    assert "Must be an integer" in str(exc_info_time.value)

    d = valid_user_fill().copy()
    d["oid"] = "id-string"  # String instead of int
    with pytest.raises(ValidationError) as exc_info_oid:
        HyperliquidRawUserFill.model_validate(d)
    assert "Must be an integer" in str(exc_info_oid.value)


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
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawUserFill.model_validate(d)
    error_str = str(exc_info.value).lower()
    assert "extra" in error_str
    assert "not permitted" in error_str


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
    """Test edge cases for cloid and hash strings (length)."""
    # Test overlong cloid
    d_cloid = valid_user_fill().copy()
    d_cloid["cloid"] = "a" * 129  # Max is 128
    with pytest.raises(ValidationError, match="cloid: String value too long"):
        HyperliquidRawUserFill.model_validate(d_cloid)

    # Test overlong hash
    d_hash = valid_user_fill().copy()
    d_hash["hash"] = "a" * 67  # Max is 66
    with pytest.raises(ValidationError, match="hash: String value too long"):
        HyperliquidRawUserFill.model_validate(d_hash)

    # Test empty optional cloid (should pass)
    d_empty_cloid = valid_user_fill().copy()
    d_empty_cloid["cloid"] = ""  # Empty string is invalid if provided
    with pytest.raises(ValidationError, match="cloid: String cannot be empty"):
        HyperliquidRawUserFill.model_validate(d_empty_cloid)

    # Test empty required hash (should fail)
    d_empty_hash = valid_user_fill().copy()
    d_empty_hash["hash"] = ""
    with pytest.raises(ValidationError, match="hash: String cannot be empty"):
        HyperliquidRawUserFill.model_validate(d_empty_hash)


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


# --- Success Cases ---
def test_hl_raw_user_fill_valid(valid_user_fill_data: dict[str, Any]) -> None:
    """Test successful validation with completely valid data."""
    fill = HyperliquidRawUserFill.model_validate(valid_user_fill_data)

    assert fill.tid == 123456789
    assert fill.coin == "ETH"
    assert fill.px == "2000.50"
    assert fill.sz == "0.1"
    assert fill.time == 1678886400123
    assert fill.side == "B"
    assert fill.oid == 987654321
    assert fill.start_position == "1.0"
    assert fill.dir == "Buy"
    assert fill.hash == "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef123456"
    assert fill.fee == "0.002"
    assert fill.is_maker is False
    assert fill.liquidation_mark_px is None
    assert fill.cloid is None
    assert fill.model_config.get("extra") == "forbid"
    assert fill.model_config.get("frozen") is True


def test_hl_raw_user_fill_optional_present(valid_user_fill_data: dict[str, Any]) -> None:
    """Test validation succeeds when optional fields are present and valid."""
    valid_user_fill_data["liquidationMarkPx"] = "1950.00"
    valid_user_fill_data["cloid"] = "my-client-order-id"
    fill = HyperliquidRawUserFill.model_validate(valid_user_fill_data)
    assert fill.liquidation_mark_px == "1950.00"
    assert fill.cloid == "my-client-order-id"


# --- Failure Cases: Type Errors ---
@pytest.mark.parametrize(
    "field, invalid_value",
    [
        # ("tid", "123"), # Validator allows numeric string -> int coercion
        ("coin", 123),
        ("px", 2000.50),  # Validator allows float -> Decimal string coercion
        ("sz", 0.1),  # Validator allows float -> Decimal string coercion
        # ("time", "1678886400123"), # Validator allows numeric string -> int coercion
        ("side", ["B"]),
        # ("oid", "987"), # Validator allows numeric string -> int coercion
        ("startPosition", 1.0),  # Validator allows float -> Decimal string coercion
        ("dir", True),
        ("hash", None),  # Hash is required string, not Optional
        ("fee", 0.002),  # Validator allows float -> Decimal string coercion
        ("isMaker", "false"),  # Must be bool
        (
            "liquidationMarkPx",
            1950.0,
        ),  # Validator allows float -> Decimal string coercion if present
        ("cloid", 12345),  # Must be string if present
    ],
)
def test_hl_raw_user_fill_invalid_types(
    valid_user_fill_data: dict[str, Any],
    field: str,
    invalid_value: object,  # Changed from Any to object
) -> None:
    """Test ValidationError is raised for incorrect field types."""
    valid_user_fill_data[field] = invalid_value
    expected_exception: type[ValidationError] | tuple[type[ValidationError], type[TypeError]] = (
        ValidationError
    )
    if field == "isMaker":
        expected_exception = (ValidationError, TypeError)  # Broadened for isMaker

    with pytest.raises(expected_exception) as exc_info:
        HyperliquidRawUserFill.model_validate(valid_user_fill_data)

    # Determine expected field name in error message (Pydantic normalizes to snake_case)
    expected_error_field = field
    if field == "startPosition":
        expected_error_field = "start_position"
    elif field == "liquidationMarkPx":
        expected_error_field = "liquidation_mark_px"
    elif field == "isMaker":
        expected_error_field = "is_maker"
    # Add other camelCase to snake_case mappings if needed

    # Check field name is in error message
    assert (
        f"'{expected_error_field}'" in str(exc_info.value)
        or f"{expected_error_field}:" in str(exc_info.value)
        or f"{expected_error_field}\n" in str(exc_info.value)
    )

    # Conditional assertion for specific 'isMaker' error message
    if field == "isMaker":
        assert "is_maker: Must be a boolean, got str." in str(exc_info.value)
    # For other fields, the general presence of 'Value error' and the field name is enough,
    # as Pydantic will detail the specific type mismatch.


# --- Failure Cases: Format/Constraint Errors ---
@pytest.mark.parametrize(
    "field, invalid_value, expected_keywords",
    [
        ("tid", -1, ("value", "-1", "cannot be negative")),
        ("coin", "", ("string", "cannot be empty")),
        ("coin", "X" * 65, ("string", "too long", "max 64")),
        ("px", "", ("string", "cannot be empty")),
        ("px", "inf", ("finite decimal", "inf")),
        ("sz", "NaN", ("finite decimal", "nan")),
        ("time", -1000, ("timestamp", "-1000", "non-negative")),
        ("side", "BUY", ("invalid", "value 'buy'")),
        ("oid", -1, ("value", "-1", "cannot be negative")),
        ("startPosition", "", ("string", "cannot be empty")),
        ("dir", "", ("string", "cannot be empty")),
        ("hash", "", ("string", "cannot be empty")),
        ("hash", "X" * 67, ("string", "too long", "max 66")),
        ("liquidationMarkPx", "", ("string", "cannot be empty")),
        ("liquidationMarkPx", "inf", ("finite decimal", "inf")),
        ("cloid", "", ("string", "cannot be empty")),
        ("cloid", "Y" * 129, ("string", "too long", "max 128")),
    ],
)
def test_hl_raw_user_fill_invalid_formats(
    valid_user_fill_data: dict[str, Any],
    field: str,
    invalid_value: object,
    expected_keywords: tuple[str, ...],
) -> None:
    """Test ValidationError for format/constraint violations using keywords."""
    valid_user_fill_data[field] = invalid_value

    # Skip the fee test case as it's known to be invalid logic for now
    if field == "fee" and invalid_value == "-0.1":
        pytest.skip("Skipping invalid test case: fee can be negative (rebates)")

    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawUserFill.model_validate(valid_user_fill_data)

    error_str = str(exc_info.value).lower()
    # Check all keywords are present
    for keyword in expected_keywords:
        assert keyword.lower() in error_str


# --- Failure Cases: Missing Required Fields ---
@pytest.mark.parametrize(
    "field_to_remove",
    [
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
    ],
)
def test_hl_raw_user_fill_missing_required(
    valid_user_fill_data: dict[str, Any], field_to_remove: str
) -> None:
    """Test ValidationError when required fields are missing."""
    del valid_user_fill_data[field_to_remove]
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawUserFill.model_validate(valid_user_fill_data)
    assert f"{field_to_remove}\n  Field required" in str(exc_info.value)


# --- Failure Cases: Extra Fields ---
def test_hl_raw_user_fill_extra_field(valid_user_fill_data: dict[str, Any]) -> None:
    """Test ValidationError when extra fields are provided (extra='forbid')."""
    valid_user_fill_data["extraField"] = 123
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawUserFill.model_validate(valid_user_fill_data)
    assert "Extra inputs are not permitted" in str(exc_info.value)
