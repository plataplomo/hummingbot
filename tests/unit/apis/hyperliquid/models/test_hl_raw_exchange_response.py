# tests/unit/apis/hyperliquid/models/test_hl_raw_exchange_response.py
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeResponseData,
    HyperliquidRawExchangeStatusFilled,
    HyperliquidRawExchangeStatusObject,
    HyperliquidRawExchangeStatusResting,
)


# --- Fixtures ---
@pytest.fixture
def valid_resting_data() -> dict[str, Any]:
    return {"oid": 12345}


@pytest.fixture
def valid_filled_data() -> dict[str, Any]:
    return {"oid": 67890, "totalSz": "1.5", "avgPx": "150.25"}


@pytest.fixture
def valid_status_object_resting(valid_resting_data: dict[str, Any]) -> dict[str, Any]:
    return {"resting": valid_resting_data}


@pytest.fixture
def valid_status_object_filled(valid_filled_data: dict[str, Any]) -> dict[str, Any]:
    return {"filled": valid_filled_data}


@pytest.fixture
def valid_status_object_error() -> dict[str, Any]:
    return {"error": "Order rejected due to insufficient margin."}


@pytest.fixture
def valid_response_data_dict(
    valid_status_object_resting: dict[str, Any], valid_status_object_filled: dict[str, Any]
) -> dict[str, Any]:
    return {
        "type": "order",
        "statuses": [
            "canceled",
            valid_status_object_resting,
            valid_status_object_filled,
            "modified",
            "success",
        ],
    }


@pytest.fixture
def valid_top_level_response(valid_response_data_dict: dict[str, Any]) -> dict[str, Any]:
    return {"status": "ok", "data": valid_response_data_dict}


# --- Success Cases ---
def test_hl_resting_valid(valid_resting_data: dict[str, Any]) -> None:
    obj = HyperliquidRawExchangeStatusResting.model_validate(valid_resting_data)
    assert obj.oid == 12345
    assert obj.model_config.get("extra") == "forbid"
    assert obj.model_config.get("frozen") is True


def test_hl_filled_valid(valid_filled_data: dict[str, Any]) -> None:
    obj = HyperliquidRawExchangeStatusFilled.model_validate(valid_filled_data)
    assert obj.oid == 67890
    assert obj.total_sz == "1.5"
    assert obj.avg_px == "150.25"
    assert obj.model_config.get("extra") == "forbid"
    assert obj.model_config.get("frozen") is True


def test_hl_status_object_valid(
    valid_status_object_resting: dict[str, Any],
    valid_status_object_filled: dict[str, Any],
    valid_status_object_error: dict[str, Any],
) -> None:
    resting = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_resting)
    assert resting.resting is not None
    assert resting.resting.oid == 12345
    assert resting.filled is None
    assert resting.error is None
    assert resting.model_config.get("extra") == "forbid"
    assert resting.model_config.get("frozen") is True

    filled = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_filled)
    assert filled.resting is None
    assert filled.filled is not None
    assert filled.filled.oid == 67890
    assert filled.error is None
    assert filled.model_config.get("extra") == "forbid"
    assert filled.model_config.get("frozen") is True

    error = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_error)
    assert error.resting is None
    assert error.filled is None
    assert error.error == "Order rejected due to insufficient margin."
    assert error.model_config.get("extra") == "forbid"
    assert error.model_config.get("frozen") is True


def test_hl_response_data_valid(valid_response_data_dict: dict[str, Any]) -> None:
    obj = HyperliquidRawExchangeResponseData.model_validate(valid_response_data_dict)
    assert obj.type == "order"
    assert len(obj.statuses) == 5
    assert obj.statuses[0] == "canceled"
    assert isinstance(obj.statuses[1], HyperliquidRawExchangeStatusObject)
    assert obj.statuses[1].resting is not None
    assert obj.statuses[1].resting.oid == 12345
    assert isinstance(obj.statuses[2], HyperliquidRawExchangeStatusObject)
    assert obj.statuses[2].filled is not None
    assert obj.statuses[2].filled.oid == 67890
    assert obj.statuses[3] == "modified"
    assert obj.statuses[4] == "success"
    assert obj.model_config.get("extra") == "forbid"
    assert obj.model_config.get("frozen") is True


def test_hl_response_valid(valid_top_level_response: dict[str, Any]) -> None:
    obj = HyperliquidRawExchangeResponse.model_validate(valid_top_level_response)
    assert obj.status == "ok"
    assert obj.data is not None
    assert obj.data.type == "order"
    assert len(obj.data.statuses) == 5
    assert obj.model_config.get("extra") == "forbid"
    assert obj.model_config.get("frozen") is True


def test_hl_response_valid_no_data() -> None:
    """Test valid response when data is explicitly None."""
    response_dict = {"status": "ok", "data": None}
    obj = HyperliquidRawExchangeResponse.model_validate(response_dict)
    assert obj.status == "ok"
    assert obj.data is None


def test_hl_response_valid_missing_data() -> None:
    """Test valid response when data key is missing."""
    response_dict = {"status": "ok"}
    obj = HyperliquidRawExchangeResponse.model_validate(response_dict)
    assert obj.status == "ok"
    assert obj.data is None


# --- Failure Cases --- #


# Resting Model Failures
@pytest.mark.parametrize(
    "invalid_data, expected_msg_part",
    [
        ({"oid": -1}, "Value -1 cannot be negative"),
        ({"oid": "abc"}, "Must be an integer"),
        ({"oid": 1.0}, "Must be an integer"),
        ({"oid": 1.0}, "Must be an integer"),
        ({}, "Field required"),
    ],
)
def test_hl_resting_invalid(invalid_data: dict[str, Any], expected_msg_part: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusResting.model_validate(invalid_data)
    assert expected_msg_part.lower() in str(exc_info.value).lower()


def test_hl_resting_extra_fields_ignored() -> None:
    """Test that extra fields are rejected due to extra='forbid'."""
    data = {"oid": 123, "extra": 1, "another": "field"}
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusResting.model_validate(data)
    assert "extra" in str(exc_info.value).lower()
    assert "not permitted" in str(exc_info.value).lower()


# Filled Model Failures
@pytest.mark.parametrize(
    "invalid_data, expected_keywords",
    [
        ({"oid": 1, "totalSz": "1.5", "avgPx": "inf"}, ("finite decimal", "inf")),
        ({"oid": 1, "totalSz": "NaN", "avgPx": "1.0"}, ("finite decimal", "nan")),
        ({"oid": 1, "totalSz": "", "avgPx": "1.0"}, ("string", "cannot be empty")),
        ({"oid": 1, "totalSz": "1.0", "avgPx": ""}, ("string", "cannot be empty")),
        ({"oid": 1, "totalSz": "1.0", "avgPx": "1..0"}, ("cannot convert", "1..0")),
        ({"oid": 1, "totalSz": 1.0, "avgPx": "1.0"}, ("expected string", "got float")),
        ({"oid": -1, "totalSz": "1", "avgPx": "1"}, ("value", "-1", "cannot be negative")),
        ({"oid": 1, "totalSz": "1"}, ("field required", "avgPx")),
        ({"oid": 1, "avgPx": "1"}, ("field required", "totalSz")),
        ({"totalSz": "1", "avgPx": "1"}, ("field required", "oid")),
    ],
)
def test_hl_filled_invalid(
    invalid_data: dict[str, Any], expected_keywords: tuple[str, ...]
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusFilled.model_validate(invalid_data)
    error_str = str(exc_info.value).lower()
    for keyword in expected_keywords:
        assert keyword.lower() in error_str


def test_hl_filled_extra_fields_ignored() -> None:
    """Test that extra fields are rejected due to extra='forbid'."""
    data = {"oid": 1, "totalSz": "1", "avgPx": "1", "extra": 1}
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusFilled.model_validate(data)
    assert "extra" in str(exc_info.value).lower()
    assert "not permitted" in str(exc_info.value).lower()


# Status Object Failures
@pytest.mark.parametrize(
    "invalid_data, expected_keywords",
    [
        ({"resting": {"oid": -1}}, ("value", "-1", "cannot be negative")),
        ({"filled": {"oid": 1, "totalSz": "", "avgPx": "1"}}, ("string", "cannot be empty")),
        ({"error": ""}, ("string", "cannot be empty")),
        ({"error": 123}, ("expected string", "got int")),
    ],
)
def test_hl_status_object_invalid(
    invalid_data: dict[str, Any], expected_keywords: tuple[str, ...]
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusObject.model_validate(invalid_data)
    error_str = str(exc_info.value).lower()
    for keyword in expected_keywords:
        assert keyword.lower() in error_str


def test_hl_status_object_extra_fields_ignored() -> None:
    """Test that extra fields are rejected due to extra='forbid'."""
    data = {"error": "Some error", "extra": 1}
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusObject.model_validate(data)
    assert "extra" in str(exc_info.value).lower()
    assert "not permitted" in str(exc_info.value).lower()


# Response Data Failures
@pytest.mark.parametrize(
    "invalid_data, expected_exception, expected_keywords",
    [
        ({"type": "", "statuses": []}, ValueError, ("string", "cannot be empty")),
        ({"type": 123, "statuses": []}, ValueError, ("expected string", "got int")),
        ({"type": "order", "statuses": 123}, TypeError, ("must be a list",)),
        (
            {"type": "order", "statuses": [1, 2]},
            ValidationError,
            ("expected string", "got int"),
        ),
        (
            {"type": "order", "statuses": ["invalid_status"]},
            ValidationError,
            ("invalid", "invalid_status"),
        ),
        (
            {"type": "order", "statuses": [{"resting": {"oid": -1}}]},
            ValidationError,
            ("value", "-1", "cannot be negative"),
        ),
        ({"type": "order"}, ValidationError, ("field required", "statuses")),
        ({"statuses": []}, ValidationError, ("field required", "type")),
    ],
)
def test_hl_response_data_invalid(
    invalid_data: dict[str, Any],
    expected_exception: type[Exception],
    expected_keywords: tuple[str, ...],
) -> None:
    with pytest.raises(expected_exception) as exc_info:
        HyperliquidRawExchangeResponseData.model_validate(invalid_data)
    error_str = str(exc_info.value).lower()
    for keyword in expected_keywords:
        assert keyword.lower() in error_str


def test_hl_response_data_extra_fields_ignored() -> None:
    """Test that extra fields are rejected due to extra='forbid'."""
    data: dict[str, Any] = {"type": "order", "statuses": [], "extra": 1}
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeResponseData.model_validate(data)
    assert "extra" in str(exc_info.value).lower()
    assert "not permitted" in str(exc_info.value).lower()


# Top Level Response Failures
@pytest.mark.parametrize(
    "invalid_data, expected_exception, expected_keywords",
    [
        ({"status": "error", "data": None}, ValidationError, ("literal_error", "status")),
        ({"status": 123, "data": None}, ValueError, ("expected string", "got int")),
        ({}, ValidationError, ("field required", "status")),
        (
            {"status": "ok", "data": {"type": "order", "statuses": [1]}},
            ValidationError,
            ("expected string", "got int"),
        ),
        ({"status": "ok", "extra_field": 1}, ValidationError, ("extra", "not permitted")),
        (
            {"status": "ok", "data": {"type": "", "statuses": []}},
            ValueError,
            ("string", "cannot be empty"),
        ),
    ],
)
def test_hl_response_invalid(
    invalid_data: dict[str, Any],
    expected_exception: type[Exception],
    expected_keywords: tuple[str, ...],
) -> None:
    with pytest.raises(expected_exception) as exc_info:
        HyperliquidRawExchangeResponse.model_validate(invalid_data)
    error_str = str(exc_info.value).lower()
    for keyword in expected_keywords:
        assert keyword.lower() in error_str


# --- Specific Tests for statuses List Validation ---


@pytest.mark.parametrize(
    "statuses_list, is_valid",
    [
        (["canceled", "modified", "success"], True),  # All valid strings
        ([{"resting": {"oid": 1}}], True),  # Valid resting object
        ([{"filled": {"oid": 2, "totalSz": "1", "avgPx": "10"}}], True),  # Valid filled object
        ([{"error": "Failed"}], True),  # Valid error object
        (["success", {"resting": {"oid": 3}}, {"error": "Timeout"}], True),  # Mixed valid
        ([], True),  # Empty list is valid
        (["canceled", "unknown"], False),  # Invalid string
        ([{"resting": {"oid": -1}}], False),  # Invalid object content (negative oid)
        ([{"filled": {"oid": 4}}], False),  # Invalid object content (missing fields)
        ([123], False),  # Wrong item type (int)
        ([None], False),  # Wrong item type (None)
        (["canceled", 123], False),  # Mixed valid string and invalid type
        ([{"resting": {"oid": 5}}, 123], False),  # Mixed valid object and invalid type
        ("not_a_list", False),  # Input not a list
    ],
)
def test_hl_response_data_statuses_validation(
    statuses_list: Any,  # noqa: ANN401 - Any is needed for test parametrization
    is_valid: bool,
) -> None:
    """Test the validation logic for the 'statuses' field specifically."""
    data_dict = {"type": "order", "statuses": statuses_list}
    if is_valid:
        try:
            response_data = HyperliquidRawExchangeResponseData.model_validate(data_dict)
            # Further checks if needed, e.g., check item types in validated_list
            assert isinstance(response_data.statuses, list)
            # Example check on item types
            for item in response_data.statuses:
                assert isinstance(item, str | HyperliquidRawExchangeStatusObject)  # Use | syntax
        except (ValidationError, ValueError, TypeError) as e:
            pytest.fail(f"Validation failed unexpectedly for valid input {statuses_list}: {e}")
    else:
        with pytest.raises((ValidationError, ValueError, TypeError)) as exc_info:
            HyperliquidRawExchangeResponseData.model_validate(data_dict)
        # Check that *some* error occurred. More specific message checks can be added.
        assert exc_info is not None
        print(
            f"Input: {statuses_list}, Expected Failure, Got Error: {exc_info.value}"
        )  # Debug print
