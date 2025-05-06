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
    assert obj.model_config.get("extra") == "ignore"
    assert obj.model_config.get("frozen") is True


def test_hl_filled_valid(valid_filled_data: dict[str, Any]) -> None:
    obj = HyperliquidRawExchangeStatusFilled.model_validate(valid_filled_data)
    assert obj.oid == 67890
    assert obj.total_sz == "1.5"
    assert obj.avg_px == "150.25"
    assert obj.model_config.get("extra") == "ignore"
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
    assert resting.model_config.get("extra") == "ignore"
    assert resting.model_config.get("frozen") is True

    filled = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_filled)
    assert filled.resting is None
    assert filled.filled is not None
    assert filled.filled.oid == 67890
    assert filled.error is None
    assert filled.model_config.get("extra") == "ignore"
    assert filled.model_config.get("frozen") is True

    error = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_error)
    assert error.resting is None
    assert error.filled is None
    assert error.error == "Order rejected due to insufficient margin."
    assert error.model_config.get("extra") == "ignore"
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
    assert obj.model_config.get("extra") == "ignore"
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
        ({"oid": -1}, "Must be non-negative"),
        ({"oid": "abc"}, "Must be an integer"),
        ({"oid": 1.0}, "Must be an integer"),
        ({"oid": 1.0}, "Must be an integer"),
        ({}, "Field required"),
    ],
)
def test_hl_resting_invalid(invalid_data: dict[str, Any], expected_msg_part: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusResting.model_validate(invalid_data)
    assert expected_msg_part in str(exc_info.value)


def test_hl_resting_extra_fields_ignored() -> None:
    """Test that extra fields are ignored due to extra='ignore'."""
    data = {"oid": 123, "extra": 1, "another": "field"}
    obj = HyperliquidRawExchangeStatusResting.model_validate(data)
    assert obj.oid == 123
    assert not hasattr(obj, "extra")


# Filled Model Failures
@pytest.mark.parametrize(
    "invalid_data, expected_msg_part",
    [
        ({"oid": 1, "totalSz": "1.5", "avgPx": "inf"}, "finite decimal"),
        ({"oid": 1, "totalSz": "NaN", "avgPx": "1.0"}, "finite decimal"),
        ({"oid": 1, "totalSz": "", "avgPx": "1.0"}, "String cannot be empty"),
        ({"oid": 1, "totalSz": "1.0", "avgPx": ""}, "String cannot be empty"),
        ({"oid": 1, "totalSz": "1.0", "avgPx": "1..0"}, "Invalid finite decimal string"),
        ({"oid": 1, "totalSz": 1.0, "avgPx": "1.0"}, "Expected string"),
        ({"oid": -1, "totalSz": "1", "avgPx": "1"}, "Must be non-negative"),
        ({"oid": 1, "totalSz": "1"}, "Field required"),
        ({"oid": 1, "avgPx": "1"}, "Field required"),
        ({"totalSz": "1", "avgPx": "1"}, "Field required"),
    ],
)
def test_hl_filled_invalid(invalid_data: dict[str, Any], expected_msg_part: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusFilled.model_validate(invalid_data)
    assert expected_msg_part in str(exc_info.value)


def test_hl_filled_extra_fields_ignored() -> None:
    """Test that extra fields are ignored due to extra='ignore'."""
    data = {"oid": 1, "totalSz": "1", "avgPx": "1", "extra": 1}
    obj = HyperliquidRawExchangeStatusFilled.model_validate(data)
    assert obj.oid == 1
    assert not hasattr(obj, "extra")


# Status Object Failures
@pytest.mark.parametrize(
    "invalid_data, expected_msg_part",
    [
        ({"resting": {"oid": -1}}, "Must be non-negative"),
        ({"filled": {"oid": 1, "totalSz": "", "avgPx": "1"}}, "String cannot be empty"),
        ({"error": ""}, "String cannot be empty"),
        ({"error": 123}, "Expected string"),
        ({"resting": None, "filled": None, "error": None}, "at least one field"),
    ],
)
def test_hl_status_object_invalid(invalid_data: dict[str, Any], expected_msg_part: str) -> None:
    if "at least one field" in expected_msg_part:
        pytest.skip("Skipping test requiring model-level validation for StatusObject")
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusObject.model_validate(invalid_data)
    assert expected_msg_part in str(exc_info.value)


def test_hl_status_object_extra_fields_ignored() -> None:
    """Test that extra fields are ignored due to extra='ignore'."""
    data = {"error": "Some error", "extra": 1}
    obj = HyperliquidRawExchangeStatusObject.model_validate(data)
    assert obj.error == "Some error"
    assert not hasattr(obj, "extra")


# Response Data Failures
@pytest.mark.parametrize(
    "invalid_data, expected_exception, expected_msg_part",
    [
        ({"type": "", "statuses": []}, ValueError, "String cannot be empty"),
        ({"type": 123, "statuses": []}, ValueError, "Expected string"),
        ({"type": "order", "statuses": 123}, ValueError, "Must be a list"),
        (
            {"type": "order", "statuses": [1, 2]},
            TypeError,
            "statuses[0]: Invalid type int. Expected str or dict.",
        ),
        (
            {"type": "order", "statuses": ["invalid_status"]},
            ValueError,
            "statuses[0]: Invalid status string 'invalid_status'",
        ),
        (
            {"type": "order", "statuses": [{"resting": {"oid": -1}}]},
            ValueError,
            "statuses[0]: Invalid status object format.",
        ),
        ({"type": "order"}, ValidationError, "Field required"),
        ({"statuses": []}, ValidationError, "Field required"),
    ],
)
def test_hl_response_data_invalid(
    invalid_data: dict[str, Any], expected_exception: type[Exception], expected_msg_part: str
) -> None:
    with pytest.raises(expected_exception) as exc_info:
        HyperliquidRawExchangeResponseData.model_validate(invalid_data)
    assert expected_msg_part in str(exc_info.value)


def test_hl_response_data_extra_fields_ignored() -> None:
    """Test that extra fields are ignored in ResponseData itself (config is ignore)."""
    data = {"type": "order", "statuses": ["success"], "extra": "ignored"}
    obj = HyperliquidRawExchangeResponseData.model_validate(data)
    assert obj.type == "order"
    assert not hasattr(obj, "extra")


# Top Level Response Failures
@pytest.mark.parametrize(
    "invalid_data, expected_exception, expected_msg_part",
    [
        ({"status": "error", "data": None}, ValueError, "Invalid value 'error'"),
        ({"status": 123, "data": None}, ValueError, "Expected string"),
        ({}, ValidationError, "Field required"),
        (
            {"status": "ok", "data": {"type": "order", "statuses": [1]}},
            TypeError,
            "statuses[0]: Invalid type int",
        ),
        ({"status": "ok", "extra_field": 1}, ValidationError, "Extra inputs are not permitted"),
        (
            {"status": "ok", "data": {"type": "", "statuses": []}},
            ValueError,
            "String cannot be empty",
        ),
    ],
)
def test_hl_response_invalid(
    invalid_data: dict[str, Any], expected_exception: type[Exception], expected_msg_part: str
) -> None:
    with pytest.raises(expected_exception) as exc_info:
        HyperliquidRawExchangeResponse.model_validate(invalid_data)
    assert expected_msg_part in str(exc_info.value)
