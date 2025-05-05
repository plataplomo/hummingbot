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

    filled = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_filled)
    assert filled.resting is None
    assert filled.filled is not None
    assert filled.filled.oid == 67890
    assert filled.error is None

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
    "invalid_data, expected_msg",
    [
        ({"oid": -1}, "Must be non-negative"),
        ({"oid": "abc"}, "Must be an integer"),
        ({}, "Field required"),  # Missing oid
        ({"oid": 123, "extra": 1}, "Extra inputs are not permitted"),  # Extra ignored
    ],
)
def test_hl_resting_invalid(invalid_data: dict[str, Any], expected_msg: str) -> None:
    # Note: extra="ignore" means extra fields don't raise error here
    if "extra" in invalid_data:
        # Just test validation works even with extra
        _ = HyperliquidRawExchangeStatusResting.model_validate(invalid_data)
    else:
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawExchangeStatusResting.model_validate(invalid_data)
        assert expected_msg in str(exc_info.value)


# Filled Model Failures
@pytest.mark.parametrize(
    "invalid_data, expected_msg",
    [
        ({"oid": 67890, "totalSz": "1.5", "avgPx": "inf"}, "finite decimal"),
        ({"oid": 67890, "totalSz": "NaN", "avgPx": "1.0"}, "finite decimal"),
        ({"oid": 67890, "totalSz": "", "avgPx": "1.0"}, "String cannot be empty"),
        ({"oid": -1, "totalSz": "1", "avgPx": "1"}, "Must be non-negative"),
        ({"oid": 67890}, "Field required"),  # missing totalSz, avgPx
    ],
)
def test_hl_filled_invalid(invalid_data: dict[str, Any], expected_msg: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeStatusFilled.model_validate(invalid_data)
    assert expected_msg in str(exc_info.value)


# Status Object Failures
@pytest.mark.parametrize(
    "invalid_data, expected_msg",
    [
        ({"resting": {"oid": -1}}, "Must be non-negative"),  # Nested validation
        ({"filled": {"oid": 1, "totalSz": "", "avgPx": "1"}}, "String cannot be empty"),
        ({"error": ""}, "String cannot be empty"),  # Optional but non-empty if present
        ({"error": 123}, "Expected string"),
        ({"unknown": 1}, "Extra inputs are not permitted"),  # Ignored
    ],
)
def test_hl_status_object_invalid(invalid_data: dict[str, Any], expected_msg: str) -> None:
    if "unknown" in invalid_data:
        _ = HyperliquidRawExchangeStatusObject.model_validate(invalid_data)
    else:
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawExchangeStatusObject.model_validate(invalid_data)
        assert expected_msg in str(exc_info.value)


# Response Data Failures
@pytest.mark.parametrize(
    "invalid_data, expected_msg",
    [
        ({"type": "", "statuses": []}, "String cannot be empty"),
        ({"type": 123, "statuses": []}, "Expected string"),
        (
            {"type": "order", "statuses": [1, 2]},
            "statuses[0]: Invalid type int. Expected str/dict.",
        ),
        ({"type": "order", "statuses": ["invalid_status"]}, "Invalid status string"),
        ({"type": "order", "statuses": [{"resting": {"oid": -1}}]}, "Must be non-negative"),
        ({"type": "order"}, "Field required"),  # Missing statuses
        (
            {"type": "order", "statuses": [], "extra": 1},
            "Extra inputs are not permitted",
        ),  # Ignored
    ],
)
def test_hl_response_data_invalid(invalid_data: dict[str, Any], expected_msg: str) -> None:
    if "extra" in invalid_data:
        _ = HyperliquidRawExchangeResponseData.model_validate(invalid_data)
    else:
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawExchangeResponseData.model_validate(invalid_data)
        assert expected_msg in str(exc_info.value)


# Top Level Response Failures
@pytest.mark.parametrize(
    "invalid_data, expected_msg",
    [
        ({"status": "error", "data": None}, "Invalid value 'error'. Expected one of {'ok'}"),
        ({"status": 123, "data": None}, "Expected string"),
        ({}, "Field required"),  # Missing status
        (
            {"status": "ok", "data": {"type": "order", "statuses": [1]}},
            "statuses[0]: Invalid type int. Expected str/dict.",
        ),  # Nested
        ({"status": "ok", "extra_field": 1}, "Extra inputs are not permitted"),  # extra='forbid'
    ],
)
def test_hl_response_invalid(invalid_data: dict[str, Any], expected_msg: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawExchangeResponse.model_validate(invalid_data)
    assert expected_msg in str(exc_info.value)


# Ensure no invalid tags remain at the end of the file
