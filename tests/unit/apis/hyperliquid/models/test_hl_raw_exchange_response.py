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

# --- Test Data Factories ---


def valid_resting_data() -> dict[str, Any]:
    return {"oid": 12345}


def valid_filled_data() -> dict[str, Any]:
    return {"oid": 67890, "totalSz": "10.5", "avgPx": "150.25"}


def valid_status_object_resting() -> dict[str, Any]:
    return {"resting": valid_resting_data()}


def valid_status_object_filled() -> dict[str, Any]:
    return {"filled": valid_filled_data()}


def valid_status_object_error() -> dict[str, Any]:
    return {"error": "Order expired"}


# Define the expected type for the statuses list items
# StatusItemType = Literal["canceled", "modified", "success"] | dict[str, Any]


# Relaxing type hint here as tests intentionally pass invalid types
def valid_response_data(statuses: list[Any] | None = None) -> dict[str, Any]:
    if statuses is None:
        statuses = [
            valid_status_object_resting(),
            "canceled",
            valid_status_object_filled(),
            "success",
        ]
    return {"type": "order", "statuses": statuses}


def valid_response(data: dict[str, Any] | None = None) -> dict[str, Any]:
    if data is None:
        data = valid_response_data()
    return {"status": "ok", "data": data}


# --- Test Cases: HyperliquidRawExchangeStatusResting ---


def test_resting_happy_path() -> None:
    data = valid_resting_data()
    obj = HyperliquidRawExchangeStatusResting.model_validate(data)
    assert obj.oid == 12345


def test_resting_invalid_oid() -> None:
    with pytest.raises(ValidationError):  # Negative OID
        HyperliquidRawExchangeStatusResting.model_validate({"oid": -1})
    with pytest.raises(ValidationError):  # Wrong type
        HyperliquidRawExchangeStatusResting.model_validate({"oid": "abc"})


def test_resting_extra_field_ignored() -> None:
    data = valid_resting_data()
    data["extra"] = "ignored"
    obj = HyperliquidRawExchangeStatusResting.model_validate(data)
    assert not hasattr(obj, "extra")


def test_resting_frozen() -> None:
    obj = HyperliquidRawExchangeStatusResting.model_validate(valid_resting_data())
    with pytest.raises(ValidationError):
        obj.oid = 999  # type: ignore


# --- Test Cases: HyperliquidRawExchangeStatusFilled ---


def test_filled_happy_path() -> None:
    data = valid_filled_data()
    obj = HyperliquidRawExchangeStatusFilled.model_validate(data)
    assert obj.oid == 67890
    assert obj.total_sz == "10.5"
    assert obj.avg_px == "150.25"


def test_filled_invalid_fields() -> None:
    # Invalid OID
    data = valid_filled_data()
    data["oid"] = -1
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusFilled.model_validate(data)
    # Invalid totalSz
    data = valid_filled_data()
    data["totalSz"] = "NaN"
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusFilled.model_validate(data)
    data = valid_filled_data()
    data["totalSz"] = ""
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusFilled.model_validate(data)
    # Invalid avgPx
    data = valid_filled_data()
    data["avgPx"] = "inf"
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusFilled.model_validate(data)
    data = valid_filled_data()
    data["avgPx"] = 150.25  # Wrong type
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusFilled.model_validate(data)


def test_filled_extra_field_ignored() -> None:
    data = valid_filled_data()
    data["extra"] = "ignored"
    obj = HyperliquidRawExchangeStatusFilled.model_validate(data)
    assert not hasattr(obj, "extra")


def test_filled_frozen() -> None:
    obj = HyperliquidRawExchangeStatusFilled.model_validate(valid_filled_data())
    with pytest.raises(ValidationError):
        obj.oid = 999  # type: ignore
    with pytest.raises(ValidationError):
        obj.avg_px = "200.0"  # type: ignore


# --- Test Cases: HyperliquidRawExchangeStatusObject ---


def test_status_object_happy_paths() -> None:
    obj_rest = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_resting())
    assert isinstance(obj_rest.resting, HyperliquidRawExchangeStatusResting)
    assert obj_rest.resting.oid == 12345
    assert obj_rest.filled is None
    assert obj_rest.error is None

    obj_fill = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_filled())
    assert isinstance(obj_fill.filled, HyperliquidRawExchangeStatusFilled)
    assert obj_fill.filled.oid == 67890
    assert obj_fill.resting is None
    assert obj_fill.error is None

    obj_err = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_error())
    assert obj_err.error == "Order expired"
    assert obj_err.resting is None
    assert obj_err.filled is None


def test_status_object_invalid_nested() -> None:
    # Invalid resting oid
    data = {"resting": {"oid": -1}}
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusObject.model_validate(data)
    # Invalid filled price
    data = {"filled": {"oid": 1, "totalSz": "1", "avgPx": "NaN"}}
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusObject.model_validate(data)
    # Invalid error string
    data = {"error": ""}  # Empty string invalid
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusObject.model_validate(data)
    data = {"error": "E" * 1025}  # Too long
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusObject.model_validate(data)


def test_status_object_extra_ignored() -> None:
    data = valid_status_object_resting()
    data["extra"] = 1
    obj = HyperliquidRawExchangeStatusObject.model_validate(data)
    assert not hasattr(obj, "extra")


def test_status_object_frozen() -> None:
    obj = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_error())
    with pytest.raises(ValidationError):
        obj.error = "New error"  # type: ignore


# --- Test Cases: HyperliquidRawExchangeResponseData ---


def test_response_data_happy_path() -> None:
    data = valid_response_data()
    obj = HyperliquidRawExchangeResponseData.model_validate(data)
    assert obj.type == "order"
    assert len(obj.statuses) == 4
    assert isinstance(obj.statuses[0], HyperliquidRawExchangeStatusObject)
    assert obj.statuses[0].resting.oid == 12345  # type: ignore
    assert obj.statuses[1] == "canceled"
    assert isinstance(obj.statuses[2], HyperliquidRawExchangeStatusObject)
    assert obj.statuses[2].filled.oid == 67890  # type: ignore
    assert obj.statuses[3] == "success"


def test_response_data_invalid_fields() -> None:
    # Invalid type
    data = valid_response_data()
    data["type"] = ""  # Empty invalid
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponseData.model_validate(data)
    data = valid_response_data()
    data["type"] = None  # Missing required
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponseData.model_validate(data)

    # Invalid statuses list itself
    data = valid_response_data()
    data["statuses"] = None  # Wrong type
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponseData.model_validate(data)
    data = valid_response_data()
    data["statuses"] = "not_a_list"
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponseData.model_validate(data)


def test_response_data_invalid_statuses_items() -> None:
    # Invalid string in list
    data = valid_response_data(statuses=["invalid_status"])
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponseData.model_validate(data)
    # Invalid object in list (bad nested structure)
    data = valid_response_data(statuses=[{"resting": {"oid": -5}}])
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponseData.model_validate(data)
    # Invalid type in list
    data = valid_response_data(statuses=[123])
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponseData.model_validate(data)
    # Mixed valid and invalid
    data = valid_response_data(statuses=["success", {"resting": {"oid": -5}}, "canceled"])
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponseData.model_validate(data)


def test_response_data_extra_ignored() -> None:
    data = valid_response_data()
    data["extra"] = 1
    obj = HyperliquidRawExchangeResponseData.model_validate(data)
    assert not hasattr(obj, "extra")


def test_response_data_frozen() -> None:
    obj = HyperliquidRawExchangeResponseData.model_validate(valid_response_data())
    with pytest.raises(ValidationError):
        obj.type = "new_type"  # type: ignore
    with pytest.raises(ValidationError):
        obj.statuses.append("another_status")  # type: ignore


# --- Test Cases: HyperliquidRawExchangeResponse ---


def test_response_happy_path() -> None:
    data = valid_response()
    obj = HyperliquidRawExchangeResponse.model_validate(data)
    assert obj.status == "ok"
    assert isinstance(obj.data, HyperliquidRawExchangeResponseData)
    assert obj.data.type == "order"
    assert len(obj.data.statuses) == 4


def test_response_data_optional() -> None:
    # Test case where data might be legitimately None or missing
    data = {"status": "ok"}
    obj = HyperliquidRawExchangeResponse.model_validate(data)
    assert obj.status == "ok"
    assert obj.data is None

    data = {"status": "ok", "data": None}
    obj = HyperliquidRawExchangeResponse.model_validate(data)
    assert obj.status == "ok"
    assert obj.data is None


def test_response_invalid_status() -> None:
    data = valid_response()
    data["status"] = "error"
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponse.model_validate(data)
    data = valid_response()
    del data["status"]
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponse.model_validate(data)


def test_response_invalid_data() -> None:
    # Data field contains completely wrong structure
    data = {"status": "ok", "data": "not_an_object"}
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponse.model_validate(data)
    # Data field contains object with invalid internal structure
    data = {"status": "ok", "data": {"type": "order", "statuses": [123]}}  # invalid status item
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponse.model_validate(data)


def test_response_extra_forbidden() -> None:
    data = valid_response()
    data["extra"] = "forbidden"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawExchangeResponse.model_validate(data)


def test_response_frozen() -> None:
    obj = HyperliquidRawExchangeResponse.model_validate(valid_response())
    with pytest.raises(ValidationError):
        obj.status = "ok"  # type: ignore # Even assigning same value should fail
    with pytest.raises(ValidationError):
        obj.data = None  # type: ignore


# Ensure no invalid tags remain at the end of the file
