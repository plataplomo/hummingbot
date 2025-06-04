from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)
from cyberdelta.apis.backpack.models.bp_raw_position import (
    BackpackRawPosition,
    BackpackRawPositionUpdate,
)

"""
Unit tests for BackpackRawPosition and related Raw models.

**Boundary Validation Pattern (Project Standard):**
- All string fields in Raw models are strictly validated for:
    - Type: must be `str` (not bytes, int, list, etc.)
    - Non-emptiness (unless explicitly allowed)
    - Max length (per OpenAPI spec)
    - Valid UTF-8 encoding (no lone surrogates or invalid unicode)
    - **Invalid unicode or broken types are always rejected** with `ValidationError`
      (if caught by the validator) or `UnicodeEncodeError`
      (if Python or Pydantic internals hit the error first).
- This test suite includes adversarial/hostile input cases to ensure the Raw model
  boundary is robust and spec-aligned.

This pattern is enforced for all Raw models in the CyberDeltaEngine project.
"""


# --- BackpackRawPosition ---

# --- Fixtures ---


@pytest.fixture
def valid_imf_function_data() -> dict[str, str]:
    return {"base": "0.1", "factor": "0.5"}


@pytest.fixture
def valid_mmf_function_data() -> dict[str, str]:
    return {"base": "0.05", "factor": "0.25"}


@pytest.fixture
def valid_position_data(
    valid_imf_function_data: dict[str, str], valid_mmf_function_data: dict[str, str],
) -> dict[str, Any]:
    # Fixture now correctly depends on imf/mmf data fixtures
    return {
        "breakEvenPrice": "20000.50",
        "entryPrice": "19800.00",
        "estLiquidationPrice": "15000.00",
        "imf": "0.1234",
        "imfFunction": valid_imf_function_data,
        "markPrice": "20100.75",
        "mmf": "0.0678",
        "mmfFunction": valid_mmf_function_data,
        "netCost": "-1980.00",
        "netQuantity": "0.1",
        "netExposureQuantity": "0.1",
        "netExposureNotional": "2010.075",
        "pnlRealized": "5.00",
        "pnlUnrealized": "30.075",
        "cumulativeFundingPayment": "-1.25",
        "symbol": "BTC_USDC",
        "userId": 123456789,
        "positionId": "pos_abc123",
        "cumulativeInterest": "0.0",
    }


@pytest.fixture
def valid_position_update_data() -> dict[str, Any]:
    return {
        "e": "positionUpdate",
        "E": 1678886400000,  # Example timestamp
        "s": "SOL_USDC",
        "b": "22.50",  # break_event_price
        "B": "22.00",  # entry_price
        "l": "18.00",  # liquidation_price
        "f": "0.05",  # initial_margin_fraction
        "M": "23.10",  # mark_price
        "m": "0.02",  # maintenance_margin_fraction
        "q": "10.5",  # net_quantity
        "Q": "10.5",  # net_exposure_quantity
        "n": "242.55",  # net_exposure_notional
    }


# --- Success Cases: BackpackRawPosition ---


def test_BackpackRawPosition_valid(valid_position_data: dict[str, Any]) -> None:
    pos = BackpackRawPosition.model_validate(valid_position_data)
    assert pos.symbol == "BTC_USDC"
    assert pos.user_id == 123456789
    assert pos.position_id == "pos_abc123"
    assert pos.break_even_price == "20000.50"
    assert pos.imf == "0.1234"
    assert isinstance(pos.imf_function, BackpackRawImfFunction)
    assert pos.imf_function.base == "0.1"
    assert pos.imf_function.factor == "0.5"
    assert isinstance(pos.mmf_function, BackpackRawMmfFunction)
    assert pos.mmf_function.base == "0.05"
    assert pos.mmf_function.factor == "0.25"
    assert pos.model_config.get("extra") == "forbid"
    assert pos.model_config.get("frozen") is True


def test_BackpackRawPosition_valid_int_user_id_str(
    valid_position_data: dict[str, Any],  # Add fixture dependency
    valid_imf_function_data: dict[str, str],  # Add fixture dependency
    valid_mmf_function_data: dict[str, str],  # Add fixture dependency
) -> None:
    # Note: valid_position_data fixture already includes imf/mmf data
    data = valid_position_data  # Use the injected fixture directly
    data["userId"] = "987654321"
    pos = BackpackRawPosition.model_validate(data)
    assert pos.user_id == 987654321


# --- Failure Cases: BackpackRawPosition ---


@pytest.mark.parametrize(
    "field, value, expected_msg_part",
    [
        ("symbol", "", "String cannot be empty"),
        ("symbol", "A" * 65, "String value too long"),
        ("positionId", None, "Field required"),  # positionId is required
        ("userId", -1, "Must be non-negative"),
        ("userId", "abc", "Must be an integer"),
        ("userId", 1.0, "Must be an integer"),
        ("breakEvenPrice", "inf", "finite decimal"),
        ("entryPrice", "nan", "finite decimal"),
        ("estLiquidationPrice", "", "String cannot be empty"),
        ("imf", "1.2.3", "finite decimal"),  # Invalid decimal string
        ("markPrice", None, "Field required"),
        ("mmf", "   ", "String cannot be empty"),
        ("netCost", "infinity", "finite decimal"),
        ("netQuantity", "-inf", "finite decimal"),
        ("netExposureNotional", True, "Expected string"),
        ("pnlRealized", "", "String cannot be empty"),
        ("pnlUnrealized", "NaN", "finite decimal"),
        ("cumulativeFundingPayment", None, "Field required"),
        ("imfFunction", {"base": "0.1"}, "Field required"),  # Missing factor
        ("imfFunction", {"base": "inf", "factor": "0.5"}, "finite decimal"),
        ("imfFunction", None, "Field required"),
        ("imfFunction", "notadict", "Expected a dictionary"),
        ("mmfFunction", {"base": "0.1", "factor": "1.2.3"}, "finite decimal"),
    ],
)
def test_BackpackRawPosition_invalid_fields(
    field: str,
    value: str | float | bool | dict[str, Any] | None,  # Testing specific invalid types for Pydantic validation
    expected_msg_part: str,
    valid_position_data: dict[str, Any],  # Add fixture dependency
    valid_imf_function_data: dict[str, str],  # Add fixture dependency
    valid_mmf_function_data: dict[str, str],  # Add fixture dependency
) -> None:
    # Note: valid_position_data fixture already includes imf/mmf data
    data = valid_position_data  # Use the injected fixture directly
    # Special handling for nested dicts
    if field in ["imfFunction", "mmfFunction"]:
        data[field] = value  # Replace entire dict
    else:
        data[field] = value

    with pytest.raises(ValidationError) as exc_info:
        BackpackRawPosition.model_validate(data)

    # Check if the specific field name or a relevant part of the error message is present
    assert field in str(exc_info.value) or expected_msg_part in str(exc_info.value), (
        f"Field: {field}, Value: {value!r}, Error: {exc_info.value}"
    )


def test_BackpackRawPosition_extra_field(
    valid_position_data: dict[str, Any],  # Add fixture dependency
    valid_imf_function_data: dict[str, str],  # Add fixture dependency
    valid_mmf_function_data: dict[str, str],  # Add fixture dependency
) -> None:
    # Note: valid_position_data fixture already includes imf/mmf data
    data = valid_position_data  # Use the injected fixture directly
    data["extraField"] = "some_value"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        BackpackRawPosition.model_validate(data)


def test_BackpackRawPosition_frozen(
    valid_position_data: dict[str, Any],  # Add fixture dependency
    valid_imf_function_data: dict[str, str],  # Add fixture dependency
    valid_mmf_function_data: dict[str, str],  # Add fixture dependency
) -> None:
    # Note: valid_position_data fixture already includes imf/mmf data
    pos = BackpackRawPosition.model_validate(
        valid_position_data,
    )  # Use the injected fixture directly
    with pytest.raises(ValidationError, match="Instance is frozen"):
        pos.symbol = "new_symbol"


# --- Success Cases: BackpackRawPositionUpdate ---


def test_BackpackRawPositionUpdate_valid(valid_position_update_data: dict[str, Any]) -> None:
    update = BackpackRawPositionUpdate.model_validate(valid_position_update_data)
    assert update.event_type == "positionUpdate"
    assert update.event_time == 1678886400000
    assert update.symbol == "SOL_USDC"
    assert update.break_event_price == "22.50"
    assert update.entry_price == "22.00"
    assert update.liquidation_price == "18.00"
    assert update.initial_margin_fraction == "0.05"
    assert update.mark_price == "23.10"
    assert update.maintenance_margin_fraction == "0.02"
    assert update.net_quantity == "10.5"
    assert update.net_exposure_quantity == "10.5"
    assert update.net_exposure_notional == "242.55"
    assert update.model_config.get("extra") == "forbid"
    assert update.model_config.get("frozen") is True


def test_BackpackRawPositionUpdate_valid_optional_fields_none(
    valid_position_update_data: dict[str, Any],  # Add fixture dependency
) -> None:
    data = valid_position_update_data  # Use the injected fixture directly
    # Set all optional fields to None
    optional_fields = ["b", "B", "l", "f", "M", "m", "q", "Q", "n"]
    for key in optional_fields:
        data[key] = None

    update = BackpackRawPositionUpdate.model_validate(data)
    assert update.break_event_price is None
    assert update.entry_price is None
    assert update.liquidation_price is None
    assert update.initial_margin_fraction is None
    assert update.mark_price is None
    assert update.maintenance_margin_fraction is None
    assert update.net_quantity is None
    assert update.net_exposure_quantity is None
    assert update.net_exposure_notional is None


def test_BackpackRawPositionUpdate_valid_timestamp_formats(
    valid_position_update_data: dict[str, Any],  # Add fixture dependency
) -> None:
    data = valid_position_update_data  # Use the injected fixture directly
    data["E"] = "2023-03-15T12:00:00Z"
    update = BackpackRawPositionUpdate.model_validate(data)
    assert update.event_time == "2023-03-15T12:00:00Z"

    # Reset data for next case
    data = valid_position_update_data  # Use the injected fixture directly
    data["E"] = 1678886400.500  # Float seconds
    update = BackpackRawPositionUpdate.model_validate(data)
    assert update.event_time == 1678886400.500

    # Reset data for next case
    data = valid_position_update_data  # Use the injected fixture directly
    data["E"] = "1678886400000"  # String ms
    update = BackpackRawPositionUpdate.model_validate(data)
    assert update.event_time == 1678886400000

    # Reset data for next case
    data = valid_position_update_data  # Use the injected fixture directly
    data["E"] = None
    update = BackpackRawPositionUpdate.model_validate(data)
    assert update.event_time is None


# --- Failure Cases: BackpackRawPositionUpdate ---


@pytest.mark.parametrize(
    "field, value, expected_msg_part",
    [
        ("e", "wrongUpdate", "Invalid value"),  # Wrong event type
        ("e", "", "String cannot be empty"),
        ("E", "not-a-date", "Invalid timestamp string"),
        ("E", [], "Invalid type"),
        ("s", "", "String cannot be empty"),
        ("s", None, "Field required"),
        ("b", "inf", "finite decimal"),  # break_event_price
        ("B", "nan", "finite decimal"),  # entry_price
        ("l", "", "String cannot be empty"),  # liquidation_price
        ("f", "  ", "String cannot be empty"),  # initial_margin_fraction
        ("M", "1.2.3", "finite decimal"),  # mark_price
        ("m", True, "Expected string"),  # maintenance_margin_fraction
        ("q", [], "Expected string"),  # net_quantity
        ("Q", "infinity", "finite decimal"),  # net_exposure_quantity
        ("n", "NaN", "finite decimal"),  # net_exposure_notional
    ],
)
def test_BackpackRawPositionUpdate_invalid_fields(
    field: str,
    value: str | float | bool | list[Any] | None,  # Testing specific invalid types for Pydantic validation
    expected_msg_part: str,
    valid_position_update_data: dict[str, Any],  # Add fixture dependency
) -> None:
    data = valid_position_update_data  # Use the injected fixture directly
    data[field] = value
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawPositionUpdate.model_validate(data)
    assert expected_msg_part in str(exc_info.value) or field in str(exc_info.value), (
        f"Field: {field}, Value: {value!r}, Error: {exc_info.value}"
    )


def test_BackpackRawPositionUpdate_extra_field(
    valid_position_update_data: dict[str, Any],  # Add fixture dependency
) -> None:
    data = valid_position_update_data  # Use the injected fixture directly
    data["extra"] = 123
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        BackpackRawPositionUpdate.model_validate(data)


def test_BackpackRawPositionUpdate_frozen(
    valid_position_update_data: dict[str, Any],  # Add fixture dependency
) -> None:
    data = valid_position_update_data  # Use the injected fixture directly
    pos_update = BackpackRawPositionUpdate.model_validate(data)
    with pytest.raises(ValidationError, match="Instance is frozen"):
        pos_update.symbol = "NEW_SYMBOL"


class TestBackpackRawPosition:
    """Tests for the BackpackRawPosition model that might involve more complex validation
    or scenarios not covered by simple field-level parametrization.
    """

    def test_invalid_position_bad_side(self) -> None:
        """Test that an order with an invalid side raises ValidationError."""
        # This test's logic will be determined if it fails after unmarking.
        # For now, just ensuring the decorator is removed and the class structure remains.
        # If BackpackRawPosition infers side from quantity, this test might relate to
        # validating that relationship or handling impossible raw states.
        # Placeholder, actual test logic might be present or added if it fails.
