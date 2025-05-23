"""
Unit Tests for Hyperliquid Raw Builder Fee Approval Model
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_builder_fee import (
    HyperliquidRawBuilderFeeApprovalResponse,
)

# --- Test Data --- #

VALID_BUILDER_FEE_RESPONSE: dict[str, Any] = {"approved": True}

# --- Fixtures --- #


@pytest.fixture
def valid_builder_fee_data() -> dict[str, Any]:
    return VALID_BUILDER_FEE_RESPONSE.copy()


# --- Test Cases for HyperliquidRawBuilderFeeApprovalResponse --- #


def test_builder_fee_valid(valid_builder_fee_data: dict[str, Any]) -> None:
    response = HyperliquidRawBuilderFeeApprovalResponse.model_validate(valid_builder_fee_data)
    assert response.approved == valid_builder_fee_data["approved"]


@pytest.mark.parametrize(
    "value",
    [
        "not-a-bool",
        123,  # not a bool
        "True",  # String true (should be handled by validator)
        "False",  # String false (should be handled by validator)
        "true",  # lowercase
        "false",  # lowercase
    ],
)
def test_builder_fee_approved_various_inputs(value: str | bool | int | float) -> None:
    data = {"approved": value}
    if isinstance(value, str) and value.lower() in ["true", "false"]:
        expected_bool = value.lower() == "true"
        response = HyperliquidRawBuilderFeeApprovalResponse.model_validate(data)
        assert response.approved == expected_bool
    else:
        with pytest.raises(ValidationError):
            HyperliquidRawBuilderFeeApprovalResponse.model_validate(data)


def test_builder_fee_missing_approved_field() -> None:
    with pytest.raises(ValidationError, match="Field required"):
        HyperliquidRawBuilderFeeApprovalResponse.model_validate({})


def test_builder_fee_extra_field(valid_builder_fee_data: dict[str, Any]) -> None:
    data_copy = valid_builder_fee_data.copy()
    data_copy["extra"] = "field"
    with pytest.raises(ValidationError):
        HyperliquidRawBuilderFeeApprovalResponse.model_validate(data_copy)
