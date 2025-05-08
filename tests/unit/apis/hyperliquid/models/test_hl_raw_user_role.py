"""
Unit Tests for Hyperliquid Raw User Role Models
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_user_role import (
    HyperliquidRawUserRoleData,
    HyperliquidRawUserRoleResponse,
)

# --- Test Data --- #

VALID_USER_ROLE_USER: dict[str, Any] = {"role": "user"}
VALID_USER_ROLE_AGENT: dict[str, Any] = {
    "role": "agent",
    "data": {"user": "0xagentuseraddress1234567890abcdef123456"},
}
VALID_USER_ROLE_VAULT: dict[str, Any] = {"role": "vault"}
VALID_USER_ROLE_SUBACCOUNT: dict[str, Any] = {
    "role": "subAccount",
    "data": {"master": "0xmasteraddress1234567890abcdef12345678"},
}
VALID_USER_ROLE_MISSING: dict[str, Any] = {"role": "missing"}

VALID_ROLE_DATA_AGENT: dict[str, str | None] = {"user": "0xagentuseraddress1234567890abcdef123456"}
VALID_ROLE_DATA_SUBACCOUNT: dict[str, str | None] = {
    "master": "0xmasteraddress1234567890abcdef12345678"
}

# --- Fixtures --- #


@pytest.fixture(
    params=[
        VALID_USER_ROLE_USER,
        VALID_USER_ROLE_AGENT,
        VALID_USER_ROLE_VAULT,
        VALID_USER_ROLE_SUBACCOUNT,
        VALID_USER_ROLE_MISSING,
    ]
)
def valid_user_role_data(request: Any) -> dict[str, Any]:
    return request.param.copy()  # Ensure individual test data is copied


@pytest.fixture(
    params=[
        VALID_ROLE_DATA_AGENT,
        VALID_ROLE_DATA_SUBACCOUNT,
        {"user": None, "master": None},  # Both None
        {},  # Empty data
    ]
)
def valid_role_data_params(request: Any) -> dict[str, str | None]:
    return request.param.copy()


# --- Test Cases for HyperliquidRawUserRoleData --- #


def test_role_data_valid(valid_role_data_params: dict[str, str | None]) -> None:
    data = HyperliquidRawUserRoleData.model_validate(valid_role_data_params)
    if "user" in valid_role_data_params:
        assert data.user == valid_role_data_params["user"]
    if "master" in valid_role_data_params:
        assert data.master == valid_role_data_params["master"]


@pytest.mark.parametrize(
    "field, value",
    [
        ("user", "not-an-address"),
        ("user", "0xshort"),
        ("master", 123),  # Invalid type
    ],
)
def test_role_data_invalid_address(field: str, value: Any) -> None:
    data_payload = {field: value}
    with pytest.raises(ValidationError):
        HyperliquidRawUserRoleData.model_validate(data_payload)


def test_role_data_allows_extra_fields() -> None:
    # extra='allow' is set on HyperliquidRawUserRoleData
    data_payload = {"user": "0xagentuseraddress1234567890abcdef123456", "extra": "allowed"}
    role_data = HyperliquidRawUserRoleData.model_validate(data_payload)
    assert role_data.user == data_payload["user"]
    # Extra field does not cause validation error and is accessible via model_extra
    assert role_data.model_extra is not None
    assert role_data.model_extra["extra"] == "allowed"


# --- Test Cases for HyperliquidRawUserRoleResponse --- #


def test_user_role_response_valid(valid_user_role_data: dict[str, Any]) -> None:
    response = HyperliquidRawUserRoleResponse.model_validate(valid_user_role_data)
    assert response.role == valid_user_role_data["role"]
    if "data" in valid_user_role_data and valid_user_role_data["data"] is not None:
        assert response.data is not None
        original_data = valid_user_role_data["data"]
        if "user" in original_data:
            assert response.data.user == original_data["user"]
        if "master" in original_data:
            assert response.data.master == original_data["master"]
    else:
        assert response.data is None


@pytest.mark.parametrize(
    "field, value, is_missing_test",
    [
        ("role", None, True),  # Required
        ("role", "unknownRole", False),  # Invalid enum value
        ("data", {"user": "invalid-address-format"}, False),  # Invalid nested data
        ("data", "not-a-dict", False),  # Invalid type for data
    ],
)
def test_user_role_response_invalid(
    valid_user_role_data: dict[str, Any], field: str, value: Any, is_missing_test: bool
) -> None:
    # Use a copy of one of the valid scenarios for manipulation
    data_copy = VALID_USER_ROLE_AGENT.copy()
    if "data" in data_copy and data_copy["data"] is not None:  # ensure data is dict for copy
        data_copy["data"] = data_copy["data"].copy()

    if is_missing_test:
        if field in data_copy:
            del data_copy[field]
    else:
        data_copy[field] = value

    with pytest.raises(ValidationError):
        HyperliquidRawUserRoleResponse.model_validate(data_copy)


def test_user_role_response_extra_field() -> None:
    data_copy = VALID_USER_ROLE_USER.copy()
    data_copy["extraField"] = "value"
    with pytest.raises(ValidationError):
        HyperliquidRawUserRoleResponse.model_validate(data_copy)


def test_user_role_data_none_valid() -> None:
    # Test when 'data' is explicitly None or missing, which is valid
    response_with_none_data = HyperliquidRawUserRoleResponse.model_validate(
        {"role": "user", "data": None}
    )
    assert response_with_none_data.data is None

    response_without_data = HyperliquidRawUserRoleResponse.model_validate({"role": "vault"})
    assert response_without_data.data is None
