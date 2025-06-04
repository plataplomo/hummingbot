"""Unit Tests for Hyperliquid Raw Subaccounts Model
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_subaccounts import (
    HyperliquidRawSubAccountsResponse,
)

# --- Test Data --- #

VALID_SUBACCOUNTS_RESPONSE: list[str] = [
    "0x1234567890abcdef1234567890abcdef12345678",
    "0xabcdef1234567890abcdef1234567890abcdef12",
]

# --- Test Cases for HyperliquidRawSubAccountsResponse (RootModel) --- #


def test_subaccounts_valid() -> None:
    response = HyperliquidRawSubAccountsResponse.model_validate(VALID_SUBACCOUNTS_RESPONSE)
    assert response.root == VALID_SUBACCOUNTS_RESPONSE


@pytest.mark.parametrize(
    "invalid_list_data",
    [
        "not-a-list",  # Not a list
        [123, "0xvalid"],  # List with non-string item
        [None, "0xvalid"],  # List with None item
        [{"key": "value"}],  # List with dict item
        ["0xshort"],  # Address too short
        ["longaddress" * 10],  # Address too long
        ["no_prefix_address_aaaaaaaaaaaaaaaaaaaaaa"],  # Missing 0x prefix
        [["nested_list"]],  # Nested list, validator expects flat list of strings
    ],
)
def test_subaccounts_invalid_root_list(
    invalid_list_data: str | int | dict[str, Any] | None,
) -> None:
    with pytest.raises(ValidationError):
        HyperliquidRawSubAccountsResponse.model_validate(invalid_list_data)


def test_subaccounts_empty_list_valid() -> None:
    response = HyperliquidRawSubAccountsResponse.model_validate([])
    assert response.root == []


# Since extra='forbid' is typically for BaseModel and RootModel handles it differently
# (validation is on the root type), an "extra field" test is not standard.
# We are primarily testing the structure and content of the root list.
