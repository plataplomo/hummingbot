"""Unit Tests for Hyperliquid Raw User Vault Equity Models."""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_user_vault_equity import (
    HyperliquidRawUserVaultEquityItem,
)

# --- Test Data --- #

VALID_USER_VAULT_EQUITY_ITEM: dict[str, Any] = {
    "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
    "equity": "742500.082809",
}

VALID_USER_VAULT_EQUITIES_RESPONSE: list[dict[str, Any]] = [
    VALID_USER_VAULT_EQUITY_ITEM,
    {
        "vaultAddress": "0xanotheraddress1234567890abcdef1234567890",
        "equity": "1000.00",
    },
]

# --- Fixtures --- #


@pytest.fixture
def valid_user_vault_equity_item_data() -> dict[str, Any]:
    """Return valid user vault equity item data for testing."""
    return VALID_USER_VAULT_EQUITY_ITEM.copy()


# --- Test Cases for HyperliquidRawUserVaultEquityItem --- #


def test_user_vault_equity_item_valid(valid_user_vault_equity_item_data: dict[str, Any]) -> None:
    """Test user vault equity item valid."""
    item = HyperliquidRawUserVaultEquityItem.model_validate(valid_user_vault_equity_item_data)
    assert item.vault_address == valid_user_vault_equity_item_data["vaultAddress"]
    assert item.equity == valid_user_vault_equity_item_data["equity"]


@pytest.mark.parametrize(
    "field, value, is_missing_test",
    [
        ("vaultAddress", None, True),  # Required
        ("vaultAddress", "not-an-address", False),
        ("vaultAddress", "0xshort", False),
        ("equity", None, True),  # Required
        ("equity", "not-a-decimal", False),
        ("equity", "Infinity", False),
    ],
)
def test_user_vault_equity_item_invalid_fields(
    valid_user_vault_equity_item_data: dict[str, Any],
    field: str,
    value: object,
    is_missing_test: bool,
) -> None:
    """Test user vault equity item invalid fields."""
    data_copy = valid_user_vault_equity_item_data.copy()
    if is_missing_test:
        if field in data_copy:
            del data_copy[field]
    else:
        data_copy[field] = value

    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate(data_copy)


def test_user_vault_equity_item_extra_field(
    valid_user_vault_equity_item_data: dict[str, Any],
) -> None:
    """Test user vault equity item extra field."""
    data_copy = valid_user_vault_equity_item_data.copy()
    data_copy["extraField"] = "someValue"
    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate(data_copy)


# --- Tests for lists of items (as typical response) --- #


def test_user_vault_equities_list_valid() -> None:
    """Test user vault equities list valid."""
    # The model is for the item, so we test validating a list of such items
    validated_items = [
        HyperliquidRawUserVaultEquityItem.model_validate(item_data)
        for item_data in VALID_USER_VAULT_EQUITIES_RESPONSE
    ]
    assert len(validated_items) == 2
    assert validated_items[0].vault_address == VALID_USER_VAULT_EQUITIES_RESPONSE[0]["vaultAddress"]


def test_user_vault_equities_list_with_invalid_item() -> None:
    """Test user vault equities list with invalid item."""
    invalid_item_data = VALID_USER_VAULT_EQUITY_ITEM.copy()
    invalid_item_data["equity"] = "not-a-decimal"
    list_with_invalid = [VALID_USER_VAULT_EQUITY_ITEM.copy(), invalid_item_data]

    with pytest.raises(ValidationError):
        [
            HyperliquidRawUserVaultEquityItem.model_validate(item_data)
            for item_data in list_with_invalid
        ]


def test_user_vault_equities_response_not_a_list() -> None:
    """Test user vault equities response not a list."""
    # If the expected response is a list, passing non-list to where list is expected
    # would typically be caught before item-wise Pydantic validation.
    # This tests the item model if it were incorrectly passed a non-dict.
    with pytest.raises(ValidationError):  # Pydantic expects a mapping for BaseModel
        HyperliquidRawUserVaultEquityItem.model_validate("not_a_dict_or_list")
