"""
Unit Tests for Hyperliquid Raw Vault Details Models
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_vault_details import (
    HyperliquidRawVaultDetailsResponse,
    HyperliquidRawVaultPerformanceHistoryItem,
    HyperliquidRawVaultRelationship,
    HyperliquidRawVaultRelationshipData,
    HyperliquidRawVaultUserEquity,
)

# --- Test Data --- #

VALID_PERFORMANCE_HISTORY_ITEM: dict[str, Any] = {"time": 1700926145201, "pnl": "123.45"}
VALID_USER_EQUITY_ITEM: dict[str, Any] = {
    "user": "0x1234567890abcdef1234567890abcdef12345678",
    "equity": "10000.50",
    "allTimePnl": "500.25",
    "daysFollowing": 10,
    "vaultEntryTime": 1690000000000,
    "lockupUntil": 1750000000000,
}
VALID_RELATIONSHIP_DATA_PARENT: dict[str, list[str]] = {"childAddresses": ["0xchild1", "0xchild2"]}
VALID_RELATIONSHIP: dict[str, Any] = {"type": "parent", "data": VALID_RELATIONSHIP_DATA_PARENT}

VALID_VAULT_DETAILS_RESPONSE: dict[str, Any] = {
    "name": "Test Vault",
    "description": "A vault for testing",
    "allowDeposits": True,
    "alwaysCloseOnWithdraw": False,
    "creator": "0xcreatoraddress1234567890abcdef1234567890",
    "vaultAddress": "0xvaultaddress1234567890abcdef1234567890",
    "maxBalance": "1000000.00",
    "currBalance": "500000.00",
    "totalPnl": "50000.00",
    "allTimePnl": "75000.00",
    "performanceHistory": [VALID_PERFORMANCE_HISTORY_ITEM],
    "userEquities": [VALID_USER_EQUITY_ITEM],
    "maxDistributable": "10000.00",
    "maxWithdrawable": "5000.00",
    "isClosed": False,
    "relationship": VALID_RELATIONSHIP,
}

# --- Fixtures --- #


@pytest.fixture
def valid_perf_history_data() -> dict[str, Any]:
    return VALID_PERFORMANCE_HISTORY_ITEM.copy()


@pytest.fixture
def valid_user_equity_data() -> dict[str, Any]:
    return VALID_USER_EQUITY_ITEM.copy()


@pytest.fixture
def valid_relationship_data() -> dict[str, list[str]]:
    return VALID_RELATIONSHIP_DATA_PARENT.copy()


@pytest.fixture
def valid_relationship() -> dict[str, Any]:
    data = VALID_RELATIONSHIP["data"].copy()
    return {"type": VALID_RELATIONSHIP["type"], "data": data}


@pytest.fixture
def valid_vault_details_data() -> dict[str, Any]:
    data = VALID_VAULT_DETAILS_RESPONSE.copy()
    data["performanceHistory"] = [
        ph.copy() for ph in VALID_VAULT_DETAILS_RESPONSE["performanceHistory"]
    ]
    data["userEquities"] = [ue.copy() for ue in VALID_VAULT_DETAILS_RESPONSE["userEquities"]]
    rel_copy = VALID_VAULT_DETAILS_RESPONSE["relationship"].copy()
    rel_copy["data"] = VALID_VAULT_DETAILS_RESPONSE["relationship"]["data"].copy()
    data["relationship"] = rel_copy
    return data


# --- Test Cases --- #


# HyperliquidRawVaultPerformanceHistoryItem Tests
def test_perf_history_item_valid(valid_perf_history_data: dict[str, Any]) -> None:
    item = HyperliquidRawVaultPerformanceHistoryItem.model_validate(valid_perf_history_data)
    assert item.time == valid_perf_history_data["time"]
    assert item.pnl == valid_perf_history_data["pnl"]


@pytest.mark.parametrize(
    "field, value",
    [
        ("time", -1),
        ("time", "not-an-int"),
        ("pnl", "not-a-decimal"),
        ("pnl", "Infinity"),
        ("pnl", None),
    ],
)
def test_perf_history_item_invalid_fields(
    valid_perf_history_data: dict[str, Any], field: str, value: Any
) -> None:
    data_copy = valid_perf_history_data.copy()
    if value is None:
        if field in data_copy:
            del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawVaultPerformanceHistoryItem.model_validate(data_copy)


def test_perf_history_item_extra_field(valid_perf_history_data: dict[str, Any]) -> None:
    data_copy = valid_perf_history_data.copy()
    data_copy["extra"] = "field"
    with pytest.raises(ValidationError):
        HyperliquidRawVaultPerformanceHistoryItem.model_validate(data_copy)


# HyperliquidRawVaultUserEquity Tests
def test_user_equity_valid(valid_user_equity_data: dict[str, Any]) -> None:
    item = HyperliquidRawVaultUserEquity.model_validate(valid_user_equity_data)
    assert item.user == valid_user_equity_data["user"]
    assert item.equity == valid_user_equity_data["equity"]
    assert item.lockup_until == valid_user_equity_data["lockupUntil"]


@pytest.mark.parametrize(
    "field, value",
    [
        ("user", "not-an-address"),
        ("user", "0x123"),
        ("equity", "invalid"),
        ("allTimePnl", "NaN"),
        ("daysFollowing", -1),
        ("vaultEntryTime", "abc"),
        ("lockupUntil", None),
    ],
)
def test_user_equity_invalid_fields(
    valid_user_equity_data: dict[str, Any], field: str, value: Any
) -> None:
    data_copy = valid_user_equity_data.copy()
    if value is None:
        if field in data_copy:
            del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawVaultUserEquity.model_validate(data_copy)


def test_user_equity_extra_field(valid_user_equity_data: dict[str, Any]) -> None:
    data_copy = valid_user_equity_data.copy()
    data_copy["extra"] = "data"
    with pytest.raises(ValidationError):
        HyperliquidRawVaultUserEquity.model_validate(data_copy)


# HyperliquidRawVaultRelationshipData Tests
def test_relationship_data_valid(valid_relationship_data: dict[str, list[str]]) -> None:
    data = HyperliquidRawVaultRelationshipData.model_validate(valid_relationship_data)
    assert data.child_addresses == valid_relationship_data["childAddresses"]


@pytest.mark.parametrize(
    "field, value",
    [
        ("childAddresses", ["0xvalid", "invalid-address"]),
        ("childAddresses", "not-a-list"),
        ("master", "not-an-address-format"),
    ],
)
def test_relationship_data_invalid(
    valid_relationship_data: dict[str, list[str]], field: str, value: Any
) -> None:
    data_copy = valid_relationship_data.copy()
    data_copy.pop("master", None)
    data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawVaultRelationshipData.model_validate(data_copy)


# HyperliquidRawVaultRelationship Tests
def test_relationship_valid(valid_relationship: dict[str, Any]) -> None:
    rel = HyperliquidRawVaultRelationship.model_validate(valid_relationship)
    assert rel.type == valid_relationship["type"]
    assert rel.data.child_addresses == valid_relationship["data"]["childAddresses"]


@pytest.mark.parametrize(
    "field, value",
    [
        ("type", None),
        ("data", None),
        ("data", {"childAddresses": ["invalid"]}),
    ],
)
def test_relationship_invalid(valid_relationship: dict[str, Any], field: str, value: Any) -> None:
    data_copy = valid_relationship.copy()
    if value is None:
        if field in data_copy:
            del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawVaultRelationship.model_validate(data_copy)


def test_relationship_extra_field(valid_relationship: dict[str, Any]) -> None:
    data_copy = valid_relationship.copy()
    data_copy["unexpected"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawVaultRelationship.model_validate(data_copy)


# HyperliquidRawVaultDetailsResponse Tests
def test_vault_details_valid(valid_vault_details_data: dict[str, Any]) -> None:
    resp = HyperliquidRawVaultDetailsResponse.model_validate(valid_vault_details_data)
    assert resp.name == valid_vault_details_data["name"]
    assert resp.allow_deposits == valid_vault_details_data["allowDeposits"]
    assert resp.vault_address == valid_vault_details_data["vaultAddress"]
    assert resp.max_balance == valid_vault_details_data["maxBalance"]
    assert resp.curr_balance == valid_vault_details_data["currBalance"]
    assert len(resp.performance_history) == 1
    assert resp.performance_history[0].time == VALID_PERFORMANCE_HISTORY_ITEM["time"]
    assert len(resp.user_equities) == 1
    assert resp.user_equities[0].user == VALID_USER_EQUITY_ITEM["user"]
    assert resp.relationship.type == VALID_RELATIONSHIP["type"]


@pytest.mark.parametrize(
    "field, value, is_missing_test",
    [
        ("name", None, True),
        ("allowDeposits", "not-a-bool", False),
        ("creator", "short", False),
        ("vaultAddress", None, True),
        ("maxBalance", "Infinity", False),
        ("currBalance", "NaN", False),
        ("totalPnl", None, True),
        ("performanceHistory", [{"time": 123}], False),
        ("userEquities", "not-a-list", False),
        ("maxDistributable", [], False),
        ("maxWithdrawable", None, True),
        ("isClosed", 123, False),
        ("relationship", None, True),
    ],
)
def test_vault_details_invalid(
    valid_vault_details_data: dict[str, Any], field: str, value: Any, is_missing_test: bool
) -> None:
    data_copy = valid_vault_details_data.copy()
    if is_missing_test:
        if field in data_copy:
            del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawVaultDetailsResponse.model_validate(data_copy)


def test_vault_details_extra_field(valid_vault_details_data: dict[str, Any]) -> None:
    data_copy = valid_vault_details_data.copy()
    data_copy["surprise"] = "field"
    with pytest.raises(ValidationError):
        HyperliquidRawVaultDetailsResponse.model_validate(data_copy)
