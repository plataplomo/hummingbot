"""
Unit Tests for Hyperliquid Raw Staking Info Models
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_staking import (
    HyperliquidRawDelegationItem,
    HyperliquidRawDelegationsResponse,
    HyperliquidRawDelegatorHistoryDelegateDelta,
    HyperliquidRawDelegatorHistoryDelta,
    HyperliquidRawDelegatorHistoryItem,
    HyperliquidRawDelegatorHistoryResponse,
    HyperliquidRawDelegatorRewardItem,
    HyperliquidRawDelegatorRewardsResponse,
    HyperliquidRawDelegatorSummaryResponse,
)

# --- Test Data --- #

VALID_DELEGATION_ITEM: dict[str, Any] = {
    "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
    "amount": "12060.16529862",
    "lockedUntilTimestamp": 1735466781353,
}
VALID_DELEGATIONS_RESPONSE: list[dict[str, Any]] = [VALID_DELEGATION_ITEM.copy()]

VALID_DELEGATOR_SUMMARY: dict[str, Any] = {
    "delegated": "12060.16529862",
    "undelegated": "0.0",
    "totalPendingWithdrawal": "0.0",
    "nPendingWithdrawals": 0,
}

VALID_HISTORY_DELEGATE_DELTA: dict[str, Any] = {
    "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
    "amount": "10000.0",
    "isUndelegate": False,
}
VALID_HISTORY_DELTA: dict[str, Any] = {"delegate": VALID_HISTORY_DELEGATE_DELTA.copy()}
VALID_HISTORY_ITEM: dict[str, Any] = {
    "time": 1735380381353,
    "hash": "0x55492465cb523f90815a041a226ba90147008d4b221a24ae8dc35a0dbede4ea4",
    "delta": VALID_HISTORY_DELTA.copy(),
}
VALID_HISTORY_RESPONSE: list[dict[str, Any]] = [VALID_HISTORY_ITEM.copy()]

VALID_REWARD_ITEM: dict[str, Any] = {
    "time": 1736726400073,
    "source": "delegation",
    "totalAmount": "0.73117184",
}
VALID_REWARDS_RESPONSE: list[dict[str, Any]] = [VALID_REWARD_ITEM.copy()]

# --- Fixtures --- #


@pytest.fixture
def valid_delegation_item_data() -> dict[str, Any]:
    return VALID_DELEGATION_ITEM.copy()


@pytest.fixture
def valid_delegator_summary_data() -> dict[str, Any]:
    return VALID_DELEGATOR_SUMMARY.copy()


@pytest.fixture
def valid_history_delegate_delta_data() -> dict[str, Any]:
    return VALID_HISTORY_DELEGATE_DELTA.copy()


@pytest.fixture
def valid_history_delta_data() -> dict[str, Any]:
    data = VALID_HISTORY_DELTA.copy()
    data["delegate"] = valid_history_delegate_delta_data()
    return data


@pytest.fixture
def valid_history_item_data() -> dict[str, Any]:
    data = VALID_HISTORY_ITEM.copy()
    data["delta"] = valid_history_delta_data()
    return data


@pytest.fixture
def valid_reward_item_data() -> dict[str, Any]:
    return VALID_REWARD_ITEM.copy()


# --- Test Cases --- #


# HyperliquidRawDelegationItem
def test_delegation_item_valid(valid_delegation_item_data: dict[str, Any]) -> None:
    item = HyperliquidRawDelegationItem.model_validate(valid_delegation_item_data)
    assert item.validator == valid_delegation_item_data["validator"]
    assert item.amount == valid_delegation_item_data["amount"]
    assert item.locked_until_timestamp == valid_delegation_item_data["lockedUntilTimestamp"]


@pytest.mark.parametrize(
    "field,val", [("validator", "short"), ("amount", "nan"), ("lockedUntilTimestamp", -1)]
)
def test_delegation_item_invalid(
    valid_delegation_item_data: dict[str, Any], field: str, val: Any
) -> None:
    d = valid_delegation_item_data.copy()
    d[field] = val
    with pytest.raises(ValidationError):
        HyperliquidRawDelegationItem.model_validate(d)


# HyperliquidRawDelegationsResponse (RootModel)
def test_delegations_response_valid() -> None:
    resp = HyperliquidRawDelegationsResponse.model_validate(VALID_DELEGATIONS_RESPONSE)
    assert len(resp.root) == 1
    assert resp.root[0].validator == VALID_DELEGATION_ITEM["validator"]


@pytest.mark.parametrize("data", ["not-list", [{"validator": "invalid"}]])
def test_delegations_response_invalid(data: Any) -> None:
    with pytest.raises(ValidationError):
        HyperliquidRawDelegationsResponse.model_validate(data)


# HyperliquidRawDelegatorSummaryResponse
def test_delegator_summary_valid(valid_delegator_summary_data: dict[str, Any]) -> None:
    item = HyperliquidRawDelegatorSummaryResponse.model_validate(valid_delegator_summary_data)
    assert item.delegated == valid_delegator_summary_data["delegated"]
    assert item.n_pending_withdrawals == valid_delegator_summary_data["nPendingWithdrawals"]


@pytest.mark.parametrize(
    "field,val", [("delegated", "nan"), ("nPendingWithdrawals", "abc"), ("undelegated", None)]
)
def test_delegator_summary_invalid(
    valid_delegator_summary_data: dict[str, Any], field: str, val: Any | None
) -> None:
    d = valid_delegator_summary_data.copy()
    if val is None:
        del d[field]
    else:
        d[field] = val
    with pytest.raises(ValidationError):
        HyperliquidRawDelegatorSummaryResponse.model_validate(d)


# HyperliquidRawDelegatorHistoryDelegateDelta
def test_hist_delegate_delta_valid(valid_history_delegate_delta_data: dict[str, Any]) -> None:
    item = HyperliquidRawDelegatorHistoryDelegateDelta.model_validate(
        valid_history_delegate_delta_data
    )
    assert item.validator == valid_history_delegate_delta_data["validator"]
    assert item.is_undelegate == valid_history_delegate_delta_data["isUndelegate"]


# HyperliquidRawDelegatorHistoryDelta
def test_hist_delta_valid(valid_history_delta_data: dict[str, Any]) -> None:
    item = HyperliquidRawDelegatorHistoryDelta.model_validate(valid_history_delta_data)
    assert item.delegate is not None
    assert item.delegate.validator == valid_history_delta_data["delegate"]["validator"]


# HyperliquidRawDelegatorHistoryItem
def test_hist_item_valid(valid_history_item_data: dict[str, Any]) -> None:
    item = HyperliquidRawDelegatorHistoryItem.model_validate(valid_history_item_data)
    assert item.hash == valid_history_item_data["hash"]
    assert item.delta.delegate is not None
    assert item.delta.delegate.amount == valid_history_item_data["delta"]["delegate"]["amount"]


# HyperliquidRawDelegatorHistoryResponse (RootModel)
def test_history_response_valid() -> None:
    resp = HyperliquidRawDelegatorHistoryResponse.model_validate(VALID_HISTORY_RESPONSE)
    assert len(resp.root) == 1
    assert resp.root[0].hash == VALID_HISTORY_ITEM["hash"]


# HyperliquidRawDelegatorRewardItem
def test_reward_item_valid(valid_reward_item_data: dict[str, Any]) -> None:
    item = HyperliquidRawDelegatorRewardItem.model_validate(valid_reward_item_data)
    assert item.source == valid_reward_item_data["source"]
    assert item.total_amount == valid_reward_item_data["totalAmount"]


# HyperliquidRawDelegatorRewardsResponse (RootModel)
def test_rewards_response_valid() -> None:
    resp = HyperliquidRawDelegatorRewardsResponse.model_validate(VALID_REWARDS_RESPONSE)
    assert len(resp.root) == 1
    assert resp.root[0].source == VALID_REWARD_ITEM["source"]


# Parametrized test for extra fields across all relevant models
@pytest.mark.parametrize(
    "model_class, valid_data_fixture_name",
    [
        (HyperliquidRawDelegationItem, "valid_delegation_item_data"),
        (HyperliquidRawDelegatorSummaryResponse, "valid_delegator_summary_data"),
        (HyperliquidRawDelegatorHistoryDelegateDelta, "valid_history_delegate_delta_data"),
        (HyperliquidRawDelegatorHistoryDelta, "valid_history_delta_data"),
        (HyperliquidRawDelegatorHistoryItem, "valid_history_item_data"),
        (HyperliquidRawDelegatorRewardItem, "valid_reward_item_data"),
    ],
)
def test_staking_models_extra_fields(
    model_class: Any, valid_data_fixture_name: str, request: Any
) -> None:
    valid_data = request.getfixturevalue(valid_data_fixture_name)
    data_copy = valid_data.copy()
    if isinstance(data_copy.get("delegate"), dict):  # For HyperliquidRawDelegatorHistoryDelta
        data_copy["delegate"] = data_copy["delegate"].copy()
    if isinstance(data_copy.get("delta"), dict):  # For HyperliquidRawDelegatorHistoryItem
        data_copy["delta"] = data_copy["delta"].copy()
        if isinstance(data_copy["delta"].get("delegate"), dict):
            data_copy["delta"]["delegate"] = data_copy["delta"]["delegate"].copy()

    data_copy["extraField"] = "test"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        model_class.model_validate(data_copy)


# Example for one of the more complex invalid cases (e.g. history item)
@pytest.mark.parametrize(
    "field, val",
    [
        ("time", "abc"),
        ("hash", "short"),
        ("delta", None),
        ("delta", {"delegate": {"validator": "invalid"}}),
    ],
)
def test_hist_item_invalid(
    valid_history_item_data: dict[str, Any], field: str, val: Any | None
) -> None:
    d = valid_history_item_data.copy()
    # Deep copy nested dicts if modifying them to avoid test interference
    if (
        field == "delta"
        and isinstance(val, dict)
        and "delegate" in val
        and isinstance(val["delegate"], dict)
    ):
        d["delta"] = {"delegate": val["delegate"].copy()}  # Simplified example
    elif field == "delta" and val is None:
        if field in d:
            del d[field]
    else:
        d[field] = val

    with pytest.raises(ValidationError):
        HyperliquidRawDelegatorHistoryItem.model_validate(d)
