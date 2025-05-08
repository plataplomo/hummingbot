"""
Unit Tests for Hyperliquid Raw Staking Info Models
"""

from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

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
def valid_history_delta_data(valid_history_delegate_delta_data: dict[str, Any]) -> dict[str, Any]:
    data = VALID_HISTORY_DELTA.copy()
    data["delegate"] = valid_history_delegate_delta_data
    return data


@pytest.fixture
def valid_history_item_data(valid_history_delta_data: dict[str, Any]) -> dict[str, Any]:
    data = VALID_HISTORY_ITEM.copy()
    data["delta"] = valid_history_delta_data
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
    valid_delegation_item_data: dict[str, Any], field: str, val: object
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
def test_delegations_response_invalid(data: object) -> None:
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
    valid_delegator_summary_data: dict[str, Any], field: str, val: object | None
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
class TestHyperliquidRawDelegatorHistoryDelta:
    def test_hist_delta_valid(self, valid_history_delegate_delta_data: dict[str, Any]) -> None:
        data = {"delegate": valid_history_delegate_delta_data}
        delta = HyperliquidRawDelegatorHistoryDelta.model_validate(data)
        assert delta.delegate is not None
        assert delta.delegate.validator == valid_history_delegate_delta_data["validator"]


# HyperliquidRawDelegatorHistoryItem
class TestHyperliquidRawDelegatorHistoryItem:
    """Test suite for HyperliquidRawDelegatorHistoryItem."""

    def test_hist_item_valid(self, valid_history_item_data: dict[str, Any]) -> None:
        item = HyperliquidRawDelegatorHistoryItem.model_validate(valid_history_item_data)
        assert item.time == valid_history_item_data["time"]
        assert item.hash == valid_history_item_data["hash"]
        assert isinstance(item.delta, HyperliquidRawDelegatorHistoryDelta)

    @pytest.mark.parametrize(
        "field,val",
        [
            ("time", "not-a-timestamp"),
            ("time", -1),
            ("hash", None),  # Test missing required field if not caught by type annotation
            ("hash", "short"),  # Fails 0x prefix and length
            ("hash", "0x" + "1" * 63),  # Not 66 chars
            ("hash", "1" * 66),  # No 0x prefix
            ("delta", None),
            ("delta", "not-a-dict"),
        ],
    )
    def test_hist_item_invalid(
        self, valid_history_item_data: dict[str, Any], field: str, val: object
    ) -> None:
        d = valid_history_item_data.copy()
        if field == "delta" and isinstance(d.get("delta"), dict):
            d["delta"] = d["delta"].copy()
            if isinstance(d["delta"].get("delegate"), dict):
                d["delta"]["delegate"] = d["delta"]["delegate"].copy()

        if val is None:
            if field in d:
                del d[field]
        else:
            d[field] = val

        with pytest.raises(ValidationError):
            HyperliquidRawDelegatorHistoryItem.model_validate(d)


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


# Extra Fields Tests
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
    model_class: type[BaseModel],
    valid_data_fixture_name: str,
    request: pytest.FixtureRequest,
) -> None:
    """Test that extra fields are rejected due to extra='forbid'."""
    valid_data: dict[str, Any] = request.getfixturevalue(valid_data_fixture_name)
    data_with_extra = valid_data.copy()
    data_with_extra["extra_field"] = "some_value"
    with pytest.raises(ValidationError) as exc_info:
        model_class.model_validate(data_with_extra)
    assert "Extra inputs are not permitted" in str(exc_info.value)
