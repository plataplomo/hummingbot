"""
Unit Tests for Hyperliquid Raw Referral Info Models
"""

from typing import Any

import pytest
from pydantic import BaseModel, ValidationError
from pytest import FixtureRequest

from cyberdelta.apis.hyperliquid.models.hl_raw_referral import (
    HyperliquidRawReferralResponse,
    HyperliquidRawReferralState,  # This is the item in referrer_state.data.referral_states
    HyperliquidRawReferredBy,
    HyperliquidRawReferrerData,
    HyperliquidRawReferrerState,
)

# --- Test Data --- #

VALID_REFERRED_BY: dict[str, str] = {
    "referrer": "0x5ac99df645f3414876c816caa18b2d234024b487",
    "code": "TESTNET",
}

VALID_REFERRAL_STATE_ITEM: dict[str, Any] = {
    "cumVlm": "960652.017122",
    "cumRewardedFeesSinceReferred": "196.838825",
    "cumFeesRewardedToReferrer": "19.683748",
    "timeJoined": 1679425029416,
    "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
}

VALID_REFERRER_DATA: dict[str, Any] = {
    "code": "TEST",
    "referralStates": [VALID_REFERRAL_STATE_ITEM.copy()],
}

VALID_REFERRER_STATE: dict[str, Any] = {"stage": "ready", "data": VALID_REFERRER_DATA.copy()}

VALID_REFERRAL_RESPONSE: dict[str, Any] = {
    "referredBy": VALID_REFERRED_BY.copy(),
    "cumVlm": "149428030.6628420055",
    "unclaimedRewards": "11.047361",
    "claimedRewards": "22.743781",
    "builderRewards": "0.027802",
    "referrerState": VALID_REFERRER_STATE.copy(),
    "rewardHistory": [],  # Example shows empty list
}

# --- Fixtures --- #


@pytest.fixture
def valid_referred_by_data() -> dict[str, str]:
    return VALID_REFERRED_BY.copy()


@pytest.fixture
def valid_referral_state_item_data() -> dict[str, Any]:
    return VALID_REFERRAL_STATE_ITEM.copy()


@pytest.fixture
def valid_referrer_data_data(valid_referral_state_item_data: dict[str, Any]) -> dict[str, Any]:
    data = VALID_REFERRER_DATA.copy()
    data["referralStates"] = [valid_referral_state_item_data]
    return data


@pytest.fixture
def valid_referrer_state_data(valid_referrer_data_data: dict[str, Any]) -> dict[str, Any]:
    data = VALID_REFERRER_STATE.copy()
    data["data"] = valid_referrer_data_data
    return data


@pytest.fixture
def valid_referral_response_data(
    valid_referred_by_data: dict[str, Any], valid_referrer_state_data: dict[str, Any]
) -> dict[str, Any]:
    data = VALID_REFERRAL_RESPONSE.copy()
    data["referredBy"] = valid_referred_by_data
    data["referrerState"] = valid_referrer_state_data
    data["rewardHistory"] = list(VALID_REFERRAL_RESPONSE["rewardHistory"])
    return data


# --- Test Cases --- #


# HyperliquidRawReferredBy
def test_referred_by_valid(valid_referred_by_data: dict[str, str]) -> None:
    item = HyperliquidRawReferredBy.model_validate(valid_referred_by_data)
    assert item.referrer == valid_referred_by_data["referrer"]
    assert item.code == valid_referred_by_data["code"]


@pytest.mark.parametrize(
    "field, value", [("referrer", "invalid"), ("code", None), ("referrer", "0x123")]
)
def test_referred_by_invalid(
    valid_referred_by_data: dict[str, str], field: str, value: str | None
) -> None:
    data_copy = valid_referred_by_data.copy()
    if value is None:
        del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawReferredBy.model_validate(data_copy)


# HyperliquidRawReferralState (item)
def test_referral_state_item_valid(valid_referral_state_item_data: dict[str, Any]) -> None:
    item = HyperliquidRawReferralState.model_validate(valid_referral_state_item_data)
    assert item.cum_vlm == valid_referral_state_item_data["cumVlm"]
    assert item.user == valid_referral_state_item_data["user"]


@pytest.mark.parametrize(
    "field, value",
    [
        ("cumVlm", "nan"),
        ("timeJoined", "abc"),
        ("user", "short"),
        ("cumRewardedFeesSinceReferred", None),
    ],
)
def test_referral_state_item_invalid(
    valid_referral_state_item_data: dict[str, Any],
    field: str,
    value: str
    | int
    | float
    | bool
    | list[Any]
    | dict[str, Any]
    | None,  # Testing specific invalid types for Pydantic validation
) -> None:
    data_copy = valid_referral_state_item_data.copy()
    if value is None:
        del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawReferralState.model_validate(data_copy)


# HyperliquidRawReferrerData
def test_referrer_data_valid(valid_referrer_data_data: dict[str, Any]) -> None:
    item = HyperliquidRawReferrerData.model_validate(valid_referrer_data_data)
    assert item.code == valid_referrer_data_data["code"]
    assert len(item.referral_states) == len(valid_referrer_data_data["referralStates"])


@pytest.mark.parametrize(
    "field, value",
    [("code", None), ("referralStates", "not-a-list"), ("referralStates", [{"user": "invalid"}])],
)
def test_referrer_data_invalid(
    valid_referrer_data_data: dict[str, Any],
    field: str,
    value: str
    | int
    | float
    | bool
    | list[Any]
    | dict[str, Any]
    | None,  # Testing specific invalid types for Pydantic validation
) -> None:
    data_copy = valid_referrer_data_data.copy()
    if value is None:
        del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawReferrerData.model_validate(data_copy)


# HyperliquidRawReferrerState
def test_referrer_state_valid(valid_referrer_state_data: dict[str, Any]) -> None:
    item = HyperliquidRawReferrerState.model_validate(valid_referrer_state_data)
    assert item.stage == valid_referrer_state_data["stage"]
    assert item.data.code == valid_referrer_state_data["data"]["code"]


@pytest.mark.parametrize(
    "field, value",
    [
        ("stage", None),
        ("data", "not-a-dict"),
        ("data", {"code": "c", "referralStates": ["invalid"]}),
    ],
)
def test_referrer_state_invalid(
    valid_referrer_state_data: dict[str, Any],
    field: str,
    value: str
    | int
    | float
    | bool
    | list[Any]
    | dict[str, Any]
    | None,  # Testing specific invalid types for Pydantic validation
) -> None:
    data_copy = valid_referrer_state_data.copy()
    if value is None:
        del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawReferrerState.model_validate(data_copy)


# HyperliquidRawReferralResponse
def test_referral_response_valid(valid_referral_response_data: dict[str, Any]) -> None:
    item = HyperliquidRawReferralResponse.model_validate(valid_referral_response_data)
    assert item.cum_vlm == valid_referral_response_data["cumVlm"]
    assert item.referred_by.code == valid_referral_response_data["referredBy"]["code"]
    assert item.referrer_state.stage == valid_referral_response_data["referrerState"]["stage"]
    assert item.reward_history == valid_referral_response_data["rewardHistory"]


@pytest.mark.parametrize(
    "field, value, is_missing_test",
    [
        ("referredBy", None, True),
        ("cumVlm", "inf", False),
        ("unclaimedRewards", None, True),
        ("rewardHistory", "not-list", False),
        (
            "referrerState",
            {"stage": "s", "data": {"code": "c", "referralStates": ["invalid"]}},
            False,
        ),
    ],
)
def test_referral_response_invalid(
    valid_referral_response_data: dict[str, Any],
    field: str,
    value: str
    | int
    | float
    | bool
    | list[Any]
    | dict[str, Any]
    | None,  # Testing specific invalid types for Pydantic validation
    is_missing_test: bool,
) -> None:
    data_copy = valid_referral_response_data.copy()
    if is_missing_test:
        if field in data_copy:
            del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawReferralResponse.model_validate(data_copy)


@pytest.mark.parametrize(
    "model_class, valid_data_fixture_name",
    [
        (HyperliquidRawReferredBy, "valid_referred_by_data"),
        (HyperliquidRawReferralState, "valid_referral_state_item_data"),
        (HyperliquidRawReferrerData, "valid_referrer_data_data"),
        (HyperliquidRawReferrerState, "valid_referrer_state_data"),
        (HyperliquidRawReferralResponse, "valid_referral_response_data"),
    ],
)
def test_all_referral_models_extra_fields(
    model_class: type[BaseModel],
    valid_data_fixture_name: str,
    request: FixtureRequest,
) -> None:
    """Test that all referral-related models forbid extra fields."""
    valid_data = request.getfixturevalue(valid_data_fixture_name)
    data_copy = valid_data.copy()
    # For nested models, need to ensure the correct part is copied if mutable
    if isinstance(data_copy.get("data"), dict):
        data_copy["data"] = data_copy["data"].copy()
    if isinstance(data_copy.get("referralStates"), list):
        data_copy["referralStates"] = [rs.copy() for rs in data_copy["referralStates"]]
    if isinstance(data_copy.get("referredBy"), dict):
        data_copy["referredBy"] = data_copy["referredBy"].copy()
    if isinstance(data_copy.get("referrerState"), dict):
        data_copy["referrerState"] = data_copy["referrerState"].copy()
        if isinstance(data_copy["referrerState"].get("data"), dict):
            data_copy["referrerState"]["data"] = data_copy["referrerState"]["data"].copy()

    data_copy["extraField"] = "test"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        model_class.model_validate(data_copy)
