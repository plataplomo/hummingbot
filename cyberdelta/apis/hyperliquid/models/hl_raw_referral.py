"""
CyberDeltaEngine: Hyperliquid API Raw Models (Referral Info)
-----------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'referral' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from typing import Annotated, cast

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawEthereumAddressStr,
    RawFiniteDecimalStr,
    RawTimestampMsInt,
    # RawDefaultString might be used or specific Annotated as decided
)
from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawReferredBy(BaseModel):
    """Raw boundary model for the 'referredBy' object."""

    referrer: RawEthereumAddressStr = Field(..., alias="referrer")
    code: Annotated[
        str,
        BeforeValidator(lambda v: validate_str_field(v, "code", max_length=64, allow_empty=False)),
    ] = Field(..., alias="code")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawReferralState(BaseModel):
    """Raw boundary model for a single referral state within the referrer data."""

    cum_vlm: RawFiniteDecimalStr = Field(..., alias="cumVlm")
    cum_rewarded_fees_since_referred: RawFiniteDecimalStr = Field(
        ..., alias="cumRewardedFeesSinceReferred"
    )
    cum_fees_rewarded_to_referrer: RawFiniteDecimalStr = Field(
        ..., alias="cumFeesRewardedToReferrer"
    )
    time_joined: RawTimestampMsInt = Field(..., alias="timeJoined")
    user: RawEthereumAddressStr = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawReferrerData(BaseModel):
    """Raw boundary model for the 'data' field within the 'referrerState'."""

    code: Annotated[
        str,
        BeforeValidator(lambda v: validate_str_field(v, "code", max_length=64, allow_empty=False)),
    ] = Field(..., alias="code")
    referral_states: list[HyperliquidRawReferralState] = Field(..., alias="referralStates")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("referral_states", mode="before")
    @classmethod
    def validate_referral_states_list(cls, v: object, info: ValidationInfo) -> list[object]:
        if not isinstance(v, list):
            raise ValueError("referral_states: Expected list")
        # Pydantic will validate each item in the list against HyperliquidRawReferralState.
        # The old check for `isinstance(item, dict)` is thus handled by Pydantic's parsing.
        return cast(list[object], v)


class HyperliquidRawReferrerState(BaseModel):
    """Raw boundary model for the 'referrerState' object."""

    stage: Annotated[
        str,
        BeforeValidator(lambda v: validate_str_field(v, "stage", max_length=32, allow_empty=False)),
    ] = Field(..., alias="stage")  # e.g., "ready"
    data: HyperliquidRawReferrerData = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawReferralResponse(BaseModel):
    """
    Raw boundary model for the user referral information response.
    """

    referred_by: HyperliquidRawReferredBy = Field(..., alias="referredBy")
    cum_vlm: RawFiniteDecimalStr = Field(..., alias="cumVlm")
    unclaimed_rewards: RawFiniteDecimalStr = Field(..., alias="unclaimedRewards")
    claimed_rewards: RawFiniteDecimalStr = Field(..., alias="claimedRewards")
    builder_rewards: RawFiniteDecimalStr = Field(..., alias="builderRewards")
    referrer_state: HyperliquidRawReferrerState = Field(..., alias="referrerState")
    reward_history: list[object] = Field(
        ..., alias="rewardHistory"
    )  # Changed Any to object, structure unknown

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("reward_history", mode="before")
    @classmethod
    def validate_reward_history_list(cls, v: object, info: ValidationInfo) -> list[object]:
        # Example shows empty list, structure unknown. Basic list validation.
        if not isinstance(v, list):
            raise ValueError("reward_history: Expected list")
        # Could add item validation if structure becomes known
        return cast(list[object], v)
