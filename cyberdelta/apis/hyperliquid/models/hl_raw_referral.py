"""
CyberDeltaEngine: Hyperliquid API Raw Models (Referral Info)
-----------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'referral' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    validate_str_field,
)


class HyperliquidRawReferredBy(BaseModel):
    """Raw boundary model for the 'referredBy' object."""

    referrer: str = Field(..., alias="referrer")
    code: str = Field(..., alias="code")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("referrer", mode="before")
    @classmethod
    def validate_referrer_address(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "referrer"
        s = validate_str_field(v, field_name=field_name, max_length=42)
        if len(s) != 42:
            raise ValueError(f"{field_name}: Expected length 42, got {len(s)}")
        if not s.startswith("0x"):
            raise ValueError(f"{field_name}: Must start with 0x")
        return s

    @field_validator("code", mode="before")
    @classmethod
    def validate_code_str(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="code", max_length=64)


class HyperliquidRawReferralState(BaseModel):
    """Raw boundary model for a single referral state within the referrer data."""

    cum_vlm: str = Field(..., alias="cumVlm")
    cum_rewarded_fees_since_referred: str = Field(..., alias="cumRewardedFeesSinceReferred")
    cum_fees_rewarded_to_referrer: str = Field(..., alias="cumFeesRewardedToReferrer")
    time_joined: int = Field(..., alias="timeJoined")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator(
        "cum_vlm",
        "cum_rewarded_fees_since_referred",
        "cum_fees_rewarded_to_referrer",
        mode="before",
    )
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "decimal_str_field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s

    @field_validator("time_joined", mode="before")
    @classmethod
    def validate_timestamp_ms(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "time_joined"
        if isinstance(v, str):
            try:
                v_int = int(v)
            except ValueError:
                raise ValueError(f"{field_name}: Expected int or int-like string") from None
        elif isinstance(v, int):
            v_int = v
        else:
            raise ValueError(f"{field_name}: Expected int or int-like string")
        if v_int < 0:
            raise ValueError(f"{field_name}: Timestamp cannot be negative")
        return v_int

    @field_validator("user", mode="before")
    @classmethod
    def validate_user_address(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "user"
        s = validate_str_field(v, field_name=field_name, max_length=42)
        if len(s) != 42:
            raise ValueError(f"{field_name}: Expected length 42, got {len(s)}")
        if not s.startswith("0x"):
            raise ValueError(f"{field_name}: Must start with 0x")
        return s


class HyperliquidRawReferrerData(BaseModel):
    """Raw boundary model for the 'data' field within the 'referrerState'."""

    code: str = Field(..., alias="code")
    referral_states: list[HyperliquidRawReferralState] = Field(..., alias="referralStates")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("code", mode="before")
    @classmethod
    def validate_code_str(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="code", max_length=64)

    @field_validator("referral_states", mode="before")
    @classmethod
    def validate_referral_states_list(cls, v: object, info: ValidationInfo) -> list[Any]:
        if not isinstance(v, list):
            raise ValueError("referral_states: Expected list")
        for item_idx, item in enumerate(v):
            if not isinstance(item, dict):
                raise ValueError(f"referral_states[{item_idx}]: Expected dict item")
        return v


class HyperliquidRawReferrerState(BaseModel):
    """Raw boundary model for the 'referrerState' object."""

    stage: str = Field(..., alias="stage")  # e.g., "ready"
    data: HyperliquidRawReferrerData = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("stage", mode="before")
    @classmethod
    def validate_stage_str(cls, v: object, info: ValidationInfo) -> str:
        # Could validate against known stages if necessary
        return validate_str_field(v, field_name="stage", max_length=32)


class HyperliquidRawReferralResponse(BaseModel):
    """
    Raw boundary model for the user referral information response.
    """

    referred_by: HyperliquidRawReferredBy = Field(..., alias="referredBy")
    cum_vlm: str = Field(..., alias="cumVlm")
    unclaimed_rewards: str = Field(..., alias="unclaimedRewards")
    claimed_rewards: str = Field(..., alias="claimedRewards")
    builder_rewards: str = Field(..., alias="builderRewards")
    referrer_state: HyperliquidRawReferrerState = Field(..., alias="referrerState")
    reward_history: list[Any] = Field(
        ..., alias="rewardHistory"
    )  # Empty in example, structure unknown

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator(
        "cum_vlm", "unclaimed_rewards", "claimed_rewards", "builder_rewards", mode="before"
    )
    @classmethod
    def validate_decimal_str_fields(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "decimal_str_field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s

    @field_validator("reward_history", mode="before")
    @classmethod
    def validate_reward_history_list(cls, v: object, info: ValidationInfo) -> list[Any]:
        # Example shows empty list, structure unknown. Basic list validation.
        if not isinstance(v, list):
            raise ValueError("reward_history: Expected list")
        # Could add item validation if structure becomes known
        return v
