"""CyberDeltaEngine: Hyperliquid API Raw Models (Referral Info).

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

from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawLaxEthereumAddressStrHL,
    RawNonNegativeFiniteDecimalStr,
    # RawDefaultString might be used or specific Annotated as decided
    RawTimestampMsInt,
)
from cyberdelta.exceptions.field_validation import ListFieldError
from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawReferredBy(BaseModel):
    """Raw boundary model for the 'referredBy' object."""

    referrer: RawLaxEthereumAddressStrHL = Field(..., alias="referrer")
    code: Annotated[
        str,
        BeforeValidator(lambda v: validate_str_field(v, "code", max_length=64, allow_empty=False)),
    ] = Field(..., alias="code")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawReferralState(BaseModel):
    """Raw boundary model for a single referral state within the referrer data."""

    cum_vlm: RawNonNegativeFiniteDecimalStr = Field(..., alias="cumVlm")
    cum_rewarded_fees_since_referred: RawNonNegativeFiniteDecimalStr = Field(
        ...,
        alias="cumRewardedFeesSinceReferred",
    )
    cum_fees_rewarded_to_referrer: RawNonNegativeFiniteDecimalStr = Field(
        ...,
        alias="cumFeesRewardedToReferrer",
    )
    time_joined: RawTimestampMsInt = Field(..., alias="timeJoined")
    user: RawLaxEthereumAddressStrHL = Field(..., alias="user")
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
        """Validate that referral_states field is a list structure.

        Ensures the input is a list before Pydantic validates each item against
        HyperliquidRawReferralState. This provides early validation of the container
        structure while delegating item validation to Pydantic's parsing.

        Args:
            v: The raw input value to validate
            info: Pydantic validation context information

        Returns:
            list[object]: The validated list for further Pydantic processing

        Raises:
            ValueError: If the input is not a list structure

        """
        if not isinstance(v, list):
            raise ListFieldError(field_name="referral_states", actual_type=type(v).__name__)
        # Pydantic will validate each item in the list against HyperliquidRawReferralState.
        # The old check for `isinstance(item, dict)` is thus handled by Pydantic's parsing.
        return cast("list[object]", v)


class HyperliquidRawReferrerState(BaseModel):
    """Raw boundary model for the 'referrerState' object."""

    stage: Annotated[
        str,
        BeforeValidator(lambda v: validate_str_field(v, "stage", max_length=32, allow_empty=False)),
    ] = Field(..., alias="stage")  # e.g., "ready"
    data: HyperliquidRawReferrerData = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawReferralResponse(BaseModel):
    """Raw boundary model for the user referral information response."""

    referred_by: HyperliquidRawReferredBy = Field(..., alias="referredBy")
    cum_vlm: RawNonNegativeFiniteDecimalStr = Field(..., alias="cumVlm")
    unclaimed_rewards: RawNonNegativeFiniteDecimalStr = Field(..., alias="unclaimedRewards")
    claimed_rewards: RawNonNegativeFiniteDecimalStr = Field(..., alias="claimedRewards")
    builder_rewards: RawNonNegativeFiniteDecimalStr = Field(..., alias="builderRewards")
    referrer_state: HyperliquidRawReferrerState = Field(..., alias="referrerState")
    reward_history: list[object] = Field(
        ...,
        alias="rewardHistory",
    )  # Changed Any to object, structure unknown

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("reward_history", mode="before")
    @classmethod
    def validate_reward_history_list(cls, v: object, info: ValidationInfo) -> list[object]:
        """Validate that reward_history field is a list structure.

        Performs basic list validation for the reward_history field. The exact structure
        of reward history items is currently unknown from API documentation, so this
        validator only ensures the container is a list.

        Args:
            v: The raw input value to validate
            info: Pydantic validation context information

        Returns:
            list[object]: The validated list for further processing

        Raises:
            ValueError: If the input is not a list structure

        Note:
            Item validation could be added if the reward history structure becomes known.

        """
        # Example shows empty list, structure unknown. Basic list validation.
        if not isinstance(v, list):
            raise ListFieldError(field_name="reward_history", actual_type=type(v).__name__)
        # Could add item validation if structure becomes known
        return cast("list[object]", v)
