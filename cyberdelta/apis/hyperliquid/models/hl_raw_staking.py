"""CyberDeltaEngine: Hyperliquid API Raw Models (Staking Info).

----------------------------------------------------------

Strict boundary validation models for the Hyperliquid staking-related info endpoints:
- delegations
- delegatorSummary
- delegatorHistory
- delegatorRewards
Validates the raw structure only.
Never use for internal business logic.
"""

from typing import cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawDefaultString,
    RawFiniteDecimalStr,
    RawLaxEthereumAddressStrHL,
    RawNonNegativeInt,
    RawStrictBool,
    RawTimestampMsInt,
    RawTxHashStr,
)


# --- Delegations --- #


class HyperliquidRawDelegationItem(BaseModel):
    """Raw boundary model for a single delegation entry."""

    validator: RawLaxEthereumAddressStrHL = Field(..., alias="validator")
    amount: RawFiniteDecimalStr = Field(..., alias="amount")
    locked_until_timestamp: RawTimestampMsInt = Field(..., alias="lockedUntilTimestamp")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# Response for 'delegations' is RootModel[list[HyperliquidRawDelegationItem]]
class HyperliquidRawDelegationsResponse(RootModel[list[HyperliquidRawDelegationItem]]):
    """Raw boundary model for the 'delegations' list response."""

    root: list[HyperliquidRawDelegationItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_delegations_list(cls, v: object) -> list[dict[str, object]]:
        """Validate that the root input is a list of delegation dictionaries.

        Ensures each delegation item is a dictionary with the expected structure
        for Pydantic to parse into HyperliquidRawDelegationItem objects.

        Args:
            v: Raw input value that should be a list of delegation objects.

        Returns:
            Validated list of delegation dictionaries.

        Raises:
            ValueError: If input is not a list or contains invalid delegation items.

        """
        if not isinstance(v, list):
            raise TypeError("Expected a list of delegations")

        list_of_objects = cast("list[object]", v)

        validated_items: list[dict[str, object]] = []
        for item_idx, item_obj in enumerate(list_of_objects):
            if not isinstance(item_obj, dict):
                raise TypeError(
                    f"Item {item_idx}: Expected dict delegation, got {type(item_obj).__name__}",
                )

            item_dict = cast("dict[str, object]", item_obj)
            validated_items.append(item_dict)
        return validated_items


# --- Delegator Summary --- #


class HyperliquidRawDelegatorSummaryResponse(BaseModel):
    """Raw boundary model for the 'delegatorSummary' response."""

    delegated: RawFiniteDecimalStr = Field(..., alias="delegated")
    undelegated: RawFiniteDecimalStr = Field(..., alias="undelegated")
    total_pending_withdrawal: RawFiniteDecimalStr = Field(..., alias="totalPendingWithdrawal")
    n_pending_withdrawals: RawNonNegativeInt = Field(..., alias="nPendingWithdrawals")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Delegator History --- #


class HyperliquidRawDelegatorHistoryDelegateDelta(BaseModel):
    """Raw boundary model for the 'delegate' details within history delta."""

    validator: RawLaxEthereumAddressStrHL = Field(..., alias="validator")
    amount: RawFiniteDecimalStr = Field(..., alias="amount")
    is_undelegate: RawStrictBool = Field(..., alias="isUndelegate")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawDelegatorHistoryDelta(BaseModel):
    """Raw boundary model for the 'delta' object within history items."""

    delegate: HyperliquidRawDelegatorHistoryDelegateDelta | None = Field(None, alias="delegate")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawDelegatorHistoryItem(BaseModel):
    """Raw boundary model for a single delegator history entry."""

    time: RawTimestampMsInt = Field(..., alias="time")
    hash: RawTxHashStr = Field(..., alias="hash")
    delta: HyperliquidRawDelegatorHistoryDelta = Field(..., alias="delta")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# Response for 'delegatorHistory' is RootModel[list[HyperliquidRawDelegatorHistoryItem]]
class HyperliquidRawDelegatorHistoryResponse(RootModel[list[HyperliquidRawDelegatorHistoryItem]]):
    """Raw boundary model for the 'delegatorHistory' list response."""

    root: list[HyperliquidRawDelegatorHistoryItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_history_list(cls, v: object) -> list[dict[str, object]]:
        """Validate that the root input is a list of delegator history dictionaries.

        Ensures each history item is a dictionary with the expected structure
        for Pydantic to parse into HyperliquidRawDelegatorHistoryItem objects.

        Args:
            v: Raw input value that should be a list of history objects.

        Returns:
            Validated list of history dictionaries.

        Raises:
            ValueError: If input is not a list or contains invalid history items.

        """
        if not isinstance(v, list):
            raise TypeError("Expected a list of history items")

        list_of_objects = cast("list[object]", v)

        validated_items: list[dict[str, object]] = []
        for item_idx, item_obj in enumerate(list_of_objects):
            if not isinstance(item_obj, dict):
                raise TypeError(
                    f"Item {item_idx}: Expected dict history item, got {type(item_obj).__name__}",
                )

            item_dict = cast("dict[str, object]", item_obj)
            validated_items.append(item_dict)
        return validated_items


# --- Delegator Rewards --- #


class HyperliquidRawDelegatorRewardItem(BaseModel):
    """Raw boundary model for a single delegator reward entry."""

    time: RawTimestampMsInt = Field(..., alias="time")
    source: RawDefaultString = Field(..., alias="source")
    total_amount: RawFiniteDecimalStr = Field(..., alias="totalAmount")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# Response for 'delegatorRewards' is RootModel[list[HyperliquidRawDelegatorRewardItem]]
class HyperliquidRawDelegatorRewardsResponse(RootModel[list[HyperliquidRawDelegatorRewardItem]]):
    """Raw boundary model for the 'delegatorRewards' list response."""

    root: list[HyperliquidRawDelegatorRewardItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_rewards_list(cls, v: object) -> list[dict[str, object]]:
        """Validate that the root input is a list of delegator reward dictionaries.

        Ensures each reward item is a dictionary with the expected structure
        for Pydantic to parse into HyperliquidRawDelegatorRewardItem objects.

        Args:
            v: Raw input value that should be a list of reward objects.

        Returns:
            Validated list of reward dictionaries.

        Raises:
            ValueError: If input is not a list or contains invalid reward items.

        """
        if not isinstance(v, list):
            raise TypeError("Expected a list of reward items")

        list_of_objects = cast("list[object]", v)

        validated_items: list[dict[str, object]] = []
        for item_idx, item_obj in enumerate(list_of_objects):
            if not isinstance(item_obj, dict):
                raise TypeError(
                    f"Item {item_idx}: Expected dict reward item, got {type(item_obj).__name__}",
                )

            item_dict = cast("dict[str, object]", item_obj)
            validated_items.append(item_dict)
        return validated_items
