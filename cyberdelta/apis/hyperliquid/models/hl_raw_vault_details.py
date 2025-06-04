"""CyberDeltaEngine: Hyperliquid API Raw Models (Vault Details)
-----------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'vaultDetails' info endpoint.
Validates the raw structure only, enforcing type and format constraints.
Never use for internal business logic.
"""

from typing import cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawDefaultString,
    RawFiniteDecimalStr,
    RawLaxEthereumAddressStrHL,
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawStrictBool,
    RawTimestampMsInt,
)


class HyperliquidRawVaultPerformanceHistoryItem(BaseModel):
    """Raw boundary model for a single performance history entry."""

    time: RawTimestampMsInt = Field(..., alias="time")
    pnl: RawFiniteDecimalStr = Field(..., alias="pnl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawVaultUserEquity(BaseModel):
    """Raw boundary model for a user's equity details within a vault."""

    user: RawLaxEthereumAddressStrHL = Field(..., alias="user")
    equity: RawFiniteDecimalStr = Field(..., alias="equity")
    all_time_pnl: RawFiniteDecimalStr = Field(..., alias="allTimePnl")
    days_following: RawNonNegativeInt = Field(..., alias="daysFollowing")
    vault_entry_time: RawTimestampMsInt = Field(..., alias="vaultEntryTime")
    lockup_until: RawTimestampMsInt = Field(..., alias="lockupUntil")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawVaultRelationshipData(BaseModel):
    """Raw boundary model for the 'data' field within the 'relationship' structure."""

    child_addresses: list[RawLaxEthereumAddressStrHL] | None = Field(None, alias="childAddresses")
    master: RawLaxEthereumAddressStrHL | None = Field(None, alias="master")

    model_config = ConfigDict(populate_by_name=True, extra="allow", frozen=True)


class HyperliquidRawVaultRelationship(BaseModel):
    """Raw boundary model for the vault relationship structure."""

    type: RawDefaultString = Field(..., alias="type", max_length=32)
    data: HyperliquidRawVaultRelationshipData = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawVaultDetailsResponse(BaseModel):
    """Strict boundary model for the Hyperliquid 'vaultDetails' info endpoint response.
    Validates the raw structure only.
    """

    name: RawDefaultString = Field(..., alias="name", max_length=1024)
    description: RawDefaultString = Field(..., alias="description", max_length=1024)
    allow_deposits: RawStrictBool = Field(..., alias="allowDeposits")
    always_close_on_withdraw: RawStrictBool = Field(..., alias="alwaysCloseOnWithdraw")
    creator: RawLaxEthereumAddressStrHL = Field(..., alias="creator")
    vault_address: RawLaxEthereumAddressStrHL = Field(..., alias="vaultAddress")
    max_balance: RawFiniteDecimalStr | None = Field(None, alias="maxBalance")
    curr_balance: RawFiniteDecimalStr = Field(..., alias="currBalance")
    total_pnl: RawFiniteDecimalStr = Field(..., alias="totalPnl")
    all_time_pnl: RawFiniteDecimalStr = Field(..., alias="allTimePnl")
    performance_history: list[HyperliquidRawVaultPerformanceHistoryItem] = Field(
        ..., alias="performanceHistory",
    )
    user_equities: list[HyperliquidRawVaultUserEquity] = Field(..., alias="userEquities")
    max_distributable: RawNonNegativeFiniteDecimalStr = Field(..., alias="maxDistributable")
    max_withdrawable: RawNonNegativeFiniteDecimalStr = Field(..., alias="maxWithdrawable")
    is_closed: RawStrictBool = Field(..., alias="isClosed")
    relationship: HyperliquidRawVaultRelationship = Field(..., alias="relationship")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("performance_history", "user_equities", mode="before")
    @classmethod
    def validate_model_list_structure(
        cls, v: object, info: ValidationInfo,
    ) -> list[dict[str, object]]:
        field_name = info.field_name or "list_field"
        if not isinstance(v, list):
            raise ValueError(f"{field_name}: Expected list, got {type(v).__name__}")

        list_of_objects = cast(list[object], v)
        assert isinstance(list_of_objects, list)

        validated_items: list[dict[str, object]] = []
        for item_idx, item_obj in enumerate(list_of_objects):
            if not isinstance(item_obj, dict):
                raise ValueError(
                    f"{field_name}[{item_idx}]: Expected dict item, got {type(item_obj).__name__}",
                )
            item_dict = cast(dict[str, object], item_obj)
            assert isinstance(item_dict, dict)
            validated_items.append(item_dict)
        return validated_items
