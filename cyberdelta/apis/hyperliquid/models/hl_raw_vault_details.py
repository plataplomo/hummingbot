"""CyberDeltaEngine: Hyperliquid API Raw Models (Vault Details).

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

from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawDefaultString,
    RawFiniteDecimalStr,
    RawLaxEthereumAddressStrHL,
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawStrictBool,
    RawTimestampMsInt,
)
from cyberdelta.exceptions.field_validation import ListFieldError


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

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawVaultRelationship(BaseModel):
    """Raw boundary model for the vault relationship structure."""

    type: RawDefaultString = Field(..., alias="type", max_length=32)
    data: HyperliquidRawVaultRelationshipData = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawVaultDetailsResponse(BaseModel):
    """Strict boundary model for the Hyperliquid 'vaultDetails' info endpoint response.

    Validates the complete vault details structure including metadata, financial data,
    performance history, user equities, and relationship information from Hyperliquid's
    vault details API endpoint. Enforces strict type and format validation.
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
        ...,
        alias="performanceHistory",
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
        cls,
        v: object,
        info: ValidationInfo,
    ) -> list[dict[str, object]]:
        """Validate that list fields contain properly structured dictionary items.

        Ensures that performance_history and user_equities fields are lists containing
        dictionary objects that can be properly validated by their respective Pydantic
        models. Provides detailed error messages for malformed data structures.

        Args:
            v: Raw value from external API (expected to be a list of dicts)
            info: Pydantic validation context containing field name

        Returns:
            Validated list of dictionary objects ready for model parsing

        Raises:
            ListFieldError: If structure is not a list or contains non-dict items

        """
        field_name = info.field_name or "list_field"
        if not isinstance(v, list):
            raise ListFieldError(field_name=field_name, actual_type=type(v).__name__)

        list_of_objects = cast("list[object]", v)

        validated_items: list[dict[str, object]] = []
        for item_idx, item_obj in enumerate(list_of_objects):
            if not isinstance(item_obj, dict):
                raise ListFieldError(
                    field_name=field_name,
                    actual_type=type(item_obj).__name__,
                    item_index=item_idx,
                    expected_item_type="dict",
                )
            item_dict = cast("dict[str, object]", item_obj)
            validated_items.append(item_dict)
        return validated_items
