"""CyberDeltaEngine: Backpack API Raw Models (User Fills).
-------------------------------------------------------

Strict Pydantic models for validating the *raw* structure of Backpack Exchange API responses
related to user fills (trades) from the `/wapi/v1/history/fills` endpoint.
Adheres to the Raw Model Policy:
- Validates external contract for individual fill records.
- Validates raw data types and basic formats (non-empty, length, finite numeric, specific enums).
- Uses `model_config(extra="forbid", frozen=True)`.
- Field validators operate on raw input and return validated raw types or raise errors.
- Contains NO business logic.
"""

from typing import overload

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
)

from cyberdelta.apis.backpack.models.bp_common_raw_types import (
    RawBpFillFeeString,
    RawBpFillPriceString,
    RawBpFillQuantityString,
    RawBpIsoTimestampString,
    RawBpNonEmptyStringMax32,
    RawBpNonEmptyStringMax128,
    RawBpNonNegativeInt,
    RawBpOptionalNonEmptyStringMax128,
    RawBpOrderSideString,
    RawBpStrictBool,
)


# --- Core Backpack Fill Model ---
class BackpackRawFill(BaseModel):
    """Pydantic model for a raw fill object from Backpack API responses.

    This model enforces strict validation of the raw data structure and types
    as defined by the Backpack Exchange API for fill events/objects.
    It uses `Annotated` types from `bp_common_raw_types.py` for consistent
    field-level validation logic.

    Attributes:
        fee (str): The fee amount as a string, validated to be parsable to a finite decimal.
        fee_symbol (str): The symbol of the asset in which the fee was paid (max_length=32).
        is_maker (bool): Whether the order was a maker order.
        order_id (str): The ID of the order that was filled (max_length=128).
        price (str): The price at which the fill occurred, as a string, validated to be
                     parsable to a finite decimal.
        quantity (str): The quantity filled, as a string, validated to be parsable to a
                        finite decimal.
        side (str): The side of the order ('Bid' or 'Ask').
        symbol (str): The trading symbol (e.g., 'SOL_USDC', max_length=32).
        timestamp (str): The ISO 8601 timestamp of the fill.
        trade_id (int): The unique ID of the trade.
        client_id (str | None): Optional client-specified order ID (max_length=128).

    """

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    # Fields are defined using aliases to match the raw API response keys.
    fee: RawBpFillFeeString = Field(..., alias="fee")
    fee_symbol: RawBpNonEmptyStringMax32 = Field(..., alias="feeSymbol")
    is_maker: RawBpStrictBool = Field(..., alias="isMaker")
    order_id: RawBpNonEmptyStringMax128 = Field(..., alias="orderId")
    price: RawBpFillPriceString = Field(..., alias="price")
    quantity: RawBpFillQuantityString = Field(..., alias="quantity")
    side: RawBpOrderSideString = Field(..., alias="side")
    symbol: RawBpNonEmptyStringMax32 = Field(..., alias="symbol")
    timestamp: RawBpIsoTimestampString = Field(..., alias="timestamp")
    trade_id: RawBpNonNegativeInt = Field(..., alias="tradeId")
    client_id: RawBpOptionalNonEmptyStringMax128 | None = Field(None, alias="clientId")


# The BackpackRawFillsList model remains structurally the same but benefits from
# the BackpackRawFill model being refactored.
class BackpackRawFillsList(RootModel[list[BackpackRawFill]]):
    """Pydantic model for a list of raw fill objects from the Backpack API.
    This typically represents the direct JSON response which is a list of fills.
    """

    # The 'root' attribute is implicitly defined by RootModel[list[BackpackRawFill]]
    # No need for: root: list[BackpackRawFill]

    model_config = ConfigDict(
        frozen=True,
    )

    @overload
    def __getitem__(self, item: int) -> BackpackRawFill: ...

    @overload
    def __getitem__(self, item: slice) -> list[BackpackRawFill]: ...

    def __getitem__(self, item: int | slice) -> BackpackRawFill | list[BackpackRawFill]:
        return self.root[item]

    def __len__(self) -> int:
        return len(self.root)
