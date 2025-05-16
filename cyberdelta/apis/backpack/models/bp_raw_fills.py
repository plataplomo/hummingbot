"""
CyberDeltaEngine: Backpack API Raw Models (User Fills)
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

from collections.abc import Iterator
from typing import overload

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from .bp_common_raw_types import (
    RawBpIsoTimestampString,
    RawBpNonEmptyStringMax32,
    RawBpNonEmptyStringMax64,
    RawBpNonEmptyStringMax128,
    RawBpNonNegativeInt,
    RawBpOptionalNonEmptyStringMax128,
    RawBpOrderSideString,
    RawBpParsableFiniteDecimalString,
    RawBpStrictBool,
)


# --- Core Backpack Fill Model ---
class BackpackRawFill(BaseModel):
    """
    Strict boundary Pydantic model for a single user fill (trade) object from the Backpack API
    endpoint `/wapi/v1/history/fills`. Corresponds to the `OrderFill` schema in Backpack's OpenAPI.

    This model validates the structure and raw data types using common annotated types.
    It enforces immutability (`frozen=True`) and forbids extra fields (`extra='forbid').

    Attributes (after Pydantic processing):
        fee (str): The fee charged for the fill (validated as a parsable decimal string).
        fee_symbol (str): The asset symbol in which the fee was charged.
        is_maker (bool): Indicates if the fill was for a maker order.
        order_id (str): The ID of the order associated with this fill.
        price (str): The execution price of the fill (validated as a parsable decimal string).
        quantity (str): The executed quantity for this fill (validated as a parsable decimal string).
        side (str): The side of the order ('Bid' or 'Ask').
        symbol (str): The trading symbol.
        timestamp (str): The execution timestamp in ISO 8601 format.
        trade_id (int): The unique ID for this trade/fill.
        client_id (str | None): Optional client-provided order ID.
    """

    fee: RawBpParsableFiniteDecimalString = Field(...)
    fee_symbol: RawBpNonEmptyStringMax32 = Field(..., alias="feeSymbol")
    is_maker: RawBpStrictBool = Field(..., alias="isMaker")
    order_id: RawBpNonEmptyStringMax128 = Field(..., alias="orderId")
    price: RawBpParsableFiniteDecimalString = Field(...)
    quantity: RawBpParsableFiniteDecimalString = Field(...)
    side: RawBpOrderSideString = Field(
        ...
    )  # Field max_length=3 is implicitly handled by RawBpOrderSideString's internal validator
    symbol: RawBpNonEmptyStringMax64 = Field(...)
    timestamp: RawBpIsoTimestampString = Field(...)
    trade_id: RawBpNonNegativeInt = Field(
        ..., alias="tradeId"
    )  # ge=0 handled by RawBpNonNegativeInt
    client_id: RawBpOptionalNonEmptyStringMax128 = Field(None, alias="clientId")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
    )

    # All individual @field_validator methods are removed as their logic
    # is now encapsulated in the Annotated types from bp_common_raw_types.py.
    # The RawBpOptionalNonEmptyStringMax128 handles the non-empty/non-whitespace check for client_id if provided.


# The BackpackRawFillsList model remains structurally the same but benefits from
# the BackpackRawFill model being refactored.
class BackpackRawFillsList(BaseModel):
    """
    Pydantic model for a list of raw fill objects from the Backpack API.
    This typically represents the direct JSON response which is a list of fills.
    """

    root: list[BackpackRawFill]

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    def __iter__(self) -> Iterator[BackpackRawFill]:
        return iter(self.root)

    @overload
    def __getitem__(self, item: int) -> BackpackRawFill: ...

    @overload
    def __getitem__(self, item: slice) -> list[BackpackRawFill]: ...

    def __getitem__(self, item: int | slice) -> BackpackRawFill | list[BackpackRawFill]:
        return self.root[item]

    def __len__(self) -> int:
        return len(self.root)
