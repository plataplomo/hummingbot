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

from decimal import Decimal
from typing import overload

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    field_serializer,
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
class BackpackRawFillResponse(BaseModel):
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
        system_order_type (str | None): Optional system order type from the exchange.

    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
    )

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
    system_order_type: RawBpOptionalNonEmptyStringMax128 | None = Field(
        None,
        alias="systemOrderType",
    )

    @field_serializer("fee", "price", "quantity")
    def serialize_decimal_fields(self, value: str) -> str:
        """Serialize decimal fields for Backpack fill events.

        Fill data from Backpack requires precise decimal handling for trading
        calculations. This serializer ensures optimal representation for
        financial data while maintaining full precision.

        Args:
            value: The decimal string value to serialize

        Returns:
            Optimized decimal string for Backpack fill data
        """
        try:
            decimal_val = Decimal(value)
            normalized = decimal_val.normalize()

            # For Backpack fills, maintain high precision for financial accuracy
            # but optimize for transmission efficiency
            large_value_threshold = 1000000
            if abs(normalized) >= large_value_threshold:
                # Large values: use scientific notation for efficiency
                return f"{normalized:.10E}"

            if abs(normalized) <= Decimal("0.0000000001") and normalized != 0:
                # Very small values: use scientific notation
                return f"{normalized:.10E}"
            # Normal range: use decimal notation with normalization
            # This removes trailing zeros while preserving precision
            return str(normalized)

        except (ValueError, TypeError, ArithmeticError):
            # Fallback to original value if conversion fails
            return value


# The BackpackRawFillsList model remains structurally the same but benefits from
# the BackpackRawFill model being refactored.
class BackpackRawFillsList(RootModel[list[BackpackRawFillResponse]]):
    """Pydantic model for a list of raw fill objects from the Backpack API.

    This typically represents the direct JSON response which is a list of fills.
    """

    # The 'root' attribute is implicitly defined by RootModel[list[BackpackRawFill]]
    # No need for: root: list[BackpackRawFill]

    model_config = ConfigDict(
        frozen=True,
    )

    @overload
    def __getitem__(self, item: int) -> BackpackRawFillResponse: ...

    @overload
    def __getitem__(self, item: slice) -> list[BackpackRawFillResponse]: ...

    def __getitem__(
        self,
        item: int | slice,
    ) -> BackpackRawFillResponse | list[BackpackRawFillResponse]:
        """Return a fill by index or a slice of fills for list-like access."""
        return self.root[item]

    def __len__(self) -> int:
        """Return the number of fills in the list."""
        return len(self.root)
