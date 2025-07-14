"""CyberDeltaEngine: Hyperliquid API Raw Models (Order Book Group).

--------------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) responses related to
the L2 order book. It is a core part of CyberDeltaEngine's boundary validation layer for
real-time and historical order book data.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Hyperliquid's order book endpoints, including price levels and
  full L2 book snapshots.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking,
  and robust format validation (e.g., max length, finite decimals, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately
  rejected. This is critical for robust, secure, and predictable operation in a
  financial system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal business models with
  type conversions and business logic.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawL2Book.model_validate(api_response_dict)
    # ...then transform to internal order book model
"""

from typing import Annotated, Any, Literal, cast

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cyberdelta.apis.exceptions.parsing import SequenceLengthError, StructureTypeError
from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawAssetString64HL,
    RawFiniteDecimalStr,
    RawInt,
    RawNonNegativeInt,
    RawPositiveFiniteDecimalStr,
)
from cyberdelta.utils.parsing import validate_str_field
from cyberdelta.utils.typing import is_dict_str_any, is_sequence_of_any


def has_exact_length(lst: list[object], length: int) -> bool:
    """Check if a list has exactly the specified length."""
    return len(lst) == length


def all_are_sequences(items: list[object]) -> bool:
    """Check if all items in a list are themselves sequences (lists or tuples)."""
    return all(is_sequence_of_any(sub) for sub in items)


# --- Price Level Submodel ---
class HyperliquidRawBookLevel(BaseModel):
    """Strict boundary model for a single price level in the order book from L2 book endpoints.

    This model validates the structure and content of each price level entry, enforcing
    strict type and format constraints for all fields. Never use for internal business logic.

    Fields:
        px (RawFiniteDecimalStr): Price at this level as a decimal string.
        sz (RawPositiveFiniteDecimalStr): Size available at this price level as a decimal string.
        n (RawNonNegativeInt): Number of orders at this price level.
    """

    px: RawFiniteDecimalStr = Field(..., alias="px")
    sz: RawPositiveFiniteDecimalStr = Field(..., alias="sz")
    n: RawNonNegativeInt = Field(..., alias="n")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- L2 Order Book Model ---
class HyperliquidRawL2Book(BaseModel):
    """Strict boundary model for a full L2 order book snapshot as returned in order book endpoints.

    This model validates the structure and content of the L2 book response, enforcing strict
    type and format constraints for all fields. Never use for internal business logic.

    Fields:
        coin (RawAssetString64HL): Asset symbol (e.g., 'ETH', 'BTC').
        levels (List[List[HyperliquidRawBookLevel]]): Nested list of price levels [bids, asks].
        time (RawInt): Snapshot timestamp (epoch ms).
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    levels: list[list[HyperliquidRawBookLevel]] = Field(..., alias="levels")
    time: RawInt = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def preprocess_orderbook_response(cls, values: object, info: ValidationInfo) -> dict[str, Any]:
        """Preprocess order book response before validation.

        This handles the preprocessing logic that was previously in the
        HyperliquidResponsePreprocessingMapper.preprocess_l2_book_response method.
        Specifically handles None responses by returning an empty order book structure.
        """
        # Handle None response (no order book data available)
        if values is None:
            # Get symbol from validation context if available
            symbol = "UNKNOWN"
            if info.context and "symbol" in info.context:
                symbol = str(info.context["symbol"])

            return {
                "coin": symbol,
                "levels": [[], []],  # [bids, asks] - both empty lists
                "time": 0,  # zero timestamp for empty book
            }

        if not is_dict_str_any(values):
            raise StructureTypeError(
                field_name="order_book_response",
                expected_structure="a dict",
                actual_type=type(values).__name__,
            )

        # values is now properly typed as dict[str, Any] due to TypeGuard
        return values

    @field_validator("levels", mode="before")
    @classmethod
    def validate_levels_structure(cls, v: object, info: ValidationInfo) -> list[list[object]]:
        """Validate that 'levels' is a list of length 2 (bids, asks), and each element is a list.

        The inner elements will be parsed by Pydantic against HyperliquidRawBookLevel.

        Args:
            v (object): The value to validate (should be a list of two lists).
            info (ValidationInfo): Pydantic validation context.

        Returns:
            list[list[object]]: The validated raw structure for 'levels'.

        Raises:
            ValueError: If the input is not a valid structure for order book levels.

        """
        field_name = info.field_name or "levels"
        if not is_sequence_of_any(v):
            raise StructureTypeError(
                field_name=field_name,
                expected_structure="a sequence (list or tuple)",
                actual_type=type(v).__name__,
            )

        if not has_exact_length(list(v), 2):
            raise SequenceLengthError(
                field_name=field_name,
                expected_length=2,
                actual_length=len(list(v)),
                sequence_type="sequence of two sequences (bids, asks)",
            )

        # v is now known to be a sequence of length 2
        bids_raw, asks_raw = v[0], v[1]

        if not is_sequence_of_any(bids_raw):
            raise StructureTypeError(
                field_name=f"{field_name}[0] (bids)",
                expected_structure="a sequence",
                actual_type=type(bids_raw).__name__,
            )
        if not is_sequence_of_any(asks_raw):
            raise StructureTypeError(
                field_name=f"{field_name}[1] (asks)",
                expected_structure="a sequence",
                actual_type=type(asks_raw).__name__,
            )

        # Further validation of individual level items (e.g. dicts with px, sz, n)
        # will be handled by Pydantic when it parses into list[list[HyperliquidRawBookLevel]].
        # This validator ensures the basic [list, list] structure.
        return cast(
            "list[list[object]]",
            v,
        )  # Return the raw validated structure for Pydantic to process further


class HyperliquidRawL2BookRequestPayload(BaseModel):
    """Strict boundary model for the request payload for the 'l2Book' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting a full L2 order book snapshot for a specific asset. Enforces strict type and format
    constraints for all fields. Never use for internal business logic.

    Fields:
        type (Literal['l2Book']): Must be 'l2Book'.
        coin (RawAssetString64HL): Asset symbol (e.g., 'ETH', 'BTC').
    """

    type: Annotated[
        Literal["l2Book"],
        BeforeValidator(
            lambda val: validate_str_field(val, "type", max_length=32, allow_empty=False),
        ),
    ] = Field("l2Book", alias="type")
    coin: RawAssetString64HL = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
