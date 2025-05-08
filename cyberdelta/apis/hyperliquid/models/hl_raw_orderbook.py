"""
CyberDeltaEngine: Hyperliquid API Raw Models (Order Book Group)
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
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawL2Book.model_validate(api_response_dict)
    # ...then transform to internal order book model
"""

from typing import Annotated, Literal, TypeGuard

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawFiniteDecimalStr,
    RawNonNegativeInt,
    RawPositiveFiniteDecimalStr,
    RawTimestampMsInt,
)
from cyberdelta.utils.parsing import validate_str_field


def is_list(obj: object) -> TypeGuard[list[object]]:
    """TypeGuard to check if an object is a list"""
    return isinstance(obj, list)


def has_exact_length(lst: list[object], length: int) -> bool:
    """Check if a list has exactly the specified length"""
    return len(lst) == length


def all_are_lists(items: list[object]) -> bool:
    """Check if all items in a list are themselves lists"""
    return all(isinstance(sub, list) for sub in items)


# --- Price Level Submodel ---
class HyperliquidRawBookLevel(BaseModel):
    """
    Strict boundary model for a single price level in the order book as returned in L2 book
    endpoints.

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
    """
    Strict boundary model for a full L2 order book snapshot as returned in order book
    endpoints.

    This model validates the structure and content of the L2 book response, enforcing strict
    type and format constraints for all fields. Never use for internal business logic.

    Fields:
        coin (RawAssetString64HL): Asset symbol (e.g., 'ETH', 'BTC').
        levels (List[List[HyperliquidRawBookLevel]]): Nested list of price levels [bids, asks].
        time (RawTimestampMsInt): Snapshot timestamp (epoch ms).
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    levels: list[list[HyperliquidRawBookLevel]] = Field(..., alias="levels")
    time: RawTimestampMsInt = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("levels", mode="before")
    @classmethod
    def validate_levels_structure(cls, v: object, info: ValidationInfo) -> object:
        """
        Validates that 'levels' is a list of length 2 (bids, asks), and each element is a list.
        The inner elements will be parsed by Pydantic against HyperliquidRawBookLevel.

        Args:
            v (object): The value to validate (should be a list of two lists).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            object: The validated raw structure for 'levels'.
        Raises:
            ValueError: If the input is not a valid structure for order book levels.
        """
        if not is_list(v):
            raise ValueError("levels: Must be a list.")

        if not has_exact_length(v, 2):
            raise ValueError("levels: Must be a list of two lists (bids, asks), length != 2.")

        # v is now known to be a list of length 2
        bids_raw, asks_raw = v[0], v[1]

        if not is_list(bids_raw):
            raise ValueError("levels[0] (bids): Must be a list.")
        if not is_list(asks_raw):
            raise ValueError("levels[1] (asks): Must be a list.")

        # Further validation of individual level items (e.g. dicts with px, sz, n)
        # will be handled by Pydantic when it parses into list[list[HyperliquidRawBookLevel]].
        # This validator ensures the basic [list, list] structure.
        return v  # Return the raw validated structure for Pydantic to process further


class HyperliquidRawL2BookRequestPayload(BaseModel):
    """
    Strict boundary model for the request payload for the 'l2Book' info type.

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
            lambda val: validate_str_field(val, "type", max_length=32, allow_empty=False)
        ),
    ] = Field("l2Book", alias="type")
    coin: RawAssetString64HL = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
