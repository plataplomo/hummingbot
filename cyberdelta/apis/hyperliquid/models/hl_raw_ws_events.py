"""CyberDeltaEngine: Hyperliquid API Raw Models (WebSocket Events Group)
--------------------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange WebSocket event payloads. It is a core part of
CyberDeltaEngine's boundary validation layer for real-time data.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures received from Hyperliquid's WebSocket channels, including user fills, order
  book updates, trades, and position updates.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking,
  and robust format validation (e.g., max length, finite decimals, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately
  rejected. This is critical for robust, secure, and predictable operation in a financial
  system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal business models with type
  conversions and business logic.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawWsFillEvent.model_validate(ws_event_dict)
    # ...then transform to internal event model

**Note:**
Do not use these models for internal business logic—use your core models for that. These are
for boundary validation only.
"""

from typing import TypeGuard, TypeVar, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawDefaultString,
    RawFiniteDecimalStr,
    RawNonNegativeInt,
    RawOptionalNonEmptyString64HL,
    RawPositiveFiniteDecimalStr,
    RawSideStr,
    RawStrictBool,
    RawTimestampMsInt,
    RawTradeHashStringHL,
)

# Import canonical BookLevel
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawBookLevel

# --- Centralized Raw User State Models ---
# The following models are imported from hl_raw_user_state.py to ensure a single
# source of truth for validation logic.
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawPositionInfo,
)

# Type variables for TypeGuard functions
T = TypeVar("T")
U = TypeVar("U")


class HyperliquidRawWsFillEvent(BaseModel):
    """Strict boundary model for a WebSocket fill event (user fill/execution) as received from the
    Hyperliquid user channel.

    This model validates the structure and content of fill events, enforcing strict type and format
    constraints for all fields. Never use for internal business logic.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        px (str): Price at which the fill occurred as a decimal string.
        sz (str): Size of the fill as a decimal string.
        side (str): Side of the trade ('B' for buy, 'A' for ask/sell).
        time (int): Timestamp of the fill event (epoch ms).
        hash (str): Unique trade hash.
        oid (int): Order ID associated with the fill.
        cloid (Optional[str]): Client order ID, if present.
        is_maker (bool): True if the user was the maker in this trade.
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    px: RawFiniteDecimalStr = Field(..., alias="px")
    sz: RawPositiveFiniteDecimalStr = Field(..., alias="sz")
    side: RawSideStr = Field(..., alias="side")
    time: RawTimestampMsInt = Field(..., alias="time")
    hash: RawTradeHashStringHL = Field(..., alias="hash")
    oid: RawNonNegativeInt = Field(..., alias="oid")
    cloid: RawOptionalNonEmptyString64HL = Field(None, alias="cloid")
    is_maker: RawStrictBool = Field(..., alias="isMaker")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


def is_list(obj: object) -> TypeGuard[list[object]]:
    """TypeGuard to check if an object is a list"""
    return isinstance(obj, list)


def has_exact_length(lst: list[object], length: int) -> bool:
    """Check if a list has exactly the specified length"""
    return len(lst) == length


def is_dict(obj: object) -> TypeGuard[dict[str, object]]:
    """TypeGuard to check if an object is a dictionary with string keys"""
    return isinstance(obj, dict)


def is_non_empty_dict(obj: object) -> TypeGuard[dict[str, object]]:
    """TypeGuard to check if an object is a non-empty dictionary with string keys"""
    return is_dict(obj) and bool(obj)


def is_list_of_list_of_dict(obj: object) -> TypeGuard[list[list[dict[str, object]]]]:
    """Type guard to check if an object is a list of two lists of dict[str, object].
    Enables static type narrowing for both Mypy and Pyright.
    """
    # Check if it's a list
    if not is_list(obj):
        return False

    # Now mypy and pyright know obj is List[Any]
    if not has_exact_length(obj, 2):
        return False

    # Check each sub-list
    for sub_obj in obj:
        if not is_list(sub_obj):
            return False

        # At this point, both mypy and pyright know sub_obj is a List[Any]
        for item in sub_obj:
            if not isinstance(item, dict):
                return False

    # If we made it here, obj is a List[List[Dict[str, Any]]]
    return True


class HyperliquidRawWsBookUpdate(BaseModel):
    """Strict boundary model for a WebSocket order book update event (l2Book channel).

    This model validates the structure and content of order
    book update events, enforcing strict type and format
    constraints for all fields. Never use for internal business logic.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        levels (List[List[HyperliquidRawBookLevel]]): Nested list of price levels [bids, asks].
        time (int): Snapshot timestamp (epoch ms).
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    levels: list[list[HyperliquidRawBookLevel]] = Field(..., alias="levels")
    time: RawTimestampMsInt = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("levels", mode="before")
    @classmethod
    def validate_levels_structure(cls, v: object, info: ValidationInfo) -> list[list[object]]:
        """Validates that 'levels' is a list of length 2 (bids, asks), and each element is a list.
        The inner elements will be parsed by Pydantic against HyperliquidRawBookLevel.
        Returns the raw validated structure for Pydantic to process further.
        """
        if not is_list(v):
            raise ValueError("levels: Must be a list.")
        if not has_exact_length(v, 2):
            raise ValueError("levels: Must be a list of two lists (bids, asks), length != 2.")
        bids_raw, asks_raw = v[0], v[1]
        if not is_list(bids_raw):
            raise ValueError("levels[0] (bids): Must be a list.")
        if not is_list(asks_raw):
            raise ValueError("levels[1] (asks): Must be a list.")
        return cast(list[list[object]], v)


class HyperliquidRawWsTradeEvent(BaseModel):
    """Represents a WebSocket trade event (trades channel) as received from the Hyperliquid public
    stream.

    This model is used to validate the structure of public trade events, which provide real-time
    trade data for an asset.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        px (str): Price at which the trade occurred.
        sz (str): Size of the trade.
        side (str): Side of the trade ('B' for buy, 'A' for ask/sell).
        time (int): Timestamp of the trade event (epoch ms).
        hash (str): Unique trade hash.
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    px: RawFiniteDecimalStr = Field(..., alias="px")
    sz: RawPositiveFiniteDecimalStr = Field(..., alias="sz")
    side: RawSideStr = Field(..., alias="side")
    time: RawTimestampMsInt = Field(..., alias="time")
    hash: RawTradeHashStringHL = Field(..., alias="hash")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawWsOrderUpdate(BaseModel):
    """Represents a WebSocket order update event (user channel) as received from the Hyperliquid
    private stream.

    This model is used to validate the structure of order update events, which notify the user of
    changes to their orders (e.g., open, filled, canceled).

    Fields:
        event_type (str): Type of the event (e.g., 'orderUpdate').
        data (dict): Event data payload (structure may vary by event type).
    """

    event_type: RawDefaultString = Field(..., alias="eventType", max_length=32)
    data: dict[str, object] = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("data", mode="before")
    @classmethod
    def validate_data(cls, v: object, info: ValidationInfo) -> dict[str, object]:
        """Ensure data is a non-empty dictionary."""
        if not isinstance(v, dict):
            raise ValueError("data: Must be a dictionary")
        if not v:  # Test expects empty dict to fail
            raise ValueError("data: Dictionary cannot be empty")
        return cast(dict[str, object], v)


class HyperliquidRawWsPositionUpdateEvent(BaseModel):
    """Represents a WebSocket position update event (user position change) as received from the
    Hyperliquid private stream.

    This model is used to validate the structure of position update events, which notify the user
    of changes to their open positions.

    Fields:
        asset (str): Asset symbol (e.g., 'ETH', 'BTC').
        position (HyperliquidRawPositionInfo): Detailed position information.
        time (int): Timestamp of the position update event (epoch ms).
    """

    asset: RawAssetString64HL = Field(..., alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    time: RawTimestampMsInt = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
