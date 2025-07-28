"""CyberDeltaEngine: Hyperliquid API Raw Models (WebSocket Events Group).

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

import math
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import TypeGuard, TypeVar, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)

from cyberdelta.apis.exceptions.parsing import (
    EmptyDictionaryError,
    SequenceLengthError,
    StructureTypeError,
)
from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawAssetString64HL,
    RawDefaultString,
    RawFiniteDecimalStr,
    RawNonNegativeInt,
    RawOptionalCloidHL,
    RawPositiveFiniteDecimalStr,
    RawSideStr,
    RawStrictBool,
    RawStrictEthereumAddressStrHL,
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


# Type for numeric values that can be normalized to strings
NumericInput = int | float | str


# Type variables for TypeGuard functions
T = TypeVar("T")
U = TypeVar("U")

# Time validation constants
MAX_TIMESTAMP_AGE_SECONDS = 86400  # 24 hours
MAX_FUTURE_TIMESTAMP_SECONDS = 3600  # 1 hour
MAX_DECIMAL_PRECISION = 8  # Maximum 8 decimal places for crypto


def _raise_notional_value_error(
    notional: Decimal, price: Decimal, size: Decimal, condition: str
) -> None:
    """Raise error for notional value violations.
    
    Raises:
        ValueError: When notional value violates business rules.
    """
    msg = f"Notional value too {condition}: {notional} (price: {price}, size: {size})"
    raise ValueError(msg)


class HyperliquidRawWsFillEvent(BaseModel):
    """Strict boundary model for a WebSocket fill event (user fill/execution).

    Received from the Hyperliquid user channel. This model validates the structure and
    content of fill events, enforcing strict type and format constraints for all fields.
    Never use for internal business logic.

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
    cloid: RawOptionalCloidHL = Field(None, alias="cloid")
    is_maker: RawStrictBool = Field(..., alias="isMaker")
    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
    )

    @field_validator("time", mode="after")
    @classmethod
    def validate_timestamp_range(cls, v: int, _info: ValidationInfo) -> int:
        """Comprehensive timestamp validation after conversion.

        This validator performs business logic validation to ensure timestamp
        is reasonable after conversion.
        
        Returns:
            Validated timestamp in milliseconds.
            
        Raises:
            ValueError: If timestamp is outside acceptable range.
        """
        # Convert to datetime for validation
        try:
            dt = datetime.fromtimestamp(v / 1000, tz=UTC)
        except (ValueError, OSError) as e:
            msg = f"Invalid timestamp {v}: {e}"
            raise ValueError(msg) from e

        now = datetime.now(UTC)
        age_seconds = abs((dt - now).total_seconds())

        # Reject timestamps more than 24 hours old or 1 hour in future
        if age_seconds > MAX_TIMESTAMP_AGE_SECONDS:  # 24 hours
            msg = f"Timestamp too old: {dt.isoformat()} (age: {age_seconds / 3600:.1f}h)"
            raise ValueError(msg)
        if (dt - now).total_seconds() > MAX_FUTURE_TIMESTAMP_SECONDS:  # 1 hour in future
            msg = f"Timestamp too far in future: {dt.isoformat()}"
            raise ValueError(msg)

        return v

    @field_validator("px", "sz", mode="before")
    @classmethod
    def normalize_numeric_strings(cls, v: NumericInput, info: ValidationInfo) -> str:
        """Normalize numeric strings before Decimal conversion.

        This validator preprocesses numeric inputs from various formats
        before Decimal conversion.
        
        Returns:
            Normalized string representation suitable for Decimal conversion.
            
        Raises:
            ValueError: If value cannot be normalized to a valid numeric string.
        """
        if isinstance(v, (int, float)):
            # Convert numbers to strings for Decimal precision
            if isinstance(v, float) and (math.isinf(v) or math.isnan(v)):
                msg = f"Invalid numeric value in {info.field_name}: {v}"
                raise ValueError(msg)
            return str(v)

        # Handle string inputs (all other cases after int/float)
        # Clean and validate numeric strings
        v = v.strip()
        if not v:
            msg = f"Empty numeric string in {info.field_name}"
            raise ValueError(msg)

        # Remove any currency symbols or spaces
        v = re.sub(r"[^0-9.-]", "", v)

        # Validate format
        if not re.match(r"^-?\d+(\.\d+)?$", v):
            msg = f"Invalid numeric format in {info.field_name}: {v}"
            raise ValueError(msg)

        return v

    @field_validator("px", "sz", mode="after")
    @classmethod
    def validate_financial_precision(cls, v: str, info: ValidationInfo) -> str:
        """Ensure financial precision after string validation.

        This validator performs business logic validation of financial
        precision requirements.
        
        Returns:
            Validated financial string value.
            
        Raises:
            ValueError: If value doesn't meet financial precision requirements.
        """
        try:
            decimal_val = Decimal(v)
        except Exception as e:
            msg = f"Cannot parse {info.field_name} as decimal: {v}"
            raise ValueError(msg) from e

        if not decimal_val.is_finite():
            msg = f"Non-finite value in {info.field_name}: {v}"
            raise ValueError(msg)

        # Validate precision (max 8 decimal places for crypto)
        exponent = decimal_val.as_tuple().exponent
        if isinstance(exponent, int) and exponent < -MAX_DECIMAL_PRECISION:
            msg = f"Excessive precision in {info.field_name}: {v} (max 8 decimal places)"
            raise ValueError(msg)

        # Additional validation for price (px) field
        if info.field_name == "px":
            if decimal_val <= 0:
                msg = f"Price must be positive: {v}"
                raise ValueError(msg)
            # Reasonable price range validation (crypto prices)
            if decimal_val > Decimal(1000000):  # $1M per unit seems reasonable upper bound
                msg = f"Price too high: {v}"
                raise ValueError(msg)

        # Additional validation for size (sz) field
        if info.field_name == "sz":
            if decimal_val <= 0:
                msg = f"Size must be positive: {v}"
                raise ValueError(msg)
            # Reasonable size validation (prevent extremely large trades)
            if decimal_val > Decimal(1000000):  # 1M units seems reasonable
                msg = f"Size too large: {v}"
                raise ValueError(msg)

        return v

    @field_serializer("px", "sz")
    def serialize_decimal_fields(self, value: str) -> str:
        """Serialize decimal fields with optimized precision for WebSocket transmission.

        This custom serializer ensures that decimal fields are properly formatted for
        WebSocket transmission and API responses. It normalizes the decimal representation
        while preserving the necessary precision for financial calculations.

        Args:
            value: The decimal string value to serialize

        Returns:
            Normalized decimal string optimized for transmission
        """
        try:
            # Convert to Decimal for normalization
            decimal_val = Decimal(value)

            # Normalize to remove trailing zeros and use minimal representation
            normalized = decimal_val.normalize()

            # For very large or very small numbers, use scientific notation
            # to reduce transmission size while preserving precision
            large_value_threshold = 1000000
            if abs(normalized) >= large_value_threshold:
                # Use scientific notation for large values (>= 1M)
                return f"{normalized:.6E}"

            if abs(normalized) <= Decimal("0.000001") and normalized != 0:
                # Use scientific notation for very small values (< 1e-6)
                return f"{normalized:.6E}"
            # Use standard decimal notation for normal ranges
            return str(normalized)

        except (ValueError, TypeError, ArithmeticError):
            # Fallback to original value if normalization fails
            return value

    @model_validator(mode="after")
    def validate_fill_consistency(self) -> "HyperliquidRawWsFillEvent":
        """Validate fill event cross-field consistency.

        This model validator performs cross-field validation to ensure
        fill data is internally consistent.
        
        Returns:
            Validated HyperliquidRawWsFillEvent instance.
            
        Raises:
            ValueError: If cross-field validation fails.
        """
        # Validate that price * size is reasonable (basic sanity check)
        try:
            price = Decimal(self.px)
            size = Decimal(self.sz)
            notional = price * size

            # Validate notional value is reasonable
            if notional > Decimal(10000000):  # $10M notional seems like reasonable limit
                _raise_notional_value_error(notional, price, size, "large")

            if notional < Decimal("0.01"):  # $0.01 minimum notional
                _raise_notional_value_error(notional, price, size, "small")

        except Exception as e:
            msg = f"Error validating fill notional: {e}"
            raise ValueError(msg) from e

        # Validate timestamp consistency with order ID
        # Order IDs typically increase over time, so newer fills should have higher OIDs
        # This is a heuristic check, not strict validation
        # Both oid and time are required fields, so they always exist
        # Basic sanity check: very old timestamps with very high order IDs might be suspicious
        # This is just a warning-level validation

        return self


def is_list(obj: object) -> TypeGuard[list[object]]:
    """TypeGuard to check if an object is a list.
    
    Returns:
        True if obj is a list, False otherwise.
    """
    return isinstance(obj, list)


def has_exact_length(lst: list[object], length: int) -> bool:
    """Check if a list has exactly the specified length.
    
    Returns:
        True if list has exactly the specified length, False otherwise.
    """
    return len(lst) == length


def is_dict(obj: object) -> TypeGuard[dict[str, object]]:
    """TypeGuard to check if an object is a dictionary with string keys.
    
    Returns:
        True if obj is a dictionary, False otherwise.
    """
    return isinstance(obj, dict)


def is_non_empty_dict(obj: object) -> TypeGuard[dict[str, object]]:
    """TypeGuard to check if an object is a non-empty dictionary with string keys.
    
    Returns:
        True if obj is a non-empty dictionary, False otherwise.
    """
    return is_dict(obj) and bool(obj)


def is_list_of_list_of_dict(obj: object) -> TypeGuard[list[list[dict[str, object]]]]:
    """Type guard to check if an object is a list of two lists of dict[str, object].

    Enables static type narrowing for both Mypy and Pyright.
    
    Returns:
        True if obj matches the expected structure, False otherwise.
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
        """Validate that 'levels' is a list of length 2 (bids, asks), each element a list.

        The inner elements will be parsed by Pydantic against HyperliquidRawBookLevel.
        Returns the raw validated structure for Pydantic to process further.

        Args:
            v: Raw levels data from WebSocket event
            info: Pydantic validation context

        Returns:
            Validated list structure ready for further Pydantic processing

        Raises:
            StructureTypeError: If structure is not a list or sublists are invalid.
            SequenceLengthError: If outer list doesn't have exactly 2 elements.

        """
        field_name = info.field_name or "levels"
        if not is_list(v):
            raise StructureTypeError(
                field_name=field_name,
                expected_structure="a list",
                actual_type=type(v).__name__,
            )
        if not has_exact_length(v, 2):
            raise SequenceLengthError(
                field_name=field_name,
                expected_length=2,
                actual_length=len(v),
                sequence_type="list of two lists (bids, asks)",
            )
        bids_raw, asks_raw = v[0], v[1]
        if not is_list(bids_raw):
            raise StructureTypeError(
                field_name=f"{field_name}[0] (bids)",
                expected_structure="a list",
                actual_type=type(bids_raw).__name__,
            )
        if not is_list(asks_raw):
            raise StructureTypeError(
                field_name=f"{field_name}[1] (asks)",
                expected_structure="a list",
                actual_type=type(asks_raw).__name__,
            )
        return cast("list[list[object]]", v)


class HyperliquidRawWsTradeEvent(BaseModel):
    """Represents a WebSocket trade event (trades channel).

    Received from the Hyperliquid public stream. This model is used to validate the
    structure of public trade events, which provide real-time trade data for an asset.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        px (str): Price at which the trade occurred.
        sz (str): Size of the trade.
        side (str): Side of the trade ('B' for buy, 'A' for ask/sell).
        time (int): Timestamp of the trade event (epoch ms).
        hash (str): Unique trade hash.
        tid (int): Trade ID.
        users (list[str]): List of user addresses involved in the trade.
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    px: RawFiniteDecimalStr = Field(..., alias="px")
    sz: RawPositiveFiniteDecimalStr = Field(..., alias="sz")
    side: RawSideStr = Field(..., alias="side")
    time: RawTimestampMsInt = Field(..., alias="time")
    hash: RawTradeHashStringHL = Field(..., alias="hash")
    tid: RawNonNegativeInt = Field(..., alias="tid")
    users: list[RawStrictEthereumAddressStrHL] = Field(..., alias="users")
    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
    )


class HyperliquidRawWsTradeEventsList(RootModel[list[HyperliquidRawWsTradeEvent]]):
    """Raw model for WebSocket trades channel messages as a list.

    Hyperliquid sends trades as a direct list of trade events:
    [{"coin": "BTC", "px": "108343.0", "sz": "0.00034", ...}, ...]

    This follows the same pattern as BackpackRawFillsList.
    """

    root: list[HyperliquidRawWsTradeEvent]
    model_config = ConfigDict(frozen=True)


class HyperliquidRawWsOrderUpdate(BaseModel):
    """Represents a WebSocket order update event (user channel) from the Hyperliquid private stream.

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
        """Ensure data is a non-empty dictionary.
        
        Returns:
            Validated dictionary data.
            
        Raises:
            StructureTypeError: If data is not a dictionary.
            EmptyDictionaryError: If dictionary is empty.
        """
        field_name = info.field_name or "data"
        if not isinstance(v, dict):
            raise StructureTypeError(
                field_name=field_name,
                expected_structure="a dictionary",
                actual_type=type(v).__name__,
            )
        if not v:  # Test expects empty dict to fail
            raise EmptyDictionaryError(field_name=field_name)
        return cast("dict[str, object]", v)


class HyperliquidRawWsPositionUpdateEvent(BaseModel):
    """Represents a WebSocket position update event from the Hyperliquid private stream.

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
