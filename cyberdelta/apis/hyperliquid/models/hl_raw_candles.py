"""CyberDeltaEngine: Hyperliquid API Raw Models (Candles Group).

-----------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) responses related
to candlestick (candle) data.
It is a core part of CyberDeltaEngine's boundary validation layer for historical
and real-time price series.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Hyperliquid's 'candleSnapshot' endpoint (REST) and 'candle'
  WebSocket channel, which provide OHLCV (open, high, low, close, volume) data for assets.
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

**Usage Examples:**
    # REST API candle snapshot
    raw = HyperliquidRawCandleSnapshot.model_validate(api_response_dict)

    # WebSocket candle message
    raw_ws = HyperliquidRawWsCandle.model_validate(websocket_data)
    # ...then transform to internal candle model
"""

from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

from cyberdelta.apis.exceptions.parsing import ParsingError, StructureTypeError
from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawDefaultString,
    RawFiniteDecimalStr,
    RawHLCandleStatusString,
    RawNonNegativeFiniteDecimalStr,
    RawTimestampMsInt,
)
from cyberdelta.utils.parsing import validate_str_field
from cyberdelta.utils.typing import is_dict_str_any, is_list_any


class HyperliquidRawCandleSnapshot(BaseModel):
    """Strict boundary model for candle snapshot response from Hyperliquid candleSnapshot endpoint.

    This model validates the raw API response which consists of parallel lists for
    timestamp, open, high, low, close, volume data, and a status string.
    It adheres to the project's Raw Model policies, including strict type/format
    validation, 'extra="forbid"', and 'frozen=True'.

    The structure mirrors the 'CandleSnapshotResponse' definition found in
    the `openapi_hl.json` specification for Hyperliquid.

    Fields:
        t (list[int]): List of candle timestamps (Unix epoch in milliseconds).
            Alias "t".
        o (list[str]): List of candle open prices, as strings representing
            finite decimals. Alias "o".
        h (list[str]): List of candle high prices, as strings representing
            finite decimals. Alias "h".
        l (list[str]): List of candle low prices, as strings representing
            finite decimals. Alias "l".
        c (list[str]): List of candle close prices, as strings representing
            finite decimals. Alias "c".
        v (list[str]): List of candle volumes, as strings representing
            non-negative, finite decimals. Alias "v".
        s (str): Status string from the API (e.g., "ok"). Alias "s".
    """

    t: list[RawTimestampMsInt] = Field(..., alias="t")
    o: list[RawFiniteDecimalStr] = Field(..., alias="o")
    h: list[RawFiniteDecimalStr] = Field(..., alias="h")
    l: list[RawFiniteDecimalStr] = Field(..., alias="l")  # noqa: E741
    c: list[RawFiniteDecimalStr] = Field(..., alias="c")
    v: list[RawNonNegativeFiniteDecimalStr] = Field(..., alias="v")
    s: RawHLCandleStatusString = Field(..., alias="s", max_length=32)

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def preprocess_candle_response(cls, values: object) -> dict[str, Any]:
        """Preprocess candle snapshot response before validation.

        This handles the preprocessing logic that was previously in the
        HyperliquidResponsePreprocessingMapper.preprocess_candle_snapshot_response method.

        Returns:
            A dictionary with properly structured candle data ready for validation.

        Raises:
            StructureTypeError: If the input is not a dict or is a non-empty list.
        """
        # Handle empty list response (no candle data available)
        if is_list_any(values):
            # DEFENSIVE CHECK: Empty list is a valid response from the API
            # Mypy=[no-any-return] Ruff=[N/A]
            if not values:  # More pythonic than len(values) == 0
                # Return a minimal empty candle snapshot with proper OHLCV structure
                return {
                    "t": [],  # timestamps
                    "o": [],  # open prices
                    "h": [],  # high prices
                    "l": [],  # low prices
                    "c": [],  # close prices
                    "v": [],  # volumes
                    "s": "ok",  # status
                }
            # If it's a non-empty list, something is wrong
            raise StructureTypeError(
                field_name="candle_snapshot_response",
                expected_structure="dict",
                actual_type="non-empty list",
            )

        if not is_dict_str_any(values):
            raise StructureTypeError(
                field_name="candle_snapshot_response",
                expected_structure="dict",
                actual_type=type(values).__name__,
            )

        # values is now properly typed as dict[str, Any] due to TypeGuard
        return values

    @model_validator(mode="after")
    def check_list_lengths(self) -> Self:
        """Check that all OHLCV lists have the same length as the timestamp list.

        Returns:
            The validated instance with all lists confirmed to have equal lengths.

        Raises:
            ParsingError: If any OHLCV list has a different length than the timestamp list.
        """
        list_len = len(self.t)
        if not (
            len(self.o) == list_len
            and len(self.h) == list_len
            and len(self.l) == list_len
            and len(self.c) == list_len
            and len(self.v) == list_len
        ):
            # Create a detailed error message for multiple length mismatches
            lengths_info = (
                f"t({len(self.t)}), o({len(self.o)}), h({len(self.h)}), "
                f"l({len(self.l)}), c({len(self.c)}), v({len(self.v)})"
            )
            raise ParsingError(
                message=f"OHLCV data lists must all have the same length. Got: {lengths_info}",
                field_name="OHLCV_lists",
                expected_type="equal-length lists",
                actual_lengths=lengths_info,
            )
        return self


class HyperliquidRawCandleRequestDetails(BaseModel):
    """Details for a candle snapshot request, nested under 'req' field."""

    coin: RawDefaultString = Field(..., min_length=1, max_length=24)
    interval: RawDefaultString = Field(..., min_length=1, max_length=8)  # e.g., "1m", "1h", "1d"
    start_time: RawTimestampMsInt = Field(..., alias="startTime")
    end_time: RawTimestampMsInt = Field(..., alias="endTime")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawCandleSnapshotRequestPayload(BaseModel):
    """Strict boundary model for the request payload for the 'candleSnapshot' info type.

    Uses a nested 'req' object to encapsulate the specific parameters required for
    requesting candlestick data from Hyperliquid's info endpoint. This model ensures
    proper validation of the request structure before sending to the API.
    """

    type: Annotated[
        Literal["candleSnapshot"],
        BeforeValidator(
            lambda v: validate_str_field(v, field_name="type", max_length=32, allow_empty=False),
        ),
    ] = Field("candleSnapshot", alias="type")
    req: HyperliquidRawCandleRequestDetails

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# WebSocket Candle Models
# -----------------------


class HyperliquidRawWsCandle(BaseModel):
    """Complete raw WebSocket candle message.

    Based on actual observed WebSocket message structure:
    {
      "channel": "candle",
      "data": {
        "t": 1752198900000,
        "s": "SOL",
        "i": "1m",
        "o": "164.12",
        "c": "164.12",
        "h": "164.12",
        "l": "164.12",
        "v": "36.5",
        "n": 1
      }
    }

    This model directly validates the 'data' field of the WebSocket message.
    """

    t: RawTimestampMsInt = Field(description="Timestamp in milliseconds")
    T: RawTimestampMsInt = Field(description="Close timestamp in milliseconds")
    s: RawDefaultString = Field(
        description="Symbol/coin (e.g., 'SOL', 'BTC')",
        min_length=1,
        max_length=24,
    )
    i: RawDefaultString = Field(
        description="Interval (e.g., '1m', '5m', '1h')",
        min_length=1,
        max_length=8,
    )
    o: RawFiniteDecimalStr = Field(description="Open price")
    c: RawFiniteDecimalStr = Field(description="Close price")
    h: RawFiniteDecimalStr = Field(description="High price")
    l: RawFiniteDecimalStr = Field(description="Low price")  # noqa: E741
    v: RawNonNegativeFiniteDecimalStr = Field(description="Volume")
    n: int = Field(description="Number of trades", default=0)  # Optional field

    model_config = ConfigDict(extra="forbid", frozen=True)
