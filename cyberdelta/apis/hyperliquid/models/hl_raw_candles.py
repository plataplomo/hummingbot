"""
CyberDeltaEngine: Hyperliquid API Raw Models (Candles Group)
-----------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) responses related
to candlestick (candle) data.
It is a core part of CyberDeltaEngine's boundary validation layer for historical and real-time
price series.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Hyperliquid's 'candleSnapshot' endpoint, which provides OHLCV
  (open, high, low, close, volume) data for assets.
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
    raw = HyperliquidRawCandleSnapshot.model_validate(api_response_dict)
    # ...then transform to internal candle model
"""

from typing import Any, Self, TypeGuard

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


def is_list(obj: object) -> TypeGuard[list[Any]]:
    """
    TypeGuard to check if an object is a list.

    Args:
        obj (object): The object to check.
    Returns:
        bool: True if obj is a list, False otherwise.
    """
    return isinstance(obj, list)


def is_string(obj: object) -> TypeGuard[str]:
    """
    TypeGuard to check if an object is a string.

    Args:
        obj (object): The object to check.
    Returns:
        bool: True if obj is a string, False otherwise.
    """
    return isinstance(obj, str)


class HyperliquidRawCandleSnapshot(BaseModel):
    """
    Strict boundary model for a candle snapshot response from the 'candleSnapshot' endpoint.

    Validates the list-based structure (t, o, h, l, c, v) and status 's'.
    Enforces strict type/format constraints and equal list lengths.

    Fields:
        t (List[int]): List of timestamps (epoch ms). Alias 't'.
        o (List[str]): List of open prices as finite decimal strings. Alias 'o'.
        h (List[str]): List of high prices as finite decimal strings. Alias 'h'.
        l (List[str]): List of low prices as finite decimal strings. Alias 'l'.
        c (List[str]): List of close prices as finite decimal strings. Alias 'c'.
        v (List[str]): List of volumes as non-negative, finite decimal strings. Alias 'v'.
        s (str): Status string (e.g., 'ok'). Alias 's'.
    """

    t: list[int] = Field(..., alias="t")
    o: list[str] = Field(..., alias="o")
    h: list[str] = Field(..., alias="h")
    l: list[str] = Field(..., alias="l")  # noqa: E741
    c: list[str] = Field(..., alias="c")
    v: list[str] = Field(..., alias="v")
    s: str = Field(..., alias="s")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("t", mode="before")
    @classmethod
    def validate_timestamp_list(cls, v: list[Any], info: ValidationInfo) -> list[int]:
        """Validate 't' is a list of non-negative integers."""
        field_name = info.field_name or "timestamp_list"

        # DEFENSIVE CHECK: Ensures v is a list before iteration, even with list[Any] hint,
        # as Pydantic might pass non-list for mode='before'. Mypy=[misc]
        if not isinstance(v, list):
            raise TypeError(f"{field_name}: Must be a list, got {type(v).__name__}.")

        validated_list: list[int] = []
        for i, item in enumerate(v):
            if not isinstance(item, int):
                raise TypeError(
                    f"{field_name}[{i}]: Must be an integer, got {type(item).__name__}."
                )
            if item < 0:
                raise ValueError(f"{field_name}[{i}]: Timestamp cannot be negative.")
            validated_list.append(item)
        return validated_list

    @field_validator("o", "h", "l", "c", mode="before")
    @classmethod
    def validate_price_list(cls, v: list[Any], info: ValidationInfo) -> list[str]:
        """Validate price lists ('o', 'h', 'l', 'c') contain finite decimal strings."""
        field_name = info.field_name or "price_list"

        # DEFENSIVE CHECK: Ensures v is a list before iteration, even with list[Any] hint,
        # as Pydantic might pass non-list for mode='before'. Mypy=[misc]
        if not isinstance(v, list):
            raise TypeError(f"{field_name}: Must be a list, got {type(v).__name__}.")

        validated_list: list[str] = []
        for i, item in enumerate(v):
            if not isinstance(item, str):
                raise TypeError(f"{field_name}[{i}]: Must be a string, got {type(item).__name__}.")

            try:
                s = validate_str_field(
                    item, field_name=f"{field_name}[{i}]", allow_empty=False, max_length=64
                )
                d = parse_decimal_value(s, allow_none=False)
                if d is None or not d.is_finite():
                    raise ValueError("Decimal value must be finite.")
                validated_list.append(s)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"{field_name}[{i}]: Invalid finite decimal string '{item}': {e}"
                ) from e
        return validated_list

    @field_validator("v", mode="before")
    @classmethod
    def validate_volume_list(cls, v: list[Any], info: ValidationInfo) -> list[str]:
        """Validate 'v' list contains non-negative, finite decimal strings."""
        field_name = info.field_name or "volume_list"

        # DEFENSIVE CHECK: Ensures v is a list before iteration, even with list[Any] hint,
        # as Pydantic might pass non-list for mode='before'. Mypy=[misc]
        if not isinstance(v, list):
            raise TypeError(f"{field_name}: Must be a list, got {type(v).__name__}.")

        validated_list: list[str] = []
        for i, item in enumerate(v):
            if not isinstance(item, str):
                raise TypeError(f"{field_name}[{i}]: Must be a string, got {type(item).__name__}.")

            try:
                s = validate_str_field(
                    item, field_name=f"{field_name}[{i}]", allow_empty=False, max_length=64
                )
                d = parse_decimal_value(s, allow_none=False)
                if d is None or not d.is_finite():
                    raise ValueError("Decimal value must be finite.")
                if d < 0:
                    raise ValueError("Volume cannot be negative.")
                validated_list.append(s)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"{field_name}[{i}]: Invalid non-negative finite decimal string '{item}': {e}"
                ) from e
        return validated_list

    @field_validator("s", mode="before")
    @classmethod
    def validate_status_str(cls, v: object, info: ValidationInfo) -> str:
        """Validate 's' status string is non-empty, max_length 32."""
        field_name = info.field_name or "status"
        return validate_str_field(v, field_name=field_name, max_length=32, allow_empty=False)

    @model_validator(mode="after")
    def check_list_lengths(self) -> Self:
        """Ensure all OHLCV lists have the same length as the timestamp list."""
        list_lengths = {
            len(self.t),
            len(self.o),
            len(self.h),
            len(self.l),
            len(self.c),
            len(self.v),
        }
        if len(list_lengths) > 1:
            raise ValueError(
                "Candle snapshot lists (t, o, h, l, c, v) must all have the same length."
            )
        return self


class HyperliquidRawCandleSnapshotRequestPayload(BaseModel):
    """
    Strict boundary model for the request payload for the 'candleSnapshot' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting candlestick data for a specific asset and interval. Enforces strict type and format
    constraints for all fields. Never use for internal business logic.

    Fields:
        type (str): Must be 'candleSnapshot'.
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        interval (str): Interval string (e.g., '1m', '1h', '1d').
        start_time (int): Start timestamp (epoch ms).
        end_time (int): End timestamp (epoch ms).
    """

    type: str = Field("candleSnapshot", alias="type")
    coin: str = Field(..., alias="coin")
    interval: str = Field(..., alias="interval")
    start_time: int = Field(..., alias="startTime")
    end_time: int = Field(..., alias="endTime")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("coin", mode="before")
    @classmethod
    def validate_coin(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'coin' field to ensure it is a string of max length 64 and valid UTF-8.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated asset symbol string.
        Raises:
            ValueError: If the input is not a valid string.
        """
        return validate_str_field(v, field_name="coin", max_length=64)

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def validate_int_strict(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates that the field is a strict integer (no float or string coercion allowed).

        Args:
            v (object): The value to validate (should be an integer).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            int: The validated integer value.
        Raises:
            ValueError: If the input is not an integer.
        """
        field_name = info.field_name or "field"
        if not isinstance(v, int):
            raise ValueError(f"{field_name}: Must be an integer (no coercion allowed)")
        return v

    @field_validator("interval", mode="before")
    @classmethod
    def validate_interval(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'interval' field to ensure it is a string of max length 16 and valid UTF-8.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated interval string.
        Raises:
            ValueError: If the input is not a valid string.
        """
        return validate_str_field(v, field_name="interval", max_length=16)
