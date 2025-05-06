"""
Backpack Raw Kline/OHLCV Model
"""

from decimal import Decimal
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

# Assume utils are available
from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


class BackpackRawKline(BaseModel):
    """
    Represents a single Kline/OHLCV data point from the Backpack /api/v1/klines endpoint.
    This model validates the raw structure (list/tuple of 12) and the raw data types/formats
    of each element *before* Pydantic performs final coercion to the hinted field types.
    It enforces strict boundary validation and prohibits business logic checks (like OHLC).

    Structure Example (from OpenAPI spec):
    [
        1672531200000,    // Start time (Unix timestamp milliseconds) - Expect int
        "40000.0",        // Open price - Expect string
        "41000.0",        // High price - Expect string
        "39000.0",        // Low price - Expect string
        "40500.0",        // Close price - Expect string
        "1000.0",         // Volume - Expect string
        1672531259999,    // End time (Unix timestamp milliseconds) - Expect int
        "40500000.0",     // Quote asset volume - Expect string
        100,              // Number of trades - Expect int
        "500.0",          // Taker buy base asset volume - Expect string
        "20250000.0",     // Taker buy quote asset volume - Expect string
        "0"               // Ignore - Expect string
    ]
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Define fields based on the array structure (indices)
    # Type hints guide Pydantic's *final* coercion *after* 'before' validators run.
    start_time_ms: int = Field(..., description="Kline start time (Unix timestamp milliseconds)")
    open_price: Decimal = Field(..., description="Open price")
    high_price: Decimal = Field(..., description="High price")
    low_price: Decimal = Field(..., description="Low price")
    close_price: Decimal = Field(..., description="Close price")
    volume: Decimal = Field(..., description="Base asset volume")
    end_time_ms: int = Field(..., description="Kline end time (Unix timestamp milliseconds)")
    quote_volume: Decimal = Field(..., description="Quote asset volume")
    trade_count: int = Field(..., description="Number of trades")
    taker_buy_base_volume: Decimal = Field(..., description="Taker buy base asset volume")
    taker_buy_quote_volume: Decimal = Field(..., description="Taker buy quote asset volume")
    ignored: str = Field(..., description="Ignore field")

    @model_validator(mode="before")
    @classmethod
    def structure_to_dict(cls, data: object) -> dict[str, Any]:
        """
        Validate the raw input is a list/tuple of 12 elements and map it to a dict.
        This runs *before* field validators.
        """
        # DEFENSIVE CHECK: Ensure runtime type is list or tuple despite signature.
        # Runtime check required as 'data: object' allows anything.
        if not isinstance(data, list | tuple):
            raise TypeError("Kline data must be a list or tuple")

        # Check length *after* confirming type
        # DEFENSIVE CHECK: Runtime check ensures len() is safe after isinstance.
        if len(data) != 12:
            raise ValueError("Kline data must be a list/tuple of exactly 12 elements")

        # Define keys corresponding to the list indices
        keys = [
            "start_time_ms",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "end_time_ms",
            "quote_volume",
            "trade_count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignored",
        ]

        # Map list values to dict using keys
        # DEFENSIVE CHECK: isinstance ensures data is iterable for zip.
        return dict(zip(keys, data, strict=False))

    # --- Field Validators (mode='before') ---
    # These run AFTER structure_to_dict but BEFORE Pydantic's default coercion

    @field_validator("start_time_ms", "end_time_ms", mode="before")
    @classmethod
    def validate_timestamp_raw(cls, v: object, info: ValidationInfo) -> object:
        """Validate raw timestamp values are non-negative integers."""
        field_name = info.field_name or "timestamp_field"
        if not isinstance(v, int):
            raise TypeError(f"{field_name}: Raw value must be an integer, got {type(v).__name__}")
        if v < 0:
            raise ValueError(f"{field_name}: Timestamp cannot be negative, got {v}")
        return v  # Return validated raw value for Pydantic's final coercion

    @field_validator("trade_count", mode="before")
    @classmethod
    def validate_count_raw(cls, v: object, info: ValidationInfo) -> object:
        """Validate raw trade count is a non-negative integer."""
        field_name = info.field_name or "trade_count"
        if not isinstance(v, int):
            raise TypeError(f"{field_name}: Raw value must be an integer, got {type(v).__name__}")
        if v < 0:
            raise ValueError(f"{field_name}: Trade count cannot be negative, got {v}")
        return v  # Return validated raw value

    @field_validator(
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        mode="before",
    )
    @classmethod
    def validate_decimal_str_raw(cls, v: object, info: ValidationInfo) -> object:
        """
        Validate raw decimal-like values. Expects a string, checks non-empty,
        max length, parseable to Decimal, and finite.
        Handles potential pre-parsed non-string types defensively.
        """
        field_name = info.field_name or "decimal_field"
        raw_value_str = ""
        try:
            # Primarily expect string input from raw list structure
            if isinstance(v, str):
                raw_value_str = v
                # Use utils for string validation (non-empty, max_length)
                s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
                # Use utils for Decimal parsing and finiteness check
                d = parse_decimal_value(s, allow_none=False, field_name=field_name)
                if d is None or not d.is_finite():
                    raise ValueError(
                        f"{field_name}: Value must be a finite decimal string (not NaN or inf)"
                    )
                # Return the validated *string* - Pydantic will coerce it to Decimal
                return s
            # Handle cases where input might already be parsed (defensive)
            elif isinstance(v, Decimal | int | float):
                d = parse_decimal_value(v, allow_none=False, field_name=field_name)
                if d is None or not d.is_finite():
                    raise ValueError(f"{field_name}: Numeric value must be finite (not NaN or inf)")
                # Return the original numeric value if finite, Pydantic will handle
                return v
            else:
                raise TypeError(
                    f"{field_name}: Raw value must be a string, Decimal, int, or float, "
                    f"got {type(v).__name__}"
                )
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"{field_name}: Validation failed for raw value '{raw_value_str or v}': {e}"
            ) from e
        except Exception as e:
            # Wrap long line
            err_msg = (
                f"{field_name}: Unexpected validation error for raw value "
                f"'{raw_value_str or v}': {e}"
            )
            raise ValueError(err_msg) from e

    @field_validator("ignored", mode="before")
    @classmethod
    def validate_ignored_raw(cls, v: object, info: ValidationInfo) -> object:
        """Validate the raw 'ignored' field (string, non-empty, max_length)."""
        field_name = info.field_name or "ignored"
        try:
            # Expect string
            s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
            # Return validated string
            return s
        except (ValueError, TypeError) as e:
            raise ValueError(f"{field_name}: Validation failed for raw value '{v}': {e}") from e
        except Exception as e:
            raise ValueError(
                f"{field_name}: Unexpected validation error for raw value '{v}': {e}"
            ) from e

    # Removed @model_validator(mode="after") validate_ohlc_relationship
    # Business logic (like OHLC checks) is forbidden in Raw models.
