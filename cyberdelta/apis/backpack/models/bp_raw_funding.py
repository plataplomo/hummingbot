"""
Backpack API Funding and Mark Price Models
-----------------------------------------

Strict Pydantic models for validating funding rate and mark price responses from the
Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business
logic.
"""

from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

# If available, import parse_decimal_value and parse_datetime_utc from the order module for consistency
try:
    from .bp_raw_order import parse_datetime_utc, parse_decimal_value
except ImportError:

    def parse_decimal_value(
        v: str | None, allow_none: bool = False, field_name: str = "field"
    ) -> Decimal | None:
        raise NotImplementedError("parse_decimal_value is not available")

    def parse_datetime_utc(v: int | float | str, field_name: str = "field") -> int | float | str:
        raise NotImplementedError("parse_datetime_utc is not available")


class BackpackRawFundingRate(BaseModel):
    """
    Pydantic model for a raw funding rate object from `/api/v1/funding` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse funding rate payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        funding_rate (str): Current funding rate (as string).
        mark_price (str): Mark price (as string).
        index_price (str): Index price (as string).
        time (int | str | float | None): Data timestamp.
    """

    symbol: str = Field(..., alias="symbol", max_length=64)
    funding_rate: str = Field(..., alias="rate", max_length=64)
    mark_price: str = Field(..., alias="markPrice", max_length=64)
    index_price: str = Field(..., alias="indexPrice", max_length=64)
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("symbol", mode="before", check_fields=False)
    @classmethod
    def validate_required_string(cls, v: Any, info: ValidationInfo) -> str:
        """
        Strictly validates required string fields for emptiness, length, and UTF-8.
        """
        field_name = info.field_name or "symbol"
        # This 'Any' type is required for Pydantic 'before' validators. See workflow docs.
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        if len(v) > 64:
            raise ValueError(f"{field_name}: String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string '{v}': {e}") from e
        return v

    @field_validator("funding_rate", "mark_price", "index_price", mode="before", check_fields=False)
    @classmethod
    def validate_decimal_string_format(cls, v: Any, info: ValidationInfo) -> str:
        """
        Strictly validates decimal string fields for emptiness and finite decimal value.
        """
        field_name = info.field_name or "field"
        # This 'Any' type is required for Pydantic 'before' validators. See workflow docs.
        if not isinstance(v, str):
            raise ValueError(
                f"{field_name}: Input must be a string representation of a number, "
                f"got {type(v).__name__}"
            )
        if not v.strip():
            raise ValueError(
                f"{field_name}: Input decimal string cannot be empty or just whitespace."
            )
        try:
            dec_val = parse_decimal_value(v, allow_none=False, field_name=field_name)
        except (InvalidOperation, ValueError, TypeError, AttributeError, NotImplementedError) as e:
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value: {e}"
            ) from e
        if dec_val is None:
            raise ValueError(
                f"{field_name}: Parsing returned None unexpectedly for non-optional field."
            )
        if not dec_val.is_finite():
            raise ValueError(
                f"{field_name}: Input must be a finite number, got '{v}' (parsed as {dec_val})."
            )
        return v

    @field_validator("time", mode="before", check_fields=False)
    @classmethod
    def validate_timestamp_format(cls, v: Any, info: ValidationInfo) -> int | float | str | None:
        """
        Strictly validates timestamp fields for type and format (int, float, or non-empty
        string).
        """
        field_name = info.field_name or "time"
        # This check is required for runtime safety with Pydantic 'before' validators.
        # The linter/type checker may flag this as always-false, but it is necessary.
        if v is None:
            raise ValueError(f"{field_name}: Value cannot be None.")
        if not isinstance(v, int | float | str):
            raise ValueError(
                f"{field_name}: Invalid type {type(v)}, expected int, float, or ISO string"
            )
        if isinstance(v, str):
            if not v.strip():
                raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        try:
            dt = parse_datetime_utc(v, field_name=field_name)
            if dt is None:
                raise ValueError(
                    f"{field_name}: Timestamp is required but received None or failed parsing."
                )
        except (ValueError, NotImplementedError) as e:
            raise ValueError(f"{field_name}: Invalid timestamp format or value '{v}': {e}") from e
        return v


class BackpackRawMarkPrice(BaseModel):
    """
    Pydantic model for a raw mark price and funding info object from `/api/v1/markPrice`
    (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse mark price payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        mark_price (str): Mark price (as string).
        funding_rate (str): Funding rate (as string).
    """

    symbol: str = Field(..., alias="symbol", max_length=64)
    mark_price: str = Field(..., alias="markPrice", max_length=64)
    funding_rate: str = Field(..., alias="fundingRate", max_length=64)
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("symbol", mode="before", check_fields=False)
    @classmethod
    def validate_required_string(cls, v: Any, info: ValidationInfo) -> str:
        """
        Strictly validates required string fields for emptiness, length, and UTF-8.
        """
        field_name = info.field_name or "symbol"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        if len(v) > 64:
            raise ValueError(f"{field_name}: String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string '{v}': {e}") from e
        return v

    @field_validator("mark_price", "funding_rate", check_fields=False)
    @classmethod
    def validate_decimal_string_format(cls, v: Any, info: ValidationInfo) -> str:
        """
        Strictly validates decimal string fields for emptiness and finite decimal value.
        """
        field_name = info.field_name or "field"
        if not isinstance(v, str):
            raise ValueError(
                f"{field_name}: Input must be a string representation of a number, got {type(v).__name__}"
            )
        if not v.strip():
            raise ValueError(
                f"{field_name}: Input decimal string cannot be empty or just whitespace."
            )
        try:
            dec_val = parse_decimal_value(v, allow_none=False, field_name=field_name)
        except (InvalidOperation, ValueError, TypeError, AttributeError, NotImplementedError) as e:
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value: {e}"
            ) from e
        if dec_val is None:
            raise ValueError(
                f"{field_name}: Parsing returned None unexpectedly for non-optional field."
            )
        if not dec_val.is_finite():
            raise ValueError(
                f"{field_name}: Input must be a finite number, got '{v}' (parsed as {dec_val})."
            )
        return v
