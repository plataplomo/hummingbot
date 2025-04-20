"""
Backpack API Funding and Mark Price Models
-----------------------------------------

Strict Pydantic models for validating funding rate and mark price responses from the
Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business
logic.
"""

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 64:
            raise ValueError("String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("rate", "mark_price", "index_price", mode="before", check_fields=False)
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        try:
            d = Decimal(v)
        except (InvalidOperation, TypeError, AttributeError) as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not d.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("time", mode="before", check_fields=False)
    @classmethod
    def validate_timestamp(cls, v: int | float | str | None) -> int | float | str | None:
        if v is None:
            return v
        if isinstance(v, int | float):
            return v
        # If not int or float, treat as string
        if v.isdigit():
            return int(v)
        # Accept ISO8601, but do not parse here
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
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None or not v.strip():
            raise ValueError("Must be a non-empty string")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if len(v) > 64:
            raise ValueError("String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("mark_price", "funding_rate", check_fields=False)
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        try:
            v.encode("utf-8", "strict")
            d = Decimal(v)
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        except (InvalidOperation, TypeError, AttributeError, ValueError) as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not d.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v
