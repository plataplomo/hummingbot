"""
Backpack API Funding and Mark Price Models
-----------------------------------------

Strict Pydantic models for validating funding rate and mark price responses from the
Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business
logic.
"""

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("symbol", mode="before", check_fields=False)
    @classmethod
    def validate_required_string(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "symbol"
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("funding_rate", "mark_price", "index_price", mode="before", check_fields=False)
    @classmethod
    def validate_decimal_string_format(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        if not isinstance(v, str):
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value, "
                f"got {type(v).__name__}"
            )
        if not v.strip():
            raise ValueError(
                f"{field_name}: Must be a non-empty string representing a decimal value"
            )
        try:
            dec_val = parse_decimal_value(v.strip(), allow_none=False, field_name=field_name)
        except Exception as err:
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value: {err}"
            ) from err
        if dec_val is None or not dec_val.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("time", mode="before", check_fields=False)
    @classmethod
    def validate_timestamp_format(cls, v: object, info: ValidationInfo) -> int | float | str | None:
        field_name = info.field_name or "time"
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
            parse_datetime_utc(v, field_name=field_name)
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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("symbol", mode="before", check_fields=False)
    @classmethod
    def validate_required_string(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "symbol"
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("mark_price", "funding_rate", check_fields=False)
    @classmethod
    def validate_decimal_string_format(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        if not isinstance(v, str):
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value, "
                f"got {type(v).__name__}"
            )
        if not v.strip():
            raise ValueError(
                f"{field_name}: Must be a non-empty string representing a decimal value"
            )
        try:
            dec_val = parse_decimal_value(v.strip(), allow_none=False, field_name=field_name)
        except Exception as err:
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value: {err}"
            ) from err
        if dec_val is None or not dec_val.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return v
