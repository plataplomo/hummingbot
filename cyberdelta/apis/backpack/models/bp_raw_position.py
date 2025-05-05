"""
Backpack API Position Models (RAW)
----------------------------------

This module defines strict Pydantic models for validating position responses from the Backpack
Exchange API. These models are used for boundary validation and transformation, not for internal
business logic.

Models:
    - BackpackRawPosition: Validates open position objects (all required fields, strict schema).
    - BackpackRawPositionUpdate: Validates position update events from the WebSocket stream.

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Timestamps accept int, float, or ISO8601-like strings.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic, ensuring
robustness and security at the data ingestion boundary.
"""

from pydantic import BaseModel, ConfigDict, Field, ValidationError, ValidationInfo, field_validator

from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)
from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
    validate_enum_field,
    validate_str_field,
)


class BackpackRawPosition(BaseModel):
    """
    Pydantic model for a raw position object from `/api/v1/position` or WebSocket position
    update events.

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse position payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        position_side (str): Position side ('LONG', 'SHORT').
        quantity (str): Position size.
        entry_price (str): Entry price.
        mark_price (str): Mark price.
        leverage (str): Leverage used.
        unrealized_pnl (str): Unrealized PnL.
        liquidation_price (str): Liquidation price.
        margin_type (str): Margin type ('ISOLATED', 'CROSS').
        margin (str): Margin allocated.
        imf (PositionImfFunction | None): Initial margin function parameters.
        sqrt_func (SqrtFunction | None): Square root function parameters.
        update_time (int | float | str | None): Last update time.
    """

    break_even_price: str = Field(..., alias="breakEvenPrice", max_length=64)
    entry_price: str = Field(..., alias="entryPrice", max_length=64)
    est_liquidation_price: str = Field(..., alias="estLiquidationPrice", max_length=64)
    imf: str = Field(..., alias="imf", max_length=64)
    imf_function: BackpackRawImfFunction = Field(..., alias="imfFunction")
    mark_price: str = Field(..., alias="markPrice", max_length=64)
    mmf: str = Field(..., alias="mmf", max_length=64)
    mmf_function: BackpackRawMmfFunction = Field(..., alias="mmfFunction")
    net_cost: str = Field(..., alias="netCost", max_length=64)
    net_quantity: str = Field(..., alias="netQuantity", max_length=64)
    net_exposure_quantity: str = Field(..., alias="netExposureQuantity", max_length=64)
    net_exposure_notional: str = Field(..., alias="netExposureNotional", max_length=64)
    pnl_realized: str = Field(..., alias="pnlRealized", max_length=64)
    pnl_unrealized: str = Field(..., alias="pnlUnrealized", max_length=64)
    cumulative_funding_payment: str = Field(..., alias="cumulativeFundingPayment", max_length=64)
    symbol: str = Field(..., alias="symbol", max_length=64)
    user_id: int = Field(..., alias="userId")
    position_id: str = Field(..., alias="positionId", max_length=64)
    cumulative_interest: str = Field(..., alias="cumulativeInterest", max_length=64)
    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", frozen=True, validate_assignment=True
    )

    @field_validator(
        "break_even_price",
        "entry_price",
        "est_liquidation_price",
        "imf",
        "mark_price",
        "mmf",
        "net_cost",
        "net_quantity",
        "net_exposure_quantity",
        "net_exposure_notional",
        "pnl_realized",
        "pnl_unrealized",
        "cumulative_funding_payment",
        "cumulative_interest",
        mode="before",
    )
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string representing a finite decimal.
        Ensures the string is valid UTF-8, not empty, and does not exceed 64 characters.
        Raises ValueError if the value is not a string, not parseable as a decimal, or not
        finite (NaN/inf).
        """
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("symbol", "position_id", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty UTF-8 string of max 64 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "field"
        return validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)

    @field_validator("user_id", mode="before")
    @classmethod
    def validate_user_id(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates that user_id is a non-negative integer.
        Raises ValueError if not an integer or negative.
        """
        field_name = info.field_name or "user_id"
        if isinstance(v, str) and v.isdigit():
            v_int = int(v)
        elif isinstance(v, int):
            v_int = v
        else:
            raise ValueError(
                f"{field_name}: Must be an integer or integer string, got {type(v).__name__}"
            )
        if v_int < 0:
            raise ValueError(f"{field_name}: Must be non-negative, got {v_int}")
        return v_int

    @field_validator("imf_function", "mmf_function", mode="before")
    @classmethod
    def validate_nested_function(
        cls, v: object, info: ValidationInfo
    ) -> BackpackRawImfFunction | BackpackRawMmfFunction:
        """
        Validates nested IMF/MMF function objects using their respective Raw models.
        Raises ValueError if input is not a dict or validation fails.
        """
        field_name = info.field_name or "nested_function"
        if not isinstance(v, dict):
            raise ValueError(f"{field_name}: Expected a dictionary, got {type(v).__name__}")
        try:
            if field_name == "imf_function":
                return BackpackRawImfFunction.model_validate(v)
            elif field_name == "mmf_function":
                return BackpackRawMmfFunction.model_validate(v)
            else:
                # Should not happen based on validator decoration
                raise ValueError(f"Unknown field name {field_name} for nested function validation")
        except ValidationError as e:
            raise ValueError(f"{field_name}: Validation failed for nested object: {e}") from e


class BackpackRawPositionUpdate(BaseModel):
    """
    Pydantic model for a raw position update event from the Backpack WebSocket stream
    (`positionUpdate`).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse position update events received from the exchange.

    Attributes:
        event_type (str): Event type (e.g., 'positionUpdate').
        event_time (int | str | float | None): Event time.
        symbol (str): Trading symbol.
        position_side (str): Position side ('LONG', 'SHORT').
        quantity (str): Position size.
        entry_price (str): Entry price.
        mark_price (str): Mark price.
        leverage (str): Leverage used.
        unrealized_pnl (str): Unrealized PnL.
        liquidation_price (str): Liquidation price.
        margin_type (str): Margin type ('ISOLATED', 'CROSS').
        margin (str): Margin allocated.
        imf (PositionImfFunction | None): Initial margin function parameters.
        sqrt_func (SqrtFunction | None): Square root function parameters.
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    symbol: str = Field(..., alias="s", max_length=64)
    break_event_price: str | None = Field(None, alias="b", max_length=64)
    entry_price: str | None = Field(None, alias="B", max_length=64)
    liquidation_price: str | None = Field(None, alias="l", max_length=64)
    initial_margin_fraction: str | None = Field(None, alias="f", max_length=64)
    mark_price: str | None = Field(None, alias="M", max_length=64)
    maintenance_margin_fraction: str | None = Field(None, alias="m", max_length=64)
    net_quantity: str | None = Field(None, alias="q", max_length=64)
    net_exposure_quantity: str | None = Field(None, alias="Q", max_length=64)
    net_exposure_notional: str | None = Field(None, alias="n", max_length=64)
    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", frozen=True, validate_assignment=True
    )

    @field_validator("event_type", mode="before")
    @classmethod
    def validate_event_type_enum(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "event_type"
        # Allow 'positionUpdate' specifically
        return validate_enum_field(v, allowed={"positionUpdate"}, field_name=field_name)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty UTF-8 string of max 64 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "field"
        return validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)

    @field_validator(
        "break_event_price",
        "entry_price",
        "liquidation_price",
        "initial_margin_fraction",
        "mark_price",
        "maintenance_margin_fraction",
        "net_quantity",
        "net_exposure_quantity",
        "net_exposure_notional",
        mode="before",
    )
    @classmethod
    def validate_optional_decimal_str(cls, v: object | None, info: ValidationInfo) -> str | None:
        """
        Validates that the value, if not None, is a non-empty string representing a finite decimal.
        Ensures the string is valid UTF-8, not empty, and does not exceed 64 characters.
        Raises ValueError if validation fails.
        """
        if v is None:
            return None
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("event_time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: object) -> int | float | str | None:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
        """
        field_name = "event_time"
        if v is None:
            return None  # Allow None if schema permits
        if isinstance(v, int | float):
            return v
        if isinstance(v, str):
            s = validate_str_field(v, field_name=field_name, allow_empty=False)
            try:
                if s.isdigit():
                    return int(s)
                # Attempt float parsing for potential scientific notation or decimals
                # Ensure it represents a standard epoch timestamp range
                ts_float = float(s)
                if 1e9 < ts_float < 3e12:  # Plausible range for s/ms epoch
                    return ts_float
            except ValueError:
                pass  # Ignore if not int/float

            # Check for ISO-like format as fallback
            if "T" in s or "-" in s or ":" in s:
                try:
                    # Full parsing check
                    _ = parse_datetime_utc(s, field_name)
                    return s  # Return original string if parsable
                except ValueError as e:
                    raise ValueError(
                        f"{field_name}: Invalid ISO-like timestamp string '{s}': {e}"
                    ) from e
            else:
                raise ValueError(
                    f"{field_name}: Invalid timestamp string '{s}' (not numeric or ISO-like)"
                )

        raise ValueError(
            f"{field_name}: Invalid type {type(v)}, expected int, float, or ISO string"
        )
