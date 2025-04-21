"""
Backpack API Position Models (RAW)
----------------------------------

This module defines strict Pydantic models for validating position responses from the Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business logic.

Models:
    - SqrtFunction: Validates the 'SqrtFunction' used in PositionImfFunction.
    - PositionImfFunction: Validates the 'PositionImfFunction' (currently only supports 'sqrt').
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

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


class SqrtFunction(BaseModel):
    """
    Pydantic model for a square root function parameterization used in position margin calculations.

    Attributes:
        a (str): Coefficient for the linear term.
        b (str): Coefficient for the square root term.
    """

    base: str = Field(..., alias="base")
    factor: str = Field(..., alias="factor")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("base", "factor", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string representing a finite decimal.
        Raises ValueError if not a string, not parseable as decimal, or not finite.
        """
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


class PositionImfFunction(BaseModel):
    """
    Pydantic model for a position initial margin function parameterization.

    Attributes:
        a (str): Coefficient for the linear term.
        b (str): Coefficient for the square root term.
        c (str): Constant term.
    """

    type: str = Field(..., alias="type")  # Must be 'sqrt'
    base: str = Field(..., alias="base")
    factor: str = Field(..., alias="factor")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_enum(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "type"
        s = validate_str_field(v, field_name=field_name)
        return validate_enum_field(s, allowed={"sqrt"}, field_name=field_name)

    @field_validator("base", "factor", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string representing a finite decimal.
        Raises ValueError if not a string, not parseable as decimal, or not finite.
        """
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


class BackpackRawPosition(BaseModel):
    """
    Pydantic model for a raw position object from `/api/v1/position` or WebSocket position update events.

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
    imf_function: PositionImfFunction = Field(..., alias="imfFunction")
    mark_price: str = Field(..., alias="markPrice", max_length=64)
    mmf: str = Field(..., alias="mmf", max_length=64)
    mmf_function: PositionImfFunction = Field(..., alias="mmfFunction")
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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

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
        Raises ValueError if the value is not a string, not parseable as a decimal, or not finite (NaN/inf).
        """
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
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
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("position_side", mode="before", check_fields=False)
    @classmethod
    def validate_position_side(cls, v: object) -> str:
        """
        Validates that position_side is a string and one of the allowed enum values.
        Raises ValueError if not a string or not in allowed set.
        """
        field_name = "position_side"
        s = validate_str_field(v, field_name=field_name)
        return validate_enum_field(s, allowed={"LONG", "SHORT"}, field_name=field_name)

    @field_validator("margin_type", mode="before", check_fields=False)
    @classmethod
    def validate_margin_type(cls, v: object) -> str:
        """
        Validates that margin_type is a string and one of the allowed enum values.
        Raises ValueError if not a string or not in allowed set.
        """
        field_name = "margin_type"
        s = validate_str_field(v, field_name=field_name)
        return validate_enum_field(s, allowed={"ISOLATED", "CROSS"}, field_name=field_name)

    @field_validator("update_time", mode="before", check_fields=False)
    @classmethod
    def validate_timestamp(cls, v: object) -> int | float | str | None:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
        """
        field_name = "update_time"
        if v is None:
            return None
        if isinstance(v, int | float):
            return v
        if isinstance(v, str):
            if not v.strip():
                raise ValueError("update_time: Input string cannot be empty or just whitespace.")
            if v.isdigit():
                return int(v)
            if "T" in v or "-" in v or ":" in v:
                return v
            raise ValueError(
                f"{field_name}: Invalid timestamp string '{v}' (not numeric or ISO8601)"
            )
        raise ValueError(
            f"{field_name}: Invalid type {type(v)}, expected int, float, or ISO string"
        )


class BackpackRawPositionUpdate(BaseModel):
    """
    Pydantic model for a raw position update event from the Backpack WebSocket stream (`positionUpdate`).

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
    symbol: str = Field(..., alias="s")
    break_event_price: str | None = Field(None, alias="b")
    entry_price: str | None = Field(None, alias="B")
    liquidation_price: str | None = Field(None, alias="l")
    initial_margin_fraction: str | None = Field(None, alias="f")
    mark_price: str | None = Field(None, alias="M")
    maintenance_margin_fraction: str | None = Field(None, alias="m")
    net_quantity: str | None = Field(None, alias="q")
    net_exposure_quantity: str | None = Field(None, alias="Q")
    net_exposure_notional: str | None = Field(None, alias="n")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator(
        "event_type", "symbol", "position_side", "margin_type", mode="before", check_fields=False
    )
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string.
        Raises ValueError if not a string or is empty.
        """
        field_name = info.field_name or "field"
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator(
        "break_event_price",
        "quantity",
        "entry_price",
        "mark_price",
        "leverage",
        "unrealized_pnl",
        "liquidation_price",
        "margin",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Validates that the value is a non-empty string representing a finite decimal.
        Raises ValueError if not a string, not parseable as decimal, or not finite.
        """
        if v is None:
            return None
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
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
            return None
        if isinstance(v, int | float):
            return v
        if isinstance(v, str):
            if not v.strip():
                raise ValueError("event_time: Input string cannot be empty or just whitespace.")
            if v.isdigit():
                return int(v)
            if "T" in v or "-" in v or ":" in v:
                return v
            raise ValueError(
                f"{field_name}: Invalid timestamp string '{v}' (not numeric or ISO8601)"
            )
        raise ValueError(
            f"{field_name}: Invalid type {type(v)}, expected int, float, or ISO string"
        )
