"""
Backpack API Position Models (RAW)
----------------------------------

Strict Pydantic models for validating position responses from the
Backpack Exchange API. These models are for boundary validation only:
- 1:1 contract with the Backpack OpenAPI schema (no optionality, no business logic)
- All fields required, types and structure must match the OpenAPI spec exactly
- Used only for parsing/validating raw API responses
"""

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SqrtFunction(BaseModel):
    """
    Pydantic model for the 'SqrtFunction' used in PositionImfFunction.
    """

    base: str = Field(..., alias="base")
    factor: str = Field(..., alias="factor")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("base", "factor", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            v.encode("utf-8", "strict")
        except (AttributeError, UnicodeEncodeError) as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        try:
            Decimal(v)
        except (InvalidOperation, TypeError) as err:
            raise ValueError("Must be a string representing a decimal value") from err
        return v


class PositionImfFunction(BaseModel):
    """
    Pydantic model for the 'PositionImfFunction' (currently only supports 'sqrt').
    """

    type: str = Field(..., alias="type")  # Must be 'sqrt'
    base: str = Field(..., alias="base")
    factor: str = Field(..., alias="factor")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_enum(cls, v: str | None) -> str | None:
        allowed = {"sqrt"}
        if v is None or v not in allowed:
            raise ValueError(f"Invalid type: {v}")
        try:
            v.encode("utf-8", "strict")
        except (AttributeError, UnicodeEncodeError) as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("base", "factor", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            v.encode("utf-8", "strict")
        except (AttributeError, UnicodeEncodeError) as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        try:
            Decimal(v)
        except (InvalidOperation, TypeError) as err:
            raise ValueError("Must be a string representing a decimal value") from err
        return v


class BackpackRawPosition(BaseModel):
    """
    Strict Pydantic model for a raw open position from `/api/v1/positions` (Backpack REST API).

    This model mirrors the Backpack OpenAPI 'FuturePositionWithMargin' schema exactly.
    All fields are required and must match the API contract. No business logic or optionality.
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
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

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
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            v.encode("utf-8", "strict")
        except (AttributeError, UnicodeEncodeError) as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        try:
            Decimal(v)
        except (InvalidOperation, TypeError) as err:
            raise ValueError("Must be a string representing a decimal value") from err
        return v

    @field_validator("symbol", "position_id", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None or not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 64:
            raise ValueError("String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except (AttributeError, UnicodeEncodeError) as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v


class BackpackRawPositionUpdate(BaseModel):
    """
    Pydantic model for a raw position update event from the Backpack WebSocket stream
    (`positionUpdate`).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse position update events received from the exchange.

    Attributes:
        event_type (str): Event type (e.g., 'positionOpened', ...).
        event_time (int | str | float | None): Event time.
        symbol (str): Trading symbol.
        break_event_price (str | None): Break event price.
        entry_price (str | None): Entry price.
        liquidation_price (str | None): Estimated liquidation price.
        initial_margin_fraction (str | None): Initial margin fraction.
        mark_price (str | None): Mark price.
        maintenance_margin_fraction (str | None): Maintenance margin fraction.
        net_quantity (str | None): Net quantity.
        net_exposure_quantity (str | None): Net exposure quantity.
        net_exposure_notional (str | None): Net exposure notional.
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
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("event_type", "symbol", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None or not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 64:
            raise ValueError("String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except (AttributeError, UnicodeEncodeError) as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

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
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            v.encode("utf-8", "strict")
        except (AttributeError, UnicodeEncodeError) as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        try:
            Decimal(v)
        except (InvalidOperation, TypeError) as err:
            raise ValueError("Must be a string representing a decimal value") from err
        return v

    @field_validator("event_time", mode="before")
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
