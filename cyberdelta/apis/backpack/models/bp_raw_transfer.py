"""
Backpack API Transfer (Deposit/Withdrawal) Models
------------------------------------------------

Defines strict Pydantic models for validating deposit and withdrawal responses from
the Backpack Exchange API. These models are used for boundary validation and
transformation, not for internal business logic.

Models:
    - BackpackRawDeposit: Validates deposit objects.
    - BackpackRawWithdrawal: Validates withdrawal objects.

Validation Pattern:
    - Strict type, format, and constraint checks on all fields.
    - `extra='forbid'` to reject unknown fields.

These models act as a strict shield between external API data and internal business
logic, ensuring robustness and security at the data ingestion boundary.
"""

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


class BackpackRawWithdrawal(BaseModel):
    """
    Pydantic model for a raw withdrawal object from `/api/v1/withdrawals` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse withdrawal payloads received from the exchange.

    Attributes:
        id (str): Withdrawal ID.
        asset (str): Asset symbol.
        amount (str): Withdrawal amount (as string).
        status (str): Withdrawal status (e.g., 'pending', 'completed').
    """

    id: str = Field(..., alias="id", max_length=64)
    asset: str = Field(..., alias="asset", max_length=32)
    amount: str = Field(..., alias="amount", max_length=64)
    status: str = Field(..., alias="status", max_length=32)
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("id", "asset", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("amount", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "amount"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: object, info: ValidationInfo) -> str:
        allowed = {"pending", "completed", "failed", "cancelled"}
        return validate_enum_field(v, allowed=allowed, field_name="status")


class BackpackRawDeposit(BaseModel):
    """
    Pydantic model for a raw deposit object from `/api/v1/deposits` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse deposit payloads received from the exchange.

    Attributes:
        id (str): Deposit ID.
        asset (str): Asset symbol.
        amount (str): Deposit amount (as string).
        status (str): Deposit status (e.g., 'pending', 'completed').
    """

    id: str = Field(..., alias="id", max_length=64)
    asset: str = Field(..., alias="asset", max_length=32)
    amount: str = Field(..., alias="amount", max_length=64)
    status: str = Field(..., alias="status", max_length=32)
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("id", "asset", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("amount", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "amount"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: object, info: ValidationInfo) -> str:
        allowed = {"pending", "completed", "failed", "cancelled"}
        return validate_enum_field(v, allowed=allowed, field_name="status")


class BackpackRawLiquidation(BaseModel):
    """
    Pydantic model for a raw liquidation event from `/api/v1/liquidations` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse liquidation event payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol. (max_length=64, per OpenAPI spec)
        price (str): Liquidation price (as string).
        quantity (str): Liquidated quantity (as string).
        side (str): Side ('buy', 'sell', etc.).
    """

    symbol: str = Field(..., alias="symbol", max_length=64)
    price: str = Field(..., alias="price", max_length=64)
    quantity: str = Field(..., alias="quantity", max_length=64)
    side: str = Field(..., alias="side", max_length=16)
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="symbol", max_length=64)

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: object, info: ValidationInfo) -> str:
        allowed = {"buy", "sell"}
        return validate_enum_field(v, allowed=allowed, field_name="side")
