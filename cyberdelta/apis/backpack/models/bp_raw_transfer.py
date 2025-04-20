"""
Backpack API Transfer, Deposit, and Liquidation Models
-----------------------------------------------------

Strict Pydantic models for validating withdrawal, deposit, and liquidation responses from
the Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business
logic.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("id", "asset", "amount", "status", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 64:
            raise ValueError("String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("amount", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not v.strip():
            raise ValueError("Must be a non-empty string representing a decimal value")
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not dec_val.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: str | None) -> str | None:
        allowed = {"pending", "completed", "failed", "cancelled"}  # Update as per spec
        if v is None or v not in allowed:
            raise ValueError(f"Invalid status: {v}")
        return v


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
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("id", "asset", "amount", "status", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 64:
            raise ValueError("String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("amount", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not v.strip():
            raise ValueError("Must be a non-empty string representing a decimal value")
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not dec_val.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: str | None) -> str | None:
        allowed = {"pending", "completed", "failed", "cancelled"}  # Update as per spec
        if v is None or v not in allowed:
            raise ValueError(f"Invalid status: {v}")
        return v


class BackpackRawLiquidation(BaseModel):
    """
    Pydantic model for a raw liquidation event from `/api/v1/liquidations` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse liquidation event payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        price (str): Liquidation price (as string).
        quantity (str): Liquidated quantity (as string).
        side (str): Side ('buy', 'sell', etc.).
    """

    symbol: str = Field(..., alias="symbol")
    price: str = Field(..., alias="price")
    quantity: str = Field(..., alias="quantity")
    side: str = Field(..., alias="side")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("symbol", "price", "quantity", "side", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not v.strip():
            raise ValueError("Must be a non-empty string representing a decimal value")
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not dec_val.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: str | None) -> str | None:
        allowed = {"buy", "sell"}  # Update as per spec
        if v is None or v not in allowed:
            raise ValueError(f"Invalid side: {v}")
        return v
