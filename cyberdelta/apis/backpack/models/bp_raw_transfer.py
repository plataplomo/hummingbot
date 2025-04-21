"""
Backpack API Transfer, Deposit, and Liquidation Models
-----------------------------------------------------

Strict Pydantic models for validating withdrawal, deposit, and liquidation responses from
the Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business
logic.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


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

    @field_validator("id", "asset", "status", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object) -> str:
        if not isinstance(v, str):
            raise ValueError(f"Must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 64:
            raise ValueError("String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Invalid UTF-8 sequence: {err}") from err
        return v

    @field_validator("amount", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object) -> str:
        if not isinstance(v, str):
            raise ValueError(
                f"Must be a string representing a decimal value, got {type(v).__name__}"
            )
        if not v.strip():
            raise ValueError("Must be a non-empty string representing a decimal value")
        try:
            dec_val = Decimal(v.strip())
        except Exception as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not dec_val.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: object) -> str:
        allowed = {"pending", "completed", "failed", "cancelled"}
        if not isinstance(v, str):
            raise ValueError(f"Status must be a string, got {type(v).__name__}")
        if v not in allowed:
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

    @field_validator("id", "asset", "status", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object) -> str:
        if not isinstance(v, str):
            raise ValueError(f"Must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 64:
            raise ValueError("String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Invalid UTF-8 sequence: {err}") from err
        return v

    @field_validator("amount", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object) -> str:
        if not isinstance(v, str):
            raise ValueError(
                f"Must be a string representing a decimal value, got {type(v).__name__}"
            )
        if not v.strip():
            raise ValueError("Must be a non-empty string representing a decimal value")
        try:
            dec_val = Decimal(v.strip())
        except Exception as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not dec_val.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: object) -> str:
        allowed = {"pending", "completed", "failed", "cancelled"}
        if not isinstance(v, str):
            raise ValueError(f"Status must be a string, got {type(v).__name__}")
        if v not in allowed:
            raise ValueError(f"Invalid status: {v}")
        return v


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
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol(cls, v: object) -> str:
        if not isinstance(v, str):
            raise ValueError(f"symbol: Must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError("symbol: Must be a non-empty string")
        if len(v) > 64:
            raise ValueError("symbol: String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"symbol: Invalid UTF-8 sequence: {err}") from err
        return v

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo | None = None) -> str:
        field_name = getattr(info, "field_name", None) if info is not None else "field"
        if not isinstance(v, str):
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value, "
                f"got {type(v).__name__}"
            )
        if not v.strip():
            raise ValueError(
                f"{field_name}: Must be a non-empty string representing a decimal value"
            )
        if len(v) > 64:
            raise ValueError(f"{field_name}: String value too long (max 64 chars)")
        try:
            dec_val = Decimal(v.strip())
        except Exception as err:
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value"
            ) from err
        if not dec_val.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: object) -> str:
        allowed = {"buy", "sell"}
        if not isinstance(v, str):
            raise ValueError(f"side: Side must be a string, got {type(v).__name__}")
        if len(v) > 16:
            raise ValueError("side: String value too long (max 16 chars)")
        if v not in allowed:
            raise ValueError(f"side: Invalid side: {v}")
        return v
