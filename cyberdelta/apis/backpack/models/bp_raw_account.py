"""
Backpack API Account and Balance Models
--------------------------------------

Strict Pydantic models for validating account and balance responses from the Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business logic.
"""

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BackpackRawAccount(BaseModel):
    """
    Pydantic model for a raw account summary from `/api/v1/account` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse account payloads received from the exchange.

    Attributes:
        id (str): Unique account identifier.
        email (str): User's registered email address.
        status (str): Account status (e.g., 'active', 'suspended').
    """

    id: str = Field(..., alias="id")
    email: str = Field(..., alias="email")
    status: str = Field(..., alias="status")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("id", "email", "status", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None or not v.strip():
            raise ValueError("Must be a non-empty string")
        return v

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: str | None) -> str | None:
        allowed = {"active", "suspended", "pending"}  # Update as per spec
        if v is None or v not in allowed:
            raise ValueError(f"Invalid status: {v}")
        return v


class BackpackRawBalance(BaseModel):
    """
    Pydantic model for a raw asset balance from `/api/v1/capital` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse balance payloads received from the exchange.

    Attributes:
        asset (str): Asset/currency symbol (e.g., 'USDC', 'BTC').
        available (str): Amount available for trading (as string).
        total (str): Total balance (as string).
    """

    asset: str = Field(..., alias="asset")
    available: str = Field(..., alias="available")
    total: str = Field(..., alias="total")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("asset", "available", "total", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None or not v.strip():
            raise ValueError("Must be a non-empty string")
        return v

    @field_validator("available", "total", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            Decimal(v)
        except (InvalidOperation, TypeError) as err:
            raise ValueError("Must be a string representing a decimal value") from err
        return v
