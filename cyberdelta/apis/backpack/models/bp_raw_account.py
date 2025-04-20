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

    id: str = Field(..., alias="id", max_length=128)
    email: str = Field(..., alias="email", max_length=254)
    status: str = Field(..., alias="status", max_length=32)
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("id", mode="before", check_fields=False)
    @classmethod
    def validate_id_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 128:
            raise ValueError("String value too long (max 128 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("email", mode="before", check_fields=False)
    @classmethod
    def validate_email_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 254:
            raise ValueError("String value too long (max 254 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("status", mode="before", check_fields=False)
    @classmethod
    def validate_status_enum(cls, v: str | None) -> str | None:
        allowed = {"active", "suspended", "pending"}
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if v not in allowed:
            raise ValueError(f"Invalid status: {v}")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
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

    asset: str = Field(..., alias="asset", max_length=32)
    available: str = Field(..., alias="available", max_length=64)
    total: str = Field(..., alias="total", max_length=64)
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("asset", "available", "total", mode="before", check_fields=False)
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

    @field_validator("available", "total", mode="before", check_fields=False)
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
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
