"""
Backpack API Account and Balance Models
--------------------------------------

Strict Pydantic models for validating account and balance responses from the Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business logic.
"""

from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


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
    def validate_id_str(cls, v: Any, info: ValidationInfo) -> str:
        """
        Strictly validates the 'id' field as a required string with max length 128 and valid
        UTF-8.
        """
        field_name = info.field_name or "id"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        if len(v) > 128:
            raise ValueError(f"{field_name}: String value too long (max 128 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(
                f"{field_name}: Invalid UTF-8 sequence in string '{v}': {err}"
            ) from err
        return v

    @field_validator("email", mode="before", check_fields=False)
    @classmethod
    def validate_email_str(cls, v: Any, info: ValidationInfo) -> str:
        """
        Strictly validates the 'email' field as a required string with max length 254 and valid
        UTF-8.
        """
        field_name = info.field_name or "email"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        if len(v) > 254:
            raise ValueError(f"{field_name}: String value too long (max 254 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(
                f"{field_name}: Invalid UTF-8 sequence in string '{v}': {err}"
            ) from err
        return v

    @field_validator("status", mode="before", check_fields=False)
    @classmethod
    def validate_status_string_and_enum(cls, v: Any, info: ValidationInfo) -> str:
        """
        Strictly validates the 'status' field as a required string enum with allowed values and
        valid UTF-8.
        """
        field_name = info.field_name or "status"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        allowed_values = {"active", "suspended", "pending"}
        if v not in allowed_values:
            raise ValueError(f"{field_name}: Invalid value '{v}'. Expected one of {allowed_values}")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(
                f"{field_name}: Invalid UTF-8 sequence in string '{v}': {err}"
            ) from err
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

    @field_validator("asset", mode="before", check_fields=False)
    @classmethod
    def validate_asset_str(cls, v: Any, info: ValidationInfo) -> str:
        """
        Strictly validates the 'asset' field as a required string with max length 32 and valid
        UTF-8.
        """
        field_name = info.field_name or "asset"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        if len(v) > 32:
            raise ValueError(f"{field_name}: String value too long (max 32 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(
                f"{field_name}: Invalid UTF-8 sequence in string '{v}': {err}"
            ) from err
        return v

    @field_validator("available", "total", mode="before", check_fields=False)
    @classmethod
    def validate_decimal_string_format(cls, v: Any, info: ValidationInfo) -> str:
        """
        Strictly validates decimal string fields for emptiness and finite decimal value.
        """
        field_name = info.field_name or "field"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(
                f"{field_name}: Input decimal string cannot be empty or just whitespace."
            )
        try:
            d = Decimal(v)
        except (InvalidOperation, TypeError, AttributeError) as err:
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value"
            ) from err
        if not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return v
