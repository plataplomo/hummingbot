"""
Backpack API Account and Balance Models
--------------------------------------

This module defines strict Pydantic models for validating account and balance responses from the Backpack Exchange API. These models are used for boundary validation and transformation, not for internal business logic.

Models:
    - BackpackRawAccount: Validates account summary objects (id, email, status).
    - BackpackRawBalance: Validates asset balance objects (asset, available, total).

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Enum fields (e.g., status) are strictly validated against allowed values.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic, ensuring robustness and security at the data ingestion boundary.
"""

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("id", mode="before", check_fields=False)
    @classmethod
    def validate_id_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the id is a non-empty UTF-8 string of max 128 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "id"
        return validate_str_field(v, field_name=field_name, max_length=128)

    @field_validator("email", mode="before", check_fields=False)
    @classmethod
    def validate_email_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the email is a non-empty UTF-8 string of max 254 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "email"
        return validate_str_field(v, field_name=field_name, max_length=254)

    @field_validator("status", mode="before", check_fields=False)
    @classmethod
    def validate_status_string_and_enum(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the status is a string, non-empty, max 32 chars, valid UTF-8, and in allowed enum values.
        Raises ValueError if not a string, is empty, exceeds max length, not valid UTF-8, or not in allowed set.
        """
        field_name = info.field_name or "status"
        allowed_values = {"active", "suspended", "pending"}
        # First, validate as string (type, non-empty, length, encoding)
        s = validate_str_field(v, field_name=field_name, max_length=32)
        # Then, validate as enum
        return validate_enum_field(s, allowed=allowed_values, field_name=field_name)


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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("asset", mode="before", check_fields=False)
    @classmethod
    def validate_asset_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the asset is a non-empty UTF-8 string of max 32 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "asset"
        return validate_str_field(v, field_name=field_name, max_length=32)

    @field_validator("available", "total", mode="before", check_fields=False)
    @classmethod
    def validate_decimal_string_format(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string representing a finite decimal (max 64 chars).
        Raises ValueError if not a string, not parseable as decimal, not finite, or exceeds max length.
        """
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s
