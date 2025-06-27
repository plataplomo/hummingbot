"""Backpack API Account and Balance Models.

--------------------------------------

This module defines strict Pydantic models for validating account and balance responses from the
Backpack Exchange API. These models are used for boundary validation and transformation, not for
internal business logic.

Models:
    - BackpackRawAccount: Validates account summary objects (id, email, status).
    - BackpackRawBalance: Validates asset balance objects (asset, available, total).

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Enum fields (e.g., status) are strictly validated against allowed values.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic,
ensuring robustness and security at the data ingestion boundary.
"""

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


class BackpackRawAccount(BaseModel):
    """Pydantic model for a raw account summary from `/api/v1/account` (Backpack REST API).

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
        """Validates that the id is a non-empty UTF-8 string of max 128 chars.

        Returns:
            str: Validated ID string.

        Raises:
            ValueError: If not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "id"
        return validate_str_field(v, field_name=field_name, max_length=128)

    @field_validator("email", mode="before", check_fields=False)
    @classmethod
    def validate_email_str(cls, v: object, info: ValidationInfo) -> str:
        """Validates that the email is a non-empty UTF-8 string of max 254 chars.

        Returns:
            str: Validated email string.

        Raises:
            ValueError: If not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "email"
        return validate_str_field(v, field_name=field_name, max_length=254)

    @field_validator("status", mode="before", check_fields=False)
    @classmethod
    def validate_status_string_and_enum(cls, v: object, info: ValidationInfo) -> str:
        """Validates status as a string with proper format and allowed enum values.

        Checks non-empty, max 32 chars, valid UTF-8, and in allowed enum values.

        Returns:
            str: Validated status string.

        Raises:
            ValueError: If not a string, is empty, exceeds max length, not valid UTF-8, or not in
                allowed set.
        """
        field_name = info.field_name or "status"
        allowed_values = {"active", "suspended", "pending"}
        # First, validate as string (type, non-empty, length, encoding)
        s = validate_str_field(v, field_name=field_name, max_length=32)
        # Then, validate as enum
        return validate_enum_field(s, allowed=allowed_values, field_name=field_name)


class BackpackRawBalance(BaseModel):
    """Pydantic model for a raw asset balance from `/api/v1/capital` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse balance payloads received from the exchange.

    According to the OpenAPI spec, the Balance schema contains:
    - available: Funds available for use
    - locked: Funds that are locked in open orders
    - staked: Funds that are staked

    The API returns: {"USDC": {"available": "0", "locked": "0", "staked": "0"}, ...}
    """

    available: str = Field(..., alias="available", max_length=64)
    locked: str = Field(..., alias="locked", max_length=64)
    staked: str = Field(..., alias="staked", max_length=64)
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("available", "locked", "staked", mode="before", check_fields=False)
    @classmethod
    def validate_decimal_string_format(cls, v: object, info: ValidationInfo) -> str:
        """Validates that the value is a non-empty string representing a finite decimal.

        Validates max 64 chars and ensures it's parseable as a finite decimal.

        Returns:
            str: Validated decimal string.

        Raises:
            ValueError: If not a string, not parseable as decimal, not finite, or exceeds max
                length.
        """
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s
