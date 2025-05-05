"""
CyberDeltaEngine: Backpack API Raw Models (Margin Functions)
------------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of Backpack's margin function components, specifically the `imfFunction` and
`mmfFunction` objects returned in position data.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Backpack's position-related endpoints.
- All models enforce strict schema validation (`extra="ignore"` as they are nested),
  strict type checking, and robust format validation (e.g., finite decimals, max length).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately
  rejected. This is critical for robust, secure, and predictable operation in a
  financial system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal business models.
- **Never use these models for internal business logic.**

**References:**
- Reverse-engineered OpenAPI spec: `openapi_backpack.json` (look for `SqrtFunction`)
"""

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


class BackpackRawImfFunction(BaseModel):
    """
    Strict boundary model for the IMF (Initial Margin Fraction) function data.
    Represents the `SqrtFunction` structure used by Backpack.

    Fields:
        base (str): Base value of the sqrt function as a decimal string.
        factor (str): Factor value of the sqrt function as a decimal string.
    """

    base: str = Field(..., alias="base")
    factor: str = Field(..., alias="factor")
    # NOTE: The PositionImfFunction in the spec has a 'type' field ("sqrt"),
    # but it seems nested within the 'imfFunction' object itself.
    # We use extra='ignore' here to handle potential nesting differences.
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    @field_validator("base", "factor", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates a required decimal string field (base or factor).
        Ensures it is a non-empty, valid finite decimal string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated decimal string.
        Raises:
            ValueError: If the input is not a valid decimal string.
        """
        field_name = info.field_name or "field"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            # DEFENSIVE CHECK: Ensure parse_decimal_value returned non-None Decimal.
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e


class BackpackRawMmfFunction(BaseModel):
    """
    Strict boundary model for the MMF (Maintenance Margin Fraction) function data.
    Represents the `SqrtFunction` structure used by Backpack.

    Fields:
        base (str): Base value of the sqrt function as a decimal string.
        factor (str): Factor value of the sqrt function as a decimal string.
    """

    base: str = Field(..., alias="base")
    factor: str = Field(..., alias="factor")
    # NOTE: The PositionImfFunction (also used for MMF) in the spec has a 'type' field ("sqrt").
    # We use extra='ignore' here to handle potential nesting differences.
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    @field_validator("base", "factor", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates a required decimal string field (base or factor).
        Ensures it is a non-empty, valid finite decimal string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated decimal string.
        Raises:
            ValueError: If the input is not a valid decimal string.
        """
        field_name = info.field_name or "field"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            # DEFENSIVE CHECK: Ensure parse_decimal_value returned non-None Decimal.
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e
