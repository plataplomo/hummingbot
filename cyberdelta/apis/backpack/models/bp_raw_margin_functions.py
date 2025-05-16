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

from pydantic import BaseModel, ConfigDict, Field

from .bp_common_raw_types import RawBpParsableFiniteDecimalString


class BackpackRawImfFunction(BaseModel):
    """
    Strict boundary model for the IMF (Initial Margin Fraction) function data.
    Represents the `SqrtFunction` structure used by Backpack.

    Fields:
        base (str): Base value (validated as a parsable decimal string).
        factor (str): Factor value (validated as a parsable decimal string).
    """

    base: RawBpParsableFiniteDecimalString = Field(..., alias="base")
    factor: RawBpParsableFiniteDecimalString = Field(..., alias="factor")
    # NOTE: The PositionImfFunction in the spec has a 'type' field ("sqrt"),
    # but it seems nested within the 'imfFunction' object itself.
    # We use extra='ignore' here to handle potential nesting differences.
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)


class BackpackRawMmfFunction(BaseModel):
    """
    Strict boundary model for the MMF (Maintenance Margin Fraction) function data.
    Represents the `SqrtFunction` structure used by Backpack.

    Fields:
        base (str): Base value (validated as a parsable decimal string).
        factor (str): Factor value (validated as a parsable decimal string).
    """

    base: RawBpParsableFiniteDecimalString = Field(..., alias="base")
    factor: RawBpParsableFiniteDecimalString = Field(..., alias="factor")
    # NOTE: The PositionImfFunction (also used for MMF) in the spec has a 'type' field ("sqrt").
    # We use extra='ignore' here to handle potential nesting differences.
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)
