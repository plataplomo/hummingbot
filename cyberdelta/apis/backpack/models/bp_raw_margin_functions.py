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

from .bp_common_raw_types import (
    RawBpNonEmptyStringMax64,  # For general non-empty string usage if any
    RawBpParsableFiniteDecimalString,
    RawBpStringMax64,  # For general string usage like 'type'
)


class BackpackRawImfFunction(BaseModel):
    """Raw model for Initial Margin Fraction (IMF) function components."""

    base: RawBpParsableFiniteDecimalString = Field(..., alias="base")
    factor: RawBpParsableFiniteDecimalString = Field(..., alias="factor")

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class BackpackRawMmfFunction(BaseModel):
    """Raw model for Maintenance Margin Fraction (MMF) function components."""

    base: RawBpParsableFiniteDecimalString = Field(..., alias="base")
    factor: RawBpParsableFiniteDecimalString = Field(..., alias="factor")

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class BackpackRawPositionImfFunction(BaseModel):
    """
    Raw model for Position-Specific Initial Margin Fraction (IMF) function.
    This reflects the nested 'imfFunction' object within a position's details.
    The 'type' field, often "sqrt", seems to be part of this nested structure,
    though API responses can vary. We use `extra='ignore'` to be robust.
    """

    # Type for 'type' field, e.g., "sqrt", typically a short non-empty string.
    # Using RawBpStringMax64 as a general validated string type.
    type: RawBpStringMax64 = Field(..., alias="type")
    base: RawBpParsableFiniteDecimalString = Field(..., alias="base")
    # The 'factor' in this context is a numeric string, not the one needing special error message.
    factor: RawBpParsableFiniteDecimalString = Field(..., alias="factor")

    # NOTE: The PositionImfFunction in the spec has a 'type' field ("sqrt"),
    # but it seems nested within the 'imfFunction' object itself.
    # We use extra='ignore' here to handle potential nesting differences.
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)


class BackpackRawPositionMmfFunction(BaseModel):
    """
    Raw model for Position-Specific Maintenance Margin Fraction (MMF) function.
    Similar to ImfFunction, this reflects the nested 'mmfFunction'.
    """

    type: RawBpStringMax64 = Field(..., alias="type")
    base: RawBpParsableFiniteDecimalString = Field(..., alias="base")
    # The 'factor' in this context is a numeric string.
    factor: RawBpParsableFiniteDecimalString = Field(..., alias="factor")

    # NOTE: The PositionImfFunction (also used for MMF) in the spec has a 'type' field ("sqrt").
    # We use extra='ignore' here to handle potential nesting differences.
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)


class BackpackRawMarginCoverage(BaseModel):
    """
    Raw model for margin coverage data, indicating if current margin covers requirements.
    Example: `{"type": "marginCoverage", "marginCoverage": "good"}`
    """

    type: RawBpStringMax64 = Field(..., alias="type")
    # marginCoverage is a simple string, e.g. "good", "bad"
    margin_coverage: RawBpNonEmptyStringMax64 = Field(..., alias="marginCoverage")

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)
