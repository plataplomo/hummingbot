"""
CyberDeltaEngine: Hyperliquid API Raw Models (AllMids Group)
-----------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) responses related
to the 'allMids' endpoint.
It is a core part of CyberDeltaEngine's boundary validation layer for price discovery
and market data.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Hyperliquid's 'allMids' endpoint, which provides a mapping of
  asset symbols to mid prices.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking,
  and robust format validation (e.g., max length, finite decimals, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately
  rejected. This is critical for robust, secure, and predictable operation in a
  financial system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal business models with
  type conversions and business logic.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawAllMids.model_validate(api_response_dict)
    # ...then transform to internal model
"""

from typing import Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawFiniteDecimalStr,
)
from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawAllMidsRequestPayload(BaseModel):
    """
    Strict boundary model for the request payload for the 'allMids' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting all mid prices for tradable assets. Enforces strict type and format
    constraints for all fields. Never use for internal business logic.

    Fields:
        type (str): Must be 'allMids'.
    """

    type: Literal["allMids"] = Field("allMids", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_string(cls, v: object) -> str:
        """
        Validates the 'type' field is a valid string. The Literal check handles the value.
        """
        return validate_str_field(v, field_name="type", max_length=32, allow_empty=False)


class HyperliquidRawAllMids(RootModel[dict[RawAssetString64HL, RawFiniteDecimalStr]]):
    """
    Strict boundary model for the response from the 'allMids' endpoint, mapping asset
    symbols to mid prices.

    This model validates the structure and content of the 'allMids' endpoint response,
    enforcing strict type and format constraints for all fields. Never use for internal
    business logic.

    Fields:
        root (Dict[RawAssetString64HL, RawFiniteDecimalStr]): Mapping from validated asset
                                                              symbol to validated mid price string.
    """

    root: dict[RawAssetString64HL, RawFiniteDecimalStr]
    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("root", mode="before")
    @classmethod
    def ensure_root_is_dict(cls, v: object, info: ValidationInfo) -> dict[str, object]:
        """
        Validates that the root input is a dictionary. Pydantic will handle
        key/value type validation using RawAssetString64HL and RawFiniteDecimalStr.
        """
        if not isinstance(v, dict):
            field_name = info.field_name if info.field_name else "all_mids_response"
            raise ValueError(
                f"Field '{field_name}': Expected a dictionary, got {type(v).__name__}."
            )
        return cast(dict[str, object], v)
