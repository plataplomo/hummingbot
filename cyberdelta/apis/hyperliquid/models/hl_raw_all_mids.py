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
    field_validator,
    model_validator,
)

# Import common types if needed (not directly used here yet, but good practice)
from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


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


class HyperliquidRawAllMids(RootModel[dict[str, str]]):
    """
    Strict boundary model for the response from the 'allMids' endpoint, mapping asset
    symbols to mid prices.

    This model validates the structure and content of the 'allMids' endpoint response,
    enforcing strict type and format constraints for all fields. Never use for internal
    business logic.

    Fields:
        root (Dict[str, str]): Mapping from asset symbol (e.g., 'ETH', 'BTC') to mid price
                               (as a string).
    """

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="before")
    @classmethod
    def validate_all_mids(cls, value: object) -> dict[str, str]:
        """
        Validates the root dictionary ensures keys/values are strings
        and values are finite decimals.
        """
        if not isinstance(value, dict):
            raise ValueError("__root__ must be a dictionary")

        dict_value = cast(dict[object, object], value)
        assert isinstance(dict_value, dict)

        validated_data: dict[str, str] = {}
        for symbol, price in dict_value.items():
            if not isinstance(symbol, str):
                raise ValueError(f"Dictionary key must be a string, got {type(symbol).__name__}")
            if not isinstance(price, str):
                raise ValueError(
                    f"Price value for key '{symbol}' must be a string, got {type(price).__name__}"
                )

            valid_symbol = validate_str_field(
                symbol, field_name=f"symbol[{symbol}]", max_length=64, allow_empty=False
            )
            valid_price = validate_str_field(
                price, field_name=f"price[{symbol}]", max_length=64, allow_empty=False
            )

            d = parse_decimal_value(valid_price, allow_none=False, field_name=f"price[{symbol}]")
            if d is None or not d.is_finite():
                raise ValueError(
                    f"price[{symbol}]: Value must be a parseable finite decimal string."
                )

            validated_data[valid_symbol] = valid_price

        return validated_data
