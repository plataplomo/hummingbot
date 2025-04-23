"""
CyberDeltaEngine: Hyperliquid API Raw Models (AllMids Group)
-----------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to the 'allMids' endpoint.
It is a core part of CyberDeltaEngine's boundary validation layer for price discovery and market data.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data structures returned by
  Hyperliquid's 'allMids' endpoint, which provides a mapping of asset symbols to mid prices.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking, and robust format validation (e.g., max length, finite decimals, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately rejected. This is critical for robust, secure, and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate external data at the boundary, then map to internal business models with type conversions and business logic.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawAllMids.model_validate(api_response_dict)
    # ...then transform to internal model
"""

from typing import Any, TypeGuard

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    field_validator,
    model_validator,
)

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


def is_dict(obj: object) -> TypeGuard[dict[str, Any]]:
    """TypeGuard to check if an object is a dictionary"""
    return isinstance(obj, dict)


def is_str_to_str_dict(obj: object) -> TypeGuard[dict[str, str]]:
    """
    TypeGuard to check if an object is a dictionary with string keys and string values.
    This is robust for both runtime and static type checking,
    and avoids unnecessary isinstance warnings.
    """
    if not isinstance(obj, dict):
        return False

    d: dict[Any, Any] = obj

    def all_str_keys_and_values(d: dict[Any, Any]) -> bool:
        for k, v in d.items():
            if not isinstance(k, str) or not isinstance(v, str):
                return False
        return True

    return all_str_keys_and_values(d)


class HyperliquidRawAllMidsRequestPayload(BaseModel):
    """
    Represents the request payload for the 'allMids' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting all mid prices for tradable assets.

    Fields:
        type (str): Must be 'allMids'.
    """

    type: str = Field("allMids", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("type", mode="before")
    @classmethod
    def validate_type(cls, v: object) -> str:
        s = validate_str_field(v, field_name="type", max_length=32)
        if s != "allMids":
            raise ValueError("type: Must be 'allMids'")
        return s


class HyperliquidRawAllMids(RootModel[dict[str, str]]):
    """
    Represents the response from the 'allMids' endpoint, mapping asset symbols to mid prices.

    This model is used to validate the structure of the 'allMids' endpoint response, which is a
    dictionary mapping asset symbols to their current mid prices as strings.

    Fields:
        __root__ (Dict[str, str]): Mapping from asset symbol (e.g., 'ETH', 'BTC') to mid price
        (as a string).
    """

    @model_validator(mode="before")
    @classmethod
    def validate_all_mids(cls, value: object) -> dict[str, str]:
        if not is_str_to_str_dict(value):
            raise ValueError("__root__ must be a dict mapping string keys to string values")

        # Validate each symbol and price
        validated_data: dict[str, str] = {}
        for symbol, price in value.items():
            valid_symbol = validate_str_field(symbol, field_name="symbol", max_length=64)
            valid_price = validate_str_field(price, field_name=f"price[{symbol}]", max_length=64)
            d = parse_decimal_value(valid_price, allow_none=False, field_name=f"price[{symbol}]")
            if d is None or not d.is_finite():
                raise ValueError(
                    f"price[{symbol}]: Value must be a finite decimal (not NaN or inf)"
                )
            validated_data[valid_symbol] = valid_price

        return validated_data
