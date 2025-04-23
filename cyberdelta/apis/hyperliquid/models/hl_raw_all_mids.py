# pyright: reportUnknownMemberType=false
# NOTE: The default value for __root__ (PydanticUndefined) is required for Pydantic v2 RootModel
# compatibility, but will trigger a type checker warning because its type is 'object', not 'dict[str, str]'.
# This is a known, accepted exception and is safe due to the runtime check below.
# See: https://docs.pydantic.dev/latest/concepts/models/#rootmodel

"""
CyberDeltaEngine: Hyperliquid API Raw Models (AllMids Group)
-----------------------------------------------------------

This module provides strict Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to the 'allMids' endpoint.
It is a core part of CyberDeltaEngine's boundary validation layer for price discovery
and market data.

**Scope & Rationale:**
- Models in this file are used to validate and parse the *external* data structures returned by
  Hyperliquid's 'allMids' endpoint, which provides a mapping of asset symbols to mid prices.
- All models enforce strict schema validation (`extra="forbid"`), ensuring that any unexpected or
  malformed fields in upstream data are immediately rejected. This is critical for robust, secure,
  and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate
  external data at the boundary, then map to internal business models with type conversions and
  business logic.

**References:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawAllMids.model_validate(api_response_dict)
    # ...then transform to internal model

**Note:**
Do not use these models for internal business logic—use your core models for that.
These are for boundary validation only.
"""

from typing import Any, TypeGuard

from pydantic import BaseModel, ConfigDict, Field, PydanticUndefined, RootModel, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


def is_dict(obj: object) -> TypeGuard[dict[str, Any]]:
    """TypeGuard to check if an object is a dictionary"""
    return isinstance(obj, dict)


def is_str_to_str_dict(obj: object) -> TypeGuard[dict[str, str]]:
    """
    TypeGuard to check if an object is a dictionary with string keys and string values
    """
    if not is_dict(obj):
        return False

    # Check if all keys are strings and all values are strings
    for k, v in obj.items():
        if not isinstance(k, str) or not isinstance(v, str):
            return False

    return True


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

    # Override __init__ to provide better validation while maintaining type safety
    def __init__(self, __root__: Any = PydanticUndefined, **data: Any) -> None:
        """
        Custom __init__ to perform strict validation on the root dict before model initialization.

        Uses explicit type checking with TypeGuards to ensure both runtime and type safety.

        Args:
            __root__: The root dictionary mapping asset symbols to mid prices (as strings).
        Raises:
            TypeError: If __root__ is not provided.
            ValueError: If any symbol or price is invalid or not a finite decimal.
        """
        # Check if __root__ is provided
        if __root__ is PydanticUndefined:
            raise TypeError("__root__ argument is required for HyperliquidRawAllMids")

        # Validate it's a dictionary with string keys and values
        if not is_str_to_str_dict(__root__):
            raise ValueError("__root__ must be a dict mapping string keys to string values")

        # Validate each symbol and price
        validated_data: dict[str, str] = {}
        for symbol, price in __root__.items():
            # Validate symbol
            valid_symbol = validate_str_field(symbol, field_name="symbol", max_length=64)

            # Validate price
            valid_price = validate_str_field(price, field_name=f"price[{symbol}]", max_length=64)

            # Check price is a valid decimal
            d = parse_decimal_value(valid_price, allow_none=False, field_name=f"price[{symbol}]")
            if d is None or not d.is_finite():
                raise ValueError(
                    f"price[{symbol}]: Value must be a finite decimal (not NaN or inf)"
                )

            # Add to validated data
            validated_data[valid_symbol] = valid_price

        # Initialize with validated data - at this point, we know it's Dict[str, str]
        super().__init__(root=validated_data)
