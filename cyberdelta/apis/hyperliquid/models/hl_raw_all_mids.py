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

from pydantic import BaseModel, ConfigDict, Field


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


class HyperliquidRawAllMids(BaseModel):
    """
    Represents the response from the 'allMids' endpoint, mapping asset symbols to mid prices.

    This model is used to validate the structure of the 'allMids' endpoint response, which is a
    dictionary mapping asset symbols to their current mid prices as strings.

    Fields:
        __root__ (Dict[str, str]): Mapping from asset symbol (e.g., 'ETH', 'BTC') to mid price
        (as a string).
    """

    __root__: dict[str, str]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
