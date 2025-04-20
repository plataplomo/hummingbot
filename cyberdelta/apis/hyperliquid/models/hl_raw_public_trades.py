"""
CyberDeltaEngine: Hyperliquid API Raw Models (Public Trades Group)
-----------------------------------------------------------------

This module provides strict Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to public trades. It is a core part of CyberDeltaEngine's boundary validation layer for real-time and historical trade data.

**Scope & Rationale:**
- Models in this file are used to validate and parse the *external* data structures returned by Hyperliquid's public trade endpoints, including individual trades, batch trade responses, and trade request payloads.
- All models enforce strict schema validation (`extra="forbid"`), ensuring that any unexpected or malformed fields in upstream data are immediately rejected. This is critical for robust, secure, and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate external data at the boundary, then map to internal business models with type conversions and business logic.

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawPublicTrade.model_validate(api_response_dict)
    # ...then transform to internal trade model

**Note:**
Do not use these models for internal business logic—use your core models for that. These are for boundary validation only.
"""

from pydantic import BaseModel, ConfigDict, Field


# --- Core Public Trade Model ---
class HyperliquidRawPublicTrade(BaseModel):
    """
    Represents a public trade object as returned in recent trades endpoints.

    This model is used to validate the structure of individual public trade entries, including asset symbol, side, price, size, timestamp, and trade hash.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        side (str): Side of the trade ('B' for buy, 'A' for ask/sell).
        px (str): Price at which the trade occurred.
        sz (str): Size of the trade.
        time (int): Timestamp of the trade event (epoch ms).
        hash (str): Unique trade hash.
    """

    coin: str = Field(..., alias="coin")
    side: str = Field(..., alias="side")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Batch/Array Response ---
class HyperliquidRawRecentTradesResponse(BaseModel):
    """
    Represents an array of public trades as returned in the 'recentTrades' endpoint response.

    This model is used to validate the structure of the batch response, which is a list of HyperliquidRawPublicTrade objects.

    Fields:
        __root__ (List[HyperliquidRawPublicTrade]): List of public trade objects.
    """

    __root__: list[HyperliquidRawPublicTrade]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Request Payload ---
class HyperliquidRawRecentTradesRequestPayload(BaseModel):
    """
    Represents the request payload for the 'recentTrades' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when requesting recent public trades for a specific asset.

    Fields:
        type (str): Must be 'recentTrades'.
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
    """

    type: str = Field("recentTrades", alias="type")
    coin: str = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
