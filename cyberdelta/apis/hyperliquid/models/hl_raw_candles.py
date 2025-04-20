"""
CyberDeltaEngine: Hyperliquid API Raw Models (Candles Group)
-----------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to candlestick (candle) data.

- All models are defined locally in this file to avoid cross-file imports between model files.
- Each `HyperliquidRaw*` model mirrors the official Hyperliquid OpenAPI spec, SDK,
  or WebSocket event payloads as closely as possible.
- All fields use `Field(..., alias=...)` to match the exact key names in Hyperliquid's JSON.
- Timestamp fields are typed as `int | str | float | None` to accept ISO8601 strings, epoch
  ms/µs/seconds, or null, per the spec.
- All models use `extra=\"forbid\"` to ensure strict schema validation—any unexpected field
  will raise a validation error.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal models with type conversions
  and business logic.
- See the Hyperliquid OpenAPI spec, SDK, and docs for field details and allowed values.

**Authoritative Reference:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

Usage:
    raw = HyperliquidRawCandleSnapshot.model_validate(api_response_dict)
    # ...then transform to internal candle model

Do not use these models for internal business logic—use your core models for that.
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawCandleSnapshot(BaseModel):
    """
    Candle snapshot response from candleSnapshot.
    Fields:
        t: List of timestamps (list[int])
        o: List of open prices (list[str])
        h: List of high prices (list[str])
        low: List of low prices (list[str]), field alias 'l'
        c: List of close prices (list[str])
        v: List of volumes (list[str])
        s: Status string (str)
    """

    t: list[int] = Field(..., alias="t")
    o: list[str] = Field(..., alias="o")
    h: list[str] = Field(..., alias="h")
    low: list[str] = Field(..., alias="l")
    c: list[str] = Field(..., alias="c")
    v: list[str] = Field(..., alias="v")
    s: str = Field(..., alias="s")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawCandleSnapshotRequestPayload(BaseModel):
    """
    Request payload for 'candleSnapshot' info type.
    Fields:
        type: Must be 'candleSnapshot'
        coin: Asset symbol (str)
        interval: Interval string (e.g., '1m', '1h', '1d')
        start_time: Start timestamp (int)
        end_time: End timestamp (int)
    """

    type: str = Field("candleSnapshot", alias="type")
    coin: str = Field(..., alias="coin")
    interval: str = Field(..., alias="interval")
    start_time: int = Field(..., alias="startTime")
    end_time: int = Field(..., alias="endTime")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
