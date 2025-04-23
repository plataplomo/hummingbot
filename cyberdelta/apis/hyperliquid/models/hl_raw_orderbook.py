"""
CyberDeltaEngine: Hyperliquid API Raw Models (Order Book Group)
--------------------------------------------------------------

This module provides strict Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to the L2 order book. It is a core
part of CyberDeltaEngine's boundary validation layer for real-time and historical order book data.

**Scope & Rationale:**
- Models in this file are used to validate and parse the *external* data structures returned by
  Hyperliquid's order book endpoints, including price levels and full L2 book snapshots.
- All models enforce strict schema validation (`extra="forbid"`), ensuring that any unexpected or
  malformed fields in upstream data are immediately rejected. This is critical for robust, secure,
  and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate
  external data at the boundary, then map to internal business models with type conversions and
  business logic.

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawL2Book.model_validate(api_response_dict)
    # ...then transform to internal order book model

**Note:**
Do not use these models for internal business logic—use your core models for that. These are for
boundary validation only.
"""

from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


# --- Price Level Submodel ---
class HyperliquidRawBookLevel(BaseModel):
    """
    Represents a single price level in the order book as returned in L2 book endpoints.

    This model is used to validate the structure of each price level entry in order book responses.

    Fields:
        px (str): Price at this level.
        sz (str): Size available at this price level.
        n (int): Number of orders at this price level.
    """

    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    n: int = Field(..., alias="n")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("px", "sz", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("n", mode="before")
    @classmethod
    def validate_n_non_negative(cls, v: object, info: ValidationInfo) -> int:
        """
        Enforce that n (number of orders at this price level) is non-negative.
        """
        if not isinstance(v, int):
            raise ValueError(f"n: Expected int, got {type(v).__name__}")
        if v < 0:
            raise ValueError("n: Number of orders must be non-negative")
        return v


# --- L2 Order Book Model ---
class HyperliquidRawL2Book(BaseModel):
    """
    Represents a full L2 order book snapshot as returned in order book endpoints.

    This model is used to validate the structure of the L2 book response, which includes the asset
    symbol, nested lists of price levels for bids and asks, and a snapshot timestamp.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        levels (List[List[HyperliquidRawBookLevel]]): Nested list of price levels [bids, asks].
        time (int): Snapshot timestamp (epoch ms).
    """

    coin: str = Field(..., alias="coin")
    levels: list[list[HyperliquidRawBookLevel]] = Field(..., alias="levels")
    time: int = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("coin", mode="before")
    @classmethod
    def validate_coin(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="coin", max_length=64)

    @field_validator("levels", mode="before")
    @classmethod
    def validate_levels_structure(
        cls, v: object, info: ValidationInfo
    ) -> list[list[HyperliquidRawBookLevel]]:
        # Type: v is expected to be a list of two lists (bids, asks), but may be Any at this point
        v_list: list[Any] = v  # type: ignore
        """
        Enforce that levels is a list of length 2 (bids, asks), and each element is a list.
        """
        # No need to check isinstance(v_list, list): v_list is always a list[Any] due to the cast above
        if len(v_list) != 2:
            raise ValueError("levels: Must be a list of two lists (bids, asks)")
        if not all(isinstance(sub, list) for sub in v_list):
            raise ValueError("levels: Each element must be a list (bids, asks)")
        # At this point, v_list is a list[list[Any]], but we expect list[list[HyperliquidRawBookLevel]]
        return cast(list[list[HyperliquidRawBookLevel]], v_list)


# --- Request Payload ---
class HyperliquidRawL2BookRequestPayload(BaseModel):
    """
    Represents the request payload for the 'l2Book' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting a full L2 order book snapshot for a specific asset.

    Fields:
        type (str): Must be 'l2Book'.
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
    """

    type: str = Field("l2Book", alias="type")
    coin: str = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("coin", mode="before")
    @classmethod
    def validate_coin(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="coin", max_length=64)
