"""CyberDeltaEngine: Hyperliquid API Raw Models (Public Trades Group).

-----------------------------------------------------------------

This module provides strict Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to public trades.
It is a core part of CyberDeltaEngine's boundary validation layer for real-time and historical
trade data.

**Scope & Rationale:**
- Models in this file are used to validate and parse the *external* data structures returned
  by Hyperliquid's public trade endpoints, including individual trades, batch trade
  responses, and trade request payloads.
- All models enforce strict schema validation (`extra="forbid"`), ensuring that any unexpected
  or malformed fields in upstream data are immediately rejected. This is critical for
  robust, secure, and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal business models with type
  conversions and business logic.

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawPublicTrade.model_validate(api_response_dict)
    # ...then transform to internal trade model

**Note:**
Do not use these models for internal business logic—use your core models for that. These are
for boundary validation only.
"""

from typing import Annotated, Literal, cast

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    RootModel,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawFiniteDecimalStr,
    RawPositiveFiniteDecimalStr,
    RawSideStr,
    RawTimestampMsInt,
    RawTradeHashStringHL,
)
from cyberdelta.utils.parsing import validate_str_field


# --- Core Public Trade Model ---
class HyperliquidRawPublicTrade(BaseModel):
    """Strict boundary model for a public trade object as returned in recent trades endpoints.

    This model validates the structure and content of individual public trade entries,
    enforcing strict type and format constraints for all fields. Never use for internal
    business logic.

    Fields:
        coin (RawAssetString64HL): Asset symbol.
        side (RawSideStr): Side of the trade ('B' for buy, 'A' for ask/sell).
        px (RawFiniteDecimalStr): Price at which the trade occurred.
        sz (RawPositiveFiniteDecimalStr): Size of the trade.
        time (RawTimestampMsInt): Timestamp of the trade event (epoch ms).
        hash (RawTradeHashStringHL): Unique trade hash.
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    side: RawSideStr = Field(..., alias="side")
    px: RawFiniteDecimalStr = Field(..., alias="px")
    sz: RawPositiveFiniteDecimalStr = Field(..., alias="sz")
    time: RawTimestampMsInt = Field(..., alias="time")
    hash: RawTradeHashStringHL = Field(..., alias="hash")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Batch/Array Response ---
class HyperliquidRawRecentTradesResponse(RootModel[list[HyperliquidRawPublicTrade]]):
    """Strict boundary model for array of public trades from the recentTrades endpoint response.

    This model validates the structure and content of the batch response, enforcing strict
    type and format constraints for all fields. Never use for internal business logic.

    Fields:
        root (List[HyperliquidRawPublicTrade]): List of public trade objects.
    """

    root: list[HyperliquidRawPublicTrade]

    @property
    def items(self) -> list[HyperliquidRawPublicTrade]:
        """Return the validated list of public trades with full type safety.

        This is the preferred way to access the root data in Pydantic v2.

        Returns:
            list[HyperliquidRawPublicTrade]: The validated list of public trade objects.

        """
        return self.root

    model_config = ConfigDict(frozen=True)

    @field_validator("root", mode="before")
    @classmethod
    def validate_trades_list(cls, v: object, info: ValidationInfo) -> list[dict[str, object]]:
        """Ensure the root input is a list of dictionaries for public trades."""
        field_name = info.field_name or "public_trades_list"

        if not isinstance(v, list):
            raise ValueError(f"Field '{field_name}': Expected a list, got {type(v).__name__}.")

        # CAST 1: For type checker, v is already confirmed list by runtime check above
        list_of_objects = cast(list[object], v)

        validated_items: list[dict[str, object]] = []
        for item_idx, item_obj in enumerate(list_of_objects):
            if not isinstance(item_obj, dict):
                item_type = type(item_obj).__name__
                raise ValueError(
                    f"Field '{field_name}', Item {item_idx}: Expected a dictionary, "
                    f"got {item_type}.",
                )

            # CAST 2: For type checker, item_obj is already confirmed dict by runtime check above
            item_dict = cast(dict[str, object], item_obj)

            validated_items.append(item_dict)
        return validated_items


# --- Request Payload ---
class HyperliquidRawRecentTradesRequestPayload(BaseModel):
    """Strict boundary model for the request payload for the 'recentTrades' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting recent public trades for a specific asset. Enforces strict type and format
    constraints for all fields. Never use for internal business logic.

    Fields:
        type (Literal['recentTrades']): Must be 'recentTrades'.
        coin (RawAssetString64HL): Asset symbol.
    """

    type: Annotated[
        Literal["recentTrades"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32, allow_empty=False)),
    ] = Field("recentTrades", alias="type")
    coin: RawAssetString64HL = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
