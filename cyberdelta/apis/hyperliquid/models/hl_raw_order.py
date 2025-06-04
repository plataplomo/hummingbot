"""CyberDeltaEngine: Hyperliquid API Raw Models (Order Action Payloads & Info Requests)
---------------------------------------------------------------------------------

This module defines Pydantic models for constructing parts of the raw
Hyperliquid Exchange API request, including placing orders and querying information.
"""

from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawFiniteDecimalStr,
    RawLaxEthereumAddressStrHL,
    RawNonNegativeInt,
    RawOptionalNonEmptyString64HL,
    RawStrictBool,
    RawTifStr,
    RawTimestampMsInt,
    RawTpslStr,
)
from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawLimitOrderTypeDetails(BaseModel):
    """Details for a limit order type.
    Corresponds to ApiTifLimit inside ApiOrderTypeLimit in openapi_hl.json.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)
    tif: RawTifStr = Field(..., description="Time in Force")


class HyperliquidRawMarketOrderTypeDetails(BaseModel):
    """Details for a market order type (currently empty as per Hyperliquid spec).
    Corresponds to ApiOrderTypeMarket in openapi_hl.json.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)
    # Hyperliquid market order type is just an empty object: {"market": {}}
    # No fields, represents an empty object: {}


class HyperliquidRawOrderType(BaseModel):
    """Represents the 'orderType' field which can be a limit or market type.
    Uses a dictionary structure as per Hyperliquid's format, e.g., {"limit": {...}}
    or {"market": {}}. This model is used as a field in HyperliquidRawPlaceOrderAction.
    """

    limit: HyperliquidRawLimitOrderTypeDetails | None = Field(default=None)
    market: HyperliquidRawMarketOrderTypeDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawTriggerDetails(BaseModel):
    """Details for a trigger order (TP/SL).
    Corresponds to ApiTriggerSpec in openapi_hl.json.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    trigger_px: RawFiniteDecimalStr = Field(..., alias="triggerPx")
    is_market: RawStrictBool = Field(..., alias="isMarket")
    tpsl: RawTpslStr


class HyperliquidRawPlaceOrderAction(BaseModel):
    """Pydantic model for the Hyperliquid raw 'place order' action payload.
    Corresponds to ApiOrderSpec in openapi_hl.json.
    Ensures strict validation of the request payload before sending to the API.
    This model replaces the previous placeholder.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    asset: RawNonNegativeInt = Field(..., description="Asset index (integer)")
    is_buy: RawStrictBool = Field(..., alias="isBuy")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawFiniteDecimalStr = Field(..., alias="sz")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    order_type: HyperliquidRawOrderType = Field(..., alias="orderType")
    trigger: HyperliquidRawTriggerDetails | None = Field(default=None)
    cloid: RawOptionalNonEmptyString64HL | None = Field(
        default=None,
        description="Client Order ID (string, e.g., user-defined or 0x...)",
    )


class HyperliquidRawQueryOrderHistoryRequestPayload(BaseModel):
    """Request payload for the 'queryOrderHistory' info type.
    Timestamps are in milliseconds.
    """

    type: Annotated[
        Literal["queryOrderHistory"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32, allow_empty=False)),
    ] = Field("queryOrderHistory")
    user: RawLaxEthereumAddressStrHL = Field(..., description="User's wallet address")
    start_time: RawTimestampMsInt = Field(..., alias="startTime")
    end_time: RawTimestampMsInt = Field(..., alias="endTime")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
