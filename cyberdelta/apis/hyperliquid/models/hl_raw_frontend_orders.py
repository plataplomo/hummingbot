"""CyberDeltaEngine: Hyperliquid API Raw Models (Frontend Open Orders).

------------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'frontendOpenOrders' info endpoint.
Validates the raw structure only, enforcing type and format constraints.
Never use for internal business logic.
"""

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawAssetString64HL,
    RawDefaultString,
    RawFiniteDecimalStr,
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawSideStr,
    RawStrictBool,
    RawTimestampMsInt,
)


class HyperliquidRawFrontendOpenOrder(BaseModel):
    """Raw boundary model for a single open order with frontend-specific fields."""

    coin: RawAssetString64HL = Field(..., alias="coin")
    is_position_tpsl: RawStrictBool = Field(..., alias="isPositionTpsl")
    is_trigger: RawStrictBool = Field(..., alias="isTrigger")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    oid: RawNonNegativeInt = Field(..., alias="oid")
    order_type: RawDefaultString = Field(..., alias="orderType", max_length=64)
    orig_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="origSz")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    side: RawSideStr = Field(..., alias="side")
    sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="sz")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    trigger_condition: RawDefaultString = Field(..., alias="triggerCondition", max_length=128)
    trigger_px: RawNonNegativeFiniteDecimalStr = Field(..., alias="triggerPx")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# The overall response is a list of these objects
# No separate top-level response model needed unless the API wraps the list
