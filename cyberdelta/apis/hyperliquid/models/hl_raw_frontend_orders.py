"""
CyberDeltaEngine: Hyperliquid API Raw Models (Frontend Open Orders)
------------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'frontendOpenOrders' info endpoint.
Validates the raw structure only, enforcing type and format constraints.
Never use for internal business logic.
"""

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawDefaultString,
    RawFiniteDecimalStr,
    RawNonNegativeInt,
    RawSideStr,
    RawStrictBool,
    RawTimestampMsInt,
)


class HyperliquidRawFrontendOpenOrder(BaseModel):
    """
    Raw boundary model for a single open order with frontend-specific fields.
    """

    coin: RawDefaultString = Field(..., alias="coin", max_length=64)
    is_position_tpsl: RawStrictBool = Field(..., alias="isPositionTpsl")
    is_trigger: RawStrictBool = Field(..., alias="isTrigger")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    oid: RawNonNegativeInt = Field(..., alias="oid")
    order_type: RawDefaultString = Field(..., alias="orderType", max_length=64)
    orig_sz: RawFiniteDecimalStr = Field(..., alias="origSz")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    side: RawSideStr = Field(..., alias="side")
    sz: RawFiniteDecimalStr = Field(..., alias="sz")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    trigger_condition: RawDefaultString = Field(..., alias="triggerCondition", max_length=128)
    trigger_px: RawFiniteDecimalStr = Field(..., alias="triggerPx")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# The overall response is a list of these objects
# No separate top-level response model needed unless the API wraps the list
