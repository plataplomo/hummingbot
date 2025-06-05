"""Hyperliquid Raw User Fill Model.

This module defines Pydantic models for validating raw user fill data from
Hyperliquid's userFills endpoint, ensuring strict type validation and format
constraints for all fill-related fields.
"""

import logging

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawDefaultString,
    RawFiniteDecimalStr,
    RawNonNegativeInt,
    RawOptionalString,
    RawSideStr,
    RawStrictBool,
    RawTimestampMsInt,
    RawTxHashStr,
)

logger = logging.getLogger(__name__)


class HyperliquidRawFill(BaseModel):
    """Raw Pydantic model for a single fill record from the Hyperliquid userFills endpoint.

    Performs strict validation on all fields based on expected types and constraints.
    """

    tid: RawNonNegativeInt = Field(..., description="Transaction ID")
    oid: RawNonNegativeInt = Field(..., description="Order ID")
    coin: RawDefaultString = Field(..., description="Asset identifier", max_length=64)
    px: RawFiniteDecimalStr = Field(..., description="Fill price", max_length=64)
    sz: RawFiniteDecimalStr = Field(..., description="Fill size", max_length=64)
    start_position: RawFiniteDecimalStr = Field(
        ...,
        alias="startPosition",
        description="Start position size",
        max_length=64,
    )
    fee: RawFiniteDecimalStr = Field(..., description="Fee paid", max_length=64)
    liquidation_mark_px: RawFiniteDecimalStr | None = Field(
        None,
        alias="liquidationMarkPx",
        description="Liquidation mark price if applicable",
        max_length=64,
    )
    time: RawTimestampMsInt = Field(..., description="Timestamp (milliseconds epoch)")
    side: RawSideStr = Field(..., description="Side ('B' for Buy, 'A' for Ask/Sell)")
    dir: RawDefaultString = Field(..., description="Direction description", max_length=64)
    hash: RawTxHashStr = Field(..., description="Transaction hash")
    is_maker: RawStrictBool = Field(..., alias="isMaker")
    cloid: RawOptionalString = Field(
        None,
        description="Client order ID if provided",
        max_length=128,
    )

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
    )
