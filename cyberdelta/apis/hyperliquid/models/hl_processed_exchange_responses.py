"""
CyberDeltaEngine: Hyperliquid API Processed Models (Exchange Responses)
-----------------------------------------------------------------------

This module defines Pydantic models that represent *processed* or *interpreted*
versions of responses from the Hyperliquid Exchange API's `/exchange` endpoint.
These models are typically returned by the `HyperliquidResponseHandler` to provide
a cleaner and more specific interface than the raw response structures.

They are not "Raw API Models" as they involve interpretation of raw data, nor are
they full "Internal Domain Models" from `cyberdelta.core.models` as they remain
closely tied to the Hyperliquid API's specific `/exchange` response semantics.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawApiErrorStringHL,
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawPositiveFiniteDecimalStr,
)


class HyperliquidSuccessfulOrderStatus(BaseModel):
    """Represents a successfully processed order status from an /exchange response."""

    status_type: Literal["resting", "filled", "canceled", "canceled_str"]
    oid: RawNonNegativeInt | None = Field(default=None)
    total_sz: RawNonNegativeFiniteDecimalStr | None = Field(default=None)  # For filled orders
    avg_px: RawPositiveFiniteDecimalStr | None = Field(default=None)  # For filled orders

    model_config = ConfigDict(frozen=True, extra="forbid")


class HyperliquidErrorStatus(BaseModel):
    """Represents an error status from an /exchange response."""

    message: RawApiErrorStringHL

    model_config = ConfigDict(frozen=True, extra="forbid")
