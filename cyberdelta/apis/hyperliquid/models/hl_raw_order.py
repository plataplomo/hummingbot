"""
CyberDeltaEngine: Hyperliquid API Raw Models (Order Action Payloads)
-------------------------------------------------------------------

This module defines Pydantic models for constructing parts of the raw
Hyperliquid Exchange API request when placing orders, specifically the
`trigger` object and the `orderType` object within an order action.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawOrderTriggerDetails(BaseModel):
    """
    Pydantic model for the 'trigger' object in a Hyperliquid order action.

    Attributes:
        trigger_px (str): The trigger price.
        is_market (bool): True if the triggered order is a market order, False for limit.
        tpsl (Literal["tp", "sl"]): Whether this is a take-profit or stop-loss trigger.
    """

    trigger_px: str = Field(..., alias="triggerPx")
    is_market: bool = Field(..., alias="isMarket")
    tpsl: Literal["tp", "sl"]

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawLimitOrderTypeDetails(BaseModel):
    """
    Details for a limit order type.
    """

    tif: Literal["Gtc", "Ioc", "Alo"]  # Add other TIFs if HL supports more

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawMarketOrderTypeDetails(BaseModel):
    """
    Details for a market order type (currently empty as per Hyperliquid spec).
    """

    # Hyperliquid market order type is just an empty object: {"market": {}}
    pass

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawOrderTypeUnion(BaseModel):
    """
    Represents the 'orderType' field which can be a limit or market type.
    Uses a dictionary structure as per Hyperliquid's format, e.g., {"limit": {...}} or {"market": {}}.
    """

    limit: HyperliquidRawLimitOrderTypeDetails | None = None
    market: HyperliquidRawMarketOrderTypeDetails | None = None

    # Validate that exactly one of limit or market is set.
    # This would typically be done with a model_validator, but for constructing the payload,
    # we ensure this in the calling code.
    model_config = ConfigDict(extra="forbid", frozen=True)


# Placeholder for the full HyperliquidRawOrderAction if needed for other contexts,
# for now, place_order will construct the dict directly using these components.
