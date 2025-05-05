from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# ... (Existing models like HyperliquidRawOrder, HyperliquidRawFill remain) ...


# --- Exchange Action Response Models ---


class HyperliquidRawExchangeStatusResting(BaseModel):
    """Raw model for a 'resting' order status within an exchange response."""

    oid: int = Field(...)
    # Add other fields observed in resting order status if needed
    # Example: remainingSz: str | None = None
    model_config = ConfigDict(extra="ignore")


class HyperliquidRawExchangeStatusFilled(BaseModel):
    """Raw model for a 'filled' order status within an exchange response."""

    oid: int = Field(...)
    total_sz: str = Field(..., alias="totalSz")
    avg_px: str = Field(..., alias="avgPx")
    # Potentially add fills list if present
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class HyperliquidRawExchangeStatusObject(BaseModel):
    """Raw model for complex status objects in exchange responses."""

    resting: HyperliquidRawExchangeStatusResting | None = Field(None)
    filled: HyperliquidRawExchangeStatusFilled | None = Field(None)
    error: str | None = Field(None)
    # Potentially add other status types like 'modified', 'canceled' if they appear as objects
    model_config = ConfigDict(extra="ignore")


class HyperliquidRawExchangeResponseData(BaseModel):
    """Raw model for the 'data' part of an exchange action response."""

    type: str = Field(...)
    # Statuses can be simple strings or complex objects
    statuses: list[
        Literal["canceled", "modified", "success"] | HyperliquidRawExchangeStatusObject
    ] = Field(...)
    model_config = ConfigDict(extra="ignore")


class HyperliquidRawExchangeResponse(BaseModel):
    """Raw model for the top-level response from the /exchange endpoint."""

    status: Literal["ok"] = Field(...)
    data: HyperliquidRawExchangeResponseData | None = Field(None)
    # Sometimes 'data' might be missing or structured differently on error/simple success?
    # Making data optional and handling its absence might be safer.
    model_config = ConfigDict(extra="ignore")
