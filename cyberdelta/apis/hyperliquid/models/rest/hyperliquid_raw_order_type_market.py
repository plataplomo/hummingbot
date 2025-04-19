"""
Market order type for orderType field.
Strictly validated (extra fields forbidden).
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawOrderTypeMarket(BaseModel):
    """
    Market order type for orderType field.
    Fields:
        market: dict (empty object)
    """

    market: dict[str, Any] = Field(..., alias="market")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
