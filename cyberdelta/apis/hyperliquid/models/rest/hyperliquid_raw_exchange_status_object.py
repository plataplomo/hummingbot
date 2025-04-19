"""
Status object for order/cancel/modify responses.
Strictly validated (extra fields forbidden).
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawExchangeStatusObject(BaseModel):
    """
    Status object for order/cancel/modify responses.
    Fields:
        resting: Resting order (dict or None)
        filled: Filled order (dict or None)
        error: Error message (str or None)
    """

    resting: dict[str, Any] | None = Field(None, alias="resting")
    filled: dict[str, Any] | None = Field(None, alias="filled")
    error: str | None = Field(None, alias="error")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
