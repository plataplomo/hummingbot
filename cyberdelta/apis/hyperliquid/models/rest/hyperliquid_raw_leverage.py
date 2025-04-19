"""
Leverage settings for a position.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawLeverage(BaseModel):
    """
    Leverage settings for a position.
    Fields:
        type: Leverage type ('cross' or 'isolated')
        value: Leverage value (int)
    """

    type: str = Field(..., alias="type")
    value: int = Field(..., alias="value")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
