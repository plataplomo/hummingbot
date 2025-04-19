"""
Time-in-force for limit orders.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawTifLimit(BaseModel):
    """
    Time-in-force for limit orders.
    Fields:
        tif: Time in force (str: 'Gtc', 'Ioc', 'Alo')
    """

    tif: str = Field(..., alias="tif")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
