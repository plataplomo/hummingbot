"""
Trigger spec for conditional orders.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawTriggerSpec(BaseModel):
    """
    Trigger spec for conditional orders.
    Fields:
        trigger_px: Trigger price (str)
        is_market: Is market order (bool)
        tpsl: Trigger type ('tp' or 'sl')
    """

    trigger_px: str = Field(..., alias="triggerPx")
    is_market: bool = Field(..., alias="isMarket")
    tpsl: str = Field(..., alias="tpsl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
