"""
Margin summary for user state.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawMarginSummary(BaseModel):
    """
    Margin summary for user state.
    Fields:
        account_value: Account value (str)
        total_margin_used: Total margin used (str)
        total_ntl_pos: Total notional position (str)
        total_raw_usd: Total raw USD (str)
    """

    account_value: str = Field(..., alias="accountValue")
    total_margin_used: str = Field(..., alias="totalMarginUsed")
    total_ntl_pos: str = Field(..., alias="totalNtlPos")
    total_raw_usd: str = Field(..., alias="totalRawUsd")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
