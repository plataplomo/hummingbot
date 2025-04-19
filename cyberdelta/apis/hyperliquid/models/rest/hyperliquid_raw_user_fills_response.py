"""
Array of user fills from userFills response.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict

from .hyperliquid_raw_user_fill import HyperliquidRawUserFill


class HyperliquidRawUserFillsResponse(BaseModel):
    """
    Array of user fills from userFills response.
    Fields:
        __root__: List of HyperliquidRawUserFill
    """

    __root__: list[HyperliquidRawUserFill]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
