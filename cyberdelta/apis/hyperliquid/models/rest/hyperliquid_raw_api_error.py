"""
Raw error response from Hyperliquid API.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawApiError(BaseModel):
    """
    Raw error response from Hyperliquid API.
    Fields:
        error: Error message string
    Strictly validated (extra fields forbidden).
    """

    error: str = Field(..., alias="error")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
