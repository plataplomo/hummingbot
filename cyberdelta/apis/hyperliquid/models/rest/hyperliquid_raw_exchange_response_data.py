"""
Structure within the 'data' field of a successful exchange action.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field

from .hyperliquid_raw_exchange_status_object import HyperliquidRawExchangeStatusObject


class HyperliquidRawExchangeResponseData(BaseModel):
    """
    Structure within the 'data' field of a successful exchange action.
    Fields:
        type: Type of response (str)
        statuses: List of status objects or strings
    """

    type: str = Field(..., alias="type")
    statuses: list[str | HyperliquidRawExchangeStatusObject] = Field(..., alias="statuses")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
