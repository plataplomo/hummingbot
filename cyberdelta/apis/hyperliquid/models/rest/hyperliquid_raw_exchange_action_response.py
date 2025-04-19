"""
Top-level response for exchange actions.
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


class HyperliquidRawExchangeActionResponse(BaseModel):
    """
    Top-level response for exchange actions.
    Fields:
        status: Status string (should be 'ok')
        data: Exchange response data (HyperliquidRawExchangeResponseData)
    """

    status: str = Field(..., alias="status")
    data: HyperliquidRawExchangeResponseData = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
