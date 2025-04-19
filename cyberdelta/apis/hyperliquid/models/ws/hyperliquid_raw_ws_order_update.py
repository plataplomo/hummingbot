"""
WebSocket order update event (user channel).
Strictly validated (extra fields forbidden).
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawWsOrderUpdate(BaseModel):
    """
    WebSocket order update event (user channel).
    Fields:
        event_type: Event type (str)
        data: Event data (dict)
    """

    event_type: str = Field(..., alias="eventType")
    data: dict[str, Any] = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
