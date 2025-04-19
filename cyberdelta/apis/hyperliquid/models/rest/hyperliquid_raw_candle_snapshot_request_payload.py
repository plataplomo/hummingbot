"""
Request payload for 'candleSnapshot' info type.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawCandleSnapshotRequestPayload(BaseModel):
    """
    Request payload for 'candleSnapshot' info type.
    Fields:
        type: Must be 'candleSnapshot'
        coin: Asset symbol (str)
        interval: Interval string (e.g., '1m', '1h', '1d')
        start_time: Start timestamp (int)
        end_time: End timestamp (int)
    """

    type: str = Field("candleSnapshot", alias="type")
    coin: str = Field(..., alias="coin")
    interval: str = Field(..., alias="interval")
    start_time: int = Field(..., alias="startTime")
    end_time: int = Field(..., alias="endTime")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
