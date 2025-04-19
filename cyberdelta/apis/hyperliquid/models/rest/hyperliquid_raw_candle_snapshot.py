"""
Candle snapshot response from candleSnapshot.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawCandleSnapshot(BaseModel):
    """
    Candle snapshot response from candleSnapshot.
    Fields:
        t: List of timestamps (list[int])
        o: List of open prices (list[str])
        h: List of high prices (list[str])
        low: List of low prices (list[str]), field alias 'l'
        c: List of close prices (list[str])
        v: List of volumes (list[str])
        s: Status string (str)
    """

    t: list[int] = Field(..., alias="t")
    o: list[str] = Field(..., alias="o")
    h: list[str] = Field(..., alias="h")
    low: list[str] = Field(..., alias="l")
    c: list[str] = Field(..., alias="c")
    v: list[str] = Field(..., alias="v")
    s: str = Field(..., alias="s")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
