"""Symbol metadata models for the symbol service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SymbolMetadata(BaseModel):
    """Metadata for a trading symbol."""

    symbol: str = Field(..., description="Original symbol")
    base_symbol: str = Field(..., description="Base asset symbol")
    quote_symbol: str | None = Field(default=None, description="Quote asset symbol for pairs")
    is_derivative: bool = Field(
        default=False, description="Whether this is a derivative instrument"
    )
    is_spot: bool = Field(default=False, description="Whether this is a spot trading pair")
    exchange_type: str | None = Field(default=None, description="Exchange type (spot/futures/perp)")
    source: str = Field(..., description="Source of metadata (mapper/fallback)")

    # Additional optional metadata
    tick_size: float | None = Field(default=None, description="Minimum price increment")
    lot_size: float | None = Field(default=None, description="Minimum quantity increment")
    min_notional: float | None = Field(default=None, description="Minimum order value")
    max_leverage: int | None = Field(default=None, description="Maximum leverage for derivatives")

    class Config:
        """Pydantic config."""

        str_strip_whitespace = True