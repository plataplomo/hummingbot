"""Symbol configuration models - Clean architecture."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.config.models.config_types import NonEmptyConfigString


class SymbolMetadataConfig(BaseModel):
    """Configuration for symbol metadata."""

    model_config = ConfigDict(extra="forbid")

    # Common fields that might be in any exchange's metadata
    asset_index: int | None = Field(None, description="Asset index (Hyperliquid)")
    symbol_id: str | None = Field(None, description="Symbol ID (Backpack)")


class SymbolMappingConfig(BaseModel):
    """Configuration for a single symbol on an exchange."""

    model_config = ConfigDict(extra="forbid")

    value: NonEmptyConfigString = Field(..., description="Symbol value on this exchange")
    exchange: NonEmptyConfigString = Field(..., description="Exchange identifier")
    metadata: SymbolMetadataConfig = Field(
        default_factory=lambda: SymbolMetadataConfig(asset_index=None, symbol_id=None),
        description="Exchange-specific metadata",
    )


class SymbolGroupConfig(BaseModel):
    """Configuration for a group of equivalent symbols across exchanges.

    Each group represents the same instrument across different exchanges.
    """

    model_config = ConfigDict(extra="forbid")

    # Canonical representation for equivalence tracking
    canonical: NonEmptyConfigString = Field(
        ..., description="Canonical symbol representation (e.g., BTC_USD)"
    )

    # Parsed components for reference
    base_asset: NonEmptyConfigString = Field(..., description="Base asset")
    quote_asset: NonEmptyConfigString | None = Field(
        None, description="Quote asset (None for single assets)"
    )
    market_type: Literal["SPOT", "PERP"] = Field("PERP", description="Market type")

    # Exchange mappings
    mappings: list[SymbolMappingConfig] = Field(
        ..., description="Symbol mappings for each exchange", min_length=1
    )

    @field_validator("mappings", mode="after")
    @classmethod
    def validate_unique_exchanges(
        cls, mappings: list[SymbolMappingConfig]
    ) -> list[SymbolMappingConfig]:
        """Ensure each exchange appears only once.
        
        Returns:
            Validated list of SymbolMappingConfig objects.
            
        Raises:
            ValueError: If duplicate exchanges are found in mappings.
        """
        exchanges = [m.exchange for m in mappings]
        if len(exchanges) != len(set(exchanges)):
            msg = "Duplicate exchange in mappings"
            raise ValueError(msg)
        return mappings
