"""Symbol configuration models.

Separated from config_models.py to avoid circular imports with smart_symbol_generator.
"""

from __future__ import annotations

from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.config.models.config_types import NonEmptyConfigString


class InternalSymbolConfig(BaseModel):
    """Configuration for internal symbol representation."""

    model_config = ConfigDict(extra="forbid")

    value: NonEmptyConfigString = Field(..., description="Internal symbol identifier")
    base_asset: NonEmptyConfigString = Field(..., description="Base asset symbol")
    quote_asset: NonEmptyConfigString | None = Field(
        None, description="Quote asset symbol (null for perpetuals)"
    )
    market_type: Literal["SPOT", "PERP"] = Field(..., description="Market type (SPOT, PERP, etc.)")


class ExchangeSymbolConfig(BaseModel):
    """Configuration for exchange-specific symbol representation."""

    model_config = ConfigDict(extra="forbid")

    value: NonEmptyConfigString = Field(..., description="Exchange-specific symbol")
    exchange_id: NonEmptyConfigString = Field(..., description="Exchange identifier")
    asset_index: int | None = Field(
        None, description="Asset index for exchange (e.g., Hyperliquid)"
    )
    symbol_id: str | None = Field(None, description="Additional symbol identifier")


class UnifiedSymbolConfig(BaseModel):
    """Configuration for unified symbol with multiple exchange mappings."""

    model_config = ConfigDict(extra="forbid")

    internal: InternalSymbolConfig = Field(..., description="Internal symbol configuration")
    exchange_mappings: dict[str, ExchangeSymbolConfig] = Field(
        default_factory=dict, description="Exchange-specific symbol mappings"
    )

    @field_validator("exchange_mappings", mode="before")
    @classmethod
    def validate_exchange_mappings(cls, v: dict[str, Any]) -> dict[str, ExchangeSymbolConfig]:
        """Validate and convert exchange mappings.
        
        Ensures exchange mappings are properly structured for cross-exchange
        symbol resolution. This is critical for maintaining consistent symbol
        references across different exchange APIs in the arbitrage system.
        
        Returns:
            dict[str, ExchangeSymbolConfig]: Validated and converted exchange mappings
            
        Raises:
            TypeError: If mapping value is neither dict nor ExchangeSymbolConfig
        """
        # Pydantic already validates v is dict[str, Any], so isinstance check not needed

        validated_mappings: dict[str, ExchangeSymbolConfig] = {}
        for exchange_id, mapping in v.items():
            if isinstance(mapping, dict):
                # Ensure exchange_id is set in the mapping
                # Extract fields explicitly for proper typing
                mapping_typed = cast(dict[str, Any], mapping)
                validated_mappings[exchange_id] = ExchangeSymbolConfig(
                    value=mapping_typed.get("value", ""),
                    exchange_id=exchange_id,
                    asset_index=mapping_typed.get("asset_index"),
                    symbol_id=mapping_typed.get("symbol_id"),
                )
            elif isinstance(mapping, ExchangeSymbolConfig):
                validated_mappings[exchange_id] = mapping
            else:
                msg = f"Invalid type for {exchange_id}: {type(mapping).__name__}"
                raise TypeError(msg)

        return validated_mappings
