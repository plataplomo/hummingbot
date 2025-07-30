# Step 4: Update Config Models and Loader

## Overview
Update the config models to work with the new single-model architecture while maintaining full type safety with Pydantic.

## Current State
- **symbol_configs.py**: Has InternalSymbolConfig, ExchangeSymbolConfig, UnifiedSymbolConfig (3-model system)
- **config_loader.py**: Loads UnifiedSymbol from config
- **Issues**: Config models match the old 3-model architecture

## Implementation

### 4.1 Update symbol_configs.py
**File**: `cyberdelta/config/models/symbol_configs.py`

Replace entire file with:
```python
"""Symbol configuration models - Clean architecture."""

from __future__ import annotations

from typing import Any, Literal

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
        default_factory=SymbolMetadataConfig,
        description="Exchange-specific metadata"
    )


class SymbolGroupConfig(BaseModel):
    """Configuration for a group of equivalent symbols across exchanges.

    Each group represents the same instrument across different exchanges.
    """

    model_config = ConfigDict(extra="forbid")

    # Canonical representation for equivalence tracking
    canonical: NonEmptyConfigString = Field(
        ...,
        description="Canonical symbol representation (e.g., BTC_USD)"
    )

    # Parsed components for reference
    base_asset: NonEmptyConfigString = Field(..., description="Base asset")
    quote_asset: NonEmptyConfigString | None = Field(
        None,
        description="Quote asset (None for single assets)"
    )
    market_type: Literal["SPOT", "PERP"] = Field(
        "PERP",
        description="Market type"
    )

    # Exchange mappings
    mappings: list[SymbolMappingConfig] = Field(
        ...,
        description="Symbol mappings for each exchange",
        min_length=1
    )

    @field_validator("mappings", mode="after")
    @classmethod
    def validate_unique_exchanges(cls, mappings: list[SymbolMappingConfig]) -> list[SymbolMappingConfig]:
        """Ensure each exchange appears only once."""
        exchanges = [m.exchange for m in mappings]
        if len(exchanges) != len(set(exchanges)):
            raise ValueError("Duplicate exchange in mappings")
        return mappings


# Backward compatibility alias
UnifiedSymbolConfig = SymbolGroupConfig
```

### 4.2 Update config_loader.py
**File**: `cyberdelta/core/symbols/config_loader.py`

Replace entire file with:
```python
"""Symbol Configuration Loader."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.config import get_app_settings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from .global_service import get_symbol_service

if TYPE_CHECKING:
    from cyberdelta.config.models.symbol_configs import SymbolGroupConfig, SymbolMappingConfig

logger = get_logger(__name__)


class ConfigSymbolLoader:
    """Loads symbols from configuration."""

    def __init__(self):
        """Initialize with global symbol service."""
        self.service = get_symbol_service()
        self._loaded_count = 0

    def load_symbols_from_config(self) -> int:
        """Load all symbol groups from configuration.

        Returns:
            Number of symbols loaded
        """
        app_settings = get_app_settings()

        # Handle both old and new config attribute names
        symbol_groups = getattr(app_settings, 'symbol_groups', None)
        if symbol_groups is None:
            # Fallback to unified_symbols for compatibility
            symbol_groups = getattr(app_settings, 'unified_symbols', [])

        logger.info(
            "loading_symbols_from_config",
            total_groups=len(symbol_groups)
        )

        for symbol_group in symbol_groups:
            try:
                self._load_symbol_group(symbol_group)
            except Exception as e:
                logger.exception(
                    "failed_to_load_symbol_group",
                    canonical=symbol_group.canonical,
                    error=str(e)
                )
                continue

        logger.info(
            "symbols_loaded",
            loaded_count=self._loaded_count,
            total_groups=len(symbol_groups)
        )

        return self._loaded_count

    def _load_symbol_group(self, symbol_group: SymbolGroupConfig) -> None:
        """Load a symbol group (equivalent symbols across exchanges).

        Args:
            symbol_group: Symbol group configuration
        """
        # Process each exchange mapping
        for mapping in symbol_group.mappings:
            try:
                self._load_symbol_mapping(mapping, symbol_group)
            except Exception as e:
                logger.warning(
                    "failed_to_load_symbol_mapping",
                    exchange=mapping.exchange,
                    value=mapping.value,
                    error=str(e)
                )
                continue

    def _load_symbol_mapping(
        self,
        mapping: SymbolMappingConfig,
        group: SymbolGroupConfig
    ) -> None:
        """Load a single symbol from a mapping.

        Args:
            mapping: Symbol mapping config
            group: Parent symbol group
        """
        # Map exchange string to enum
        try:
            exchange = ExchangeName(mapping.exchange.lower())
        except ValueError:
            logger.warning(
                "unknown_exchange",
                exchange=mapping.exchange,
                symbol_value=mapping.value
            )
            return

        # Build metadata kwargs
        metadata_kwargs = self._build_metadata_kwargs(exchange, mapping.metadata)

        # Create symbol
        symbol = self.service.create_symbol(
            mapping.value,
            exchange,
            **metadata_kwargs
        )

        # Register for equivalence tracking
        self.service.register_symbol(symbol)
        self._loaded_count += 1

        logger.debug(
            "symbol_loaded",
            value=symbol.value,
            exchange=symbol.exchange.value,
            canonical=group.canonical,
            base_asset=group.base_asset,
            metadata=metadata_kwargs
        )

    def _build_metadata_kwargs(
        self,
        exchange: ExchangeName,
        metadata: SymbolMetadataConfig
    ) -> dict[str, Any]:
        """Build metadata kwargs for symbol creation.

        Args:
            exchange: Exchange enum
            metadata: Metadata config

        Returns:
            Kwargs for symbol metadata
        """
        kwargs = {}

        # Hyperliquid metadata
        if exchange == ExchangeName.HYPERLIQUID:
            if metadata.asset_index is not None:
                kwargs['asset_index'] = metadata.asset_index

        # Backpack metadata
        elif exchange == ExchangeName.BACKPACK:
            if metadata.symbol_id is not None:
                try:
                    kwargs['symbol_id'] = int(metadata.symbol_id)
                except ValueError:
                    raise ValueError(f"Invalid symbol_id: {metadata.symbol_id}")
            else:
                # Backpack requires symbol_id
                raise ValueError("Backpack symbols require symbol_id")

        return kwargs


def load_symbols_from_config() -> int:
    """Load all symbols from config.

    Returns:
        Number of symbols loaded
    """
    loader = ConfigSymbolLoader()
    return loader.load_symbols_from_config()
```

### 4.3 Create Migration Helper
**File**: `cyberdelta/config/models/symbol_config_migration.py`

```python
"""Migration utilities for symbol config format."""

from typing import Any
from cyberdelta.config.models.symbol_configs import (
    SymbolGroupConfig,
    SymbolMappingConfig,
    SymbolMetadataConfig,
)

def migrate_unified_to_group(unified_config: dict[str, Any]) -> dict[str, Any]:
    """Migrate old UnifiedSymbolConfig format to SymbolGroupConfig.

    This allows existing config files to work with new architecture.
    """
    # Extract internal config
    internal = unified_config.get('internal', {})

    # Build group config
    group_config = {
        'canonical': f"{internal.get('base_asset', '')}_{internal.get('quote_asset', 'USD')}"
            if internal.get('quote_asset') else internal.get('value', ''),
        'base_asset': internal.get('base_asset', ''),
        'quote_asset': internal.get('quote_asset'),
        'market_type': internal.get('market_type', 'PERP'),
        'mappings': []
    }

    # Convert exchange mappings
    for exchange_id, mapping in unified_config.get('exchange_mappings', {}).items():
        metadata = SymbolMetadataConfig(
            asset_index=mapping.get('asset_index'),
            symbol_id=mapping.get('symbol_id')
        )

        group_config['mappings'].append({
            'value': mapping.get('value', ''),
            'exchange': exchange_id,
            'metadata': metadata.model_dump()
        })

    return group_config
```

### 4.4 Update Example Config
**File**: `config.yaml` (example section)

```yaml
# Symbol Configuration
symbol_groups:
  - canonical: "BTC_USD"
    base_asset: "BTC"
    quote_asset: "USD"
    market_type: "PERP"
    mappings:
      - value: "BTC-PERP"
        exchange: "hyperliquid"
        metadata:
          asset_index: 0
      - value: "BTC_PERP"
        exchange: "backpack"
        metadata:
          symbol_id: "1"

  - canonical: "ETH_USD"
    base_asset: "ETH"
    quote_asset: "USD"
    market_type: "PERP"
    mappings:
      - value: "ETH-PERP"
        exchange: "hyperliquid"
        metadata:
          asset_index: 1
      - value: "ETH_PERP"
        exchange: "backpack"
        metadata:
          symbol_id: "2"
```

## Benefits of This Approach

1. **Type Safety**: Full Pydantic validation throughout
2. **Single Model**: Config now matches single-model architecture
3. **Cleaner Structure**: No more artificial InternalSymbol/ExchangeSymbol split
4. **Extensible**: Easy to add new metadata fields
5. **Migration Path**: Can handle old config format during transition

## Testing
1. Test config loading with new format
2. Test migration from old format
3. Test validation errors
4. Test metadata extraction

## Success Criteria
- [ ] Config models updated
- [ ] Loader works with new format
- [ ] Type safety maintained
- [ ] Migration path available
- [ ] All tests pass

## Next: Step 5
Create comprehensive tests and usage examples.
