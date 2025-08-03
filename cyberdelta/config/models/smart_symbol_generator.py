# cyberdelta/config/models/smart_symbol_generator.py
"""Smart Symbol Generator.

Generates UnifiedSymbolConfig objects from the streamlined smart configuration
format, maintaining full compatibility with the existing symbol system.
"""

from __future__ import annotations

from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig
from cyberdelta.config.models.symbol_configs import (
    SymbolGroupConfig,
    SymbolMappingConfig,
    SymbolMetadataConfig,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName


logger = get_logger(__name__)


class SmartSymbolGenerator:
    """Generate SymbolGroupConfig objects from smart configuration.

    Transforms the concise smart configuration format into the standard
    SymbolGroupConfig objects expected by the ConfigSymbolLoader.
    """

    def __init__(self, smart_config: SmartSymbolsConfig) -> None:
        """Initialize the smart symbol generator.

        Args:
            smart_config: The smart symbols configuration
        """
        self.smart_config = smart_config
        self.patterns = smart_config.patterns
        self.defaults = smart_config.defaults
        self.overrides = smart_config.overrides

        logger.debug(
            "smart_symbol_generator_initialized",
            symbol_count=len(smart_config.list),
            has_patterns=True,
            has_overrides=len(smart_config.overrides) > 0,
        )

    def generate_symbol_groups(self) -> list[SymbolGroupConfig]:
        """Generate list of SymbolGroupConfig from smart configuration.
        
        Returns:
            List of SymbolGroupConfig objects generated from smart configuration.
        """
        symbol_groups: list[SymbolGroupConfig] = []

        for symbol in self.smart_config.list:
            try:
                symbol_group = self._generate_symbol_group(symbol)
                symbol_groups.append(symbol_group)
                logger.debug("symbol_generated", symbol=symbol)
            except Exception as e:
                logger.exception("symbol_generation_failed", symbol=symbol, error=str(e))
                raise

        logger.info(
            "smart_symbols_generated",
            total_symbols=len(symbol_groups),
            symbols=[s.canonical for s in symbol_groups],
        )

        return symbol_groups

    def _generate_symbol_group(self, symbol: str) -> SymbolGroupConfig:
        """Generate single SymbolGroupConfig from symbol string.

        Uses the new unified symbol architecture.
        
        Returns:
            SymbolGroupConfig object for the given symbol.
            
        Raises:
            ValueError: If symbol or market type configuration is invalid.
        """
        # Get market type from defaults
        market_type_str = self.defaults.get("market_type", "PERP")

        # Generate canonical representation
        canonical = symbol  # For perps, canonical is typically just the symbol

        # Generate exchange mappings using patterns
        mappings: list[SymbolMappingConfig] = []

        # Iterate over supported exchanges
        supported_exchanges = ["hyperliquid", "backpack"]  # Extensible list

        for exchange_name_str in supported_exchanges:
            if not hasattr(self.patterns, exchange_name_str):
                logger.debug(
                    "exchange_pattern_not_found", exchange=exchange_name_str, symbol=symbol
                )
                continue  # Skip if pattern not defined

            try:
                exchange_patterns = getattr(self.patterns, exchange_name_str)
                ExchangeName(exchange_name_str)  # Validate enum exists

                # Check for custom override first
                if symbol in self.overrides and exchange_name_str in self.overrides[symbol]:
                    exchange_value = self.overrides[symbol][exchange_name_str]
                    logger.debug(
                        "using_override",
                        symbol=symbol,
                        exchange=exchange_name_str,
                        value=exchange_value,
                    )
                else:
                    # Use pattern to generate exchange value
                    market_type_key = market_type_str.lower()  # Use string value directly
                    default_pattern = "{symbol}"  # noqa: RUF027
                    pattern = exchange_patterns.get(
                        market_type_key, exchange_patterns.get("perp", default_pattern)
                    )
                    exchange_value = pattern.format(symbol=symbol, base=symbol, quote="USDC")
                    logger.debug(
                        "using_pattern",
                        symbol=symbol,
                        exchange=exchange_name_str,
                        pattern=pattern,
                        value=exchange_value,
                    )

                # Create metadata for this exchange
                metadata = SymbolMetadataConfig(asset_index=None, symbol_id=None)

                # Create mapping
                mapping = SymbolMappingConfig(
                    value=exchange_value, exchange=exchange_name_str, metadata=metadata
                )
                mappings.append(mapping)

            except Exception:
                logger.exception(
                    "exchange_mapping_failed", symbol=symbol, exchange=exchange_name_str
                )
                raise

        # Validate we have at least one exchange mapping
        if not mappings:
            msg = f"No valid exchange mappings generated for symbol {symbol}"
            raise ValueError(msg)

        return SymbolGroupConfig(
            canonical=canonical,
            base_asset=symbol,
            quote_asset=None,  # For perps, quote is typically None in canonical form
            market_type=market_type_str,
            mappings=mappings,
        )
