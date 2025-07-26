# cyberdelta/config/models/smart_symbol_generator.py
"""Smart Symbol Generator.

Generates UnifiedSymbolConfig objects from the streamlined smart configuration
format, maintaining full compatibility with the existing symbol system.
"""

from __future__ import annotations

from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig
from cyberdelta.config.models.symbol_configs import (
    ExchangeSymbolConfig,
    InternalSymbolConfig,
    UnifiedSymbolConfig,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName


logger = get_logger(__name__)


class SmartSymbolGenerator:
    """Generate UnifiedSymbolConfig objects from smart configuration.

    Transforms the concise smart configuration format into the standard
    UnifiedSymbolConfig objects expected by the ConfigSymbolLoader.
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

    def generate_unified_symbols(self) -> list[UnifiedSymbolConfig]:
        """Generate list of UnifiedSymbolConfig from smart configuration."""
        unified_symbols: list[UnifiedSymbolConfig] = []

        for symbol in self.smart_config.list:
            try:
                unified_symbol = self._generate_unified_symbol(symbol)
                unified_symbols.append(unified_symbol)
                logger.debug("symbol_generated", symbol=symbol)
            except Exception as e:
                logger.exception("symbol_generation_failed", symbol=symbol, error=str(e))
                raise

        logger.info(
            "smart_symbols_generated",
            total_symbols=len(unified_symbols),
            symbols=[s.internal.value for s in unified_symbols],
        )

        return unified_symbols

    def _generate_unified_symbol(self, symbol: str) -> UnifiedSymbolConfig:
        """Generate single UnifiedSymbolConfig from symbol string.

        Uses the existing enum validation and model structure to ensure
        compatibility with the current system.
        """
        # Create internal symbol config with string market type (no enum conversion)
        market_type_str = self.defaults.get("market_type", "PERP")

        internal_config = InternalSymbolConfig(
            value=symbol,
            base_asset=symbol,  # For PERP, base_asset = symbol
            quote_asset=None,  # For PERP, quote_asset = null
            market_type=market_type_str,  # Use string directly, InternalSymbolConfig expects string
        )

        # Generate exchange mappings using patterns
        exchange_mappings: dict[str, ExchangeSymbolConfig] = {}

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

                exchange_mappings[exchange_name_str] = ExchangeSymbolConfig(
                    value=exchange_value,
                    exchange_id=exchange_name_str,  # String format for config
                    asset_index=None,  # Optional field, will be populated by registry
                    symbol_id=None,  # Optional field, will be populated by registry
                )

            except Exception:
                logger.exception(
                    "exchange_mapping_failed", symbol=symbol, exchange=exchange_name_str
                )
                raise

        # Validate we have at least one exchange mapping
        if not exchange_mappings:
            msg = f"No valid exchange mappings generated for symbol {symbol}"
            raise ValueError(msg)

        return UnifiedSymbolConfig(internal=internal_config, exchange_mappings=exchange_mappings)
