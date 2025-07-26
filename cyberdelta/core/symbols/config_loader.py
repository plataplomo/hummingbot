"""Configuration-driven symbol registry loader for unified symbol system.

This module loads unified symbols directly from configuration into the symbol registry,
bypassing all legacy compatibility layers for optimal performance.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cyberdelta.config import get_app_settings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.exceptions import SymbolValidationError
from cyberdelta.core.symbols.models import ExchangeSymbol, InternalSymbol, UnifiedSymbol
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.enums.exchange_names import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.config.models.symbol_configs import UnifiedSymbolConfig

logger = get_logger(__name__)


class ConfigSymbolLoader:
    """Loads symbols directly from configuration into the symbol service.

    This class provides a clean, direct path from configuration to the symbol service
    without going through legacy compatibility layers. It's designed for optimal
    performance and simplicity in the new unified symbol system.
    """

    def __init__(self) -> None:
        """Initialize the configuration symbol loader."""
        self.service: SymbolService = SymbolService()
        self._loaded_symbols: set[str] = set()

    def load_symbols_from_config(self) -> int:
        """Load all unified symbols from configuration into the service.

        Returns:
            Number of symbols successfully loaded

        Raises:
            SymbolValidationError: If symbol validation fails
        """
        app_settings = get_app_settings()
        unified_symbols = app_settings.unified_symbols

        logger.info(
            "Starting symbol loading from configuration", total_symbols=len(unified_symbols)
        )

        loaded_count = 0

        for symbol_config in unified_symbols:
            try:
                unified_symbol = self._create_unified_symbol_from_config(symbol_config)
                self.service.register_symbol(unified_symbol)
                self._loaded_symbols.add(unified_symbol.internal.value)
                loaded_count += 1

                logger.debug(
                    "Symbol loaded successfully",
                    internal_symbol=unified_symbol.internal.value,
                    exchange_count=len(unified_symbol.exchange_mappings),
                )

            except Exception as e:
                logger.exception(
                    "Failed to load symbol from configuration",
                    symbol_config=(
                        symbol_config.model_dump()
                        if hasattr(symbol_config, "model_dump")
                        else str(symbol_config)
                    ),
                    error=str(e),
                )
                continue

        logger.info(
            "Symbol loading completed",
            loaded_count=loaded_count,
            total_symbols=len(unified_symbols),
            success_rate=(
                f"{(loaded_count / len(unified_symbols) * 100):.1f}%" if unified_symbols else "0%"
            ),
        )

        return loaded_count

    def _create_unified_symbol_from_config(self, config: UnifiedSymbolConfig) -> UnifiedSymbol:
        """Create a UnifiedSymbol from configuration.

        Args:
            config: Configuration for the unified symbol

        Returns:
            UnifiedSymbol instance

        Raises:
            SymbolValidationError: If symbol validation fails
        """
        # Create internal symbol
        try:
            market_type = MarketType(config.internal.market_type)
        except ValueError as e:
            raise SymbolValidationError(
                symbol=config.internal.value,
                reason=f"Invalid market type '{config.internal.market_type}'",
            ) from e

        internal_symbol = InternalSymbol(
            value=config.internal.value,
            base_asset=config.internal.base_asset,
            quote_asset=config.internal.quote_asset,
            market_type=market_type,
        )

        # Create exchange mappings
        exchange_mappings: dict[str, ExchangeSymbol] = {}

        for exchange_id_str, exchange_config in config.exchange_mappings.items():
            try:
                exchange_id = ExchangeName(exchange_id_str.lower())
            except ValueError:
                logger.warning(
                    "Skipping unknown exchange in symbol configuration",
                    exchange_id=exchange_id_str,
                    internal_symbol=config.internal.value,
                )
                continue

            # Convert symbol_id from str to int if provided
            symbol_id = None
            if exchange_config.symbol_id is not None:
                try:
                    symbol_id = int(exchange_config.symbol_id)
                except ValueError:
                    logger.warning(
                        "invalid_symbol_id",
                        symbol=config.internal.value,
                        exchange=exchange_id,
                        symbol_id=exchange_config.symbol_id,
                        message="symbol_id must be numeric, ignoring",
                    )

            exchange_symbol = ExchangeSymbol(
                value=exchange_config.value,
                exchange_id=exchange_id,
                internal_symbol=internal_symbol,
                asset_index=exchange_config.asset_index,
                symbol_id=symbol_id,
            )

            exchange_mappings[exchange_id.value] = exchange_symbol

        return UnifiedSymbol(internal=internal_symbol, exchange_mappings=exchange_mappings)

    def get_loaded_symbols(self) -> set[str]:
        """Get set of internal symbols that have been loaded.

        Returns:
            Set of internal symbol names that were successfully loaded
        """
        return self._loaded_symbols.copy()

    def clear_service(self) -> None:
        """Clear all symbols from the service.

        This is useful for testing or reloading symbols.
        """
        self.service.clear()
        self._loaded_symbols.clear()
        logger.info("Symbol service cleared")


def load_symbols_from_config() -> int:
    """Convenience function to load symbols from configuration.

    Returns:
        Number of symbols successfully loaded
    """
    loader = ConfigSymbolLoader()
    return loader.load_symbols_from_config()


def get_config_symbol_loader() -> ConfigSymbolLoader:
    """Get a ConfigSymbolLoader instance.

    Returns:
        ConfigSymbolLoader instance
    """
    return ConfigSymbolLoader()
