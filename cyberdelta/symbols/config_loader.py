"""Symbol Configuration Loader."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.config import get_app_settings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.symbol_mapping import (
    SymbolMappingErrorMessages,
    SymbolMappingFieldError,
)

from .global_service import get_symbol_service


if TYPE_CHECKING:
    from cyberdelta.config.models.symbol_configs import (
        SymbolGroupConfig,
        SymbolMappingConfig,
        SymbolMetadataConfig,
    )


logger = get_logger(__name__)


class ConfigSymbolLoader:
    """Loads symbols from configuration."""

    def __init__(self) -> None:
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
        symbol_groups = getattr(app_settings, "symbol_groups", None)
        if symbol_groups is None:
            # Fallback to unified_symbols for compatibility
            symbol_groups = getattr(app_settings, "unified_symbols", [])

        logger.info("loading_symbols_from_config", total_groups=len(symbol_groups))

        for symbol_group in symbol_groups:
            try:
                self._load_symbol_group(symbol_group)
            except Exception as e:
                logger.exception(
                    "failed_to_load_symbol_group", canonical=symbol_group.canonical, error=str(e)
                )
                continue

        logger.info(
            "symbols_loaded", loaded_count=self._loaded_count, total_groups=len(symbol_groups)
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
            except (ValueError, KeyError, AttributeError) as e:
                logger.warning(
                    "failed_to_load_symbol_mapping",
                    exchange=mapping.exchange,
                    value=mapping.value,
                    error=str(e),
                )
                continue

    def _load_symbol_mapping(self, mapping: SymbolMappingConfig, group: SymbolGroupConfig) -> None:
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
                "unknown_exchange", exchange=mapping.exchange, symbol_value=mapping.value
            )
            return

        # Build metadata kwargs
        metadata_kwargs = self._build_metadata_kwargs(exchange, mapping.metadata)

        # Create symbol
        asset_index = metadata_kwargs.get("asset_index")
        symbol_id = metadata_kwargs.get("symbol_id")

        symbol = self.service.create_symbol(
            mapping.value, exchange, asset_index=asset_index, symbol_id=symbol_id
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
            metadata=metadata_kwargs,
        )

    def _build_metadata_kwargs(
        self, exchange: ExchangeName, metadata: SymbolMetadataConfig
    ) -> dict[str, Any]:
        """Build metadata kwargs for symbol creation.

        Args:
            exchange: Exchange enum
            metadata: Metadata config

        Returns:
            Kwargs for symbol metadata

        Raises:
            SymbolMappingFieldError: If metadata configuration is invalid.
        """
        kwargs: dict[str, Any] = {}

        # Hyperliquid metadata
        if exchange == ExchangeName.HYPERLIQUID:
            if metadata.asset_index is not None:
                kwargs["asset_index"] = metadata.asset_index

        # Backpack metadata
        elif exchange == ExchangeName.BACKPACK:
            if metadata.symbol_id is not None:
                try:
                    kwargs["symbol_id"] = int(metadata.symbol_id)
                except ValueError as e:
                    msg = f"{SymbolMappingErrorMessages.SYMBOL_ID_INVALID}: {metadata.symbol_id}"
                    raise SymbolMappingFieldError(
                        msg,
                        field_name="symbol_id",
                        field_value=metadata.symbol_id,
                        exchange_id=exchange.value,
                        original_exception=e,
                    ) from e
            else:
                # Backpack requires symbol_id
                raise SymbolMappingFieldError(
                    SymbolMappingErrorMessages.SYMBOL_ID_REQUIRED_BACKPACK,
                    field_name="symbol_id",
                    exchange_id=exchange.value,
                )

        return kwargs


def load_symbols_from_config() -> int:
    """Load all symbols from config.

    Returns:
        Number of symbols loaded
    """
    loader = ConfigSymbolLoader()
    return loader.load_symbols_from_config()
