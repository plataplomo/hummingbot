"""Symbol Mapping System.

This module provides the SymbolMapper class for translating between internal trading symbols
and exchange-specific symbol formats across different trading platforms.
"""

from __future__ import annotations

from typing import Any, cast  # Add Any and cast imports

from cyberdelta.config.structlog_config import get_logger


# Assuming a config structure like:
# config = {
#     "exchanges": {
#         "exchange_id_1": {
#             "symbols": {
#                 "INTERNAL_SYMBOL_A": "EXCHANGE_SYMBOL_X",
#                 "INTERNAL_SYMBOL_B": "EXCHANGE_SYMBOL_Y",
#             }
#         },
#         "exchange_id_2": {
#              "symbols": {
#                 "INTERNAL_SYMBOL_A": "EXCHANGE_SYMBOL_Z",
#             }
#         }
#     }
# }

logger = get_logger(__name__)  # Use standard logging logger


class SymbolMappingError(Exception):
    """Custom exception for symbol mapping failures."""

    pass


class SymbolMapper:
    """Centralized utility for mapping between internal symbols and exchange-specific symbols.

    Loads mapping configuration and provides translation methods.
    Includes basic validation during initialization.
    """

    def __init__(self, exchanges_config: dict[str, Any]) -> None:
        """Initialize the SymbolMapper and load mappings from the provided config.

        Args:
            exchanges_config: A dictionary where keys are exchange_ids and values are
                              dictionaries containing a "symbols" map.
                              Example: {"exchange_A": {"symbols": {"BTC": "BTC-USD"}}, ...}

        Raises:
            SymbolMappingError: If config structure is invalid or missing essential parts.

        """
        self._internal_to_exchange: dict[
            str,
            dict[str, str],
        ] = {}  # {internal: {exchange: exchange_symbol}}
        self._exchange_to_internal: dict[
            str,
            dict[str, str],
        ] = {}  # {exchange: {exchange_symbol: internal}}
        self._all_internal_symbols: set[str] = set()
        self.raw_config = exchanges_config  # Store for debugging

        self._validate_config_structure(exchanges_config)
        self._process_exchanges_config(exchanges_config)
        self._validate_config()  # Perform post-load validation if needed

        logger.info(
            f"SymbolMapper initialized. Loaded mappings for "
            f"{len(self._exchange_to_internal)} exchanges. Found "
            f"{len(self._all_internal_symbols)} unique internal symbols.",
        )

    def _validate_config_structure(self, exchanges_config: dict[str, Any]) -> None:
        """Validate the basic structure of the exchanges configuration."""
        if not isinstance(exchanges_config, dict):  # pyright: ignore [reportUnnecessaryIsInstance]
            raise SymbolMappingError(
                f"Invalid configuration: Expected a dictionary of exchanges, "
                f"got {type(exchanges_config)}",
            )

    def _process_exchanges_config(self, exchanges_config: dict[str, Any]) -> None:
        """Process the exchanges configuration and build symbol mappings."""
        for exchange_id, exchange_data_any in exchanges_config.items():
            self._process_single_exchange(exchange_id, exchange_data_any)

    def _process_single_exchange(self, exchange_id: str, exchange_data_any: object) -> None:
        """Process symbol mappings for a single exchange."""
        if not isinstance(exchange_data_any, dict):
            logger.warning(
                f"Skipping exchange '{exchange_id}': Expected a dictionary for exchange data, "
                f"got {type(exchange_data_any)}.",
            )
            return

        exchange_data: dict[str, Any] = cast(dict[str, Any], exchange_data_any)

        if "symbols" not in exchange_data:
            logger.warning(
                f"Skipping exchange '{exchange_id}': Missing 'symbols' configuration.",
            )
            return

        symbol_map = exchange_data["symbols"]
        if not isinstance(symbol_map, dict):
            logger.warning(
                f"Skipping exchange '{exchange_id}': 'symbols' must be a dictionary.",
            )
            return

        symbol_map_dict: dict[str, Any] = cast(dict[str, Any], symbol_map)
        self._process_symbol_mappings(exchange_id, symbol_map_dict)

    def _process_symbol_mappings(self, exchange_id: str, symbol_map_dict: dict[str, Any]) -> None:
        """Process symbol mappings for a specific exchange."""
        self._exchange_to_internal[exchange_id] = {}

        for internal_symbol, exchange_symbol in symbol_map_dict.items():
            self._process_single_symbol_mapping(exchange_id, internal_symbol, exchange_symbol)

    def _process_single_symbol_mapping(
        self, exchange_id: str, internal_symbol: str, exchange_symbol: object
    ) -> None:
        """Process a single symbol mapping entry."""
        # internal_symbol is guaranteed to be str since it's a dict key from config
        if not isinstance(exchange_symbol, str):
            logger.warning(
                f"Invalid symbol map value for ex '{exchange_id}': "
                f"Skip ({internal_symbol}: {exchange_symbol}). Value must be str.",
            )
            return

        self._add_internal_to_exchange_mapping(exchange_id, internal_symbol, exchange_symbol)
        self._add_exchange_to_internal_mapping(exchange_id, internal_symbol, exchange_symbol)
        self._all_internal_symbols.add(internal_symbol)

    def _add_internal_to_exchange_mapping(
        self, exchange_id: str, internal_symbol: str, exchange_symbol: str
    ) -> None:
        """Add mapping from internal symbol to exchange symbol."""
        if internal_symbol not in self._internal_to_exchange:
            self._internal_to_exchange[internal_symbol] = {}
        if exchange_id in self._internal_to_exchange[internal_symbol]:
            logger.warning(
                f"Duplicate internal symbol '{internal_symbol}' definition "
                f"for exchange '{exchange_id}'. Overwriting.",
            )
        self._internal_to_exchange[internal_symbol][exchange_id] = exchange_symbol

    def _add_exchange_to_internal_mapping(
        self, exchange_id: str, internal_symbol: str, exchange_symbol: str
    ) -> None:
        """Add mapping from exchange symbol to internal symbol."""
        if exchange_symbol in self._exchange_to_internal[exchange_id]:
            logger.warning(
                f"Duplicate exchange symbol '{exchange_symbol}' mapped for "
                f"exchange '{exchange_id}'. Overwriting mapping to internal "
                f"'{internal_symbol}'.",
            )
        self._exchange_to_internal[exchange_id][exchange_symbol] = internal_symbol

    def _validate_config(self) -> None:
        """Perform validation checks on the loaded symbol mapping configuration.

        (Placeholder for more complex validation, e.g., checking for required symbols).
        """
        # Example validation: Ensure every internal symbol is mapped somewhere?
        # Example validation: Ensure consistency across exchanges if needed?
        # Currently, basic structure validation is done in __init__.
        logger.debug("SymbolMapper configuration validation step completed (basic checks only).")
        # Add more sophisticated validation logic here in the future if required.

    def get_exchange_symbol(self, internal_symbol: str, exchange_id: str) -> str | None:
        """Get the exchange-specific symbol for a given internal symbol and exchange ID.

        Args:
            internal_symbol: The internal symbol (e.g., "BTC").
            exchange_id: The ID of the exchange (e.g., "hyperliquid").

        Returns:
            The exchange-specific symbol (e.g., "BTC-PERP"), or None if not found.

        """
        return self._internal_to_exchange.get(internal_symbol, {}).get(exchange_id)

    def get_internal_symbol(self, exchange_symbol: str, exchange_id: str) -> str | None:
        """Get the internal symbol for a given exchange-specific symbol and exchange ID.

        Args:
            exchange_symbol: The exchange-specific symbol (e.g., "BTC-PERP").
            exchange_id: The ID of the exchange (e.g., "hyperliquid").

        Returns:
            The internal symbol (e.g., "BTC"), or None if not found.

        """
        return self._exchange_to_internal.get(exchange_id, {}).get(exchange_symbol)

    def get_all_internal_symbols(self) -> list[str]:
        """Get a list of all unique internal symbols defined in the configuration.

        Returns:
            A list of internal symbol strings.

        """
        return sorted(self._all_internal_symbols)

    def get_exchange_symbols_for_internal(self, internal_symbol: str) -> dict[str, str]:
        """Get a dictionary of all exchange-specific symbols for a given internal symbol.

        Args:
            internal_symbol: The internal symbol.

        Returns:
            A dictionary where keys are exchange IDs and values are the corresponding
            exchange-specific symbols. Returns an empty dict if the internal symbol is unknown.

        """
        return self._internal_to_exchange.get(internal_symbol, {}).copy()  # Return a copy

    def get_internal_symbols_for_exchange(self, exchange_id: str) -> dict[str, str]:
        """Get a dictionary mapping exchange-specific symbols to internal symbols.

        This method maps exchange-specific symbols to internal symbols for a given exchange.

        Args:
            exchange_id: The exchange identifier

        Returns:
            Dictionary mapping exchange symbols to internal symbols

        Raises:
            SymbolMappingError: If exchange_id is not found in the configuration

        """
        return self._exchange_to_internal.get(exchange_id, {}).copy()  # Return a copy
