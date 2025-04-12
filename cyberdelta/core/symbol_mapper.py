from __future__ import annotations

import logging  # Use standard logging

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

logger = logging.getLogger(__name__)  # Use standard logging logger


class SymbolMappingError(Exception):
    """Custom exception for symbol mapping failures."""

    pass


class SymbolMapper:
    """
    Centralized utility for mapping between internal symbols and exchange-specific symbols.

    Loads mapping configuration and provides translation methods.
    Includes basic validation during initialization.
    """

    def __init__(self, config: dict) -> None:
        """
        Initializes the SymbolMapper and loads mappings from the provided config.

        Args:
            config: The application configuration dictionary.

        Raises:
            SymbolMappingError: If the configuration structure is invalid or missing essential parts.
        """
        self._internal_to_exchange: dict[
            str, dict[str, str]
        ] = {}  # {internal: {exchange: exchange_symbol}}
        self._exchange_to_internal: dict[
            str, dict[str, str]
        ] = {}  # {exchange: {exchange_symbol: internal}}
        self._all_internal_symbols: set[str] = set()

        if not isinstance(config, dict) or "exchanges" not in config:
            raise SymbolMappingError("Invalid configuration structure: 'exchanges' key missing.")

        exchanges_config = config["exchanges"]
        if not isinstance(exchanges_config, dict):
            raise SymbolMappingError("Invalid configuration: 'exchanges' must be a dictionary.")

        for exchange_id, exchange_data in exchanges_config.items():
            if not isinstance(exchange_data, dict) or "symbols" not in exchange_data:
                logger.warning(
                    f"Skipping exchange '{exchange_id}': Missing 'symbols' configuration."
                )
                continue

            symbol_map = exchange_data["symbols"]
            if not isinstance(symbol_map, dict):
                logger.warning(
                    f"Skipping exchange '{exchange_id}': 'symbols' must be a dictionary."
                )
                continue

            self._exchange_to_internal[exchange_id] = {}
            for internal_symbol, exchange_symbol in symbol_map.items():
                if not isinstance(internal_symbol, str) or not isinstance(exchange_symbol, str):
                    logger.warning(
                        f"Invalid symbol mapping entry for exchange '{exchange_id}': "
                        f"Skipping ({internal_symbol}: {exchange_symbol}). Both must be strings."
                    )
                    continue

                # Add to internal -> exchange map
                if internal_symbol not in self._internal_to_exchange:
                    self._internal_to_exchange[internal_symbol] = {}
                if exchange_id in self._internal_to_exchange[internal_symbol]:
                    logger.warning(
                        f"Duplicate internal symbol '{internal_symbol}' definition "
                        f"for exchange '{exchange_id}'. Overwriting."
                    )
                self._internal_to_exchange[internal_symbol][exchange_id] = exchange_symbol

                # Add to exchange -> internal map
                if exchange_symbol in self._exchange_to_internal[exchange_id]:
                    logger.warning(
                        f"Duplicate exchange symbol '{exchange_symbol}' mapped for "
                        f"exchange '{exchange_id}'. Overwriting mapping to internal '{internal_symbol}'."
                    )
                self._exchange_to_internal[exchange_id][exchange_symbol] = internal_symbol

                # Add to set of all known internal symbols
                self._all_internal_symbols.add(internal_symbol)

        self._validate_config()  # Perform post-load validation if needed
        logger.info(
            f"SymbolMapper initialized. Loaded mappings for {len(self._exchange_to_internal)} exchanges. "
            f"Found {len(self._all_internal_symbols)} unique internal symbols."
        )

    def _validate_config(self) -> None:
        """
        Performs validation checks on the loaded symbol mapping configuration.
        (Placeholder for more complex validation, e.g., checking for required symbols).
        """
        # Example validation: Ensure every internal symbol is mapped somewhere?
        # Example validation: Ensure consistency across exchanges if needed?
        # Currently, basic structure validation is done in __init__.
        logger.debug("SymbolMapper configuration validation step completed (basic checks only).")
        # Add more sophisticated validation logic here in the future if required.

    def get_exchange_symbol(self, internal_symbol: str, exchange_id: str) -> str | None:
        """
        Get the exchange-specific symbol for a given internal symbol and exchange ID.

        Args:
            internal_symbol: The internal symbol (e.g., "BTC").
            exchange_id: The ID of the exchange (e.g., "hyperliquid").

        Returns:
            The exchange-specific symbol (e.g., "BTC-PERP"), or None if not found.
        """
        return self._internal_to_exchange.get(internal_symbol, {}).get(exchange_id)

    def get_internal_symbol(self, exchange_symbol: str, exchange_id: str) -> str | None:
        """
        Get the internal symbol for a given exchange-specific symbol and exchange ID.

        Args:
            exchange_symbol: The exchange-specific symbol (e.g., "BTC-PERP").
            exchange_id: The ID of the exchange (e.g., "hyperliquid").

        Returns:
            The internal symbol (e.g., "BTC"), or None if not found.
        """
        return self._exchange_to_internal.get(exchange_id, {}).get(exchange_symbol)

    def get_all_internal_symbols(self) -> list[str]:
        """
        Get a list of all unique internal symbols defined in the configuration.

        Returns:
            A list of internal symbol strings.
        """
        return sorted(list(self._all_internal_symbols))

    def get_exchange_symbols_for_internal(self, internal_symbol: str) -> dict[str, str]:
        """
        Get a dictionary of all exchange-specific symbols for a given internal symbol.

        Args:
            internal_symbol: The internal symbol.

        Returns:
            A dictionary where keys are exchange IDs and values are the corresponding
            exchange-specific symbols. Returns an empty dict if the internal symbol is unknown.
        """
        return self._internal_to_exchange.get(internal_symbol, {}).copy()  # Return a copy

    def get_internal_symbols_for_exchange(self, exchange_id: str) -> dict[str, str]:
        """
        Get a dictionary mapping exchange-specific symbols to internal symbols for a given exchange.

        Args:
            exchange_id: The ID of the exchange.

        Returns:
            A dictionary where keys are exchange-specific symbols and values are the
            corresponding internal symbols. Returns an empty dict if the exchange ID is unknown.
        """
        return self._exchange_to_internal.get(exchange_id, {}).copy()  # Return a copy
