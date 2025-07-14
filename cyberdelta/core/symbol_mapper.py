"""Symbol Mapping System - V0.0.1 Refactored.

This module provides a strict, fail-fast SymbolMapper implementation that ensures
type safety and proper error handling for production trading systems.
"""

from __future__ import annotations

import re
from threading import RLock
from typing import Any, Final

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.exceptions import (
    ExchangeNotSupportedError,
    InvalidSymbolFormatError,
    SymbolMappingConfigurationError,
    SymbolMappingError,
    SymbolMappingErrorMessages,
    SymbolMappingFieldError,
    SymbolNotFoundError,
)


logger = get_logger(__name__)


# Import error message constants
ErrorMessages = SymbolMappingErrorMessages


class SymbolMapper:
    """Strict, type-safe symbol mapper for production trading systems.

    This implementation:
    - Fails fast on invalid inputs
    - Uses proper type validation
    - Is thread-safe
    - Provides comprehensive error messages
    - No backward compatibility concerns
    """

    # Symbol validation patterns
    INTERNAL_SYMBOL_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Z0-9]{2,10}$")
    EXCHANGE_SYMBOL_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Z0-9_-]{2,20}$")

    def __init__(self, exchanges_config: dict[str, Any]) -> None:
        """Initialize with strict validation.

        Args:
            exchanges_config: Exchange configuration dictionary.

        Raises:
            SymbolMappingError: If configuration is invalid.
        """
        self._lock: Final[RLock] = RLock()

        # Validate configuration structure immediately
        self._validate_config_structure(exchanges_config)

        # Initialize internal mappings
        self._internal_to_exchange: Final[dict[str, dict[str, str]]] = {}
        self._exchange_to_internal: Final[dict[str, dict[str, str]]] = {}
        self._all_internal_symbols: Final[set[str]] = set()
        self._supported_exchanges: Final[set[str]] = set()

        # Process configuration with strict validation
        self._process_exchanges_config(exchanges_config)

        # Final validation
        self._validate_final_state()

        logger.info(
            "Symbol mapper initialized",
            exchanges_count=len(self._supported_exchanges),
            internal_symbols_count=len(self._all_internal_symbols),
            supported_exchanges=list(self._supported_exchanges),
            internal_symbols=sorted(self._all_internal_symbols),
        )

    def _validate_config_structure(self, config: dict[str, Any]) -> None:
        """Validate the basic structure of configuration."""
        # Config is already typed as dict[str, Any] so no isinstance check needed
        if not config:
            raise SymbolMappingConfigurationError(
                ErrorMessages.CONFIG_EMPTY, config_type="config_structure"
            )

    def _process_exchanges_config(self, config: dict[str, Any]) -> None:
        """Process exchanges configuration with strict validation."""
        for exchange_id, exchange_data in config.items():
            self._process_single_exchange(exchange_id, exchange_data)

    def _process_single_exchange(self, exchange_id: str, exchange_data: dict[str, Any]) -> None:
        """Process a single exchange configuration."""
        # Validate exchange ID
        if not exchange_id.strip():
            raise SymbolMappingConfigurationError(
                ErrorMessages.EXCHANGE_ID_INVALID,
                exchange_id=exchange_id,
                config_type="exchange_id",
                metadata={"exchange_id": exchange_id},
            )

        exchange_id = exchange_id.strip()

        # Check if exchange is enabled (default to True if not specified)
        if not exchange_data.get("enabled", True):
            logger.info("Skipping disabled exchange", exchange_id=exchange_id)
            return

        # Validate and process symbols section
        self._validate_and_process_symbols(exchange_id, exchange_data)
        self._supported_exchanges.add(exchange_id)

    def _validate_and_process_symbols(
        self, exchange_id: str, exchange_data: dict[str, Any]
    ) -> None:
        """Validate and process symbols section for an exchange."""
        # Validate symbols section exists
        if "symbols" not in exchange_data:
            raise SymbolMappingConfigurationError(
                ErrorMessages.SYMBOLS_MISSING,
                exchange_id=exchange_id,
                config_type="symbols_section",
                metadata={"exchange_id": exchange_id},
            )

        symbols_value = exchange_data["symbols"]
        if not isinstance(symbols_value, dict):
            raise SymbolMappingConfigurationError(
                ErrorMessages.SYMBOLS_NOT_DICT,
                exchange_id=exchange_id,
                config_type="symbols_format",
                metadata={"exchange_id": exchange_id, "symbols_type": type(symbols_value).__name__},
            )

        # Type narrowing: we now know symbols_value is a dict
        symbols_dict: dict[str, str] = symbols_value

        if not symbols_dict:
            raise SymbolMappingConfigurationError(
                ErrorMessages.SYMBOLS_EMPTY,
                exchange_id=exchange_id,
                config_type="symbols_content",
                metadata={"exchange_id": exchange_id},
            )

        # Initialize exchange mappings
        self._exchange_to_internal[exchange_id] = {}
        self._internal_to_exchange[exchange_id] = {}

        # Process each symbol mapping individually to avoid type issues
        for internal_symbol, exchange_symbol in symbols_dict.items():
            self._process_single_symbol_mapping(exchange_id, internal_symbol, exchange_symbol)

        logger.debug(
            "Processed symbol mappings for exchange",
            exchange_id=exchange_id,
            symbol_count=len(symbols_dict),
        )

    def _process_single_symbol_mapping(
        self, exchange_id: str, internal_symbol: str, exchange_symbol: object
    ) -> None:
        """Process a single symbol mapping with strict validation."""
        # Validate internal symbol
        if not internal_symbol.strip():
            raise SymbolMappingFieldError(
                ErrorMessages.INTERNAL_SYMBOL_INVALID,
                field_name="internal_symbol",
                field_value=internal_symbol,
                exchange_id=exchange_id,
                metadata={"exchange_id": exchange_id, "internal_symbol": internal_symbol},
            )

        internal_symbol = internal_symbol.strip()

        if not self.INTERNAL_SYMBOL_PATTERN.match(internal_symbol):
            raise InvalidSymbolFormatError(
                ErrorMessages.INTERNAL_SYMBOL_PATTERN,
                symbol=internal_symbol,
                expected_pattern="^[A-Z0-9]{2,10}$",
                symbol_type="internal",
                exchange_id=exchange_id,
                metadata={
                    "exchange_id": exchange_id,
                    "internal_symbol": internal_symbol,
                    "expected_pattern": "^[A-Z0-9]{2,10}$",
                },
            )

        # Validate exchange symbol
        if not isinstance(exchange_symbol, str) or not exchange_symbol.strip():
            raise SymbolMappingFieldError(
                ErrorMessages.EXCHANGE_SYMBOL_INVALID,
                field_name="exchange_symbol",
                field_value=exchange_symbol,
                exchange_id=exchange_id,
                symbol=internal_symbol,
                metadata={
                    "exchange_id": exchange_id,
                    "internal_symbol": internal_symbol,
                    "exchange_symbol": exchange_symbol,
                },
            )

        exchange_symbol = exchange_symbol.strip()

        if not self.EXCHANGE_SYMBOL_PATTERN.match(exchange_symbol):
            raise InvalidSymbolFormatError(
                ErrorMessages.EXCHANGE_SYMBOL_PATTERN,
                symbol=exchange_symbol,
                expected_pattern="^[A-Z0-9_-]{2,20}$",
                symbol_type="exchange",
                exchange_id=exchange_id,
                metadata={
                    "exchange_id": exchange_id,
                    "internal_symbol": internal_symbol,
                    "exchange_symbol": exchange_symbol,
                    "expected_pattern": "^[A-Z0-9_-]{2,20}$",
                },
            )

        # Check for duplicate internal symbol on same exchange
        if (
            internal_symbol in self._internal_to_exchange
            and exchange_id in self._internal_to_exchange[internal_symbol]
        ):
            raise SymbolMappingConfigurationError(
                ErrorMessages.DUPLICATE_INTERNAL,
                exchange_id=exchange_id,
                config_type="duplicate_internal_symbol",
                metadata={
                    "exchange_id": exchange_id,
                    "internal_symbol": internal_symbol,
                    "error_type": "duplicate_internal_symbol",
                },
            )

        # Check for duplicate exchange symbol on same exchange
        if exchange_symbol in self._exchange_to_internal[exchange_id]:
            existing_internal = self._exchange_to_internal[exchange_id][exchange_symbol]
            raise SymbolMappingConfigurationError(
                ErrorMessages.DUPLICATE_EXCHANGE,
                exchange_id=exchange_id,
                config_type="duplicate_exchange_symbol",
                metadata={
                    "exchange_id": exchange_id,
                    "exchange_symbol": exchange_symbol,
                    "existing_internal": existing_internal,
                    "new_internal": internal_symbol,
                    "error_type": "duplicate_exchange_symbol",
                },
            )

        # Add mappings
        if internal_symbol not in self._internal_to_exchange:
            self._internal_to_exchange[internal_symbol] = {}

        self._internal_to_exchange[internal_symbol][exchange_id] = exchange_symbol
        self._exchange_to_internal[exchange_id][exchange_symbol] = internal_symbol
        self._all_internal_symbols.add(internal_symbol)

    def _validate_final_state(self) -> None:
        """Validate the final state of the symbol mapper."""
        if not self._supported_exchanges:
            raise SymbolMappingConfigurationError(
                ErrorMessages.NO_EXCHANGES, config_type="final_validation"
            )

        if not self._all_internal_symbols:
            raise SymbolMappingConfigurationError(
                ErrorMessages.NO_SYMBOLS, config_type="final_validation"
            )

        # Log warnings for symbols not available on all exchanges
        for internal_symbol in self._all_internal_symbols:
            available_exchanges = set(self._internal_to_exchange[internal_symbol].keys())
            missing_exchanges = self._supported_exchanges - available_exchanges

            if missing_exchanges:
                logger.warning(
                    "Symbol not available on all exchanges",
                    internal_symbol=internal_symbol,
                    available_exchanges=sorted(available_exchanges),
                    missing_exchanges=sorted(missing_exchanges),
                )

    def get_exchange_symbol(self, internal_symbol: str, exchange_id: str) -> str:
        """Get exchange-specific symbol from internal symbol.

        Args:
            internal_symbol: Internal symbol (e.g., "BTC").
            exchange_id: Exchange identifier (e.g., "hyperliquid").

        Returns:
            Exchange-specific symbol.

        Raises:
            SymbolNotFoundError: If symbol not found.
            ExchangeNotFoundError: If exchange not supported.
            InvalidSymbolError: If parameters are invalid.
        """
        self._validate_inputs(internal_symbol, exchange_id)

        with self._lock:
            if exchange_id not in self._supported_exchanges:
                raise ExchangeNotSupportedError(
                    ErrorMessages.EXCHANGE_NOT_SUPPORTED,
                    exchange_id=exchange_id,
                    supported_exchanges=list(self._supported_exchanges),
                    metadata={
                        "exchange_id": exchange_id,
                        "supported_exchanges": list(self._supported_exchanges),
                    },
                )

            if internal_symbol not in self._internal_to_exchange:
                raise SymbolNotFoundError(
                    ErrorMessages.INTERNAL_NOT_FOUND,
                    symbol=internal_symbol,
                    lookup_type="internal_to_exchange",
                    available_symbols=sorted(self._all_internal_symbols),
                    metadata={
                        "internal_symbol": internal_symbol,
                        "available_symbols": sorted(self._all_internal_symbols),
                    },
                )

            exchange_symbol = self._internal_to_exchange[internal_symbol].get(exchange_id)
            if exchange_symbol is None:
                available_exchanges = list(self._internal_to_exchange[internal_symbol])
                raise SymbolNotFoundError(
                    ErrorMessages.SYMBOL_NOT_ON_EXCHANGE,
                    symbol=internal_symbol,
                    exchange_id=exchange_id,
                    lookup_type="symbol_on_exchange",
                    available_exchanges=available_exchanges,
                    metadata={
                        "internal_symbol": internal_symbol,
                        "exchange_id": exchange_id,
                        "available_exchanges": available_exchanges,
                    },
                )

            return exchange_symbol

    def get_internal_symbol(self, exchange_symbol: str, exchange_id: str) -> str:
        """Get internal symbol from exchange-specific symbol.

        Args:
            exchange_symbol: Exchange-specific symbol (e.g., "BTC_PERP").
            exchange_id: Exchange identifier (e.g., "backpack").

        Returns:
            Internal symbol.

        Raises:
            SymbolNotFoundError: If symbol not found.
            ExchangeNotFoundError: If exchange not supported.
            InvalidSymbolError: If parameters are invalid.
        """
        self._validate_inputs(exchange_symbol, exchange_id)

        with self._lock:
            if exchange_id not in self._supported_exchanges:
                raise ExchangeNotSupportedError(
                    ErrorMessages.EXCHANGE_NOT_SUPPORTED,
                    exchange_id=exchange_id,
                    supported_exchanges=list(self._supported_exchanges),
                    metadata={
                        "exchange_id": exchange_id,
                        "supported_exchanges": list(self._supported_exchanges),
                    },
                )

            internal_symbol = self._exchange_to_internal[exchange_id].get(exchange_symbol)
            if internal_symbol is None:
                available_symbols = list(self._exchange_to_internal[exchange_id])
                raise SymbolNotFoundError(
                    ErrorMessages.EXCHANGE_SYMBOL_NOT_FOUND,
                    symbol=exchange_symbol,
                    exchange_id=exchange_id,
                    lookup_type="exchange_to_internal",
                    available_symbols=available_symbols,
                    metadata={
                        "exchange_symbol": exchange_symbol,
                        "exchange_id": exchange_id,
                        "available_symbols": available_symbols,
                    },
                )

            return internal_symbol

    def get_all_internal_symbols(self) -> list[str]:
        """Get all configured internal symbols."""
        with self._lock:
            return sorted(self._all_internal_symbols)

    def get_exchange_symbols_for_internal(self, internal_symbol: str) -> dict[str, str]:
        """Get all exchange symbols for an internal symbol.

        Args:
            internal_symbol: Internal symbol.

        Returns:
            Dictionary mapping exchange IDs to exchange symbols.

        Raises:
            SymbolNotFoundError: If internal symbol not found.
            InvalidSymbolError: If internal symbol is invalid.
        """
        self._validate_internal_symbol(internal_symbol)

        with self._lock:
            if internal_symbol not in self._internal_to_exchange:
                raise SymbolNotFoundError(
                    ErrorMessages.INTERNAL_NOT_FOUND,
                    symbol=internal_symbol,
                    lookup_type="internal_symbols_lookup",
                    available_symbols=sorted(self._all_internal_symbols),
                    metadata={
                        "internal_symbol": internal_symbol,
                        "available_symbols": sorted(self._all_internal_symbols),
                    },
                )

            return self._internal_to_exchange[internal_symbol].copy()

    def get_internal_symbols_for_exchange(self, exchange_id: str) -> dict[str, str]:
        """Get all internal symbols for an exchange.

        Args:
            exchange_id: Exchange identifier.

        Returns:
            Dictionary mapping exchange symbols to internal symbols.

        Raises:
            ExchangeNotFoundError: If exchange not supported.
            InvalidSymbolError: If exchange_id is invalid.
        """
        self._validate_exchange_id(exchange_id)

        with self._lock:
            if exchange_id not in self._supported_exchanges:
                raise ExchangeNotSupportedError(
                    ErrorMessages.EXCHANGE_NOT_SUPPORTED,
                    exchange_id=exchange_id,
                    supported_exchanges=list(self._supported_exchanges),
                    metadata={
                        "exchange_id": exchange_id,
                        "supported_exchanges": list(self._supported_exchanges),
                    },
                )

            return self._exchange_to_internal[exchange_id].copy()

    def is_symbol_supported(self, internal_symbol: str, exchange_id: str) -> bool:
        """Check if symbol is supported on exchange."""
        try:
            self.get_exchange_symbol(internal_symbol, exchange_id)
        except (
            SymbolNotFoundError,
            ExchangeNotSupportedError,
            InvalidSymbolFormatError,
            SymbolMappingFieldError,
        ):
            return False
        else:
            return True

    def validate_symbol_pair(
        self, internal_symbol: str, long_exchange: str, short_exchange: str
    ) -> None:
        """Validate symbol is available on both exchanges for arbitrage.

        Args:
            internal_symbol: Internal symbol.
            long_exchange: Exchange for long position.
            short_exchange: Exchange for short position.

        Raises:
            SymbolMappingError: If symbol not available on either exchange.
        """
        errors: list[str] = []

        # Validate long exchange
        try:
            self.get_exchange_symbol(internal_symbol, long_exchange)
        except (
            SymbolNotFoundError,
            ExchangeNotSupportedError,
            InvalidSymbolFormatError,
            SymbolMappingFieldError,
        ) as e:
            errors.append(f"Long exchange: {e}")

        # Validate short exchange
        try:
            self.get_exchange_symbol(internal_symbol, short_exchange)
        except (
            SymbolNotFoundError,
            ExchangeNotSupportedError,
            InvalidSymbolFormatError,
            SymbolMappingFieldError,
        ) as e:
            errors.append(f"Short exchange: {e}")

        if errors:
            raise SymbolMappingError(
                ErrorMessages.SYMBOL_PAIR_FAILED,
                exchange_id=f"{long_exchange}/{short_exchange}",
                symbol=internal_symbol,
                metadata={
                    "internal_symbol": internal_symbol,
                    "long_exchange": long_exchange,
                    "short_exchange": short_exchange,
                    "errors": errors,
                },
            )

    def get_symbol_coverage(self, internal_symbol: str) -> dict[str, bool]:
        """Get symbol availability across all configured exchanges.

        Args:
            internal_symbol: Internal symbol.

        Returns:
            Dictionary mapping exchange IDs to availability.
        """
        self._validate_internal_symbol(internal_symbol)

        with self._lock:
            coverage: dict[str, bool] = {}
            for exchange_id in self._supported_exchanges:
                coverage[exchange_id] = self.is_symbol_supported(internal_symbol, exchange_id)
            return coverage

    def get_supported_exchanges(self) -> list[str]:
        """Get list of all supported exchange IDs."""
        with self._lock:
            return sorted(self._supported_exchanges)

    def _validate_inputs(self, symbol: str, exchange_id: str) -> None:
        """Validate common input parameters."""
        if not symbol.strip():
            raise SymbolMappingFieldError(
                ErrorMessages.SYMBOL_REQUIRED,
                field_name="symbol",
                field_value=symbol,
                metadata={"symbol": symbol, "symbol_type": type(symbol).__name__},
            )

        if not exchange_id.strip():
            raise SymbolMappingFieldError(
                ErrorMessages.EXCHANGE_ID_REQUIRED,
                field_name="exchange_id",
                field_value=exchange_id,
                metadata={
                    "exchange_id": exchange_id,
                    "exchange_id_type": type(exchange_id).__name__,
                },
            )

    def _validate_internal_symbol(self, internal_symbol: str) -> None:
        """Validate internal symbol format."""
        if not internal_symbol.strip():
            raise SymbolMappingFieldError(
                ErrorMessages.INTERNAL_SYMBOL_INVALID,
                field_name="internal_symbol",
                field_value=internal_symbol,
                metadata={"internal_symbol": internal_symbol},
            )

        internal_symbol = internal_symbol.strip()
        if not self.INTERNAL_SYMBOL_PATTERN.match(internal_symbol):
            raise InvalidSymbolFormatError(
                ErrorMessages.INTERNAL_SYMBOL_PATTERN,
                symbol=internal_symbol,
                expected_pattern="^[A-Z0-9]{2,10}$",
                symbol_type="internal",
                metadata={
                    "internal_symbol": internal_symbol,
                    "expected_pattern": "^[A-Z0-9]{2,10}$",
                },
            )

    def _validate_exchange_id(self, exchange_id: str) -> None:
        """Validate exchange ID format."""
        if not exchange_id.strip():
            raise SymbolMappingFieldError(
                ErrorMessages.EXCHANGE_ID_REQUIRED,
                field_name="exchange_id",
                field_value=exchange_id,
                metadata={"exchange_id": exchange_id},
            )
