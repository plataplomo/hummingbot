"""Symbol normalization service to decouple symbol mapping dependencies."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


if TYPE_CHECKING:
    from cyberdelta.core.symbol_mapper import SymbolMapper

logger = get_logger(__name__)


class SymbolNormalizationService(BasePortfolioService):
    """Abstracts symbol mapping and normalization.

    Provides fallback mechanisms and decouples the portfolio system
    from hard dependencies on SymbolMapper, addressing the tight
    coupling issues identified in the original PortfolioTracker.
    """

    # Symbol parsing constants
    MIN_SYMBOL_PARTS = 2  # Minimum parts when splitting by separator

    # Symbol validation constants
    MIN_SYMBOL_LENGTH = 2  # Minimum length for a valid symbol part
    MAX_SYMBOL_LENGTH = 10  # Maximum length for a valid symbol part

    def __init__(
        self,
        name: str = "SymbolNormalizationService",
        config: dict[str, Any] | None = None,
        symbol_mapper: SymbolMapper | None = None,
    ) -> None:
        """Initialize the symbol normalization service.

        Args:
            name: Service name
            config: Configuration dictionary
            symbol_mapper: Optional SymbolMapper instance
        """
        cfg = config or {}
        super().__init__(name, config)

        self.symbol_mapper = symbol_mapper
        self.fallback_enabled = cfg.get("fallback_enabled", True)
        self.strict_mode = cfg.get("strict_mode", False)

        # Cache for symbol mappings
        self._symbol_cache: dict[str, str] = {}
        self._metadata_cache: dict[str, dict[str, Any]] = {}

        logger.info(
            "symbol_normalization_service_created",
            service_name=name,
            has_symbol_mapper=symbol_mapper is not None,
            fallback_enabled=self.fallback_enabled,
            strict_mode=self.strict_mode,
        )

    async def _start_internal(self) -> None:
        """Start the symbol service."""
        logger.info("symbol_normalization_service_starting")

        # Validate symbol mapper if in strict mode
        if self.strict_mode and not self.symbol_mapper:
            raise RuntimeError

    async def _stop_internal(self) -> None:
        """Stop the symbol service."""
        logger.info("symbol_normalization_service_stopping")

        # Clear caches
        self._symbol_cache.clear()
        self._metadata_cache.clear()

    def get_base_symbol(self, symbol: str) -> str:
        """Get base symbol from trading pair.

        This replaces the unsafe fallback logic from the original
        PortfolioTracker with proper error handling and caching.

        Args:
            symbol: Trading symbol (e.g., 'BTC-PERP', 'ETH/USD')

        Returns:
            Base symbol (e.g., 'BTC', 'ETH')

        Raises:
            ValueError: If symbol cannot be parsed and strict mode is enabled
        """
        self._ensure_running()

        # Check cache first
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]

        base_symbol = None

        # Try symbol mapper first if available
        if self.symbol_mapper:
            try:
                base_symbol = self._get_base_symbol_from_mapper(symbol)
                if base_symbol:
                    logger.debug(
                        "base_symbol_from_mapper",
                        symbol=symbol,
                        base_symbol=base_symbol,
                    )
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                logger.warning(
                    "symbol_mapper_failed",
                    symbol=symbol,
                    error=str(e),
                )
                if self.strict_mode:
                    raise ValueError from e

        # Fallback to parsing if mapper failed or unavailable
        if not base_symbol and self.fallback_enabled:
            base_symbol = self._parse_base_symbol_fallback(symbol)
            if base_symbol:
                logger.debug(
                    "base_symbol_from_fallback",
                    symbol=symbol,
                    base_symbol=base_symbol,
                )

        # Handle failure cases
        if not base_symbol:
            if self.strict_mode:
                raise ValueError
            logger.warning(
                "base_symbol_fallback_to_original",
                symbol=symbol,
            )
            base_symbol = symbol  # Last resort fallback

        # Cache the result
        self._symbol_cache[symbol] = base_symbol

        return base_symbol

    def normalize_symbol(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol for specific exchange.

        Args:
            symbol: Trading symbol
            exchange_id: Exchange identifier

        Returns:
            Normalized symbol for the exchange
        """
        self._ensure_running()

        cache_key = f"{exchange_id}:{symbol}"

        # Check cache
        if cache_key in self._symbol_cache:
            return self._symbol_cache[cache_key]

        normalized = symbol  # Default to original

        # Try symbol mapper
        if self.symbol_mapper:
            try:
                normalized = self._normalize_symbol_with_mapper(symbol, exchange_id)
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                logger.warning(
                    "symbol_normalization_failed",
                    symbol=symbol,
                    exchange_id=exchange_id,
                    error=str(e),
                )
                if self.strict_mode:
                    raise

        # Apply exchange-specific rules if no mapper
        if normalized == symbol and self.fallback_enabled:
            normalized = self._apply_exchange_rules(symbol, exchange_id)

        # Cache the result
        self._symbol_cache[cache_key] = normalized

        logger.debug(
            "symbol_normalized",
            original=symbol,
            normalized=normalized,
            exchange_id=exchange_id,
        )

        return normalized

    def get_symbol_metadata(self, symbol: str) -> dict[str, Any]:
        """Get metadata for symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Dictionary with symbol metadata
        """
        self._ensure_running()

        # Check cache
        if symbol in self._metadata_cache:
            return self._metadata_cache[symbol]

        metadata = {}

        # Try to get from symbol mapper
        if self.symbol_mapper:
            try:
                metadata = self._get_metadata_from_mapper(symbol)
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                logger.warning(
                    "metadata_fetch_failed",
                    symbol=symbol,
                    error=str(e),
                )

        # Add fallback metadata
        if not metadata:
            metadata = self._generate_fallback_metadata(symbol)

        # Cache the result
        self._metadata_cache[symbol] = metadata

        return metadata

    def _get_base_symbol_from_mapper(self, symbol: str) -> str | None:
        """Get base symbol using SymbolMapper.

        Args:
            symbol: Trading symbol

        Returns:
            Base symbol or None if not found
        """
        if not self.symbol_mapper:
            return None

        # TODO: Implement proper SymbolMapper API when get_base_symbol method exists
        # For now, use fallback logic
        return self._parse_base_symbol_fallback(symbol)

    def _parse_base_symbol_fallback(self, symbol: str) -> str | None:
        """Parse base symbol using fallback logic.

        This implements safer fallback parsing compared to the
        original PortfolioTracker's unsafe string splitting.

        Args:
            symbol: Trading symbol

        Returns:
            Base symbol or None if parsing fails
        """
        if not symbol:
            return None

        # Common separators in trading symbols
        separators = ["-", "/", "_", ":"]

        for sep in separators:
            if sep in symbol:
                parts = symbol.split(sep)
                if len(parts) >= self.MIN_SYMBOL_PARTS and parts[0]:
                    base = parts[0].strip().upper()
                    if self._is_valid_symbol_part(base):
                        return base

        # If no separators found, check if it's a valid single symbol
        clean_symbol = symbol.strip().upper()
        if self._is_valid_symbol_part(clean_symbol):
            return clean_symbol

        return None

    def _normalize_symbol_with_mapper(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol using SymbolMapper.

        Args:
            symbol: Trading symbol
            exchange_id: Exchange identifier

        Returns:
            Normalized symbol
        """
        if not self.symbol_mapper:
            return symbol

        # TODO: Implement proper SymbolMapper API when normalize_symbol method exists
        # For now, use fallback logic
        return self._apply_exchange_rules(symbol, exchange_id)

    def _apply_exchange_rules(self, symbol: str, exchange_id: str) -> str:
        """Apply exchange-specific normalization rules.

        Args:
            symbol: Trading symbol
            exchange_id: Exchange identifier

        Returns:
            Normalized symbol
        """
        # Exchange-specific rules
        rules = {
            "hyperliquid": {
                "separator": "-",
                "suffix_map": {"PERP": "PERP", "USD": "USD"},
            },
            "backpack": {
                "separator": "_",
                "suffix_map": {"USDC": "USDC", "SOL": "SOL"},
            },
        }

        exchange_rules = rules.get(exchange_id.lower(), {})
        if not exchange_rules:
            return symbol

        # Apply separator normalization
        separator = str(exchange_rules.get("separator", "-"))
        if "/" in symbol:
            symbol = symbol.replace("/", separator)
        elif "_" in symbol:
            symbol = symbol.replace("_", separator)

        return symbol

    def _get_metadata_from_mapper(self, symbol: str) -> dict[str, Any]:
        """Get metadata from SymbolMapper.

        Args:
            symbol: Trading symbol

        Returns:
            Metadata dictionary
        """
        if not self.symbol_mapper:
            return {}

        # This would use the actual SymbolMapper API
        # For now, return empty dict
        return {}

    def _generate_fallback_metadata(self, symbol: str) -> dict[str, Any]:
        """Generate fallback metadata for symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Metadata dictionary
        """
        base_symbol = self.get_base_symbol(symbol)

        return {
            "symbol": symbol,
            "base_symbol": base_symbol,
            "is_derivative": "-PERP" in symbol.upper() or "PERP" in symbol.upper(),
            "is_spot": "/" in symbol or "_" in symbol,
            "source": "fallback",
        }

    def _is_valid_symbol_part(self, part: str) -> bool:
        """Check if a symbol part is valid.

        Args:
            part: Symbol part to validate

        Returns:
            True if valid symbol part
        """
        if not part:
            return False

        # Basic validation rules
        if len(part) < self.MIN_SYMBOL_LENGTH or len(part) > self.MAX_SYMBOL_LENGTH:
            return False

        # Should contain only letters and numbers
        if not part.isalnum():
            return False

        # Should start with a letter
        return part[0].isalpha()

    def clear_cache(self) -> None:
        """Clear all cached symbol data."""
        self._symbol_cache.clear()
        self._metadata_cache.clear()

        logger.info("symbol_cache_cleared")

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        return {
            "symbol_cache_size": len(self._symbol_cache),
            "metadata_cache_size": len(self._metadata_cache),
            "total_cache_entries": len(self._symbol_cache) + len(self._metadata_cache),
        }

    def get_service_stats(self) -> dict[str, Any]:
        """Get service statistics.

        Returns:
            Dictionary with service statistics
        """
        return {
            "service_name": self.name,
            "is_running": self.is_running,
            "has_symbol_mapper": self.symbol_mapper is not None,
            "fallback_enabled": self.fallback_enabled,
            "strict_mode": self.strict_mode,
            **self.get_cache_stats(),
        }
