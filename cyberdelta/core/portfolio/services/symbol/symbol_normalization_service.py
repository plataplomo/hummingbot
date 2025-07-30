"""Symbol normalization service for exchange-specific symbol mapping and normalization."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService
from cyberdelta.core.portfolio.services.symbol.symbol_cache_service import SymbolCacheService
from cyberdelta.core.portfolio.services.symbol.symbol_parsing_service import SymbolParsingService

if TYPE_CHECKING:
    from cyberdelta.core.symbols.service import SymbolService as SymbolMapper

logger = get_logger(__name__)


class SymbolNormalizationService(BasePortfolioService):
    """Handles symbol normalization and exchange-specific mapping rules."""

    def __init__(
        self,
        symbol_mapper: SymbolMapper | None = None,
        cache_service: SymbolCacheService | None = None,
        parsing_service: SymbolParsingService | None = None,
        config: dict[str, Any] | None = None,
    ):
        super().__init__("symbol_normalization_service")
        self.config = config or {}
        self.logger = get_logger(__name__)
        
        self.symbol_mapper = symbol_mapper
        self.cache_service = cache_service
        self.parsing_service = parsing_service
        
        self.fallback_enabled = self.config.get("fallback_enabled", True)
        self.strict_mode = self.config.get("strict_mode", False)

    async def _initialize_service(self) -> None:
        """Initialize symbol normalization service."""
        self.logger.info("Initializing symbol normalization service")

    async def _shutdown_service(self) -> None:
        """Shutdown symbol normalization service."""
        self.logger.info("Shutting down symbol normalization service")

    async def _start_internal(self) -> None:
        """Start internal normalization operations."""
        pass

    async def _stop_internal(self) -> None:
        """Stop internal normalization operations."""
        pass

    def normalize_symbol(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol for a specific exchange with caching and fallback."""
        if not symbol:
            return symbol

        # Check cache first
        cache_key = f"{exchange_id}:{symbol}"
        if self.cache_service:
            cached = self.cache_service.get_cached_symbol(cache_key)
            if cached:
                return cached

        # Try symbol mapper normalization
        if self.symbol_mapper:
            normalized = self._normalize_symbol_with_mapper(symbol, exchange_id)
            if normalized:
                if self.cache_service:
                    self.cache_service.cache_symbol(cache_key, normalized)
                return normalized

        # Apply exchange-specific rules as fallback
        normalized = self._apply_exchange_rules(symbol, exchange_id)
        if self.cache_service:
            self.cache_service.cache_symbol(cache_key, normalized)
        
        return normalized

    def _normalize_symbol_with_mapper(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol using the symbol mapper."""
        if not self.symbol_mapper:
            return symbol

        try:
            normalized = self.symbol_mapper.normalize_symbol(symbol, exchange_id)
            if normalized:
                self.logger.debug(
                    "symbol_normalized_with_mapper",
                    original=symbol,
                    normalized=normalized,
                    exchange=exchange_id,
                )
                return normalized
        except Exception as e:
            self.logger.warning(
                "symbol_mapper_normalization_error",
                symbol=symbol,
                exchange=exchange_id,
                error=str(e),
            )

        return symbol

    def _apply_exchange_rules(self, symbol: str, exchange_id: str) -> str:
        """Apply generic normalization rules using configurable patterns."""
        normalized = symbol.upper().strip()

        # Generic normalization patterns - no exchange-specific logic
        # Apply common transformations that work across exchanges
        
        # Remove common suffixes that might need normalization
        suffixes_to_remove = ["-USD", "-USDC", "-USDT"]
        for suffix in suffixes_to_remove:
            if normalized.endswith(suffix):
                base = normalized[:-len(suffix)]
                # Keep base symbol without suffix for consistent format
                normalized = base
                break
        
        # Convert common separators to consistent format
        if "_" in normalized:
            normalized = normalized.replace("_", "-")
        elif " " in normalized:
            normalized = normalized.replace(" ", "-")
        
        # Apply quote currency standardization if needed
        # This uses generic logic rather than exchange-specific rules
        if "-" not in normalized and len(normalized) > 3:
            # If it looks like a base symbol without quote currency,
            # let the exchange-specific symbol mapper handle it
            pass
        
        # Convert common quote currency variations
        if normalized.endswith("USDT"):
            # Normalize USDT to USD for consistency
            normalized = normalized[:-4] + "USD"

        self.logger.debug(
            "generic_normalization_applied",
            original=symbol,
            normalized=normalized,
            exchange=exchange_id,
        )

        return normalized

    def denormalize_symbol(self, symbol: str, exchange_id: str) -> str:
        """Convert normalized symbol back to exchange-specific format."""
        if not symbol:
            return symbol

        # Try symbol mapper first
        if self.symbol_mapper:
            try:
                # Some mappers might have a denormalize method
                if hasattr(self.symbol_mapper, 'denormalize_symbol'):
                    denormalized = self.symbol_mapper.denormalize_symbol(symbol, exchange_id)
                    if denormalized:
                        return denormalized
            except Exception as e:
                self.logger.warning(
                    "symbol_mapper_denormalization_error",
                    symbol=symbol,
                    exchange=exchange_id,
                    error=str(e),
                )

        # Apply reverse exchange rules
        denormalized = self._apply_reverse_exchange_rules(symbol, exchange_id)
        return denormalized

    def _apply_reverse_exchange_rules(self, symbol: str, exchange_id: str) -> str:
        """Apply generic denormalization rules using configurable patterns."""
        denormalized = symbol

        # Generic denormalization patterns - no exchange-specific logic
        # Apply reverse transformations that work generically
        
        # If symbol mapper is available, let it handle exchange-specific formatting
        if self.symbol_mapper and hasattr(self.symbol_mapper, 'format_for_exchange'):
            try:
                formatted = self.symbol_mapper.format_for_exchange(symbol, exchange_id)
                if formatted:
                    return formatted
            except Exception as e:
                self.logger.debug(f"Symbol mapper formatting failed: {e}")
        
        # Generic fallback formatting
        # Ensure consistent separator format
        if "_" in denormalized:
            denormalized = denormalized.replace("_", "-")
        
        # Add quote currency if missing and symbol looks incomplete
        if "-" not in denormalized and len(denormalized) > 2:
            # Check if it ends with common quote currencies
            common_quotes = ["USD", "USDT", "USDC", "BTC", "ETH"]
            has_quote = any(denormalized.endswith(quote) for quote in common_quotes)
            
            if not has_quote:
                # Add generic quote currency (let exchange handle specifics)
                denormalized = denormalized + "-USD"
        
        # Restore common quote currency format if needed
        if denormalized.endswith("USD") and "-" not in denormalized:
            base = denormalized[:-3]
            denormalized = f"{base}-USD"

        self.logger.debug(
            "generic_denormalization_applied",
            original=symbol,
            denormalized=denormalized,
            exchange=exchange_id,
        )

        return denormalized

    def get_supported_exchanges(self) -> list[str]:
        """Get list of exchanges that can be handled generically."""
        # Return empty list since we now handle all exchanges generically
        # The symbol mapper should provide exchange-specific support
        if self.symbol_mapper and hasattr(self.symbol_mapper, 'get_supported_exchanges'):
            return self.symbol_mapper.get_supported_exchanges()
        return []

    def is_normalized_format(self, symbol: str, exchange_id: str) -> bool:
        """Check if symbol is already in normalized format for the exchange."""
        if not symbol:
            return False

        normalized = self.normalize_symbol(symbol, exchange_id)
        return symbol == normalized

    def bulk_normalize_symbols(self, symbols: list[str], exchange_id: str) -> dict[str, str]:
        """Normalize multiple symbols efficiently."""
        results = {}
        
        for symbol in symbols:
            try:
                normalized = self.normalize_symbol(symbol, exchange_id)
                results[symbol] = normalized
            except Exception as e:
                self.logger.warning(
                    "bulk_normalization_error",
                    symbol=symbol,
                    exchange=exchange_id,
                    error=str(e),
                )
                results[symbol] = symbol  # Fallback to original
        
        return results

    def get_normalization_stats(self) -> dict[str, Any]:
        """Get statistics about normalization operations."""
        stats = {
            "has_symbol_mapper": self.symbol_mapper is not None,
            "fallback_enabled": self.fallback_enabled,
            "strict_mode": self.strict_mode,
            "supported_exchanges": len(self.get_supported_exchanges()),
        }

        # Add cache stats if available
        if self.cache_service:
            cache_stats = self.cache_service.get_cache_stats()
            stats["cache_stats"] = cache_stats

        return stats