"""Symbol parsing service for fallback symbol parsing and validation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

if TYPE_CHECKING:
    from cyberdelta.core.symbols.service import SymbolService as SymbolMapper

logger = get_logger(__name__)


class SymbolParsingService(BasePortfolioService):
    """Handles fallback symbol parsing and validation when symbol mapper is unavailable."""

    # Symbol parsing constants
    MIN_SYMBOL_PARTS = 2  # Minimum parts when splitting by separator
    MIN_SYMBOL_LENGTH = 2  # Minimum length for a valid symbol part
    MAX_SYMBOL_LENGTH = 10  # Maximum length for a valid symbol part

    def __init__(self, symbol_mapper: SymbolMapper | None = None, config: dict[str, Any] | None = None):
        super().__init__("symbol_parsing_service")
        self.config = config or {}
        self.logger = get_logger(__name__)
        
        self.symbol_mapper = symbol_mapper
        self.fallback_enabled = self.config.get("fallback_enabled", True)
        self.strict_mode = self.config.get("strict_mode", False)

    async def _initialize_service(self) -> None:
        """Initialize symbol parsing service."""
        self.logger.info("Initializing symbol parsing service")

    async def _shutdown_service(self) -> None:
        """Shutdown symbol parsing service."""
        self.logger.info("Shutting down symbol parsing service")

    async def _start_internal(self) -> None:
        """Start internal parsing operations."""
        pass

    async def _stop_internal(self) -> None:
        """Stop internal parsing operations."""
        pass

    def get_base_symbol(self, symbol: str) -> str:
        """Extract base symbol with fallback mechanisms."""
        if not symbol:
            return symbol

        # Try symbol mapper first
        if self.symbol_mapper:
            base_symbol = self._get_base_symbol_from_mapper(symbol)
            if base_symbol:
                return base_symbol

        # Fallback to internal parsing
        if self.fallback_enabled:
            base_symbol = self._parse_base_symbol_fallback(symbol)
            if base_symbol:
                return base_symbol

        # Final fallback - return original or handle failure
        return self._handle_symbol_resolution_failure(symbol, None)

    def _get_base_symbol_from_mapper(self, symbol: str) -> str | None:
        """Get base symbol using the symbol mapper."""
        if not self.symbol_mapper:
            return None

        try:
            base_symbol = self.symbol_mapper.get_base_symbol(symbol)
            if base_symbol and base_symbol != symbol:
                self.logger.debug("base_symbol_resolved", symbol=symbol, base_symbol=base_symbol)
                return base_symbol
        except Exception as e:
            self.logger.warning(
                "symbol_mapper_base_symbol_error",
                symbol=symbol,
                error=str(e),
            )
        return None

    def _parse_base_symbol_fallback(self, symbol: str) -> str | None:
        """Parse base symbol using fallback logic."""
        if not self.fallback_enabled:
            return None

        # Try common separators in order of likelihood
        separators = ["-", "_", "/", ":"]
        
        for separator in separators:
            if separator in symbol:
                parts = symbol.split(separator)
                if len(parts) >= self.MIN_SYMBOL_PARTS:
                    # Take the first valid part as base symbol
                    for part in parts:
                        if self._is_valid_symbol_part(part):
                            self.logger.debug(
                                "fallback_base_symbol_parsed",
                                symbol=symbol,
                                base_symbol=part,
                                separator=separator,
                            )
                            return part.upper()

        # No separator found or no valid parts - check if symbol itself is valid
        if self._is_valid_symbol_part(symbol):
            return symbol.upper()

        self.logger.debug("fallback_base_symbol_parsing_failed", symbol=symbol)
        return None

    def _handle_symbol_resolution_failure(self, symbol: str, base_symbol: str | None) -> str:
        """Handle case where symbol resolution completely fails."""
        if self.strict_mode:
            error_msg = f"Symbol resolution failed for: {symbol}"
            self.logger.error("strict_mode_symbol_failure", symbol=symbol)
            raise ValueError(error_msg)

        # Return original symbol as last resort
        self.logger.warning(
            "symbol_resolution_failed_using_original",
            symbol=symbol,
            attempted_base=base_symbol,
        )
        return symbol

    def _is_valid_symbol_part(self, part: str) -> bool:
        """Validate that a symbol part meets basic criteria."""
        if not part:
            return False
        
        # Basic length check
        if len(part) < self.MIN_SYMBOL_LENGTH or len(part) > self.MAX_SYMBOL_LENGTH:
            return False
        
        # Must be alphanumeric (allowing some special chars)
        if not part.replace("-", "").replace("_", "").isalnum():
            return False
        
        # Should not be all numbers (unlikely to be a valid symbol)
        if part.isdigit():
            return False
        
        # Should have at least one letter
        if not any(c.isalpha() for c in part):
            return False
        
        return True

    def try_symbol_mapper(self, symbol: str) -> str | None:
        """Try to resolve symbol using the symbol mapper."""
        if not self.symbol_mapper:
            return None

        try:
            normalized = self.symbol_mapper.normalize_symbol(symbol, "default")
            if normalized and normalized != symbol:
                return normalized
        except Exception as e:
            self.logger.warning(
                "symbol_mapper_error",
                symbol=symbol,
                error=str(e),
            )
        return None

    def try_fallback_parsing(self, symbol: str) -> str | None:
        """Try fallback parsing when symbol mapper fails."""
        if not self.fallback_enabled:
            return None

        # Simple uppercase normalization
        normalized = symbol.upper().strip()
        
        # Remove common prefixes/suffixes that might interfere
        prefixes_to_remove = ["SPOT-", "PERP-", "FUT-"]
        suffixes_to_remove = ["-PERP", "-SPOT", "-USD", "-USDT"]
        
        for prefix in prefixes_to_remove:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]
                break
        
        for suffix in suffixes_to_remove:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]
                break
        
        if normalized != symbol.upper().strip() and self._is_valid_symbol_part(normalized):
            self.logger.debug(
                "fallback_parsing_success",
                original=symbol,
                normalized=normalized,
            )
            return normalized
        
        return None

    def validate_symbol(self, symbol: str) -> bool:
        """Validate if a symbol meets basic criteria."""
        if not symbol:
            return False

        # Basic validation
        if not self._is_valid_symbol_part(symbol):
            return False

        # Additional checks for full symbols
        if len(symbol) > 20:  # Very long symbols are suspicious
            return False

        # Check for obvious invalid patterns
        invalid_patterns = ["...", "___", "---", "//"]
        for pattern in invalid_patterns:
            if pattern in symbol:
                return False

        return True

    def extract_symbol_components(self, symbol: str) -> dict[str, str | None]:
        """Extract components from a complex symbol."""
        components = {
            "base": None,
            "quote": None,
            "type": None,
            "expiry": None,
        }

        if not symbol:
            return components

        # Try to extract base symbol
        components["base"] = self.get_base_symbol(symbol)

        # Try to identify quote currency (common ones)
        quote_currencies = ["USD", "USDT", "USDC", "BTC", "ETH"]
        for quote in quote_currencies:
            if quote in symbol.upper():
                components["quote"] = quote
                break

        # Try to identify instrument type
        if any(x in symbol.upper() for x in ["PERP", "PERPETUAL"]):
            components["type"] = "PERPETUAL"
        elif any(x in symbol.upper() for x in ["SPOT"]):
            components["type"] = "SPOT"
        elif any(x in symbol.upper() for x in ["FUT", "FUTURE"]):
            components["type"] = "FUTURE"

        return components