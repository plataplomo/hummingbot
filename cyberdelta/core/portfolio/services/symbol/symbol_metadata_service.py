"""Symbol metadata service for retrieving and generating symbol metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService
from cyberdelta.core.portfolio.services.symbol.symbol_cache_service import SymbolCacheService
from cyberdelta.core.portfolio.services.symbol.symbol_metadata import SymbolMetadata
from cyberdelta.core.portfolio.services.symbol.symbol_parsing_service import SymbolParsingService

if TYPE_CHECKING:
    from cyberdelta.core.symbols.service import SymbolService as SymbolMapper

logger = get_logger(__name__)


class SymbolMetadataService(BasePortfolioService):
    """Handles symbol metadata retrieval and generation with fallback mechanisms."""

    def __init__(
        self,
        symbol_mapper: SymbolMapper | None = None,
        cache_service: SymbolCacheService | None = None,
        parsing_service: SymbolParsingService | None = None,
        config: dict[str, Any] | None = None,
    ):
        super().__init__("symbol_metadata_service")
        self.config = config or {}
        self.logger = get_logger(__name__)
        
        self.symbol_mapper = symbol_mapper
        self.cache_service = cache_service
        self.parsing_service = parsing_service
        
        self.fallback_enabled = self.config.get("fallback_enabled", True)

    async def _initialize_service(self) -> None:
        """Initialize symbol metadata service."""
        self.logger.info("Initializing symbol metadata service")

    async def _shutdown_service(self) -> None:
        """Shutdown symbol metadata service."""
        self.logger.info("Shutting down symbol metadata service")

    async def _start_internal(self) -> None:
        """Start internal metadata operations."""
        pass

    async def _stop_internal(self) -> None:
        """Stop internal metadata operations."""
        pass

    def get_symbol_metadata(self, symbol: str) -> SymbolMetadata:
        """Get comprehensive metadata for a symbol with caching and fallback."""
        if not symbol:
            return self._generate_fallback_metadata(symbol)

        # Check cache first
        if self.cache_service:
            cached = self.cache_service.get_cached_metadata(symbol)
            if cached:
                return cached

        # Try to get from symbol mapper
        if self.symbol_mapper:
            metadata = self._get_metadata_from_mapper(symbol)
            if metadata:
                if self.cache_service:
                    self.cache_service.cache_metadata(symbol, metadata)
                return metadata

        # Generate fallback metadata
        metadata = self._generate_fallback_metadata(symbol)
        if self.cache_service:
            self.cache_service.cache_metadata(symbol, metadata)
        
        return metadata

    def _get_metadata_from_mapper(self, symbol: str) -> SymbolMetadata | None:
        """Get metadata from the symbol mapper."""
        if not self.symbol_mapper:
            return None

        try:
            # Try to get metadata if the mapper supports it
            if hasattr(self.symbol_mapper, 'get_symbol_metadata'):
                mapper_metadata = self.symbol_mapper.get_symbol_metadata(symbol)
                if mapper_metadata:
                    # Convert mapper metadata to our format
                    return self._convert_mapper_metadata(symbol, mapper_metadata)
            
            # If no direct metadata method, try to get basic info
            base_symbol = self.symbol_mapper.get_base_symbol(symbol)
            if base_symbol:
                return SymbolMetadata(
                    symbol=symbol,
                    base_symbol=base_symbol,
                    quote_symbol=self._extract_quote_from_symbol(symbol, base_symbol),
                    is_derivative=self._is_derivative_symbol(symbol),
                    source="symbol_mapper_basic",
                )
        except Exception as e:
            self.logger.warning(
                "symbol_mapper_metadata_error",
                symbol=symbol,
                error=str(e),
            )
        
        return None

    def _convert_mapper_metadata(self, symbol: str, mapper_metadata: Any) -> SymbolMetadata:
        """Convert symbol mapper metadata to our standard format."""
        # This is a generic converter - adapt based on actual mapper metadata format
        try:
            return SymbolMetadata(
                symbol=symbol,
                base_symbol=getattr(mapper_metadata, 'base_symbol', symbol),
                quote_symbol=getattr(mapper_metadata, 'quote_symbol', None),
                exchange_symbol=getattr(mapper_metadata, 'exchange_symbol', symbol),
                instrument_type=getattr(mapper_metadata, 'instrument_type', None),
                is_derivative=getattr(mapper_metadata, 'is_derivative', False),
                min_quantity=getattr(mapper_metadata, 'min_quantity', None),
                max_quantity=getattr(mapper_metadata, 'max_quantity', None),
                tick_size=getattr(mapper_metadata, 'tick_size', None),
                contract_size=getattr(mapper_metadata, 'contract_size', None),
                source="symbol_mapper_full",
            )
        except Exception as e:
            self.logger.warning(
                "mapper_metadata_conversion_error",
                symbol=symbol,
                error=str(e),
            )
            # Fallback to basic metadata
            return self._generate_fallback_metadata(symbol)

    def _generate_fallback_metadata(self, symbol: str) -> SymbolMetadata:
        """Generate fallback metadata when mapper is unavailable."""
        if not symbol:
            return SymbolMetadata(
                symbol="",
                base_symbol="",
                source="fallback_empty",
            )

        # Extract components if parsing service is available
        if self.parsing_service:
            components = self.parsing_service.extract_symbol_components(symbol)
            base_symbol = components.get("base") or symbol
            quote_symbol = components.get("quote")
            instrument_type = components.get("type")
        else:
            # Basic parsing
            base_symbol = self._extract_base_symbol_basic(symbol)
            quote_symbol = self._extract_quote_from_symbol(symbol, base_symbol)
            instrument_type = self._detect_instrument_type(symbol)

        # Determine if it's a derivative
        is_derivative = self._is_derivative_symbol(symbol)

        # Generate reasonable defaults
        metadata = SymbolMetadata(
            symbol=symbol,
            base_symbol=base_symbol,
            quote_symbol=quote_symbol,
            exchange_symbol=symbol,
            instrument_type=instrument_type,
            is_derivative=is_derivative,
            min_quantity=0.001 if not is_derivative else 1.0,
            max_quantity=1000000.0,
            tick_size=0.01 if quote_symbol in ["USD", "USDT", "USDC"] else 0.00000001,
            contract_size=1.0 if is_derivative else None,
            source="fallback_generated",
        )

        self.logger.debug(
            "fallback_metadata_generated",
            symbol=symbol,
            base_symbol=base_symbol,
            quote_symbol=quote_symbol,
            is_derivative=is_derivative,
        )

        return metadata

    def _extract_base_symbol_basic(self, symbol: str) -> str:
        """Extract base symbol using basic parsing."""
        if not symbol:
            return symbol

        # Try common separators
        for separator in ["-", "_", "/"]:
            if separator in symbol:
                parts = symbol.split(separator)
                if parts and len(parts[0]) >= 2:
                    return parts[0].upper()

        # No separator found, return the symbol itself
        return symbol.upper()

    def _extract_quote_from_symbol(self, symbol: str, base_symbol: str) -> str | None:
        """Extract quote currency from symbol."""
        if not symbol or not base_symbol:
            return None

        symbol_upper = symbol.upper()
        base_upper = base_symbol.upper()

        # Common quote currencies
        quote_currencies = ["USD", "USDT", "USDC", "BTC", "ETH", "BNB", "BUSD"]
        
        for quote in quote_currencies:
            if quote in symbol_upper and quote != base_upper:
                # Check if it appears after the base symbol
                if base_upper in symbol_upper:
                    base_index = symbol_upper.find(base_upper)
                    quote_index = symbol_upper.find(quote)
                    if quote_index > base_index:
                        return quote

        return None

    def _detect_instrument_type(self, symbol: str) -> str | None:
        """Detect instrument type from symbol."""
        symbol_upper = symbol.upper()
        
        if any(x in symbol_upper for x in ["PERP", "PERPETUAL"]):
            return "PERPETUAL"
        elif any(x in symbol_upper for x in ["FUT", "FUTURE"]):
            return "FUTURE"
        elif any(x in symbol_upper for x in ["OPT", "OPTION"]):
            return "OPTION"
        elif any(x in symbol_upper for x in ["SPOT"]):
            return "SPOT"
        
        return None

    def _is_derivative_symbol(self, symbol: str) -> bool:
        """Determine if symbol represents a derivative instrument."""
        symbol_upper = symbol.upper()
        
        derivative_indicators = [
            "PERP", "PERPETUAL", "FUT", "FUTURE", "OPT", "OPTION",
            "SWAP", "MARGIN"
        ]
        
        return any(indicator in symbol_upper for indicator in derivative_indicators)

    def bulk_get_metadata(self, symbols: list[str]) -> dict[str, SymbolMetadata]:
        """Get metadata for multiple symbols efficiently."""
        results = {}
        
        for symbol in symbols:
            try:
                metadata = self.get_symbol_metadata(symbol)
                results[symbol] = metadata
            except Exception as e:
                self.logger.warning(
                    "bulk_metadata_error",
                    symbol=symbol,
                    error=str(e),
                )
                # Generate minimal fallback
                results[symbol] = SymbolMetadata(
                    symbol=symbol,
                    base_symbol=symbol,
                    source="error_fallback",
                )
        
        return results

    def update_metadata(self, symbol: str, updates: dict[str, Any]) -> SymbolMetadata:
        """Update metadata for a symbol."""
        current_metadata = self.get_symbol_metadata(symbol)
        
        # Create updated metadata
        metadata_dict = {
            "symbol": current_metadata.symbol,
            "base_symbol": current_metadata.base_symbol,
            "quote_symbol": current_metadata.quote_symbol,
            "exchange_symbol": current_metadata.exchange_symbol,
            "instrument_type": current_metadata.instrument_type,
            "is_derivative": current_metadata.is_derivative,
            "min_quantity": current_metadata.min_quantity,
            "max_quantity": current_metadata.max_quantity,
            "tick_size": current_metadata.tick_size,
            "contract_size": current_metadata.contract_size,
            "source": current_metadata.source + "_updated",
        }
        
        # Apply updates
        for key, value in updates.items():
            if key in metadata_dict:
                metadata_dict[key] = value
        
        updated_metadata = SymbolMetadata(**metadata_dict)
        
        # Update cache
        if self.cache_service:
            self.cache_service.cache_metadata(symbol, updated_metadata)
        
        self.logger.info(
            "metadata_updated",
            symbol=symbol,
            updates=list(updates.keys()),
        )
        
        return updated_metadata

    def get_metadata_stats(self) -> dict[str, Any]:
        """Get statistics about metadata operations."""
        stats = {
            "has_symbol_mapper": self.symbol_mapper is not None,
            "fallback_enabled": self.fallback_enabled,
            "has_parsing_service": self.parsing_service is not None,
        }

        # Add cache stats if available
        if self.cache_service:
            cache_stats = self.cache_service.get_cache_stats()
            stats["metadata_cache_size"] = cache_stats.get("metadata_cache_size", 0)
            stats["cache_hit_rate"] = cache_stats.get("hit_rate", 0.0)

        return stats