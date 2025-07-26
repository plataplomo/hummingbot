"""API layer symbol integration for CyberDeltaEngine.

This module provides the integration layer between the new unified symbol system
and the existing API infrastructure, enabling seamless symbol handling across
all exchange APIs with backward compatibility.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, NoReturn, Protocol, TypedDict, Unpack

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols import (
    ExchangeSymbol,
    InternalSymbol,
    SymbolService,
    SymbolValidator,
)
from cyberdelta.core.symbols.exceptions import (
    SymbolError,
    SymbolNotFoundError,
    SymbolRegistryError,
    SymbolValidationError,
)
from cyberdelta.core.symbols.models import SymbolType
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import (
    FieldError as CoreFieldError,
    TypeFieldError,
)


class TransformationKwargs(TypedDict, total=False):
    """Type definition for transformation keyword arguments."""

    market_type: MarketType | None
    force_validation: bool
    enable_cache: bool
    timeout_seconds: float


class SymbolServiceProtocol(Protocol):
    """Protocol defining the symbol service interface for API integration."""

    def get_base_symbol(self, symbol: str) -> str:
        """Get base symbol from trading pair."""
        ...

    def normalize_symbol(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol for exchange."""
        ...

    def get_symbol_metadata(self, symbol: str) -> dict[str, Any]:
        """Get symbol metadata."""
        ...

    def validate_symbol(self, symbol: str, exchange_id: str) -> bool:
        """Validate symbol for exchange."""
        ...


@dataclass
class SymbolIntegrationMetrics:
    """Metrics for symbol integration layer."""

    api_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    validation_errors: int = 0
    transformation_errors: int = 0
    fallback_activations: int = 0
    total_processing_time_ms: float = 0.0

    def record_api_request(self, processing_time_ms: float) -> None:
        """Record an API request with processing time."""
        self.api_requests += 1
        self.total_processing_time_ms += processing_time_ms

    def record_cache_hit(self) -> None:
        """Record a cache hit."""
        self.cache_hits += 1

    def record_cache_miss(self) -> None:
        """Record a cache miss."""
        self.cache_misses += 1

    def record_validation_error(self) -> None:
        """Record a validation error."""
        self.validation_errors += 1

    def record_transformation_error(self) -> None:
        """Record a transformation error."""
        self.transformation_errors += 1

    def record_fallback_activation(self) -> None:
        """Record a fallback activation."""
        self.fallback_activations += 1

    @property
    def average_processing_time_ms(self) -> float:
        """Get average processing time per request."""
        if self.api_requests == 0:
            return 0.0
        return self.total_processing_time_ms / self.api_requests

    @property
    def cache_hit_rate(self) -> float:
        """Get cache hit rate as percentage."""
        total_cache_requests = self.cache_hits + self.cache_misses
        if total_cache_requests == 0:
            return 0.0
        return (self.cache_hits / total_cache_requests) * 100.0

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "api_requests": self.api_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "validation_errors": self.validation_errors,
            "transformation_errors": self.transformation_errors,
            "fallback_activations": self.fallback_activations,
            "average_processing_time_ms": self.average_processing_time_ms,
            "cache_hit_rate": self.cache_hit_rate,
        }


class SymbolIntegrationService:
    """Unified symbol integration service for API layer.

    This service provides a single interface for all symbol operations across
    the API layer, integrating the new symbol system with existing infrastructure
    while maintaining backward compatibility.
    """

    def __init__(
        self,
        enable_fallback: bool = True,
        enable_metrics: bool = True,
        cache_timeout_seconds: float = 5.0,
    ) -> None:
        """Initialize symbol integration service.

        Args:
            enable_fallback: Whether to enable fallback strategies
            enable_metrics: Whether to collect performance metrics
            cache_timeout_seconds: Timeout for cache operations
        """
        self.service = SymbolService()
        self.validator = SymbolValidator()

        self.enable_fallback = enable_fallback
        self.enable_metrics = enable_metrics
        self.cache_timeout_seconds = cache_timeout_seconds

        # Metrics collection
        self.metrics = SymbolIntegrationMetrics() if enable_metrics else None

        # Cache for expensive operations
        self._symbol_cache: dict[str, Any] = {}
        self._cache_timestamps: dict[str, datetime] = {}

    async def get_internal_symbol(
        self,
        exchange_symbol: str,
        exchange_id: str | ExchangeName,
        **kwargs: Unpack[TransformationKwargs],
    ) -> InternalSymbol:
        """Get internal symbol from exchange symbol.

        Args:
            exchange_symbol: Exchange-specific symbol
            exchange_id: Exchange identifier
            **kwargs: Additional parameters for transformation

        Returns:
            InternalSymbol instance

        Raises:
            SymbolNotFoundError: If symbol not found
            SymbolValidationError: If symbol validation fails
        """
        start_time = time.perf_counter()

        try:
            # Normalize exchange_id
            normalized_exchange_id = self._normalize_exchange_id(exchange_id)

            # Check cache first
            cache_key = f"internal:{normalized_exchange_id.value}:{exchange_symbol}"
            cached_result = self._check_cache_for_internal_symbol(cache_key)
            if cached_result:
                return cached_result

            # Try registry lookup with fallback
            return self._get_internal_symbol_from_registry_or_fallback(
                exchange_symbol, normalized_exchange_id, cache_key, **kwargs
            )

        finally:
            if self.metrics:
                processing_time = (time.perf_counter() - start_time) * 1000
                self.metrics.record_api_request(processing_time)

    def _normalize_exchange_id(self, exchange_id: str | ExchangeName) -> ExchangeName:
        """Normalize exchange ID to ExchangeName enum."""
        if not isinstance(exchange_id, ExchangeName):
            return ExchangeName(exchange_id.lower())
        return exchange_id

    def _check_cache_for_internal_symbol(self, cache_key: str) -> InternalSymbol | None:
        """Check cache for internal symbol and record metrics."""
        cached_result = self._get_from_cache(cache_key)
        if cached_result and isinstance(cached_result, InternalSymbol):
            if self.metrics:
                self.metrics.record_cache_hit()
            return cached_result

        if self.metrics:
            self.metrics.record_cache_miss()
        return None

    def _get_internal_symbol_from_registry_or_fallback(
        self,
        exchange_symbol: str,
        exchange_id: ExchangeName,
        cache_key: str,
        **kwargs: Unpack[TransformationKwargs],
    ) -> InternalSymbol:
        """Get internal symbol from registry or use fallback transformer."""
        try:
            result = self.service.get_internal_symbol(exchange_symbol, exchange_id.value)
        except SymbolNotFoundError:
            return self._fallback_to_transformer(exchange_symbol, exchange_id, cache_key, **kwargs)
        else:
            self._set_cache(cache_key, result)
            return result

    def _fallback_to_transformer(
        self,
        exchange_symbol: str,
        exchange_id: ExchangeName,
        cache_key: str,
        **kwargs: Unpack[TransformationKwargs],
    ) -> InternalSymbol:
        """Try transformer as fallback when registry lookup fails."""
        if not self.enable_fallback:
            raise SymbolNotFoundError(exchange_symbol, f"exchange {exchange_id.value}")

        if self.metrics:
            self.metrics.record_fallback_activation()

        try:
            result = self.service.get_internal_symbol(exchange_symbol, exchange_id.value)
        except Exception as e:
            if self.metrics:
                self.metrics.record_transformation_error()
            raise SymbolNotFoundError(
                exchange_symbol,
                f"exchange {exchange_id.value}",
                details={"error": str(e)},
            ) from e
        else:
            self._set_cache(cache_key, result)
            return result

    async def get_exchange_symbol(
        self,
        internal_symbol: str,
        exchange_id: str | ExchangeName,
        market_type: MarketType | None = None,
    ) -> ExchangeSymbol:
        """Get exchange symbol from internal symbol.

        Args:
            internal_symbol: Internal symbol value
            exchange_id: Exchange identifier
            market_type: Optional market type for transformation

        Returns:
            ExchangeSymbol instance

        Raises:
            SymbolNotFoundError: If symbol not found
            SymbolValidationError: If symbol validation fails
        """
        start_time = time.perf_counter()

        try:
            # Normalize exchange_id
            normalized_exchange_id = self._normalize_exchange_id(exchange_id)

            # Check cache first
            cache_key = f"exchange:{internal_symbol}:{normalized_exchange_id.value}"
            cached_result = self._check_cache_for_exchange_symbol(cache_key)
            if cached_result:
                return cached_result

            # Try registry lookup with fallback
            return self._get_exchange_symbol_from_registry_or_fallback(
                internal_symbol, normalized_exchange_id, cache_key, market_type
            )

        finally:
            if self.metrics:
                processing_time = (time.perf_counter() - start_time) * 1000
                self.metrics.record_api_request(processing_time)

    def _check_cache_for_exchange_symbol(self, cache_key: str) -> ExchangeSymbol | None:
        """Check cache for exchange symbol and record metrics."""
        cached_result = self._get_from_cache(cache_key)
        if cached_result and isinstance(cached_result, ExchangeSymbol):
            if self.metrics:
                self.metrics.record_cache_hit()
            return cached_result

        if self.metrics:
            self.metrics.record_cache_miss()
        return None

    def _get_exchange_symbol_from_registry_or_fallback(
        self,
        internal_symbol: str,
        exchange_id: ExchangeName,
        cache_key: str,
        market_type: MarketType | None,
    ) -> ExchangeSymbol:
        """Get exchange symbol from registry or use fallback transformer."""
        try:
            result = self.service.get_exchange_symbol(internal_symbol, exchange_id.value)
        except SymbolNotFoundError:
            return self._fallback_to_transformer_for_exchange(
                internal_symbol, exchange_id, cache_key, market_type
            )
        else:
            self._set_cache(cache_key, result)
            return result

    def _fallback_to_transformer_for_exchange(
        self,
        internal_symbol: str,
        exchange_id: ExchangeName,
        cache_key: str,
        market_type: MarketType | None,
    ) -> ExchangeSymbol:
        """Try transformer as fallback when registry lookup fails."""
        if not self.enable_fallback:
            raise SymbolNotFoundError(internal_symbol, f"exchange {exchange_id.value}")

        if self.metrics:
            self.metrics.record_fallback_activation()

        try:
            result = self.service.get_exchange_symbol(internal_symbol, exchange_id.value)
        except Exception as e:
            if self.metrics:
                self.metrics.record_transformation_error()
            raise SymbolNotFoundError(
                internal_symbol,
                f"exchange {exchange_id.value}",
                details={"error": str(e)},
            ) from e
        else:
            self._set_cache(cache_key, result)
            return result

    def get_base_symbol(self, symbol: str) -> str:
        """Get base symbol from trading pair (Portfolio compatibility).

        Args:
            symbol: Symbol to parse

        Returns:
            Base asset symbol
        """
        # Use fallback parsing directly since service doesn't have get_base_symbol
        if self.metrics and self.enable_fallback:
            self.metrics.record_fallback_activation()
        return self._parse_base_symbol_fallback(symbol)

    def normalize_symbol(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol for exchange (Portfolio compatibility).

        Args:
            symbol: Symbol to normalize
            exchange_id: Exchange identifier string

        Returns:
            Normalized symbol for the exchange
        """
        # Use fallback normalization directly since service doesn't have normalize_symbol
        if self.metrics and self.enable_fallback:
            self.metrics.record_fallback_activation()
        return self._apply_fallback_normalization(symbol, exchange_id)

    def get_symbol_metadata(self, symbol: str) -> dict[str, Any]:
        """Get symbol metadata (Portfolio compatibility).

        Args:
            symbol: Symbol to get metadata for

        Returns:
            Dictionary with symbol metadata
        """
        # Use fallback metadata generation directly since service doesn't have get_symbol_metadata
        if self.metrics and self.enable_fallback:
            self.metrics.record_fallback_activation()
        return self._generate_fallback_metadata(symbol)

    def validate_symbol(
        self,
        symbol: str,
        exchange_id: str,
        symbol_type: str | None = None,
    ) -> bool:
        """Validate symbol for exchange.

        Args:
            symbol: Symbol to validate
            exchange_id: Exchange identifier
            symbol_type: Optional symbol type for validation

        Returns:
            True if valid, False otherwise
        """
        try:
            # Convert exchange_id
            exchange_name = ExchangeName(exchange_id.lower())

            # Determine symbol type
            sym_type = SymbolType(symbol_type.lower()) if symbol_type else SymbolType.EXCHANGE

            # Use unified validator
            self.validator.validate_symbol(symbol, sym_type, exchange_name)
        except (
            SymbolError,
            SymbolNotFoundError,
            SymbolRegistryError,
            SymbolValidationError,
            CoreFieldError,
            TypeFieldError,
            ValueError,
            KeyError,
            AttributeError,
        ):
            if self.metrics:
                self.metrics.record_validation_error()
            # Log the specific error for debugging without exposing sensitive details
            # Note: Using a more specific exception set improves error handling
            return False
        else:
            return True

    def _raise_invalid_websocket_symbol(self, value: str | int) -> NoReturn:
        """Raise SymbolValidationError for invalid WebSocket symbol."""
        raise SymbolValidationError(str(value), "Invalid WebSocket symbol")

    async def validate_websocket_symbol(
        self,
        value: str | int,
        field_name: str,
        exchange_id: str | ExchangeName,
    ) -> str:
        """Validate symbol from WebSocket with integer support.

        Args:
            value: Symbol value (string or integer)
            field_name: Field name for context
            exchange_id: Exchange identifier

        Returns:
            Normalized symbol string

        Raises:
            SymbolValidationError: If validation fails
        """
        try:
            # Normalize exchange_id
            if not isinstance(exchange_id, ExchangeName):
                exchange_id = ExchangeName(exchange_id.lower())

            # Use direct validation for WebSocket symbols
            return str(value)
        except (
            SymbolError,
            SymbolNotFoundError,
            SymbolRegistryError,
            SymbolValidationError,
            CoreFieldError,
            TypeFieldError,
            ValueError,
            KeyError,
            AttributeError,
            RuntimeError,
        ) as e:
            if self.metrics:
                self.metrics.record_validation_error()
            raise SymbolValidationError(
                str(value), f"WebSocket symbol validation failed: {e}"
            ) from e

    async def batch_transform_symbols(
        self,
        symbols: list[str],
        from_exchange: str | ExchangeName,
        to_exchange: str | ExchangeName,
        **kwargs: Unpack[TransformationKwargs],
    ) -> dict[str, Any]:
        """Transform multiple symbols between exchanges.

        Args:
            symbols: List of symbols to transform
            from_exchange: Source exchange
            to_exchange: Target exchange
            **kwargs: Additional transformation parameters

        Returns:
            Dictionary with success/failure statistics
        """
        # Normalize exchange names
        from_exchange = (
            ExchangeName(from_exchange.lower())
            if not isinstance(from_exchange, ExchangeName)
            else from_exchange
        )
        to_exchange = (
            ExchangeName(to_exchange.lower())
            if not isinstance(to_exchange, ExchangeName)
            else to_exchange
        )

        # Use service for batch operations
        result = self.service.batch_transform_symbols(symbols, from_exchange.value)
        return {
            "successful_transforms": result.successful_transforms,
            "failed_transforms": result.failed_transforms,
            "success_count": result.success_count,
            "failure_count": result.failure_count,
            "success_rate": result.success_rate,
        }

    def validate_arbitrage_compatibility(
        self,
        internal_symbol: str,
        exchange_ids: list[str | ExchangeName],
    ) -> dict[str, Any]:
        """Validate symbol compatibility for arbitrage across exchanges.

        Args:
            internal_symbol: Internal symbol to validate
            exchange_ids: List of exchanges for arbitrage

        Returns:
            Dictionary with compatibility results
        """
        # Normalize exchange IDs
        normalized_exchanges: list[ExchangeName] = []
        for exchange_id in exchange_ids:
            normalized_exchange = (
                ExchangeName(exchange_id.lower())
                if not isinstance(exchange_id, ExchangeName)
                else exchange_id
            )
            normalized_exchanges.append(normalized_exchange)

        # Use service for arbitrage validation
        result = self.service.validate_arbitrage_compatibility(
            internal_symbol, [ex.value for ex in normalized_exchanges]
        )
        return {
            "is_arbitrage_compatible": result.is_arbitrage_compatible,
            "exchange_availability": result.exchange_availability,
            "compatibility_warnings": result.compatibility_warnings,
            "available_exchanges": result.available_exchanges,
            "unavailable_exchanges": result.unavailable_exchanges,
        }

    def get_integration_statistics(self) -> dict[str, Any]:
        """Get comprehensive integration statistics.

        Returns:
            Dictionary with integration statistics
        """
        stats: dict[str, Any] = {
            "service_config": {
                "enable_fallback": self.enable_fallback,
                "enable_metrics": self.enable_metrics,
                "cache_timeout_seconds": self.cache_timeout_seconds,
            },
            "cache_info": {
                "total_entries": len(self._symbol_cache),
                "active_entries": self._count_active_cache_entries(),
            },
        }

        # Add metrics if enabled
        if self.metrics:
            stats["metrics"] = self.metrics.to_dict()

        # Add service stats
        stats["service_stats"] = {
            "supported_exchanges": self.service.get_supported_exchanges(),
            "total_symbols": len(self.service.get_all_symbols()),
        }

        return stats

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._symbol_cache.clear()
        self._cache_timestamps.clear()

        # Clear service cache
        self.service.clear()

    @asynccontextmanager
    async def performance_context(self, operation_name: str) -> AsyncIterator[None]:
        """Context manager for performance tracking."""
        start_time = time.perf_counter()
        try:
            yield
        finally:
            if self.metrics:
                processing_time = (time.perf_counter() - start_time) * 1000
                self.metrics.record_api_request(processing_time)

    def _get_from_cache(self, key: str) -> InternalSymbol | ExchangeSymbol | None:
        """Get value from cache with TTL check."""
        if key not in self._symbol_cache:
            return None

        timestamp = self._cache_timestamps.get(key)
        if timestamp is None:
            return None

        # Check TTL
        age_seconds = (datetime.now(UTC) - timestamp).total_seconds()
        if age_seconds > self.cache_timeout_seconds:
            # Remove expired entry
            self._symbol_cache.pop(key, None)
            self._cache_timestamps.pop(key, None)
            return None

        value = self._symbol_cache[key]
        if isinstance(value, (InternalSymbol, ExchangeSymbol)):
            return value
        return None

    def _set_cache(self, key: str, value: InternalSymbol | ExchangeSymbol) -> None:
        """Set value in cache with timestamp."""
        self._symbol_cache[key] = value
        self._cache_timestamps[key] = datetime.now(UTC)

    def _count_active_cache_entries(self) -> int:
        """Count non-expired cache entries."""
        now = datetime.now(UTC)
        active_count = 0

        for timestamp in self._cache_timestamps.values():
            age_seconds = (now - timestamp).total_seconds()
            if age_seconds <= self.cache_timeout_seconds:
                active_count += 1

        return active_count

    def _parse_base_symbol_fallback(self, symbol: str) -> str:
        """Parse base symbol using fallback logic."""
        if not symbol:
            return symbol

        # Common separators
        for sep in ["-", "/", "_", ":"]:
            if sep in symbol:
                parts = symbol.split(sep, 1)
                if parts[0]:
                    return parts[0].strip().upper()

        return symbol.strip().upper()

    def _apply_fallback_normalization(self, symbol: str, exchange_id: str) -> str:
        """Apply exchange-specific normalization rules."""
        exchange_lower = exchange_id.lower()

        if exchange_lower == "hyperliquid":
            return symbol.replace("_", "-")  # Hyperliquid uses hyphens
        if exchange_lower == "backpack":
            return symbol.replace("-", "_")  # Backpack uses underscores

        return symbol

    def _generate_fallback_metadata(self, symbol: str) -> dict[str, Any]:
        """Generate fallback metadata for unknown symbols."""
        base_symbol = self._parse_base_symbol_fallback(symbol)
        is_derivative = "PERP" in symbol.upper() or "-PERP" in symbol.upper()
        is_spot = any(sep in symbol for sep in ["/", "_", "-"]) and not is_derivative

        return {
            "symbol": symbol,
            "base_symbol": base_symbol,
            "quote_symbol": None,
            "is_derivative": is_derivative,
            "is_spot": is_spot,
            "exchange_type": "perp" if is_derivative else "spot" if is_spot else None,
            "tick_size": None,
            "min_order_size": None,
            "max_order_size": None,
            "source": "fallback",
            "last_updated": datetime.now(UTC).isoformat(),
        }


class _SymbolIntegrationServiceSingleton:
    """Singleton holder for SymbolIntegrationService."""

    def __init__(self) -> None:
        """Initialize singleton holder."""
        self._instance: SymbolIntegrationService | None = None

    def get_instance(
        self,
        enable_fallback: bool = True,
        enable_metrics: bool = True,
        cache_timeout_seconds: float = 5.0,
    ) -> SymbolIntegrationService:
        """Get or create the service instance."""
        if self._instance is None:
            self._instance = SymbolIntegrationService(
                enable_fallback=enable_fallback,
                enable_metrics=enable_metrics,
                cache_timeout_seconds=cache_timeout_seconds,
            )
        return self._instance

    def reset_instance(self) -> None:
        """Reset the service instance (for testing)."""
        self._instance = None


# Module-level singleton instance
_integration_service_singleton = _SymbolIntegrationServiceSingleton()


def get_symbol_integration_service(
    enable_fallback: bool = True,
    enable_metrics: bool = True,
    cache_timeout_seconds: float = 5.0,
) -> SymbolIntegrationService:
    """Get or create the global symbol integration service.

    Args:
        enable_fallback: Whether to enable fallback strategies
        enable_metrics: Whether to collect performance metrics
        cache_timeout_seconds: Timeout for cache operations

    Returns:
        SymbolIntegrationService instance
    """
    return _integration_service_singleton.get_instance(
        enable_fallback=enable_fallback,
        enable_metrics=enable_metrics,
        cache_timeout_seconds=cache_timeout_seconds,
    )


# Convenience functions for API layer
async def api_get_internal_symbol(
    exchange_symbol: str,
    exchange_id: str | ExchangeName,
    **kwargs: Unpack[TransformationKwargs],
) -> InternalSymbol:
    """Convenience function for getting internal symbol in API layer."""
    service = get_symbol_integration_service()
    return await service.get_internal_symbol(exchange_symbol, exchange_id, **kwargs)


async def api_get_exchange_symbol(
    internal_symbol: str,
    exchange_id: str | ExchangeName,
    market_type: MarketType | None = None,
) -> ExchangeSymbol:
    """Convenience function for getting exchange symbol in API layer."""
    service = get_symbol_integration_service()
    return await service.get_exchange_symbol(internal_symbol, exchange_id, market_type)


def api_normalize_symbol(symbol: str, exchange_id: str) -> str:
    """Convenience function for normalizing symbol in API layer."""
    service = get_symbol_integration_service()
    return service.normalize_symbol(symbol, exchange_id)


def api_get_base_symbol(symbol: str) -> str:
    """Convenience function for getting base symbol in API layer."""
    service = get_symbol_integration_service()
    return service.get_base_symbol(symbol)


async def api_validate_websocket_symbol(
    value: str | int,
    field_name: str,
    exchange_id: str | ExchangeName,
) -> str:
    """Convenience function for validating WebSocket symbol in API layer."""
    service = get_symbol_integration_service()
    return await service.validate_websocket_symbol(value, field_name, exchange_id)
