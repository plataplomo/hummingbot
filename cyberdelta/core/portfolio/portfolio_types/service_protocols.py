"""Protocol interfaces for portfolio services."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, TypeVar


if TYPE_CHECKING:
    pass

K_contra = TypeVar("K_contra", contravariant=True, bound=object)  # Key type for cache
V = TypeVar("V", bound=object)  # Value type for cache


class PortfolioServiceProtocol(Protocol):
    """Protocol for portfolio infrastructure services."""

    @property
    def name(self) -> str:
        """Service name."""
        ...

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        ...

    async def start(self) -> None:
        """Start the service."""
        ...

    async def stop(self) -> None:
        """Stop the service."""
        ...

    async def health_check(self) -> bool:
        """Perform health check."""
        ...

    async def validate_trade(self, trade: object) -> dict[str, Any]:
        """Validate a trade."""
        ...

    async def execute_with_resilience(
        self, operation: str, func: object, *args: object, **kwargs: object
    ) -> object:
        """Execute operation with resilience."""
        ...

    def get_validation_statistics(self) -> dict[str, Any]:
        """Get validation statistics."""
        ...

    def get_resilience_status(self) -> dict[str, Any]:
        """Get resilience status."""
        ...


class PriceServiceProtocol(Protocol):
    """Protocol for price data services."""

    async def get_current_price(self, symbol: str, exchange_id: str | None = None) -> Decimal:
        """Get current price for a symbol."""
        ...

    async def get_price_in_currency(
        self, symbol: str, target_currency: str, exchange_id: str | None = None
    ) -> Decimal:
        """Get price converted to target currency."""
        ...

    async def batch_get_prices(
        self, symbols: list[str], target_currency: str
    ) -> dict[str, Decimal]:
        """Get prices for multiple symbols."""
        ...


class CacheServiceProtocol(Protocol[K_contra, V]):
    """Protocol for caching services."""

    async def get(self, key: K_contra) -> V | None:
        """Get value from cache."""
        ...

    async def set(self, key: K_contra, value: V, ttl: float | None = None) -> None:
        """Set value in cache with optional TTL."""
        ...

    async def delete(self, key: K_contra) -> bool:
        """Delete value from cache."""
        ...

    async def clear(self) -> None:
        """Clear all cache entries."""
        ...

    async def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        ...


class PersistenceServiceProtocol(Protocol):
    """Protocol for data persistence services."""

    async def save_state(self, state_data: dict[str, Any]) -> None:
        """Save portfolio state."""
        ...

    async def load_state(self) -> dict[str, Any]:
        """Load portfolio state."""
        ...

    async def backup_state(self, backup_id: str) -> None:
        """Create state backup."""
        ...

    async def restore_state(self, backup_id: str) -> dict[str, Any]:
        """Restore state from backup."""
        ...


class SymbolServiceProtocol(Protocol):
    """Protocol for symbol normalization services."""

    def get_base_symbol(self, symbol: str) -> str:
        """Get base symbol from trading pair."""
        ...

    def normalize_symbol(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol for specific exchange."""
        ...

    def get_symbol_metadata(self, symbol: str) -> dict[str, Any]:
        """Get metadata for symbol."""
        ...
