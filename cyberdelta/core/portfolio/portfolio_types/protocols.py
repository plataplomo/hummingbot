"""Portfolio protocols and interfaces.

This module consolidates all protocol definitions from:
- service_protocols.py
- manager_protocols.py
- serializable_protocol.py
- protocols/*.py
"""

from __future__ import annotations

from abc import abstractmethod
from decimal import Decimal
from typing import Any, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel, Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.models import DerivativePosition as Position, Order, SpotBalance, Trade
from cyberdelta.core.infrastructure.exceptions.base import CoreError as PortfolioError
from cyberdelta.core.portfolio.exceptions.state import StateValidationError
from cyberdelta.core.symbols import Symbol
# Import SpotBalance from core models, not portfolio_types models
from cyberdelta.core.models import SpotBalance as PortfolioSpotBalance

from .models import (
    ExposureMetrics,
    PortfolioState,
    PortfolioUpdate,
    Position as PortfolioPosition,
)


# Type variables
T = TypeVar("T", bound=BaseModel)
T_co = TypeVar("T_co", covariant=True)
K_contra = TypeVar("K_contra", contravariant=True, bound=object)  # Key type for cache
V = TypeVar("V", bound=object)  # Value type for cache

# Constants
HIT_RATE_TOLERANCE = 0.001  # Tolerance for hit rate floating point comparison


# ==================== Service Data Models ====================

class ServiceProtocolValidationError(PortfolioError):
    """Raised when service protocol validation fails."""

    def __init__(
        self,
        field_type: str,
        requirement: str = "must be positive",
    ) -> None:
        """Initialize service protocol validation error."""
        message = f"{field_type} {requirement}"
        super().__init__(
            message,
            error_code="SERVICE_PROTOCOL_VALIDATION_ERROR",
            context={"field_type": field_type, "requirement": requirement},
        )


@dataclass
class ServiceValidationResult:
    """Service validation result structure."""
    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


@dataclass
class ServiceValidationStats:
    """Service validation statistics structure."""
    total_validations: int = Field(default=0, ge=0)
    successful_validations: int = Field(default=0, ge=0)
    failed_validations: int = Field(default=0, ge=0)
    validation_errors: int = Field(default=0, ge=0)

    @field_validator(
        "successful_validations", "failed_validations", "validation_errors", mode="before"
    )
    @classmethod
    def validate_counts(cls, v: int, info: ValidationInfo) -> int:
        """Validate validation counts do not exceed total."""
        field_name = info.field_name
        if field_name in {"successful_validations", "failed_validations"}:
            # These should not exceed total_validations
            total = info.data.get("total_validations", 0)
            if v > total:
                raise StateValidationError(
                    message=f"{field_name} cannot exceed total_validations",
                    validation_errors=[f"{field_name} cannot exceed total_validations"],
                    component="ServiceValidationStats",
                )
        return v


@dataclass
class ServiceResilienceStatus:
    """Service resilience status structure."""
    circuit_breaker_state: str = Field(default="closed")
    retry_count: int = Field(default=0, ge=0)
    last_failure_time: float | None = Field(default=None, ge=0)
    is_healthy: bool = Field(default=True)

    @field_validator("circuit_breaker_state", mode="before")
    @classmethod
    def validate_state(cls, v: str) -> str:
        """Validate circuit breaker state is valid."""
        valid_states = {"open", "closed", "half_open"}
        if v.lower() not in valid_states:
            valid_states_str = ", ".join(valid_states)
            message = f"Invalid circuit breaker state: {v}. Must be one of: {valid_states_str}"
            raise StateValidationError(
                message=message, validation_errors=[message], component="ServiceResilienceStatus"
            )
        return v.lower()


@dataclass
class ServiceCacheStats:
    """Service cache statistics structure."""
    hits: int = Field(default=0, ge=0)
    misses: int = Field(default=0, ge=0)
    hit_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    total_size: int = Field(default=0, ge=0)
    ttl_expirations: int = Field(default=0, ge=0)

    @field_validator("hit_rate", mode="before")
    @classmethod
    def validate_hit_rate(cls, v: float, info: ValidationInfo) -> float:
        """Validate hit rate matches calculated value."""
        hits = info.data.get("hits", 0)
        misses = info.data.get("misses", 0)
        total = hits + misses
        if total > 0:
            calculated_rate = hits / total
            # Allow small floating point differences
            if abs(v - calculated_rate) > HIT_RATE_TOLERANCE:
                message = f"Hit rate {v} doesn't match calculated rate {calculated_rate}"
                raise StateValidationError(
                    message=message, validation_errors=[message], component="ServiceCacheStats"
                )
        return v


@dataclass
class ServiceStateData:
    """Service state data structure for persistence."""
    timestamp: float = Field(gt=0)
    version: str = Field(min_length=1)
    data: dict[str, str | int | float | bool | list[str] | dict[str, str | int | float | bool]] = (
        Field(default_factory=dict)
    )

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: float) -> float:
        """Validate timestamp is positive."""
        if v <= 0:
            raise ServiceProtocolValidationError(field_type="Timestamp")
        return v



# ==================== Core Manager Protocols ====================

@runtime_checkable
class PortfolioManagerProtocol(Protocol[T_co]):
    """Core portfolio management interface."""

    @abstractmethod
    async def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        """Get total portfolio capital in base currency."""
        ...

    @abstractmethod
    async def get_positions(self, exchange_id: str | None = None) -> list[Position]:
        """Get positions, optionally filtered by exchange."""
        ...

    @abstractmethod
    async def get_balances(self, exchange_id: str | None = None) -> dict[str, SpotBalance]:
        """Get balances, optionally filtered by exchange."""
        ...

    @abstractmethod
    async def get_exposure_metrics(self, valuation_asset: str = "USDC") -> ExposureMetrics:
        """Get current exposure metrics."""
        ...

    @abstractmethod
    async def get_portfolio_state(self) -> PortfolioState:
        """Get complete portfolio state."""
        ...

    @abstractmethod
    async def update_portfolio(self, update: PortfolioUpdate) -> None:
        """Apply portfolio update."""
        ...


@runtime_checkable
class BalanceManagerProtocol(Protocol):
    """Protocol for balance management."""

    @abstractmethod
    async def get_balance(self, exchange_id: str, asset: str) -> SpotBalance | None:
        """Get balance for specific asset on exchange."""
        ...

    @abstractmethod
    async def get_all_balances(self, exchange_id: str | None = None) -> dict[str, SpotBalance]:
        """Get all balances, optionally filtered by exchange."""
        ...

    @abstractmethod
    async def update_balance(self, exchange_id: str, balance: SpotBalance) -> None:
        """Update balance for specific asset."""
        ...

    @abstractmethod
    async def update_balances(self, exchange_id: str, balances: list[SpotBalance]) -> None:
        """Update multiple balances."""
        ...


@runtime_checkable
class PositionManagerProtocol(Protocol):
    """Protocol for position management."""

    @abstractmethod
    async def get_position(self, exchange_id: str, symbol: Symbol) -> Position | None:
        """Get position for specific symbol on exchange."""
        ...

    @abstractmethod
    async def get_all_positions(self, exchange_id: str | None = None) -> list[Position]:
        """Get all positions, optionally filtered by exchange."""
        ...

    @abstractmethod
    async def update_position(self, exchange_id: str, position: Position) -> None:
        """Update position for specific symbol."""
        ...

    @abstractmethod
    async def update_positions(self, exchange_id: str, positions: list[Position]) -> None:
        """Update multiple positions."""
        ...

    @abstractmethod
    async def close_position(self, exchange_id: str, symbol: Symbol) -> None:
        """Mark position as closed."""
        ...


@runtime_checkable
class OrderManagerProtocol(Protocol):
    """Protocol for order management."""

    @abstractmethod
    async def get_order(self, exchange_id: str, order_id: str) -> Order | None:
        """Get order by ID."""
        ...

    @abstractmethod
    async def get_open_orders(self, exchange_id: str | None = None) -> list[Order]:
        """Get all open orders, optionally filtered by exchange."""
        ...

    @abstractmethod
    async def add_order(self, exchange_id: str, order: Order) -> None:
        """Add new order."""
        ...

    @abstractmethod
    async def update_order(self, exchange_id: str, order: Order) -> None:
        """Update existing order."""
        ...

    @abstractmethod
    async def cancel_order(self, exchange_id: str, order_id: str) -> None:
        """Cancel order."""
        ...


# ==================== State Manager Protocols ====================

@runtime_checkable
class StateManagerProtocol(Protocol):
    """Protocol for state management with read/write/admin operations."""

    # Read operations
    @abstractmethod
    async def get_balance(self, exchange_id: str, asset: str) -> SpotBalance | None:
        """Get balance for specific asset."""
        ...

    @abstractmethod
    async def get_all_balances(self, exchange_id: str | None = None) -> dict[str, SpotBalance]:
        """Get all balances."""
        ...

    @abstractmethod
    async def get_position(self, exchange_id: str, symbol: Symbol) -> Position | None:
        """Get position for specific symbol."""
        ...

    @abstractmethod
    async def get_all_positions(self, exchange_id: str | None = None) -> list[Position]:
        """Get all positions."""
        ...

    @abstractmethod
    async def get_order(self, exchange_id: str, order_id: str) -> Order | None:
        """Get order by ID."""
        ...

    @abstractmethod
    async def get_open_orders(self, exchange_id: str | None = None) -> list[Order]:
        """Get open orders."""
        ...

    @abstractmethod
    async def get_recent_trades(self, limit: int = 100) -> list[Trade]:
        """Get recent trades."""
        ...

    @abstractmethod
    async def get_portfolio_value(self, base_currency: str = "USDC") -> Decimal:
        """Get total portfolio value."""
        ...

    @abstractmethod
    async def get_exposure_metrics(self, valuation_asset: str = "USDC") -> dict[str, Decimal]:
        """Get exposure metrics."""
        ...

    # Write operations
    @abstractmethod
    async def update_balance(self, exchange_id: str, balance: SpotBalance) -> None:
        """Update balance."""
        ...

    @abstractmethod
    async def update_position(self, exchange_id: str, position: Position) -> None:
        """Update position."""
        ...

    @abstractmethod
    async def add_order(self, exchange_id: str, order: Order) -> None:
        """Add order."""
        ...

    @abstractmethod
    async def update_order(self, exchange_id: str, order: Order) -> None:
        """Update order."""
        ...

    @abstractmethod
    async def add_trade(self, trade: Trade) -> None:
        """Add trade."""
        ...

    # Admin operations
    @abstractmethod
    async def clear_state(self) -> None:
        """Clear all state."""
        ...

    @abstractmethod
    async def export_state(self) -> dict[str, Any]:
        """Export complete state."""
        ...

    @abstractmethod
    async def import_state(self, state_data: dict[str, Any]) -> None:
        """Import state data."""
        ...

    @abstractmethod
    async def create_snapshot(self, snapshot_id: str) -> None:
        """Create state snapshot."""
        ...

    @abstractmethod
    async def restore_snapshot(self, snapshot_id: str) -> None:
        """Restore from snapshot."""
        ...


# ==================== Service Infrastructure Protocols ====================

@runtime_checkable
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

    async def validate_trade(self, trade: object) -> ServiceValidationResult:
        """Validate a trade."""
        ...

    async def execute_with_resilience(
        self, operation: str, func: object, *args: object, **kwargs: object
    ) -> object:
        """Execute operation with resilience."""
        ...

    def get_validation_statistics(self) -> ServiceValidationStats:
        """Get validation statistics."""
        ...

    def get_resilience_status(self) -> ServiceResilienceStatus:
        """Get resilience status."""
        ...


@runtime_checkable
class ServiceLifecycle(Protocol):
    """Protocol for services with managed lifecycle."""

    @property
    def is_initialized(self) -> bool:
        """Check if the service has been initialized."""
        ...

    @property
    def is_running(self) -> bool:
        """Check if the service is currently running."""
        ...

    async def initialize(self) -> None:
        """Initialize the service."""
        ...

    async def start(self) -> None:
        """Start the service."""
        ...

    async def stop(self) -> None:
        """Stop the service."""
        ...

    async def health_check(self) -> bool:
        """Perform a health check on the service."""
        ...


# ==================== Exchange API Interface ====================
# NOTE: Portfolio module should not directly import from cyberdelta.apis.*
# Instead, the concrete ExchangeAPI instances should be injected at runtime
# This maintains clean architecture by inverting the dependency


# ==================== Data Service Protocols ====================

@runtime_checkable
class PriceServiceProtocol(Protocol):
    """Protocol for price data services."""

    async def get_current_price(self, symbol: Symbol, exchange_id: str | None = None) -> Decimal:
        """Get current price for a symbol."""
        ...

    async def get_price_in_currency(
        self, symbol: Symbol, target_currency: str, exchange_id: str | None = None
    ) -> Decimal:
        """Get price converted to target currency."""
        ...

    async def batch_get_prices(
        self, symbols: list[str], target_currency: str
    ) -> dict[str, Decimal]:
        """Get prices for multiple symbols."""
        ...


@runtime_checkable
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

    async def get_stats(self) -> ServiceCacheStats:
        """Get cache statistics."""
        ...


@runtime_checkable
class PersistenceServiceProtocol(Protocol):
    """Protocol for data persistence services."""

    async def save_state(self, state_data: ServiceStateData) -> None:
        """Save portfolio state."""
        ...

    async def load_state(self) -> ServiceStateData:
        """Load portfolio state."""
        ...

    async def backup_state(self, backup_id: str) -> None:
        """Create state backup."""
        ...

    async def restore_state(self, backup_id: str) -> ServiceStateData:
        """Restore state from backup."""
        ...




# ==================== Behavioral Protocols ====================

@runtime_checkable
class CalculatorProtocol(Protocol):
    """Financial calculator interface."""

    @abstractmethod
    async def calculate(self, input_data: Any) -> Any:
        """Perform calculation."""
        ...


@runtime_checkable
class ValidatorProtocol(Protocol):
    """Data validator interface."""

    @abstractmethod
    async def validate(self, data: Any) -> ServiceValidationResult:
        """Validate data."""
        ...


@runtime_checkable
class MetricsCollectorProtocol(Protocol):
    """Metrics collection interface."""

    @abstractmethod
    async def collect_metrics(self) -> dict[str, float]:
        """Collect current metrics."""
        ...

    @abstractmethod
    async def record_metric(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Record a metric value."""
        ...


# ==================== Utility Protocols ====================

@runtime_checkable
class SerializableProtocol(Protocol):
    """Protocol for serializable objects."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        ...

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SerializableProtocol:
        """Create from dictionary."""
        ...


@runtime_checkable
class TimestampedProtocol(Protocol):
    """Protocol for timestamped objects."""

    @property
    def timestamp(self) -> float:
        """Unix timestamp."""
        ...

    @property
    def created_at(self) -> float:
        """Creation timestamp."""
        ...


@runtime_checkable
class Cacheable(Protocol):
    """Protocol for objects that support caching."""

    def invalidate_cache(self) -> None:
        """Invalidate the object's cache."""
        ...

    def warm_cache(self) -> None:
        """Warm the object's cache."""
        ...


# ==================== Event Protocols ====================

@runtime_checkable
class EventHandlerProtocol(Protocol):
    """Protocol for event handlers."""

    @abstractmethod
    async def handle_event(self, event: Any) -> None:
        """Handle an event."""
        ...

    @abstractmethod
    def can_handle(self, event_type: str) -> bool:
        """Check if handler can handle event type."""
        ...


@runtime_checkable
class EventDispatcherProtocol(Protocol):
    """Protocol for event dispatchers."""

    @abstractmethod
    async def dispatch(self, event: Any) -> None:
        """Dispatch an event."""
        ...

    @abstractmethod
    def register_handler(self, event_type: str, handler: EventHandlerProtocol) -> None:
        """Register event handler."""
        ...

    @abstractmethod
    def unregister_handler(self, event_type: str, handler: EventHandlerProtocol) -> None:
        """Unregister event handler."""
        ...


# ==================== Validation Protocols ====================

@runtime_checkable
class ValidationProtocol(Protocol):
    """Protocol for validation operations."""

    @abstractmethod
    async def validate(self, data: Any) -> ServiceValidationResult:
        """Validate data."""
        ...

    @abstractmethod
    def get_validation_rules(self) -> list[str]:
        """Get list of validation rules."""
        ...


@runtime_checkable
class ValidationMiddlewareProtocol(Protocol):
    """Protocol for validation middleware."""

    @abstractmethod
    async def pre_validate(self, data: Any) -> ServiceValidationResult:
        """Pre-operation validation."""
        ...

    @abstractmethod
    async def post_validate(self, data: Any, result: Any) -> ServiceValidationResult:
        """Post-operation validation."""
        ...


# ==================== State Protocols ====================

@runtime_checkable
class StateStorableProtocol(Protocol):
    """Protocol for objects that can be stored as state."""

    @property
    def state_key(self) -> str:
        """Unique key for state storage."""
        ...

    def to_state_dict(self) -> dict[str, Any]:
        """Convert to state dictionary."""
        ...

    @classmethod
    def from_state_dict(cls, data: dict[str, Any]) -> StateStorableProtocol:
        """Create from state dictionary."""
        ...


@runtime_checkable
class StateContainerProtocol(Protocol[T]):
    """Protocol for state containers."""

    @abstractmethod
    def add(self, key: str, value: T) -> None:
        """Add item to container."""
        ...

    @abstractmethod
    def get(self, key: str) -> T | None:
        """Get item from container."""
        ...

    @abstractmethod
    def remove(self, key: str) -> T | None:
        """Remove item from container."""
        ...

    @abstractmethod
    def clear(self) -> None:
        """Clear all items."""
        ...

    @property
    def size(self) -> int:
        """Number of items in container."""
        ...


# ==================== Lifecycle Protocols ====================

@runtime_checkable
class InitializableProtocol(Protocol):
    """Protocol for objects requiring initialization."""

    @property
    def is_initialized(self) -> bool:
        """Check if initialized."""
        ...

    async def initialize(self) -> None:
        """Initialize the object."""
        ...


@runtime_checkable
class DisposableProtocol(Protocol):
    """Protocol for objects requiring cleanup."""

    async def dispose(self) -> None:
        """Clean up resources."""
        ...

    @property
    def is_disposed(self) -> bool:
        """Check if disposed."""
        ...


# ==================== Concurrency Protocols ====================

@runtime_checkable
class LockableProtocol(Protocol):
    """Protocol for objects supporting locking."""

    async def acquire_lock(self, timeout: float | None = None) -> bool:
        """Acquire lock."""
        ...

    async def release_lock(self) -> None:
        """Release lock."""
        ...

    @property
    def is_locked(self) -> bool:
        """Check if locked."""
        ...


@runtime_checkable
class ThreadSafeProtocol(Protocol):
    """Protocol for thread-safe operations."""

    def is_thread_safe(self) -> bool:
        """Check if operations are thread-safe."""
        ...

    async def execute_atomic(self, operation: Any) -> Any:
        """Execute operation atomically."""
        ...