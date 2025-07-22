"""Protocol interfaces for portfolio services."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Protocol, TypeVar

from pydantic import Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.portfolio.exceptions.base import PortfolioError
from cyberdelta.core.portfolio.exceptions.state import StateValidationError


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


if TYPE_CHECKING:
    pass

K_contra = TypeVar("K_contra", contravariant=True, bound=object)  # Key type for cache
V = TypeVar("V", bound=object)  # Value type for cache

# Constants
HIT_RATE_TOLERANCE = 0.001  # Tolerance for hit rate floating point comparison


# Type definitions for removing Any usage - using Pydantic dataclasses
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


@dataclass
class ServiceSymbolMetadata:
    """Service symbol metadata structure."""

    base_asset: str = Field(min_length=1)
    quote_asset: str = Field(min_length=1)
    exchange_type: str = Field(min_length=1)
    is_perpetual: bool = Field(default=False)
    contract_size: float | None = Field(default=None, gt=0)

    @field_validator("base_asset", "quote_asset", "exchange_type", mode="before")
    @classmethod
    def validate_strings(cls, v: str) -> str:
        """Validate asset and exchange type strings are non-empty."""
        if not v or not v.strip():
            raise ServiceProtocolValidationError(
                field_type="Asset and exchange type strings", requirement="cannot be empty"
            )
        return v.strip().upper()

    @field_validator("contract_size", mode="before")
    @classmethod
    def validate_contract_size(cls, v: float | None, info: ValidationInfo) -> float | None:
        """Validate contract size is positive for perpetual contracts."""
        if v is not None and info.data.get("is_perpetual", False) and v <= 0:
            raise ServiceProtocolValidationError(
                field_type="Contract size", requirement="must be positive for perpetual contracts"
            )
        return v


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

    async def get_stats(self) -> ServiceCacheStats:
        """Get cache statistics."""
        ...


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


class SymbolServiceProtocol(Protocol):
    """Protocol for symbol normalization services."""

    def get_base_symbol(self, symbol: str) -> str:
        """Get base symbol from trading pair."""
        ...

    def normalize_symbol(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol for specific exchange."""
        ...

    def get_symbol_metadata(self, symbol: str) -> ServiceSymbolMetadata:
        """Get metadata for symbol."""
        ...
