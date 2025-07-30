"""Protocol definitions for portfolio components."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from cyberdelta.core.models import DerivativePosition, SpotBalance
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.portfolio.portfolio_types.models import PortfolioSnapshot, PortfolioState
from cyberdelta.core.portfolio.portfolio_types.infrastructure import (
    StateUpdateResult,
    StateValidationResult,
)
from cyberdelta.enums.exchange_names import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.core.models import Trade


@runtime_checkable
class Initializable(Protocol):
    """Protocol for components that can be initialized."""

    async def initialize(self) -> None:
        """Initialize the component."""
        ...


@runtime_checkable
class Shutdownable(Protocol):
    """Protocol for components that can be shut down."""

    async def shutdown(self) -> None:
        """Shutdown the component."""
        ...


@runtime_checkable
class LifecycleManaged(Initializable, Shutdownable, Protocol):
    """Protocol for components with full lifecycle management."""


@runtime_checkable
class TradeProcessor(Protocol):
    """Protocol for components that can process trades."""

    async def process_trade(self, trade: Trade) -> bool:
        """Process a trade."""
        ...


@runtime_checkable
class ServiceProvider(Protocol):
    """Protocol for service components."""

    @property
    def initialized(self) -> bool:
        """Check if the service is initialized."""
        ...


@runtime_checkable
class Serializable(Protocol):
    """Protocol for objects that can be serialized to dict."""

    def to_dict(self) -> dict[str, Any]:
        """Convert object to dictionary representation."""
        ...


@runtime_checkable
class ComponentStatus(Protocol):
    """Protocol for components with status tracking."""

    @property
    def is_running(self) -> bool:
        """Check if component is running."""
        ...


# Enhanced protocols following risk module patterns


@runtime_checkable
class StateContainerProtocol(Protocol):
    """Protocol for state container operations."""

    async def get_balances(self, exchange: ExchangeName) -> dict[str, SpotBalance]:
        """Get balances for an exchange."""
        ...

    async def update_balances(
        self, exchange: ExchangeName, balances: dict[str, SpotBalance]
    ) -> StateUpdateResult:
        """Update balances for an exchange."""
        ...

    async def get_positions(self, exchange: ExchangeName) -> dict[str, DerivativePosition]:
        """Get positions for an exchange."""
        ...

    async def update_positions(
        self, exchange: ExchangeName, positions: dict[str, DerivativePosition]
    ) -> StateUpdateResult:
        """Update positions for an exchange."""
        ...

    async def get_orders(self, exchange: ExchangeName) -> list[Order]:
        """Get orders for an exchange."""
        ...

    async def add_trade(self, exchange: ExchangeName, trade: Trade) -> StateUpdateResult:
        """Add a trade to the state."""
        ...

    async def create_snapshot(self, exchange: ExchangeName) -> PortfolioSnapshot:
        """Create a portfolio snapshot."""
        ...


@runtime_checkable
class PriceServiceProtocol(Protocol):
    """Protocol for price service operations."""

    async def get_price(self, symbol: str, exchange: ExchangeName) -> Decimal | None:
        """Get current price for a symbol."""
        ...

    async def get_prices(self, symbols: list[str], exchange: ExchangeName) -> dict[str, Decimal]:
        """Get prices for multiple symbols."""
        ...

    async def get_mark_price(self, symbol: str, exchange: ExchangeName) -> Decimal | None:
        """Get mark price for a symbol."""
        ...


@runtime_checkable
class ValidationServiceProtocol(Protocol):
    """Protocol for validation service operations."""

    async def validate_balance(self, balance: SpotBalance, asset: str) -> StateValidationResult:
        """Validate a balance."""
        ...

    async def validate_position(self, position: DerivativePosition) -> StateValidationResult:
        """Validate a position."""
        ...

    async def validate_trade(self, trade: Trade) -> StateValidationResult:
        """Validate a trade."""
        ...

    async def validate_order(self, order: Order) -> StateValidationResult:
        """Validate an order."""
        ...

    async def validate_portfolio_state(self, state: PortfolioState) -> StateValidationResult:
        """Validate entire portfolio state."""
        ...

    # CacheServiceProtocol is now defined in types/service_protocols.py as a generic protocol

    async def clear(self) -> int:
        """Clear all cache entries."""
        ...

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        ...


@runtime_checkable
class MetricsCollectorProtocol(Protocol):
    """Protocol for metrics collection."""

    def record_calculation_time(self, calculator_name: str, duration_ms: float) -> None:
        """Record calculation execution time."""
        ...

    def record_validation_result(
        self, validator_name: str, is_valid: bool, duration_ms: float
    ) -> None:
        """Record validation result."""
        ...

    def record_state_update(self, manager_name: str, success: bool, duration_ms: float) -> None:
        """Record state update result."""
        ...

    def get_metrics(self) -> dict[str, Any]:
        """Get collected metrics."""
        ...
