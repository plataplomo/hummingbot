"""Protocol interfaces for portfolio managers."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade
    from cyberdelta.core.portfolio.portfolio_types.calculation_types import PortfolioExposureResult
    from cyberdelta.core.portfolio.portfolio_types.portfolio_models import (
        PortfolioSnapshot,
        PortfolioUpdate,
    )
    from cyberdelta.core.portfolio.portfolio_types.update_models import (
        CapitalSummary,
        ExposureMetrics,
        ManagerStats,
        PnLSummary,
        PortfolioSummary,
        StateBackup,
        StateValidationResult,
    )

T_co = TypeVar("T_co", covariant=True, bound=object)


@runtime_checkable
class PortfolioManagerProtocol(Protocol[T_co]):
    """Protocol for portfolio state managers."""

    @property
    def name(self) -> str:
        """Manager name."""
        ...

    @property
    def is_initialized(self) -> bool:
        """Check if manager is initialized."""
        ...

    async def initialize(self) -> None:
        """Initialize the manager."""
        ...

    async def shutdown(self) -> None:
        """Shutdown the manager."""
        ...


@runtime_checkable
class BalanceManagerProtocol(Protocol):
    """Protocol for balance management operations."""

    async def update_balances(self, exchange_id: str, balances: dict[str, SpotBalance]) -> None:
        """Update balances for an exchange."""
        ...

    async def get_balance(self, exchange_id: str, asset: str) -> SpotBalance | None:
        """Get balance for a specific asset."""
        ...

    async def get_total_balance_in_currency(self, asset: str, target_currency: str) -> Decimal:
        """Get total balance across exchanges in target currency."""
        ...

    async def update_balance_from_trade(self, trade: Trade) -> None:
        """Update balance from trade execution."""
        ...


@runtime_checkable
class PositionManagerProtocol(Protocol):
    """Protocol for position management operations."""

    async def update_positions(self, exchange_id: str, positions: list[DerivativePosition]) -> None:
        """Update positions for an exchange."""
        ...

    async def update_position_from_trade(self, trade: Trade) -> None:
        """Update position from trade execution."""
        ...

    async def get_position(self, exchange_id: str, symbol: str) -> DerivativePosition | None:
        """Get position for a specific symbol."""
        ...

    async def get_positions_by_symbol(self, symbol: str) -> list[DerivativePosition]:
        """Get all positions for a symbol across exchanges."""
        ...


@runtime_checkable
class OrderManagerProtocol(Protocol):
    """Protocol for order management operations."""

    async def update_orders(self, exchange_id: str, orders: list[Order]) -> None:
        """Update orders for an exchange."""
        ...

    async def get_order(self, exchange_id: str, order_id: str) -> Order | None:
        """Get order by ID."""
        ...

    async def get_orders_by_symbol(self, symbol: str) -> list[Order]:
        """Get all orders for a symbol across exchanges."""
        ...


@runtime_checkable
class StateManagerProtocol(Protocol):
    """Protocol for the main portfolio state manager."""

    async def process_trade(self, trade: Trade) -> bool:
        """Process a trade execution."""
        ...

    async def update_from_orchestrator(self, update_data: PortfolioUpdate) -> None:
        """Update portfolio state from orchestrator."""
        ...

    async def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        """Get current portfolio snapshot."""
        ...

    async def calculate_total_pnl(self) -> Decimal:
        """Calculate total portfolio P&L."""
        ...

    async def calculate_exposure_metrics(self) -> ExposureMetrics:
        """Calculate portfolio exposure metrics."""
        ...

    async def get_total_capital(self) -> CapitalSummary:
        """Get total capital summary."""
        ...

    async def get_portfolio_summary(self) -> PortfolioSummary:
        """Get portfolio summary."""
        ...

    async def calculate_portfolio_exposure(self, base_currency: str) -> PortfolioExposureResult:
        """Calculate portfolio exposure in base currency."""
        ...

    async def get_pnl_summary(self) -> PnLSummary:
        """Get P&L summary."""
        ...

    async def get_all_positions(self) -> dict[str, DerivativePosition]:
        """Get all positions across exchanges."""
        ...

    async def get_all_balances(self) -> dict[str, SpotBalance]:
        """Get all balances across exchanges."""
        ...

    async def get_all_orders(self) -> dict[str, Order]:
        """Get all orders across exchanges."""
        ...

    async def get_manager_stats(self) -> ManagerStats:
        """Get manager statistics."""
        ...

    async def update_balances(self, exchange_id: str, balances: dict[str, SpotBalance]) -> None:
        """Update balances for an exchange."""
        ...

    async def update_positions(self, exchange_id: str, positions: list[DerivativePosition]) -> None:
        """Update positions for an exchange."""
        ...

    async def initialize(self) -> None:
        """Initialize the state manager."""
        ...

    async def shutdown(self) -> None:
        """Shutdown the state manager."""
        ...


class StateManagerReadProtocol(Protocol):
    """Read-only operations for state management."""

    async def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        """Get current portfolio snapshot."""
        ...

    async def calculate_total_pnl(self) -> Decimal:
        """Calculate total portfolio P&L."""
        ...

    async def calculate_exposure_metrics(self) -> ExposureMetrics:
        """Calculate portfolio exposure metrics."""
        ...

    async def get_total_capital(self) -> CapitalSummary:
        """Get total capital summary."""
        ...

    async def get_portfolio_summary(self) -> PortfolioSummary:
        """Get portfolio summary."""
        ...

    async def calculate_portfolio_exposure(self, base_currency: str) -> PortfolioExposureResult:
        """Calculate portfolio exposure in base currency."""
        ...

    async def get_pnl_summary(self) -> PnLSummary:
        """Get P&L summary."""
        ...

    async def get_all_positions(self) -> dict[str, DerivativePosition]:
        """Get all positions across exchanges."""
        ...

    async def get_all_balances(self) -> dict[str, SpotBalance]:
        """Get all balances across exchanges."""
        ...

    async def get_all_orders(self) -> dict[str, Order]:
        """Get all orders across exchanges."""
        ...

    async def get_manager_stats(self) -> ManagerStats:
        """Get manager statistics."""
        ...


class StateManagerWriteProtocol(Protocol):
    """Write operations for state management."""

    async def process_trade(self, trade: Trade) -> bool:
        """Process a trade execution."""
        ...

    async def update_from_orchestrator(self, update_data: PortfolioUpdate) -> None:
        """Update portfolio state from orchestrator."""
        ...

    async def update_balances(self, exchange_id: str, balances: dict[str, SpotBalance]) -> None:
        """Update balances for an exchange."""
        ...

    async def update_positions(self, exchange_id: str, positions: list[DerivativePosition]) -> None:
        """Update positions for an exchange."""
        ...


class StateManagerAdminProtocol(Protocol):
    """Administrative operations for state management."""

    async def initialize(self) -> None:
        """Initialize the state manager."""
        ...

    async def shutdown(self) -> None:
        """Shutdown the state manager."""
        ...

    async def reset_state(self) -> None:
        """Reset internal state."""
        ...

    async def validate_state(self) -> StateValidationResult:
        """Validate internal state consistency."""
        ...

    async def backup_state(self) -> StateBackup:
        """Create state backup."""
        ...

    async def restore_state(self, backup_data: StateBackup) -> None:
        """Restore state from backup."""
        ...
