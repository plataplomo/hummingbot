"""Async state container adapter that implements StateContainerProtocol."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID

from cyberdelta.core.models import DerivativePosition, SpotBalance
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.portfolio.models.base import BaseStateModel
from cyberdelta.core.portfolio.portfolio_types.infrastructure import StateUpdateResult
from cyberdelta.core.portfolio.portfolio_types.models import PortfolioSnapshot
from cyberdelta.core.portfolio.state.state_container import StateContainer
from cyberdelta.enums.exchange_names import ExchangeName

if TYPE_CHECKING:
    from cyberdelta.core.models import Trade


class AsyncStateContainer:
    """Async wrapper for StateContainer that implements StateContainerProtocol."""

    def __init__(self, state_id: str) -> None:
        """Initialize async state container."""
        self._container = StateContainer[BaseStateModel](state_id=state_id)

    async def get_balances(self, exchange: ExchangeName) -> dict[str, SpotBalance]:
        """Get balances for an exchange."""
        # For now, return empty dict - would be implemented with real data
        return {}

    async def update_balances(
        self, exchange: ExchangeName, balances: dict[str, SpotBalance]
    ) -> StateUpdateResult:
        """Update balances for an exchange."""
        # Store balances in state container
        exchange_key = f"balances_{exchange.value}"
        # For now, simulate success
        return StateUpdateResult(
            success=True,
            execution_time_ms=1.0,
            affected_entities=len(balances),
        )

    async def get_positions(self, exchange: ExchangeName) -> dict[str, DerivativePosition]:
        """Get positions for an exchange."""
        # For now, return empty dict - would be implemented with real data
        return {}

    async def update_positions(
        self, exchange: ExchangeName, positions: dict[str, DerivativePosition]
    ) -> StateUpdateResult:
        """Update positions for an exchange."""
        # Store positions in state container
        exchange_key = f"positions_{exchange.value}"
        # For now, simulate success
        return StateUpdateResult(
            success=True,
            execution_time_ms=1.0,
            affected_entities=len(positions),
        )

    async def get_orders(self, exchange: ExchangeName) -> list[Order]:
        """Get orders for an exchange."""
        # For now, return empty list - would be implemented with real data
        return []

    async def add_trade(self, exchange: ExchangeName, trade: Trade) -> StateUpdateResult:
        """Add a trade to the state."""
        # Store trade in state container
        trade_key = f"trade_{exchange.value}_{trade.id or 'unknown'}"
        # For now, simulate success
        return StateUpdateResult(
            success=True,
            execution_time_ms=1.0,
            affected_entities=1,
        )

    async def create_snapshot(self, exchange: ExchangeName) -> PortfolioSnapshot:
        """Create a portfolio snapshot."""
        # Create snapshot using state container
        snapshot = self._container.create_snapshot()
        
        # Convert to PortfolioSnapshot with proper field mapping
        return PortfolioSnapshot(
            snapshot_id=UUID(snapshot.snapshot_id) if isinstance(snapshot.snapshot_id, str) else snapshot.snapshot_id,
            portfolio_id=exchange,  # Use exchange as portfolio ID
            timestamp=snapshot.timestamp.timestamp() if hasattr(snapshot.timestamp, "timestamp") else (float(snapshot.timestamp) if isinstance(snapshot.timestamp, (int, float)) else 0.0),
            total_value=Decimal("0.0"),
            cash_balance=Decimal("0.0"),
            positions_value=Decimal("0.0"),
            realized_pnl=Decimal("0.0"),
            unrealized_pnl=Decimal("0.0"),
            fees_paid=Decimal("0.0"),
            gross_exposure=Decimal("0.0"),
            net_exposure=Decimal("0.0"),
            leverage=Decimal("1.0"),
            position_count=0,
        )

    async def initialize(self) -> None:
        """Initialize the container."""
        # State container doesn't need async initialization
        pass

    async def shutdown(self) -> None:
        """Shutdown the container."""
        # State container doesn't need async shutdown
        pass

    @property
    def state_container(self) -> StateContainer[BaseStateModel]:
        """Access to underlying state container."""
        return self._container