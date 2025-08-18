"""Portfolio update operations.

This module contains all update/modification operations extracted from PortfolioService
to maintain file size under 600 lines while keeping the same business logic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.trading import PositionEventType
from cyberdelta.exceptions.portfolio import MissingPriceDataError
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models import DerivativePosition, SpotBalance
from cyberdelta.models.events import PositionEvent
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.symbols.models import Symbol


if TYPE_CHECKING:
    from cyberdelta.config.models import AppSettings
    from cyberdelta.domain.portfolio.balance_manager import BalanceManager
    from cyberdelta.domain.portfolio.position_manager import PositionManager
    from cyberdelta.domain.portfolio.state_manager import PortfolioStateManager

logger = get_logger(__name__)


class PortfolioUpdates:
    """Update operations for portfolio service.

    This class contains all update/modification operations extracted from PortfolioService
    to maintain file size limits while preserving exact business logic.
    """

    def __init__(
        self,
        config: AppSettings,
        state_manager: PortfolioStateManager,
        balance_manager: BalanceManager,
        position_manager: PositionManager,
        event_bus: EventBus,
    ) -> None:
        """Initialize update operations.

        Args:
            config: Application settings
            state_manager: State management module
            balance_manager: Balance management module
            position_manager: Position management module
            event_bus: Event bus for publishing portfolio events
        """
        self.config = config
        self._state_manager = state_manager
        self._balance_manager = balance_manager
        self._position_manager = position_manager
        self._event_bus = event_bus

    async def initialize_state(self) -> PortfolioState:
        """Initialize portfolio state.

        Returns:
            Initial portfolio state
        """
        initial_state = await self._state_manager.initialize_state()
        logger.info(
            "portfolio_state_initialized",
            balance_count=len(initial_state.balances),
            position_count=len(initial_state.positions),
        )
        return initial_state

    async def save_state(self) -> None:
        """Save current portfolio state to storage."""
        await self._state_manager.save_state()

    async def create_snapshot(self) -> None:
        """Create a snapshot of current portfolio state."""
        await self._state_manager.create_snapshot()

    async def delete_snapshot(self, snapshot_name: str) -> None:
        """Delete a portfolio snapshot.

        Args:
            snapshot_name: Name/timestamp of snapshot to delete
        """
        await self._state_manager.delete_snapshot(snapshot_name)

    async def load_from_storage(self) -> None:
        """Load portfolio state from storage."""
        await self._state_manager.load_from_storage()
        logger.info("portfolio_loaded_from_storage")

    async def update_balance_directly(
        self,
        asset: Symbol,
        exchange: ExchangeName,
        available: Decimal,
        total: Decimal | None = None,
    ) -> SpotBalance:
        """Update balance directly (for reconciliation).

        Args:
            asset: Asset symbol
            exchange: Exchange name
            available: Available balance
            total: Total balance (defaults to available if not provided)

        Returns:
            Updated balance object
        """
        # Create SpotBalance object
        new_balance = SpotBalance(
            exchange=exchange,
            asset=asset,
            timestamp=datetime.now(UTC),
            total_quantity=total if total is not None else available,
            available_quantity=available,
        )

        await self._balance_manager.update_balance_directly(
            asset=asset, exchange=exchange, new_balance=new_balance
        )
        return new_balance

    async def set_position(self, position: DerivativePosition) -> DerivativePosition:
        """Set position directly (for reconciliation).

        Args:
            position: Position to set

        Returns:
            Updated position
        """
        await self._position_manager.set_position(
            symbol=position.symbol, exchange=position.exchange, position=position
        )
        # Publish position update event
        await self._publish_position_update_event(
            position=position,
            event_type=PositionEventType.UPDATED,
        )
        return position

    async def remove_position(self, symbol: Symbol, exchange: ExchangeName) -> None:
        """Remove a position.

        Args:
            symbol: Position symbol
            exchange: Exchange name
        """
        await self._position_manager.remove_position(symbol, exchange)
        logger.info(
            "position_removed",
            symbol=symbol.value,
            exchange=exchange.value,
        )

    async def update_from_fill(self, fill: Fill) -> None:
        """Update portfolio from a fill.

        This is the main entry point for updating portfolio state when
        trades are executed.

        Args:
            fill: Fill to process
        """
        logger.info(
            "updating_portfolio_from_fill",
            fill_id=fill.id,
            symbol=fill.symbol.value,
            exchange=fill.exchange.value,
            side=fill.side.value,
            quantity=float(fill.quantity),
            price=float(fill.price),
        )

        # Update position
        await self._update_position_from_fill(fill)

        # Update balance
        await self._update_balance_from_fill(fill)

        # Save state after updates
        await self.save_state()

        logger.info(
            "portfolio_updated_from_fill",
            fill_id=fill.id,
            symbol=fill.symbol.value,
        )

    async def _update_position_from_fill(self, fill: Fill) -> None:
        """Update position from fill."""
        await self._position_manager.update_position_from_fill(fill)

    async def _update_balance_from_fill(self, fill: Fill) -> None:
        """Update balance from fill."""
        await self._balance_manager.update_balance_from_fill(fill)

    async def _publish_position_update_event(
        self,
        position: DerivativePosition,
        event_type: PositionEventType,
        fill: Fill | None = None,
    ) -> None:
        """Publish position update event.

        Args:
            position: Updated position
            event_type: Type of position event
            fill: Optional fill that triggered the update
        """
        # Note: fill data could be used for detailed event payload if needed

        event = PositionEvent(
            position_id=f"{position.symbol.value}_{position.exchange.value}",
            symbol=position.symbol.value,
            exchange=position.exchange,
            event_type=event_type,
            size=position.size,
            average_price=position.entry_price
            if position.entry_price is not None
            else (
                position.mark_price
                if position.mark_price is not None
                else self._fail_on_missing_price(position)
            ),
            realized_pnl=position.realized_pnl,
            unrealized_pnl=position.unrealized_pnl,
        )

        try:
            await self._event_bus.publish(event)
            logger.debug(
                "position_event_published",
                event_type=event_type.value,
                symbol=position.symbol.value,
                exchange=position.exchange.value,
            )
        except Exception as e:
            logger.exception(
                "failed_to_publish_position_event",
                event_type=event_type.value,
                error=str(e),
                symbol=position.symbol.value,
                exchange=position.exchange.value,
            )

    def _fail_on_missing_price(self, position: DerivativePosition) -> Decimal:
        """Fail fast when position has no price available.

        Args:
            position: Position missing price data

        Raises:
            MissingPriceDataError: Always raises with detailed error message
        """
        raise MissingPriceDataError(
            position.symbol.value,
            position.exchange.value,
        )
