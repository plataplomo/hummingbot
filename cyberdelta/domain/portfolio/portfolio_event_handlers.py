"""Portfolio Domain Event Handlers.

This module contains event handlers for portfolio-related events including:
- Position events (opened, updated, closed, liquidated)
- Balance events (updated, locked, unlocked, settled)
- PnL calculations and event emission
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import msgspec

from cyberdelta.config.models.event_system_config import EventHandlerConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.base_event_handler import EventHandlerActor
from cyberdelta.enums import OrderSide
from cyberdelta.enums.event_bus import HandlerPriority
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.monitoring import BalanceEventType
from cyberdelta.enums.trading import PositionEventType
from cyberdelta.exceptions.portfolio import (
    MissingClosePriceError,
    MissingRealizedPnLError,
)
from cyberdelta.exceptions.trading import (
    BalanceLockError,
    PositionNoneError,
    RealizedPnLNoneError,
)
from cyberdelta.models import DerivativePosition, SpotBalance
from cyberdelta.models.events.core import BalanceEvent, PositionEvent
from cyberdelta.protocols.domain.portfolio.balance_management import BalanceManagerProtocol
from cyberdelta.protocols.domain.portfolio.position_management import PositionManagerProtocol
from cyberdelta.symbols.global_service import get_symbol_service
from cyberdelta.symbols.models import Symbol


if TYPE_CHECKING:
    from cyberdelta.infrastructure.event_bus import EventBus

logger = get_logger(__name__)


# Using existing portfolio protocols from cyberdelta.protocols.domain.portfolio


class PortfolioPositionEventHandler(EventHandlerActor):
    """Handles position-related events for portfolio management.

    This handler processes position events and maintains portfolio state including:
    - Position tracking (opened, updated, closed, liquidated)
    - PnL calculations and updates
    - Position cache management for performance
    - Integration with position service
    """

    def __init__(
        self,
        handler_id: str,
        event_bus: "EventBus",
        config: EventHandlerConfig,
        position_service: PositionManagerProtocol,
    ) -> None:
        """Initialize position event handler.

        Args:
            handler_id: Unique identifier for this handler
            event_bus: EventBus instance for event subscription
            config: Event handler configuration
            position_service: Service for position operations
        """
        super().__init__(handler_id, event_bus, config)
        self._position_service = position_service
        self._symbol_service = get_symbol_service()

    async def on_start(self) -> None:
        """Initialize handler and set up subscriptions."""
        # Subscribe to position events with high priority
        self.event_bus.subscribe(
            PositionEvent, self.handle_with_degradation, priority=HandlerPriority.HIGH
        )

        # Cache warming not needed (would cache Symbol objects which aren't msgspec.Struct)

        await super().on_start()
        logger.info(
            "portfolio_position_handler_started",
            handler_id=self.handler_id,
            subscriptions=["PositionEvent"],
            priority="HIGH",
        )

    async def on_degrade(self) -> None:
        """Handle degraded mode - limit to essential operations only."""
        await super().on_degrade()
        logger.warning(
            "portfolio_position_handler_degraded",
            handler_id=self.handler_id,
            mode="essential_operations_only",
        )

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Handle position events.

        Args:
            event: Position event to process (must be PositionEvent)
        """
        if not isinstance(event, PositionEvent):
            logger.warning(
                "unexpected_event_type",
                event_type=type(event).__name__,
                expected="PositionEvent",
                handler_id=self.handler_id,
            )
            return

        # Convert symbol at boundary
        symbol = self._get_or_create_symbol(event.symbol, event.exchange)

        logger.info(
            "processing_position_event",
            event_type=event.event_type,
            symbol=symbol.value,
            exchange=event.exchange.value,
            position_size=event.size,
            handler_id=self.handler_id,
        )

        # Route to specific handlers based on event type
        if event.event_type == PositionEventType.OPENED:
            await self._handle_position_opened(event, symbol)
        elif event.event_type == PositionEventType.UPDATED:
            await self._handle_position_updated(event, symbol)
        elif event.event_type == PositionEventType.CLOSED:
            await self._handle_position_closed(event, symbol)
        elif event.event_type == PositionEventType.LIQUIDATED:
            await self._handle_position_liquidated(event, symbol)
        # All literal types are handled above, no else needed

    async def _handle_position_opened(self, event: PositionEvent, symbol: Symbol) -> None:
        """Handle position opened event.

        Args:
            event: Position event
            symbol: Converted symbol object
        """
        logger.info(
            "position_opened",
            symbol=symbol.value,
            exchange=event.exchange.value,
            quantity=event.size,
            entry_price=event.average_price,
        )

        # Create position record using DerivativePosition
        new_position = DerivativePosition(
            exchange=event.exchange,
            symbol=symbol,
            side=OrderSide.BUY if event.size > 0 else OrderSide.SELL,
            size=event.size,
            entry_price=event.average_price,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
            unrealized_pnl=event.unrealized_pnl,
            realized_pnl=event.realized_pnl,
        )

        # Update position using position service protocol
        await self._position_service.update_position_directly(symbol, event.exchange, new_position)

        # Symbol created successfully - no caching needed (cache only accepts msgspec.Struct)

    async def _handle_position_updated(self, event: PositionEvent, symbol: Symbol) -> None:
        """Handle position updated event with PnL calculation.

        Args:
            event: Position event
            symbol: Converted symbol object
        """
        # Get current position from all positions
        positions = await self._position_service.get_exchange_positions(event.exchange)
        current_position = positions.get(symbol.value)
        if not current_position:
            logger.warning(
                "position_update_for_nonexistent_position",
                symbol=symbol.value,
                exchange=event.exchange.value,
            )
            return

        # Calculate new average entry price if quantity changed
        new_quantity = event.size
        new_entry_price = event.average_price

        # Calculate unrealized PnL
        unrealized_pnl = event.unrealized_pnl or current_position.unrealized_pnl

        logger.info(
            "position_updated",
            symbol=symbol.value,
            exchange=event.exchange.value,
            old_quantity=current_position.size,
            new_quantity=new_quantity,
            new_entry_price=new_entry_price,
            unrealized_pnl=unrealized_pnl,
        )

        # Update position
        updated_position = DerivativePosition(
            exchange=event.exchange,
            symbol=symbol,
            side=OrderSide.BUY if new_quantity > 0 else OrderSide.SELL,
            size=new_quantity,
            entry_price=new_entry_price,
            unrealized_pnl=unrealized_pnl,
            realized_pnl=current_position.realized_pnl,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
        )

        await self._position_service.update_position_directly(
            symbol, event.exchange, updated_position
        )

    async def _handle_position_closed(self, event: PositionEvent, symbol: Symbol) -> None:
        """Handle position closed event with realized PnL calculation.

        Args:
            event: Position event
            symbol: Converted symbol object

        Raises:
            MissingClosePriceError: If position close event is missing close price.
            MissingRealizedPnLError: If position close event is missing realized PnL.
        """
        # Get current position for PnL calculation from all positions
        positions = await self._position_service.get_exchange_positions(event.exchange)
        current_position = positions.get(symbol.value)
        if not current_position:
            logger.warning(
                "position_close_for_nonexistent_position",
                symbol=symbol.value,
                exchange=event.exchange.value,
            )
            return

        # Calculate realized PnL - require explicit close data
        if event.close_price is None:
            raise MissingClosePriceError(symbol.value)
        if event.realized_pnl is None:
            raise MissingRealizedPnLError(symbol.value)

        exit_price = event.close_price
        realized_pnl = event.realized_pnl

        logger.info(
            "position_closed",
            symbol=symbol.value,
            exchange=event.exchange.value,
            quantity=current_position.size,
            exit_price=exit_price,
            realized_pnl=realized_pnl,
        )

        # Create closed position record (zero size)
        closed_position = DerivativePosition(
            exchange=event.exchange,
            symbol=symbol,
            side=current_position.side,  # Keep original side
            size=Decimal(0),  # Position is closed
            entry_price=None,  # No entry price for closed position
            unrealized_pnl=Decimal(0),  # No unrealized PnL on closed position
            realized_pnl=self._get_current_realized_pnl(current_position) + realized_pnl,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
        )

        await self._position_service.update_position_directly(
            symbol, event.exchange, closed_position
        )

    async def _handle_position_liquidated(self, event: PositionEvent, symbol: Symbol) -> None:
        """Handle position liquidated event.

        Args:
            event: Position event
            symbol: Converted symbol object

        Raises:
            MissingRealizedPnLError: If position liquidation event is missing realized PnL.
        """
        logger.critical(
            "position_liquidated",
            symbol=symbol.value,
            exchange=event.exchange.value,
            quantity=event.size,
            liquidation_price=event.close_price,
            realized_pnl=event.realized_pnl,
        )

        # Get current position from all positions
        positions = await self._position_service.get_exchange_positions(event.exchange)
        current_position = positions.get(symbol.value)
        if not current_position:
            logger.error(
                "liquidation_for_nonexistent_position",
                symbol=symbol.value,
                exchange=event.exchange.value,
            )
            return

        # Calculate final realized PnL - require explicit data
        if event.realized_pnl is None:
            raise MissingRealizedPnLError(symbol.value)
        realized_pnl = event.realized_pnl

        # Create liquidated position record
        liquidated_position = DerivativePosition(
            exchange=event.exchange,
            symbol=symbol,
            side=current_position.side,  # Keep original side
            size=Decimal(0),  # Position is liquidated
            entry_price=None,  # No entry price for liquidated position
            unrealized_pnl=Decimal(0),  # No unrealized PnL on liquidated position
            realized_pnl=self._get_current_realized_pnl(current_position) + realized_pnl,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
        )

        await self._position_service.update_position_directly(
            symbol, event.exchange, liquidated_position
        )

    def _get_or_create_symbol(self, symbol_str: str, exchange: ExchangeName) -> Symbol:
        """Create Symbol object.

        Args:
            symbol_str: Symbol string from event
            exchange: Exchange name

        Returns:
            Symbol object
        """
        # Create new symbol (no caching since cache only accepts msgspec.Struct)
        return self._symbol_service.create_symbol(symbol_str, exchange)

    def _get_current_realized_pnl(self, position: DerivativePosition | None) -> Decimal:
        """Get current realized PnL from position, failing fast on None.

        Args:
            position: Current position or None if no position exists

        Returns:
            Current realized PnL as Decimal

        Raises:
            PositionNoneError: If position is None
            RealizedPnLNoneError: If realized_pnl is None
        """
        if position is None:
            raise PositionNoneError
        if position.realized_pnl is None:
            raise RealizedPnLNoneError
        return position.realized_pnl

    # Cache warming methods removed - cannot cache Symbol objects (not msgspec.Struct)


class PortfolioBalanceEventHandler(EventHandlerActor):
    """Handles balance-related events for portfolio management.

    This handler processes balance events and maintains portfolio state including:
    - Balance tracking (updated, locked, unlocked, settled)
    - Balance cache management for performance
    - Integration with balance service
    """

    def __init__(
        self,
        handler_id: str,
        event_bus: "EventBus",
        config: EventHandlerConfig,
        balance_service: BalanceManagerProtocol,
    ) -> None:
        """Initialize balance event handler.

        Args:
            handler_id: Unique identifier for this handler
            event_bus: EventBus instance for event subscription
            config: Event handler configuration
            balance_service: Service for balance operations
        """
        super().__init__(handler_id, event_bus, config)
        self._balance_service = balance_service
        self._symbol_service = get_symbol_service()

    async def on_start(self) -> None:
        """Initialize handler and set up subscriptions."""
        # Subscribe to balance events with high priority
        self.event_bus.subscribe(
            BalanceEvent, self.handle_with_degradation, priority=HandlerPriority.HIGH
        )

        # Cache warming not needed (would cache Symbol objects which aren't msgspec.Struct)

        await super().on_start()
        logger.info(
            "portfolio_balance_handler_started",
            handler_id=self.handler_id,
            subscriptions=["BalanceEvent"],
            priority="HIGH",
        )

    async def on_degrade(self) -> None:
        """Handle degraded mode - limit to essential operations only."""
        await super().on_degrade()
        logger.warning(
            "portfolio_balance_handler_degraded",
            handler_id=self.handler_id,
            mode="essential_operations_only",
        )

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Handle balance events.

        Args:
            event: Balance event to process (must be BalanceEvent)
        """
        if not isinstance(event, BalanceEvent):
            logger.warning(
                "unexpected_event_type",
                event_type=type(event).__name__,
                expected="BalanceEvent",
                handler_id=self.handler_id,
            )
            return

        # Convert currency symbol at boundary
        asset_symbol = self._get_or_create_symbol(event.currency, event.exchange)

        logger.info(
            "processing_balance_event",
            event_type=event.event_type,
            asset=asset_symbol.value,
            exchange=event.exchange.value,
            old_balance=event.old_balance,
            new_balance=event.new_balance,
            handler_id=self.handler_id,
        )

        # Route to specific handlers based on event type
        if event.event_type == BalanceEventType.UPDATED:
            await self._handle_balance_updated(event, asset_symbol)
        elif event.event_type == BalanceEventType.LOCKED:
            await self._handle_balance_locked(event, asset_symbol)
        elif event.event_type == BalanceEventType.UNLOCKED:
            await self._handle_balance_unlocked(event, asset_symbol)
        elif event.event_type == BalanceEventType.SETTLED:
            await self._handle_balance_settled(event, asset_symbol)
        # All literal types are handled above, no else needed

    async def _handle_balance_updated(self, event: BalanceEvent, asset_symbol: Symbol) -> None:
        """Handle balance updated event.

        Args:
            event: Balance event
            asset_symbol: Converted asset symbol object
        """
        logger.info(
            "balance_updated",
            asset=asset_symbol.value,
            exchange=event.exchange.value,
            old_balance=event.old_balance,
            new_balance=event.new_balance,
        )

        # Create updated balance record - assuming new_balance is the total
        new_balance = SpotBalance(
            exchange=event.exchange,
            asset=asset_symbol,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
            total_quantity=event.new_balance,
            available_quantity=event.new_balance,  # Assuming all is available for simple case
        )

        # Update balance in service
        await self._balance_service.update_balance_directly(
            asset_symbol, event.exchange, new_balance
        )

        # Asset symbol created successfully - no caching needed (cache only accepts msgspec.Struct)

    async def _handle_balance_locked(self, event: BalanceEvent, asset_symbol: Symbol) -> None:
        """Handle balance locked event.

        Args:
            event: Balance event
            asset_symbol: Converted asset symbol object

        Raises:
            BalanceLockError: If locked amount data is missing
        """
        # Require explicit locked amount data
        if event.locked_amount is None:
            raise BalanceLockError(asset_symbol.value, "locked_amount")
        locked_amount = event.locked_amount

        logger.info(
            "balance_locked",
            asset=asset_symbol.value,
            exchange=event.exchange.value,
            locked_amount=locked_amount,
            new_balance=event.new_balance,
        )

        # Get current balance for update from all balances
        balances = await self._balance_service.get_exchange_balances(event.exchange)
        current_balance = balances.get(asset_symbol.value)
        if not current_balance:
            logger.warning(
                "balance_lock_for_nonexistent_balance",
                asset=asset_symbol.value,
                exchange=event.exchange.value,
            )
            return

        # Update balance with locked amount
        updated_balance = SpotBalance(
            exchange=event.exchange,
            asset=asset_symbol,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
            total_quantity=current_balance.total_quantity,  # Total unchanged
            available_quantity=current_balance.total_quantity - locked_amount,  # Reduced by lock
        )

        await self._balance_service.update_balance_directly(
            asset_symbol, event.exchange, updated_balance
        )

    async def _handle_balance_unlocked(self, event: BalanceEvent, asset_symbol: Symbol) -> None:
        """Handle balance unlocked event.

        Args:
            event: Balance event
            asset_symbol: Converted asset symbol object
        """
        logger.info(
            "balance_unlocked",
            asset=asset_symbol.value,
            exchange=event.exchange.value,
            old_balance=event.old_balance,
            new_balance=event.new_balance,
        )

        # Create unlocked balance record
        updated_balance = SpotBalance(
            exchange=event.exchange,
            asset=asset_symbol,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
            total_quantity=event.new_balance,
            available_quantity=event.new_balance,  # Assuming unlocked means available
        )

        await self._balance_service.update_balance_directly(
            asset_symbol, event.exchange, updated_balance
        )

    async def _handle_balance_settled(self, event: BalanceEvent, asset_symbol: Symbol) -> None:
        """Handle balance settled event.

        Args:
            event: Balance event
            asset_symbol: Converted asset symbol object
        """
        logger.info(
            "balance_settled",
            asset=asset_symbol.value,
            exchange=event.exchange.value,
            old_balance=event.old_balance,
            new_balance=event.new_balance,
        )

        # Create final settled balance
        settled_balance = SpotBalance(
            exchange=event.exchange,
            asset=asset_symbol,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
            total_quantity=event.new_balance,
            available_quantity=event.new_balance,  # Settled balance is fully available
        )

        await self._balance_service.update_balance_directly(
            asset_symbol, event.exchange, settled_balance
        )

    def _get_or_create_symbol(self, symbol_str: str, exchange: ExchangeName) -> Symbol:
        """Create Symbol object.

        Args:
            symbol_str: Symbol string from event
            exchange: Exchange name

        Returns:
            Symbol object
        """
        # Create new symbol (no caching since cache only accepts msgspec.Struct)
        return self._symbol_service.create_symbol(symbol_str, exchange)

    def _get_current_realized_pnl(self, position: DerivativePosition | None) -> Decimal:
        """Get current realized PnL from position, failing fast on None.

        Args:
            position: Current position or None if no position exists

        Returns:
            Current realized PnL as Decimal

        Raises:
            PositionNoneError: If position is None
            RealizedPnLNoneError: If realized_pnl is None
        """
        if position is None:
            raise PositionNoneError
        if position.realized_pnl is None:
            raise RealizedPnLNoneError
        return position.realized_pnl

    # Cache warming methods removed - cannot cache Symbol objects (not msgspec.Struct)
