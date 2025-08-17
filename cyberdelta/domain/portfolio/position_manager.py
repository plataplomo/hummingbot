"""Position management operations for portfolio service.

This module handles all position-related operations including:
- Position tracking and updates
- Position lifecycle management
- PnL calculations for positions
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import msgspec
import structlog

from cyberdelta.config import AppSettings
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.exceptions.portfolio import PortfolioStateNotInitializedError, PositionNotFoundError
from cyberdelta.models import DerivativePosition
from cyberdelta.models.events.queries import PositionQuery, PositionQueryResponse
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.portfolio.fill_result import FillApplicationResult, PositionChangeResult
from cyberdelta.models.portfolio.pnl_report import ReconciliationReport
from cyberdelta.protocols.domain.portfolio import (
    PortfolioStateManagerProtocol,
    PositionManagerProtocol,
)
from cyberdelta.protocols.financial import PnLCalculatorProtocol
from cyberdelta.symbols.global_service import get_symbol_service
from cyberdelta.symbols.models import Symbol


if TYPE_CHECKING:
    from cyberdelta.infrastructure.event_bus import EventBus


logger = structlog.get_logger(__name__)


class PositionManager(PositionManagerProtocol):
    """Manages position operations for the portfolio service.

    This class handles:
    - Position tracking and updates from trades
    - Position lifecycle (open, update, close)
    - Position-specific PnL calculations
    """

    def __init__(
        self,
        config: AppSettings,
        state_manager: PortfolioStateManagerProtocol,
        pnl_calculator: PnLCalculatorProtocol,
        event_bus: "EventBus",  # REQUIRED - no fallbacks
    ) -> None:
        """Initialize position manager.

        Args:
            config: Application settings
            state_manager: Portfolio state manager
            pnl_calculator: PnL calculator for centralized calculations (required)
            event_bus: Event bus for async queries (REQUIRED)
        """
        self.config = config
        self._state_manager = state_manager
        self._pnl_calculator = pnl_calculator
        self._event_bus = event_bus

        # Cache frequently used settings
        self._position_tolerance = config.validation.position_size_tolerance
        self._position_closure_threshold = config.validation.position_closure_threshold
        self._max_position_age = config.validation.max_position_age_seconds

        # Subscribe to position queries
        self._event_bus.subscribe(PositionQuery, self._handle_position_query)
        logger.info("position_manager_initialized_with_event_bus")

    async def has_position(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
    ) -> bool:
        """Check if position exists - NOW TRULY ASYNC with events!

        Args:
            symbol: Trading symbol
            exchange: Exchange name

        Returns:
            True if position exists, False otherwise

        Raises:
            TimeoutError: If query times out
        """
        query = PositionQuery(
            request_id=str(uuid.uuid4()),
            symbol=symbol.value,
            exchange=exchange,
            query_type="exists",
        )

        # Use event bus request/response pattern - this is TRULY async!
        response = await self._event_bus.request(query, timeout_seconds=0.5)

        if isinstance(response, PositionQueryResponse):
            return response.exists

        # No fallback - fail fast
        raise TimeoutError

    async def get_position(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
    ) -> DerivativePosition:
        """Get position for specific symbol on exchange.

        Args:
            symbol: Trading symbol
            exchange: Exchange name

        Returns:
            Position for the symbol/exchange

        Raises:
            PositionNotFoundError: If position doesn't exist
            PortfolioStateNotInitializedError: If portfolio state not initialized
        """
        state = await self._state_manager.get_state()
        if not state:
            raise PortfolioStateNotInitializedError

        key = f"{exchange.value}:{symbol.value}"
        position = state.positions.get(key)
        if position is None:
            raise PositionNotFoundError(symbol.value, exchange.value)
        return position

    async def get_all_positions(self) -> dict[str, DerivativePosition]:
        """Get all positions across all exchanges.

        Returns:
            Dictionary of positions keyed by "{exchange}:{symbol}"
        """
        state = await self._state_manager.get_state()
        if not state:
            return {}
        return state.positions.copy()

    async def get_positions_for_exchange(
        self,
        exchange: ExchangeName,
    ) -> dict[str, DerivativePosition]:
        """Get all positions for a specific exchange.

        Args:
            exchange: Exchange to filter by

        Returns:
            Dictionary of positions keyed by "{exchange}:{symbol}"
        """
        state = await self._state_manager.get_state()
        if not state:
            return {}

        prefix = f"{exchange.value}:"
        return {k: v for k, v in state.positions.items() if k.startswith(prefix)}

    async def update_position_from_fill(self, fill: Fill) -> Decimal:
        """Update position based on fill execution.

        Args:
            fill: Executed fill

        Returns:
            Realized PnL (Decimal(0) if position was opened/increased)
        """
        state = await self._state_manager.get_state()
        if not state:
            return Decimal(0)

        # Fill.exchange is a string, use directly for key
        position_key = f"{fill.exchange}:{fill.symbol.value}"
        position = state.positions.get(position_key)

        # Calculate new position using position management logic
        if position:
            # Apply fill to existing position
            result = self._apply_fill_to_position(position, fill)
            realized_pnl = result.realized_pnl
            new_avg_price = result.new_average_price
            position_closed = result.position_closed

            # Calculate new quantity (signed)
            current_qty = position.size if position.side == OrderSide.BUY else -position.size
            fill_qty = fill.quantity if fill.side == OrderSide.BUY else -fill.quantity
            new_quantity = current_qty + fill_qty
        else:
            new_quantity = fill.quantity if fill.side == OrderSide.BUY else -fill.quantity
            realized_pnl = Decimal(0)  # No None, always Decimal
            new_avg_price = fill.price
            position_closed = abs(new_quantity) < self._position_closure_threshold

        # Update or create position
        if position_closed:  # Position closed
            if position_key in state.positions:
                del state.positions[position_key]
                logger.info(
                    "Position closed",
                    symbol=fill.symbol.value,
                    exchange=fill.exchange,
                    realized_pnl=realized_pnl,
                )
        else:
            # Create/update position
            # Convert fill.exchange string to ExchangeName
            exchange_enum = ExchangeName(fill.exchange)
            state.positions[position_key] = DerivativePosition(
                exchange=exchange_enum,
                symbol=fill.symbol,
                side=OrderSide.BUY if new_quantity > 0 else OrderSide.SELL,
                size=abs(new_quantity),
                entry_price=new_avg_price,
                timestamp=datetime.now(UTC),
                unrealized_pnl=Decimal(0),  # Would calculate based on current price
            )

            logger.info(
                "Position updated",
                symbol=fill.symbol.value,
                exchange=fill.exchange,
                new_size=abs(new_quantity),
                avg_price=new_avg_price,
            )

        # Update state timestamp and save
        state.timestamp = datetime.now(UTC)
        await self._state_manager.save_state()

        return realized_pnl

    async def calculate_position_pnl(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
        current_price: Decimal,
    ) -> dict[str, Decimal]:
        """Calculate PnL for a specific position using centralized calculator.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            current_price: Current market price

        Returns:
            Dictionary with unrealized_pnl and pnl_percentage
        """
        position = await self.get_position(symbol, exchange)
        if not position or not position.entry_price:
            return {"unrealized_pnl": Decimal(0), "pnl_percentage": Decimal(0)}

        # Use centralized calculator for PnL calculation
        pnl_result = self._pnl_calculator.calculate_unrealized_pnl(
            position=position,
            mark_price=current_price,
            include_fees=False,  # Basic calculation without fees
        )

        # Calculate percentage
        position_value = position.size * position.entry_price
        pnl_percentage = (
            (pnl_result.amount / position_value * 100) if position_value > 0 else Decimal(0)
        )

        return {
            "unrealized_pnl": pnl_result.amount,
            "pnl_percentage": pnl_percentage,
        }

    async def get_total_exposure(self) -> Decimal:
        """Get total position exposure across all exchanges in USD.

        Returns:
            Total exposure in USD
        """
        positions = await self.get_all_positions()
        return self._calculate_exposure(positions)

    async def get_exchange_exposure(self, exchange: ExchangeName) -> Decimal:
        """Get total position exposure for a specific exchange in USD.

        Args:
            exchange: Exchange to calculate exposure for

        Returns:
            Total exposure in USD for the exchange
        """
        positions = await self.get_positions_for_exchange(exchange)
        return self._calculate_exposure(positions)

    def _calculate_exposure(self, positions: dict[str, DerivativePosition]) -> Decimal:
        """Calculate total exposure for given positions.

        Args:
            positions: Dictionary of positions

        Returns:
            Total exposure in USD
        """
        total = Decimal(0)
        for position in positions.values():
            if position.entry_price:
                total += position.size * position.entry_price
        return total

    async def validate_position(
        self,
        exchange: ExchangeName,
        symbol: Symbol,
        expected_size: Decimal,
    ) -> bool:
        """Validate position against expected size.

        Args:
            exchange: Exchange name
            symbol: Trading symbol
            expected_size: Expected position size

        Returns:
            True if position is within tolerance
        """
        actual = await self.get_position(symbol, exchange)
        if not actual:
            return abs(expected_size) < self._position_tolerance

        diff = abs(actual.size - abs(expected_size))
        return diff <= self._position_tolerance

    async def reconcile_positions(
        self,
        exchange_positions: list[DerivativePosition],
        exchange: ExchangeName,
    ) -> ReconciliationReport:
        """Reconcile local positions with exchange data.

        Args:
            exchange_positions: Positions from exchange
            exchange: Exchange name

        Returns:
            Typed reconciliation results
        """
        position_discrepancies: list[str] = []
        updated = 0

        state = await self._state_manager.get_state()
        if not state:
            return ReconciliationReport(
                reconciliation_timestamp=datetime.now(UTC),
                reconciliation_successful=False,
                total_discrepancies=0,
                exchange_results={exchange: False},
                balance_discrepancies=[],
                position_discrepancies=[],
                error_messages=["No portfolio state available"],
            )

        # Process each exchange position
        for exchange_position in exchange_positions:
            symbol = exchange_position.symbol
            key = f"{exchange.value}:{symbol.value}"

            # Get exchange position data
            exchange_size = abs(exchange_position.size)

            # Get local position
            local_position = state.positions.get(key)

            if local_position:
                # Check for discrepancy
                size_diff = abs(local_position.size - exchange_size)
                if size_diff > self._position_tolerance:
                    position_discrepancies.append(
                        f"{symbol.value}: local={local_position.size}, "
                        f"exchange={exchange_size}, diff={size_diff}"
                    )

                    # Update to match exchange
                    if self.config.state.reconciliation_enabled and exchange_size > 0:
                        # Use the exchange position directly
                        state.positions[key] = exchange_position
                        updated += 1
            elif exchange_size > 0:
                # New position from exchange - use the exchange position directly
                state.positions[key] = exchange_position
                updated += 1

        # Remove positions that don't exist on exchange
        local_keys = [k for k in state.positions if k.startswith(f"{exchange.value}:")]
        exchange_keys = [f"{exchange.value}:{pos.symbol.value}" for pos in exchange_positions]

        for key in local_keys:
            if key not in exchange_keys and self.config.state.reconciliation_enabled:
                del state.positions[key]
                updated += 1

        # Save if updated
        if updated > 0:
            state.timestamp = datetime.now(UTC)
            await self._state_manager.save_state()

        return ReconciliationReport(
            reconciliation_timestamp=datetime.now(UTC),
            reconciliation_successful=len(position_discrepancies) == 0,
            total_discrepancies=len(position_discrepancies),
            exchange_results={exchange: len(position_discrepancies) == 0},
            balance_discrepancies=[],
            position_discrepancies=position_discrepancies,
            error_messages=None,
        )

    async def get_exchange_positions(self, exchange: ExchangeName) -> dict[str, DerivativePosition]:
        """Get all positions for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            Dictionary of symbol -> DerivativePosition for the exchange
        """
        positions = await self.get_positions_for_exchange(exchange)
        result: dict[str, DerivativePosition] = {}

        # Convert from full keys to just symbol strings
        prefix = f"{exchange.value}:"
        for key, position in positions.items():
            if key.startswith(prefix):
                symbol_str = key[len(prefix) :]
                result[symbol_str] = position

        return result

    async def update_position_directly(
        self, symbol: Symbol, exchange: ExchangeName, new_position: DerivativePosition | None
    ) -> None:
        """Update position directly (for reconciliation).

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            new_position: New position to set, None to remove

        Note:
            This method combines update and delete for backward compatibility.
            Consider using set_position or remove_position for clearer intent.
        """
        if new_position is None:
            await self.remove_position(symbol, exchange)
        else:
            await self.set_position(symbol, exchange, new_position)

    async def set_position(
        self, symbol: Symbol, exchange: ExchangeName, new_position: DerivativePosition
    ) -> None:
        """Set position directly (for reconciliation).

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            new_position: New position to set
        """
        state = await self._state_manager.get_state()
        if not state:
            return

        key = f"{exchange.value}:{symbol.value}"
        state.positions[key] = new_position

        logger.info(
            "position_updated_directly",
            exchange=exchange.value,
            symbol=symbol.value,
            side=new_position.side.value,
            size=new_position.size,
            entry_price=new_position.entry_price,
        )

        state.timestamp = datetime.now(UTC)
        await self._state_manager.save_state()

    async def remove_position(self, symbol: Symbol, exchange: ExchangeName) -> None:
        """Remove position directly (for reconciliation).

        Args:
            symbol: Trading symbol
            exchange: Exchange name

        Raises:
            PositionNotFoundError: If position doesn't exist
            PortfolioStateNotInitializedError: If portfolio state not initialized
        """
        state = await self._state_manager.get_state()
        if not state:
            raise PortfolioStateNotInitializedError

        key = f"{exchange.value}:{symbol.value}"
        if key not in state.positions:
            raise PositionNotFoundError(symbol.value, exchange.value)

        del state.positions[key]
        logger.info(
            "position_removed_directly",
            exchange=exchange.value,
            symbol=symbol.value,
        )

        state.timestamp = datetime.now(UTC)
        await self._state_manager.save_state()

    def _apply_fill_to_position(
        self, position: DerivativePosition, fill: Fill
    ) -> FillApplicationResult:
        """Apply a fill to an existing position and calculate realized PnL with calculator.

        This method encapsulates the business logic for updating a position based on a fill,
        including calculating realized PnL when reducing/closing positions and updating
        the average entry price.

        Args:
            position: Existing position to update
            fill: The fill to apply to this position

        Returns:
            FillApplicationResult with:
            - realized_pnl: Realized PnL (Decimal(0) if position opened/increased)
            - new_average_price: Updated average entry price after the fill
            - was_reducing_position: Whether the fill reduced the position
            - position_closed: Whether the position was fully closed

        Note:
            This method calculates but does not update the position. The caller is responsible
            for updating the position fields based on the returned values.
        """
        # Calculate new quantity
        new_quantity = self._calculate_new_quantity(position, fill)

        # Use centralized calculator for realized PnL if position is being reduced
        current_qty = position.size if position.side == OrderSide.BUY else -position.size
        is_reducing = current_qty != 0 and abs(new_quantity) < abs(current_qty)

        if is_reducing:
            # Position is being reduced - use centralized calculator
            pnl_result = self._pnl_calculator.calculate_realized_pnl(
                position=position,
                fill=fill,
                include_fees=False,  # Basic calculation without fees
            )
            realized_pnl = pnl_result.amount
        else:
            realized_pnl = Decimal(0)  # No None, always Decimal

        # Calculate new average price
        new_average_price = self._calculate_average_price(position, fill, new_quantity)

        # Check if position is closed
        position_closed = abs(new_quantity) < self._position_closure_threshold

        return FillApplicationResult(
            realized_pnl=realized_pnl,
            new_average_price=new_average_price,
            was_reducing_position=is_reducing,
            position_closed=position_closed,
        )

    def _calculate_new_quantity(self, position: DerivativePosition, fill: Fill) -> Decimal:
        """Calculate new position quantity after fill.

        Args:
            position: Current position
            fill: New fill to apply

        Returns:
            New position quantity (signed)
        """
        # Current position quantity (signed)
        current_qty = position.size
        if position.side == OrderSide.SELL:
            current_qty = -current_qty

        # Fill quantity (signed)
        fill_qty = fill.quantity
        if fill.side == OrderSide.SELL:
            fill_qty = -fill_qty

        # New position quantity
        return current_qty + fill_qty

    def _calculate_position_change(
        self, position: DerivativePosition, fill: Fill
    ) -> PositionChangeResult:
        """Calculate position change from fill.

        Args:
            position: Current position
            fill: New fill to apply

        Returns:
            PositionChangeResult with new quantity and realized PnL
        """
        # Current position quantity (signed)
        current_qty = position.size
        if position.side == OrderSide.SELL:
            current_qty = -current_qty

        # Fill quantity (signed)
        fill_qty = fill.quantity
        if fill.side == OrderSide.SELL:
            fill_qty = -fill_qty

        # New position quantity
        new_qty = current_qty + fill_qty

        # Calculate realized PnL if reducing/closing position
        is_reducing = current_qty != 0 and abs(new_qty) < abs(current_qty)

        if is_reducing:
            # Position is being reduced
            reduced_qty = abs(current_qty) - abs(new_qty)
            if position.entry_price:
                if current_qty > 0:  # Was long
                    realized_pnl = reduced_qty * (fill.price - position.entry_price)
                else:  # Was short
                    realized_pnl = reduced_qty * (position.entry_price - fill.price)
            else:
                realized_pnl = Decimal(0)
        else:
            realized_pnl = Decimal(0)

        return PositionChangeResult(
            new_quantity=new_qty, realized_pnl=realized_pnl, was_reducing=is_reducing
        )

    def _calculate_average_price(
        self, position: DerivativePosition, fill: Fill, new_quantity: Decimal
    ) -> Decimal:
        """Calculate new average price after fill.

        Args:
            position: Current position
            fill: New fill
            new_quantity: New position quantity (signed)

        Returns:
            New average price
        """
        # If position flipped sides, use trade price
        old_signed_qty = position.size
        if position.side == OrderSide.SELL:
            old_signed_qty = -old_signed_qty

        if (old_signed_qty > 0 and new_quantity < 0) or (old_signed_qty < 0 and new_quantity > 0):
            return fill.price

        # Calculate weighted average for same-side fills
        if not position.entry_price:
            return fill.price

        old_value = abs(old_signed_qty) * position.entry_price
        fill_value = fill.quantity * fill.price
        total_value = old_value + fill_value
        total_quantity = abs(old_signed_qty) + fill.quantity

        if total_quantity == 0:
            return fill.price

        return total_value / total_quantity

    async def _handle_position_query(self, query: msgspec.Struct) -> None:
        """Handle position queries via event bus.

        This method processes PositionQuery events and sends responses,
        enabling truly async position operations.

        Args:
            query: PositionQuery event
        """
        if not isinstance(query, PositionQuery):
            return

        try:
            # Get symbol from the symbol service
            symbol_service = get_symbol_service()
            symbol = symbol_service.create_symbol(query.symbol, query.exchange)

            if query.query_type == "exists":
                # Check existence directly from state
                state = await self._state_manager.get_state()
                exists = False
                if state:
                    key = f"{query.exchange.value}:{query.symbol}"
                    exists = key in state.positions

                response = PositionQueryResponse(request_id=query.request_id, exists=exists)

            elif query.query_type == "details":
                # Get position details
                try:
                    position = await self.get_position(symbol, query.exchange)
                    response = PositionQueryResponse(
                        request_id=query.request_id,
                        exists=True,
                        size=position.size,
                        entry_price=position.entry_price,
                        unrealized_pnl=position.unrealized_pnl,
                    )
                except PositionNotFoundError:
                    response = PositionQueryResponse(request_id=query.request_id, exists=False)

            elif query.query_type == "pnl":
                # Get PnL
                try:
                    position = await self.get_position(symbol, query.exchange)
                    response = PositionQueryResponse(
                        request_id=query.request_id,
                        exists=True,
                        unrealized_pnl=position.unrealized_pnl or Decimal(0),
                    )
                except PositionNotFoundError:
                    response = PositionQueryResponse(
                        request_id=query.request_id, exists=False, unrealized_pnl=Decimal(0)
                    )

            else:
                response = PositionQueryResponse(
                    request_id=query.request_id,
                    exists=False,
                    error=f"Unknown query type: {query.query_type}",
                )

            # Send response
            await self._event_bus.respond(query.request_id, response)

        except (PositionNotFoundError, PortfolioStateNotInitializedError) as e:
            logger.exception(
                "position_query_handler_error", request_id=query.request_id, error=str(e)
            )
            error_response = PositionQueryResponse(
                request_id=query.request_id, exists=False, error=str(e)
            )
            await self._event_bus.respond(query.request_id, error_response)
