"""Position management operations for portfolio service.

This module handles all position-related operations including:
- Position tracking and updates
- Position lifecycle management
- PnL calculations for positions
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog

from cyberdelta.config import AppSettings
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.models import DerivativePosition
from cyberdelta.models.market.trade import Trade
from cyberdelta.protocols.domain.portfolio import (
    PortfolioStateManagerProtocol,
    PositionManagerProtocol,
)
from cyberdelta.symbols.global_service import get_symbol_service
from cyberdelta.symbols.models import Symbol


logger = structlog.get_logger(__name__)

# Module-level symbol service initialization
# Following the pattern from momentum_strategy.py - initialize once at module level
# This avoids repeated calls to get_symbol_service() in methods
_symbol_service = get_symbol_service()


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
    ) -> None:
        """Initialize position manager.

        Args:
            config: Application settings
            state_manager: Portfolio state manager
        """
        self.config = config
        self._state_manager = state_manager

        # Cache frequently used settings
        self._position_tolerance = config.validation.position_size_tolerance
        self._position_closure_threshold = config.validation.position_closure_threshold
        self._max_position_age = config.validation.max_position_age_seconds

    async def get_position(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
    ) -> DerivativePosition | None:
        """Get position for specific symbol on exchange.

        Args:
            symbol: Trading symbol
            exchange: Exchange name

        Returns:
            Position if found, None otherwise
        """
        state = await self._state_manager.get_state()
        if not state:
            return None

        key = f"{exchange.value}:{symbol.value}"
        return state.positions.get(key)

    async def get_all_positions(
        self,
        exchange: ExchangeName | None = None,
    ) -> dict[str, DerivativePosition]:
        """Get all positions, optionally filtered by exchange.

        Args:
            exchange: Optional exchange to filter by

        Returns:
            Dictionary of positions
        """
        state = await self._state_manager.get_state()
        if not state:
            return {}

        if exchange:
            return {k: v for k, v in state.positions.items() if k.startswith(f"{exchange.value}:")}
        return state.positions.copy()

    async def update_position_from_trade(self, trade: Trade) -> Decimal | None:
        """Update position based on trade execution.

        Args:
            trade: Executed trade

        Returns:
            Realized PnL if position was closed/reduced, None otherwise
        """
        state = await self._state_manager.get_state()
        if not state:
            return None

        # Trade.exchange is a string, use directly for key
        position_key = f"{trade.exchange}:{trade.symbol.value}"
        position = state.positions.get(position_key)

        # Calculate new position
        if position:
            new_quantity, realized_pnl = self._calculate_position_change(position, trade)
        else:
            new_quantity = trade.quantity if trade.side == OrderSide.BUY else -trade.quantity
            realized_pnl = None

        # Update or create position
        if abs(new_quantity) < self._position_closure_threshold:  # Position closed
            if position_key in state.positions:
                del state.positions[position_key]
                logger.info(
                    "Position closed",
                    symbol=trade.symbol.value,
                    exchange=trade.exchange,
                    realized_pnl=float(realized_pnl) if realized_pnl else 0,
                )
        else:
            # Calculate new average price
            if position:
                new_avg_price = self._calculate_average_price(position, trade, new_quantity)
            else:
                new_avg_price = trade.price

            # Create/update position
            # Convert trade.exchange string to ExchangeName
            exchange_enum = ExchangeName(trade.exchange)
            state.positions[position_key] = DerivativePosition(
                exchange=exchange_enum,
                symbol=trade.symbol,
                side=OrderSide.BUY if new_quantity > 0 else OrderSide.SELL,
                size=abs(new_quantity),
                entry_price=new_avg_price,
                timestamp=datetime.now(UTC),
                unrealized_pnl=Decimal(0),  # Would calculate based on current price
            )

            logger.info(
                "Position updated",
                symbol=trade.symbol.value,
                exchange=trade.exchange,
                new_size=float(abs(new_quantity)),
                avg_price=float(new_avg_price),
            )

        # Update state timestamp and save
        state.timestamp = datetime.now(UTC)
        await self._state_manager.save_state()

        return realized_pnl

    def _calculate_position_change(
        self,
        position: DerivativePosition,
        trade: Trade,
    ) -> tuple[Decimal, Decimal | None]:
        """Calculate position change from trade.

        Args:
            position: Current position
            trade: New trade

        Returns:
            Tuple of (new_quantity, realized_pnl)
        """
        # Current position quantity (signed)
        current_qty = position.size
        if position.side == OrderSide.SELL:
            current_qty = -current_qty

        # Trade quantity (signed)
        trade_qty = trade.quantity
        if trade.side == OrderSide.SELL:
            trade_qty = -trade_qty

        # New position quantity
        new_qty = current_qty + trade_qty

        # Calculate realized PnL if reducing/closing position
        realized_pnl = None
        if current_qty != 0 and abs(new_qty) < abs(current_qty):
            # Position is being reduced
            reduced_qty = abs(current_qty) - abs(new_qty)
            if position.entry_price:
                if current_qty > 0:  # Was long
                    realized_pnl = reduced_qty * (trade.price - position.entry_price)
                else:  # Was short
                    realized_pnl = reduced_qty * (position.entry_price - trade.price)

        return new_qty, realized_pnl

    def _calculate_average_price(
        self,
        position: DerivativePosition,
        trade: Trade,
        new_quantity: Decimal,
    ) -> Decimal:
        """Calculate new average price after trade.

        Args:
            position: Current position
            trade: New trade
            new_quantity: New position quantity (signed)

        Returns:
            New average price
        """
        # If position flipped sides, use trade price
        old_signed_qty = position.size
        if position.side == OrderSide.SELL:
            old_signed_qty = -old_signed_qty

        if (old_signed_qty > 0 and new_quantity < 0) or (old_signed_qty < 0 and new_quantity > 0):
            return trade.price

        # Calculate weighted average for same-side trades
        if not position.entry_price:
            return trade.price

        old_value = abs(old_signed_qty) * position.entry_price
        trade_value = trade.quantity * trade.price
        total_value = old_value + trade_value
        total_quantity = abs(old_signed_qty) + trade.quantity

        if total_quantity == 0:
            return trade.price

        return total_value / total_quantity

    async def calculate_position_pnl(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
        current_price: Decimal,
    ) -> dict[str, Decimal]:
        """Calculate PnL for a specific position.

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

        # Calculate unrealized PnL
        if position.side == OrderSide.BUY:
            unrealized_pnl = position.size * (current_price - position.entry_price)
        else:
            unrealized_pnl = position.size * (position.entry_price - current_price)

        # Calculate percentage
        position_value = position.size * position.entry_price
        pnl_percentage = (
            (unrealized_pnl / position_value * 100) if position_value > 0 else Decimal(0)
        )

        return {
            "unrealized_pnl": unrealized_pnl,
            "pnl_percentage": pnl_percentage,
        }

    async def get_total_exposure(self, exchange: ExchangeName | None = None) -> Decimal:
        """Get total position exposure in USD.

        Args:
            exchange: Optional exchange to filter by

        Returns:
            Total exposure in USD
        """
        positions = await self.get_all_positions(exchange)
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
        exchange_positions: dict[str, Any],
        exchange: ExchangeName,
    ) -> dict[str, Any]:
        """Reconcile local positions with exchange data.

        Args:
            exchange_positions: Positions from exchange
            exchange: Exchange name

        Returns:
            Reconciliation results
        """
        discrepancies: list[dict[str, Any]] = []
        updated = 0

        state = await self._state_manager.get_state()
        if not state:
            return {"error": "No portfolio state"}

        # Process each exchange position
        for symbol_str, position_data in exchange_positions.items():
            # Create Symbol object using service
            symbol = _symbol_service.create_symbol(symbol_str, exchange)
            key = f"{exchange.value}:{symbol_str}"

            # Get exchange position data
            exchange_size = abs(Decimal(str(position_data.get("size", 0))))
            exchange_side = position_data.get("side", "").upper()
            exchange_entry = Decimal(str(position_data.get("entry_price", 0)))

            # Get local position
            local_position = state.positions.get(key)

            if local_position:
                # Check for discrepancy
                size_diff = abs(local_position.size - exchange_size)
                if size_diff > self._position_tolerance:
                    discrepancies.append({
                        "symbol": symbol_str,
                        "local_size": float(local_position.size),
                        "exchange_size": float(exchange_size),
                        "difference": float(size_diff),
                    })

                    # Update to match exchange
                    if self.config.state.reconciliation_enabled and exchange_size > 0:
                        state.positions[key] = DerivativePosition(
                            exchange=exchange,
                            symbol=symbol,
                            side=OrderSide[exchange_side] if exchange_side else OrderSide.BUY,
                            size=exchange_size,
                            entry_price=(
                                exchange_entry if exchange_entry > 0 else local_position.entry_price
                            ),
                            timestamp=datetime.now(UTC),
                            unrealized_pnl=Decimal(0),
                        )
                        updated += 1
            elif exchange_size > 0:
                # New position from exchange
                state.positions[key] = DerivativePosition(
                    exchange=exchange,
                    symbol=symbol,
                    side=OrderSide[exchange_side] if exchange_side else OrderSide.BUY,
                    size=exchange_size,
                    entry_price=exchange_entry,
                    timestamp=datetime.now(UTC),
                    unrealized_pnl=Decimal(0),
                )
                updated += 1

        # Remove positions that don't exist on exchange
        local_keys = [k for k in state.positions if k.startswith(f"{exchange.value}:")]
        exchange_keys = [f"{exchange.value}:{s}" for s in exchange_positions]

        for key in local_keys:
            if key not in exchange_keys and self.config.state.reconciliation_enabled:
                del state.positions[key]
                updated += 1

        # Save if updated
        if updated > 0:
            state.timestamp = datetime.now(UTC)
            await self._state_manager.save_state()

        return {
            "discrepancies": discrepancies,
            "updated": updated,
            "total_checked": len(exchange_positions),
            "removed": (
                len(local_keys) - len(exchange_keys) if len(local_keys) > len(exchange_keys) else 0
            ),
        }

    async def get_exchange_positions(self, exchange: ExchangeName) -> dict[str, DerivativePosition]:
        """Get all positions for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            Dictionary of symbol -> DerivativePosition for the exchange
        """
        positions = await self.get_all_positions(exchange)
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
        """
        state = await self._state_manager.get_state()
        if not state:
            return

        key = f"{exchange.value}:{symbol.value}"

        if new_position is None:
            # Remove position
            if key in state.positions:
                del state.positions[key]
                logger.info(
                    "position_removed_directly",
                    exchange=exchange.value,
                    symbol=symbol.value,
                )
        else:
            # Update/add position
            state.positions[key] = new_position
            logger.info(
                "position_updated_directly",
                exchange=exchange.value,
                symbol=symbol.value,
                side=new_position.side.value,
                size=float(new_position.size),
                entry_price=float(new_position.entry_price) if new_position.entry_price else None,
            )

        state.timestamp = datetime.now(UTC)
        await self._state_manager.save_state()
