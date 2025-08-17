"""Portfolio state management module.

This module handles portfolio state operations including initialization,
state access, and basic state updates.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.financial.calculators.mark_to_market_calculator import (
    MarkToMarketCalculator,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.trading import OrderSide
from cyberdelta.exceptions.portfolio import (
    MissingEntryPriceError,
    PortfolioNotInitializedError,
    PortfolioStateNotInitializedError,
)
from cyberdelta.models import DerivativePosition, Fill, SpotBalance
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.symbols.models import BaseSymbol, HyperliquidMetadata, Symbol


if TYPE_CHECKING:
    from cyberdelta.protocols.domain.portfolio import PortfolioStorageProtocol

from cyberdelta.protocols.domain.portfolio import PortfolioStateManagerProtocol


logger = get_logger(__name__)


class PortfolioStateManager(PortfolioStateManagerProtocol):
    """Manages portfolio state operations and persistence.

    Configuration Integration:
    - Uses config.general.state_file for primary state persistence
    - Uses config.general.state_backup_directory for state backups
    - Uses config.general.state_save_interval for auto-save frequency
    - Uses config.state.update_timeout for state update operations
    - Uses config.validation settings for all validations

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - NO assumptions about data validity
    """

    def __init__(
        self,
        config: AppSettings,
        storage: PortfolioStorageProtocol,
    ) -> None:
        """Initialize state manager with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            storage: Storage protocol implementation for persistence
        """
        self.config = config
        self._storage = storage
        self._state_lock = asyncio.Lock()
        self._cached_state: PortfolioState | None = None

        # Extract configuration values - NO hardcoded defaults
        self._balance_tolerance = config.validation.balance_tolerance
        self._position_tolerance = config.validation.position_size_tolerance
        self._max_position_age = config.validation.max_position_age_seconds

        # State management settings from config
        self._save_interval = config.general.state_save_interval
        self._backup_count = config.general.state_backup_count
        self._update_timeout = config.state.update_timeout
        self._atomic_updates = config.state.atomic_updates

        logger.info(
            "state_manager_initialized",
            balance_tolerance=self._balance_tolerance,
            position_tolerance=self._position_tolerance,
            save_interval=self._save_interval,
            atomic_updates=self._atomic_updates,
        )

    async def initialize_state(self) -> PortfolioState:
        """Initialize portfolio state with persisted data.

        Loads existing portfolio state from storage or creates empty state
        if none exists.

        Returns:
            Initialized portfolio state
        """
        async with self._state_lock:
            logger.info("portfolio_state_initializing")

            try:
                self._cached_state = await self._storage.load_state()

                if self._cached_state is None:
                    # Create empty state - NO default values
                    self._cached_state = PortfolioState(
                        balances={}, positions={}, timestamp=datetime.now(UTC)
                    )

                    logger.info("portfolio_state_created_empty", reason="no_existing_state")
                else:
                    logger.info(
                        "portfolio_state_loaded",
                        balance_count=len(self._cached_state.balances),
                        position_count=len(self._cached_state.positions),
                        state_timestamp=self._cached_state.timestamp.isoformat(),
                    )

            except Exception as e:
                logger.exception("portfolio_initialization_failed", error=str(e))
                raise
            else:
                return self._cached_state

    async def get_balance(self, asset: Symbol, exchange: ExchangeName) -> SpotBalance | None:
        """Get balance for specific asset on exchange.

        Args:
            asset: Asset symbol (Symbol object, NOT string)
            exchange: Exchange name (ExchangeName enum, NOT string)

        Returns:
            SpotBalance if found, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Symbol object, NOT string
        - Uses ExchangeName enum, NOT string
        - NO assumptions about asset existence
        """
        async with self._state_lock:
            if self._cached_state is None:
                logger.warning(
                    "balance_request_before_initialization",
                    asset=asset.value,
                    exchange=exchange.value,
                )
                return None

            key = f"{exchange.value}:{asset.value}"
            balance = self._cached_state.balances.get(key)

            logger.debug(
                "balance_retrieved",
                asset=asset.value,
                exchange=exchange.value,
                found=balance is not None,
                balance_value=balance.total_quantity if balance else None,
            )

            return balance

    async def get_position(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> DerivativePosition | None:
        """Get position for specific symbol on exchange.

        Args:
            symbol: Trading symbol (Symbol object, NOT string)
            exchange: Exchange name (ExchangeName enum, NOT string)

        Returns:
            DerivativePosition if found, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Symbol object, NOT string
        - Uses ExchangeName enum, NOT string
        - NO assumptions about position existence
        """
        async with self._state_lock:
            if self._cached_state is None:
                logger.warning(
                    "position_request_before_initialization",
                    symbol=symbol.value,
                    exchange=exchange.value,
                )
                return None

            key = f"{exchange.value}:{symbol.value}"
            position = self._cached_state.positions.get(key)

            logger.debug(
                "position_retrieved",
                symbol=symbol.value,
                exchange=exchange.value,
                found=position is not None,
                position_size=position.size if position else None,
            )

            return position

    async def get_state(self) -> PortfolioState:
        """Get current portfolio state.

        Following CODING_STANDARDS.md:
        - Returns typed PortfolioState, NOT dict
        - Immutable snapshot, not live reference

        Returns:
            Current portfolio state snapshot

        Raises:
            PortfolioNotInitializedError: If portfolio is not initialized
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise PortfolioNotInitializedError

            # Return a copy to prevent external mutation
            return PortfolioState(
                balances=self._cached_state.balances.copy(),
                positions=self._cached_state.positions.copy(),
                timestamp=self._cached_state.timestamp,
                total_equity_usd=self._cached_state.total_equity_usd,
            )

    async def update_from_fill(self, fill: Fill) -> None:
        """Update portfolio state from fill execution.

        Following CODING_STANDARDS.md:
        - Uses Fill object with Symbol/ExchangeName types
        - All calculations use Decimal
        - NO assumptions about fill validity
        - Atomic state updates with persistence

        Args:
            fill: Fill object containing execution details

        Raises:
            PortfolioNotInitializedError: If portfolio is not initialized
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise PortfolioNotInitializedError

            logger.info(
                "portfolio_update_from_fill_starting",
                fill_id=fill.id,
                symbol=fill.symbol.value,
                exchange=fill.exchange,  # exchange is str in Fill model
                side=fill.side.value,
                quantity=fill.quantity,
                price=fill.price,
            )

            try:
                # Update position from fill
                await self._update_position_from_fill(fill)

                # Update balance from fill
                await self._update_balance_from_fill(fill)

                # Update state timestamp
                self._cached_state.timestamp = datetime.now(UTC)

                # Persist state using configured strategy
                if self._atomic_updates:
                    await self._storage.save_state(self._cached_state)

                logger.info(
                    "portfolio_update_from_fill_completed",
                    fill_id=fill.id,
                    atomic_save=self._atomic_updates,
                )

            except Exception as e:
                logger.exception("portfolio_update_from_fill_failed", fill_id=fill.id, error=str(e))
                raise

    async def _update_position_from_fill(self, fill: Fill) -> None:
        """Update position based on fill execution.

        Following CODING_STANDARDS.md:
        - All position calculations use Decimal
        - NO assumptions about existing position
        - Proper handling of position opening/closing

        Args:
            fill: Fill execution details

        Raises:
            PortfolioStateNotInitializedError: If portfolio state is not initialized
            MissingEntryPriceError: If position is missing entry price for PnL calculation
        """
        if self._cached_state is None:
            raise PortfolioStateNotInitializedError

        position_key = f"{fill.exchange}:{fill.symbol.value}"
        current_position = self._cached_state.positions.get(position_key)

        if current_position is None:
            # Opening new position - convert exchange string to ExchangeName
            exchange_enum = ExchangeName(fill.exchange)
            new_position = DerivativePosition(
                exchange=exchange_enum,
                symbol=fill.symbol,
                side=fill.side,
                size=fill.quantity,
                entry_price=fill.price,
                timestamp=fill.executed_at,
                unrealized_pnl=Decimal(0),  # Start with zero unrealized PnL
            )

            self._cached_state.positions[position_key] = new_position

            logger.info(
                "position_opened",
                symbol=fill.symbol.value,
                exchange=fill.exchange,
                size=new_position.size,
                entry_price=new_position.entry_price or 0.0,
            )

        else:
            # Updating existing position
            quantity_change = fill.quantity if fill.side == OrderSide.BUY else -fill.quantity

            new_size = current_position.size + quantity_change

            if new_size == Decimal(0):
                # Position closed
                realized_pnl = self._calculate_realized_pnl(current_position, fill)
                self._cached_state.positions.pop(position_key, None)

                logger.info(
                    "position_closed",
                    symbol=fill.symbol.value,
                    exchange=fill.exchange,
                    realized_pnl=realized_pnl or None,
                )

            else:
                # Position size changed - calculate new average price
                if current_position.entry_price is None:
                    raise MissingEntryPriceError(position_key)

                new_avg_price = self._calculate_average_price(
                    current_position.size,
                    current_position.entry_price,
                    fill.quantity,
                    fill.price,
                    fill.side,
                )

                # Create updated position
                updated_position = DerivativePosition(
                    exchange=current_position.exchange,
                    symbol=current_position.symbol,
                    side=current_position.side,
                    size=new_size,
                    entry_price=new_avg_price,
                    timestamp=fill.executed_at,
                    unrealized_pnl=current_position.unrealized_pnl,  # Preserve until recalculation
                )

                self._cached_state.positions[position_key] = updated_position

                logger.info(
                    "position_updated",
                    symbol=fill.symbol.value,
                    exchange=fill.exchange,
                    old_size=current_position.size,
                    new_size=new_size,
                    new_avg_price=new_avg_price,
                )

    async def _update_balance_from_fill(self, fill: Fill) -> None:
        """Update balance based on fill execution.

        Following CODING_STANDARDS.md:
        - Balance calculations use Decimal
        - NO assumptions about quote asset
        - Proper fee handling from fill data

        Args:
            fill: Fill execution details

        Raises:
            PortfolioStateNotInitializedError: If portfolio state is not initialized
        """
        if self._cached_state is None:
            raise PortfolioStateNotInitializedError

        # Get quote asset from symbol - simplified for now
        # TODO: This will be enhanced when symbol service integration is added
        quote_asset_str = self._extract_quote_asset(fill.symbol)

        # For now, create a basic Symbol object for the quote asset
        # This will be improved when the symbol service is integrated
        quote_asset = BaseSymbol[HyperliquidMetadata](
            value=quote_asset_str,
            exchange=ExchangeName.HYPERLIQUID,  # Default for now
            metadata=HyperliquidMetadata(),
        )

        balance_key = f"{fill.exchange}:{quote_asset.value}"
        current_balance = self._cached_state.balances.get(balance_key)

        if current_balance is None:
            # Create new balance entry with zero starting balance
            # In production, this should trigger a reconciliation
            exchange_enum = ExchangeName(fill.exchange)
            current_balance = SpotBalance(
                exchange=exchange_enum,
                asset=quote_asset,
                timestamp=fill.executed_at,
                total_quantity=Decimal(0),
                available_quantity=Decimal(0),
            )

            logger.warning(
                "balance_created_from_fill",
                asset=quote_asset.value,
                exchange=fill.exchange,
                reason="no_existing_balance",
            )

        # Calculate balance change from fill
        fill_cost = fill.quantity * fill.price
        balance_change = -fill_cost if fill.side == OrderSide.BUY else fill_cost

        # Subtract fees
        balance_change -= fill.fee

        # Create updated balance
        new_total = current_balance.total_quantity + balance_change
        new_available = current_balance.available_quantity + balance_change

        updated_balance = SpotBalance(
            exchange=current_balance.exchange,
            asset=current_balance.asset,
            timestamp=fill.executed_at,
            total_quantity=new_total,
            available_quantity=new_available,
        )

        self._cached_state.balances[balance_key] = updated_balance

        logger.info(
            "balance_updated_from_fill",
            asset=quote_asset.value,
            exchange=fill.exchange,
            balance_change=balance_change,
            new_total=new_total,
            fee_paid=fill.fee,
        )

    def _calculate_average_price(
        self,
        existing_size: Decimal,
        existing_price: Decimal,
        new_quantity: Decimal,
        new_price: Decimal,
        trade_side: OrderSide,
    ) -> Decimal:
        """Calculate new average price after trade.

        Args:
            existing_size: Current position size
            existing_price: Current average entry price
            new_quantity: Quantity of new trade
            new_price: Price of new trade
            trade_side: Side of the trade (BUY/SELL)

        Returns:
            New weighted average price

        IMPORTANT: Following CODING_STANDARDS.md:
        - All calculations use Decimal for precision
        - NO assumptions about trade direction
        """
        if trade_side == OrderSide.SELL:
            # Selling reduces position, doesn't change average price
            return existing_price

        # For BUY orders, calculate weighted average
        existing_value = existing_size * existing_price
        new_value = new_quantity * new_price
        total_size = existing_size + new_quantity

        if total_size == Decimal(0):
            return Decimal(0)

        return (existing_value + new_value) / total_size

    def _calculate_realized_pnl(self, position: DerivativePosition, fill: Fill) -> Decimal | None:
        """Calculate realized PnL using centralized financial calculator.

        Args:
            position: Position being closed
            fill: Fill that closes the position

        Returns:
            Realized PnL in quote currency, None if cannot calculate

        Note:
            This method delegates to the centralized MarkToMarketCalculator for consistency
            across all PnL calculations in the system. This ensures single source of truth.

        Raises:
            MissingEntryPriceError: If position is missing entry price
        """
        if position.entry_price is None:
            raise MissingEntryPriceError(f"{position.exchange}:{position.symbol.value}")

        # Delegate to centralized calculator for consistency
        try:
            # Use centralized calculator with configuration
            calculator = MarkToMarketCalculator(self.config, fee_calculator=None)
            result = calculator.calculate_realized_pnl(position, fill, include_fees=False)
            calculated_pnl = result.amount
        except (ValueError, TypeError, AttributeError):
            # Fallback to original logic if calculator fails
            # This ensures compatibility during transition period
            entry_price = position.entry_price

            if fill.side == OrderSide.BUY:
                # Closing short position
                if position.side == OrderSide.SELL:
                    calculated_pnl = (entry_price - fill.price) * fill.quantity
                else:
                    # Cannot calculate - position and fill sides don't match for closing
                    calculated_pnl = None
            # Closing long position
            elif position.side == OrderSide.BUY:
                calculated_pnl = (fill.price - entry_price) * fill.quantity
            else:
                # Cannot calculate - position and fill sides don't match for closing
                calculated_pnl = None
        else:
            # Calculator succeeded, no fallback needed
            pass

        return calculated_pnl

    def _extract_quote_asset(self, symbol: Symbol) -> str:
        """Extract quote asset from trading symbol.

        Args:
            symbol: Trading symbol to extract quote asset from

        Returns:
            Quote asset string

        NOTE: This is a simplified implementation.
        In production, this should use the symbol service.
        """
        symbol_str = symbol.value

        # Simple heuristic for common patterns
        if "_" in symbol_str:
            return symbol_str.split("_")[-1]
        if "USD" in symbol_str:
            return "USD"
        if "USDC" in symbol_str:
            return "USDC"
        if "USDT" in symbol_str:
            return "USDT"
        # Default fallback - this should not happen in production
        logger.warning("quote_asset_extraction_fallback", symbol=symbol_str, fallback="USDC")
        return "USDC"

    async def save_state(self) -> None:
        """Save current portfolio state to storage.

        Uses configured save intervals and atomic write strategy.

        Following CODING_STANDARDS.md:
        - Save intervals from config, NO hardcoded values
        - Atomic writes if configured
        - Explicit error handling

        Raises:
            PortfolioNotInitializedError: If portfolio is not initialized
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise PortfolioNotInitializedError

            try:
                await self._storage.save_state(self._cached_state)

                logger.info(
                    "portfolio_state_saved",
                    balance_count=len(self._cached_state.balances),
                    position_count=len(self._cached_state.positions),
                    timestamp=self._cached_state.timestamp.isoformat(),
                )

            except Exception as e:
                logger.exception("portfolio_state_save_failed", error=str(e))
                raise

    async def load_from_storage(self) -> None:
        """Force reload portfolio state from storage.

        Useful for refreshing state from external updates.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit reload, NO silent state changes
        - Proper error handling with context
        """
        async with self._state_lock:
            try:
                loaded_state = await self._storage.load_state()

                if loaded_state is not None:
                    old_timestamp = self._cached_state.timestamp if self._cached_state else None

                    self._cached_state = loaded_state

                    logger.info(
                        "portfolio_state_reloaded",
                        old_timestamp=old_timestamp.isoformat() if old_timestamp else None,
                        new_timestamp=loaded_state.timestamp.isoformat(),
                        balance_count=len(loaded_state.balances),
                        position_count=len(loaded_state.positions),
                    )
                else:
                    logger.warning("portfolio_reload_no_state_found")

            except Exception as e:
                logger.exception("portfolio_reload_failed", error=str(e))
                raise

    async def get_total_equity_usd(self) -> Decimal:
        """Get total portfolio equity in USD.

        Returns:
            Total equity across all exchanges in USD
        """
        if self._cached_state is None:
            return Decimal(0)

        total = Decimal(0)

        # Sum all USD-denominated balances
        for balance in self._cached_state.balances.values():
            if balance.asset.value in {"USD", "USDC", "USDT"}:
                total += balance.total_quantity

        # Add position values (would need market prices in production)
        # For now, just use entry prices as approximation
        for position in self._cached_state.positions.values():
            if position.entry_price:
                position_value = position.size * position.entry_price
                total += position_value

        return total

    async def create_snapshot(self) -> None:
        """Create a backup snapshot of current portfolio state.

        Raises:
            PortfolioNotInitializedError: If portfolio is not initialized
        """
        if self._cached_state is None:
            raise PortfolioNotInitializedError

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        snapshot_name = f"snapshot_{timestamp}"

        await self._storage.save_snapshot(self._cached_state, snapshot_name)

        logger.info(
            "portfolio_snapshot_created",
            snapshot_name=snapshot_name,
            balance_count=len(self._cached_state.balances),
            position_count=len(self._cached_state.positions),
        )

    async def update_balance(
        self,
        exchange: ExchangeName,
        asset: Symbol,
        new_balance: SpotBalance,
    ) -> None:
        """Update balance directly (for reconciliation).

        Args:
            exchange: Exchange name
            asset: Asset symbol
            new_balance: New balance to set

        Raises:
            PortfolioNotInitializedError: If portfolio is not initialized
        """
        if self._cached_state is None:
            raise PortfolioNotInitializedError

        key = f"{exchange.value}:{asset.value}"
        self._cached_state.balances[key] = new_balance
        self._cached_state.timestamp = datetime.now(UTC)

        logger.info(
            "balance_updated_directly",
            exchange=exchange.value,
            asset=asset.value,
            total=new_balance.total_quantity,
            available=new_balance.available_quantity,
        )

    async def update_position(
        self,
        exchange: ExchangeName,
        symbol: Symbol,
        new_position: DerivativePosition | None,
    ) -> None:
        """Update position directly (for reconciliation).

        Args:
            exchange: Exchange name
            symbol: Trading symbol
            new_position: New position to set, None to remove

        Raises:
            PortfolioNotInitializedError: If portfolio is not initialized
        """
        if self._cached_state is None:
            raise PortfolioNotInitializedError

        key = f"{exchange.value}:{symbol.value}"

        if new_position is None:
            # Remove position
            if key in self._cached_state.positions:
                del self._cached_state.positions[key]
                logger.info(
                    "position_removed",
                    exchange=exchange.value,
                    symbol=symbol.value,
                )
        else:
            # Update/add position
            self._cached_state.positions[key] = new_position
            logger.info(
                "position_updated_directly",
                exchange=exchange.value,
                symbol=symbol.value,
                side=new_position.side.value,
                size=new_position.size,
                entry_price=new_position.entry_price or None,
            )

        self._cached_state.timestamp = datetime.now(UTC)

    async def list_snapshots(self) -> list[str]:
        """List all available portfolio snapshots.

        Returns:
            List of snapshot names/identifiers
        """
        snapshots = await self._storage.list_snapshots()

        logger.info(
            "portfolio_snapshots_listed",
            snapshot_count=len(snapshots),
        )

        return snapshots

    async def delete_snapshot(self, snapshot_name: str) -> None:
        """Delete a named snapshot.

        Args:
            snapshot_name: Name/identifier of snapshot to delete
        """
        await self._storage.delete_snapshot(snapshot_name)

        logger.info(
            "portfolio_snapshot_deleted",
            snapshot_name=snapshot_name,
        )
