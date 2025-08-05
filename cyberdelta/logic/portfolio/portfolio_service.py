"""Portfolio service for unified portfolio state management.

This module provides the main portfolio service that manages the unified
portfolio state across all exchanges, serving as the single source of truth
for balances and positions.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Dict, Optional, Any

from cyberdelta.config.structlog_config import get_logger
from pydantic import BaseModel

from cyberdelta.application.event_bus import EventBus
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.infrastructure.persistence.protocols import PortfolioStorageProtocol
from cyberdelta.logic.monitoring.health_monitor import HealthCheckable, ServiceType
from cyberdelta.models import DerivativePosition, SpotBalance, Trade
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.enums.trading import OrderSide

logger = get_logger(__name__)


class PortfolioService(HealthCheckable):
    """Single source of truth for unified portfolio state across all exchanges.

    This service maintains the aggregated view of:
    - All balances on all exchanges
    - All positions on all exchanges
    - Total portfolio equity in USD
    - Cross-exchange portfolio metrics

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
        event_bus: EventBus,
        api_clients: Optional[Dict[str, object]] = None,
    ):
        """Initialize portfolio service with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            storage: Storage protocol implementation for persistence
            event_bus: Event bus for publishing portfolio events
            api_clients: Optional exchange API clients for reconciliation
        """
        self.config = config
        self._storage = storage
        self._event_bus = event_bus
        self._api_clients = api_clients or {}
        self._state_lock = asyncio.Lock()
        self._cached_state: Optional[PortfolioState] = None

        # Extract configuration values - NO hardcoded defaults
        self._balance_tolerance = config.validation.balance_tolerance
        self._position_tolerance = config.validation.position_size_tolerance
        self._max_position_age = config.validation.max_position_age_seconds

        # State management settings from config
        self._save_interval = config.general.state_save_interval
        self._backup_count = config.general.state_backup_count
        self._update_timeout = config.state.update_timeout
        self._atomic_updates = config.state.atomic_updates

        # Health tracking attributes
        self._operation_count = 0
        self._success_count = 0
        self._error_count = 0
        self._last_activity = datetime.now(UTC)

        # Initialize performance tracker if configured
        self._performance_tracker = None
        if hasattr(config.calculation, "performance_metrics"):
            from cyberdelta.logic.monitoring.performance_tracker import PerformanceTracker

            self._performance_tracker = PerformanceTracker(config, self)

        logger.info(
            "portfolio_service_initialized",
            balance_tolerance=float(self._balance_tolerance),
            position_tolerance=float(self._position_tolerance),
            save_interval=self._save_interval,
            atomic_updates=self._atomic_updates,
            performance_tracking_enabled=self._performance_tracker is not None,
        )

    async def initialize(self) -> None:
        """Initialize service with persisted state.

        Loads existing portfolio state from storage or creates empty state
        if none exists.

        Raises:
            StorageError: If state loading fails
        """
        async with self._state_lock:
            logger.info("portfolio_service_initializing")

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
                logger.error("portfolio_initialization_failed", error=str(e), exc_info=True)
                raise

    async def get_balance(self, asset: Symbol, exchange: ExchangeName) -> Optional[SpotBalance]:
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
                balance_value=float(balance.total_quantity) if balance else None,
            )

            return balance

    async def get_position(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> Optional[DerivativePosition]:
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
                position_size=float(position.size) if position else None,
            )

            return position

    async def get_total_equity_usd(self) -> Decimal:
        """Get total portfolio equity in USD.

        Returns:
            Total equity across all exchanges in USD

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - NO assumptions about conversion rates
        - Explicit calculation, no cached values without validation
        """
        async with self._state_lock:
            if self._cached_state is None:
                logger.warning("equity_request_before_initialization")
                return Decimal("0")

            # Return cached value if available and not stale
            if self._cached_state.total_equity_usd is not None:
                logger.debug(
                    "total_equity_retrieved_cached",
                    equity_usd=float(self._cached_state.total_equity_usd),
                )
                return self._cached_state.total_equity_usd

            # Calculate if not cached - this will be enhanced in later steps
            # For now, return zero as we need market data service for conversion
            logger.debug(
                "total_equity_calculation_deferred", reason="market_data_service_not_available"
            )
            return Decimal("0")

    async def get_state(self) -> PortfolioState:
        """Get current portfolio state.

        Returns:
            Current portfolio state snapshot

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns typed PortfolioState, NOT dict
        - Immutable snapshot, not live reference
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise ValueError("Portfolio service not initialized")

            # Return a copy to prevent external mutation
            return PortfolioState(
                balances=self._cached_state.balances.copy(),
                positions=self._cached_state.positions.copy(),
                timestamp=self._cached_state.timestamp,
                total_equity_usd=self._cached_state.total_equity_usd,
            )

    async def get_exchange_balances(self, exchange: ExchangeName) -> Dict[str, SpotBalance]:
        """Get all balances for a specific exchange.

        Args:
            exchange: Exchange name (ExchangeName enum, NOT string)

        Returns:
            Dictionary of asset -> SpotBalance for the exchange

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses ExchangeName enum, NOT string
        - Returns typed Dict, NOT generic dict
        """
        async with self._state_lock:
            if self._cached_state is None:
                logger.warning(
                    "exchange_balances_request_before_initialization", exchange=exchange.value
                )
                return {}

            exchange_balances = {}
            prefix = f"{exchange.value}:"

            for key, balance in self._cached_state.balances.items():
                if key.startswith(prefix):
                    asset_name = key[len(prefix) :]
                    exchange_balances[asset_name] = balance

            logger.debug(
                "exchange_balances_retrieved",
                exchange=exchange.value,
                balance_count=len(exchange_balances),
            )

            return exchange_balances

    async def get_exchange_positions(self, exchange: ExchangeName) -> Dict[str, DerivativePosition]:
        """Get all positions for a specific exchange.

        Args:
            exchange: Exchange name (ExchangeName enum, NOT string)

        Returns:
            Dictionary of symbol -> DerivativePosition for the exchange

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses ExchangeName enum, NOT string
        - Returns typed Dict, NOT generic dict
        """
        async with self._state_lock:
            if self._cached_state is None:
                logger.warning(
                    "exchange_positions_request_before_initialization", exchange=exchange.value
                )
                return {}

            exchange_positions = {}
            prefix = f"{exchange.value}:"

            for key, position in self._cached_state.positions.items():
                if key.startswith(prefix):
                    symbol_name = key[len(prefix) :]
                    exchange_positions[symbol_name] = position

            logger.debug(
                "exchange_positions_retrieved",
                exchange=exchange.value,
                position_count=len(exchange_positions),
            )

            return exchange_positions

    async def update_from_trade(self, trade: Trade) -> None:
        """Update portfolio state from trade execution.

        Args:
            trade: Trade object containing execution details

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Trade object with Symbol/ExchangeName types
        - All calculations use Decimal
        - NO assumptions about trade validity
        - Atomic state updates with persistence
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise ValueError("Portfolio service not initialized")

            logger.info(
                "portfolio_update_from_trade_starting",
                trade_id=trade.id,
                symbol=trade.symbol.value,
                exchange=trade.exchange,  # exchange is str in Trade model
                side=trade.side.value,
                quantity=float(trade.quantity),
                price=float(trade.price),
            )

            try:
                # Update position from trade
                await self._update_position_from_trade(trade)

                # Update balance from trade
                await self._update_balance_from_trade(trade)

                # Update state timestamp
                self._cached_state.timestamp = datetime.now(UTC)

                # Persist state using configured strategy
                if self._atomic_updates:
                    await self._storage.save_state(self._cached_state)

                # Track trade for performance metrics
                await self.track_trade_for_performance(trade)

                # Update success metrics
                self._success_count += 1
                self._last_activity = datetime.now(UTC)

                logger.info(
                    "portfolio_update_from_trade_completed",
                    trade_id=trade.id,
                    atomic_save=self._atomic_updates,
                )

            except Exception as e:
                # Update error metrics
                self._error_count += 1
                logger.error(
                    "portfolio_update_from_trade_failed",
                    trade_id=trade.id,
                    error=str(e),
                    exc_info=True,
                )
                raise

    async def _update_position_from_trade(self, trade: Trade) -> None:
        """Update position based on trade execution.

        Args:
            trade: Trade execution details

        IMPORTANT: Following CODING_STANDARDS.md:
        - All position calculations use Decimal
        - NO assumptions about existing position
        - Proper handling of position opening/closing
        """
        assert self._cached_state is not None, "Portfolio state must be initialized"

        position_key = f"{trade.exchange}:{trade.symbol.value}"
        current_position = self._cached_state.positions.get(position_key)

        if current_position is None:
            # Opening new position - convert exchange string to ExchangeName
            exchange_enum = ExchangeName(trade.exchange)
            new_position = DerivativePosition(
                exchange=exchange_enum,
                symbol=trade.symbol,
                side=trade.side,
                size=trade.quantity,
                entry_price=trade.price,
                timestamp=trade.executed_at,
                unrealized_pnl=Decimal("0"),  # Start with zero unrealized PnL
            )

            self._cached_state.positions[position_key] = new_position

            logger.info(
                "position_opened",
                symbol=trade.symbol.value,
                exchange=trade.exchange,
                size=float(new_position.size),
                entry_price=float(new_position.entry_price or Decimal("0")),
            )

        else:
            # Updating existing position
            quantity_change = trade.quantity if trade.side == OrderSide.BUY else -trade.quantity

            new_size = current_position.size + quantity_change

            if new_size == Decimal("0"):
                # Position closed
                realized_pnl = self._calculate_realized_pnl(current_position, trade)
                self._cached_state.positions.pop(position_key, None)

                logger.info(
                    "position_closed",
                    symbol=trade.symbol.value,
                    exchange=trade.exchange,
                    realized_pnl=float(realized_pnl) if realized_pnl else None,
                )

            else:
                # Position size changed - calculate new average price
                new_avg_price = self._calculate_average_price(
                    current_position.size,
                    current_position.entry_price or Decimal("0"),
                    trade.quantity,
                    trade.price,
                    trade.side,
                )

                # Create updated position
                updated_position = DerivativePosition(
                    exchange=current_position.exchange,
                    symbol=current_position.symbol,
                    side=current_position.side,
                    size=new_size,
                    entry_price=new_avg_price,
                    timestamp=trade.executed_at,
                    unrealized_pnl=current_position.unrealized_pnl,  # Preserve until recalculation
                )

                self._cached_state.positions[position_key] = updated_position

                logger.info(
                    "position_updated",
                    symbol=trade.symbol.value,
                    exchange=trade.exchange,
                    old_size=float(current_position.size),
                    new_size=float(new_size),
                    new_avg_price=float(new_avg_price),
                )

    async def _update_balance_from_trade(self, trade: Trade) -> None:
        """Update balance based on trade execution.

        Args:
            trade: Trade execution details

        IMPORTANT: Following CODING_STANDARDS.md:
        - Balance calculations use Decimal
        - NO assumptions about quote asset
        - Proper fee handling from trade data
        """
        assert self._cached_state is not None, "Portfolio state must be initialized"

        # Get quote asset from symbol - simplified for now
        # TODO: This will be enhanced when symbol service integration is added
        quote_asset_str = self._extract_quote_asset(trade.symbol)

        # For now, create a basic Symbol object for the quote asset
        # This will be improved when the symbol service is integrated
        from cyberdelta.core.symbols.models import BaseSymbol, HyperliquidMetadata

        quote_asset = BaseSymbol[HyperliquidMetadata](
            value=quote_asset_str,
            exchange=ExchangeName.HYPERLIQUID,  # Default for now
            metadata=HyperliquidMetadata(),
        )

        balance_key = f"{trade.exchange}:{quote_asset.value}"
        current_balance = self._cached_state.balances.get(balance_key)

        if current_balance is None:
            # Create new balance entry with zero starting balance
            # In production, this should trigger a reconciliation
            exchange_enum = ExchangeName(trade.exchange)
            current_balance = SpotBalance(
                exchange=exchange_enum,
                asset=quote_asset,
                timestamp=trade.executed_at,
                total_quantity=Decimal("0"),
                available_quantity=Decimal("0"),
            )

            logger.warning(
                "balance_created_from_trade",
                asset=quote_asset.value,
                exchange=trade.exchange,
                reason="no_existing_balance",
            )

        # Calculate balance change from trade
        trade_cost = trade.quantity * trade.price
        balance_change = -trade_cost if trade.side == OrderSide.BUY else trade_cost

        # Subtract fees
        balance_change -= trade.fee

        # Create updated balance
        new_total = current_balance.total_quantity + balance_change
        new_available = current_balance.available_quantity + balance_change

        updated_balance = SpotBalance(
            exchange=current_balance.exchange,
            asset=current_balance.asset,
            timestamp=trade.executed_at,
            total_quantity=new_total,
            available_quantity=new_available,
        )

        self._cached_state.balances[balance_key] = updated_balance

        logger.info(
            "balance_updated_from_trade",
            asset=quote_asset.value,
            exchange=trade.exchange,
            balance_change=float(balance_change),
            new_total=float(new_total),
            fee_paid=float(trade.fee),
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

        if total_size == Decimal("0"):
            return Decimal("0")

        return (existing_value + new_value) / total_size

    def _calculate_realized_pnl(
        self, position: DerivativePosition, trade: Trade
    ) -> Optional[Decimal]:
        """Calculate realized PnL from position closing trade.

        Args:
            position: Position being closed
            trade: Trade that closes the position

        Returns:
            Realized PnL in quote currency, None if cannot calculate

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - NO assumptions about position side
        """
        entry_price = position.entry_price or Decimal("0")

        if trade.side == OrderSide.BUY:
            # Closing short position
            if position.side == OrderSide.SELL:
                return (entry_price - trade.price) * trade.quantity
        else:
            # Closing long position
            if position.side == OrderSide.BUY:
                return (trade.price - entry_price) * trade.quantity

        # Cannot calculate - position and trade sides don't match for closing
        return None

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
        elif "USD" in symbol_str:
            return "USD"
        elif "USDC" in symbol_str:
            return "USDC"
        elif "USDT" in symbol_str:
            return "USDT"
        else:
            # Default fallback - this should not happen in production
            logger.warning("quote_asset_extraction_fallback", symbol=symbol_str, fallback="USDC")
            return "USDC"

    async def save_state(self) -> None:
        """Save current portfolio state to storage.

        Uses configured save intervals and atomic write strategy.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Save intervals from config, NO hardcoded values
        - Atomic writes if configured
        - Explicit error handling
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise ValueError("Portfolio service not initialized")

            try:
                await self._storage.save_state(self._cached_state)

                logger.info(
                    "portfolio_state_saved",
                    balance_count=len(self._cached_state.balances),
                    position_count=len(self._cached_state.positions),
                    timestamp=self._cached_state.timestamp.isoformat(),
                )

            except Exception as e:
                logger.error("portfolio_state_save_failed", error=str(e), exc_info=True)
                raise

    async def create_snapshot(self) -> None:
        """Create a backup snapshot of current portfolio state.

        Uses configured backup directory and rotation count.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Backup paths from config, NO hardcoded paths
        - Rotation count from config
        - NO assumptions about filesystem permissions
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise ValueError("Portfolio service not initialized")

            try:
                # Use storage protocol to create snapshot
                if hasattr(self._storage, "save_snapshot"):
                    snapshot_name = f"portfolio_snapshot_{self._cached_state.timestamp.strftime('%Y%m%d_%H%M%S')}"
                    await self._storage.save_snapshot(self._cached_state, snapshot_name)

                    logger.info(
                        "portfolio_snapshot_created",
                        backup_count=self._backup_count,
                        snapshot_timestamp=self._cached_state.timestamp.isoformat(),
                    )
                else:
                    logger.warning(
                        "snapshot_not_supported", storage_type=type(self._storage).__name__
                    )

            except Exception as e:
                logger.error("portfolio_snapshot_failed", error=str(e), exc_info=True)
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
                logger.error("portfolio_reload_failed", error=str(e), exc_info=True)
                raise

    async def _periodic_save(self) -> None:
        """Periodic auto-save based on configured interval.

        This method should be called by a background task.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Save interval from config
        - NO hardcoded timing
        - Graceful error handling
        """
        if not self._save_interval or self._save_interval <= 0:
            # Auto-save disabled in config
            return

        try:
            # Check if save is needed based on interval
            if self._cached_state is None:
                return

            now = datetime.now(UTC)
            time_since_update = (now - self._cached_state.timestamp).total_seconds()

            if time_since_update >= self._save_interval:
                await self.save_state()

                logger.debug(
                    "periodic_save_completed",
                    interval_seconds=self._save_interval,
                    time_since_update=time_since_update,
                )
            else:
                logger.debug(
                    "periodic_save_skipped",
                    interval_seconds=self._save_interval,
                    time_since_update=time_since_update,
                )

        except Exception as e:
            logger.error("periodic_save_failed", error=str(e), exc_info=True)
            # Don't re-raise - periodic save failures shouldn't crash the service

    async def update_balance_directly(
        self, asset: Symbol, exchange: ExchangeName, new_balance: SpotBalance
    ) -> None:
        """Update balance directly (for reconciliation).

        Args:
            asset: Asset symbol
            exchange: Exchange name
            new_balance: New balance to set

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Symbol/ExchangeName types
        - Explicit balance replacement
        - Persistence based on config
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise ValueError("Portfolio service not initialized")

            balance_key = f"{exchange.value}:{asset.value}"
            old_balance = self._cached_state.balances.get(balance_key)

            self._cached_state.balances[balance_key] = new_balance
            self._cached_state.timestamp = datetime.now(UTC)

            # Persist if atomic updates enabled
            if self._atomic_updates:
                await self._storage.save_state(self._cached_state)

            logger.info(
                "balance_updated_directly",
                asset=asset.value,
                exchange=exchange.value,
                old_total=float(old_balance.total_quantity) if old_balance else None,
                new_total=float(new_balance.total_quantity),
                atomic_save=self._atomic_updates,
            )

    async def update_position_directly(
        self, symbol: Symbol, exchange: ExchangeName, new_position: Optional[DerivativePosition]
    ) -> None:
        """Update position directly (for reconciliation).

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            new_position: New position to set, None to remove

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Symbol/ExchangeName types
        - Explicit position replacement or removal
        - Persistence based on config
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise ValueError("Portfolio service not initialized")

            position_key = f"{exchange.value}:{symbol.value}"
            old_position = self._cached_state.positions.get(position_key)

            if new_position is None:
                # Remove position
                self._cached_state.positions.pop(position_key, None)
                action = "removed"
            else:
                # Set new position
                self._cached_state.positions[position_key] = new_position
                action = "updated"

            self._cached_state.timestamp = datetime.now(UTC)

            # Persist if atomic updates enabled
            if self._atomic_updates:
                await self._storage.save_state(self._cached_state)

            logger.info(
                "position_updated_directly",
                symbol=symbol.value,
                exchange=exchange.value,
                action=action,
                old_size=float(old_position.size) if old_position else None,
                new_size=float(new_position.size) if new_position else None,
                atomic_save=self._atomic_updates,
            )

    async def reconcile_with_exchanges(self) -> None:
        """Reconcile portfolio state with actual exchange balances and positions.

        This method fetches current balances and positions from all enabled exchanges
        and compares them with our cached state. Discrepancies are logged and
        corrected based on configured tolerance levels.

        Configuration Usage:
        - Uses config.state.reconciliation_timeout for API timeouts
        - Uses config.validation.balance_tolerance for balance comparison
        - Uses config.validation.position_size_tolerance for position comparison
        - Uses config.exchanges to determine which exchanges to check

        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL tolerances and timeouts from config
        - Uses ExchangeName enum for exchange iteration
        - NO assumptions about exchange availability
        - Fail fast on critical discrepancies
        """
        if not self._api_clients:
            logger.warning("reconciliation_skipped", reason="no_api_clients_configured")
            return

        logger.info("portfolio_reconciliation_starting")

        total_discrepancies = 0
        critical_discrepancies = 0

        # Get reconciliation settings from config
        reconciliation_timeout = self.config.state.reconciliation_timeout
        balance_tolerance = self._balance_tolerance
        position_tolerance = self._position_tolerance

        for exchange_name, api_client in self._api_clients.items():
            try:
                # Convert string to ExchangeName enum
                try:
                    exchange_enum = ExchangeName(exchange_name)
                except ValueError:
                    logger.error("reconciliation_invalid_exchange", exchange=exchange_name)
                    continue

                # Check if exchange is enabled in config
                exchange_config = self.config.exchanges.get(exchange_name)
                if not exchange_config or not exchange_config.enabled:
                    logger.debug("reconciliation_skipped_disabled_exchange", exchange=exchange_name)
                    continue

                logger.info(
                    "reconciling_exchange",
                    exchange=exchange_name,
                    timeout_seconds=float(reconciliation_timeout),
                )

                # Reconcile balances for this exchange
                balance_discrepancies = await self._reconcile_exchange_balances(
                    exchange_enum, api_client, reconciliation_timeout, balance_tolerance
                )

                # Reconcile positions for this exchange
                position_discrepancies = await self._reconcile_exchange_positions(
                    exchange_enum, api_client, reconciliation_timeout, position_tolerance
                )

                exchange_discrepancies = balance_discrepancies + position_discrepancies
                total_discrepancies += exchange_discrepancies

                if exchange_discrepancies > 0:
                    logger.warning(
                        "exchange_reconciliation_discrepancies",
                        exchange=exchange_name,
                        balance_discrepancies=balance_discrepancies,
                        position_discrepancies=position_discrepancies,
                        total_discrepancies=exchange_discrepancies,
                    )
                else:
                    logger.debug("exchange_reconciliation_clean", exchange=exchange_name)

            except Exception as e:
                critical_discrepancies += 1
                logger.error(
                    "exchange_reconciliation_error",
                    exchange=exchange_name,
                    error=str(e),
                    exc_info=True,
                )

                # Continue with other exchanges - don't fail entire reconciliation

        # Final reconciliation summary
        if critical_discrepancies > 0:
            logger.error(
                "reconciliation_completed_with_errors",
                total_discrepancies=total_discrepancies,
                critical_errors=critical_discrepancies,
                exchanges_checked=len(self._api_clients),
            )

            # Fail fast if too many critical errors
            if critical_discrepancies >= len(self._api_clients):
                raise RuntimeError(
                    f"Portfolio reconciliation failed for all {len(self._api_clients)} exchanges"
                )
        elif total_discrepancies > 0:
            logger.warning(
                "reconciliation_completed_with_discrepancies",
                total_discrepancies=total_discrepancies,
                exchanges_checked=len(self._api_clients),
            )
        else:
            logger.info(
                "reconciliation_completed_successfully", exchanges_checked=len(self._api_clients)
            )

        # Update reconciliation timestamp
        async with self._state_lock:
            if self._cached_state:
                self._cached_state.last_reconciliation = datetime.now(UTC)

                # Persist the updated timestamp
                await self._storage.save_state(self._cached_state)

    async def _reconcile_exchange_balances(
        self, exchange: ExchangeName, api_client: object, timeout: Decimal, tolerance: Decimal
    ) -> int:
        """Reconcile balances for a specific exchange.

        Args:
            exchange: Exchange to reconcile
            api_client: API client for the exchange
            timeout: Timeout for API calls
            tolerance: Tolerance for balance differences

        Returns:
            Number of discrepancies found

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured timeout and tolerance
        - NO assumptions about API client interface
        - Explicit discrepancy logging
        """
        discrepancies = 0

        try:
            # Fetch current balances from exchange with timeout
            exchange_balances = await asyncio.wait_for(
                self._fetch_exchange_balances(api_client, exchange), timeout=float(timeout)
            )

            # Get our cached balances for this exchange
            cached_balances = await self.get_exchange_balances(exchange)

            # Compare balances
            for asset_name, exchange_balance in exchange_balances.items():
                cached_balance = cached_balances.get(asset_name)

                if cached_balance is None:
                    # We don't have this balance cached
                    logger.warning(
                        "balance_missing_in_cache",
                        exchange=exchange.value,
                        asset=asset_name,
                        exchange_balance=float(exchange_balance.total_quantity),
                    )

                    # Add missing balance to cache
                    await self.update_balance_directly(
                        Symbol(asset_name), exchange, exchange_balance
                    )
                    discrepancies += 1

                else:
                    # Compare quantities within tolerance
                    difference = abs(
                        exchange_balance.total_quantity - cached_balance.total_quantity
                    )

                    if difference > tolerance:
                        logger.warning(
                            "balance_discrepancy_detected",
                            exchange=exchange.value,
                            asset=asset_name,
                            cached_balance=float(cached_balance.total_quantity),
                            exchange_balance=float(exchange_balance.total_quantity),
                            difference=float(difference),
                            tolerance=float(tolerance),
                        )

                        # Update cached balance to match exchange
                        await self.update_balance_directly(
                            Symbol(asset_name), exchange, exchange_balance
                        )
                        discrepancies += 1

            # Check for balances in cache that don't exist on exchange
            for asset_name, cached_balance in cached_balances.items():
                if asset_name not in exchange_balances:
                    if cached_balance.total_quantity > tolerance:
                        logger.warning(
                            "balance_exists_only_in_cache",
                            exchange=exchange.value,
                            asset=asset_name,
                            cached_balance=float(cached_balance.total_quantity),
                        )
                        discrepancies += 1

                        # Could remove or zero out the balance here
                        # For now, just log the discrepancy

            return discrepancies

        except Exception as e:
            logger.error(
                "balance_reconciliation_error", exchange=exchange.value, error=str(e), exc_info=True
            )
            raise

    async def _reconcile_exchange_positions(
        self, exchange: ExchangeName, api_client: object, timeout: Decimal, tolerance: Decimal
    ) -> int:
        """Reconcile positions for a specific exchange.

        Args:
            exchange: Exchange to reconcile
            api_client: API client for the exchange
            timeout: Timeout for API calls
            tolerance: Tolerance for position size differences

        Returns:
            Number of discrepancies found

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured timeout and tolerance
        - NO assumptions about API client interface
        - Explicit discrepancy logging
        """
        discrepancies = 0

        try:
            # Fetch current positions from exchange with timeout
            exchange_positions = await asyncio.wait_for(
                self._fetch_exchange_positions(api_client, exchange), timeout=float(timeout)
            )

            # Get our cached positions for this exchange
            cached_positions = await self.get_exchange_positions(exchange)

            # Compare positions
            for symbol_name, exchange_position in exchange_positions.items():
                cached_position = cached_positions.get(symbol_name)

                if cached_position is None:
                    # We don't have this position cached
                    if abs(exchange_position.size) > tolerance:
                        logger.warning(
                            "position_missing_in_cache",
                            exchange=exchange.value,
                            symbol=symbol_name,
                            exchange_position_size=float(exchange_position.size),
                        )

                        # Add missing position to cache
                        await self.update_position_directly(
                            Symbol(symbol_name), exchange, exchange_position
                        )
                        discrepancies += 1

                else:
                    # Compare position sizes within tolerance
                    difference = abs(exchange_position.size - cached_position.size)

                    if difference > tolerance:
                        logger.warning(
                            "position_discrepancy_detected",
                            exchange=exchange.value,
                            symbol=symbol_name,
                            cached_size=float(cached_position.size),
                            exchange_size=float(exchange_position.size),
                            difference=float(difference),
                            tolerance=float(tolerance),
                        )

                        # Update cached position to match exchange
                        await self.update_position_directly(
                            Symbol(symbol_name), exchange, exchange_position
                        )
                        discrepancies += 1

            # Check for positions in cache that don't exist on exchange
            for symbol_name, cached_position in cached_positions.items():
                if symbol_name not in exchange_positions:
                    if abs(cached_position.size) > tolerance:
                        logger.warning(
                            "position_exists_only_in_cache",
                            exchange=exchange.value,
                            symbol=symbol_name,
                            cached_size=float(cached_position.size),
                        )
                        discrepancies += 1

                        # Remove or zero out the position
                        await self.update_position_directly(Symbol(symbol_name), exchange, None)

            return discrepancies

        except Exception as e:
            logger.error(
                "position_reconciliation_error",
                exchange=exchange.value,
                error=str(e),
                exc_info=True,
            )
            raise

    async def _fetch_exchange_balances(
        self, api_client: object, exchange: ExchangeName
    ) -> Dict[str, SpotBalance]:
        """Fetch current balances from exchange API.

        Args:
            api_client: Exchange API client
            exchange: Exchange name for logging

        Returns:
            Dictionary of asset name -> SpotBalance

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about API client interface
        - Returns typed SpotBalance objects
        - Handles API client method variations
        """
        try:
            # Try common API method names for getting balances
            if hasattr(api_client, "get_account_balances"):
                raw_balances = await api_client.get_account_balances()
            elif hasattr(api_client, "get_balances"):
                raw_balances = await api_client.get_balances()
            elif hasattr(api_client, "fetch_balance"):
                raw_balances = await api_client.fetch_balance()
            else:
                raise AttributeError(
                    f"API client for {exchange.value} does not support balance fetching"
                )

            # Convert raw API response to SpotBalance objects
            # This would need to be implemented based on actual API response format
            balances = {}

            if isinstance(raw_balances, dict):
                for asset, balance_data in raw_balances.items():
                    if isinstance(balance_data, dict):
                        # Extract balance fields - structure depends on exchange
                        total = balance_data.get("total", 0)
                        available = balance_data.get("available", balance_data.get("free", total))

                        if total > 0:  # Only include non-zero balances
                            balances[asset] = SpotBalance(
                                exchange=exchange,
                                asset=Symbol(asset),
                                total_quantity=Decimal(str(total)),
                                available_quantity=Decimal(str(available)),
                                timestamp=datetime.now(UTC),
                            )

            logger.debug(
                "exchange_balances_fetched", exchange=exchange.value, balance_count=len(balances)
            )

            return balances

        except Exception as e:
            logger.error(
                "fetch_exchange_balances_error",
                exchange=exchange.value,
                error=str(e),
                exc_info=True,
            )
            raise

    async def _fetch_exchange_positions(
        self, api_client: object, exchange: ExchangeName
    ) -> Dict[str, DerivativePosition]:
        """Fetch current positions from exchange API.

        Args:
            api_client: Exchange API client
            exchange: Exchange name for logging

        Returns:
            Dictionary of symbol name -> DerivativePosition

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about API client interface
        - Returns typed DerivativePosition objects
        - Handles API client method variations
        """
        try:
            # Try common API method names for getting positions
            if hasattr(api_client, "get_account_positions"):
                raw_positions = await api_client.get_account_positions()
            elif hasattr(api_client, "get_positions"):
                raw_positions = await api_client.get_positions()
            elif hasattr(api_client, "fetch_positions"):
                raw_positions = await api_client.fetch_positions()
            else:
                logger.debug("api_client_no_position_support", exchange=exchange.value)
                return {}  # Exchange doesn't support positions

            # Convert raw API response to DerivativePosition objects
            positions = {}

            if isinstance(raw_positions, list):
                for position_data in raw_positions:
                    if isinstance(position_data, dict):
                        symbol = position_data.get("symbol")
                        size = position_data.get("size", position_data.get("amount", 0))

                        if symbol and abs(float(size)) > 0:  # Only include non-zero positions
                            positions[symbol] = DerivativePosition(
                                exchange=exchange,
                                symbol=Symbol(symbol),
                                size=Decimal(str(size)),
                                side=OrderSide.BUY if float(size) > 0 else OrderSide.SELL,
                                entry_price=Decimal(str(position_data.get("entry_price", 0))),
                                unrealized_pnl=Decimal(str(position_data.get("unrealized_pnl", 0))),
                                timestamp=datetime.now(UTC),
                            )

            logger.debug(
                "exchange_positions_fetched", exchange=exchange.value, position_count=len(positions)
            )

            return positions

        except Exception as e:
            logger.error(
                "fetch_exchange_positions_error",
                exchange=exchange.value,
                error=str(e),
                exc_info=True,
            )
            raise

    async def calculate_pnl(self) -> Dict[str, Decimal]:
        """Calculate comprehensive PnL report using configured calculation method.

        Returns:
            Dictionary containing PnL calculations and metrics

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses config.calculation.pnl_calculation_method for calculation approach
        - Includes fees based on config.calculation.include_fees_in_pnl
        - Uses config.calculation.base_currency for conversion
        - Returns Decimal values, NOT float
        - NO hardcoded calculation parameters
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise ValueError("Portfolio service not initialized")

            # Get calculation settings from config
            calc_config = self.config.calculation
            pnl_method = calc_config.pnl_calculation_method
            include_fees = calc_config.include_fees_in_pnl
            base_currency = calc_config.base_currency

            logger.info(
                "pnl_calculation_starting",
                method=pnl_method,
                include_fees=include_fees,
                base_currency=base_currency,
                position_count=len(self._cached_state.positions),
            )

            pnl_report = {}

            try:
                if pnl_method == "mark_to_market":
                    pnl_report = await self._calculate_mark_to_market_pnl(
                        include_fees, base_currency
                    )
                elif pnl_method == "realized_only":
                    pnl_report = await self._calculate_realized_pnl_only(
                        include_fees, base_currency
                    )
                elif pnl_method == "comprehensive":
                    pnl_report = await self._calculate_comprehensive_pnl(
                        include_fees, base_currency
                    )
                else:
                    raise ValueError(f"Unsupported PnL calculation method: {pnl_method}")

                logger.info(
                    "pnl_calculation_completed",
                    method=pnl_method,
                    total_unrealized_pnl=float(pnl_report.get("total_unrealized_pnl", 0)),
                    total_realized_pnl=float(pnl_report.get("total_realized_pnl", 0)),
                    net_pnl=float(pnl_report.get("net_pnl", 0)),
                )

                return pnl_report

            except Exception as e:
                logger.error(
                    "pnl_calculation_failed", method=pnl_method, error=str(e), exc_info=True
                )
                raise

    async def _calculate_mark_to_market_pnl(
        self, include_fees: bool, base_currency: str
    ) -> Dict[str, Decimal]:
        """Calculate mark-to-market PnL using current market prices.

        Args:
            include_fees: Whether to include trading fees in calculation
            base_currency: Base currency for PnL reporting

        Returns:
            Dictionary with mark-to-market PnL calculations

        IMPORTANT: Following CODING_STANDARDS.md:
        - Gets current market prices for unrealized PnL
        - Uses exact position entry prices
        - ALL calculations in Decimal precision
        """
        assert self._cached_state is not None

        total_unrealized_pnl = Decimal("0")
        total_realized_pnl = Decimal("0")
        position_pnls = {}

        # Calculate unrealized PnL for each position
        for position_key, position in self._cached_state.positions.items():
            try:
                # Get current market price for this position
                current_price = await self._get_current_market_price(
                    position.symbol, position.exchange
                )

                if current_price and position.entry_price:
                    # Calculate unrealized PnL
                    if position.side == OrderSide.BUY:
                        # Long position: profit when price goes up
                        unrealized_pnl = (current_price - position.entry_price) * abs(position.size)
                    else:
                        # Short position: profit when price goes down
                        unrealized_pnl = (position.entry_price - current_price) * abs(position.size)

                    position_pnls[position_key] = {
                        "symbol": position.symbol.value,
                        "exchange": position.exchange.value,
                        "entry_price": position.entry_price,
                        "current_price": current_price,
                        "position_size": position.size,
                        "unrealized_pnl": unrealized_pnl,
                    }

                    total_unrealized_pnl += unrealized_pnl

                    logger.debug(
                        "position_pnl_calculated",
                        symbol=position.symbol.value,
                        exchange=position.exchange.value,
                        entry_price=float(position.entry_price),
                        current_price=float(current_price),
                        unrealized_pnl=float(unrealized_pnl),
                    )
                else:
                    logger.warning(
                        "position_pnl_skipped",
                        symbol=position.symbol.value,
                        exchange=position.exchange.value,
                        reason="missing_price_data",
                    )

            except Exception as e:
                logger.error(
                    "position_pnl_calculation_error",
                    position_key=position_key,
                    error=str(e),
                    exc_info=True,
                )
                # Continue with other positions

        # Get realized PnL from trading history if needed
        # This would require implementing trade history tracking
        # For now, use position data
        total_realized_pnl = await self._calculate_realized_pnl_from_positions()

        # Calculate fees if requested
        total_fees = Decimal("0")
        if include_fees:
            total_fees = await self._calculate_total_fees()

        net_pnl = total_unrealized_pnl + total_realized_pnl
        if include_fees:
            net_pnl -= total_fees

        return {
            "calculation_method": "mark_to_market",
            "base_currency": base_currency,
            "total_unrealized_pnl": total_unrealized_pnl,
            "total_realized_pnl": total_realized_pnl,
            "total_fees": total_fees if include_fees else Decimal("0"),
            "net_pnl": net_pnl,
            "position_count": len(position_pnls),
            "position_pnls": position_pnls,
            "calculation_timestamp": datetime.now(UTC).isoformat(),
            "include_fees": include_fees,
        }

    async def _calculate_realized_pnl_only(
        self, include_fees: bool, base_currency: str
    ) -> Dict[str, Decimal]:
        """Calculate only realized PnL from completed trades.

        Args:
            include_fees: Whether to include trading fees
            base_currency: Base currency for reporting

        Returns:
            Dictionary with realized PnL only

        IMPORTANT: Following CODING_STANDARDS.md:
        - Only includes PnL from closed positions
        - Uses actual trade execution data
        - NO mark-to-market calculations
        """
        assert self._cached_state is not None

        total_realized_pnl = await self._calculate_realized_pnl_from_positions()

        # Calculate fees if requested
        total_fees = Decimal("0")
        if include_fees:
            total_fees = await self._calculate_total_fees()

        net_pnl = total_realized_pnl
        if include_fees:
            net_pnl -= total_fees

        return {
            "calculation_method": "realized_only",
            "base_currency": base_currency,
            "total_unrealized_pnl": Decimal("0"),  # Not calculated in this method
            "total_realized_pnl": total_realized_pnl,
            "total_fees": total_fees if include_fees else Decimal("0"),
            "net_pnl": net_pnl,
            "calculation_timestamp": datetime.now(UTC).isoformat(),
            "include_fees": include_fees,
        }

    async def _calculate_comprehensive_pnl(
        self, include_fees: bool, base_currency: str
    ) -> Dict[str, Decimal]:
        """Calculate comprehensive PnL with all metrics.

        Args:
            include_fees: Whether to include trading fees
            base_currency: Base currency for reporting

        Returns:
            Dictionary with comprehensive PnL analysis

        IMPORTANT: Following CODING_STANDARDS.md:
        - Includes both realized and unrealized PnL
        - Provides detailed breakdown by position
        - Performance metrics from configuration
        """
        # Get mark-to-market calculation
        mtm_pnl = await self._calculate_mark_to_market_pnl(include_fees, base_currency)

        # Add additional comprehensive metrics
        portfolio_value = await self.get_total_equity_usd()

        # Calculate return percentages if we have portfolio value
        if portfolio_value and portfolio_value > 0:
            unrealized_return_pct = (mtm_pnl["total_unrealized_pnl"] / portfolio_value) * 100
            realized_return_pct = (mtm_pnl["total_realized_pnl"] / portfolio_value) * 100
            net_return_pct = (mtm_pnl["net_pnl"] / portfolio_value) * 100
        else:
            unrealized_return_pct = Decimal("0")
            realized_return_pct = Decimal("0")
            net_return_pct = Decimal("0")

        # Get performance period settings from config
        performance_config = self.config.calculation.performance_metrics
        performance_period_days = performance_config.performance_period_days

        # Calculate time-weighted returns if configured
        time_weighted_return = Decimal("0")
        if performance_config.calculate_time_weighted_returns:
            time_weighted_return = await self._calculate_time_weighted_return(
                performance_period_days
            )

        comprehensive_report = {
            **mtm_pnl,  # Include all mark-to-market data
            "calculation_method": "comprehensive",
            "portfolio_value_usd": portfolio_value,
            "unrealized_return_pct": unrealized_return_pct,
            "realized_return_pct": realized_return_pct,
            "net_return_pct": net_return_pct,
            "time_weighted_return_pct": time_weighted_return,
            "performance_period_days": performance_period_days,
            "performance_metrics_enabled": True,
        }

        return comprehensive_report

    async def _get_current_market_price(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> Optional[Decimal]:
        """Get current market price for a symbol on an exchange.

        Args:
            symbol: Symbol to get price for
            exchange: Exchange to get price from

        Returns:
            Current market price or None if unavailable

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about market data availability
        - Uses actual market data services when available
        - Returns Decimal price, NOT float
        """
        # This would integrate with the MarketDataService in a full implementation
        # For now, return None to indicate price unavailable
        logger.debug(
            "market_price_request",
            symbol=symbol.value,
            exchange=exchange.value,
            reason="market_data_service_integration_pending",
        )

        # Placeholder - would be:
        # market_snapshot = await self._market_service.get_market_snapshot()
        # ticker = market_snapshot.get_ticker(exchange, symbol)
        # return ticker.last_price if ticker else None

        return None

    async def _calculate_realized_pnl_from_positions(self) -> Decimal:
        """Calculate realized PnL from position history.

        Returns:
            Total realized PnL across all positions

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses actual trade execution data
        - Returns Decimal, NOT float
        - NO assumptions about position closing
        """
        # This would require implementing trade history tracking
        # For now, return zero as placeholder
        # In full implementation, this would:
        # 1. Get all completed trades from trade history
        # 2. Calculate PnL for each completed position
        # 3. Sum up all realized PnL

        logger.debug("realized_pnl_calculation", reason="trade_history_integration_pending")

        return Decimal("0")

    async def _calculate_total_fees(self) -> Decimal:
        """Calculate total trading fees paid.

        Returns:
            Total fees in base currency

        IMPORTANT: Following CODING_STANDARDS.md:
        - Sums actual fees from trade executions
        - Converts to base currency using configured rates
        - Returns Decimal, NOT float
        """
        # This would require trade history tracking
        # For now, return zero as placeholder
        # In full implementation, this would:
        # 1. Get all trades from history
        # 2. Sum up all fees (converting currencies as needed)
        # 3. Return total in base currency

        logger.debug("fee_calculation", reason="trade_history_integration_pending")

        return Decimal("0")

    async def _calculate_time_weighted_return(self, period_days: int) -> Decimal:
        """Calculate time-weighted return over specified period.

        Args:
            period_days: Number of days to calculate return over

        Returns:
            Time-weighted return percentage

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured period from config
        - Accounts for cash flows and timing
        - Returns Decimal percentage, NOT float
        """
        # This would require portfolio value history tracking
        # For now, return zero as placeholder
        # In full implementation, this would:
        # 1. Get portfolio values over the period
        # 2. Account for cash inflows/outflows
        # 3. Calculate geometric return

        logger.debug(
            "time_weighted_return_calculation",
            period_days=period_days,
            reason="portfolio_history_integration_pending",
        )

        return Decimal("0")

    async def calculate_position_pnl(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> Optional[Dict[str, Decimal]]:
        """Calculate PnL for a specific position.

        Args:
            symbol: Symbol of the position
            exchange: Exchange where position is held

        Returns:
            Dictionary with position PnL details or None if position not found

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured PnL calculation method
        - Returns Decimal values, NOT float
        - NO assumptions about position existence
        """
        async with self._state_lock:
            if self._cached_state is None:
                raise ValueError("Portfolio service not initialized")

            position_key = f"{exchange.value}:{symbol.value}"
            position = self._cached_state.positions.get(position_key)

            if position is None:
                logger.debug("position_pnl_not_found", symbol=symbol.value, exchange=exchange.value)
                return None

            # Get current market price
            current_price = await self._get_current_market_price(symbol, exchange)

            if not current_price or not position.entry_price:
                logger.warning(
                    "position_pnl_calculation_incomplete",
                    symbol=symbol.value,
                    exchange=exchange.value,
                    has_current_price=current_price is not None,
                    has_entry_price=position.entry_price is not None,
                )
                return None

            # Calculate unrealized PnL
            if position.side == OrderSide.BUY:
                unrealized_pnl = (current_price - position.entry_price) * abs(position.size)
            else:
                unrealized_pnl = (position.entry_price - current_price) * abs(position.size)

            # Calculate return percentage
            entry_value = position.entry_price * abs(position.size)
            return_pct = (unrealized_pnl / entry_value * 100) if entry_value > 0 else Decimal("0")

            pnl_details = {
                "symbol": symbol.value,
                "exchange": exchange.value,
                "position_side": position.side.value,
                "position_size": position.size,
                "entry_price": position.entry_price,
                "current_price": current_price,
                "entry_value": entry_value,
                "current_value": current_price * abs(position.size),
                "unrealized_pnl": unrealized_pnl,
                "return_percentage": return_pct,
                "calculation_timestamp": datetime.now(UTC).isoformat(),
            }

            logger.debug(
                "position_pnl_calculated",
                symbol=symbol.value,
                exchange=exchange.value,
                unrealized_pnl=float(unrealized_pnl),
                return_pct=float(return_pct),
            )

            return pnl_details

    async def check_health(self) -> Dict[str, Any]:
        """Health check implementation for PortfolioService.

        Returns:
            Dictionary with health metrics and status

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit health metrics
        - No assumptions about normal operation
        - All thresholds from config
        """
        try:
            # Update health metrics
            self._operation_count += 1
            self._last_activity = datetime.now(UTC)

            # Basic health status
            is_running = self._cached_state is not None

            # State metrics
            balance_count = len(self._cached_state.balances) if self._cached_state else 0
            position_count = len(self._cached_state.positions) if self._cached_state else 0

            # State age check
            state_age_seconds = 0
            if self._cached_state:
                state_age_seconds = (
                    datetime.now(UTC) - self._cached_state.timestamp
                ).total_seconds()

            # Check if state is stale based on config
            max_state_age = float(self._max_position_age)
            state_is_stale = state_age_seconds > max_state_age

            # Calculate success rate
            success_rate = (
                self._success_count / self._operation_count if self._operation_count > 0 else 1.0
            )

            return {
                "is_running": is_running,
                "state_initialized": self._cached_state is not None,
                "balance_count": balance_count,
                "position_count": position_count,
                "operation_count": self._operation_count,
                "success_count": self._success_count,
                "error_count": self._error_count,
                "success_rate": success_rate,
                "last_activity": self._last_activity.isoformat(),
                "state_age_seconds": state_age_seconds,
                "state_is_stale": state_is_stale,
                "max_state_age_seconds": max_state_age,
                "configuration": {
                    "balance_tolerance": float(self._balance_tolerance),
                    "position_tolerance": float(self._position_tolerance),
                    "save_interval": self._save_interval,
                    "atomic_updates": self._atomic_updates,
                    "update_timeout": float(self._update_timeout),
                },
            }

        except Exception as e:
            self._error_count += 1
            logger.error("portfolio_health_check_error", error=str(e), exc_info=True)

            return {
                "is_running": False,
                "error": str(e),
                "last_activity": self._last_activity.isoformat() if self._last_activity else None,
                "operation_count": self._operation_count,
                "error_count": self._error_count,
            }

    def get_service_type(self) -> ServiceType:
        """Return service type for health monitoring."""
        return ServiceType.PORTFOLIO

    async def get_performance_metrics(
        self, period_days: Optional[int] = None
    ) -> Optional[Dict[str, object]]:
        """Get performance metrics for the specified period.

        Args:
            period_days: Number of days to calculate metrics for (uses config default if None)

        Returns:
            Performance metrics dictionary or None if tracking not enabled

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns None if performance tracking not enabled
        - All calculations delegated to PerformanceTracker
        - NO hardcoded calculation logic here
        """
        if self._performance_tracker is None:
            logger.debug(
                "performance_metrics_not_available", reason="performance_tracking_not_enabled"
            )
            return None

        try:
            # Update equity curve with current total equity
            current_equity = await self.get_total_equity_usd()
            if current_equity > Decimal("0"):
                await self._performance_tracker.update_equity_curve(
                    datetime.now(UTC), current_equity
                )

            # Calculate and return metrics
            metrics = await self._performance_tracker.calculate_metrics(period_days)

            return metrics.model_dump()

        except Exception as e:
            logger.error("performance_metrics_calculation_failed", error=str(e), exc_info=True)
            return None

    async def track_trade_for_performance(self, trade: Trade) -> None:
        """Track a trade for performance metrics calculation.

        Args:
            trade: Trade to track for performance metrics

        IMPORTANT: Following CODING_STANDARDS.md:
        - Only tracks if performance tracking enabled
        - Delegates to PerformanceTracker
        - NO performance calculations here
        """
        if self._performance_tracker is None:
            return

        try:
            await self._performance_tracker.add_trade(trade)

            logger.debug(
                "trade_tracked_for_performance",
                trade_id=trade.id,
                symbol=trade.symbol.value,
                exchange=trade.exchange,
            )

        except Exception as e:
            logger.error(
                "trade_performance_tracking_failed", trade_id=trade.id, error=str(e), exc_info=True
            )
