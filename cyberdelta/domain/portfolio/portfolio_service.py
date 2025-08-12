"""Portfolio service for managing portfolio state across exchanges.

This is the main orchestrator that uses all the specialized modules:
- PortfolioStateManager: State initialization and persistence
- ReconciliationEngine: Exchange reconciliation
- PnLCalculator: PnL calculations
- BalanceManager: Balance operations
- PositionManager: Position operations
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.monitoring import BalanceEventType, ServiceType
from cyberdelta.enums.trading import OrderSide, PositionEventType
from cyberdelta.exceptions.portfolio import (
    PortfolioNotInitializedError,
    ReconciliationError,
)
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models import DerivativePosition, SpotBalance
from cyberdelta.models.events import BalanceEvent, PositionEvent
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.monitoring.system_health_models import ExecutionStatistics
from cyberdelta.models.portfolio.pnl_report import (
    PnLReport,
    PositionPnLDetail,
    ReconciliationReport,
)
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.models.trading.order_tracker_statistics import OrderTrackerStatistics
from cyberdelta.protocols import HealthCheckable
from cyberdelta.protocols.domain.portfolio import PortfolioStorageProtocol
from cyberdelta.symbols.models import Symbol

from .balance_manager import BalanceManager
from .pnl_calculator import PnLCalculator
from .position_manager import PositionManager
from .reconciliation_engine import ReconciliationEngine
from .state_manager import PortfolioStateManager


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.domain.monitoring.performance_tracker import PerformanceTracker

logger = get_logger(__name__)


class PortfolioService(HealthCheckable):
    """Main portfolio service orchestrator.

    This service coordinates all portfolio operations by delegating to
    specialized modules. It provides a unified interface for portfolio
    management across all exchanges.

    Configuration Integration:
    - All configuration through AppSettings
    - NO hardcoded values
    - Follows CODING_STANDARDS.md strictly
    """

    def __init__(
        self,
        config: AppSettings,
        storage: PortfolioStorageProtocol,
        event_bus: EventBus,
        api_clients: dict[str, ExchangeAPI] | None = None,
        performance_tracker: PerformanceTracker | None = None,
    ) -> None:
        """Initialize portfolio service with all modules.

        Args:
            config: Application settings
            storage: Storage implementation for persistence
            event_bus: Event bus for publishing portfolio events
            api_clients: Optional exchange API clients for reconciliation
            performance_tracker: Optional performance tracker for metrics
        """
        self.config = config
        self._event_bus = event_bus
        self._api_clients = api_clients or {}
        self._performance_tracker = performance_tracker
        # Initialize modules
        self._state_manager = PortfolioStateManager(config, storage)
        self._balance_manager = BalanceManager(config, self._state_manager)
        self._position_manager = PositionManager(config, self._state_manager)
        self._reconciliation_engine = ReconciliationEngine(
            config, storage, self._balance_manager, self._position_manager
        )
        self._pnl_calculator = PnLCalculator(config, self._state_manager)

        # Health tracking attributes
        self._operation_count = 0
        self._success_count = 0
        self._error_count = 0
        self._last_activity = datetime.now(UTC)

        logger.info(
            "portfolio_service_initialized",
            modules_initialized=5,
            api_clients_count=len(self._api_clients),
            performance_tracking_enabled=performance_tracker is not None,
        )

    async def _publish_position_update_event(
        self, fill: Fill, realized_pnl: Decimal | None
    ) -> None:
        """Publish position update event from fill.

        Args:
            fill: Fill that caused the position update
            realized_pnl: Realized PnL from position change (if any)
        """
        try:
            # Get current position for the symbol
            position = await self._position_manager.get_position(fill.symbol, fill.exchange)

            position_event = PositionEvent(
                position_id=f"{fill.symbol}_{fill.exchange.value}",
                symbol=str(fill.symbol),
                exchange=fill.exchange,
                event_type=PositionEventType.UPDATED,
                size=position.size if position else Decimal(0),
                average_price=(
                    position.entry_price if position and position.entry_price else fill.price
                ),
                realized_pnl=realized_pnl,
                timestamp=time.time(),
            )
            await self._event_bus.publish(position_event)

        except (ValueError, TypeError, ConnectionError, TimeoutError) as e:
            logger.warning(
                "position_event_publishing_failed",
                fill_id=fill.id,
                symbol=fill.symbol.value,
                error=str(e),
            )
            # Don't re-raise - event publishing failure shouldn't cancel portfolio update

    async def _publish_balance_update_event(self, fill: Fill) -> None:
        """Publish balance update event from fill.

        Args:
            fill: Fill that caused the balance update
        """
        try:
            # Determine the currency affected by the fill
            if fill.side == OrderSide.BUY:
                # Buying: spent quote currency, received base currency
                affected_currency = (
                    str(fill.symbol.quote_asset)
                    if hasattr(fill.symbol, "quote_asset")
                    else str(fill.symbol)
                )
            else:
                # Selling: spent base currency, received quote currency
                # Fill.symbol is a Symbol object, convert to string for now
                affected_currency = (
                    str(fill.symbol.base_asset)
                    if hasattr(fill.symbol, "base_asset")
                    else str(fill.symbol)
                )

            # Get current balance for the affected currency
            balances = await self._balance_manager.get_exchange_balances(fill.exchange)
            current_balance = next(
                (b for b in balances.values() if str(b.asset) == affected_currency), None
            )

            balance_event = BalanceEvent(
                account_id=f"portfolio_{fill.exchange.value}",
                currency=affected_currency,
                exchange=fill.exchange,
                event_type=BalanceEventType.UPDATED,
                old_balance=Decimal(0),  # We don't track old balance in fill
                new_balance=current_balance.total_quantity if current_balance else Decimal(0),
                locked_amount=(
                    (current_balance.total_quantity - current_balance.available_quantity)
                    if current_balance
                    else Decimal(0)
                ),
                timestamp=time.time(),
            )
            await self._event_bus.publish(balance_event)

        except (ValueError, TypeError, ConnectionError, TimeoutError) as e:
            logger.warning(
                "balance_event_publishing_failed",
                fill_id=fill.id,
                symbol=fill.symbol.value,
                error=str(e),
            )
            # Don't re-raise - event publishing failure shouldn't cancel portfolio update

    async def initialize(self) -> None:
        """Initialize portfolio service by loading state."""
        await self._state_manager.initialize_state()
        logger.info("portfolio_service_initialized")

    # State operations (delegated to state manager)

    async def get_state(self) -> PortfolioState:
        """Get current portfolio state.

        Returns:
            Current portfolio state
        """
        # get_state() already raises PortfolioNotInitializedError if state is None
        return await self._state_manager.get_state()

    async def save_state(self) -> None:
        """Save current portfolio state."""
        await self._state_manager.save_state()

    async def get_total_equity_usd(self) -> Decimal:
        """Get total portfolio equity in USD.

        Returns:
            Total equity across all exchanges in USD
        """
        return await self._state_manager.get_total_equity_usd()

    async def create_snapshot(self) -> None:
        """Create a backup snapshot of current portfolio state."""
        await self._state_manager.create_snapshot()

    async def list_snapshots(self) -> list[str]:
        """List all available portfolio snapshots.

        Returns:
            List of snapshot names/identifiers
        """
        return await self._state_manager.list_snapshots()

    async def delete_snapshot(self, snapshot_name: str) -> None:
        """Delete a named snapshot.

        Args:
            snapshot_name: Name/identifier of snapshot to delete
        """
        await self._state_manager.delete_snapshot(snapshot_name)

    async def load_from_storage(self) -> None:
        """Force reload portfolio state from storage."""
        await self._state_manager.load_from_storage()

    # Balance operations (delegated to balance manager)

    async def get_balance(
        self,
        asset: Symbol,
        exchange: ExchangeName,
    ) -> SpotBalance | None:
        """Get balance for specific asset on exchange.

        Args:
            asset: Asset symbol
            exchange: Exchange name

        Returns:
            Balance if found, None otherwise
        """
        return await self._balance_manager.get_balance(asset, exchange)

    async def get_exchange_balances(
        self,
        exchange: ExchangeName,
    ) -> dict[str, SpotBalance]:
        """Get all balances for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            Dictionary of asset -> SpotBalance for the exchange
        """
        state = await self._state_manager.get_state()
        if not state:
            return {}

        exchange_balances: dict[str, SpotBalance] = {}
        prefix = f"{exchange.value}:"

        for key, balance in state.balances.items():
            if key.startswith(prefix):
                asset_name = key[len(prefix) :]
                exchange_balances[asset_name] = balance

        return exchange_balances

    async def update_balance_directly(
        self,
        asset: Symbol,
        exchange: ExchangeName,
        new_balance: SpotBalance,
    ) -> None:
        """Update balance directly (for reconciliation).

        Args:
            asset: Asset symbol
            exchange: Exchange name
            new_balance: New balance to set
        """
        await self._state_manager.update_balance(exchange, asset, new_balance)

    # Position operations (delegated to position manager)

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
        return await self._position_manager.get_position(symbol, exchange)

    async def get_exchange_positions(
        self,
        exchange: ExchangeName,
    ) -> dict[str, DerivativePosition]:
        """Get all positions for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            Dictionary of symbol -> DerivativePosition for the exchange
        """
        return await self._position_manager.get_all_positions(exchange)

    async def update_position_directly(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
        new_position: DerivativePosition | None,
    ) -> None:
        """Update position directly (for reconciliation).

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            new_position: New position to set, None to remove
        """
        await self._state_manager.update_position(exchange, symbol, new_position)

    # Fill updates (coordinated across managers)

    async def update_from_fill(self, fill: Fill) -> None:
        """Update portfolio state from fill execution.

        This coordinates updates across position and balance managers.

        Args:
            fill: Executed fill
        """
        try:
            self._operation_count += 1

            # Update position
            realized_pnl = await self._position_manager.update_position_from_fill(fill)

            # Update balance
            await self._balance_manager.update_balance_from_fill(fill)

            # Track fill for performance if enabled
            await self.track_fill_for_performance(fill)

            # Publish portfolio events
            await self._publish_position_update_event(fill, realized_pnl)
            await self._publish_balance_update_event(fill)

            self._success_count += 1
            self._last_activity = datetime.now(UTC)

            logger.info(
                "portfolio_updated_from_fill",
                fill_id=fill.id,
                symbol=fill.symbol.value,
                exchange=fill.exchange,
                side=fill.side.value,
                quantity=fill.quantity,
                price=fill.price,
                realized_pnl=realized_pnl or None,
            )

        except Exception as e:
            self._error_count += 1
            logger.exception(
                "portfolio_update_from_fill_failed",
                fill_id=fill.id,
                error=str(e),
            )
            raise

    # PnL operations (delegated to PnL calculator)

    async def calculate_pnl(self) -> PnLReport:
        """Calculate comprehensive PnL report.

        Returns:
            Typed comprehensive PnL report
        """
        return await self._pnl_calculator.calculate_pnl()

    async def calculate_position_pnl(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
    ) -> PositionPnLDetail | None:
        """Calculate PnL for specific position.

        Args:
            symbol: Trading symbol
            exchange: Exchange name

        Returns:
            Typed PnL data for the position or None if not found
        """
        return await self._pnl_calculator.calculate_position_pnl(symbol, exchange)

    # Reconciliation operations (delegated to reconciliation engine)

    async def reconcile_with_exchanges(self) -> ReconciliationReport:
        """Reconcile portfolio state with all exchanges.

        Returns:
            Typed reconciliation results including errors and discrepancies

        Raises:
            ReconciliationError: If reconciliation fails critically
        """
        if not self._api_clients:
            logger.warning("reconciliation_skipped", reason="no_api_clients_configured")
            return ReconciliationReport(
                reconciliation_timestamp=datetime.now(UTC),
                reconciliation_successful=False,
                total_discrepancies=0,
                exchange_results={},
                balance_discrepancies=[],
                position_discrepancies=[],
                error_messages=["No API clients configured"],
            )

        results = await self._reconciliation_engine.reconcile_with_exchanges(self._api_clients)

        # Check for critical errors
        critical_errors = len(results.error_messages) if results.error_messages else 0
        if critical_errors >= len(self._api_clients):
            raise ReconciliationError(len(self._api_clients))

        return results

    # Health check implementation

    async def check_health(self) -> ExecutionStatistics:
        """Health check implementation for PortfolioService.

        Returns:
            Dictionary with health metrics and status
        """
        try:
            self._operation_count += 1
            self._last_activity = datetime.now(UTC)

            # Get state info
            try:
                state = await self._state_manager.get_state()
                position_count = len(state.positions)
                # Note: State age could be calculated here if needed for future health checks
            except PortfolioNotInitializedError:
                position_count = 0

            # Calculate success rate
            success_rate = (
                self._success_count / self._operation_count if self._operation_count > 0 else 1.0
            )

            # Create OrderTrackerStatistics for composition
            order_stats = OrderTrackerStatistics(
                active_orders=position_count,
                total_orders=self._operation_count,
                success_count=self._success_count,
                error_count=self._error_count,
                success_rate=Decimal(str(success_rate)),
                last_activity_timestamp=self._last_activity,
            )

            return ExecutionStatistics(
                order_tracking=order_stats,
                api_clients_available=len(self._api_clients),
                safe_mode_enabled=False,
                max_slippage_pct=Decimal(0),
                max_retries=0,
            )

        except Exception as e:
            self._error_count += 1
            logger.exception("portfolio_health_check_error", error=str(e))

            # Create OrderTrackerStatistics for error case
            order_stats = OrderTrackerStatistics(
                active_orders=0,
                total_orders=self._operation_count,
                success_count=self._success_count,
                error_count=self._error_count,
                success_rate=Decimal(0),
                last_activity_timestamp=self._last_activity,
            )

            return ExecutionStatistics(
                order_tracking=order_stats,
                api_clients_available=0,
                safe_mode_enabled=False,
                max_slippage_pct=Decimal(0),
                max_retries=0,
            )

    def get_service_type(self) -> ServiceType:
        """Return service type for health monitoring."""
        return ServiceType.PORTFOLIO

    # Performance tracking

    async def get_performance_metrics(
        self,
        period_days: int | None = None,
    ) -> dict[str, object] | None:
        """Get performance metrics for the specified period.

        Args:
            period_days: Number of days to calculate metrics for

        Returns:
            Performance metrics dictionary or None if not enabled
        """
        if self._performance_tracker is None:
            return None

        try:
            # Update equity curve with current total equity
            current_equity = await self.get_total_equity_usd()
            if current_equity > Decimal(0):
                await self._performance_tracker.update_equity_curve(
                    datetime.now(UTC), current_equity
                )

            # Calculate and return metrics
            metrics = await self._performance_tracker.calculate_metrics(period_days)
            return metrics.model_dump()

        except Exception as e:
            logger.exception("performance_metrics_calculation_failed", error=str(e))
            return None

    async def track_fill_for_performance(self, fill: Fill) -> None:
        """Track a fill for performance metrics calculation.

        Args:
            fill: Fill to track for performance metrics
        """
        if self._performance_tracker is None:
            return

        try:
            await self._performance_tracker.add_fill(fill)

            logger.debug(
                "fill_tracked_for_performance",
                fill_id=fill.id,
                symbol=fill.symbol.value,
                exchange=fill.exchange,
            )

        except Exception as e:
            logger.exception(
                "fill_performance_tracking_failed",
                fill_id=fill.id,
                error=str(e),
            )

    # Helper methods

    async def _get_current_market_price(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
    ) -> Decimal | None:
        """Get current market price for a symbol on an exchange.

        Args:
            symbol: Symbol to get price for
            exchange: Exchange to get price from

        Returns:
            Current market price or None if unavailable
        """
        # This would integrate with MarketDataService
        # For now, return None to indicate price unavailable
        logger.debug(
            "market_price_request",
            symbol=symbol.value,
            exchange=exchange.value,
            reason="market_data_service_integration_pending",
        )
        return None

    # Compatibility methods for existing code

    async def _update_position_from_fill(self, fill: Fill) -> None:
        """Update position from fill (compatibility wrapper)."""
        await self._position_manager.update_position_from_fill(fill)

    async def _update_balance_from_fill(self, fill: Fill) -> None:
        """Update balance from fill (compatibility wrapper)."""
        await self._balance_manager.update_balance_from_fill(fill)

    def _calculate_average_price(
        self,
        existing_size: Decimal,
        existing_price: Decimal,
        new_quantity: Decimal,
        new_price: Decimal,
        trade_side: OrderSide,
    ) -> Decimal:
        """Calculate average price (compatibility wrapper).

        Returns:
            Weighted average price
        """
        if trade_side == OrderSide.SELL:
            return existing_price

        existing_value = existing_size * existing_price
        new_value = new_quantity * new_price
        total_size = existing_size + new_quantity

        if total_size == Decimal(0):
            return Decimal(0)

        return (existing_value + new_value) / total_size

    def _calculate_realized_pnl(
        self,
        position: DerivativePosition,
        fill: Fill,
    ) -> Decimal | None:
        """Calculate realized PnL (compatibility wrapper).

        Returns:
            Realized PnL amount or None if cannot calculate
        """
        if position.entry_price is None:
            logger.warning(
                "position_missing_entry_price",
                position_key=f"{position.exchange}:{position.symbol.value}",
                message="Cannot calculate realized PnL without entry price",
            )
            return None

        entry_price = position.entry_price

        if fill.side == OrderSide.BUY:
            if position.side == OrderSide.SELL:
                return (entry_price - fill.price) * fill.quantity
        elif position.side == OrderSide.BUY:
            return (fill.price - entry_price) * fill.quantity

        return None

    def _extract_quote_asset(self, symbol: Symbol) -> str:
        """Extract quote asset from symbol (compatibility wrapper).

        Returns:
            Quote asset string
        """
        symbol_str = symbol.value

        if "_" in symbol_str:
            return symbol_str.split("_")[-1]
        if "USD" in symbol_str:
            return "USD"
        if "USDC" in symbol_str:
            return "USDC"
        if "USDT" in symbol_str:
            return "USDT"

        logger.warning(
            "quote_asset_extraction_fallback",
            symbol=symbol_str,
            fallback="USDC",
        )
        return "USDC"
