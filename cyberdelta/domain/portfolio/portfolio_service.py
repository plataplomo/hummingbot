"""Portfolio service for managing portfolio state across exchanges.

This is the main orchestrator that uses all the specialized modules:
- PortfolioStateManager: State initialization and persistence
- ReconciliationEngine: Exchange reconciliation
- MarkToMarketCalculator: PnL calculations
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
from cyberdelta.domain.financial.calculators.mark_to_market_calculator import MarkToMarketCalculator
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.monitoring import ServiceType
from cyberdelta.enums.trading import OrderSide, PositionEventType
from cyberdelta.exceptions.portfolio import (
    PortfolioNotInitializedError,
    ReconciliationError,
)
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models import DerivativePosition, SpotBalance
from cyberdelta.models.events import PositionEvent
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
from cyberdelta.protocols.domain.market_data import MarketDataServiceProtocol
from cyberdelta.protocols.domain.portfolio import PortfolioStorageProtocol
from cyberdelta.symbols.models import Symbol

from .balance_manager import BalanceManager
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
        market_data_service: MarketDataServiceProtocol | None = None,
    ) -> None:
        """Initialize portfolio service with all modules.

        Args:
            config: Application settings
            storage: Storage implementation for persistence
            event_bus: Event bus for publishing portfolio events
            api_clients: Optional exchange API clients for reconciliation
            performance_tracker: Optional performance tracker for metrics
            market_data_service: Optional market data service for price data
        """
        self.config = config
        self._event_bus = event_bus
        self._api_clients = api_clients or {}
        self._performance_tracker = performance_tracker
        self._market_data_service = market_data_service
        # Initialize modules
        self._state_manager = PortfolioStateManager(config, storage)
        self._balance_manager = BalanceManager(config, self._state_manager)
        # Create PnL calculator before position manager (required dependency)
        self._pnl_calculator = MarkToMarketCalculator(config)
        self._position_manager = PositionManager(
            config, self._state_manager, self._pnl_calculator, self._event_bus
        )
        self._reconciliation_engine = ReconciliationEngine(
            config, storage, self._balance_manager, self._position_manager
        )

        # Health tracking attributes
        self._operation_count = 0
        self._success_count = 0
        self._error_count = 0
        self._last_activity = datetime.now(UTC)

        # Cache financial settings
        self._financial_config = config.financial
        self._base_currency = self._financial_config.currency.base_currency
        self._include_fees_default = self._financial_config.pnl.include_fees_in_pnl
        self._stablecoin_list = set(self._financial_config.currency.stablecoin_list)

        logger.info(
            "portfolio_service_initialized",
            modules_initialized=5,
            api_clients_count=len(self._api_clients),
            performance_tracking_enabled=performance_tracker is not None,
            market_data_service_enabled=market_data_service is not None,
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

        TODO: ARCHITECTURAL ISSUE - This method contains unsafe assumptions and reverse-engineering

        PROBLEMS:
        1. We're trying to reverse-engineer old_balance from current balance - this is impossible
           and incorrect because we don't know what other operations happened in between
        2. We're making assumptions about which currency was affected and how
        3. We're calculating balance changes after the fact instead of tracking transitions
        4. This approach fails with concurrent operations and multiple fills

        SOLUTION NEEDED:
        - Implement proper BalanceTransition tracking in BalanceManager
        - BalanceManager should capture old_balance BEFORE updates
        - Return BalanceTransition objects that contain both old and new state
        - Use these transitions for accurate event publishing

        FOR NOW: Skipping balance event publishing to avoid unsafe assumptions
        This will be properly implemented when BalanceTransition architecture is added.
        """
        # TODO: Remove this entire method once BalanceTransition architecture is implemented
        # Current implementation makes unsafe assumptions about balance state transitions
        # and tries to reverse-engineer old balance from current state, which is wrong

        logger.debug(
            "balance_event_publishing_skipped",
            fill_id=fill.id,
            symbol=fill.symbol.value,
            exchange=fill.exchange.value,
            reason="Unsafe assumptions removed - awaiting BalanceTransition architecture",
        )

        # TODO: Replace this skip with proper BalanceTransition-based implementation

    async def initialize(self) -> None:
        """Initialize portfolio service by loading state."""
        await self._state_manager.initialize_state()
        logger.info("portfolio_service_initialized")

    async def initialize_state(self) -> PortfolioState:
        """Initialize and return portfolio state - protocol compliance method.

        Returns:
            Initialized portfolio state
        """
        await self._state_manager.initialize_state()
        return await self._state_manager.get_state()

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
        return await self._position_manager.get_positions_for_exchange(exchange)

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

        Raises:
            PortfolioNotInitializedError: If portfolio state is not initialized
        """
        # Get current portfolio state
        portfolio_state = await self.get_state()
        if not portfolio_state:
            raise PortfolioNotInitializedError

        # Fetch current market prices for all positions
        mark_prices: dict[str, Decimal] = {}
        total_equity_usd = Decimal(0)
        total_exposure_usd = Decimal(0)
        position_pnls: dict[str, PositionPnLDetail] = {}

        # Calculate individual position PnL and aggregate data
        for position_key, position in portfolio_state.positions.items():
            if position.size == Decimal(0):
                continue

            # Extract symbol and exchange from position
            symbol = position.symbol
            exchange = position.exchange

            # Get current market price
            current_price = await self._get_current_market_price(symbol, exchange)
            if current_price is None:
                logger.warning(
                    "pnl_calculation_missing_price",
                    position_key=position_key,
                    symbol=symbol.value,
                    exchange=exchange.value,
                    reason="Market price unavailable - position excluded from PnL",
                )
                continue

            # Store price for portfolio calculation
            symbol_key = symbol.value
            mark_prices[symbol_key] = current_price

            # Calculate position PnL
            position_pnl = self._pnl_calculator.calculate_unrealized_pnl(
                position=position, mark_price=current_price, include_fees=True
            )

            # Calculate market value
            market_value_usd = current_price * abs(position.size)
            total_exposure_usd += market_value_usd

            # Create position PnL detail
            position_detail = PositionPnLDetail(
                symbol=symbol,
                exchange=exchange,
                unrealized_pnl_usd=position_pnl.amount,
                realized_pnl_usd=Decimal(0),  # Realized PnL tracked separately
                total_pnl_usd=position_pnl.amount,
                entry_price=position.entry_price or Decimal(0),
                current_price=current_price,
                quantity=position.size,
                market_value_usd=market_value_usd,
            )
            position_pnls[position_key] = position_detail

        # Calculate portfolio PnL using fetched prices
        portfolio_pnl_result = self._pnl_calculator.calculate_portfolio_pnl(
            positions=list(portfolio_state.positions.values()),
            mark_prices=mark_prices,
            include_fees=True,
        )

        # Calculate total equity (balances + position values)
        for balance in portfolio_state.balances.values():
            # Convert balance to USD equivalent using configured stablecoins
            if balance.asset.value in self._stablecoin_list:
                total_equity_usd += balance.total_quantity

        # Add unrealized PnL to equity
        total_equity_usd += portfolio_pnl_result.amount

        return PnLReport(
            total_unrealized_pnl_usd=portfolio_pnl_result.amount,
            total_realized_pnl_usd=Decimal(0),  # Would need fill tracking for this
            net_pnl_usd=portfolio_pnl_result.amount,
            total_equity_usd=total_equity_usd,
            total_exposure_usd=total_exposure_usd,
            calculation_timestamp=portfolio_pnl_result.calculation_timestamp,
            calculation_method=portfolio_pnl_result.calculation_method,
            fees_included=self._include_fees_default,
            base_currency=self._base_currency,
            position_pnls=position_pnls,
        )

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
        # Get current portfolio state
        portfolio_state = await self.get_state()
        if not portfolio_state:
            return None

        # Find the specific position using the key format
        position_key = f"{exchange.value}:{symbol.value}"
        position = portfolio_state.positions.get(position_key)

        if not position:
            return None

        # Skip zero positions
        if position.size == Decimal(0):
            return None

        # Get current market price
        current_price = await self._get_current_market_price(symbol, exchange)
        if current_price is None:
            logger.warning(
                "position_pnl_calculation_no_price",
                position_key=position_key,
                symbol=symbol.value,
                exchange=exchange.value,
                reason="Market price unavailable for position PnL calculation",
            )
            # Return detail with zero values if price unavailable
            return PositionPnLDetail(
                symbol=symbol,
                exchange=exchange,
                unrealized_pnl_usd=Decimal(0),
                realized_pnl_usd=Decimal(0),
                total_pnl_usd=Decimal(0),
                entry_price=position.entry_price or Decimal(0),
                current_price=Decimal(0),
                quantity=position.size,
                market_value_usd=Decimal(0),
            )

        # Calculate unrealized PnL for this position
        pnl_result = self._pnl_calculator.calculate_unrealized_pnl(
            position=position, mark_price=current_price, include_fees=True
        )

        # Calculate market value
        market_value_usd = current_price * abs(position.size)

        return PositionPnLDetail(
            symbol=symbol,
            exchange=exchange,
            unrealized_pnl_usd=pnl_result.amount,
            realized_pnl_usd=Decimal(0),  # Realized PnL tracked separately
            total_pnl_usd=pnl_result.amount,
            entry_price=position.entry_price or Decimal(0),
            current_price=current_price,
            quantity=position.size,
            market_value_usd=market_value_usd,
        )

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
                position_count = 0  # This is a count, not financial - OK to hardcode

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
                safe_mode_enabled=self.config.general.safe_mode,
                max_slippage_pct=self.config.execution.max_slippage_pct,
                max_retries=self.config.execution.max_retries,
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
                safe_mode_enabled=self.config.general.safe_mode,
                max_slippage_pct=self.config.execution.max_slippage_pct,
                max_retries=self.config.execution.max_retries,
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
        if self._market_data_service is None:
            logger.debug(
                "market_price_unavailable_no_service",
                symbol=symbol.value,
                exchange=exchange.value,
                reason="No market data service configured",
            )
            return None

        try:
            ticker = await self._market_data_service.get_ticker(symbol)
        except (ValueError, TypeError, ConnectionError, TimeoutError) as e:
            logger.warning(
                "market_price_fetch_error",
                symbol=symbol.value,
                exchange=exchange.value,
                error=str(e),
            )
            return None

        # ticker cannot be None here due to protocol guarantees
        # The get_ticker method raises ValueError if ticker is not available

        if ticker.price is None:
            logger.debug(
                "market_price_unavailable_no_price",
                symbol=symbol.value,
                exchange=exchange.value,
                reason="Price not available in ticker",
            )
            return None

        return ticker.price

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

        # Try underscore separator first
        if "_" in symbol_str:
            return symbol_str.split("_")[-1]

        # Check against configured stablecoins
        for stablecoin in self._stablecoin_list:
            if stablecoin in symbol_str:
                return stablecoin

        # Fallback to configured base currency
        logger.warning(
            "quote_asset_extraction_fallback",
            symbol=symbol_str,
            fallback=self._base_currency,
        )
        return self._base_currency
