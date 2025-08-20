"""Portfolio service for managing portfolio state across exchanges.

This is the main orchestrator that uses all the specialized modules:
- PortfolioStateManager: State initialization and persistence
- ReconciliationEngine: Exchange reconciliation
- MarkToMarketCalculator: PnL calculations
- BalanceManager: Balance operations
- PositionManager: Position operations
- PortfolioQueries: Query operations (split for file size)
- PortfolioUpdates: Update operations (split for file size)
- PortfolioCalculations: Calculation operations (split for file size)
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.financial.calculators.mark_to_market_calculator import MarkToMarketCalculator
from cyberdelta.domain.monitoring.health_metrics import HealthMetrics
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.monitoring import ServiceType
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models import DerivativePosition, SpotBalance
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.portfolio.pnl_report import (
    PnLReport,
    PositionPnLDetail,
    ReconciliationReport,
)
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.protocols import HealthCheckable
from cyberdelta.protocols.domain.market_data import MarketDataServiceProtocol
from cyberdelta.protocols.domain.portfolio import (
    PortfolioStateManagerProtocol,
    PortfolioStorageProtocol,
)
from cyberdelta.symbols.models import Symbol

from .balance_manager import BalanceManager
from .portfolio_calculations import PortfolioCalculations
from .portfolio_queries import PortfolioQueries
from .portfolio_updates import PortfolioUpdates
from .position_manager import PositionManager
from .reconciliation_engine import ReconciliationEngine
from .state_manager import PortfolioStateManager


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI

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

    File size management:
    - Operations split into PortfolioQueries, PortfolioUpdates, PortfolioCalculations
    - Each module stays under 600 lines
    - Business logic preserved exactly as before
    """

    def __init__(
        self,
        config: AppSettings,
        storage: PortfolioStorageProtocol,
        event_bus: EventBus,
        api_clients: dict[str, ExchangeAPI],
        market_data_service: MarketDataServiceProtocol,
    ) -> None:
        """Initialize portfolio service with all modules.

        Args:
            config: Application settings
            storage: Storage implementation for persistence
            event_bus: Event bus for publishing portfolio events
            api_clients: Exchange API clients for reconciliation
            market_data_service: Market data service for price data
        """
        self.config = config
        self._event_bus = event_bus
        self._api_clients = api_clients
        self._market_data_service = market_data_service

        # Initialize core modules
        self._state_manager = PortfolioStateManager(config, storage)
        self._balance_manager = BalanceManager(config, self._state_manager)

        # Create PnL calculator before position manager (required dependency)
        self._pnl_calculator = MarkToMarketCalculator(config)
        self._position_manager = PositionManager(
            config, self._state_manager, self._pnl_calculator, self._event_bus
        )

        # Initialize reconciliation engine (required for calculations)
        self._reconciliation_engine = ReconciliationEngine(
            config=config,
            storage=storage,
            balance_manager=self._balance_manager,
            position_manager=self._position_manager,
        )

        # Initialize operation modules (split for file size management)
        self._queries = PortfolioQueries(
            config=config,
            state_manager=self._state_manager,
            balance_manager=self._balance_manager,
            position_manager=self._position_manager,
        )

        self._updates = PortfolioUpdates(
            config=config,
            state_manager=self._state_manager,
            balance_manager=self._balance_manager,
            position_manager=self._position_manager,
            event_bus=self._event_bus,
        )

        # Initialize calculations module
        self._calculations = PortfolioCalculations(
            config=config,
            state_manager=self._state_manager,
            pnl_calculator=self._pnl_calculator,
            reconciliation_engine=self._reconciliation_engine,
            api_clients=self._api_clients,
            market_data_service=self._market_data_service,
        )

        logger.info(
            "portfolio_service_initialized",
            reconciliation_engine_type=type(self._reconciliation_engine).__name__,
            market_data_service_type=type(self._market_data_service).__name__,
            exchange_count=len(self._api_clients),
        )

    # ========== Properties ==========

    @property
    def state_manager(self) -> PortfolioStateManagerProtocol:
        """Get the portfolio state manager for protocol compliance.

        Returns:
            The state manager that implements PortfolioStateManagerProtocol
        """
        return self._state_manager

    # ========== Initialization Methods ==========

    async def initialize(self) -> None:
        """Initialize portfolio service (compatibility method)."""
        await self.initialize_state()

    async def initialize_state(self) -> PortfolioState:
        """Initialize portfolio state.

        Delegates to PortfolioUpdates module.

        Returns:
            Initial portfolio state
        """
        return await self._updates.initialize_state()

    # ========== Query Methods (delegated to PortfolioQueries) ==========

    async def get_state(self) -> PortfolioState:
        """Get current portfolio state.

        Returns:
            Current portfolio state
        """
        return await self._queries.get_state()

    async def get_total_equity_usd(self) -> Decimal:
        """Get total portfolio equity in USD.

        Returns:
            Total equity in USD
        """
        return await self._queries.get_total_equity_usd()

    async def list_snapshots(self) -> list[str]:
        """List available portfolio snapshots.

        Returns:
            List of snapshot identifiers
        """
        return await self._queries.list_snapshots()

    async def get_balance(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
        balance_type: str = "available",
    ) -> Decimal:
        """Get balance for specific asset.

        Args:
            symbol: Asset symbol
            exchange: Exchange name
            balance_type: Type of balance to get

        Returns:
            Balance amount in Decimal
        """
        return await self._queries.get_balance(symbol, exchange, balance_type)

    async def get_exchange_balances(
        self,
        exchange: ExchangeName,
        non_zero_only: bool = True,
    ) -> list[SpotBalance]:
        """Get all balances for a specific exchange.

        Args:
            exchange: Exchange name
            non_zero_only: Whether to exclude zero balances

        Returns:
            List of SpotBalance objects
        """
        return await self._queries.get_exchange_balances(exchange, non_zero_only)

    async def get_position(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> DerivativePosition | None:
        """Get position for specific symbol and exchange.

        Args:
            symbol: Trading symbol
            exchange: Exchange name

        Returns:
            Position if exists, None otherwise
        """
        return await self._queries.get_position(symbol, exchange)

    async def get_exchange_positions(self, exchange: ExchangeName) -> list[DerivativePosition]:
        """Get all positions for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            List of DerivativePosition objects
        """
        return await self._queries.get_exchange_positions(exchange)

    # ========== Update Methods (delegated to PortfolioUpdates) ==========

    async def save_state(self) -> None:
        """Save current portfolio state to storage."""
        await self._updates.save_state()

    async def create_snapshot(self) -> None:
        """Create a snapshot of current portfolio state."""
        await self._updates.create_snapshot()

    async def delete_snapshot(self, snapshot_name: str) -> None:
        """Delete a portfolio snapshot."""
        await self._updates.delete_snapshot(snapshot_name)

    async def load_from_storage(self) -> None:
        """Load portfolio state from storage."""
        await self._updates.load_from_storage()

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
            total: Total balance if different from available

        Returns:
            Updated SpotBalance object
        """
        return await self._updates.update_balance_directly(asset, exchange, available, total)

    async def set_position(self, position: DerivativePosition) -> DerivativePosition:
        """Set position directly (for reconciliation).

        Args:
            position: Position to set

        Returns:
            The position that was set
        """
        return await self._updates.set_position(position)

    async def remove_position(self, symbol: Symbol, exchange: ExchangeName) -> None:
        """Remove a position."""
        await self._updates.remove_position(symbol, exchange)

    async def update_from_fill(self, fill: Fill) -> None:
        """Update portfolio from a fill."""
        await self._updates.update_from_fill(fill)

    # ========== Calculation Methods (delegated to PortfolioCalculations) ==========

    async def calculate_pnl(self) -> PnLReport:
        """Calculate comprehensive PnL report.

        Returns:
            PnLReport with all calculations
        """
        return await self._calculations.calculate_pnl()

    async def calculate_position_pnl(
        self,
        position: DerivativePosition,
        mark_price: Decimal | None = None,
    ) -> PositionPnLDetail:
        """Calculate PnL for a single position.

        Args:
            position: Position to calculate PnL for
            mark_price: Optional override for mark price

        Returns:
            PositionPnLDetail with calculations
        """
        return await self._calculations.calculate_position_pnl(position, mark_price)

    async def reconcile_with_exchanges(self) -> ReconciliationReport:
        """Reconcile portfolio state with exchanges.

        Returns:
            ReconciliationReport with differences
        """
        return await self._calculations.reconcile_with_exchanges()

    async def check_health(self) -> HealthMetrics:
        """Check portfolio service health.

        Returns:
            Health metrics for the portfolio service
        """
        # Get the latest portfolio state
        state = await self.get_state()

        # Determine if service is healthy based on state
        has_data = bool(state.balances or state.positions)

        return HealthMetrics(
            response_time_ms=None,  # Could track query response times
            error_count=0,  # Could track reconciliation errors
            success_count=1 if has_data else 0,  # Basic health indicator
            last_activity=state.timestamp if has_data else None,
            uptime_seconds=None,  # Could track from service start
            memory_usage_mb=None,  # Could get from system metrics
            cpu_usage_percent=None,  # Could get from system metrics
            is_running=True,  # Service is running if we're checking health
        )

    def get_service_type(self) -> ServiceType:
        """Get service type for monitoring.

        Returns:
            ServiceType enum value
        """
        return ServiceType.PORTFOLIO
