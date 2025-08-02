"""Null object pattern implementations for optional services."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.calculations import (
    PortfolioExposureResult,
)
from cyberdelta.core.portfolio.portfolio_types.protocols import (
    BalanceManagerProtocol,
    OrderManagerProtocol,
    PortfolioManagerProtocol,
    PositionManagerProtocol,
    StateManagerProtocol,
)
from cyberdelta.core.portfolio.portfolio_types.models import (
    PortfolioSnapshot,
    PortfolioUpdate,
)
from cyberdelta.core.portfolio.portfolio_types.infrastructure import (
    ResilienceMetrics,
    ResilienceResult,
)
from cyberdelta.core.portfolio.portfolio_types.models import (
    CapitalSummary,
    ExposureMetrics,
    ManagerStats,
    PerformanceMetrics,
    PnLSummary,
    PortfolioSummary,
)
from cyberdelta.core.portfolio.portfolio_types.infrastructure import ValidationResult
# Import SpotBalance from core models, not portfolio_types models
from cyberdelta.core.models import Trade, DerivativePosition, SpotBalance
from cyberdelta.core.symbols import Symbol


if TYPE_CHECKING:
    from cyberdelta.core.models import Order
    from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData as PortfolioState


logger = get_logger(__name__)


class NullBalanceManager(BalanceManagerProtocol):
    """Null object implementation of BalanceManagerProtocol."""

    def __init__(self) -> None:
        """Initialize null balance manager."""
        logger.warning(
            "null_balance_manager_created",
            message="Using null balance manager - balance operations will be no-ops",
        )

    async def update_balances(self, exchange_id: str, balances: list[SpotBalance]) -> None:
        """No-op balance update."""
        logger.debug("null_balance_update", exchange_id=exchange_id, balance_count=len(balances))

    async def get_balance(self, exchange_id: str, asset: str) -> SpotBalance | None:
        """Return None for all balance queries."""
        logger.debug("null_balance_get", exchange_id=exchange_id, asset=asset)
        return None

    async def get_total_balance_in_currency(self, asset: str, target_currency: str) -> Decimal:
        """Return zero balance."""
        logger.debug("null_balance_total", asset=asset, target_currency=target_currency)
        return Decimal(0)

    async def update_balance_from_trade(self, trade: Trade) -> None:
        """No-op trade update."""
        logger.debug("null_balance_trade_update", trade_id=trade.id)
    
    async def update_balance(self, exchange_id: str, balance: SpotBalance) -> None:
        """No-op single balance update."""
        logger.debug("null_balance_single_update", exchange_id=exchange_id, asset=balance.asset)
    
    async def get_all_balances(self, exchange_id: str | None = None) -> dict[str, SpotBalance]:
        """Return empty balances."""
        logger.debug("null_balance_get_all", exchange_id=exchange_id)
        return {}


class NullPositionManager(PositionManagerProtocol):
    """Null object implementation of PositionManagerProtocol."""

    def __init__(self) -> None:
        """Initialize null position manager."""
        logger.warning(
            "null_position_manager_created",
            message="Using null position manager - position operations will be no-ops",
        )

    async def update_positions(self, exchange_id: str, positions: list[DerivativePosition]) -> None:
        """No-op position update."""
        logger.debug("null_position_update", exchange_id=exchange_id, position_count=len(positions))

    async def update_position_from_trade(self, trade: Trade) -> None:
        """No-op trade update."""
        logger.debug("null_position_trade_update", trade_id=trade.id)

    async def get_position(self, exchange_id: str, symbol: Symbol) -> DerivativePosition | None:
        """Return None for all position queries."""
        logger.debug("null_position_get", exchange_id=exchange_id, symbol=symbol)
        return None

    async def get_positions_by_symbol(self, symbol: Symbol) -> list[DerivativePosition]:
        """Return empty list."""
        logger.debug("null_positions_by_symbol", symbol=symbol)
        return []
    
    async def update_position(self, exchange_id: str, position: DerivativePosition) -> None:
        """No-op position update."""
        logger.debug("null_position_update", exchange_id=exchange_id, symbol=position.symbol)
    
    async def close_position(self, exchange_id: str, symbol: Symbol) -> None:
        """No-op position close."""
        logger.debug("null_position_close", exchange_id=exchange_id, symbol=symbol)
    
    async def get_all_positions(self, exchange_id: str | None = None) -> list[DerivativePosition]:
        """Return empty positions."""
        logger.debug("null_position_get_all", exchange_id=exchange_id)
        return []


class NullOrderManager(OrderManagerProtocol):
    """Null object implementation of OrderManagerProtocol."""

    def __init__(self) -> None:
        """Initialize null order manager."""
        logger.warning(
            "null_order_manager_created",
            message="Using null order manager - order operations will be no-ops",
        )

    async def update_orders(self, exchange_id: str, orders: list[Order]) -> None:
        """No-op order update."""
        logger.debug("null_order_update", exchange_id=exchange_id, order_count=len(orders))

    async def get_order(self, exchange_id: str, order_id: str) -> Order | None:
        """Return None for all order queries."""
        logger.debug("null_order_get", exchange_id=exchange_id, order_id=order_id)
        return None

    async def get_orders_by_symbol(self, symbol: Symbol) -> list[Order]:
        """Return empty list."""
        logger.debug("null_orders_by_symbol", symbol=symbol)
        return []
    
    async def get_open_orders(self, exchange_id: str | None = None) -> list[Order]:
        """Return empty list of orders."""
        logger.debug("null_get_open_orders", exchange_id=exchange_id)
        return []
    
    async def add_order(self, exchange_id: str, order: Order) -> None:
        """No-op order add."""
        logger.debug("null_add_order", exchange_id=exchange_id, order_id=order.client_order_id)
    
    async def update_order(self, exchange_id: str, order: Order) -> None:
        """No-op order update."""
        logger.debug("null_update_order", exchange_id=exchange_id, order_id=order.client_order_id)
    
    async def cancel_order(self, exchange_id: str, order_id: str) -> None:
        """No-op order cancel."""
        logger.debug("null_cancel_order", exchange_id=exchange_id, order_id=order_id)


class NullStateManager(StateManagerProtocol):
    """Null object implementation of StateManagerProtocol."""

    def __init__(self) -> None:
        """Initialize null state manager."""
        logger.warning(
            "null_state_manager_created",
            message="Using null state manager - all operations will return empty/zero values",
        )

    async def process_trade(self, trade: Trade) -> bool:
        """Always return True (success) but do nothing.

        Returns:
            bool: Always True indicating successful processing
        """
        logger.debug("null_process_trade", trade_id=trade.id)
        return True

    async def update_from_orchestrator(self, update_data: PortfolioUpdate) -> None:
        """No-op orchestrator update."""
        logger.debug("null_orchestrator_update", update_id=str(update_data.update_id))

    async def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        """Return empty portfolio snapshot."""
        import time
        return PortfolioSnapshot(
            portfolio_id="null_portfolio",
            timestamp=time.time(),
            total_value=Decimal(0),
            cash_balance=Decimal(0),
            positions_value=Decimal(0),
            realized_pnl=Decimal(0),
            unrealized_pnl=Decimal(0),
            fees_paid=Decimal(0),
            gross_exposure=Decimal(0),
            net_exposure=Decimal(0),
            leverage=Decimal(0),
            position_count=0
        )

    async def calculate_total_pnl(self) -> Decimal:
        """Return zero PnL."""
        return Decimal(0)

    async def calculate_exposure_metrics(self) -> ExposureMetrics:
        """Return empty exposure metrics."""
        return ExposureMetrics(
            total_exposure=Decimal(0),
            gross_exposure=Decimal(0),
            net_exposure=Decimal(0),
            long_exposure=Decimal(0),
            short_exposure=Decimal(0),
        )

    async def get_total_capital(self) -> CapitalSummary:
        """Return zero capital."""
        return CapitalSummary(
            total_capital=Decimal(0),
            free_capital=Decimal(0),
            used_capital=Decimal(0),
            reserved_capital=Decimal(0),
            by_exchange={},
            by_asset={},
        )

    async def get_portfolio_summary(self) -> PortfolioSummary:
        """Return empty portfolio summary."""
        from datetime import datetime, UTC
        
        return PortfolioSummary(
            portfolio_id="null_portfolio",
            timestamp=datetime.now(UTC),
            total_value=Decimal(0),
            cash_balance=Decimal(0),
            positions_value=Decimal(0),
            total_pnl=Decimal(0),
            daily_pnl=Decimal(0),
            leverage=Decimal(0),
            exposure=Decimal(0),
            var_95=None,
            active_positions=0,
            open_orders=0,
            today_trades=0,
            is_healthy=True,
            warnings=[],
        )

    async def calculate_portfolio_exposure(self, base_currency: str) -> PortfolioExposureResult:
        """Return zero exposure."""
        return PortfolioExposureResult(
            total_long_exposure=Decimal(0),
            total_short_exposure=Decimal(0),
            total_net_exposure=Decimal(0),
            total_gross_exposure=Decimal(0),
            currency=base_currency,
            by_symbol={},
            by_exchange={},
        )

    async def get_pnl_summary(self) -> PnLSummary:
        """Return zero PnL summary."""
        return PnLSummary(
            total_pnl=Decimal(0),
            realized_pnl=Decimal(0),
            unrealized_pnl=Decimal(0),
            fees_paid=Decimal(0),
            net_pnl=Decimal(0),
            by_exchange={},
            by_symbol={},
        )

    async def get_all_positions(self, exchange_id: str | None = None) -> list[DerivativePosition]:
        """Return empty positions."""
        return []

    async def get_all_balances(self, exchange_id: str | None = None) -> dict[str, SpotBalance]:
        """Return empty balances."""
        return {}
    
    async def get_balance(self, exchange_id: str, asset: str) -> SpotBalance | None:
        """Return None for all balance queries."""
        logger.debug("null_state_balance_get", exchange_id=exchange_id, asset=asset)
        return None
    
    async def get_position(self, exchange_id: str, symbol: Symbol) -> DerivativePosition | None:
        """Return None for all position queries."""
        logger.debug("null_state_position_get", exchange_id=exchange_id, symbol=symbol)
        return None
    
    async def get_order(self, exchange_id: str, order_id: str) -> Order | None:
        """Return None for all order queries."""
        logger.debug("null_state_order_get", exchange_id=exchange_id, order_id=order_id)
        return None
    
    async def get_open_orders(self, exchange_id: str | None = None) -> list[Order]:
        """Return empty list of orders."""
        logger.debug("null_state_get_open_orders", exchange_id=exchange_id)
        return []
    
    async def get_recent_trades(self, limit: int = 100) -> list[Trade]:
        """Return empty list of trades."""
        logger.debug("null_state_get_recent_trades", limit=limit)
        return []
    
    async def get_portfolio_value(self, base_currency: str = "USDC") -> Decimal:
        """Return zero portfolio value."""
        logger.debug("null_state_get_portfolio_value", base_currency=base_currency)
        return Decimal(0)
    
    async def get_exposure_metrics(self, valuation_asset: str = "USDC") -> dict[str, Decimal]:
        """Return empty exposure metrics."""
        logger.debug("null_state_get_exposure_metrics", valuation_asset=valuation_asset)
        return {}
    
    async def update_balance(self, exchange_id: str, balance: SpotBalance) -> None:
        """No-op balance update."""
        logger.debug("null_state_update_balance", exchange_id=exchange_id, asset=balance.asset)
    
    async def update_position(self, exchange_id: str, position: DerivativePosition) -> None:
        """No-op position update."""
        logger.debug("null_state_update_position", exchange_id=exchange_id, symbol=position.symbol)
    
    async def add_order(self, exchange_id: str, order: Order) -> None:
        """No-op order add."""
        logger.debug("null_state_add_order", exchange_id=exchange_id, order_id=order.client_order_id)
    
    async def update_order(self, exchange_id: str, order: Order) -> None:
        """No-op order update."""
        logger.debug("null_state_update_order", exchange_id=exchange_id, order_id=order.client_order_id)
    
    async def add_trade(self, trade: Trade) -> None:
        """No-op trade add."""
        logger.debug("null_state_add_trade", trade_id=trade.id)
    
    async def clear_state(self) -> None:
        """No-op state clear."""
        logger.debug("null_state_clear")
    
    async def export_state(self) -> dict[str, Any]:
        """Return empty state export."""
        logger.debug("null_state_export")
        return {}
    
    async def import_state(self, state_data: dict[str, Any]) -> None:
        """No-op state import."""
        logger.debug("null_state_import", data_keys=list(state_data.keys()))
    
    async def create_snapshot(self, snapshot_id: str) -> None:
        """No-op snapshot creation."""
        logger.debug("null_state_create_snapshot", snapshot_id=snapshot_id)
    
    async def restore_snapshot(self, snapshot_id: str) -> None:
        """No-op snapshot restore."""
        logger.debug("null_state_restore_snapshot", snapshot_id=snapshot_id)

    async def get_all_orders(self) -> dict[str, Order]:
        """Return empty orders."""
        return {}

    async def get_manager_stats(self) -> ManagerStats:
        """Return empty stats."""
        return ManagerStats(
            manager_name="null_state_manager",
            operations_count=0,
            success_count=0,
            error_count=0,
            avg_response_time_ms=0.0,
            last_operation_time=None,
            uptime_seconds=0.0,
        )

    async def update_balances(self, exchange_id: str, balances: dict[str, SpotBalance]) -> None:
        """No-op balance update."""
        logger.debug("null_state_balance_update", exchange_id=exchange_id)

    async def update_positions(self, exchange_id: str, positions: list[DerivativePosition]) -> None:
        """No-op position update."""
        logger.debug("null_state_position_update", exchange_id=exchange_id)

    async def initialize(self) -> None:
        """No-op initialization."""
        logger.debug("null_state_initialize")

    async def shutdown(self) -> None:
        """No-op shutdown."""
        logger.debug("null_state_shutdown")


class NullPortfolioManager(PortfolioManagerProtocol[None]):
    """Null object implementation of PortfolioManagerProtocol."""

    @property
    def name(self) -> str:
        """Manager name."""
        return "NullPortfolioManager"

    @property
    def is_initialized(self) -> bool:
        """Always initialized."""
        return True

    async def initialize(self) -> None:
        """No-op initialization."""
        logger.debug("null_portfolio_initialize")

    async def shutdown(self) -> None:
        """No-op shutdown."""
        logger.debug("null_portfolio_shutdown")
    
    async def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        """Return zero capital."""
        logger.debug("null_portfolio_get_total_capital", base_currency=base_currency)
        return Decimal(0)
    
    async def get_positions(self, exchange_id: str | None = None) -> list[DerivativePosition]:
        """Return empty positions."""
        logger.debug("null_portfolio_get_positions", exchange_id=exchange_id)
        return []
    
    async def get_balances(self, exchange_id: str | None = None) -> dict[str, SpotBalance]:
        """Return empty balances."""
        logger.debug("null_portfolio_get_balances", exchange_id=exchange_id)
        return {}
    
    async def get_exposure_metrics(self, valuation_asset: str = "USDC") -> ExposureMetrics:
        """Return empty exposure metrics."""
        logger.debug("null_portfolio_get_exposure_metrics", valuation_asset=valuation_asset)
        return ExposureMetrics(
            total_exposure=Decimal(0),
            gross_exposure=Decimal(0),
            net_exposure=Decimal(0),
            long_exposure=Decimal(0),
            short_exposure=Decimal(0),
        )
    
    async def get_portfolio_state(self) -> PortfolioState:
        """Return minimal portfolio state."""
        from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData
        logger.debug("null_portfolio_get_portfolio_state")
        # Return a minimal PortfolioState with required fields
        return PortfolioStateData(
            state_id="null_portfolio_state",
            portfolio_id="null_portfolio",
            total_account_value=Decimal(0)
        )
    
    async def update_portfolio(self, update: PortfolioUpdate) -> None:
        """No-op portfolio update."""
        logger.debug("null_portfolio_update", update_id=str(update.update_id))


class NullResilienceService:
    """Null object implementation for resilience service."""

    def __init__(self) -> None:
        """Initialize null resilience service."""
        logger.warning(
            "null_resilience_service_created",
            message="Using null resilience service - no resilience features active",
        )

    async def execute_with_resilience(
        self,
        service_name: str,
        func: Callable[..., Any],
        *args: object,
        **kwargs: object,
    ) -> object:
        """Execute function directly without resilience.

        Returns:
            object: Result of the function execution
        """
        logger.debug("null_resilience_execute", service_name=service_name)
        return await func(*args, **kwargs)

    def get_resilience_result(self, value: object) -> ResilienceResult[object]:
        """Return successful result without resilience metrics."""
        metrics = ResilienceMetrics(
            total_requests=1,
            successful_requests=1,
            failed_requests=0,
            timeouts=0,
            circuit_breaker_opens=0,
            fallback_successes=0,
            fallback_failures=0,
            average_response_time_ms=0.0,
            p95_response_time_ms=0.0,
            p99_response_time_ms=0.0,
        )
        # Create a ResilienceResult instance directly
        from cyberdelta.core.portfolio.portfolio_types.infrastructure import ResilienceResult
        return ResilienceResult(
            success=True,
            value=value,
            metrics=metrics,
            error=None,
            used_fallback=False,
        )


class NullValidationService:
    """Null object implementation for validation service."""

    def __init__(self) -> None:
        """Initialize null validation service."""
        logger.warning(
            "null_validation_service_created",
            message="Using null validation service - all validations will pass",
        )

    async def validate_trade(self, trade: Trade) -> ValidationResult[Trade]:
        """Always return valid.

        Returns:
            ValidationResult[Trade]: Successful validation result for the trade
        """
        return ValidationResult[Trade](
            is_valid=True,
            validated_data=trade,
            issues=[],
            validator_name="null_validator",
        )

    async def validate_balance(self, balance: SpotBalance) -> ValidationResult[SpotBalance]:
        """Always return valid.

        Returns:
            ValidationResult[SpotBalance]: Successful validation result for the balance
        """
        return ValidationResult[SpotBalance](
            is_valid=True,
            validated_data=balance,
            issues=[],
            validator_name="null_validator",
        )

    async def validate_position(
        self, position: DerivativePosition
    ) -> ValidationResult[DerivativePosition]:
        """Always return valid.

        Returns:
            ValidationResult[DerivativePosition]: Successful validation result for the position
        """
        return ValidationResult[DerivativePosition](
            is_valid=True,
            validated_data=position,
            issues=[],
            validator_name="null_validator",
        )

    async def validate_portfolio_state(self) -> ValidationResult[object]:
        """Always return valid.

        Returns:
            ValidationResult[object]: Successful validation result for the portfolio state
        """
        return ValidationResult[object](
            is_valid=True,
            validated_data={},
            issues=[],
            validator_name="null_validator",
        )


# Factory functions for creating null objects
def create_null_balance_manager() -> BalanceManagerProtocol:
    """Create null balance manager.

    Returns:
        BalanceManagerProtocol: Null balance manager instance
    """
    return NullBalanceManager()


def create_null_position_manager() -> PositionManagerProtocol:
    """Create null position manager.

    Returns:
        PositionManagerProtocol: Null position manager instance
    """
    return NullPositionManager()


def create_null_order_manager() -> OrderManagerProtocol:
    """Create null order manager.

    Returns:
        OrderManagerProtocol: Null order manager instance
    """
    return NullOrderManager()


def create_null_state_manager() -> StateManagerProtocol:
    """Create null state manager.

    Returns:
        StateManagerProtocol: Null state manager instance
    """
    return NullStateManager()


def create_null_portfolio_manager() -> PortfolioManagerProtocol[None]:
    """Create null portfolio manager.

    Returns:
        PortfolioManagerProtocol[None]: Null portfolio manager instance
    """
    return NullPortfolioManager()


def create_null_resilience_service() -> NullResilienceService:
    """Create null resilience service.

    Returns:
        NullResilienceService: Null resilience service instance
    """
    return NullResilienceService()


def create_null_validation_service() -> NullValidationService:
    """Create null validation service.

    Returns:
        NullValidationService: Null validation service instance
    """
    return NullValidationService()
