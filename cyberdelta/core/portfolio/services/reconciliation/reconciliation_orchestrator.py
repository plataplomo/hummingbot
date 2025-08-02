"""Reconciliation orchestrator - coordinates focused reconciliation services."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.infrastructure.services.base_service import BaseService

from .balance_reconciliation_service import BalanceDiscrepancy, BalanceReconciliationService
from .order_reconciliation_service import OrderDiscrepancy, OrderReconciliationService
from .position_reconciliation_service import PositionDiscrepancy, PositionReconciliationService
from .trade_reconciliation_service import TradeDiscrepancy, TradeReconciliationService

if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade

logger = get_logger(__name__)


class ReconciliationResult:
    """Result of portfolio reconciliation."""
    
    def __init__(
        self,
        is_reconciled: bool,
        balance_discrepancies: list[BalanceDiscrepancy],
        position_discrepancies: list[PositionDiscrepancy],
        order_discrepancies: list[OrderDiscrepancy],
        trade_discrepancies: list[TradeDiscrepancy],
    ):
        self.is_reconciled = is_reconciled
        self.balance_discrepancies = balance_discrepancies
        self.position_discrepancies = position_discrepancies
        self.order_discrepancies = order_discrepancies
        self.trade_discrepancies = trade_discrepancies
        
        # Calculate totals
        self.total_discrepancies = (
            len(balance_discrepancies) +
            len(position_discrepancies) +
            len(order_discrepancies) +
            len(trade_discrepancies)
        )
        
        # Separate by severity
        self.errors = []
        self.warnings = []
        self.info = []
        
        discrepancy_lists: list[list[Any]] = [
            balance_discrepancies,
            position_discrepancies,
            order_discrepancies,
            trade_discrepancies
        ]
        
        for discrepancy_list in discrepancy_lists:
            for disc in discrepancy_list:
                if disc.severity == "error":
                    self.errors.append(disc)
                elif disc.severity == "warning":
                    self.warnings.append(disc)
                else:
                    self.info.append(disc)
        
        # Build summary
        self.summary = {
            "is_reconciled": is_reconciled,
            "total_discrepancies": self.total_discrepancies,
            "errors": len(self.errors),
            "warnings": len(self.warnings),
            "info": len(self.info),
            "by_type": {
                "balance": len(balance_discrepancies),
                "position": len(position_discrepancies),
                "order": len(order_discrepancies),
                "trade": len(trade_discrepancies),
            }
        }


class ReconciliationOrchestrator(BaseService):
    """Orchestrates portfolio reconciliation using focused services."""
    
    # Service dependencies
    balance_service: BalanceReconciliationService = Field(
        ..., description="Balance reconciliation service"
    )
    position_service: PositionReconciliationService = Field(
        ..., description="Position reconciliation service"
    )
    order_service: OrderReconciliationService = Field(
        ..., description="Order reconciliation service"
    )
    trade_service: TradeReconciliationService = Field(
        ..., description="Trade reconciliation service"
    )
    
    # Configuration
    enable_balance_checks: bool = Field(default=True)
    enable_position_checks: bool = Field(default=True)
    enable_order_checks: bool = Field(default=True)
    enable_trade_checks: bool = Field(default=True)
    
    # Thresholds
    integrity_score_healthy_threshold: int = Field(
        default=95,
        ge=0,
        le=100,
        description="Minimum integrity score for healthy portfolio"
    )
    warning_count_threshold: int = Field(
        default=10,
        ge=0,
        description="Maximum warnings before recommending review"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
    async def _initialize_internal(self) -> None:
        """Initialize orchestrator and all services."""
        # Start services using base class lifecycle methods
        await self.balance_service.start()
        await self.position_service.start()
        await self.order_service.start()
        await self.trade_service.start()
        
        logger.info("Reconciliation orchestrator initialized")
    
    async def _shutdown_internal(self) -> None:
        """Shutdown orchestrator and all services."""
        # Stop services using base class lifecycle methods
        await self.trade_service.stop()
        await self.order_service.stop()
        await self.position_service.stop()
        await self.balance_service.stop()
        
        logger.info("Reconciliation orchestrator shutdown")
    
    async def reconcile_portfolio(
        self,
        balances: dict[str, dict[str, SpotBalance]] | None = None,
        positions: dict[str, dict[str, DerivativePosition]] | None = None,
        orders: dict[str, list[Order]] | None = None,
        trades: dict[str, list[Trade]] | None = None,
    ) -> ReconciliationResult:
        """Reconcile entire portfolio using focused services.
        
        Args:
            balances: Spot balances by exchange and asset
            positions: Derivative positions by exchange and symbol
            orders: Orders by exchange
            trades: Trades by exchange
            
        Returns:
            Comprehensive reconciliation result
        """
        # Run reconciliation checks in parallel
        balance_discrepancies = []
        position_discrepancies = []
        order_discrepancies = []
        trade_discrepancies = []
        
        # Balance reconciliation
        if self.enable_balance_checks and balances:
            balance_discrepancies = await self.balance_service.reconcile_balances(balances)
            logger.info(
                "Balance reconciliation complete",
                discrepancies=len(balance_discrepancies)
            )
        
        # Position reconciliation
        if self.enable_position_checks and positions:
            position_discrepancies = await self.position_service.reconcile_positions(positions)
            logger.info(
                "Position reconciliation complete",
                discrepancies=len(position_discrepancies)
            )
        
        # Order reconciliation
        if self.enable_order_checks and orders:
            order_discrepancies = await self.order_service.reconcile_orders(orders)
            logger.info(
                "Order reconciliation complete",
                discrepancies=len(order_discrepancies)
            )
        
        # Trade reconciliation
        if self.enable_trade_checks and trades:
            trade_discrepancies = await self.trade_service.reconcile_trades(trades)
            logger.info(
                "Trade reconciliation complete",
                discrepancies=len(trade_discrepancies)
            )
        
        # Determine if portfolio is reconciled
        error_count = sum(
            1 for disc in (
                balance_discrepancies +
                position_discrepancies +
                order_discrepancies +
                trade_discrepancies
            )
            if disc.severity == "error"
        )
        
        is_reconciled = error_count == 0
        
        # Create result
        result = ReconciliationResult(
            is_reconciled=is_reconciled,
            balance_discrepancies=balance_discrepancies,
            position_discrepancies=position_discrepancies,
            order_discrepancies=order_discrepancies,
            trade_discrepancies=trade_discrepancies,
        )
        
        # Log summary
        logger.info(
            "Portfolio reconciliation complete",
            **result.summary
        )
        
        return result
    
    async def validate_portfolio_integrity(
        self,
        balances: dict[str, dict[str, SpotBalance]] | None = None,
        positions: dict[str, dict[str, DerivativePosition]] | None = None,
        orders: dict[str, list[Order]] | None = None,
        trades: dict[str, list[Trade]] | None = None,
    ) -> dict[str, Any]:
        """Validate overall portfolio integrity.
        
        Returns:
            Dictionary with integrity metrics and recommendations
        """
        # Run full reconciliation
        result = await self.reconcile_portfolio(
            balances=balances,
            positions=positions,
            orders=orders,
            trades=trades,
        )
        
        # Calculate integrity score
        total_checks = 0
        passed_checks = 0
        
        # Count checks performed
        if balances:
            total_checks += len(balances) * 3  # Multiple checks per balance
        if positions:
            total_checks += len(positions) * 4  # Multiple checks per position
        if orders:
            total_checks += sum(len(o) for o in orders.values())
        if trades:
            total_checks += sum(len(t) for t in trades.values())
        
        # Calculate passed checks
        if total_checks > 0:
            failed_checks = len(result.errors)
            passed_checks = max(0, total_checks - failed_checks)
            integrity_score = (passed_checks / total_checks) * 100
        else:
            integrity_score = 100.0
        
        # Determine health status
        if integrity_score >= self.integrity_score_healthy_threshold:
            health_status = "healthy"
        elif integrity_score >= 80:
            health_status = "warning"
        else:
            health_status = "critical"
        
        # Build recommendations
        recommendations = []
        
        if result.errors:
            recommendations.append(
                f"Address {len(result.errors)} critical errors immediately"
            )
        
        if len(result.warnings) > self.warning_count_threshold:
            recommendations.append(
                f"Review {len(result.warnings)} warnings - exceeds threshold"
            )
        
        # Type-specific recommendations
        if result.balance_discrepancies:
            recommendations.append(
                "Investigate balance discrepancies - possible sync issues"
            )
        
        if result.position_discrepancies:
            recommendations.append(
                "Review position calculations and leverage limits"
            )
        
        if result.order_discrepancies:
            recommendations.append(
                "Check for stale orders and order management logic"
            )
        
        if result.trade_discrepancies:
            recommendations.append(
                "Verify trade execution and recording accuracy"
            )
        
        return {
            "integrity_score": round(integrity_score, 2),
            "health_status": health_status,
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "reconciliation_summary": result.summary,
            "recommendations": recommendations,
        }