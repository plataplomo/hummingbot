"""Portfolio health checking service for balance, position, and order health."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.models import HealthStatus
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.portfolio_types.protocols import PortfolioManagerProtocol


@dataclass
class PortfolioHealthAlert:
    """Alert for portfolio health issues."""
    alert_type: str
    severity: str
    message: str
    details: dict[str, Any]
    timestamp: datetime | None = None
    
    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now(UTC)


@dataclass
class PortfolioHealthMetric:
    """Metric for portfolio health."""
    metric_name: str
    current_value: float
    threshold_value: float | None = None
    status: HealthStatus = HealthStatus.HEALTHY
    timestamp: datetime | None = None
    
    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now(UTC)


class PortfolioHealthChecker(BasePortfolioService):
    """Checks health of portfolio data - balances, positions, orders."""

    def __init__(
        self, 
        portfolio_manager: PortfolioManagerProtocol[Any],
        config: dict[str, Any] | None = None
    ):
        super().__init__("portfolio_health_checker")
        self.portfolio_manager = portfolio_manager
        self.config = config or {}
        self.logger = get_logger(__name__)
        
        # Health thresholds
        self.balance_thresholds = {
            "negative_balance_warning": Decimal(0),
            "low_balance_warning": Decimal(100),
            "balance_staleness_hours": 6
        }
        
        self.position_thresholds = {
            "large_position_warning": Decimal(10000),
            "position_staleness_hours": 4,
            "unrealized_loss_warning": Decimal(500)
        }
        
        self.order_thresholds = {
            "stale_order_hours": 24,
            "failed_order_rate_warning": 0.1
        }

    async def _initialize_service(self) -> None:
        """Initialize portfolio health checker."""
        self.logger.info("Initializing portfolio health checker")

    async def _shutdown_service(self) -> None:
        """Shutdown portfolio health checker."""
        self.logger.info("Shutting down portfolio health checker")

    async def check_portfolio_health(self) -> dict[str, Any]:
        """Check overall portfolio health."""
        current_time = datetime.now(UTC)
        
        # Check different aspects of portfolio health
        balance_results = await self._check_balance_health()
        position_results = await self._check_position_health() 
        order_results = await self._check_order_health()
        
        # Combine results
        all_metrics = (
            balance_results["metrics"] + 
            position_results["metrics"] + 
            order_results["metrics"]
        )
        
        all_alerts = (
            balance_results["alerts"] +
            position_results["alerts"] +
            order_results["alerts"]
        )
        
        # Calculate overall health score
        overall_score = self._calculate_health_score(all_metrics)
        overall_status = self._determine_overall_status(all_metrics, all_alerts)
        
        return {
            "overall_status": overall_status.value,
            "overall_score": overall_score,
            "balance_health": balance_results,
            "position_health": position_results,
            "order_health": order_results,
            "total_metrics": len(all_metrics),
            "total_alerts": len(all_alerts),
            "timestamp": current_time
        }

    async def _check_balance_health(self) -> dict[str, Any]:
        """Check health of portfolio balances."""
        metrics = []
        alerts = []
        current_time = datetime.now(UTC)
        
        try:
            # Get balances from all exchanges
            balances = await self.portfolio_manager.get_balances()
            
            for exchange_id, exchange_balances in balances.items():
                for asset, balance in exchange_balances.items():
                    # Check for negative balances
                    if balance.total_quantity < self.balance_thresholds["negative_balance_warning"]:
                        alerts.append(PortfolioHealthAlert(
                            alert_type="negative_balance",
                            severity="critical",
                            message=f"Negative balance detected for {asset} on {exchange_id}",
                            details={
                                "exchange": exchange_id,
                                "asset": asset,
                                "balance": float(balance.total_quantity)
                            }
                        ))
                    
                    # Check for low balances
                    elif balance.total_quantity < self.balance_thresholds["low_balance_warning"]:
                        alerts.append(PortfolioHealthAlert(
                            alert_type="low_balance",
                            severity="warning", 
                            message=f"Low balance for {asset} on {exchange_id}",
                            details={
                                "exchange": exchange_id,
                                "asset": asset,
                                "balance": float(balance.total_quantity)
                            }
                        ))
                    
                    # Check balance staleness
                    if balance.timestamp:
                        staleness_hours = (current_time - balance.timestamp).total_seconds() / 3600
                        if staleness_hours > self.balance_thresholds["balance_staleness_hours"]:
                            alerts.append(PortfolioHealthAlert(
                                alert_type="stale_balance",
                                severity="warning",
                                message=f"Stale balance data for {asset} on {exchange_id}",
                                details={
                                    "exchange": exchange_id,
                                    "asset": asset,
                                    "staleness_hours": staleness_hours
                                }
                            ))
                    
                    # Create health metric
                    status = HealthStatus.HEALTHY
                    if any(alert.alert_type in ["negative_balance", "stale_balance"] and 
                          alert.details.get("asset") == asset and 
                          alert.details.get("exchange") == exchange_id for alert in alerts):
                        status = HealthStatus.WARNING
                    
                    metrics.append(PortfolioHealthMetric(
                        metric_name=f"balance_{exchange_id}_{asset}",
                        current_value=float(balance.total_quantity),
                        threshold_value=float(self.balance_thresholds["low_balance_warning"]),
                        status=status
                    ))
            
        except Exception as e:
            self.logger.error(f"Error checking balance health: {e}")
            alerts.append(PortfolioHealthAlert(
                alert_type="balance_check_error",
                severity="critical",
                message="Failed to check balance health",
                details={"error": str(e)}
            ))
        
        return {
            "metrics": metrics,
            "alerts": alerts,
            "timestamp": current_time
        }

    async def _check_position_health(self) -> dict[str, Any]:
        """Check health of portfolio positions."""
        metrics = []
        alerts = []
        current_time = datetime.now(UTC)
        
        try:
            # Get positions from all exchanges
            positions = await self.portfolio_manager.get_positions()
            
            for position in positions:
                # Check for large positions
                position_value = abs(position.size) * (position.mark_price or position.entry_price or Decimal(0))
                if position_value > self.position_thresholds["large_position_warning"]:
                    alerts.append(PortfolioHealthAlert(
                        alert_type="large_position",
                        severity="warning",
                        message=f"Large position detected: {position.symbol} on {position.exchange}",
                        details={
                            "exchange": position.exchange,
                            "symbol": position.symbol,
                            "size": float(position.size),
                            "value": float(position_value)
                        }
                    ))
                
                # Check for significant unrealized losses
                if (position.unrealized_pnl and 
                    position.unrealized_pnl < -self.position_thresholds["unrealized_loss_warning"]):
                    alerts.append(PortfolioHealthAlert(
                        alert_type="unrealized_loss",
                        severity="warning",
                        message=f"Significant unrealized loss: {position.symbol} on {position.exchange}",
                        details={
                            "exchange": position.exchange,
                            "symbol": position.symbol,
                            "unrealized_pnl": float(position.unrealized_pnl)
                        }
                    ))
                
                # Create position health metric
                status = HealthStatus.HEALTHY
                if position.unrealized_pnl and position.unrealized_pnl < -self.position_thresholds["unrealized_loss_warning"]:
                    status = HealthStatus.WARNING
                
                metrics.append(PortfolioHealthMetric(
                    metric_name=f"position_{position.exchange}_{position.symbol}",
                    current_value=float(position.unrealized_pnl or 0),
                    threshold_value=float(-self.position_thresholds["unrealized_loss_warning"]),
                    status=status
                ))
                
        except Exception as e:
            self.logger.error(f"Error checking position health: {e}")
            alerts.append(PortfolioHealthAlert(
                alert_type="position_check_error",
                severity="critical",
                message="Failed to check position health",
                details={"error": str(e)}
            ))
        
        return {
            "metrics": metrics,
            "alerts": alerts,
            "timestamp": current_time
        }

    async def _check_order_health(self) -> dict[str, Any]:
        """Check health of portfolio orders."""
        metrics = []
        alerts = []
        current_time = datetime.now(UTC)
        
        try:
            # This would integrate with order management system
            # For now, create placeholder health checks
            
            # Example: Check for stale orders
            # orders = await self.portfolio_manager.get_orders()  # If this method exists
            
            # Placeholder metric
            metrics.append(PortfolioHealthMetric(
                metric_name="order_health_placeholder",
                current_value=1.0,
                threshold_value=1.0,
                status=HealthStatus.HEALTHY
            ))
                
        except Exception as e:
            self.logger.error(f"Error checking order health: {e}")
            alerts.append(PortfolioHealthAlert(
                alert_type="order_check_error",
                severity="critical",
                message="Failed to check order health",
                details={"error": str(e)}
            ))
        
        return {
            "metrics": metrics,
            "alerts": alerts,
            "timestamp": current_time
        }

    def _calculate_health_score(self, metrics: list[PortfolioHealthMetric]) -> float:
        """Calculate overall health score from metrics."""
        if not metrics:
            return 0.0
        
        # Score based on status distribution
        status_weights = {
            HealthStatus.HEALTHY: 1.0,
            HealthStatus.WARNING: 0.6,
            HealthStatus.ERROR: 0.2,
            HealthStatus.UNKNOWN: 0.5
        }
        
        total_score = sum(status_weights.get(metric.status, 0.5) for metric in metrics)
        return total_score / len(metrics)

    def _determine_overall_status(
        self, 
        metrics: list[PortfolioHealthMetric], 
        alerts: list[PortfolioHealthAlert]
    ) -> HealthStatus:
        """Determine overall portfolio health status."""
        # Check for critical alerts
        critical_alerts = [a for a in alerts if a.severity == "critical"]
        if critical_alerts:
            return HealthStatus.ERROR
        
        # Check metric statuses
        metric_statuses = [metric.status for metric in metrics]
        
        if HealthStatus.ERROR in metric_statuses:
            return HealthStatus.ERROR
        if HealthStatus.WARNING in metric_statuses:
            return HealthStatus.WARNING
        return HealthStatus.HEALTHY