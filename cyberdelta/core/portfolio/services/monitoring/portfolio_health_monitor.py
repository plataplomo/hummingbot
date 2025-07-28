"""Portfolio health monitoring service for system health tracking."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

import psutil
from pydantic import BaseModel, ConfigDict, Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


class HealthMetricMetadata(BaseModel):
    """Typed metadata for health metrics."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Metric source information
    source_service: str | None = None
    collection_method: str | None = None
    aggregation_period: str | None = None

    # Threshold information
    baseline_value: float | None = None
    previous_value: float | None = None
    trend_direction: str | None = None  # "up", "down", "stable"

    # Additional context
    related_metrics: list[str] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)


class HealthAlertMetadata(BaseModel):
    """Typed metadata for health alerts."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Alert tracking
    trigger_condition: str | None = None
    escalation_level: int | None = Field(default=None, ge=0, le=5)
    auto_resolve: bool = Field(default=False)

    # Context information
    related_alerts: list[str] = Field(default_factory=list)
    impact_assessment: str | None = None
    recommended_action: str | None = None

    # Timing information
    first_occurrence: datetime | None = None
    last_occurrence: datetime | None = None
    occurrence_count: int = Field(default=1, ge=1)


class HealthReportMetadata(BaseModel):
    """Typed metadata for health reports."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Report generation
    generation_time_ms: float | None = Field(default=None, ge=0)
    report_version: str | None = None
    included_components: list[str] = Field(default_factory=list)

    # Aggregation information
    metrics_count: int = Field(default=0, ge=0)
    alerts_count: int = Field(default=0, ge=0)
    data_freshness: str | None = None  # "real-time", "cached", "stale"

    # Context
    trigger_reason: str | None = None  # "scheduled", "manual", "threshold"
    requested_by: str | None = None


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition, Order, SpotBalance

logger = get_logger(__name__)

# Constants
STALE_ORDER_THRESHOLD_SECONDS = 3600  # 1 hour
MAX_ALERTS_THRESHOLD = 10


class HealthStatus(Enum):
    """Portfolio health status levels."""

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class HealthMetric:
    """Individual health metric with value and status."""

    name: str
    value: float
    status: HealthStatus
    threshold_warning: float | None = None
    threshold_critical: float | None = None
    unit: str = ""
    description: str = ""
    metadata: HealthMetricMetadata = Field(default_factory=HealthMetricMetadata)
    measured_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


@dataclass
class HealthAlert:
    """Health alert for system issues."""

    alert_id: str
    severity: HealthStatus
    title: str
    description: str
    component: str
    metric_name: str
    metric_value: float
    threshold_value: float
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    acknowledged: bool = False
    resolved: bool = False
    metadata: HealthAlertMetadata = Field(default_factory=HealthAlertMetadata)


@dataclass
class HealthReport:
    """Comprehensive health report."""

    overall_status: HealthStatus
    health_score: float
    metrics: list[HealthMetric]
    alerts: list[HealthAlert]
    recommendations: list[str]
    report_timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: HealthReportMetadata = Field(default_factory=HealthReportMetadata)


class PortfolioHealthMonitor(BasePortfolioService):
    """Service for monitoring portfolio system health and performance.

    This service provides:
    - Real-time health monitoring
    - Performance metrics tracking
    - Alert generation and management
    - Health score calculation
    - Trend analysis
    """

    def __init__(
        self, name: str = "PortfolioHealthMonitor", config: dict[str, Any] | None = None
    ) -> None:
        """Initialize the health monitor.

        Args:
            name: Service name
            config: Configuration dictionary
        """
        cfg = config or {}
        super().__init__(name, config)

        # Health monitoring configuration
        self.monitoring_interval = cfg.get("monitoring_interval", 60)  # seconds
        self.health_history_size = cfg.get("health_history_size", 1000)
        self.alert_retention_days = cfg.get("alert_retention_days", 30)

        # Health thresholds
        self.thresholds = {
            "balance_inconsistency": {
                "warning": 0.01,
                "critical": 0.1,
            },
            "position_risk": {
                "warning": 0.05,
                "critical": 0.1,
            },
            "order_fill_rate": {
                "warning": 0.8,
                "critical": 0.6,
            },
            "system_response_time": {
                "warning": 1.0,
                "critical": 5.0,
            },
            "memory_usage": {
                "warning": 0.8,
                "critical": 0.95,
            },
            "error_rate": {
                "warning": 0.01,
                "critical": 0.05,
            },
        }

        # Health history
        self.health_history: list[HealthReport] = []
        self.active_alerts: list[HealthAlert] = []

        # Performance tracking
        self.performance_metrics: dict[str, int | float | datetime] = {
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "avg_response_time": 0.0,
            "last_update": datetime.now(UTC),
        }

        # Component health tracking
        self.component_health = {
            "balance_manager": HealthStatus.UNKNOWN,
            "position_manager": HealthStatus.UNKNOWN,
            "order_manager": HealthStatus.UNKNOWN,
            "pnl_calculator": HealthStatus.UNKNOWN,
            "risk_calculator": HealthStatus.UNKNOWN,
            "data_screeners": HealthStatus.UNKNOWN,
            "event_dispatcher": HealthStatus.UNKNOWN,
            "persistence_service": HealthStatus.UNKNOWN,
        }

        logger.info(
            "portfolio_health_monitor_created",
            service_name=name,
            monitoring_interval=self.monitoring_interval,
            health_history_size=self.health_history_size,
        )

    async def _initialize_service(self) -> None:
        """Initialize the health monitor."""
        logger.info("portfolio_health_monitor_initializing")

        # Initialize component health checks
        await self._initialize_component_monitoring()

    async def _shutdown_service(self) -> None:
        """Shutdown the health monitor."""
        logger.info("portfolio_health_monitor_shutting_down")

        # Save final health report
        await self._save_final_health_report()

    async def _initialize_component_monitoring(self) -> None:
        """Initialize monitoring for all components."""
        for component in self.component_health:
            self.component_health[component] = HealthStatus.HEALTHY

    async def _save_final_health_report(self) -> None:
        """Save final health report on shutdown."""
        if self.health_history:
            final_report = self.health_history[-1]
            logger.info(
                "final_health_report",
                overall_status=final_report.overall_status.value,
                health_score=final_report.health_score,
                total_alerts=len(final_report.alerts),
            )

    async def check_portfolio_health(
        self,
        balances: dict[str, dict[str, SpotBalance]],
        positions: dict[str, dict[str, DerivativePosition]],
        orders: dict[str, dict[str, Order]],
    ) -> HealthReport:
        """Perform comprehensive health check.

        Args:
            balances: Portfolio balances
            positions: Portfolio positions
            orders: Portfolio orders

        Returns:
            HealthReport with current health status

        Raises:
            RuntimeError: If the health monitor is not running
        """
        if not self.is_running:
            raise RuntimeError

        metrics: list[HealthMetric] = []
        alerts: list[HealthAlert] = []

        # Check balance health
        balance_metrics, balance_alerts = await self._check_balance_health(balances)
        metrics.extend(balance_metrics)
        alerts.extend(balance_alerts)

        # Check position health
        position_metrics, position_alerts = await self._check_position_health(positions)
        metrics.extend(position_metrics)
        alerts.extend(position_alerts)

        # Check order health
        order_metrics, order_alerts = await self._check_order_health(orders)
        metrics.extend(order_metrics)
        alerts.extend(order_alerts)

        # Check system health
        system_metrics, system_alerts = await self._check_system_health()
        metrics.extend(system_metrics)
        alerts.extend(system_alerts)

        # Check component health
        component_metrics, component_alerts = await self._check_component_health()
        metrics.extend(component_metrics)
        alerts.extend(component_alerts)

        # Calculate overall health score
        health_score = self._calculate_health_score(metrics)
        overall_status = self._determine_overall_status(metrics, alerts)

        # Generate recommendations
        recommendations = self._generate_recommendations(metrics, alerts)

        # Create health report
        report = HealthReport(
            overall_status=overall_status,
            health_score=health_score,
            metrics=metrics,
            alerts=alerts,
            recommendations=recommendations,
        )

        # Update health history
        self.health_history.append(report)
        if len(self.health_history) > self.health_history_size:
            self.health_history.pop(0)

        # Update active alerts
        self.active_alerts.extend(alerts)
        self._cleanup_old_alerts()

        logger.info(
            "portfolio_health_check_completed",
            overall_status=overall_status.value,
            health_score=health_score,
            total_metrics=len(metrics),
            total_alerts=len(alerts),
        )

        return report

    async def _check_balance_health(
        self, balances: dict[str, dict[str, SpotBalance]]
    ) -> tuple[list[HealthMetric], list[HealthAlert]]:
        """Check balance-related health metrics.

        Args:
            balances: Portfolio balances

        Returns:
            Tuple of (metrics, alerts)
        """
        metrics: list[HealthMetric] = []
        alerts: list[HealthAlert] = []

        try:
            total_balance_value = Decimal(0)
            inconsistent_balances = 0
            total_balances = 0

            for exchange_balances in balances.values():
                for balance in exchange_balances.values():
                    total_balances += 1

                    # Check balance consistency
                    # SpotBalance model doesn't have locked field - check extension details
                    locked = Decimal(0)
                    if balance.bp_details:
                        locked += balance.bp_details.open_order_quantity or Decimal(0)
                        locked += balance.bp_details.lend_quantity or Decimal(0)
                    expected_total = balance.available_quantity + locked
                    if abs(balance.total_quantity - expected_total) > Decimal("0.001"):
                        inconsistent_balances += 1

                    # Add to total value (assuming USD equivalent)
                    total_balance_value += balance.total_quantity

            # Balance inconsistency ratio
            inconsistency_ratio = inconsistent_balances / max(total_balances, 1)
            inconsistency_status = self._get_status_from_thresholds(
                inconsistency_ratio, "balance_inconsistency"
            )

            metrics.append(
                HealthMetric(
                    name="balance_inconsistency_ratio",
                    value=inconsistency_ratio,
                    status=inconsistency_status,
                    threshold_warning=self.thresholds["balance_inconsistency"]["warning"],
                    threshold_critical=self.thresholds["balance_inconsistency"]["critical"],
                    unit="ratio",
                    description="Ratio of inconsistent balances to total balances",
                    metadata=HealthMetricMetadata(
                        tags={
                            "inconsistent_balances": str(inconsistent_balances),
                            "total_balances": str(total_balances),
                        }
                    ),
                )
            )

            # Create alert if threshold exceeded
            if inconsistency_status in {HealthStatus.WARNING, HealthStatus.CRITICAL}:
                alerts.append(
                    HealthAlert(
                        alert_id=f"balance_inconsistency_{datetime.now(UTC).timestamp()}",
                        severity=inconsistency_status,
                        title="Balance Inconsistency Detected",
                        description=(
                            f"Found {inconsistent_balances} inconsistent balances "
                            f"out of {total_balances}"
                        ),
                        component="balance_manager",
                        metric_name="balance_inconsistency_ratio",
                        metric_value=inconsistency_ratio,
                        threshold_value=self.thresholds["balance_inconsistency"]["warning"],
                    )
                )

            # Total balance value
            metrics.append(
                HealthMetric(
                    name="total_balance_value",
                    value=float(total_balance_value),
                    status=HealthStatus.HEALTHY,
                    unit="USD",
                    description="Total value of all balances",
                    metadata=HealthMetricMetadata(
                        tags={
                            "total_exchanges": str(len(balances)),
                            "total_currencies": str(
                                sum(len(ex_balances) for ex_balances in balances.values())
                            ),
                        }
                    ),
                )
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.exception("balance_health_check_failed")

            metrics.append(
                HealthMetric(
                    name="balance_health_check_error",
                    value=1.0,
                    status=HealthStatus.CRITICAL,
                    description=f"Balance health check failed: {e!s}",
                )
            )

        return metrics, alerts

    async def _check_position_health(
        self, positions: dict[str, dict[str, DerivativePosition]]
    ) -> tuple[list[HealthMetric], list[HealthAlert]]:
        """Check position-related health metrics.

        Args:
            positions: Portfolio positions

        Returns:
            Tuple of (metrics, alerts)
        """
        metrics: list[HealthMetric] = []
        alerts: list[HealthAlert] = []

        try:
            total_position_value = Decimal(0)
            high_risk_positions = 0
            total_positions = 0

            for exchange_positions in positions.values():
                for position in exchange_positions.values():
                    if position.size == 0:
                        continue

                    total_positions += 1

                    # Calculate position value
                    position_value = Decimal(0)
                    if position.entry_price is not None:
                        position_value = abs(position.size) * position.entry_price
                        total_position_value += position_value

                    # Check for high-risk positions (simplified risk check)
                    if position_value > Decimal(100000):  # $100k position
                        high_risk_positions += 1

            # Position risk ratio
            if total_positions > 0:
                risk_ratio = high_risk_positions / total_positions
                risk_status = self._get_status_from_thresholds(risk_ratio, "position_risk")

                metrics.append(
                    HealthMetric(
                        name="position_risk_ratio",
                        value=risk_ratio,
                        status=risk_status,
                        threshold_warning=self.thresholds["position_risk"]["warning"],
                        threshold_critical=self.thresholds["position_risk"]["critical"],
                        unit="ratio",
                        description="Ratio of high-risk positions to total positions",
                        metadata=HealthMetricMetadata(
                            tags={
                                "high_risk_positions": str(high_risk_positions),
                                "total_positions": str(total_positions),
                            }
                        ),
                    )
                )

                # Create alert if threshold exceeded
                if risk_status in {HealthStatus.WARNING, HealthStatus.CRITICAL}:
                    alerts.append(
                        HealthAlert(
                            alert_id=f"position_risk_{datetime.now(UTC).timestamp()}",
                            severity=risk_status,
                            title="High Position Risk Detected",
                            description=(
                                f"Found {high_risk_positions} high-risk positions "
                                f"out of {total_positions}"
                            ),
                            component="position_manager",
                            metric_name="position_risk_ratio",
                            metric_value=risk_ratio,
                            threshold_value=self.thresholds["position_risk"]["warning"],
                        )
                    )

            # Total position value
            metrics.append(
                HealthMetric(
                    name="total_position_value",
                    value=float(total_position_value),
                    status=HealthStatus.HEALTHY,
                    unit="USD",
                    description="Total value of all positions",
                    metadata=HealthMetricMetadata(
                        tags={
                            "total_exchanges": str(len(positions)),
                            "total_positions": str(total_positions),
                        }
                    ),
                )
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.exception("position_health_check_failed")

            metrics.append(
                HealthMetric(
                    name="position_health_check_error",
                    value=1.0,
                    status=HealthStatus.CRITICAL,
                    description=f"Position health check failed: {e!s}",
                )
            )

        return metrics, alerts

    async def _check_order_health(
        self, orders: dict[str, dict[str, Order]]
    ) -> tuple[list[HealthMetric], list[HealthAlert]]:
        """Check order-related health metrics.

        Args:
            orders: Portfolio orders

        Returns:
            Tuple of (metrics, alerts)
        """
        metrics: list[HealthMetric] = []
        alerts: list[HealthAlert] = []

        try:
            total_orders = 0
            filled_orders = 0
            stale_orders = 0

            current_time = datetime.now(UTC)

            for exchange_orders in orders.values():
                for order in exchange_orders.values():
                    total_orders += 1

                    # Check order status
                    if order.status == OrderStatus.FILLED:
                        filled_orders += 1

                    # Check for stale orders (Order model has created_at field)
                    order_created_at = order.created_at
                    if order_created_at:
                        # Order model guarantees created_at is datetime object
                        order_age = (current_time - order_created_at).total_seconds()
                        if order_age > STALE_ORDER_THRESHOLD_SECONDS and order.status in {
                            OrderStatus.NEW,
                            OrderStatus.PARTIALLY_FILLED,
                        }:  # 1 hour
                            stale_orders += 1

            # Order fill rate
            if total_orders > 0:
                fill_rate = filled_orders / total_orders
                fill_status = self._get_status_from_thresholds(
                    fill_rate, "order_fill_rate", reverse=True
                )

                metrics.append(
                    HealthMetric(
                        name="order_fill_rate",
                        value=fill_rate,
                        status=fill_status,
                        threshold_warning=self.thresholds["order_fill_rate"]["warning"],
                        threshold_critical=self.thresholds["order_fill_rate"]["critical"],
                        unit="ratio",
                        description="Ratio of filled orders to total orders",
                        metadata=HealthMetricMetadata(
                            tags={
                                "filled_orders": str(filled_orders),
                                "total_orders": str(total_orders),
                            }
                        ),
                    )
                )

                # Create alert if threshold exceeded
                if fill_status in {HealthStatus.WARNING, HealthStatus.CRITICAL}:
                    alerts.append(
                        HealthAlert(
                            alert_id=f"order_fill_rate_{datetime.now(UTC).timestamp()}",
                            severity=fill_status,
                            title="Low Order Fill Rate",
                            description=f"Order fill rate is {fill_rate:.2%}",
                            component="order_manager",
                            metric_name="order_fill_rate",
                            metric_value=fill_rate,
                            threshold_value=self.thresholds["order_fill_rate"]["warning"],
                        )
                    )

            # Stale orders
            metrics.append(
                HealthMetric(
                    name="stale_orders",
                    value=stale_orders,
                    status=HealthStatus.WARNING if stale_orders > 0 else HealthStatus.HEALTHY,
                    unit="count",
                    description="Number of stale orders (>1 hour old)",
                    metadata=HealthMetricMetadata(
                        tags={
                            "stale_orders": str(stale_orders),
                            "total_orders": str(total_orders),
                        }
                    ),
                )
            )

            if stale_orders > 0:
                alerts.append(
                    HealthAlert(
                        alert_id=f"stale_orders_{datetime.now(UTC).timestamp()}",
                        severity=HealthStatus.WARNING,
                        title="Stale Orders Detected",
                        description=f"Found {stale_orders} stale orders",
                        component="order_manager",
                        metric_name="stale_orders",
                        metric_value=stale_orders,
                        threshold_value=0,
                    )
                )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.exception("order_health_check_failed")

            metrics.append(
                HealthMetric(
                    name="order_health_check_error",
                    value=1.0,
                    status=HealthStatus.CRITICAL,
                    description=f"Order health check failed: {e!s}",
                )
            )

        return metrics, alerts

    async def _check_system_health(self) -> tuple[list[HealthMetric], list[HealthAlert]]:
        """Check system-level health metrics.

        Returns:
            Tuple of (metrics, alerts)
        """
        metrics: list[HealthMetric] = []
        alerts: list[HealthAlert] = []

        try:
            # Memory usage
            memory_usage = psutil.virtual_memory().percent / 100
            memory_status = self._get_status_from_thresholds(memory_usage, "memory_usage")

            metrics.append(
                HealthMetric(
                    name="memory_usage",
                    value=memory_usage,
                    status=memory_status,
                    threshold_warning=self.thresholds["memory_usage"]["warning"],
                    threshold_critical=self.thresholds["memory_usage"]["critical"],
                    unit="percent",
                    description="System memory usage percentage",
                )
            )

            if memory_status in {HealthStatus.WARNING, HealthStatus.CRITICAL}:
                alerts.append(
                    HealthAlert(
                        alert_id=f"memory_usage_{datetime.now(UTC).timestamp()}",
                        severity=memory_status,
                        title="High Memory Usage",
                        description=f"Memory usage is {memory_usage:.1%}",
                        component="system",
                        metric_name="memory_usage",
                        metric_value=memory_usage,
                        threshold_value=self.thresholds["memory_usage"]["warning"],
                    )
                )

            # Error rate
            failed_ops_value = self.performance_metrics.get("failed_operations", 0)
            total_ops_value = self.performance_metrics.get("total_operations", 0)

            if isinstance(failed_ops_value, (int, float)) and isinstance(
                total_ops_value, (int, float)
            ):
                failed_ops = int(failed_ops_value)
                total_ops = int(total_ops_value)
                error_rate = failed_ops / max(total_ops, 1)
            else:
                error_rate = 0.0
            error_status = self._get_status_from_thresholds(error_rate, "error_rate")

            metrics.append(
                HealthMetric(
                    name="error_rate",
                    value=error_rate,
                    status=error_status,
                    threshold_warning=self.thresholds["error_rate"]["warning"],
                    threshold_critical=self.thresholds["error_rate"]["critical"],
                    unit="ratio",
                    description="System error rate",
                    metadata=HealthMetricMetadata(
                        tags={
                            k: str(v)
                            for k, v in self.performance_metrics.items()
                            if isinstance(v, (str, int, float))
                        }
                    ),
                )
            )

            if error_status in {HealthStatus.WARNING, HealthStatus.CRITICAL}:
                alerts.append(
                    HealthAlert(
                        alert_id=f"error_rate_{datetime.now(UTC).timestamp()}",
                        severity=error_status,
                        title="High Error Rate",
                        description=f"Error rate is {error_rate:.2%}",
                        component="system",
                        metric_name="error_rate",
                        metric_value=error_rate,
                        threshold_value=self.thresholds["error_rate"]["warning"],
                    )
                )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.exception("system_health_check_failed")

            metrics.append(
                HealthMetric(
                    name="system_health_check_error",
                    value=1.0,
                    status=HealthStatus.CRITICAL,
                    description=f"System health check failed: {e!s}",
                )
            )

        return metrics, alerts

    async def _check_component_health(self) -> tuple[list[HealthMetric], list[HealthAlert]]:
        """Check health of individual components.

        Returns:
            Tuple of (metrics, alerts)
        """
        metrics: list[HealthMetric] = []
        alerts: list[HealthAlert] = []

        for component_name, health_status in self.component_health.items():
            status_value = {
                HealthStatus.HEALTHY: 1.0,
                HealthStatus.WARNING: 0.5,
                HealthStatus.CRITICAL: 0.0,
                HealthStatus.UNKNOWN: 0.25,
            }[health_status]

            metrics.append(
                HealthMetric(
                    name=f"{component_name}_health",
                    value=status_value,
                    status=health_status,
                    unit="health_score",
                    description=f"Health status of {component_name}",
                )
            )

            if health_status in {HealthStatus.WARNING, HealthStatus.CRITICAL}:
                alerts.append(
                    HealthAlert(
                        alert_id=f"{component_name}_health_{datetime.now(UTC).timestamp()}",
                        severity=health_status,
                        title=f"Component Health Issue: {component_name}",
                        description=f"Component {component_name} is in {health_status.value} state",
                        component=component_name,
                        metric_name=f"{component_name}_health",
                        metric_value=status_value,
                        threshold_value=0.5,
                    )
                )

        return metrics, alerts

    def _get_status_from_thresholds(
        self, value: float, threshold_key: str, reverse: bool = False
    ) -> HealthStatus:
        """Get health status based on thresholds.

        Args:
            value: Metric value
            threshold_key: Threshold configuration key
            reverse: If True, lower values are worse

        Returns:
            Health status
        """
        if threshold_key not in self.thresholds:
            return HealthStatus.UNKNOWN

        warning_threshold = self.thresholds[threshold_key]["warning"]
        critical_threshold = self.thresholds[threshold_key]["critical"]

        if reverse:
            if value < critical_threshold:
                return HealthStatus.CRITICAL
            if value < warning_threshold:
                return HealthStatus.WARNING
            return HealthStatus.HEALTHY
        if value > critical_threshold:
            return HealthStatus.CRITICAL
        if value > warning_threshold:
            return HealthStatus.WARNING
        return HealthStatus.HEALTHY

    def _calculate_health_score(self, metrics: list[HealthMetric]) -> float:
        """Calculate overall health score from metrics.

        Args:
            metrics: List of health metrics

        Returns:
            Health score (0-100)
        """
        if not metrics:
            return 0.0

        status_weights = {
            HealthStatus.HEALTHY: 1.0,
            HealthStatus.WARNING: 0.5,
            HealthStatus.CRITICAL: 0.0,
            HealthStatus.UNKNOWN: 0.25,
        }

        total_score = sum(status_weights[metric.status] for metric in metrics)
        return (total_score / len(metrics)) * 100

    def _determine_overall_status(
        self, metrics: list[HealthMetric], alerts: list[HealthAlert]
    ) -> HealthStatus:
        """Determine overall health status.

        Args:
            metrics: List of health metrics
            alerts: List of health alerts

        Returns:
            Overall health status
        """
        # Check for critical alerts
        critical_alerts = [a for a in alerts if a.severity == HealthStatus.CRITICAL]
        if critical_alerts:
            return HealthStatus.CRITICAL

        # Check for warning alerts
        warning_alerts = [a for a in alerts if a.severity == HealthStatus.WARNING]
        if warning_alerts:
            return HealthStatus.WARNING

        # Check metrics
        critical_metrics = [m for m in metrics if m.status == HealthStatus.CRITICAL]
        if critical_metrics:
            return HealthStatus.CRITICAL

        warning_metrics = [m for m in metrics if m.status == HealthStatus.WARNING]
        if warning_metrics:
            return HealthStatus.WARNING

        return HealthStatus.HEALTHY

    def _generate_recommendations(
        self, metrics: list[HealthMetric], alerts: list[HealthAlert]
    ) -> list[str]:
        """Generate recommendations based on health metrics and alerts.

        Args:
            metrics: List of health metrics
            alerts: List of health alerts

        Returns:
            List of recommendations
        """
        recommendations: list[str] = []

        # Check for critical issues
        critical_alerts = [a for a in alerts if a.severity == HealthStatus.CRITICAL]
        if critical_alerts:
            recommendations.append("Immediate attention required: Address critical system issues")

        # Check for specific metric issues
        for metric in metrics:
            if (
                metric.name == "balance_inconsistency_ratio"
                and metric.status != HealthStatus.HEALTHY
            ):
                recommendations.append(
                    "Review balance calculation logic and reconciliation processes"
                )

            if metric.name == "position_risk_ratio" and metric.status != HealthStatus.HEALTHY:
                recommendations.append("Review position sizing and risk management rules")

            if metric.name == "order_fill_rate" and metric.status != HealthStatus.HEALTHY:
                recommendations.append("Investigate order execution issues and market conditions")

            if metric.name == "memory_usage" and metric.status != HealthStatus.HEALTHY:
                recommendations.append("Consider memory optimization and garbage collection tuning")

            if metric.name == "error_rate" and metric.status != HealthStatus.HEALTHY:
                recommendations.append("Review error logs and improve error handling")

        # Check for stale orders
        stale_order_alerts = [a for a in alerts if a.metric_name == "stale_orders"]
        if stale_order_alerts:
            recommendations.append("Clean up stale orders and review order management processes")

        # General recommendations
        if len(alerts) > MAX_ALERTS_THRESHOLD:
            recommendations.append(
                "Consider implementing automated alert management and escalation"
            )

        return recommendations

    def _cleanup_old_alerts(self) -> None:
        """Clean up old alerts."""
        cutoff_time = datetime.now(UTC) - timedelta(days=self.alert_retention_days)
        self.active_alerts = [
            alert for alert in self.active_alerts if alert.created_at > cutoff_time
        ]

    def update_component_health(self, component: str, status: HealthStatus) -> None:
        """Update health status for a specific component.

        Args:
            component: Component name
            status: New health status
        """
        if component in self.component_health:
            old_status = self.component_health[component]
            self.component_health[component] = status

            logger.info(
                "component_health_updated",
                component=component,
                old_status=old_status.value,
                new_status=status.value,
            )

    def record_operation(self, success: bool, response_time: float = 0.0) -> None:
        """Record an operation for performance tracking.

        Args:
            success: Whether the operation was successful
            response_time: Operation response time in seconds
        """
        total_ops = self.performance_metrics.get("total_operations", 0)
        if isinstance(total_ops, (int, float)):
            self.performance_metrics["total_operations"] = int(total_ops) + 1

        if success:
            successful_ops = self.performance_metrics.get("successful_operations", 0)
            if isinstance(successful_ops, (int, float)):
                self.performance_metrics["successful_operations"] = int(successful_ops) + 1
        else:
            failed_ops = self.performance_metrics.get("failed_operations", 0)
            if isinstance(failed_ops, (int, float)):
                self.performance_metrics["failed_operations"] = int(failed_ops) + 1

        # Update average response time
        total_ops_value = self.performance_metrics.get("total_operations", 0)
        current_avg_value = self.performance_metrics.get("avg_response_time", 0.0)

        if isinstance(total_ops_value, (int, float)) and isinstance(
            current_avg_value, (int, float)
        ):
            total_ops = int(total_ops_value)
            current_avg = float(current_avg_value)
            self.performance_metrics["avg_response_time"] = (
                current_avg * (total_ops - 1) + response_time
            ) / total_ops

        self.performance_metrics["last_update"] = datetime.now(UTC)

    def get_health_summary(self) -> dict[str, Any]:
        """Get a summary of current health status.

        Returns:
            Dictionary with health summary
        """
        if not self.health_history:
            return {
                "status": "unknown",
                "message": "No health data available",
            }

        latest_report = self.health_history[-1]

        return {
            "overall_status": latest_report.overall_status.value,
            "health_score": latest_report.health_score,
            "active_alerts": len(self.active_alerts),
            "critical_alerts": len([
                a for a in self.active_alerts if a.severity == HealthStatus.CRITICAL
            ]),
            "warning_alerts": len([
                a for a in self.active_alerts if a.severity == HealthStatus.WARNING
            ]),
            "component_health": {
                component: status.value for component, status in self.component_health.items()
            },
            "performance_metrics": self.performance_metrics,
            "last_check": latest_report.report_timestamp,
        }

    async def _start_internal(self) -> None:
        """Initialize internal state (required by BasePortfolioService)."""
        await self._initialize_service()

    async def _stop_internal(self) -> None:
        """Shutdown internal state (required by BasePortfolioService)."""
        await self._shutdown_service()
