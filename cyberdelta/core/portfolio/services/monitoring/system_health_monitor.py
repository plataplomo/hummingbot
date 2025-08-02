"""System health monitoring service for infrastructure and performance metrics."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psutil
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.models import HealthStatus
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


@dataclass
class SystemHealthMetric:
    """System-level health metric."""
    metric_name: str
    current_value: float
    threshold_value: float | None = None
    unit: str = ""
    status: HealthStatus = HealthStatus.HEALTHY
    timestamp: datetime | None = None
    
    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now(UTC)


@dataclass
class SystemHealthAlert:
    """System-level health alert."""
    alert_type: str
    severity: str
    message: str
    metric_name: str
    current_value: float
    threshold_value: float
    timestamp: datetime | None = None
    
    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now(UTC)


class SystemHealthMonitor(BasePortfolioService):
    """Monitors system-level health metrics like CPU, memory, disk usage."""

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__("system_health_monitor", config)
        # Access the thresholds from the provided config dict for backwards compatibility
        config_dict = config or {}
        self.logger = get_logger(__name__)
        
        # System thresholds
        self.thresholds = {
            "cpu_usage_warning": config_dict.get("cpu_warning", 80.0),
            "cpu_usage_critical": config_dict.get("cpu_critical", 95.0),
            "memory_usage_warning": config_dict.get("memory_warning", 85.0),
            "memory_usage_critical": config_dict.get("memory_critical", 95.0),
            "disk_usage_warning": config_dict.get("disk_warning", 80.0),
            "disk_usage_critical": config_dict.get("disk_critical", 90.0),
            "response_time_warning": config_dict.get("response_time_warning", 1000.0),  # ms
            "response_time_critical": config_dict.get("response_time_critical", 5000.0)  # ms
        }
        
        # Performance tracking
        self.operation_times: list[float] = []
        self.operation_successes: list[bool] = []
        self.max_operation_history = 1000

    async def _initialize_service(self) -> None:
        """Initialize system health monitor."""
        self.logger.info("Initializing system health monitor")

    async def _shutdown_service(self) -> None:
        """Shutdown system health monitor."""
        self.logger.info("Shutting down system health monitor")

    async def check_system_health(self) -> dict[str, Any]:
        """Check comprehensive system health."""
        current_time = datetime.now(UTC)
        
        # Collect system metrics
        metrics = []
        alerts = []
        
        # CPU metrics
        cpu_metrics, cpu_alerts = self._check_cpu_health()
        metrics.extend(cpu_metrics)
        alerts.extend(cpu_alerts)
        
        # Memory metrics
        memory_metrics, memory_alerts = self._check_memory_health()
        metrics.extend(memory_metrics)
        alerts.extend(memory_alerts)
        
        # Disk metrics
        disk_metrics, disk_alerts = self._check_disk_health()
        metrics.extend(disk_metrics)
        alerts.extend(disk_alerts)
        
        # Performance metrics
        perf_metrics, perf_alerts = self._check_performance_health()
        metrics.extend(perf_metrics)
        alerts.extend(perf_alerts)
        
        # Calculate overall system health
        overall_status = self._determine_system_status(metrics, alerts)
        health_score = self._calculate_system_score(metrics)
        
        return {
            "overall_status": overall_status.value,
            "health_score": health_score,
            "metrics": [self._metric_to_dict(m) for m in metrics],
            "alerts": [self._alert_to_dict(a) for a in alerts],
            "total_metrics": len(metrics),
            "total_alerts": len(alerts),
            "timestamp": current_time
        }

    def _check_cpu_health(self) -> tuple[list[SystemHealthMetric], list[SystemHealthAlert]]:
        """Check CPU health metrics."""
        metrics = []
        alerts = []
        
        try:
            # Get CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Determine status
            status = HealthStatus.HEALTHY
            if cpu_percent >= self.thresholds["cpu_usage_critical"]:
                status = HealthStatus.ERROR
                alerts.append(SystemHealthAlert(
                    alert_type="high_cpu_usage",
                    severity="critical",
                    message=f"Critical CPU usage: {cpu_percent:.1f}%",
                    metric_name="cpu_usage",
                    current_value=cpu_percent,
                    threshold_value=self.thresholds["cpu_usage_critical"]
                ))
            elif cpu_percent >= self.thresholds["cpu_usage_warning"]:
                status = HealthStatus.WARNING
                alerts.append(SystemHealthAlert(
                    alert_type="high_cpu_usage",
                    severity="warning", 
                    message=f"High CPU usage: {cpu_percent:.1f}%",
                    metric_name="cpu_usage",
                    current_value=cpu_percent,
                    threshold_value=self.thresholds["cpu_usage_warning"]
                ))
            
            metrics.append(SystemHealthMetric(
                metric_name="cpu_usage",
                current_value=cpu_percent,
                threshold_value=self.thresholds["cpu_usage_warning"],
                unit="%",
                status=status
            ))
            
            # CPU count metric
            cpu_count = psutil.cpu_count()
            if cpu_count is not None:
                metrics.append(SystemHealthMetric(
                    metric_name="cpu_count",
                    current_value=float(cpu_count),
                    unit="cores",
                    status=HealthStatus.HEALTHY
                ))
            
        except Exception as e:
            self.logger.error(f"Error checking CPU health: {e}")
            alerts.append(SystemHealthAlert(
                alert_type="cpu_check_error",
                severity="critical",
                message="Failed to check CPU health",
                metric_name="cpu_usage",
                current_value=0.0,
                threshold_value=0.0
            ))
        
        return metrics, alerts

    def _check_memory_health(self) -> tuple[list[SystemHealthMetric], list[SystemHealthAlert]]:
        """Check memory health metrics."""
        metrics = []
        alerts = []
        
        try:
            # Get memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Determine status
            status = HealthStatus.HEALTHY
            if memory_percent >= self.thresholds["memory_usage_critical"]:
                status = HealthStatus.ERROR
                alerts.append(SystemHealthAlert(
                    alert_type="high_memory_usage",
                    severity="critical",
                    message=f"Critical memory usage: {memory_percent:.1f}%",
                    metric_name="memory_usage",
                    current_value=memory_percent,
                    threshold_value=self.thresholds["memory_usage_critical"]
                ))
            elif memory_percent >= self.thresholds["memory_usage_warning"]:
                status = HealthStatus.WARNING
                alerts.append(SystemHealthAlert(
                    alert_type="high_memory_usage",
                    severity="warning",
                    message=f"High memory usage: {memory_percent:.1f}%", 
                    metric_name="memory_usage",
                    current_value=memory_percent,
                    threshold_value=self.thresholds["memory_usage_warning"]
                ))
            
            metrics.extend([
                SystemHealthMetric(
                    metric_name="memory_usage",
                    current_value=memory_percent,
                    threshold_value=self.thresholds["memory_usage_warning"],
                    unit="%",
                    status=status
                ),
                SystemHealthMetric(
                    metric_name="memory_total",
                    current_value=float(memory.total / (1024**3)),  # GB
                    unit="GB",
                    status=HealthStatus.HEALTHY
                ),
                SystemHealthMetric(
                    metric_name="memory_available",
                    current_value=float(memory.available / (1024**3)),  # GB
                    unit="GB",
                    status=HealthStatus.HEALTHY
                )
            ])
            
        except Exception as e:
            self.logger.error(f"Error checking memory health: {e}")
            alerts.append(SystemHealthAlert(
                alert_type="memory_check_error",
                severity="critical",
                message="Failed to check memory health",
                metric_name="memory_usage",
                current_value=0.0,
                threshold_value=0.0
            ))
        
        return metrics, alerts

    def _check_disk_health(self) -> tuple[list[SystemHealthMetric], list[SystemHealthAlert]]:
        """Check disk health metrics."""
        metrics = []
        alerts = []
        
        try:
            # Get disk usage for root partition
            disk = psutil.disk_usage("/")
            disk_percent = (disk.used / disk.total) * 100
            
            # Determine status
            status = HealthStatus.HEALTHY
            if disk_percent >= self.thresholds["disk_usage_critical"]:
                status = HealthStatus.ERROR
                alerts.append(SystemHealthAlert(
                    alert_type="high_disk_usage",
                    severity="critical",
                    message=f"Critical disk usage: {disk_percent:.1f}%",
                    metric_name="disk_usage",
                    current_value=disk_percent,
                    threshold_value=self.thresholds["disk_usage_critical"]
                ))
            elif disk_percent >= self.thresholds["disk_usage_warning"]:
                status = HealthStatus.WARNING
                alerts.append(SystemHealthAlert(
                    alert_type="high_disk_usage",
                    severity="warning",
                    message=f"High disk usage: {disk_percent:.1f}%",
                    metric_name="disk_usage", 
                    current_value=disk_percent,
                    threshold_value=self.thresholds["disk_usage_warning"]
                ))
            
            metrics.extend([
                SystemHealthMetric(
                    metric_name="disk_usage",
                    current_value=disk_percent,
                    threshold_value=self.thresholds["disk_usage_warning"],
                    unit="%",
                    status=status
                ),
                SystemHealthMetric(
                    metric_name="disk_total",
                    current_value=float(disk.total / (1024**3)),  # GB
                    unit="GB",
                    status=HealthStatus.HEALTHY
                ),
                SystemHealthMetric(
                    metric_name="disk_free",
                    current_value=float(disk.free / (1024**3)),  # GB
                    unit="GB",
                    status=HealthStatus.HEALTHY
                )
            ])
            
        except Exception as e:
            self.logger.error(f"Error checking disk health: {e}")
            alerts.append(SystemHealthAlert(
                alert_type="disk_check_error",
                severity="critical",
                message="Failed to check disk health",
                metric_name="disk_usage",
                current_value=0.0,
                threshold_value=0.0
            ))
        
        return metrics, alerts

    def _check_performance_health(self) -> tuple[list[SystemHealthMetric], list[SystemHealthAlert]]:
        """Check application performance health."""
        metrics = []
        alerts = []
        
        try:
            # Calculate average response time
            if self.operation_times:
                avg_response_time = sum(self.operation_times) / len(self.operation_times)
                
                # Determine status
                status = HealthStatus.HEALTHY
                if avg_response_time >= self.thresholds["response_time_critical"]:
                    status = HealthStatus.ERROR
                    alerts.append(SystemHealthAlert(
                        alert_type="slow_response_time",
                        severity="critical",
                        message=f"Critical response time: {avg_response_time:.1f}ms",
                        metric_name="avg_response_time",
                        current_value=avg_response_time,
                        threshold_value=self.thresholds["response_time_critical"]
                    ))
                elif avg_response_time >= self.thresholds["response_time_warning"]:
                    status = HealthStatus.WARNING
                    alerts.append(SystemHealthAlert(
                        alert_type="slow_response_time",
                        severity="warning",
                        message=f"Slow response time: {avg_response_time:.1f}ms",
                        metric_name="avg_response_time",
                        current_value=avg_response_time,
                        threshold_value=self.thresholds["response_time_warning"]
                    ))
                
                metrics.append(SystemHealthMetric(
                    metric_name="avg_response_time",
                    current_value=avg_response_time,
                    threshold_value=self.thresholds["response_time_warning"],
                    unit="ms",
                    status=status
                ))
            
            # Calculate success rate
            if self.operation_successes:
                success_rate = sum(self.operation_successes) / len(self.operation_successes) * 100
                
                status = HealthStatus.HEALTHY
                if success_rate < 90:
                    status = HealthStatus.WARNING
                    alerts.append(SystemHealthAlert(
                        alert_type="low_success_rate",
                        severity="warning",
                        message=f"Low success rate: {success_rate:.1f}%",
                        metric_name="operation_success_rate",
                        current_value=success_rate,
                        threshold_value=90.0
                    ))
                
                metrics.append(SystemHealthMetric(
                    metric_name="operation_success_rate",
                    current_value=success_rate,
                    threshold_value=90.0,
                    unit="%",
                    status=status
                ))
            
        except Exception as e:
            self.logger.error(f"Error checking performance health: {e}")
            alerts.append(SystemHealthAlert(
                alert_type="performance_check_error",
                severity="critical",
                message="Failed to check performance health",
                metric_name="performance",
                current_value=0.0,
                threshold_value=0.0
            ))
        
        return metrics, alerts

    def record_operation(self, success: bool, response_time: float = 0.0) -> None:
        """Record operation for performance tracking."""
        self.operation_successes.append(success)
        if response_time > 0:
            self.operation_times.append(response_time)
        
        # Keep only recent history
        if len(self.operation_successes) > self.max_operation_history:
            self.operation_successes = self.operation_successes[-self.max_operation_history:]
        
        if len(self.operation_times) > self.max_operation_history:
            self.operation_times = self.operation_times[-self.max_operation_history:]

    def _determine_system_status(
        self, 
        metrics: list[SystemHealthMetric], 
        alerts: list[SystemHealthAlert]
    ) -> HealthStatus:
        """Determine overall system status."""
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
        if HealthStatus.WARNING in metric_statuses:
            return HealthStatus.WARNING
        return HealthStatus.HEALTHY

    def _calculate_system_score(self, metrics: list[SystemHealthMetric]) -> float:
        """Calculate overall system health score."""
        if not metrics:
            return 0.0
        
        status_weights = {
            HealthStatus.HEALTHY: 1.0,
            HealthStatus.WARNING: 0.8,
            HealthStatus.WARNING: 0.6,
            HealthStatus.ERROR: 0.2,
            HealthStatus.UNKNOWN: 0.5
        }
        
        total_score = sum(status_weights.get(metric.status, 0.5) for metric in metrics)
        return total_score / len(metrics)

    def _metric_to_dict(self, metric: SystemHealthMetric) -> dict[str, Any]:
        """Convert metric to dictionary."""
        return {
            "metric_name": metric.metric_name,
            "current_value": metric.current_value,
            "threshold_value": metric.threshold_value,
            "unit": metric.unit,
            "status": metric.status.value,
            "timestamp": metric.timestamp.isoformat() if metric.timestamp else None
        }

    def _alert_to_dict(self, alert: SystemHealthAlert) -> dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "alert_type": alert.alert_type,
            "severity": alert.severity,
            "message": alert.message,
            "metric_name": alert.metric_name,
            "current_value": alert.current_value,
            "threshold_value": alert.threshold_value,
            "timestamp": alert.timestamp.isoformat() if alert.timestamp else None
        }