"""Health monitoring component for portfolio analytics."""
from __future__ import annotations

from datetime import datetime, timedelta, UTC
from typing import Dict, List, Any, Optional
from enum import Enum

from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)


class HealthStatus(str, Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"


class ComponentStatus(str, Enum):
    """Component operational status."""
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    STARTING = "starting"
    STOPPING = "stopping"


class HealthMonitor:
    """Monitor health of analytics system components."""
    
    def __init__(self) -> None:
        """Initialize health monitor."""
        self._initialized = False
        self._component_status: Dict[str, Dict[str, Any]] = {}
        self._health_checks: Dict[str, Any] = {}
        self._health_history: List[Dict[str, Any]] = []
        self._check_interval = 60  # seconds
        
    async def initialize(self) -> None:
        """Initialize the health monitor."""
        if self._initialized:
            return
            
        logger.info("Initializing health monitor")
        
        # Register default health checks
        self._register_default_checks()
        
        self._initialized = True
        
    async def shutdown(self) -> None:
        """Shutdown the health monitor."""
        if not self._initialized:
            return
            
        logger.info("Shutting down health monitor")
        self._initialized = False
        
    async def check_health(self) -> Dict[str, Any]:
        """Perform health check on all components.
        
        Returns:
            Overall health status and component details
        """
        timestamp = datetime.now(UTC)
        health_results = {}
        
        # Check each registered component
        for component_name, check_func in self._health_checks.items():
            try:
                result = await check_func()
                health_results[component_name] = result
                self._component_status[component_name] = {
                    "status": result.get("status", ComponentStatus.ERROR),
                    "last_check": timestamp,
                    "details": result
                }
            except Exception as e:
                logger.error(
                    "Health check failed",
                    component=component_name,
                    error=str(e)
                )
                health_results[component_name] = {
                    "status": ComponentStatus.ERROR,
                    "error": str(e)
                }
                
        # Calculate overall health
        overall_status = self._calculate_overall_health(health_results)
        
        # Create health report
        health_report = {
            "timestamp": timestamp,
            "overall_status": overall_status,
            "components": health_results,
            "metrics": await self._collect_metrics()
        }
        
        # Add to history
        self._health_history.append(health_report)
        
        # Trim history
        self._trim_history()
        
        return health_report
        
    async def get_status(self) -> Dict[str, Any]:
        """Get current health status.
        
        Returns:
            Current health status summary
        """
        # Get latest health check or perform new one
        if not self._health_history:
            return await self.check_health()
            
        latest = self._health_history[-1]
        age = (datetime.now(UTC) - latest["timestamp"]).total_seconds()
        
        # If latest check is too old, perform new one
        if age > self._check_interval * 2:
            return await self.check_health()
            
        return latest
        
    def register_health_check(self, component_name: str, check_func: Any) -> None:
        """Register a health check function for a component.
        
        Args:
            component_name: Name of component
            check_func: Async function that returns health status
        """
        self._health_checks[component_name] = check_func
        logger.info(f"Registered health check for: {component_name}")
        
    def get_component_status(self, component_name: str) -> Optional[Dict[str, Any]]:
        """Get status of specific component.
        
        Args:
            component_name: Name of component
            
        Returns:
            Component status or None if not found
        """
        return self._component_status.get(component_name)
        
    def get_health_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get health check history.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List of historical health reports
        """
        cutoff = datetime.now(UTC) - timedelta(hours=hours)
        
        return [
            h for h in self._health_history
            if h["timestamp"] > cutoff
        ]
        
    def _register_default_checks(self) -> None:
        """Register default health checks."""
        # Default checks for core components
        self.register_health_check("portfolio_manager", self._check_portfolio_manager)
        self.register_health_check("performance_calculator", self._check_performance_calculator)
        self.register_health_check("attribution_analyzer", self._check_attribution_analyzer)
        self.register_health_check("report_generator", self._check_report_generator)
        self.register_health_check("alert_manager", self._check_alert_manager)
        
    async def _check_portfolio_manager(self) -> Dict[str, Any]:
        """Check portfolio manager health."""
        # Placeholder - would check actual component
        return {
            "status": ComponentStatus.RUNNING,
            "message": "Portfolio manager operational"
        }
        
    async def _check_performance_calculator(self) -> Dict[str, Any]:
        """Check performance calculator health."""
        # Placeholder - would check actual component
        return {
            "status": ComponentStatus.RUNNING,
            "message": "Performance calculator operational",
            "last_calculation": datetime.now(UTC) - timedelta(minutes=5),
            "calculation_time_ms": 250
        }
        
    async def _check_attribution_analyzer(self) -> Dict[str, Any]:
        """Check attribution analyzer health."""
        # Placeholder - would check actual component
        return {
            "status": ComponentStatus.RUNNING,
            "message": "Attribution analyzer operational"
        }
        
    async def _check_report_generator(self) -> Dict[str, Any]:
        """Check report generator health."""
        # Placeholder - would check actual component
        return {
            "status": ComponentStatus.RUNNING,
            "message": "Report generator operational",
            "reports_generated_today": 12
        }
        
    async def _check_alert_manager(self) -> Dict[str, Any]:
        """Check alert manager health."""
        # Placeholder - would check actual component
        return {
            "status": ComponentStatus.RUNNING,
            "message": "Alert manager operational",
            "active_alerts": 2
        }
        
    def _calculate_overall_health(self, component_results: Dict[str, Any]) -> HealthStatus:
        """Calculate overall system health from component results."""
        if not component_results:
            return HealthStatus.UNHEALTHY
            
        statuses = []
        for result in component_results.values():
            status = result.get("status", ComponentStatus.ERROR)
            if status == ComponentStatus.ERROR:
                statuses.append(HealthStatus.CRITICAL)
            elif status == ComponentStatus.STOPPED:
                statuses.append(HealthStatus.UNHEALTHY)
            elif status == ComponentStatus.STARTING or status == ComponentStatus.STOPPING:
                statuses.append(HealthStatus.DEGRADED)
            else:
                statuses.append(HealthStatus.HEALTHY)
                
        # Determine overall status
        if HealthStatus.CRITICAL in statuses:
            return HealthStatus.CRITICAL
        elif HealthStatus.UNHEALTHY in statuses:
            return HealthStatus.UNHEALTHY
        elif HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
            
    async def _collect_metrics(self) -> Dict[str, Any]:
        """Collect system metrics."""
        return {
            "uptime_seconds": self._calculate_uptime(),
            "health_checks_performed": len(self._health_history),
            "components_monitored": len(self._health_checks),
            "last_error_count": self._count_recent_errors()
        }
        
    def _calculate_uptime(self) -> float:
        """Calculate system uptime."""
        if not self._health_history:
            return 0.0
            
        first_check = self._health_history[0]["timestamp"]
        return float((datetime.now(UTC) - first_check).total_seconds())
        
    def _count_recent_errors(self) -> int:
        """Count errors in last hour."""
        cutoff = datetime.now(UTC) - timedelta(hours=1)
        error_count = 0
        
        for report in self._health_history:
            if report["timestamp"] < cutoff:
                continue
                
            for component_result in report.get("components", {}).values():
                if component_result.get("status") == ComponentStatus.ERROR:
                    error_count += 1
                    
        return error_count
        
    def _trim_history(self) -> None:
        """Trim old entries from health history."""
        # Keep last 7 days
        cutoff = datetime.now(UTC) - timedelta(days=7)
        self._health_history = [
            h for h in self._health_history
            if h["timestamp"] > cutoff
        ]