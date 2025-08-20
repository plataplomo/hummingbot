"""WebSocket Error System Health Checks.

Provides comprehensive health checks for the new WebSocket error system,
including component validation, performance monitoring, and system readiness checks.

This is part of the 100-step WebSocket Type Safety refactoring plan (Step 74).
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from cyberdelta.apis.common.error_foundation import (
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.enums.websocket import HealthStatus, WebSocketErrorCode
from cyberdelta.apis.exceptions.websocket import WebSocketStreamError
from cyberdelta.apis.models.websocket import StreamErrorContext
from cyberdelta.apis.models.websocket.health import (
    ComponentStatus,
    HealthCheckConfig,
    PerformanceHealth,
    SystemHealth,
)
from cyberdelta.apis.websocket.metrics.error_metrics import (
    WebSocketErrorMetrics,
)
from cyberdelta.enums import ExchangeName


class WebSocketErrorHealthCheck:
    """Health check system for WebSocket error handling."""

    def __init__(
        self,
        config: HealthCheckConfig | None = None,
        metrics_collector: WebSocketErrorMetrics | None = None,
    ) -> None:
        """Initialize health check system.

        Args:
            config: Health check configuration
            metrics_collector: Metrics collector to monitor
        """
        self.config = config or HealthCheckConfig()
        self.metrics_collector = metrics_collector
        self._last_check: SystemHealth | None = None
        self._check_history: list[SystemHealth] = []
        self._health_callbacks: list[Callable[[SystemHealth], None]] = []
        self._component_checks: dict[str, Callable[[], ComponentStatus]] = {}

        # Register default component checks
        self._register_default_checks()

    def _register_default_checks(self) -> None:
        """Register default component health checks."""
        if self.config.check_error_handler:
            self._component_checks["error_handler"] = self._check_error_handler

        if self.config.check_metrics_collector:
            self._component_checks["metrics_collector"] = self._check_metrics_collector

        if self.config.check_recovery_system:
            self._component_checks["recovery_system"] = self._check_recovery_system

    def register_component_check(
        self, name: str, check_func: Callable[[], ComponentStatus]
    ) -> None:
        """Register a custom component health check.

        Args:
            name: Component name
            check_func: Function that returns ComponentStatus
        """
        self._component_checks[name] = check_func

    def add_health_callback(self, callback: Callable[[SystemHealth], None]) -> None:
        """Add a callback for health status changes.

        Args:
            callback: Function to call on health changes
        """
        self._health_callbacks.append(callback)

    async def check_health(self) -> SystemHealth:
        """Perform comprehensive health check.

        Returns:
            SystemHealth with current status
        """
        components: list[ComponentStatus] = []

        # Check all registered components
        for name, check_func in self._component_checks.items():
            try:
                status = check_func()
                components.append(status)
            except (RuntimeError, ValueError, TypeError, AttributeError, OSError) as e:
                components.append(
                    ComponentStatus(
                        name=name, status=HealthStatus.UNHEALTHY, message=f"Check failed: {e}"
                    )
                )

        # Check performance if enabled
        performance = None
        if self.config.check_performance:
            performance = await self._check_performance()

        # Calculate error metrics
        error_rate, recovery_rate = self._calculate_error_metrics()

        # Determine overall status
        overall_status = self._determine_overall_status(
            components, performance, error_rate, recovery_rate
        )

        # Create health report
        health = SystemHealth(
            overall_status=overall_status,
            components=components,
            performance=performance,
            error_rate=error_rate,
            recovery_success_rate=recovery_rate,
        )

        # Store and notify
        self._last_check = health
        self._check_history.append(health)
        self._notify_callbacks(health)

        return health

    def _check_error_handler(self) -> ComponentStatus:
        """Check error handler health.

        Returns:
            ComponentStatus for error handler
        """
        try:
            # Create test error
            test_context = StreamErrorContext(
                connection_id="health-check-12345",
                exchange=ExchangeName.HYPERLIQUID,
                error_timestamp_ms=int(time.time() * 1000),
            )

            test_error = WebSocketStreamError(
                message="Health check test error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=test_context,
            )

            # Verify error creation succeeded
            if test_error.code == WebSocketErrorCode.CONNECTION_LOST:
                return ComponentStatus(
                    name="error_handler",
                    status=HealthStatus.HEALTHY,
                    message="Error handler functioning correctly",
                )

            return ComponentStatus(
                name="error_handler",
                status=HealthStatus.DEGRADED,
                message="Error handler not creating errors correctly",
            )

        except (RuntimeError, ValueError, TypeError, AttributeError, ImportError, OSError) as e:
            return ComponentStatus(
                name="error_handler",
                status=HealthStatus.UNHEALTHY,
                message=f"Error handler check failed: {e}",
            )

    def _check_metrics_collector(self) -> ComponentStatus:
        """Check metrics collector health.

        Returns:
            ComponentStatus for metrics collector
        """
        if not self.metrics_collector:
            return ComponentStatus(
                name="metrics_collector",
                status=HealthStatus.UNKNOWN,
                message="Metrics collector not configured",
            )

        try:
            # Get current metrics
            metrics = self.metrics_collector.get_summary()

            # Check if metrics are being collected
            if metrics.total_errors > 0 or self.metrics_collector.has_errors:
                return ComponentStatus(
                    name="metrics_collector",
                    status=HealthStatus.HEALTHY,
                    message="Metrics collector active",
                    metadata={"total_errors": metrics.total_errors},
                )

            # No errors collected yet might be normal
            return ComponentStatus(
                name="metrics_collector",
                status=HealthStatus.HEALTHY,
                message="Metrics collector ready (no errors recorded)",
            )

        except (RuntimeError, ValueError, TypeError, AttributeError, OSError) as e:
            return ComponentStatus(
                name="metrics_collector",
                status=HealthStatus.UNHEALTHY,
                message=f"Metrics collector check failed: {e}",
            )

    def _check_recovery_system(self) -> ComponentStatus:
        """Check recovery system health.

        Returns:
            ComponentStatus for recovery system
        """
        try:
            # Test recovery strategy determination
            test_context = StreamErrorContext(
                connection_id="health-check-12345",
                exchange=ExchangeName.HYPERLIQUID,
                error_timestamp_ms=int(time.time() * 1000),
                reconnect_count=2,
            )

            test_error = WebSocketStreamError(
                message="Recovery test",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=test_context,
            )

            strategy = test_error.get_recovery_strategy()

            if strategy != WebSocketRecoveryStrategy.NONE:
                return ComponentStatus(
                    name="recovery_system",
                    status=HealthStatus.HEALTHY,
                    message="Recovery system functioning",
                    metadata={"test_strategy": strategy.value},
                )

            return ComponentStatus(
                name="recovery_system",
                status=HealthStatus.DEGRADED,
                message="Recovery system returned NONE for retryable error",
            )

        except (RuntimeError, ValueError, TypeError, AttributeError, OSError) as e:
            return ComponentStatus(
                name="recovery_system",
                status=HealthStatus.UNHEALTHY,
                message=f"Recovery system check failed: {e}",
            )

    async def _check_performance(self) -> PerformanceHealth:
        """Check performance health.

        Returns:
            PerformanceHealth metrics
        """
        # Measure error creation time
        start = time.perf_counter()
        for _ in range(100):
            test_context = StreamErrorContext(
                connection_id="perf-test-12345",
                exchange=ExchangeName.HYPERLIQUID,
                error_timestamp_ms=int(time.time() * 1000),
            )
            WebSocketStreamError(
                message="Performance test",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=test_context,
            )
        error_creation_us = ((time.perf_counter() - start) / 100) * 1_000_000

        # Measure context creation time
        start = time.perf_counter()
        for _ in range(100):
            StreamErrorContext(
                connection_id="perf-test-12345",
                exchange=ExchangeName.HYPERLIQUID,
                error_timestamp_ms=int(time.time() * 1000),
            )
        context_creation_us = ((time.perf_counter() - start) / 100) * 1_000_000

        # Calculate handler overhead (simplified)
        handler_overhead_percent = 10.0  # Placeholder - would need actual measurement

        # Get memory usage (simplified)
        memory_usage_mb = sys.getsizeof(self) / (1024 * 1024)

        # Determine performance status
        status = HealthStatus.HEALTHY
        if error_creation_us > self.config.max_error_creation_us:
            status = HealthStatus.DEGRADED
        if context_creation_us > self.config.max_context_creation_us:
            status = HealthStatus.DEGRADED
        if handler_overhead_percent > self.config.max_handler_overhead_percent:
            status = HealthStatus.DEGRADED
        if memory_usage_mb > self.config.max_memory_usage_mb:
            status = HealthStatus.UNHEALTHY

        return PerformanceHealth(
            error_creation_us=error_creation_us,
            context_creation_us=context_creation_us,
            handler_overhead_percent=handler_overhead_percent,
            memory_usage_mb=memory_usage_mb,
            status=status,
        )

    def _calculate_error_metrics(self) -> tuple[float, float]:
        """Calculate error rate and recovery success rate.

        Returns:
            Tuple of (error_rate, recovery_success_rate)
        """
        if not self.metrics_collector:
            return 0.0, 1.0

        try:
            metrics = self.metrics_collector.get_summary()

            # Calculate error rate (simplified)
            error_rate = (
                metrics.total_errors / (self.config.metrics_window_minutes * 60)
                if metrics.total_errors > 0
                else 0.0
            )

            # Calculate recovery success rate (convert percentage to ratio)
            recovery_rate = self.metrics_collector.get_recovery_success_rate() / 100.0
        except (RuntimeError, ValueError, TypeError, AttributeError, OSError):
            return 0.0, 1.0
        else:
            return error_rate, recovery_rate

    def _determine_overall_status(
        self,
        components: list[ComponentStatus],
        performance: PerformanceHealth | None,
        error_rate: float,
        recovery_rate: float,
    ) -> HealthStatus:
        """Determine overall system health status.

        Args:
            components: Component health statuses
            performance: Performance health metrics
            error_rate: Current error rate
            recovery_rate: Recovery success rate

        Returns:
            Overall HealthStatus
        """
        # Check for any unhealthy components
        unhealthy_count = sum(1 for c in components if c.status == HealthStatus.UNHEALTHY)
        degraded_count = sum(1 for c in components if c.status == HealthStatus.DEGRADED)

        if unhealthy_count > 0:
            return HealthStatus.UNHEALTHY

        # Check performance
        if performance and performance.status == HealthStatus.UNHEALTHY:
            return HealthStatus.UNHEALTHY

        # Check error metrics
        if error_rate > self.config.max_error_rate:
            return HealthStatus.DEGRADED

        if recovery_rate < self.config.min_recovery_success_rate:
            return HealthStatus.DEGRADED

        # Check for degraded components
        if degraded_count > 0:
            return HealthStatus.DEGRADED

        if performance and performance.status == HealthStatus.DEGRADED:
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    def _notify_callbacks(self, health: SystemHealth) -> None:
        """Notify registered callbacks of health status.

        Args:
            health: Current system health
        """
        for callback in self._health_callbacks:
            with contextlib.suppress(RuntimeError, ValueError, TypeError, AttributeError, OSError):
                callback(health)

    def get_last_health(self) -> SystemHealth | None:
        """Get the last health check result.

        Returns:
            Last SystemHealth or None
        """
        return self._last_check

    def get_health_history(self, minutes: int = 60) -> list[SystemHealth]:
        """Get health check history.

        Args:
            minutes: Number of minutes of history to return

        Returns:
            List of SystemHealth checks
        """
        cutoff = datetime.now(UTC) - timedelta(minutes=minutes)
        return [h for h in self._check_history if h.check_timestamp >= cutoff]

    def is_healthy(self) -> bool:
        """Quick check if system is healthy.

        Returns:
            True if system is healthy
        """
        if not self._last_check:
            return False
        return self._last_check.overall_status == HealthStatus.HEALTHY

    async def start_continuous_monitoring(self, interval_seconds: int | None = None) -> None:
        """Start continuous health monitoring.

        Args:
            interval_seconds: Check interval (uses config default if None)
        """
        interval = interval_seconds or self.config.check_interval_seconds

        while True:
            with contextlib.suppress(RuntimeError, ValueError, TypeError, AttributeError, OSError):
                await self.check_health()

            await asyncio.sleep(interval)

    def generate_health_report(self) -> str:
        """Generate a human-readable health report.

        Returns:
            Health report as string
        """
        if not self._last_check:
            return "No health check data available"

        health = self._last_check

        lines = [
            "=" * 60,
            "WebSocket Error System Health Report",
            "=" * 60,
            f"Timestamp: {health.check_timestamp.isoformat()}",
            f"Overall Status: {health.overall_status.value.upper()}",
            "",
            "Components:",
        ]

        for component in health.components:
            status_symbol = {
                HealthStatus.HEALTHY: "✅",
                HealthStatus.DEGRADED: "⚠️",
                HealthStatus.UNHEALTHY: "❌",
                HealthStatus.UNKNOWN: "❓",
            }.get(component.status, "?")

            lines.append(f"  {status_symbol} {component.name}: {component.message}")

        if health.performance:
            lines.extend([
                "",
                "Performance:",
                f"  Error Creation: {health.performance.error_creation_us:.1f}μs",
                f"  Context Creation: {health.performance.context_creation_us:.1f}μs",
                f"  Handler Overhead: {health.performance.handler_overhead_percent:.1f}%",
                f"  Memory Usage: {health.performance.memory_usage_mb:.1f}MB",
                f"  Status: {health.performance.status.value}",
            ])

        lines.extend([
            "",
            "Metrics:",
            f"  Error Rate: {health.error_rate:.3f} errors/sec",
            f"  Recovery Success: {health.recovery_success_rate:.1%}",
        ])

        if health.last_error:
            lines.extend([
                "",
                f"Last Error: {health.last_error}",
            ])

        lines.append("=" * 60)

        return "\n".join(lines)
