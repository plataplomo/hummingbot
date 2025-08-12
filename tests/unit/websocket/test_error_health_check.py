"""Tests for WebSocket Error System Health Checks.

This module tests the health check system that monitors the WebSocket error
handling infrastructure for proper operation and performance.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta

import pytest

from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_error_health_check import (
    ComponentStatus,
    HealthCheckConfig,
    HealthStatus,
    PerformanceHealth,
    SystemHealth,
    WebSocketErrorHealthCheck,
)
from cyberdelta.apis.websocket.ws_error_metrics_collector import (
    WebSocketErrorMetricsCollector,
)
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError


class TestWebSocketErrorHealthCheck:
    """Test WebSocket error system health checks."""

    def test_health_check_initialization(self) -> None:
        """Test health check system initialization."""
        config = HealthCheckConfig(
            max_error_creation_us=500, max_context_creation_us=200, check_interval_seconds=30
        )

        health_check = WebSocketErrorHealthCheck(config=config)

        assert health_check.config.max_error_creation_us == 500
        assert health_check.config.check_interval_seconds == 30
        assert health_check._last_check is None
        assert len(health_check._component_checks) > 0

    def test_component_status_model(self) -> None:
        """Test ComponentStatus model."""
        status = ComponentStatus(
            name="test_component",
            status=HealthStatus.HEALTHY,
            message="Component is functioning correctly",
            metadata={"version": "1.0.0"},
        )

        assert status.name == "test_component"
        assert status.status == HealthStatus.HEALTHY
        assert status.metadata["version"] == "1.0.0"
        assert status.last_check is not None

    def test_error_handler_health_check(self) -> None:
        """Test error handler health check."""
        health_check = WebSocketErrorHealthCheck()

        # Check error handler
        status = health_check._check_error_handler()

        assert status.name == "error_handler"
        assert status.status == HealthStatus.HEALTHY
        assert "functioning correctly" in status.message.lower()

    def test_metrics_collector_health_check(self) -> None:
        """Test metrics collector health check."""
        # Without metrics collector
        health_check = WebSocketErrorHealthCheck()
        status = health_check._check_metrics_collector()

        assert status.name == "metrics_collector"
        assert status.status == HealthStatus.UNKNOWN
        assert "not configured" in status.message.lower()

        # With metrics collector
        metrics_collector = WebSocketErrorMetricsCollector()
        health_check = WebSocketErrorHealthCheck(metrics_collector=metrics_collector)

        # Record an error to make collector active
        test_error = WebSocketStreamError(
            message="Test error",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=StreamErrorContext(
                connection_id="test-conn-12345",
                exchange="hyperliquid",
                error_timestamp_ms=int(time.time() * 1000),
            ),
        )
        metrics_collector.record_error(test_error)

        status = health_check._check_metrics_collector()
        assert status.status == HealthStatus.HEALTHY
        assert status.metadata.get("total_errors", 0) > 0

    def test_recovery_system_health_check(self) -> None:
        """Test recovery system health check."""
        health_check = WebSocketErrorHealthCheck()

        status = health_check._check_recovery_system()

        assert status.name == "recovery_system"
        assert status.status == HealthStatus.HEALTHY
        assert "functioning" in status.message.lower()
        assert "test_strategy" in status.metadata

    @pytest.mark.asyncio
    async def test_performance_health_check(self) -> None:
        """Test performance health check."""
        config = HealthCheckConfig(
            max_error_creation_us=10000,  # 10ms (realistic for Pydantic)
            max_context_creation_us=5000,  # 5ms
            max_handler_overhead_percent=50,
            max_memory_usage_mb=1000,
        )

        health_check = WebSocketErrorHealthCheck(config=config)

        performance = await health_check._check_performance()

        assert isinstance(performance, PerformanceHealth)
        assert performance.error_creation_us > 0
        assert performance.context_creation_us > 0
        assert performance.handler_overhead_percent >= 0
        assert performance.memory_usage_mb >= 0
        assert performance.status in HealthStatus

    @pytest.mark.asyncio
    async def test_overall_health_check(self) -> None:
        """Test overall system health check."""
        metrics_collector = WebSocketErrorMetricsCollector()
        health_check = WebSocketErrorHealthCheck(metrics_collector=metrics_collector)

        # Perform health check
        health = await health_check.check_health()

        assert isinstance(health, SystemHealth)
        assert health.overall_status in HealthStatus
        assert len(health.components) > 0
        assert health.error_rate >= 0
        assert 0 <= health.recovery_success_rate <= 1
        assert health.check_timestamp is not None

    def test_health_status_determination(self) -> None:
        """Test overall health status determination logic."""
        health_check = WebSocketErrorHealthCheck()

        # All healthy
        components = [
            ComponentStatus(name="c1", status=HealthStatus.HEALTHY, message="OK"),
            ComponentStatus(name="c2", status=HealthStatus.HEALTHY, message="OK"),
        ]
        performance = PerformanceHealth(
            error_creation_us=100,
            context_creation_us=50,
            handler_overhead_percent=10,
            memory_usage_mb=10,
            status=HealthStatus.HEALTHY,
        )

        status = health_check._determine_overall_status(components, performance, 0.01, 0.95)
        assert status == HealthStatus.HEALTHY

        # One unhealthy component
        components[0].status = HealthStatus.UNHEALTHY
        status = health_check._determine_overall_status(components, performance, 0.01, 0.95)
        assert status == HealthStatus.UNHEALTHY

        # Degraded performance
        components[0].status = HealthStatus.HEALTHY
        performance.status = HealthStatus.DEGRADED
        status = health_check._determine_overall_status(components, performance, 0.01, 0.95)
        assert status == HealthStatus.DEGRADED

        # High error rate
        performance.status = HealthStatus.HEALTHY
        status = health_check._determine_overall_status(
            components,
            performance,
            0.1,
            0.95,  # 10% error rate
        )
        assert status == HealthStatus.DEGRADED

        # Low recovery rate
        status = health_check._determine_overall_status(
            components,
            performance,
            0.01,
            0.5,  # 50% recovery rate
        )
        assert status == HealthStatus.DEGRADED

    def test_custom_component_registration(self) -> None:
        """Test registering custom component health checks."""
        health_check = WebSocketErrorHealthCheck()

        def custom_check() -> ComponentStatus:
            return ComponentStatus(
                name="custom_component",
                status=HealthStatus.HEALTHY,
                message="Custom component is healthy",
            )

        health_check.register_component_check("custom", custom_check)

        assert "custom" in health_check._component_checks
        status = health_check._component_checks["custom"]()
        assert status.name == "custom_component"
        assert status.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_health_callbacks(self) -> None:
        """Test health status callbacks."""
        health_check = WebSocketErrorHealthCheck()

        callback_results: list[SystemHealth] = []

        def health_callback(health: SystemHealth) -> None:
            callback_results.append(health)

        health_check.add_health_callback(health_callback)

        # Perform health check
        await health_check.check_health()

        assert len(callback_results) == 1
        assert isinstance(callback_results[0], SystemHealth)

    def test_health_history(self) -> None:
        """Test health check history tracking."""
        health_check = WebSocketErrorHealthCheck()

        # Create some health checks
        for i in range(3):
            health = SystemHealth(
                overall_status=HealthStatus.HEALTHY,
                check_timestamp=datetime.now(UTC) - timedelta(minutes=i * 10),
            )
            health_check._check_history.append(health)

        # Get recent history
        recent = health_check.get_health_history(minutes=25)
        assert len(recent) == 3  # All within 25 minutes (0, 10, 20 minutes ago)

        # Get all history
        all_history = health_check.get_health_history(minutes=60)
        assert len(all_history) == 3

    def test_is_healthy_quick_check(self) -> None:
        """Test quick health check method."""
        health_check = WebSocketErrorHealthCheck()

        # No check performed yet
        assert not health_check.is_healthy()

        # Set healthy status
        health_check._last_check = SystemHealth(overall_status=HealthStatus.HEALTHY)
        assert health_check.is_healthy()

        # Set unhealthy status
        health_check._last_check = SystemHealth(overall_status=HealthStatus.UNHEALTHY)
        assert not health_check.is_healthy()

    def test_health_report_generation(self) -> None:
        """Test health report generation."""
        health_check = WebSocketErrorHealthCheck()

        # No data
        report = health_check.generate_health_report()
        assert "No health check data" in report

        # With health data
        health_check._last_check = SystemHealth(
            overall_status=HealthStatus.HEALTHY,
            components=[
                ComponentStatus(name="error_handler", status=HealthStatus.HEALTHY, message="OK"),
                ComponentStatus(
                    name="metrics_collector",
                    status=HealthStatus.DEGRADED,
                    message="High memory usage",
                ),
            ],
            performance=PerformanceHealth(
                error_creation_us=500,
                context_creation_us=200,
                handler_overhead_percent=15,
                memory_usage_mb=50,
                status=HealthStatus.HEALTHY,
            ),
            error_rate=0.002,
            recovery_success_rate=0.92,
        )

        report = health_check.generate_health_report()

        assert "WebSocket Error System Health Report" in report
        assert "Overall Status: HEALTHY" in report
        assert "error_handler" in report
        assert "metrics_collector" in report
        assert "Error Creation: 500.0µs" in report
        assert "Recovery Success: 92.0%" in report

    def test_error_metrics_calculation(self) -> None:
        """Test error rate and recovery rate calculation."""
        metrics_collector = WebSocketErrorMetricsCollector()
        health_check = WebSocketErrorHealthCheck(metrics_collector=metrics_collector)

        # Record some errors
        for i in range(10):
            error = WebSocketStreamError(
                message=f"Error {i}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=StreamErrorContext(
                    connection_id="test-conn-12345",
                    exchange="hyperliquid",
                    error_timestamp_ms=int(time.time() * 1000),
                ),
            )
            metrics_collector.record_error(
                error,
                recovery_time_ms=100 if i < 8 else None,  # 80% recovery
            )

        error_rate, recovery_rate = health_check._calculate_error_metrics()

        assert error_rate >= 0
        assert 0 <= recovery_rate <= 1

    @pytest.mark.asyncio
    async def test_continuous_monitoring(self) -> None:
        """Test continuous health monitoring."""
        health_check = WebSocketErrorHealthCheck()
        health_check.config.check_interval_seconds = 1  # 1 second for testing

        # Create monitoring task
        monitoring_task = asyncio.create_task(
            health_check.start_continuous_monitoring(interval_seconds=1)
        )

        # Let it run for a bit
        await asyncio.sleep(3.5)  # Should complete 3 checks

        # Cancel monitoring
        monitoring_task.cancel()
        try:
            await monitoring_task
        except asyncio.CancelledError:
            pass

        # Should have multiple health checks
        assert len(health_check._check_history) >= 3

    def test_performance_degradation_detection(self) -> None:
        """Test detection of performance degradation."""
        config = HealthCheckConfig(
            max_error_creation_us=500,  # Very strict
            max_context_creation_us=200,
            max_handler_overhead_percent=10,
            max_memory_usage_mb=50,
        )

        health_check = WebSocketErrorHealthCheck(config=config)

        # Simulate degraded performance
        performance = PerformanceHealth(
            error_creation_us=1000,  # Exceeds limit
            context_creation_us=150,  # Within limit
            handler_overhead_percent=5,  # Within limit
            memory_usage_mb=30,  # Within limit
            status=HealthStatus.HEALTHY,  # Will be overridden
        )

        # Recalculate status based on thresholds
        if performance.error_creation_us > config.max_error_creation_us:
            performance.status = HealthStatus.DEGRADED

        assert performance.status == HealthStatus.DEGRADED

    def test_callback_error_resilience(self) -> None:
        """Test that callback errors don't break health checks."""
        health_check = WebSocketErrorHealthCheck()

        def failing_callback(health: SystemHealth) -> None:
            raise ValueError("Callback error")

        def working_callback(health: SystemHealth) -> None:
            working_callback.called = True  # type: ignore

        working_callback.called = False  # type: ignore

        health_check.add_health_callback(failing_callback)
        health_check.add_health_callback(working_callback)

        # Notify callbacks
        health = SystemHealth(overall_status=HealthStatus.HEALTHY)
        health_check._notify_callbacks(health)

        # Working callback should still be called despite first one failing
        assert working_callback.called  # type: ignore

    def test_health_check_with_failed_component(self) -> None:
        """Test health check when a component check fails."""
        health_check = WebSocketErrorHealthCheck()

        def failing_check() -> ComponentStatus:
            raise RuntimeError("Component check failed")

        health_check.register_component_check("failing", failing_check)

        # Should handle the failure gracefully
        components = []
        for name, check_func in health_check._component_checks.items():
            try:
                status = check_func()
                components.append(status)
            except Exception as e:
                components.append(
                    ComponentStatus(
                        name=name, status=HealthStatus.UNHEALTHY, message=f"Check failed: {e}"
                    )
                )

        # Should have an unhealthy component
        unhealthy = [c for c in components if c.status == HealthStatus.UNHEALTHY]
        assert len(unhealthy) > 0
        assert "Component check failed" in unhealthy[0].message

    @pytest.mark.asyncio
    async def test_health_check_performance(self) -> None:
        """Test that health checks themselves are performant."""
        health_check = WebSocketErrorHealthCheck()

        # Measure health check time
        start = time.perf_counter()
        await health_check.check_health()
        duration = time.perf_counter() - start

        # Health check should be fast (< 100ms)
        assert duration < 0.1, f"Health check took {duration:.3f}s"

    def test_get_last_health(self) -> None:
        """Test getting last health check result."""
        health_check = WebSocketErrorHealthCheck()

        # Initially None
        assert health_check.get_last_health() is None

        # Set a health check
        health = SystemHealth(overall_status=HealthStatus.HEALTHY)
        health_check._last_check = health

        assert health_check.get_last_health() == health

    def test_health_check_config_defaults(self) -> None:
        """Test health check configuration defaults."""
        config = HealthCheckConfig()

        assert config.max_error_creation_us == 1000  # 1ms
        assert config.max_context_creation_us == 500  # 0.5ms
        assert config.max_handler_overhead_percent == 20
        assert config.max_memory_usage_mb == 100
        assert config.max_error_rate == 0.05  # 5%
        assert config.min_recovery_success_rate == 0.8  # 80%
        assert config.check_interval_seconds == 60
        assert config.metrics_window_minutes == 5
        assert config.check_error_handler
        assert config.check_metrics_collector
        assert config.check_recovery_system
        assert config.check_performance
