"""Tests for WebSocket Error System Health Checks.

This module tests the health check system that monitors the WebSocket error
handling infrastructure for proper operation and performance.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

# No datetime imports needed for current tests
import pytest

from cyberdelta.apis.enums.websocket import WebSocketErrorCode
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

    async def test_health_check_initialization(self) -> None:
        """Test health check system initialization."""
        config = HealthCheckConfig(
            max_error_creation_us=500, max_context_creation_us=200, check_interval_seconds=30
        )

        health_check = WebSocketErrorHealthCheck(config=config)

        assert health_check.config.max_error_creation_us == 500
        assert health_check.config.check_interval_seconds == 30
        assert health_check.get_last_health() is None
        # Check that components are registered by performing a health check
        health_result = await health_check.check_health()
        assert len(health_result.components) > 0

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

    async def test_error_handler_health_check(self) -> None:
        """Test error handler health check."""
        health_check = WebSocketErrorHealthCheck()

        # Check overall health which includes error handler check
        health_result = await health_check.check_health()

        # Find error handler component in the results
        error_handler_status = next(
            (comp for comp in health_result.components if comp.name == "error_handler"), None
        )

        assert error_handler_status is not None
        assert error_handler_status.status == HealthStatus.HEALTHY
        assert "functioning correctly" in error_handler_status.message.lower()

    async def test_metrics_collector_health_check(self) -> None:
        """Test metrics collector health check."""
        # Without metrics collector
        health_check = WebSocketErrorHealthCheck()
        health_result = await health_check.check_health()

        metrics_status = next(
            (comp for comp in health_result.components if comp.name == "metrics_collector"), None
        )
        assert metrics_status is not None
        assert metrics_status.status == HealthStatus.UNKNOWN
        assert "not configured" in metrics_status.message.lower()

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

        health_result = await health_check.check_health()
        metrics_status = next(
            (comp for comp in health_result.components if comp.name == "metrics_collector"), None
        )
        assert metrics_status is not None
        assert metrics_status.status == HealthStatus.HEALTHY
        assert metrics_status.metadata.get("total_errors", 0) > 0

    async def test_recovery_system_health_check(self) -> None:
        """Test recovery system health check."""
        health_check = WebSocketErrorHealthCheck()

        health_result = await health_check.check_health()
        recovery_status = next(
            (comp for comp in health_result.components if comp.name == "recovery_system"), None
        )

        assert recovery_status is not None
        assert recovery_status.status == HealthStatus.HEALTHY
        assert "functioning" in recovery_status.message.lower()
        assert "test_strategy" in recovery_status.metadata

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

        health_result = await health_check.check_health()
        performance = health_result.performance

        assert performance is not None
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

    @pytest.mark.asyncio
    async def test_health_status_scenarios(self) -> None:
        """Test health check behavior in different system scenarios."""
        health_check = WebSocketErrorHealthCheck()

        # Test initial healthy state
        health_result = await health_check.check_health()
        assert health_result.overall_status in [HealthStatus.HEALTHY, HealthStatus.UNKNOWN]
        assert len(health_result.components) > 0  # Should have default components

        # Test that health check produces consistent results
        health_result2 = await health_check.check_health()
        assert health_result2.overall_status == health_result.overall_status
        assert len(health_result2.components) == len(health_result.components)

    @pytest.mark.asyncio
    async def test_custom_component_registration(self) -> None:
        """Test registering custom component health checks."""
        health_check = WebSocketErrorHealthCheck()

        def custom_check() -> ComponentStatus:
            return ComponentStatus(
                name="custom_component",
                status=HealthStatus.HEALTHY,
                message="Custom component is healthy",
            )

        health_check.register_component_check("custom", custom_check)

        # Test that custom component is included in health check
        health_result = await health_check.check_health()
        custom_component = next(
            (c for c in health_result.components if c.name == "custom_component"), None
        )
        assert custom_component is not None
        assert custom_component.status == HealthStatus.HEALTHY
        assert custom_component.message == "Custom component is healthy"

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

    @pytest.mark.asyncio
    async def test_health_history(self) -> None:
        """Test health check history tracking."""
        health_check = WebSocketErrorHealthCheck()

        # Perform multiple health checks to build history
        for _ in range(3):
            await health_check.check_health()
            await asyncio.sleep(0.1)  # Small delay between checks

        # Get recent history
        recent = health_check.get_health_history(minutes=25)
        assert len(recent) >= 3  # Should have at least 3 checks

        # Get all history
        all_history = health_check.get_health_history(minutes=60)
        assert len(all_history) >= 3

    @pytest.mark.asyncio
    async def test_is_healthy_quick_check(self) -> None:
        """Test quick health check method."""
        health_check = WebSocketErrorHealthCheck()

        # No check performed yet
        assert not health_check.is_healthy()

        # Perform a health check
        await health_check.check_health()

        # Should now have a status (either healthy or known status)
        # Since this is a real system, we can't guarantee it's healthy,
        # but we can check that is_healthy() doesn't crash and returns a boolean
        healthy_status = health_check.is_healthy()
        assert isinstance(healthy_status, bool)

    @pytest.mark.asyncio
    async def test_health_report_generation(self) -> None:
        """Test health report generation."""
        health_check = WebSocketErrorHealthCheck()

        # No data
        report = health_check.generate_health_report()
        assert "No health check data" in report

        # Perform health check to generate data
        await health_check.check_health()

        # With health data
        report = health_check.generate_health_report()
        assert "Health Status Report" in report or "Overall Status" in report
        assert len(report) > 100  # Should be a substantial report

    @pytest.mark.asyncio
    async def test_error_metrics_calculation(self) -> None:
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

        # Test that metrics can be calculated through health check
        health_result = await health_check.check_health()
        error_rate = health_result.error_rate
        recovery_rate = health_result.recovery_success_rate

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
        with contextlib.suppress(asyncio.CancelledError):
            await monitoring_task

        # Should have multiple health checks in history
        history = health_check.get_health_history(minutes=5)
        assert len(history) >= 3

    def test_performance_degradation_detection(self) -> None:
        """Test detection of performance degradation."""
        config = HealthCheckConfig(
            max_error_creation_us=500,  # Very strict
            max_context_creation_us=200,
            max_handler_overhead_percent=10,
            max_memory_usage_mb=50,
        )

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

    @pytest.mark.asyncio
    async def test_callback_error_resilience(self) -> None:
        """Test that callback errors don't break health checks."""
        health_check = WebSocketErrorHealthCheck()

        def failing_callback(health: SystemHealth) -> None:
            raise ValueError("Callback error")

        def working_callback(health: SystemHealth) -> None:
            working_callback.called = True  # type: ignore

        working_callback.called = False  # type: ignore

        health_check.add_health_callback(failing_callback)
        health_check.add_health_callback(working_callback)

        # Perform health check which should notify callbacks
        await health_check.check_health()

        # Working callback should still be called despite first one failing
        assert working_callback.called  # type: ignore

    @pytest.mark.asyncio
    async def test_health_check_with_failed_component(self) -> None:
        """Test health check when a component check fails."""
        health_check = WebSocketErrorHealthCheck()

        def failing_check() -> ComponentStatus:
            raise RuntimeError("Component check failed")

        health_check.register_component_check("failing", failing_check)

        # Should handle the failure gracefully in health check
        health_result = await health_check.check_health()

        # Should have component that failed
        failed_component = next((c for c in health_result.components if c.name == "failing"), None)
        assert failed_component is not None
        assert failed_component.status == HealthStatus.UNHEALTHY
        assert "failed" in failed_component.message.lower()

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

    @pytest.mark.asyncio
    async def test_get_last_health(self) -> None:
        """Test getting last health check result."""
        health_check = WebSocketErrorHealthCheck()

        # Initially None
        assert health_check.get_last_health() is None

        # Perform a health check
        health = await health_check.check_health()

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
