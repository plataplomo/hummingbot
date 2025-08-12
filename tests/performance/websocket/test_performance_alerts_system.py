"""Performance alerts system tests for WebSocket error handling.

This module tests the performance alerting system that monitors WebSocket error
operations and triggers alerts when performance degrades beyond thresholds.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any
from unittest.mock import Mock

import pytest

from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class AlertLevel(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class PerformanceAlert:
    """Performance alert data model."""

    def __init__(
        self,
        metric_name: str,
        alert_level: AlertLevel,
        current_value: float,
        threshold_value: float,
        message: str,
        timestamp: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.metric_name = metric_name
        self.alert_level = alert_level
        self.current_value = current_value
        self.threshold_value = threshold_value
        self.message = message
        self.timestamp = timestamp or datetime.now(UTC)
        self.metadata = metadata or {}


class PerformanceThreshold:
    """Performance threshold configuration."""

    def __init__(
        self,
        metric_name: str,
        warning_threshold: float,
        error_threshold: float,
        critical_threshold: float,
        check_interval_ms: int = 1000,
        cooldown_ms: int = 60000,
    ) -> None:
        self.metric_name = metric_name
        self.warning_threshold = warning_threshold
        self.error_threshold = error_threshold
        self.critical_threshold = critical_threshold
        self.check_interval_ms = check_interval_ms
        self.cooldown_ms = cooldown_ms
        self.last_alert_time: datetime | None = None


class PerformanceAlertsSystem:
    """System for monitoring performance and generating alerts."""

    def __init__(self) -> None:
        self.thresholds: dict[str, PerformanceThreshold] = {}
        self.alerts: list[PerformanceAlert] = []
        self.alert_handlers: list[Callable[[PerformanceAlert], None]] = []
        self.metrics_buffer: dict[str, list[float]] = {}
        self.enabled = True

    def register_threshold(self, threshold: PerformanceThreshold) -> None:
        """Register a performance threshold."""
        if not self.enabled:
            return
        self.thresholds[threshold.metric_name] = threshold
        if threshold.metric_name not in self.metrics_buffer:
            self.metrics_buffer[threshold.metric_name] = []

    def record_metric(self, metric_name: str, value: float) -> None:
        """Record a metric value."""
        if not self.enabled:
            return

        if metric_name not in self.metrics_buffer:
            self.metrics_buffer[metric_name] = []

        self.metrics_buffer[metric_name].append(value)

        # Check if we should evaluate thresholds
        if metric_name in self.thresholds:
            self._check_threshold(metric_name, value)

    def _check_threshold(self, metric_name: str, value: float) -> None:
        """Check if a metric value exceeds thresholds."""
        threshold = self.thresholds[metric_name]

        # Check cooldown period
        if threshold.last_alert_time:
            cooldown_elapsed = (
                datetime.now(UTC) - threshold.last_alert_time
            ).total_seconds() * 1000
            if cooldown_elapsed < threshold.cooldown_ms:
                return

        # Determine alert level
        alert_level = None
        threshold_value = 0.0

        if value >= threshold.critical_threshold:
            alert_level = AlertLevel.CRITICAL
            threshold_value = threshold.critical_threshold
        elif value >= threshold.error_threshold:
            alert_level = AlertLevel.ERROR
            threshold_value = threshold.error_threshold
        elif value >= threshold.warning_threshold:
            alert_level = AlertLevel.WARNING
            threshold_value = threshold.warning_threshold

        if alert_level:
            self._trigger_alert(
                metric_name=metric_name,
                alert_level=alert_level,
                current_value=value,
                threshold_value=threshold_value,
            )
            threshold.last_alert_time = datetime.now(UTC)

    def _trigger_alert(
        self,
        metric_name: str,
        alert_level: AlertLevel,
        current_value: float,
        threshold_value: float,
    ) -> None:
        """Trigger a performance alert."""
        alert = PerformanceAlert(
            metric_name=metric_name,
            alert_level=alert_level,
            current_value=current_value,
            threshold_value=threshold_value,
            message=f"Performance threshold exceeded for {metric_name}: {current_value:.2f} > {threshold_value:.2f}",
        )

        self.alerts.append(alert)

        # Notify handlers
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception:
                pass  # Don't let handler errors break alerting

    def add_alert_handler(self, handler: Callable[[PerformanceAlert], None]) -> None:
        """Add an alert handler."""
        self.alert_handlers.append(handler)

    def get_recent_alerts(self, minutes: int = 5) -> list[PerformanceAlert]:
        """Get alerts from the last N minutes."""
        cutoff_time = datetime.now(UTC) - timedelta(minutes=minutes)
        return [alert for alert in self.alerts if alert.timestamp >= cutoff_time]

    def get_metrics_summary(self, metric_name: str) -> dict[str, float]:
        """Get summary statistics for a metric."""
        if metric_name not in self.metrics_buffer or not self.metrics_buffer[metric_name]:
            return {}

        values = self.metrics_buffer[metric_name]
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "last": values[-1] if values else 0.0,
        }

    def clear_alerts(self) -> None:
        """Clear all alerts."""
        self.alerts.clear()

    def reset_metrics(self, metric_name: str | None = None) -> None:
        """Reset metrics buffer."""
        if metric_name:
            if metric_name in self.metrics_buffer:
                self.metrics_buffer[metric_name].clear()
        else:
            for buffer in self.metrics_buffer.values():
                buffer.clear()


class TestPerformanceAlertsSystem:
    """Test performance alerting system for WebSocket errors."""

    def test_threshold_registration(self) -> None:
        """Test registering performance thresholds."""
        alerts_system = PerformanceAlertsSystem()

        # Register threshold for error creation time
        threshold = PerformanceThreshold(
            metric_name="error_creation_ms",
            warning_threshold=0.5,  # 0.5ms warning
            error_threshold=1.0,  # 1ms error
            critical_threshold=2.0,  # 2ms critical
        )

        alerts_system.register_threshold(threshold)

        assert "error_creation_ms" in alerts_system.thresholds
        assert alerts_system.thresholds["error_creation_ms"].warning_threshold == 0.5
        assert "error_creation_ms" in alerts_system.metrics_buffer

    def test_alert_triggering_warning(self) -> None:
        """Test triggering warning level alerts."""
        alerts_system = PerformanceAlertsSystem()
        alert_handler = Mock()
        alerts_system.add_alert_handler(alert_handler)

        # Register threshold
        threshold = PerformanceThreshold(
            metric_name="response_time_ms",
            warning_threshold=100,
            error_threshold=500,
            critical_threshold=1000,
        )
        alerts_system.register_threshold(threshold)

        # Record metric below threshold - no alert
        alerts_system.record_metric("response_time_ms", 50)
        assert len(alerts_system.alerts) == 0
        alert_handler.assert_not_called()

        # Record metric at warning level
        alerts_system.record_metric("response_time_ms", 150)
        assert len(alerts_system.alerts) == 1
        assert alerts_system.alerts[0].alert_level == AlertLevel.WARNING
        assert alerts_system.alerts[0].current_value == 150
        alert_handler.assert_called_once()

    def test_alert_triggering_escalation(self) -> None:
        """Test alert escalation through severity levels."""
        alerts_system = PerformanceAlertsSystem()

        threshold = PerformanceThreshold(
            metric_name="error_rate",
            warning_threshold=10,
            error_threshold=50,
            critical_threshold=100,
            cooldown_ms=0,  # No cooldown for testing
        )
        alerts_system.register_threshold(threshold)

        # Warning level
        alerts_system.record_metric("error_rate", 15)
        assert alerts_system.alerts[-1].alert_level == AlertLevel.WARNING

        # Error level
        alerts_system.record_metric("error_rate", 60)
        assert alerts_system.alerts[-1].alert_level == AlertLevel.ERROR

        # Critical level
        alerts_system.record_metric("error_rate", 150)
        assert alerts_system.alerts[-1].alert_level == AlertLevel.CRITICAL

        assert len(alerts_system.alerts) == 3

    def test_alert_cooldown_period(self) -> None:
        """Test alert cooldown to prevent spam."""
        alerts_system = PerformanceAlertsSystem()

        threshold = PerformanceThreshold(
            metric_name="memory_usage_mb",
            warning_threshold=100,
            error_threshold=200,
            critical_threshold=500,
            cooldown_ms=100,  # 100ms cooldown
        )
        alerts_system.register_threshold(threshold)

        # First alert
        alerts_system.record_metric("memory_usage_mb", 150)
        assert len(alerts_system.alerts) == 1

        # Second alert within cooldown - should be ignored
        alerts_system.record_metric("memory_usage_mb", 160)
        assert len(alerts_system.alerts) == 1  # Still only 1 alert

        # Wait for cooldown
        time.sleep(0.15)  # 150ms

        # Third alert after cooldown - should trigger
        alerts_system.record_metric("memory_usage_mb", 170)
        assert len(alerts_system.alerts) == 2

    def test_multiple_alert_handlers(self) -> None:
        """Test multiple alert handlers."""
        alerts_system = PerformanceAlertsSystem()

        handler1_alerts = []
        handler2_alerts = []

        alerts_system.add_alert_handler(lambda alert: handler1_alerts.append(alert))
        alerts_system.add_alert_handler(lambda alert: handler2_alerts.append(alert))

        threshold = PerformanceThreshold(
            metric_name="cpu_usage", warning_threshold=50, error_threshold=80, critical_threshold=95
        )
        alerts_system.register_threshold(threshold)

        # Trigger alert
        alerts_system.record_metric("cpu_usage", 85)

        # Both handlers should receive the alert
        assert len(handler1_alerts) == 1
        assert len(handler2_alerts) == 1
        assert handler1_alerts[0].alert_level == AlertLevel.ERROR
        assert handler2_alerts[0].alert_level == AlertLevel.ERROR

    def test_websocket_error_performance_alerts(self) -> None:
        """Test alerts for WebSocket error operation performance.

        Based on actual performance requirements:
        - Error creation: Currently ~70µs (should be <10µs ideally)
        - Adapter conversion: Should be <50µs (mapping operation)
        """
        alerts_system = PerformanceAlertsSystem()

        # Register realistic thresholds based on current performance
        # These will alert when performance degrades from current levels
        alerts_system.register_threshold(
            PerformanceThreshold(
                metric_name="ws_error_creation_us",
                warning_threshold=100,  # Current is ~70µs, warn at 100µs
                error_threshold=200,  # Error at 200µs (3x ideal)
                critical_threshold=500,  # Critical at 500µs (system unusable)
            )
        )

        alerts_system.register_threshold(
            PerformanceThreshold(
                metric_name="ws_adapter_conversion_us",
                warning_threshold=50,  # Should be fast (mapping)
                error_threshold=100,  # 100µs is too slow for mapping
                critical_threshold=200,  # 200µs would impact throughput
            )
        )

        # Simulate WebSocket error operations
        for i in range(100):
            # Measure error creation
            start_time = time.perf_counter()
            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
            creation_time_us = (time.perf_counter() - start_time) * 1_000_000
            alerts_system.record_metric("ws_error_creation_us", creation_time_us)

            # Measure adapter conversion
            start_time = time.perf_counter()
            api_error = WebSocketErrorAdapter.to_api_error(error)
            conversion_time_us = (time.perf_counter() - start_time) * 1_000_000
            alerts_system.record_metric("ws_adapter_conversion_us", conversion_time_us)

        # Check for any performance alerts
        summary = alerts_system.get_metrics_summary("ws_error_creation_us")
        assert "avg" in summary

        # Most operations should be fast enough to avoid alerts
        # But verify the system is monitoring
        assert len(alerts_system.metrics_buffer["ws_error_creation_us"]) == 100
        assert len(alerts_system.metrics_buffer["ws_adapter_conversion_us"]) == 100

    def test_recent_alerts_filtering(self) -> None:
        """Test getting recent alerts only."""
        alerts_system = PerformanceAlertsSystem()

        # Create alerts at different times
        old_alert = PerformanceAlert(
            metric_name="old_metric",
            alert_level=AlertLevel.WARNING,
            current_value=100,
            threshold_value=50,
            message="Old alert",
            timestamp=datetime.now(UTC) - timedelta(minutes=10),
        )

        recent_alert = PerformanceAlert(
            metric_name="recent_metric",
            alert_level=AlertLevel.ERROR,
            current_value=200,
            threshold_value=100,
            message="Recent alert",
            timestamp=datetime.now(UTC) - timedelta(minutes=2),
        )

        alerts_system.alerts.extend([old_alert, recent_alert])

        # Get alerts from last 5 minutes
        recent_alerts = alerts_system.get_recent_alerts(minutes=5)

        assert len(recent_alerts) == 1
        assert recent_alerts[0].metric_name == "recent_metric"

    def test_metrics_summary_statistics(self) -> None:
        """Test metrics summary calculation."""
        alerts_system = PerformanceAlertsSystem()

        # Record various metric values
        metric_values = [10, 20, 30, 40, 50, 15, 25, 35, 45, 55]
        for value in metric_values:
            alerts_system.record_metric("test_metric", value)

        summary = alerts_system.get_metrics_summary("test_metric")

        assert summary["count"] == 10
        assert summary["min"] == 10
        assert summary["max"] == 55
        assert summary["avg"] == 32.5
        assert summary["last"] == 55

    def test_alert_system_disable_enable(self) -> None:
        """Test disabling and enabling alert system."""
        alerts_system = PerformanceAlertsSystem()

        threshold = PerformanceThreshold(
            metric_name="test_metric",
            warning_threshold=10,
            error_threshold=20,
            critical_threshold=30,
            cooldown_ms=0,  # No cooldown for this test
        )
        alerts_system.register_threshold(threshold)

        # Record metric that should trigger alert
        alerts_system.record_metric("test_metric", 25)
        assert len(alerts_system.alerts) == 1

        # Disable system
        alerts_system.enabled = False

        # Record metric - should not trigger alert
        alerts_system.record_metric("test_metric", 35)
        assert len(alerts_system.alerts) == 1  # Still only 1

        # Re-enable system
        alerts_system.enabled = True

        # Record metric - should trigger alert again
        alerts_system.record_metric("test_metric", 40)
        assert len(alerts_system.alerts) == 2

    def test_handler_error_resilience(self) -> None:
        """Test that handler errors don't break alerting."""
        alerts_system = PerformanceAlertsSystem()

        good_handler_alerts = []

        # Add handlers - one that throws, one that works
        alerts_system.add_alert_handler(lambda alert: exec('raise ValueError("Handler error")'))
        alerts_system.add_alert_handler(lambda alert: good_handler_alerts.append(alert))

        threshold = PerformanceThreshold(
            metric_name="resilience_test",
            warning_threshold=10,
            error_threshold=20,
            critical_threshold=30,
        )
        alerts_system.register_threshold(threshold)

        # Trigger alert - should still work despite first handler error
        alerts_system.record_metric("resilience_test", 25)

        assert len(alerts_system.alerts) == 1
        assert len(good_handler_alerts) == 1

    @pytest.mark.asyncio
    async def test_async_alert_monitoring(self) -> None:
        """Test alert system with async operations."""
        alerts_system = PerformanceAlertsSystem()

        threshold = PerformanceThreshold(
            metric_name="async_operation_ms",
            warning_threshold=10,
            error_threshold=50,
            critical_threshold=100,
        )
        alerts_system.register_threshold(threshold)

        async def slow_operation():
            await asyncio.sleep(0.06)  # 60ms - should trigger error alert
            return "done"

        # Measure async operation
        start_time = time.perf_counter()
        result = await slow_operation()
        operation_time_ms = (time.perf_counter() - start_time) * 1000

        alerts_system.record_metric("async_operation_ms", operation_time_ms)

        # Should have triggered error level alert
        assert len(alerts_system.alerts) == 1
        assert alerts_system.alerts[0].alert_level == AlertLevel.ERROR
        assert alerts_system.alerts[0].current_value > 50

    def test_alert_metadata(self) -> None:
        """Test alert metadata storage."""
        alerts_system = PerformanceAlertsSystem()

        alert = PerformanceAlert(
            metric_name="test_metric",
            alert_level=AlertLevel.CRITICAL,
            current_value=150,
            threshold_value=100,
            message="Test alert",
            metadata={
                "error_code": "CONNECTION_LOST",
                "exchange": "hyperliquid",
                "connection_id": "conn-123",
            },
        )

        alerts_system.alerts.append(alert)

        assert alerts_system.alerts[0].metadata["error_code"] == "CONNECTION_LOST"
        assert alerts_system.alerts[0].metadata["exchange"] == "hyperliquid"
        assert alerts_system.alerts[0].metadata["connection_id"] == "conn-123"

    def test_clear_and_reset_operations(self) -> None:
        """Test clearing alerts and resetting metrics."""
        alerts_system = PerformanceAlertsSystem()

        # Add some data
        alerts_system.record_metric("metric1", 10)
        alerts_system.record_metric("metric1", 20)
        alerts_system.record_metric("metric2", 30)

        alerts_system.alerts.append(
            PerformanceAlert(
                metric_name="test",
                alert_level=AlertLevel.INFO,
                current_value=1,
                threshold_value=0,
                message="Test",
            )
        )

        # Clear alerts
        alerts_system.clear_alerts()
        assert len(alerts_system.alerts) == 0
        assert len(alerts_system.metrics_buffer["metric1"]) == 2  # Metrics still there

        # Reset specific metric
        alerts_system.reset_metrics("metric1")
        assert len(alerts_system.metrics_buffer["metric1"]) == 0
        assert len(alerts_system.metrics_buffer["metric2"]) == 1  # metric2 unchanged

        # Reset all metrics
        alerts_system.reset_metrics()
        assert len(alerts_system.metrics_buffer["metric2"]) == 0

    def test_performance_degradation_detection(self) -> None:
        """Test detecting performance degradation over time."""
        alerts_system = PerformanceAlertsSystem()

        threshold = PerformanceThreshold(
            metric_name="operation_time_ms",
            warning_threshold=10,
            error_threshold=20,
            critical_threshold=50,
            cooldown_ms=0,  # No cooldown for testing
        )
        alerts_system.register_threshold(threshold)

        # Simulate gradual performance degradation
        baseline_time = 5.0
        for i in range(20):
            # Gradually increase time
            current_time = baseline_time + (i * 2)  # 5, 7, 9, 11, ..., 43
            alerts_system.record_metric("operation_time_ms", current_time)

        # Check alerts were triggered at appropriate levels
        warning_alerts = [a for a in alerts_system.alerts if a.alert_level == AlertLevel.WARNING]
        error_alerts = [a for a in alerts_system.alerts if a.alert_level == AlertLevel.ERROR]
        critical_alerts = [a for a in alerts_system.alerts if a.alert_level == AlertLevel.CRITICAL]

        assert len(warning_alerts) > 0  # Should have warning alerts
        assert len(error_alerts) > 0  # Should have error alerts
        assert len(critical_alerts) == 0  # Shouldn't reach critical (max is 43)

        # Verify degradation was detected
        summary = alerts_system.get_metrics_summary("operation_time_ms")
        assert summary["max"] > summary["min"] * 5  # Significant degradation
