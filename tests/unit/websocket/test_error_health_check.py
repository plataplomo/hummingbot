"""Tests for WebSocket Error System Health Checks.

This module tests the health check functionality of the WebSocket error handling
system using the current architecture with .
"""

from __future__ import annotations

import pytest

from cyberdelta.apis.enums.websocket import HealthStatus
from cyberdelta.apis.enums.websocket.error_codes import WebSocketErrorCode
from cyberdelta.apis.exceptions.websocket.stream_error import WebSocketStreamError
from cyberdelta.apis.models.websocket.error_context import StreamErrorContext
from cyberdelta.apis.models.websocket.health import (
    ComponentStatus,
    HealthCheckConfig,
    SystemHealth,
)
from cyberdelta.apis.websocket.error_context.error_handler_factory import (
    WebSocketErrorHandlerFactory,
)
from cyberdelta.enums import ExchangeName
from tests.unit.websocket.test_helpers import MockWebSocketErrorHandlerRegistry


class TestWebSocketErrorSystemHealthCheck:
    """Test WebSocket error system health check functionality."""

    @pytest.fixture
    def registry(self) -> MockWebSocketErrorHandlerRegistry:
        """Create fresh registry for testing.

        Returns:
            MockWebSocketErrorHandlerRegistry: Clean registry for health testing.
        """
        return MockWebSocketErrorHandlerRegistry()

    @pytest.fixture
    def error_context(self) -> StreamErrorContext:
        """Create error context for testing.

        Returns:
            StreamErrorContext: Sample error context for health tests.
        """
        return StreamErrorContext(
            connection_id="health-test-conn",
            exchange=ExchangeName.HYPERLIQUID,
            channel="l2Book",
            topic="BTC",
            sequence_number=1,
            user_id="health-test-user",
            session_id="health-test-session",
            environment="test",
            raw_message_size=256,
        )

    def test_registry_health_check_empty(self, registry: MockWebSocketErrorHandlerRegistry) -> None:
        """Test health check with no handlers registered."""
        health = registry.health_check()

        assert isinstance(health, dict)
        assert "registry_healthy" in health
        assert "active_handlers" in health
        assert "issues" in health

        # Empty registry should be healthy but have no active handlers
        assert health["registry_healthy"] is True
        assert health["active_handlers"] == 0

    def test_registry_health_check_with_handlers(
        self, registry: MockWebSocketErrorHandlerRegistry
    ) -> None:
        """Test health check with active handlers."""
        # Create handlers for different exchanges
        hl_handler = registry.get_handler(ExchangeName.HYPERLIQUID)
        bp_handler = registry.get_handler(ExchangeName.BACKPACK)

        # Perform health check
        health = registry.health_check()

        assert health["registry_healthy"] is True
        assert health["active_handlers"] == 2

        # Keep references to prevent GC
        del hl_handler, bp_handler

    def test_registry_statistics(self, registry: MockWebSocketErrorHandlerRegistry) -> None:
        """Test registry statistics collection."""
        # Initially no handlers
        stats = registry.get_registry_statistics()
        assert isinstance(stats, dict)
        assert "total_created" in stats
        assert "active_handlers" in stats
        assert stats["total_created"] == 0
        assert stats["active_handlers"] == 0

        # Create some handlers
        handler1 = registry.get_handler(ExchangeName.HYPERLIQUID)
        handler2 = registry.get_handler(ExchangeName.BACKPACK)

        # Check updated stats
        stats = registry.get_registry_statistics()
        assert stats["total_created"] == 2
        assert stats["active_handlers"] == 2

        # Keep references
        del handler1, handler2

    @pytest.mark.asyncio
    async def test_error_handler_health_after_processing(
        self,
        registry: MockWebSocketErrorHandlerRegistry,
        error_context: StreamErrorContext,
    ) -> None:
        """Test error handler health after processing errors."""
        # Get handler and process some errors
        handler = registry.get_handler(ExchangeName.HYPERLIQUID)

        # Create test error
        error = WebSocketStreamError(
            message="Test error for health check",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=error_context,
        )

        # Process error
        await handler.handle_stream_error(error)

        # Check that handler is still healthy after processing
        registry_health = registry.health_check()
        assert registry_health["registry_healthy"] is True
        assert isinstance(registry_health["active_handlers"], int)
        assert registry_health["active_handlers"] >= 1

        # Check that metrics are being collected if enabled
        if handler.metrics_collector:
            stats = handler.metrics_collector.get_statistics()
            assert stats.total_errors_recorded >= 1

    def test_handler_factory_health_validation(self) -> None:
        """Test error handler factory configuration validation."""
        # Test valid configuration
        config = WebSocketErrorHandlerFactory.create_default_config(
            exchange=ExchangeName.HYPERLIQUID,
            environment="test",
        )

        errors = WebSocketErrorHandlerFactory.validate_configuration(config)
        assert errors == []  # Should have no validation errors

        # Test invalid configuration
        config.recovery.max_recovery_attempts = -1
        config.recovery.initial_backoff_ms = 50  # Below minimum

        errors = WebSocketErrorHandlerFactory.validate_configuration(config)
        assert len(errors) >= 2  # Should have validation errors
        assert any("max_recovery_attempts" in error for error in errors)
        assert any("initial_backoff_ms" in error for error in errors)

    def test_component_status_model(self) -> None:
        """Test ComponentStatus model functionality."""
        component = ComponentStatus(
            name="error_handler",
            status=HealthStatus.HEALTHY,
            message="Operating normally",
        )

        assert component.name == "error_handler"
        assert component.message == "Operating normally"
        assert component.last_check is not None
        assert isinstance(component.metadata, dict)

    def test_system_health_model(self) -> None:
        """Test SystemHealth model functionality."""
        health = SystemHealth(
            overall_status=HealthStatus.HEALTHY,
            components=[],
            error_rate=0.05,
            recovery_success_rate=0.95,
        )

        assert health.error_rate == 0.05
        assert health.recovery_success_rate == 0.95
        assert health.check_timestamp is not None
        assert isinstance(health.components, list)

    def test_health_check_config_model(self) -> None:
        """Test HealthCheckConfig model functionality."""
        config = HealthCheckConfig(
            check_interval_seconds=5,
            max_error_rate=0.1,
            min_recovery_success_rate=0.8,
        )

        assert config.check_interval_seconds == 5
        assert config.max_error_rate == 0.1
        assert config.min_recovery_success_rate == 0.8


class TestErrorHandlerHealthIntegration:
    """Test error handler health monitoring integration."""

    def test_minimal_handler_health(self) -> None:
        """Test health of minimal error handler."""
        handler = WebSocketErrorHandlerFactory.create_minimal_handler(
            exchange=ExchangeName.BACKPACK
        )

        # Handler should be properly configured
        assert handler is not None
        assert handler.config is not None

        # Handler should have recovery components
        assert handler.recovery_executor is not None
        assert handler.recovery_policy is not None

    def test_handler_with_metrics_health(self) -> None:
        """Test health of handler with metrics enabled."""
        config = WebSocketErrorHandlerFactory.create_default_config(
            exchange=ExchangeName.HYPERLIQUID,
            environment="test",
        )
        config.metrics.enable_metrics_collection = True

        handler = WebSocketErrorHandlerFactory.create_handler(
            exchange=ExchangeName.HYPERLIQUID,
            config=config,
        )

        # Handler should have metrics collection enabled
        assert handler.metrics_collector is not None

        # Metrics should be accessible and functional
        stats = handler.metrics_collector.get_statistics()
        assert hasattr(stats, "total_errors_recorded")
        assert stats.total_errors_recorded == 0  # No errors recorded yet

    @pytest.mark.asyncio
    async def test_registry_health_during_error_processing(self) -> None:
        """Test registry health during active error processing."""
        registry = MockWebSocketErrorHandlerRegistry()

        # Get handler and process errors
        handler = registry.get_handler(ExchangeName.HYPERLIQUID)

        # Create multiple test errors
        errors = [
            WebSocketStreamError(
                message=f"Test error {i}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=StreamErrorContext(
                    connection_id="test-conn",
                    exchange=ExchangeName.HYPERLIQUID,
                    channel="test",
                    environment="test",
                ),
            )
            for i in range(3)
        ]

        # Process errors
        for error in errors:
            await handler.handle_stream_error(error)

        # Registry should remain healthy
        health = registry.health_check()
        assert health["registry_healthy"] is True
        assert isinstance(health["active_handlers"], int)
        assert health["active_handlers"] >= 1

        # Keep handler reference
        del handler
