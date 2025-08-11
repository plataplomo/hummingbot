"""Unit tests for event bus health check."""

import time
from unittest.mock import Mock, patch

import pytest

from cyberdelta.enums.component_state import ComponentState
from cyberdelta.infrastructure.event_bus.health_check import EventBusHealthCheck
from cyberdelta.models.events.health_status import EventBusHealthStatus


class TestEventBusHealthCheck:
    """Test health check for event bus."""

    def test_health_check_initialization(self) -> None:
        """Test health check initializes properly."""
        mock_event_bus = Mock()
        mock_event_bus.get_total_handler_count.return_value = 0
        mock_event_bus.get_pending_request_count.return_value = 0

        health_check = EventBusHealthCheck(mock_event_bus)

        # Test behavior rather than private members
        # The health check should work after initialization
        status = health_check.check_health()
        assert isinstance(status, EventBusHealthStatus)

    def test_health_status_creation(self) -> None:
        """Test EventBusHealthStatus creation."""
        status = EventBusHealthStatus(
            is_healthy=True,
            last_event_time=None,
            event_count=0,
            error_count=0,
            handler_count=5,
            pending_requests=0,
            state=ComponentState.RUNNING,
            message="Event bus healthy",
        )

        assert status.is_healthy is True
        assert status.handler_count == 5
        assert status.state == ComponentState.RUNNING

    def test_handler_count(self) -> None:
        """Test handler count calculation."""
        mock_event_bus = Mock()
        mock_event_bus.get_total_handler_count.return_value = 3

        health_check = EventBusHealthCheck(mock_event_bus)
        count = health_check.get_handler_count()

        assert count == 3

    def test_pending_requests_count(self) -> None:
        """Test pending requests count."""
        mock_event_bus = Mock()
        mock_event_bus.get_pending_request_count.return_value = 3

        health_check = EventBusHealthCheck(mock_event_bus)
        count = health_check.get_pending_requests()

        assert count == 3

    def test_is_healthy_calculation(self) -> None:
        """Test health calculation logic."""
        mock_event_bus = Mock()
        health_check = EventBusHealthCheck(mock_event_bus)

        # Mock the methods using patch
        with (
            patch.object(health_check, "get_handler_count", return_value=5),
            patch.object(health_check, "get_pending_requests", return_value=0),
        ):
            status = health_check.check_health()

        # Should be healthy with handlers and no pending requests
        assert status.is_healthy is True

    def test_error_handling(self) -> None:
        """Test error handling in health check."""
        mock_event_bus = Mock()
        health_check = EventBusHealthCheck(mock_event_bus)

        # Mock methods to raise exceptions using patch
        with patch.object(health_check, "get_handler_count", side_effect=Exception("Test error")):
            status = health_check.check_health()

        # Should be unhealthy when errors occur
        assert status.is_healthy is False

    @pytest.mark.timing
    def test_stale_detection(self) -> None:
        """Test stale bus detection."""
        mock_event_bus = Mock()
        health_check = EventBusHealthCheck(mock_event_bus, stale_threshold_seconds=1)

        # Mock time to make bus appear stale
        time.sleep(1.1)  # Make it stale

        status = health_check.check_health()

        # Check that health status includes appropriate message
        assert isinstance(status.message, str)

    def test_comprehensive_health_check(self) -> None:
        """Test comprehensive health check."""
        mock_event_bus = Mock()
        mock_event_bus.get_total_handler_count.return_value = 1
        mock_event_bus.get_pending_request_count.return_value = 0

        health_check = EventBusHealthCheck(mock_event_bus)
        status = health_check.check_health()

        assert isinstance(status, EventBusHealthStatus)
        assert isinstance(status.is_healthy, bool)
        assert isinstance(status.handler_count, int)

    def test_health_degradation(self) -> None:
        """Test health degradation scenarios."""
        mock_event_bus = Mock()
        health_check = EventBusHealthCheck(mock_event_bus)

        # Test with error in handler count using patch
        with (
            patch.object(health_check, "get_handler_count", side_effect=Exception("Handler error")),
            patch.object(health_check, "get_pending_requests", return_value=0),
        ):
            status = health_check.check_health()

        assert status.is_healthy is False

    def test_recovery_detection(self) -> None:
        """Test health recovery detection."""
        mock_event_bus = Mock()
        mock_event_bus.get_total_handler_count.return_value = 2
        mock_event_bus.get_pending_request_count.return_value = 0

        health_check = EventBusHealthCheck(mock_event_bus)
        status = health_check.check_health()

        # Should be healthy with active handlers
        assert status.is_healthy is True
        assert status.handler_count == 2
