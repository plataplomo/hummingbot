"""Compatibility tests for WebSocket error system migration.

Tests the compatibility layer between old APIError and new WebSocketStreamError
systems, ensuring smooth migration without breaking existing functionality.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, Field, ValidationError

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode
from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.ws_dual_error_manager import (
    DualErrorManager,
    ErrorSystemMode,
)
from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_exceptions import (
    WebSocketAuthenticationError,
    WebSocketConnectionError,
    WebSocketRateLimitError,
    WebSocketSubscriptionError,
    WebSocketValidationError,
)
from cyberdelta.apis.websocket.ws_migration_tracker import (
    ComponentStatus,
    MigrationPhase,
    WebSocketMigrationTracker,
)
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def test_context() -> StreamErrorContext:
    """Test error context for compatibility tests."""
    return StreamErrorContext(
        connection_id="compat-test-conn",
        exchange="hyperliquid",
        channel="orderbook",
        topic="BTC-USD",
        sequence_number=100,
        user_id="test-user",
        session_id="test-session",
        environment="test",
        error_timestamp_ms=int(datetime.now().timestamp() * 1000),
    )


@pytest.fixture
def mock_old_handler() -> AsyncMock:
    """Mock old APIError handler."""
    handler = AsyncMock()
    handler.handle_error = AsyncMock()
    handler.handle_validation_error = AsyncMock()
    return handler


@pytest.fixture
def mock_new_handler() -> AsyncMock:
    """Mock new WebSocketStreamError handler."""
    handler = AsyncMock()
    handler.handle_stream_error = AsyncMock()
    handler.handle_validation_error = AsyncMock()
    handler.handle_connection_error = AsyncMock()
    return handler


@pytest.fixture
def dual_manager(mock_old_handler, mock_new_handler) -> DualErrorManager:
    """Create dual error manager for testing."""
    return DualErrorManager(
        old_handler=mock_old_handler,
        new_handler=mock_new_handler,
        mode=ErrorSystemMode.DUAL_COMPARE,
        enable_comparison=True,
        log_differences=False,  # Disable logging in tests
    )


@pytest.fixture
def migration_tracker() -> WebSocketMigrationTracker:
    """Create migration tracker for testing."""
    return WebSocketMigrationTracker(
        state_file=None,  # Don't save state in tests
        auto_save=False,
    )


# ============================================================================
# Adapter Compatibility Tests
# ============================================================================


class TestWebSocketErrorAdapter:
    """Test compatibility adapter between error systems."""

    def test_adapter_converts_connection_error(self, test_context):
        """Test converting WebSocket connection error to APIError."""
        ws_error = WebSocketConnectionError(
            message="Connection lost",
            context=test_context,
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        assert isinstance(api_error, APIError)
        assert api_error.message == "Connection lost"
        assert api_error.code == APIErrorCode.CONNECTION_ERROR.value
        assert api_error.exchange_code == WebSocketErrorCode.CONNECTION_LOST.value
        assert api_error.metadata["error_domain"] == "websocket_stream"
        assert api_error.metadata["connection_id"] == "compat-test-conn"

    def test_adapter_converts_validation_error(self, test_context):
        """Test converting WebSocket validation error to APIError."""
        ws_error = WebSocketValidationError(
            message="Invalid field value",
            context=test_context,
            field="quantity",
            value=-1,
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        assert isinstance(api_error, APIError)
        assert api_error.code == APIErrorCode.INVALID_REQUEST.value
        assert api_error.metadata["ws_error_code"] == "VALIDATION_FAILED"

    def test_adapter_converts_rate_limit_error(self, test_context):
        """Test converting WebSocket rate limit error to APIError."""
        ws_error = WebSocketRateLimitError(
            context=test_context,
            retry_after_ms=5000,
            limit=100,
            window_ms=60000,
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        assert api_error.code == APIErrorCode.RATE_LIMITED.value
        assert api_error.retry_after == 5.0  # Converted to seconds

    def test_adapter_preserves_severity(self, test_context):
        """Test that adapter maps severity to HTTP status correctly."""
        # Test different severities
        severities_and_statuses = [
            (ErrorSeverity.INFO, 200),
            (ErrorSeverity.WARNING, 400),
            (ErrorSeverity.ERROR, 500),
            (ErrorSeverity.CRITICAL, 503),
        ]

        for severity, expected_status in severities_and_statuses:
            ws_error = WebSocketStreamError(
                message="Test error",
                code=WebSocketErrorCode.UNKNOWN_ERROR,
                context=test_context,
                severity=severity,
                recovery_strategy=WebSocketRecoveryStrategy.NONE,
            )

            api_error = WebSocketErrorAdapter.to_api_error(ws_error)
            assert api_error.http_status == expected_status

    def test_adapter_maps_recovery_strategy(self, test_context):
        """Test that adapter correctly maps recovery strategies."""
        # Retryable strategies
        retryable_strategies = [
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            WebSocketRecoveryStrategy.LINEAR_BACKOFF,
        ]

        for strategy in retryable_strategies:
            ws_error = WebSocketStreamError(
                message="Test error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=test_context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=strategy,
            )

            is_retryable = WebSocketErrorAdapter.is_retryable_ws_error(ws_error)
            assert is_retryable is True

        # Non-retryable strategies
        ws_error.recovery_strategy = WebSocketRecoveryStrategy.NONE
        is_retryable = WebSocketErrorAdapter.is_retryable_ws_error(ws_error)
        assert is_retryable is False

    def test_adapter_extracts_monitoring_data(self, test_context):
        """Test extracting monitoring data for legacy dashboards."""
        ws_error = WebSocketStreamError(
            message="Test monitoring",
            code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
            context=test_context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
        )

        monitoring_data = WebSocketErrorAdapter.get_legacy_monitoring_data(ws_error)

        assert monitoring_data["error_type"] == "websocket"
        assert monitoring_data["error_code"] == WebSocketErrorCode.SUBSCRIPTION_FAILED.value
        assert monitoring_data["severity"] == ErrorSeverity.ERROR.value
        assert monitoring_data["is_retryable"] is True
        assert monitoring_data["connection_id"] == "compat-test-conn"
        assert monitoring_data["exchange"] == "hyperliquid"

    def test_adapter_handles_error_chain(self, test_context):
        """Test that adapter handles error chains correctly."""
        # Add error chain to context
        test_context.add_to_error_chain(
            error_class="ConnectionError",
            error_message="Initial connection failed",
            error_code="CONN_001",
        )
        test_context.add_to_error_chain(
            error_class="TimeoutError",
            error_message="Connection timeout",
            error_code="TIMEOUT_001",
        )

        ws_error = WebSocketConnectionError(
            message="Connection failed after retries",
            context=test_context,
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        assert "error_chain" in api_error.metadata
        assert len(api_error.metadata["error_chain"]) == 2
        assert api_error.metadata["error_chain"][0]["error_class"] == "ConnectionError"


# ============================================================================
# Dual Error Manager Tests
# ============================================================================


class TestDualErrorManager:
    """Test dual error system manager."""

    async def test_dual_manager_old_only_mode(
        self,
        dual_manager,
        mock_old_handler,
        mock_new_handler,
        test_context,
    ):
        """Test dual manager in OLD_ONLY mode."""
        dual_manager.set_mode(ErrorSystemMode.OLD_ONLY)

        error = Exception("Test error")
        await dual_manager.handle_error_dual(error, test_context)

        mock_old_handler.handle_error.assert_called_once()
        mock_new_handler.handle_stream_error.assert_not_called()

    async def test_dual_manager_new_only_mode(
        self,
        dual_manager,
        mock_old_handler,
        mock_new_handler,
        test_context,
    ):
        """Test dual manager in NEW_ONLY mode."""
        dual_manager.set_mode(ErrorSystemMode.NEW_ONLY)

        error = WebSocketConnectionError(
            message="Test error",
            context=test_context,
        )
        await dual_manager.handle_error_dual(error, test_context)

        mock_new_handler.handle_stream_error.assert_called_once()
        mock_old_handler.handle_error.assert_not_called()

    async def test_dual_manager_dual_passive_mode(
        self,
        dual_manager,
        mock_old_handler,
        mock_new_handler,
        test_context,
    ):
        """Test dual manager in DUAL_PASSIVE mode."""
        dual_manager.set_mode(ErrorSystemMode.DUAL_PASSIVE)

        error = Exception("Test error")
        await dual_manager.handle_error_dual(error, test_context)

        # Old handler should be called immediately
        mock_old_handler.handle_error.assert_called_once()

        # New handler should be called in background (eventually)
        import asyncio

        await asyncio.sleep(0.1)  # Give background task time

    async def test_dual_manager_dual_compare_mode(
        self,
        dual_manager,
        mock_old_handler,
        mock_new_handler,
        test_context,
    ):
        """Test dual manager in DUAL_COMPARE mode."""
        dual_manager.set_mode(ErrorSystemMode.DUAL_COMPARE)

        # Configure mock returns for comparison
        mock_old_handler.handle_error.return_value = None
        mock_new_handler.handle_connection_error.return_value = None

        error = Exception("Test error")
        await dual_manager.handle_error_dual(error, test_context)

        # Both handlers should be called
        mock_old_handler.handle_error.assert_called_once()
        mock_new_handler.handle_connection_error.assert_called_once()

        # Check statistics
        stats = dual_manager.get_statistics()
        assert stats["both_systems_handled"] == 1

    async def test_dual_manager_validation_error_handling(
        self,
        dual_manager,
        mock_old_handler,
        mock_new_handler,
        test_context,
    ):
        """Test handling validation errors in dual manager."""
        dual_manager.set_mode(ErrorSystemMode.DUAL_COMPARE)

        class TestModel(BaseModel):
            field: str = Field(...)

        try:
            TestModel()
        except ValidationError as e:
            validation_error = e

        payload = TestModel(field="test")
        await dual_manager.handle_error_dual(validation_error, test_context, payload)

        mock_old_handler.handle_validation_error.assert_called_once()
        mock_new_handler.handle_validation_error.assert_called_once()

    def test_dual_manager_statistics(self, dual_manager):
        """Test dual manager statistics tracking."""
        stats = dual_manager.get_statistics()

        assert stats["mode"] == ErrorSystemMode.DUAL_COMPARE.value
        assert stats["total_errors_handled"] == 0
        assert stats["compatibility_rate"] == "100.00%"
        assert "uptime_hours" in stats

    async def test_dual_manager_mode_recommendation(self, dual_manager):
        """Test dual manager mode change recommendations."""
        # Not enough data initially
        recommendation = await dual_manager.recommend_mode_change()
        assert recommendation is None

        # Simulate successful handling
        dual_manager.statistics.total_errors_handled = 200
        dual_manager.statistics.new_system_handled = 200
        dual_manager.statistics.compatibility_failures = 2  # 99% compatibility

        dual_manager.set_mode(ErrorSystemMode.DUAL_PASSIVE)
        recommendation = await dual_manager.recommend_mode_change()
        # Should recommend DUAL_ACTIVE when compatibility is high
        assert recommendation == ErrorSystemMode.DUAL_ACTIVE


# ============================================================================
# Migration Tracker Tests
# ============================================================================


class TestMigrationTracker:
    """Test migration progress tracker."""

    def test_tracker_initialization(self, migration_tracker):
        """Test tracker initializes with correct components."""
        assert len(migration_tracker.components) > 0
        assert "ws_processor" in migration_tracker.components
        assert "ws_router" in migration_tracker.components
        assert migration_tracker.current_phase == MigrationPhase.PLANNING

    def test_tracker_component_update(self, migration_tracker):
        """Test updating component status."""
        migration_tracker.update_component(
            "ws_processor",
            status=ComponentStatus.IN_PROGRESS,
            error_mode=ErrorSystemMode.DUAL_PASSIVE,
            notes="Started migration",
        )

        component = migration_tracker.components["ws_processor"]
        assert component.status == ComponentStatus.IN_PROGRESS
        assert component.started_at is not None
        assert component.notes == "Started migration"

    def test_tracker_phase_progression(self, migration_tracker):
        """Test phase progression tracking."""
        migration_tracker.set_phase(MigrationPhase.FOUNDATION)
        assert migration_tracker.current_phase == MigrationPhase.FOUNDATION

        # Should create checkpoint
        assert len(migration_tracker.checkpoints) == 1

    def test_tracker_checkpoint_creation(self, migration_tracker):
        """Test checkpoint creation with statistics."""
        # Mark some components as migrated
        migration_tracker.update_component(
            "ws_error_handler",
            status=ComponentStatus.MIGRATED,
            compatibility_rate=98.5,
            errors_handled=100,
        )
        migration_tracker.update_component(
            "ws_error_recovery",
            status=ComponentStatus.MIGRATED,
            compatibility_rate=99.0,
            errors_handled=50,
        )

        checkpoint = migration_tracker.create_checkpoint("Test checkpoint")

        assert checkpoint.components_total > 0
        assert checkpoint.components_migrated == 2
        assert checkpoint.overall_compatibility > 98.0
        assert checkpoint.notes == "Test checkpoint"

    def test_tracker_status_summary(self, migration_tracker):
        """Test getting status summary."""
        # Update some components
        migration_tracker.update_component(
            "ws_processor",
            status=ComponentStatus.IN_PROGRESS,
        )
        migration_tracker.update_component(
            "ws_router",
            status=ComponentStatus.MIGRATED,
        )

        summary = migration_tracker.get_status_summary()

        assert summary["current_phase"] == MigrationPhase.PLANNING.value
        assert "overall_progress" in summary
        assert summary["components"]["total"] > 0
        assert len(summary["active_components"]) == 1
        assert "ws_processor" in summary["active_components"]

    def test_tracker_component_report(self, migration_tracker):
        """Test getting detailed component report."""
        migration_tracker.update_component(
            "ws_processor",
            status=ComponentStatus.TESTING,
            error_mode=ErrorSystemMode.DUAL_ACTIVE,
            compatibility_rate=97.5,
            errors_handled=250,
            issue="Minor compatibility issue with legacy format",
        )

        report = migration_tracker.get_component_report("ws_processor")

        assert report is not None
        assert report["status"] == ComponentStatus.TESTING.value
        assert report["compatibility_rate"] == "97.50%"
        assert report["errors_handled"] == 250
        assert len(report["issues"]) == 1

    def test_tracker_phase_report(self, migration_tracker):
        """Test getting phase-specific report."""
        migration_tracker.set_phase(MigrationPhase.FOUNDATION)

        # Mark foundation components
        migration_tracker.update_component(
            "ws_error_handler",
            status=ComponentStatus.MIGRATED,
        )

        report = migration_tracker.get_phase_report()

        assert report["phase"] == MigrationPhase.FOUNDATION.value
        assert report["components"]["completed"] == 1
        assert "ws_error_handler" in report["component_list"]

    def test_tracker_recommendation(self, migration_tracker):
        """Test component migration recommendation."""
        # First recommendation should be ws_error_handler
        recommendation = migration_tracker.recommend_next_component()
        assert recommendation == "ws_error_handler"

        # Mark it as migrated
        migration_tracker.update_component(
            "ws_error_handler",
            status=ComponentStatus.MIGRATED,
        )

        # Next should be ws_error_recovery
        recommendation = migration_tracker.recommend_next_component()
        assert recommendation == "ws_error_recovery"

    def test_tracker_state_persistence(self, migration_tracker, tmp_path):
        """Test saving and loading tracker state."""
        state_file = tmp_path / "migration_state.json"

        # Update tracker state
        migration_tracker.set_phase(MigrationPhase.INTEGRATION)
        migration_tracker.update_component(
            "ws_processor",
            status=ComponentStatus.MIGRATED,
            compatibility_rate=99.5,
        )
        migration_tracker.create_checkpoint("Test save")

        # Save state
        migration_tracker.save_state(state_file)

        # Create new tracker and load state
        new_tracker = WebSocketMigrationTracker()
        new_tracker.load_state(state_file)

        assert new_tracker.current_phase == MigrationPhase.INTEGRATION
        assert new_tracker.components["ws_processor"].status == ComponentStatus.MIGRATED
        assert new_tracker.components["ws_processor"].compatibility_rate == 99.5
        assert len(new_tracker.checkpoints) == 1


# ============================================================================
# End-to-End Compatibility Tests
# ============================================================================


class TestEndToEndCompatibility:
    """Test end-to-end compatibility scenarios."""

    async def test_progressive_migration_workflow(
        self,
        dual_manager,
        migration_tracker,
        test_context,
    ):
        """Test progressive migration from old to new system."""
        # Start in DUAL_PASSIVE mode
        dual_manager.set_mode(ErrorSystemMode.DUAL_PASSIVE)
        migration_tracker.set_phase(MigrationPhase.FOUNDATION)

        # Simulate handling errors
        for i in range(10):
            error = WebSocketConnectionError(
                message=f"Test error {i}",
                context=test_context,
            )
            await dual_manager.handle_error_dual(error, test_context)

        # Check statistics
        stats = dual_manager.get_statistics()
        assert stats["total_errors_handled"] == 10

        # Update migration tracker
        migration_tracker.update_component(
            "ws_error_handler",
            status=ComponentStatus.TESTING,
            errors_handled=10,
            compatibility_rate=100.0,
        )

        # Progress to DUAL_ACTIVE
        dual_manager.set_mode(ErrorSystemMode.DUAL_ACTIVE)
        migration_tracker.update_component(
            "ws_error_handler",
            status=ComponentStatus.MIGRATED,
        )

        # Verify migration progress
        summary = migration_tracker.get_status_summary()
        assert summary["components"]["migrated"] >= 1

    async def test_error_compatibility_across_types(
        self,
        test_context,
    ):
        """Test that all error types can be converted."""
        error_types = [
            WebSocketConnectionError(
                message="Connection error",
                context=test_context,
            ),
            WebSocketValidationError(
                message="Validation error",
                context=test_context,
                field="test",
                value=123,
            ),
            WebSocketSubscriptionError(
                message="Subscription error",
                context=test_context,
                channel="test",
            ),
            WebSocketAuthenticationError(
                message="Auth error",
                context=test_context,
            ),
            WebSocketRateLimitError(
                context=test_context,
                retry_after_ms=1000,
            ),
        ]

        for ws_error in error_types:
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)
            assert isinstance(api_error, APIError)
            assert api_error.message
            assert api_error.code
            assert api_error.metadata["error_domain"] == "websocket_stream"

    def test_circuit_breaker_compatibility(self, test_context):
        """Test circuit breaker trigger compatibility."""
        # Create error that should trigger circuit breaker
        ws_error = WebSocketStreamError(
            message="Critical error",
            code=WebSocketErrorCode.SECURITY_VIOLATION,
            context=test_context,
            severity=ErrorSeverity.CRITICAL,
            recovery_strategy=WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
        )

        should_break = WebSocketErrorAdapter.should_circuit_break(ws_error)
        assert should_break is True

        # Create error that shouldn't trigger
        ws_error = WebSocketStreamError(
            message="Minor error",
            code=WebSocketErrorCode.MESSAGE_PARSING_ERROR,
            context=test_context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.IGNORE,
        )

        should_break = WebSocketErrorAdapter.should_circuit_break(ws_error)
        assert should_break is False

    def test_alert_level_mapping(self, test_context):
        """Test alert level mapping for monitoring."""
        severity_to_alert = [
            (ErrorSeverity.INFO, "info"),
            (ErrorSeverity.WARNING, "warning"),
            (ErrorSeverity.ERROR, "error"),
            (ErrorSeverity.CRITICAL, "critical"),
        ]

        for severity, expected_alert in severity_to_alert:
            ws_error = WebSocketStreamError(
                message="Test",
                code=WebSocketErrorCode.UNKNOWN_ERROR,
                context=test_context,
                severity=severity,
                recovery_strategy=WebSocketRecoveryStrategy.NONE,
            )

            alert_level = WebSocketErrorAdapter.get_alert_level(ws_error)
            assert alert_level == expected_alert
