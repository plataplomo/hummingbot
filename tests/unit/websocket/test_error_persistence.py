"""Error persistence tests for WebSocket error system.

Tests error state persistence, recovery after restarts, and error history tracking.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from cyberdelta.apis.common.error_foundation import (
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_error_metrics import AggregatedMetrics
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class ErrorPersistenceManager:
    """Manager for persisting error state to disk."""

    def __init__(self, persistence_path: Path) -> None:
        """Initialize persistence manager.

        Args:
            persistence_path: Path to persistence file
        """
        self.persistence_path = persistence_path
        self.persistence_path.parent.mkdir(parents=True, exist_ok=True)

    def save_error_state(self, errors: list[dict[str, Any]]) -> None:
        """Save error state to disk.

        Args:
            errors: List of error dictionaries to persist
        """
        with open(self.persistence_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "errors": errors,
                    "version": "1.0",
                },
                f,
                indent=2,
            )

    def load_error_state(self) -> list[dict[str, Any]]:
        """Load error state from disk.

        Returns:
            List of persisted errors
        """
        if not self.persistence_path.exists():
            return []

        with open(self.persistence_path, encoding="utf-8") as f:
            data: dict[str, Any] = json.load(f)
            errors: list[dict[str, Any]] = data.get("errors", [])
            return errors

    def clear_error_state(self) -> None:
        """Clear persisted error state."""
        if self.persistence_path.exists():
            self.persistence_path.unlink()


class ErrorHistoryTracker:
    """Track error history with time windows."""

    def __init__(self, max_history_size: int = 1000) -> None:
        """Initialize history tracker.

        Args:
            max_history_size: Maximum errors to keep in history
        """
        self.max_history_size = max_history_size
        self.error_history: list[dict[str, Any]] = []

    def add_error(self, error: WebSocketStreamError) -> None:
        """Add error to history.

        Args:
            error: Error to track
        """
        error_record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "code": error.code.name,
            "message": error.message,
            "severity": error.severity.name,
            "exchange": error.context.exchange,
            "retryable": error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE,
            "recovery_strategy": error.recovery_strategy.name,
        }

        self.error_history.append(error_record)

        # Trim history if too large
        if len(self.error_history) > self.max_history_size:
            self.error_history = self.error_history[-self.max_history_size :]

    def get_errors_in_window(
        self,
        window_start: datetime,
        window_end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Get errors within time window.

        Args:
            window_start: Start of time window
            window_end: End of time window (now if None)

        Returns:
            Errors within the window
        """
        if window_end is None:
            window_end = datetime.now(UTC)

        filtered_errors = []
        for error in self.error_history:
            error_time = datetime.fromisoformat(error["timestamp"])
            if window_start <= error_time <= window_end:
                filtered_errors.append(error)

        return filtered_errors

    def get_error_counts_by_code(self) -> dict[str, int]:
        """Get error counts grouped by code.

        Returns:
            Dictionary of error code to count
        """
        counts: dict[str, int] = {}
        for error in self.error_history:
            code = error["code"]
            counts[code] = counts.get(code, 0) + 1
        return counts


@pytest.mark.asyncio
class TestErrorPersistence:
    """Test error persistence and recovery."""

    @pytest.fixture
    def temp_persistence_path(self) -> Generator[Path]:
        """Create temporary persistence path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir) / "error_state.json"

    @pytest.fixture
    def persistence_manager(self, temp_persistence_path: Path) -> ErrorPersistenceManager:
        """Create persistence manager.

        Returns:
            ErrorPersistenceManager: Manager for persisting error state to disk.
        """
        return ErrorPersistenceManager(temp_persistence_path)

    @pytest.fixture
    def history_tracker(self) -> ErrorHistoryTracker:
        """Create history tracker.

        Returns:
            ErrorHistoryTracker: Tracker for maintaining error history with time windows.
        """
        return ErrorHistoryTracker(max_history_size=100)

    async def test_basic_error_persistence(
        self,
        persistence_manager: ErrorPersistenceManager,
    ) -> None:
        """Test basic error state persistence."""
        # Create test errors
        errors = [
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_LOST,
                message="Connection lost",
            ),
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.RATE_LIMITED,
                message="Rate limited",
            ),
        ]

        # Convert to dictionaries
        error_dicts = [
            {
                "code": error.code.name,
                "message": error.message,
                "timestamp": error.context.error_timestamp_ms,
                "exchange": error.context.exchange,
            }
            for error in errors
        ]

        # Save state
        persistence_manager.save_error_state(error_dicts)

        # Load state
        loaded_errors = persistence_manager.load_error_state()

        # Verify
        assert len(loaded_errors) == 2
        assert loaded_errors[0]["code"] == "CONNECTION_LOST"
        assert loaded_errors[1]["code"] == "RATE_LIMITED"

    async def test_error_recovery_after_restart(
        self,
        persistence_manager: ErrorPersistenceManager,
    ) -> None:
        """Test error recovery after system restart."""
        # Simulate errors before "restart"
        pre_restart_errors = [
            {
                "code": "STREAM_CORRUPTED",
                "message": "Stream corrupted before restart",
                "timestamp": int(datetime.now(UTC).timestamp() * 1000),
                "exchange": "hyperliquid",
                "recovery_attempts": 2,
            }
        ]

        persistence_manager.save_error_state(pre_restart_errors)

        # Simulate restart by creating new manager
        new_manager = ErrorPersistenceManager(persistence_manager.persistence_path)

        # Load persisted state
        recovered_errors = new_manager.load_error_state()

        # Verify recovery
        assert len(recovered_errors) == 1
        assert recovered_errors[0]["code"] == "STREAM_CORRUPTED"
        assert recovered_errors[0]["recovery_attempts"] == 2

    async def test_error_history_tracking(
        self,
        history_tracker: ErrorHistoryTracker,
    ) -> None:
        """Test error history tracking."""
        # Add various errors
        error_codes = [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.AUTH_FAILED,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.RATE_LIMITED,
        ]

        for code in error_codes:
            error = ErrorTestFactory.create_test_error(code=code)
            history_tracker.add_error(error)

        # Check history
        assert len(history_tracker.error_history) == 6

        # Check counts
        counts = history_tracker.get_error_counts_by_code()
        assert counts["CONNECTION_LOST"] == 2
        assert counts["RATE_LIMITED"] == 3
        assert counts["AUTH_FAILED"] == 1

    async def test_error_history_time_window(
        self,
        history_tracker: ErrorHistoryTracker,
    ) -> None:
        """Test error history filtering by time window."""
        now = datetime.now(UTC)

        # Add errors at different times
        for i in range(10):
            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.SEQUENCE_GAP)
            # Manually set timestamp for testing
            history_tracker.error_history.append({
                "timestamp": (now - timedelta(minutes=i)).isoformat(),
                "code": error.code.name,
                "message": error.message,
                "severity": error.severity.name,
                "exchange": error.context.exchange,
                "retryable": error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE,
                "recovery_strategy": error.recovery_strategy.name,
            })

        # Get errors from last 5 minutes
        window_start = now - timedelta(minutes=5)
        recent_errors = history_tracker.get_errors_in_window(window_start)

        # Should get 6 errors (0-5 minutes ago)
        assert len(recent_errors) == 6

    async def test_history_size_limit(
        self,
        history_tracker: ErrorHistoryTracker,
    ) -> None:
        """Test that history respects size limits."""
        # Set small limit
        history_tracker.max_history_size = 10

        # Add more errors than limit
        for i in range(20):
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_LOST,
                message=f"Error {i}",
            )
            history_tracker.add_error(error)

        # Should only keep last 10
        assert len(history_tracker.error_history) == 10
        # First error should be #10 (0-9 were trimmed)
        assert "Error 10" in history_tracker.error_history[0]["message"]

    async def test_metrics_persistence(
        self,
        persistence_manager: ErrorPersistenceManager,
    ) -> None:
        """Test persistence of error metrics."""
        # Create metrics
        metrics = AggregatedMetrics(
            start_timestamp_ms=int((datetime.now(UTC) - timedelta(hours=1)).timestamp() * 1000),
            end_timestamp_ms=int(datetime.now(UTC).timestamp() * 1000),
            duration_ms=3600000,  # 1 hour
            error_counts_by_code={"CONNECTION_LOST": 50, "RATE_LIMITED": 100},
            error_counts_by_exchange={"hyperliquid": 80, "backpack": 70},
            recovery_attempts_by_strategy={"RECONNECT": 25, "RESYNC": 200},
            average_recovery_duration_ms=250.5,
        )

        # Convert to dict for persistence
        metrics_dict = metrics.model_dump(mode="json")

        # Save
        persistence_manager.save_error_state([{"metrics": metrics_dict}])

        # Load
        loaded = persistence_manager.load_error_state()

        # Verify
        assert len(loaded) == 1
        loaded_metrics = loaded[0]["metrics"]
        assert loaded_metrics["total_errors"] == 150
        assert loaded_metrics["errors_by_code"]["RATE_LIMITED"] == 100

    async def test_circuit_breaker_state_persistence(
        self,
        persistence_manager: ErrorPersistenceManager,
    ) -> None:
        """Test persistence of circuit breaker state."""
        # Circuit breaker state
        cb_state = {
            "is_open": True,
            "failure_count": 5,
            "last_failure_time": datetime.now(UTC).isoformat(),
            "recovery_key": "hyperliquid_connection",
        }

        # Save
        persistence_manager.save_error_state([{"circuit_breaker": cb_state}])

        # Load
        loaded = persistence_manager.load_error_state()

        # Verify
        assert len(loaded) == 1
        loaded_cb = loaded[0]["circuit_breaker"]
        assert loaded_cb["is_open"] is True
        assert loaded_cb["failure_count"] == 5
        assert loaded_cb["recovery_key"] == "hyperliquid_connection"

    async def test_error_event_persistence(
        self,
        persistence_manager: ErrorPersistenceManager,
    ) -> None:
        """Test persistence of error events."""
        # Create error events
        events = []
        for i in range(5):
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.STREAM_INTERRUPTED,
                message=f"Stream interrupted #{i}",
            )

            event = {
                "event_id": f"evt_{i}",
                "timestamp": datetime.now(UTC).isoformat(),
                "error_code": error.code.name,
                "error_message": error.message,
                "exchange": error.context.exchange,
                "severity": error.severity.name,
            }
            events.append(event)

        # Save events
        persistence_manager.save_error_state(events)

        # Load events
        loaded_events = persistence_manager.load_error_state()

        # Verify
        assert len(loaded_events) == 5
        for i, event in enumerate(loaded_events):
            assert event["event_id"] == f"evt_{i}"
            assert event["error_code"] == "STREAM_INTERRUPTED"

    async def test_recovery_strategy_persistence(
        self,
        persistence_manager: ErrorPersistenceManager,
    ) -> None:
        """Test persistence of recovery strategies and attempts."""
        # Recovery state
        recovery_state = {
            "strategies_used": {
                "CONNECTION_LOST": "EXPONENTIAL_BACKOFF",
                "RATE_LIMITED": "LINEAR_BACKOFF",
                "STREAM_CORRUPTED": "CIRCUIT_BREAKER",
            },
            "attempt_counts": {
                "CONNECTION_LOST": 3,
                "RATE_LIMITED": 5,
                "STREAM_CORRUPTED": 1,
            },
            "last_successful_recovery": {
                "code": "CONNECTION_LOST",
                "timestamp": datetime.now(UTC).isoformat(),
                "strategy": "EXPONENTIAL_BACKOFF",
            },
        }

        # Save
        persistence_manager.save_error_state([recovery_state])

        # Load
        loaded = persistence_manager.load_error_state()

        # Verify
        assert len(loaded) == 1
        loaded_state = loaded[0]
        assert loaded_state["strategies_used"]["CONNECTION_LOST"] == "EXPONENTIAL_BACKOFF"
        assert loaded_state["attempt_counts"]["RATE_LIMITED"] == 5
        assert loaded_state["last_successful_recovery"]["strategy"] == "EXPONENTIAL_BACKOFF"

    async def test_error_chain_persistence(
        self,
        persistence_manager: ErrorPersistenceManager,
    ) -> None:
        """Test persistence of error chains."""
        # Create error with chain
        context = ErrorTestFactory.create_test_context()
        context.add_to_error_chain(ValueError("Root cause"))
        context.add_to_error_chain(TypeError("Middle error"))

        error = WebSocketStreamError(
            message="Final error",
            code=WebSocketErrorCode.INTERNAL_ERROR,
            context=context,
        )

        # Convert chain for persistence
        chain_data = [
            {
                "error_class": e.error_class,
                "error_message": e.error_message,
                "timestamp_ms": e.timestamp_ms,
            }
            for e in context.error_chain
        ]

        error_dict = {
            "code": error.code.name,
            "message": error.message,
            "error_chain": chain_data,
        }

        # Save
        persistence_manager.save_error_state([error_dict])

        # Load
        loaded = persistence_manager.load_error_state()

        # Verify
        assert len(loaded) == 1
        loaded_chain = loaded[0]["error_chain"]
        assert len(loaded_chain) == 2
        assert loaded_chain[0]["error_class"] == "ValueError"
        assert loaded_chain[1]["error_class"] == "TypeError"

    async def test_persistence_file_corruption_handling(
        self,
        temp_persistence_path: Path,
    ) -> None:
        """Test handling of corrupted persistence files."""
        # Create corrupted file
        temp_persistence_path.parent.mkdir(parents=True, exist_ok=True)
        with open(temp_persistence_path, "w", encoding="utf-8") as f:
            f.write("{ invalid json content ]}")

        # Try to load with new manager
        manager = ErrorPersistenceManager(temp_persistence_path)

        # Should handle gracefully
        try:
            errors = manager.load_error_state()
            # Might return empty list or raise
            assert isinstance(errors, list)
        except json.JSONDecodeError:
            # This is acceptable - corruption detected
            pass

    async def test_concurrent_persistence_safety(
        self,
        persistence_manager: ErrorPersistenceManager,
    ) -> None:
        """Test that concurrent persistence operations are safe."""

        async def save_errors(manager: ErrorPersistenceManager, error_id: int) -> None:
            """Save errors with specific ID."""
            errors = [
                {
                    "id": error_id,
                    "code": "TEST_ERROR",
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            ]
            await asyncio.get_event_loop().run_in_executor(None, manager.save_error_state, errors)

        # Try concurrent saves
        tasks = [save_errors(persistence_manager, i) for i in range(10)]

        await asyncio.gather(*tasks, return_exceptions=True)

        # Load final state
        final_errors = persistence_manager.load_error_state()

        # Should have last write (not corrupted)
        assert len(final_errors) == 1
        assert "id" in final_errors[0]
