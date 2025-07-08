"""Thread-safe execution state management for ExecutionHandler refactoring.

This module provides centralized, thread-safe management of execution lifecycle
with proper state transitions, history tracking, and cleanup operations.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger
from cyberdelta.core.models.execution import ExecutionStatus, TradeExecution
from cyberdelta.core.services.interfaces import (
    BaseAsyncService,
    IStateManager,
    StateManagerConfig,
)


if TYPE_CHECKING:
    from cyberdelta.core.risk_manager import SizedOpportunity


class ThreadSafeExecutionStateManager(BaseAsyncService, IStateManager):
    """Thread-safe execution state manager with lifecycle management."""

    def __init__(
        self, config: StateManagerConfig | None = None, logger: TraceLevelLogger | None = None
    ) -> None:
        """Initialize execution state manager.

        Args:
            config: State manager configuration
            logger: Optional logger instance
        """
        super().__init__(logger)
        self.config = config or StateManagerConfig()
        self.logger = logger or get_logger(__name__)

        # Thread-safe storage
        self._active_executions: dict[str, TradeExecution] = {}
        self._execution_history: list[TradeExecution] = []
        self._execution_lock = asyncio.Lock()
        # Additional synchronous lock for fallback scenarios
        self._sync_lock = threading.Lock()

        # Cleanup task
        self._cleanup_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the state manager and background cleanup task."""
        await super().start()

        # Start cleanup task
        self._cleanup_task = self._create_background_task(self._periodic_cleanup())

        self.logger.info(
            "State manager started",
            cleanup_interval=self.config.cleanup_interval_seconds,
            max_history=self.config.max_execution_history,
        )

    async def stop(self) -> None:
        """Stop the state manager and cleanup resources."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()

        await super().stop()

        # Clear state (optional, for clean shutdown)
        async with self._execution_lock:
            active_count = len(self._active_executions)
            history_count = len(self._execution_history)

            self._active_executions.clear()
            self._execution_history.clear()

        self.logger.info(
            "State manager stopped",
            final_active_count=active_count,
            final_history_count=history_count,
        )

    async def create_execution(self, opportunity: SizedOpportunity) -> TradeExecution:
        """Create new execution with proper initialization.

        Args:
            opportunity: Sized arbitrage opportunity

        Returns:
            New TradeExecution instance
        """
        # Generate unique execution ID
        execution_id = str(uuid.uuid4())

        # Create new execution
        execution = TradeExecution(opportunity=opportunity)
        # Override the auto-generated ID if needed
        if execution_id:
            execution.id = execution_id

        # Store in active executions
        async with self._execution_lock:
            self._active_executions[execution_id] = execution

        self.logger.info(
            "Execution created",
            execution_id=execution_id,
            symbol=opportunity.opportunity.symbol,
            long_exchange=opportunity.opportunity.long_exchange,
            short_exchange=opportunity.opportunity.short_exchange,
            total_active=len(self._active_executions),
        )

        return execution

    async def update_execution_status(
        self, execution_id: str, status: ExecutionStatus, error_message: str | None = None
    ) -> bool:
        """Update execution status in a thread-safe manner.

        Args:
            execution_id: Execution identifier
            status: New execution status
            error_message: Optional error message

        Returns:
            True if update was successful
        """
        async with self._execution_lock:
            execution = self._active_executions.get(execution_id)
            if not execution:
                self.logger.warning(
                    "Attempted to update non-existent execution",
                    execution_id=execution_id,
                    requested_status=status,
                )
                return False

            # Validate state transition
            if not self._is_valid_transition(execution.status, status):
                self.logger.warning(
                    "Invalid state transition attempted",
                    execution_id=execution_id,
                    current_status=execution.status,
                    requested_status=status,
                )
                return False

            # Update execution
            old_status = execution.status
            execution.status = status
            # Updated time is tracked implicitly

            if error_message:
                execution.error_message = error_message

            self.logger.info(
                "Execution status updated",
                execution_id=execution_id,
                old_status=old_status,
                new_status=status,
                error_message=error_message,
            )

            return True

    async def get_execution(self, execution_id: str) -> TradeExecution | None:
        """Get execution by ID.

        Args:
            execution_id: Execution identifier

        Returns:
            TradeExecution if found, None otherwise
        """
        async with self._execution_lock:
            return self._active_executions.get(execution_id)

    async def finalize_execution(self, execution_id: str) -> TradeExecution | None:
        """Move execution from active to history.

        Args:
            execution_id: Execution identifier

        Returns:
            Finalized TradeExecution if successful
        """
        async with self._execution_lock:
            execution = self._active_executions.pop(execution_id, None)
            if not execution:
                self.logger.warning(
                    "Attempted to finalize non-existent execution", execution_id=execution_id
                )
                return None

            # Update finalization timestamp
            # Finalized time is tracked implicitly

            # Add to history
            self._execution_history.append(execution)

            # Maintain history size limit
            if len(self._execution_history) > self.config.max_execution_history:
                removed = self._execution_history.pop(0)
                self.logger.debug(
                    "Removed old execution from history",
                    removed_execution_id=removed.id,
                    history_size=len(self._execution_history),
                )

            self.logger.info(
                "Execution finalized",
                execution_id=execution_id,
                final_status=execution.status,
                total_active=len(self._active_executions),
                history_size=len(self._execution_history),
            )

            return execution

    def get_active_executions(self) -> list[TradeExecution]:
        """Get all active executions.

        Returns:
            List of active TradeExecution instances
        """
        # Note: This method is synchronous to match the interface
        # We use run_coroutine_threadsafe to safely access the async version
        try:
            # Get the event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're in an async context, we need to schedule the coroutine
                future = asyncio.run_coroutine_threadsafe(self.get_active_executions_async(), loop)
                # Wait for result with a reasonable timeout
                return future.result(timeout=1.0)
            # If no loop is running, we can run it directly
            return asyncio.run(self.get_active_executions_async())
        except (RuntimeError, TimeoutError) as e:
            self.logger.warning(
                "Failed to get active executions with async lock, using sync fallback",
                error=str(e),
                error_type=type(e).__name__,
            )
            # Fallback: Use threading.Lock for thread-safe access
            # This ensures we don't have race conditions even in the fallback case
            with self._sync_lock:
                return list(self._active_executions.values())

    async def get_active_executions_async(self) -> list[TradeExecution]:
        """Get all active executions (async version).

        Returns:
            List of active TradeExecution instances
        """
        async with self._execution_lock:
            return list(self._active_executions.values())

    async def get_execution_history(self) -> list[TradeExecution]:
        """Get execution history.

        Returns:
            List of historical TradeExecution instances
        """
        async with self._execution_lock:
            return list(self._execution_history)

    async def cleanup_old_executions(self) -> int:
        """Clean up old executions from history.

        Returns:
            Number of executions removed
        """
        if self.config.max_execution_age_hours <= 0:
            return 0

        current_time = time.time()
        age_threshold = self.config.max_execution_age_hours * 3600
        removed_count = 0

        async with self._execution_lock:
            # Filter out old executions
            original_count = len(self._execution_history)
            self._execution_history = [
                execution
                for execution in self._execution_history
                # Check if execution is still active (simplified age check)
                if (
                    execution.end_time is None
                    or current_time - execution.end_time.timestamp() < age_threshold
                )
            ]
            removed_count = original_count - len(self._execution_history)

        if removed_count > 0:
            self.logger.info(
                "Cleaned up old executions",
                removed_count=removed_count,
                remaining_count=len(self._execution_history),
                age_threshold_hours=self.config.max_execution_age_hours,
            )

        return removed_count

    def _is_valid_transition(
        self, current_status: ExecutionStatus, new_status: ExecutionStatus
    ) -> bool:
        """Validate if a status transition is allowed.

        Args:
            current_status: Current execution status
            new_status: Requested new status

        Returns:
            True if transition is valid
        """
        # Define valid state transitions
        valid_transitions: dict[ExecutionStatus, set[ExecutionStatus]] = {
            ExecutionStatus.PENDING: {
                ExecutionStatus.EXECUTING,
                ExecutionStatus.FAILED,
                ExecutionStatus.REJECTED,
            },
            ExecutionStatus.EXECUTING: {
                ExecutionStatus.PARTIALLY_COMPLETED,
                ExecutionStatus.COMPENSATING,
                ExecutionStatus.COMPLETED,
                ExecutionStatus.FAILED,
            },
            ExecutionStatus.PARTIALLY_COMPLETED: {
                ExecutionStatus.COMPENSATING,
                ExecutionStatus.COMPLETED,
                ExecutionStatus.FAILED,
            },
            ExecutionStatus.COMPENSATING: {
                ExecutionStatus.PARTIALLY_COMPLETED,
                ExecutionStatus.FAILED,
            },
            # Terminal states (no transitions allowed)
            ExecutionStatus.COMPLETED: set[ExecutionStatus](),
            ExecutionStatus.FAILED: set[ExecutionStatus](),
            ExecutionStatus.REJECTED: set[ExecutionStatus](),
        }

        # Allow same-status "updates" (for timestamp refresh)
        if current_status == new_status:
            return True

        # Check if transition is in valid set
        allowed_transitions = valid_transitions.get(current_status, set[ExecutionStatus]())
        return new_status in allowed_transitions

    async def _periodic_cleanup(self) -> None:
        """Background task for periodic cleanup of old executions."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.cleanup_interval_seconds)

                if self._shutdown_event.is_set():
                    break

                # Perform cleanup
                removed_count = await self.cleanup_old_executions()

                if removed_count > 0:
                    self.logger.debug("Periodic cleanup completed", removed_count=removed_count)

            except asyncio.CancelledError:
                self.logger.info("Cleanup task cancelled")
                break
            except Exception as e:
                self.logger.exception("Error in periodic cleanup", error=str(e))
                # Continue running despite errors
                await asyncio.sleep(60)  # Wait before retrying

    async def get_stats(self) -> dict[str, Any]:
        """Get state manager statistics.

        Returns:
            Dictionary with current statistics
        """
        async with self._execution_lock:
            active_executions = list(self._active_executions.values())
            history_executions = list(self._execution_history)

        # Calculate status distribution for active executions
        status_counts: dict[str, int] = {}
        for execution in active_executions:
            status = execution.status.name
            status_counts[status] = status_counts.get(status, 0) + 1

        # Calculate average execution time for completed executions
        completed_times: list[float] = []
        for execution in history_executions:
            if execution.end_time and execution.start_time:
                duration = (execution.end_time - execution.start_time).total_seconds()
                completed_times.append(duration)

        avg_execution_time = sum(completed_times) / len(completed_times) if completed_times else 0

        return {
            "active_executions": len(active_executions),
            "history_executions": len(history_executions),
            "status_distribution": status_counts,
            "average_execution_time_seconds": avg_execution_time,
            "max_history_size": self.config.max_execution_history,
            "cleanup_interval_seconds": self.config.cleanup_interval_seconds,
            "max_execution_age_hours": self.config.max_execution_age_hours,
        }
