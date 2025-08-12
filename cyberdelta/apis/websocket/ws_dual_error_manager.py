"""Dual error system manager for migration period.

Manages both old APIError-based and new WebSocketStreamError systems
during the migration period, allowing for comparison and validation
of both systems side-by-side.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Protocol

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.websocket.ws_error_handler_factory import WebSocketErrorHandlerFactory
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.enums import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler


logger = logging.getLogger(__name__)


# Migration statistics constants  
MIN_ERRORS_FOR_RECOMMENDATION = 100
COMPATIBILITY_RATE_THRESHOLD_95 = 95
COMPATIBILITY_RATE_THRESHOLD_99 = 99
NEW_FAILURE_RATE_THRESHOLD_1 = 1
NEW_FAILURE_RATE_THRESHOLD_01 = 0.1


class LegacyErrorHandler(Protocol):
    """Protocol for legacy APIError-based error handlers."""

    async def handle_validation_error(
        self, 
        error: ValidationError,
        context: WebSocketContextProtocol,
        payload: BaseModel,
    ) -> None:
        """Handle validation error with legacy system."""
        ...

    async def handle_error(self, error: Exception) -> None:
        """Handle general error with legacy system."""
        ...


class ErrorSystemMode(Enum):
    """Mode for error system operation."""

    OLD_ONLY = "old_only"  # Use only old APIError system
    NEW_ONLY = "new_only"  # Use only new WebSocketStreamError system
    DUAL_PASSIVE = "dual_passive"  # Use old system, log new system
    DUAL_ACTIVE = "dual_active"  # Use new system, fallback to old
    DUAL_COMPARE = "dual_compare"  # Use both and compare results


@dataclass
class ComparisonResult:
    """Result of comparing old and new error systems."""

    timestamp: datetime = field(default_factory=datetime.now)
    error_type: str = ""
    old_system_result: dict[str, object] = field(default_factory=lambda: dict[str, object]())
    new_system_result: dict[str, object] = field(default_factory=lambda: dict[str, object]())
    differences: list[str] = field(default_factory=lambda: list[str]())
    is_compatible: bool = True

    def add_difference(self, diff: str) -> None:
        """Add a difference found between systems."""
        self.differences.append(diff)
        self.is_compatible = False


@dataclass
class MigrationStatistics:
    """Statistics for dual error system operation."""

    total_errors_handled: int = 0
    old_system_handled: int = 0
    new_system_handled: int = 0
    both_systems_handled: int = 0
    compatibility_failures: int = 0
    old_system_failures: int = 0
    new_system_failures: int = 0
    comparison_results: list[ComparisonResult] = field(default_factory=lambda: list[ComparisonResult]())
    started_at: datetime = field(default_factory=datetime.now)

    def get_compatibility_rate(self) -> float:
        """Get percentage of compatible results."""
        if not self.comparison_results:
            return 100.0
        compatible = sum(1 for r in self.comparison_results if r.is_compatible)
        return (compatible / len(self.comparison_results)) * 100

    def get_uptime_hours(self) -> float:
        """Get hours since statistics started."""
        return (datetime.now(UTC) - self.started_at).total_seconds() / 3600


class DualErrorManager:
    """Manages both old and new error systems during migration."""

    def __init__(
        self,
        old_handler: LegacyErrorHandler | None = None,
        new_handler: WebSocketStreamErrorHandler | None = None,
        mode: ErrorSystemMode = ErrorSystemMode.DUAL_PASSIVE,
        enable_comparison: bool = True,
        log_differences: bool = True,
        max_comparison_history: int = 1000,
    ):
        """Initialize dual error manager.

        Args:
            old_handler: Old APIError-based handler
            new_handler: New WebSocketStreamError handler
            mode: Operating mode for the dual system
            enable_comparison: Enable result comparison
            log_differences: Log differences when found
            max_comparison_history: Maximum comparison results to keep
        """
        self.old_handler = old_handler
        self.new_handler = new_handler
        self.mode = mode
        self.enable_comparison = enable_comparison
        self.log_differences = log_differences
        self.max_comparison_history = max_comparison_history

        self.statistics = MigrationStatistics()
        self._lock = asyncio.Lock()
        self.call_context: dict[str, object] = {}  # For tracking call context

        # Validate configuration
        self._validate_configuration()

    def _validate_configuration(self) -> None:
        """Validate manager configuration."""
        if self.mode == ErrorSystemMode.OLD_ONLY and not self.old_handler:
            raise ValueError("Old handler required for OLD_ONLY mode")

        if self.mode == ErrorSystemMode.NEW_ONLY and not self.new_handler:
            raise ValueError("New handler required for NEW_ONLY mode")

        if self.mode in [
            ErrorSystemMode.DUAL_PASSIVE,
            ErrorSystemMode.DUAL_ACTIVE,
            ErrorSystemMode.DUAL_COMPARE,
        ]:
            if not self.old_handler or not self.new_handler:
                raise ValueError("Both handlers required for DUAL modes")

    async def handle_error_dual(
        self,
        error: Exception,
        context: WebSocketContextProtocol,
        payload: BaseModel | dict[str, object] | None = None,
    ) -> None:
        """Handle error with both old and new systems.

        Args:
            error: The error to handle
            context: WebSocket context
            payload: Optional payload that caused the error
        """
        async with self._lock:
            self.statistics.total_errors_handled += 1

        if self.mode == ErrorSystemMode.OLD_ONLY:
            await self._handle_with_old_system(error, context, payload)

        elif self.mode == ErrorSystemMode.NEW_ONLY:
            await self._handle_with_new_system(error, context, payload)

        elif self.mode == ErrorSystemMode.DUAL_PASSIVE:
            # Use old system as primary
            await self._handle_with_old_system(error, context, payload)

            # Try new system in background (non-blocking)
            asyncio.create_task(self._try_new_system_passive(error, context, payload))

        elif self.mode == ErrorSystemMode.DUAL_ACTIVE:
            # Use new system as primary
            try:
                await self._handle_with_new_system(error, context, payload)
            except Exception as e:
                logger.warning("New system failed, falling back to old: %s", e)
                async with self._lock:
                    self.statistics.new_system_failures += 1
                await self._handle_with_old_system(error, context, payload)

        elif self.mode == ErrorSystemMode.DUAL_COMPARE:
            # Run both systems and compare
            await self._handle_with_comparison(error, context, payload)

    async def _handle_with_old_system(
        self,
        error: Exception,
        context: WebSocketContextProtocol,
        payload: BaseModel | dict[str, object] | None,
    ) -> dict[str, object]:
        """Handle error with old APIError system.

        Returns:
            Result dictionary for comparison
        """
        if not self.old_handler:
            raise ValueError("Old handler not configured")

        result: dict[str, object] = {"handled": False, "error": None, "recovery": None}

        try:
            if isinstance(error, ValidationError):
                # Convert to APIError for old system
                await self.old_handler.handle_validation_error(error, context, payload)
            else:
                await self.old_handler.handle_error(error)

            async with self._lock:
                self.statistics.old_system_handled += 1

            result["handled"] = True

            # Extract result information
            if isinstance(error, APIError):
                error_info = {
                    "code": error.code,
                    "message": error.message,
                    "http_status": error.http_status,
                    "is_retryable": error.is_retryable,
                }
                result["error"] = error_info

        except Exception as e:
            logger.error("Old system error handling failed: %s", e)
            async with self._lock:
                self.statistics.old_system_failures += 1
            result["error"] = str(e)

        return result

    async def _handle_with_new_system(
        self,
        error: Exception,
        context: WebSocketContextProtocol,
        payload: BaseModel | dict[str, object] | None,
    ) -> dict[str, object]:
        """Handle error with new WebSocketStreamError system.

        Returns:
            Result dictionary for comparison
        """
        if not self.new_handler:
            raise ValueError("New handler not configured")

        result: dict[str, object] = {"handled": False, "error": None, "recovery": None}

        try:
            if isinstance(error, ValidationError):
                if isinstance(payload, BaseModel):
                    await self.new_handler.handle_validation_error(error, context, payload)
                else:
                    # Convert dict to BaseModel if needed
                    # For now, skip if not BaseModel
                    logger.warning("Payload not BaseModel, skipping new handler")
                    return result
            elif isinstance(error, WebSocketStreamError):
                await self.new_handler.handle_stream_error(error)
            else:
                # Handle generic exception
                await self.new_handler.handle_connection_error(
                    context=context,
                    error=error,
                    message=str(error),
                )

            async with self._lock:
                self.statistics.new_system_handled += 1

            result["handled"] = True

            # Extract result information
            if isinstance(error, WebSocketStreamError):
                error_info = {
                    "code": error.code.value,
                    "message": error.message,
                    "severity": error.severity.name,
                    "recovery_strategy": error.recovery_strategy.name,
                    "is_retryable": error.is_retryable,
                }
                result["error"] = error_info

        except Exception as e:
            logger.error(f"New system error handling failed: {e}")
            async with self._lock:
                self.statistics.new_system_failures += 1
            result["error"] = str(e)

        return result

    async def _try_new_system_passive(
        self,
        error: Exception,
        context: WebSocketContextProtocol,
        payload: BaseModel | dict[str, object] | None,
    ) -> None:
        """Try new system in passive mode (non-blocking)."""
        try:
            result = await self._handle_with_new_system(error, context, payload)
            if result["handled"]:
                logger.debug("New system successfully handled error in passive mode")
        except Exception as e:
            logger.debug(f"New system passive test failed (expected during migration): {e}")

    async def _handle_with_comparison(
        self,
        error: Exception,
        context: WebSocketContextProtocol,
        payload: BaseModel | dict[str, object] | None,
    ) -> None:
        """Handle with both systems and compare results."""
        # Run both systems
        results = await asyncio.gather(
            self._handle_with_old_system(error, context, payload),
            self._handle_with_new_system(error, context, payload),
            return_exceptions=True,
        )

        # Handle exceptions from gather and ensure proper types with validation
        # Validate and extract old system result
        old_result: dict[str, object]
        if isinstance(results[0], Exception):
            old_result = {"handled": False, "error": str(results[0]), "system": "old"}
        elif isinstance(results[0], dict):
            # Validate required fields for comparison
            old_result = self._validate_result_structure(results[0], "old")
        else:
            logger.error(f"Invalid old system result type: {type(results[0])}")
            old_result = {
                "handled": False,
                "error": f"Invalid result type: {type(results[0])}",
                "system": "old",
            }

        # Validate and extract new system result
        new_result: dict[str, object]
        if isinstance(results[1], Exception):
            new_result = {"handled": False, "error": str(results[1]), "system": "new"}
        elif isinstance(results[1], dict):
            # Validate required fields for comparison
            new_result = self._validate_result_structure(results[1], "new")
        else:
            logger.error(f"Invalid new system result type: {type(results[1])}")
            new_result = {
                "handled": False,
                "error": f"Invalid result type: {type(results[1])}",
                "system": "new",
            }

        async with self._lock:
            self.statistics.both_systems_handled += 1

        if self.enable_comparison:
            comparison = self._compare_results(
                error_type=type(error).__name__,
                old_result=old_result,
                new_result=new_result,
            )

            async with self._lock:
                self.statistics.comparison_results.append(comparison)

                # Trim history if needed
                if len(self.statistics.comparison_results) > self.max_comparison_history:
                    self.statistics.comparison_results = self.statistics.comparison_results[
                        -self.max_comparison_history :
                    ]

                if not comparison.is_compatible:
                    self.statistics.compatibility_failures += 1

                    if self.log_differences:
                        logger.warning(
                            f"Compatibility issue found for {comparison.error_type}: "
                            f"{', '.join(comparison.differences)}"
                        )

    def _compare_results(
        self,
        error_type: str,
        old_result: dict[str, object],
        new_result: dict[str, object],
    ) -> ComparisonResult:
        """Compare results from old and new systems.

        Args:
            error_type: Type of error handled
            old_result: Result from old system
            new_result: Result from new system

        Returns:
            Comparison result
        """
        comparison = ComparisonResult(
            error_type=error_type,
            old_system_result=old_result,
            new_system_result=new_result,
        )

        # Compare handled status
        if old_result.get("handled") != new_result.get("handled"):
            comparison.add_difference(
                f"Handled status differs: old={old_result.get('handled')}, "
                f"new={new_result.get('handled')}"
            )

        # Compare error details if both handled
        if old_result.get("handled") and new_result.get("handled"):
            old_error = old_result.get("error", {})
            new_error = new_result.get("error", {})

            # Compare retryability
            old_retryable = old_error.get("is_retryable")
            new_retryable = new_error.get("is_retryable")
            if old_retryable != new_retryable:
                comparison.add_difference(
                    f"Retryability differs: old={old_retryable}, new={new_retryable}"
                )

            # Compare error codes (allowing for different code systems)
            # This is expected to differ during migration
            if old_error.get("code") and new_error.get("code"):
                # Log but don't mark as incompatible
                logger.debug(
                    f"Error codes: old={old_error.get('code')}, new={new_error.get('code')}"
                )

        return comparison

    def _validate_result_structure(
        self, result: dict[str, object], system_name: str
    ) -> dict[str, object]:
        """Validate and normalize result structure from error handlers.

        Args:
            result: Result dictionary from error handler
            system_name: Name of the system ("old" or "new") for logging

        Returns:
            Validated and normalized result dictionary
        """
        validated_result = result.copy()

        # Ensure required fields exist with defaults
        if "handled" not in validated_result:
            logger.warning(
                f"{system_name} system result missing 'handled' field, defaulting to False"
            )
            validated_result["handled"] = False

        if "error" not in validated_result:
            validated_result["error"] = None

        if "recovery" not in validated_result:
            validated_result["recovery"] = None

        # Add system identifier
        validated_result["system"] = system_name

        # Validate handled field type
        if not isinstance(validated_result["handled"], bool):
            logger.warning(
                f"{system_name} system 'handled' field is not boolean: {type(validated_result['handled'])}"
            )
            validated_result["handled"] = bool(validated_result["handled"])

        # Validate error field structure if present
        error_value = validated_result["error"]
        if error_value is not None:
            if isinstance(error_value, dict):
                # Work with the dict directly as dict[str, Any]
                if "is_retryable" not in error_value:
                    error_value["is_retryable"] = None
                if "code" not in error_value:
                    error_value["code"] = "unknown"
                validated_result["error"] = error_value
            elif isinstance(validated_result["error"], str):
                # String error - convert to dict for comparison
                validated_result["error"] = {
                    "message": validated_result["error"],
                    "is_retryable": None,
                    "code": "string_error",
                }

        return validated_result

    def set_mode(self, mode: ErrorSystemMode) -> None:
        """Change the operating mode.

        Args:
            mode: New operating mode
        """
        self._validate_configuration()
        old_mode = self.mode
        self.mode = mode
        logger.info(f"Error system mode changed from {old_mode.value} to {mode.value}")

    def get_statistics(self) -> dict[str, object]:
        """Get migration statistics.

        Returns:
            Dictionary with statistics
        """
        return {
            "mode": self.mode.value,
            "total_errors_handled": self.statistics.total_errors_handled,
            "old_system_handled": self.statistics.old_system_handled,
            "new_system_handled": self.statistics.new_system_handled,
            "both_systems_handled": self.statistics.both_systems_handled,
            "compatibility_rate": f"{self.statistics.get_compatibility_rate():.2f}%",
            "compatibility_failures": self.statistics.compatibility_failures,
            "old_system_failures": self.statistics.old_system_failures,
            "new_system_failures": self.statistics.new_system_failures,
            "uptime_hours": f"{self.statistics.get_uptime_hours():.2f}",
            "comparison_count": len(self.statistics.comparison_results),
        }

    def get_recent_differences(self, limit: int = 10) -> list[ComparisonResult]:
        """Get recent compatibility differences.

        Args:
            limit: Maximum number of results to return

        Returns:
            List of recent comparison results with differences
        """
        incompatible = [r for r in self.statistics.comparison_results if not r.is_compatible]
        return incompatible[-limit:]

    def reset_statistics(self) -> None:
        """Reset migration statistics."""
        self.statistics = MigrationStatistics()
        logger.info("Migration statistics reset")

    @classmethod
    def create_for_migration(
        cls,
        exchange: ExchangeName,
        initial_mode: ErrorSystemMode = ErrorSystemMode.DUAL_PASSIVE,
    ) -> DualErrorManager:
        """Create a dual error manager for migration.

        Args:
            exchange: Exchange enum value
            initial_mode: Initial operating mode

        Returns:
            Configured dual error manager
        """
        # Old handler is not yet implemented - during Phase 2 migration we only have new handler
        # When old APIError-based handler is implemented, it will be created here:
        # old_handler = LegacyAPIErrorHandler(exchange_config)
        # For now, set to None and use NEW_ONLY mode to avoid dual system complexity
        old_handler = None

        # Create new handler
        new_handler = WebSocketErrorHandlerFactory.create_minimal_handler(exchange)

        return cls(
            old_handler=old_handler,
            new_handler=new_handler,
            mode=ErrorSystemMode.NEW_ONLY if old_handler is None else initial_mode,
            enable_comparison=old_handler is not None,
            log_differences=True,
        )

    async def recommend_mode_change(self) -> ErrorSystemMode | None:
        """Recommend a mode change based on statistics.

        Returns:
            Recommended mode or None if no change needed
        """
        if self.statistics.total_errors_handled < MIN_ERRORS_FOR_RECOMMENDATION:
            # Not enough data
            return None

        compatibility_rate = self.statistics.get_compatibility_rate()
        new_failure_rate = (
            self.statistics.new_system_failures / max(self.statistics.new_system_handled, 1)
        ) * 100

        current_mode = self.mode

        # Recommend progression through modes
        if current_mode == ErrorSystemMode.DUAL_PASSIVE:
            if (compatibility_rate > COMPATIBILITY_RATE_THRESHOLD_95 and 
                new_failure_rate < NEW_FAILURE_RATE_THRESHOLD_1):
                return ErrorSystemMode.DUAL_ACTIVE

        elif current_mode == ErrorSystemMode.DUAL_ACTIVE:
            if (compatibility_rate > COMPATIBILITY_RATE_THRESHOLD_99 and 
                new_failure_rate < NEW_FAILURE_RATE_THRESHOLD_01):
                return ErrorSystemMode.NEW_ONLY

        elif current_mode == ErrorSystemMode.DUAL_COMPARE:
            if compatibility_rate > COMPATIBILITY_RATE_THRESHOLD_95:
                return ErrorSystemMode.DUAL_ACTIVE

        return None
