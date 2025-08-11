"""Workflow handler registry with type-safe handler management.

Provides workflow handler registration and lookup with proper type safety.
"""

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.exceptions import RequiredFieldError
from cyberdelta.protocols import WorkflowHandler


logger = get_logger(__name__)


class WorkflowRegistry:
    """Type-safe registry for workflow handlers."""

    def __init__(self) -> None:
        """Initialize empty registry."""
        self._handlers: dict[str, WorkflowHandler] = {}

    def register_handler(self, event_type: str, handler: WorkflowHandler) -> None:
        """Register a handler for an event type.

        Args:
            event_type: The event type string
            handler: Handler implementing WorkflowHandler protocol

        Raises:
            RequiredFieldError: If event_type is empty
        """
        if not event_type or not event_type.strip():
            raise RequiredFieldError(
                field_name="event_type", context="workflow handler registration"
            )

        if event_type in self._handlers:
            existing_handler = self._handlers[event_type]
            logger.warning(
                "workflow_handler_overridden",
                event_type=event_type,
                previous_handler=type(existing_handler).__name__,
                new_handler=type(handler).__name__,
            )

        self._handlers[event_type] = handler
        logger.info(
            "workflow_handler_registered",
            event_type=event_type,
            handler_type=type(handler).__name__,
        )

    def get_handler(self, event_type: str) -> WorkflowHandler | None:
        """Get handler for event type.

        Args:
            event_type: The event type to look up

        Returns:
            Handler if found, None otherwise
        """
        return self._handlers.get(event_type)

    def list_event_types(self) -> list[str]:
        """List all registered event types.

        Returns:
            List of registered event type strings
        """
        return list(self._handlers.keys())

    def clear(self) -> None:
        """Clear all registered handlers."""
        handler_count = len(self._handlers)
        self._handlers.clear()
        logger.info("workflow_registry_cleared", handler_count=handler_count)
