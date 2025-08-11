"""Graceful shutdown workflow event model."""

from typing import ClassVar

from cyberdelta.models.events.workflow.base import BaseWorkflowEvent


class GracefulShutdownEvent(BaseWorkflowEvent):
    """Graceful shutdown workflow event.

    Encapsulates parameters for graceful system shutdown operations
    with explicit controls for each aspect of the shutdown process.

    All shutdown parameters require explicit specification to ensure
    proper system state management during termination procedures.
    """

    # Class attribute for workflow type identification
    WORKFLOW_TYPE: ClassVar[str] = "GracefulShutdown"

    # Required shutdown parameters (no defaults)
    close_positions: bool  # Close all open trading positions
    save_state: bool  # Persist system state before shutdown
    notify_services: bool  # Send shutdown notifications

    # Optional parameters with explicit defaults
    timeout_seconds: float | None = None  # Maximum shutdown duration
