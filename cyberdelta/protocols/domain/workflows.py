"""Workflow execution protocols.

Defines protocols for workflow handlers following domain-driven design principles.
"""

from typing import Protocol

from cyberdelta.models.events.workflow.base import BaseWorkflowEvent
from cyberdelta.models.events.workflow_context import WorkflowContextModel


class WorkflowHandler(Protocol):
    """Protocol for workflow handlers.

    Defines the interface that all workflow handlers must implement
    for type-safe workflow execution.
    """

    async def execute(self, event: BaseWorkflowEvent) -> WorkflowContextModel:
        """Execute workflow for the given event.

        Args:
            event: The workflow event to process

        Returns:
            WorkflowContextModel with execution results

        Raises:
            Exception: If workflow execution fails
        """
        ...
