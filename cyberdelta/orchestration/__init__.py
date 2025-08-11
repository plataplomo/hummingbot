"""Orchestration layer for complex workflows using custom msgspec events.

This module provides workflow definitions for complex trading operations
that require coordination across multiple services and event handlers.

Enhanced with custom BaseWorkflowEvent integration for Steps 86-92.
"""

from cyberdelta.models.events.workflow import (
    BaseWorkflowEvent,
    EmergencyLiquidationEvent,
    GracefulShutdownEvent,
    PlaceOrderWorkflowEvent,
    RebalanceWorkflowEvent,
)
from cyberdelta.orchestration.audit import WorkflowAuditLogger
from cyberdelta.orchestration.orchestrator import WorkflowOrchestrator
from cyberdelta.orchestration.workflows import (
    EmergencyLiquidationHandler,
    GracefulShutdownHandler,
    PlaceOrderWorkflowHandler,
    RebalanceWorkflowHandler,
)


__all__ = [
    "BaseWorkflowEvent",
    "EmergencyLiquidationEvent",
    "EmergencyLiquidationHandler",
    "GracefulShutdownEvent",
    "GracefulShutdownHandler",
    "PlaceOrderWorkflowEvent",
    "PlaceOrderWorkflowHandler",
    "RebalanceWorkflowEvent",
    "RebalanceWorkflowHandler",
    "WorkflowAuditLogger",
    "WorkflowOrchestrator",
]
