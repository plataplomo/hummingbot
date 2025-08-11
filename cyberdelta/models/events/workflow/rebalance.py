"""Rebalancing workflow event model."""

from decimal import Decimal
from typing import ClassVar

from cyberdelta.models.events.workflow.base import BaseWorkflowEvent


class RebalanceWorkflowEvent(BaseWorkflowEvent):
    """Portfolio rebalancing workflow event.

    Encapsulates parameters for portfolio rebalancing operations
    with comprehensive validation and explicit parameter requirements.

    All rebalancing parameters must be explicitly provided to ensure
    proper risk management and prevent unintended portfolio modifications.
    """

    # Class attribute for workflow type identification
    WORKFLOW_TYPE: ClassVar[str] = "RebalanceWorkflow"

    # Required rebalancing parameters (no defaults)
    target_allocations: dict[str, Decimal]  # Symbol -> target percentage
    max_slippage: Decimal  # Maximum acceptable slippage
    rebalance_mode: str  # Rebalancing strategy mode
    dry_run: bool  # Execute in simulation mode
