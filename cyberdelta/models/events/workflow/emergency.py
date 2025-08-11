"""Emergency liquidation workflow event model."""

from decimal import Decimal
from typing import ClassVar

from cyberdelta.models.events.workflow.base import BaseWorkflowEvent


class EmergencyLiquidationEvent(BaseWorkflowEvent):
    """Emergency liquidation workflow event.

    Encapsulates parameters for emergency liquidation operations
    with comprehensive safety controls and explicit parameter requirements.

    All emergency operations require explicit justification and controls
    to ensure proper audit trails and risk management compliance.
    """

    # Class attribute for workflow type identification
    WORKFLOW_TYPE: ClassVar[str] = "EmergencyLiquidation"

    # Required emergency parameters (no defaults)
    reason: str  # Reason for emergency liquidation (audit requirement)
    force: bool  # Force execution despite safety checks

    # Optional parameters with explicit defaults
    positions: list[str] | None = None  # Specific positions (None = all)
    max_loss: Decimal | None = None  # Maximum acceptable loss threshold
