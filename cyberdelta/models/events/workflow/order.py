"""Order placement workflow event model."""

from decimal import Decimal
from typing import ClassVar

from cyberdelta.enums.trading import OrderSide, OrderType
from cyberdelta.models.events.workflow.base import BaseWorkflowEvent


class PlaceOrderWorkflowEvent(BaseWorkflowEvent):
    """Order placement workflow event.

    Encapsulates all parameters required for order placement workflow
    with comprehensive type safety and explicit parameter requirements.

    All financial parameters must be explicitly provided with no defaults
    to ensure trading system safety and prevent unintended operations.
    """

    # Class attribute for workflow type identification
    WORKFLOW_TYPE: ClassVar[str] = "PlaceOrderWorkflow"

    # Required workflow parameters (no defaults)
    symbol: str  # String representation of Symbol (boundary pattern)
    side: OrderSide  # Enum provides type safety
    quantity: Decimal  # Always use Decimal for financial precision
    price: Decimal | None  # Explicit None allowed for market orders
    order_type: OrderType  # Must be explicitly specified

    # Optional parameters with explicit defaults
    strategy_id: str | None = None
