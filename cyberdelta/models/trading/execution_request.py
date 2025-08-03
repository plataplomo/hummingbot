"""Execution request model for trade execution.

This module provides the ExecutionRequest model which represents a
risk-approved request to execute a trade based on a signal.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from cyberdelta.enums import OrderType, TimeInForce
from cyberdelta.models import TradeSignal

# Forward reference for PositionSize
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from cyberdelta.models.risk.assessment import PositionSize


class ExecutionRequest(BaseModel):
    """Request to execute a trade based on risk-approved signal.
    
    This model represents a trade that has been approved by risk management
    and is ready for execution. It combines the original signal with the
    calculated position size and execution parameters.
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - Uses proper enum types (OrderType, TimeInForce)
    - References domain models (TradeSignal, PositionSize)
    """
    
    signal: TradeSignal = Field(
        description="The original trading signal that triggered this execution"
    )
    
    position_size: "PositionSize" = Field(
        description="Risk-calculated position size for this trade"
    )
    
    # Execution parameters - NO defaults per CODING_STANDARDS
    order_type: OrderType = Field(
        description="Order type to use for execution (LIMIT, MARKET, etc.)"
    )
    
    time_in_force: TimeInForce = Field(
        description="Time in force for the order (GTC, IOC, etc.)"
    )
    
    # Optional metadata for tracking/audit
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional metadata for execution tracking"
    )
    
    class Config:
        """Pydantic configuration."""
        frozen = True  # Immutable for thread safety
        validate_assignment = True