"""Execution models for the ExecutionHandler.

This module contains execution-related models that are shared across
the execution system to avoid circular imports.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field, computed_field


if TYPE_CHECKING:
    pass


class ExecutionStatus(Enum):
    """Status of an execution."""

    PENDING = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    PARTIALLY_COMPLETED = auto()
    COMPENSATING = auto()
    REJECTED = auto()


class TradeExecution(BaseModel):
    """Represents a trade execution across multiple exchanges."""

    # Core fields
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    opportunity: Any = Field(..., description="Sized arbitrage opportunity")
    status: ExecutionStatus = Field(default=ExecutionStatus.PENDING)
    error_message: str | None = Field(default=None)

    # Order tracking
    long_order_id: str | None = Field(default=None)
    short_order_id: str | None = Field(default=None)
    long_position_id: str | None = Field(default=None)
    short_position_id: str | None = Field(default=None)
    long_order_response: dict[str, Any] | None = Field(default=None)
    short_order_response: dict[str, Any] | None = Field(default=None)

    # Timing
    start_time: datetime | None = Field(default=None)
    end_time: datetime | None = Field(default=None)

    # Fill information with validation
    long_fill_price: Decimal | None = Field(
        default=None, ge=0, description="Long fill price must be positive"
    )
    short_fill_price: Decimal | None = Field(
        default=None, ge=0, description="Short fill price must be positive"
    )
    long_fill_quantity: Decimal | None = Field(
        default=None, ge=0, description="Long fill quantity must be positive"
    )
    short_fill_quantity: Decimal | None = Field(
        default=None, ge=0, description="Short fill quantity must be positive"
    )
    realized_pnl: Decimal | None = Field(default=None, description="Realized profit/loss")

    model_config = {
        "arbitrary_types_allowed": True,  # Allow SizedOpportunity type
        "use_enum_values": False,  # Keep enum objects
        "validate_assignment": True,  # Validate on assignment
    }

    @computed_field
    def duration_seconds(self) -> float | None:
        """Calculate execution duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @computed_field
    def is_completed(self) -> bool:
        """Check if execution is in a completed state."""
        return self.status in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.REJECTED,
        }

    @computed_field
    def has_fills(self) -> bool:
        """Check if execution has any fills."""
        return bool(self.long_fill_quantity or self.short_fill_quantity)

    def model_dump_execution_summary(self) -> dict[str, Any]:
        """Export execution summary for logging/monitoring."""
        return {
            "id": self.id,
            "status": self.status.name,
            "symbol": (self.opportunity.opportunity.symbol if self.opportunity else None),
            "long_exchange": (
                self.opportunity.opportunity.long_exchange if self.opportunity else None
            ),
            "short_exchange": (
                self.opportunity.opportunity.short_exchange if self.opportunity else None
            ),
            "duration_seconds": self.duration_seconds,
            "realized_pnl": float(self.realized_pnl) if self.realized_pnl else None,
            "error_message": self.error_message,
            "has_fills": self.has_fills,
        }

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for backwards compatibility."""
        return {
            "id": self.id,
            "opportunity": {
                "symbol": (self.opportunity.opportunity.symbol if self.opportunity else None),
                "long_exchange": (
                    self.opportunity.opportunity.long_exchange if self.opportunity else None
                ),
                "short_exchange": (
                    self.opportunity.opportunity.short_exchange if self.opportunity else None
                ),
                "long_price": (
                    float(getattr(self.opportunity.opportunity, "long_price", 0))
                    if self.opportunity
                    else 0
                ),
                "short_price": (
                    float(getattr(self.opportunity.opportunity, "short_price", 0))
                    if self.opportunity
                    else 0
                ),
                "expected_profit": (
                    float(self.opportunity.opportunity.expected_profit or 0)
                    if self.opportunity
                    else 0
                ),
                "long_size": float(self.opportunity.long_size) if self.opportunity else 0,
                "short_size": float(self.opportunity.short_size) if self.opportunity else 0,
                "timestamp": (
                    self.opportunity.opportunity.timestamp.isoformat()
                    if self.opportunity and self.opportunity.opportunity.timestamp
                    else None
                ),
            },
            "status": self.status.name,
            "long_order_id": self.long_order_id,
            "short_order_id": self.short_order_id,
            "error_message": self.error_message,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "long_fill_price": float(self.long_fill_price) if self.long_fill_price else None,
            "short_fill_price": float(self.short_fill_price) if self.short_fill_price else None,
            "long_fill_quantity": (
                float(self.long_fill_quantity) if self.long_fill_quantity else None
            ),
            "short_fill_quantity": (
                float(self.short_fill_quantity) if self.short_fill_quantity else None
            ),
            "realized_pnl": float(self.realized_pnl) if self.realized_pnl else None,
        }
