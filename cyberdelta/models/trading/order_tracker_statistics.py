"""Typed models for order tracker statistics.

This module provides a focused model for order tracker statistics,
separate from ExecutionStatistics which contains execution engine specific data.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class OrderTrackerStatistics(BaseModel):
    """Type-safe order tracker statistics.

    This model contains only the statistics that the OrderTracker actually tracks,
    avoiding the abstraction violation of forcing it to return ExecutionStatistics.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All rates use Decimal type for precision
    """

    active_orders: int = Field(description="Number of currently active orders")

    total_orders: int = Field(description="Total orders tracked since startup")

    success_count: int = Field(description="Number of successful order executions")

    error_count: int = Field(description="Number of order execution errors")

    success_rate: Decimal = Field(description="Order execution success rate (0-1)")

    last_activity_timestamp: datetime | None = Field(
        default=None, description="Timestamp of last tracking activity"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True
