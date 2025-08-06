"""Typed models for fill processing statistics and order updates.

This module provides typed Pydantic models for fill statistics tracking
and order update data, ensuring type safety without duplication.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

# Import Trade at top to avoid E402
from cyberdelta.models.market.trade import Trade


class OrderUpdateData(BaseModel):
    """Type-safe order update data from exchange.

    This model contains order update information received from exchanges,
    ensuring type safety for order status processing.
    """

    status: str | None = Field(default=None, description="New order status")

    update_type: str | None = Field(default=None, description="Type of update")

    fill: Trade | None = Field(default=None, description="Fill data if order was filled")

    timestamp: datetime | None = Field(default=None, description="Update timestamp")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class FillStatistics(BaseModel):
    """Type-safe fill processing statistics.

    This model provides statistics for fill processing operations,
    replacing dict[str, Any] patterns in fill handlers.
    """

    fill_handler_available: bool = Field(description="Whether fill handler is available")

    total_fills_processed: int = Field(description="Total number of fills processed")

    total_fees_usd: Decimal = Field(description="Total fees paid in USD")

    average_fill_size_usd: Decimal = Field(description="Average fill size in USD")

    success_rate: Decimal = Field(description="Fill processing success rate (0-1)")

    last_fill_timestamp: datetime | None = Field(
        default=None, description="Timestamp of last processed fill"
    )

    message: str | None = Field(default=None, description="Optional status message")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True
