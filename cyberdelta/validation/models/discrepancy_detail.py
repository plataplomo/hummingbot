from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class DiscrepancyDetail(BaseModel):
    """
    Represents a single detected discrepancy during position reconciliation.
    """

    symbol: str = Field(..., description="The trading symbol of the asset with a discrepancy.")
    discrepancy_type: Literal[
        "size",
        "entry_price",
        "mark_price",
        "liquidation_price",
        "unrealized_pnl",
        "api_parsing_error",
        "local_parsing_error",
        "reconciliation_error",  # Generic error during the reconciliation process for a symbol
        "unknown_api_symbol",  # Symbol reported by API but not expected/tracked locally
        "unknown_local_symbol",  # Symbol tracked locally but not reported by API (or flat on API)
    ] = Field(..., description="The type or category of the discrepancy.")

    # Values are stored as strings to accommodate various representations (e.g., "None", numeric strings)
    # and to avoid precision issues if they were to be converted back and forth from Decimal just for storage here.
    # The actual comparison and numeric operations happen with Decimals before this model is created.
    exchange_value: str | None = Field(
        default=None, description="The value reported by the exchange (or N/A)."
    )
    local_value: str | None = Field(default=None, description="The value tracked locally (or N/A).")

    # The 'discrepancy_amount' field has been removed. The difference can be inferred from
    # exchange_value and local_value if they are numeric, or this model can be extended
    # if a specific string representation of the difference is needed.
    # For non-numeric discrepancies (e.g. parsing errors), an amount isn't applicable.

    details: str | None = Field(
        default=None,
        description="Additional details or context about the discrepancy, especially for errors.",
    )

    model_config = ConfigDict(frozen=True, extra="forbid")


class HistoricalDiscrepancyRecord(BaseModel):
    """
    Represents a discrepancy record with its context (exchange, time) and status.
    This model is mutable to allow updating the 'is_corrected' status.
    """

    detail: DiscrepancyDetail = Field(..., description="The core details of the discrepancy.")
    exchange_id: str = Field(
        ..., description="The identifier of the exchange where the discrepancy was observed."
    )
    recorded_at: datetime = Field(
        ..., description="The timestamp when the discrepancy was recorded."
    )
    is_corrected: bool = Field(
        False, description="Whether this discrepancy has been successfully corrected."
    )

    model_config = ConfigDict(validate_assignment=True, extra="forbid")
