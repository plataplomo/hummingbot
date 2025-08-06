"""Typed models for PnL reporting data.

This module provides typed Pydantic models to replace dict[str, Any] patterns
in PnL calculations, ensuring type safety for financial reporting.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.symbols.models import Symbol


class PositionPnLDetail(BaseModel):
    """Type-safe PnL details for a specific position.

    This model replaces dict[str, Any] returns in position PnL calculations
    to ensure type safety for position-specific financial data.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All monetary values use Decimal type
    - Explicit units in field names (e.g., _usd)
    """

    symbol: Symbol = Field(description="Symbol of the position")

    exchange: ExchangeName = Field(description="Exchange where position is held")

    unrealized_pnl_usd: Decimal = Field(description="Unrealized PnL in USD")

    realized_pnl_usd: Decimal = Field(description="Realized PnL in USD")

    total_pnl_usd: Decimal = Field(description="Total PnL (realized + unrealized) in USD")

    entry_price: Decimal = Field(description="Average entry price of position")

    current_price: Decimal = Field(description="Current market price")

    quantity: Decimal = Field(description="Position quantity in base asset units")

    market_value_usd: Decimal = Field(description="Current market value in USD")

    # Optional performance metrics
    pnl_percentage: Decimal | None = Field(
        default=None, description="PnL as percentage of entry value"
    )

    fees_paid_usd: Decimal | None = Field(
        default=None, description="Total fees paid for this position in USD"
    )

    holding_period_days: Decimal | None = Field(
        default=None, description="Days position has been held"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class PnLReport(BaseModel):
    """Type-safe comprehensive PnL report.

    This model replaces dict[str, Any] returns in PnL calculations
    to ensure type safety for comprehensive financial reporting.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All monetary values use Decimal type
    - Explicit units in field names (e.g., _usd)
    """

    total_unrealized_pnl_usd: Decimal = Field(
        description="Total unrealized PnL across all positions in USD"
    )

    total_realized_pnl_usd: Decimal = Field(
        description="Total realized PnL across all positions in USD"
    )

    net_pnl_usd: Decimal = Field(description="Net PnL (realized + unrealized) in USD")

    total_equity_usd: Decimal = Field(description="Total portfolio equity in USD")

    total_exposure_usd: Decimal = Field(description="Total position exposure in USD")

    calculation_timestamp: datetime = Field(description="UTC timestamp when PnL was calculated")

    calculation_method: str = Field(description="Method used for PnL calculation")

    fees_included: bool = Field(description="Whether fees are included in calculations")

    base_currency: str = Field(description="Base currency for calculations")

    # Position-specific PnL details
    position_pnls: dict[str, PositionPnLDetail] = Field(
        description="PnL details keyed by '{exchange}:{symbol}'"
    )

    # Optional performance metrics
    daily_pnl_usd: Decimal | None = Field(default=None, description="PnL for current day in USD")

    weekly_pnl_usd: Decimal | None = Field(default=None, description="PnL for current week in USD")

    monthly_pnl_usd: Decimal | None = Field(
        default=None, description="PnL for current month in USD"
    )

    pnl_percentage: Decimal | None = Field(
        default=None, description="Total PnL as percentage of equity"
    )

    total_fees_usd: Decimal | None = Field(
        default=None, description="Total fees paid across all positions in USD"
    )

    def get_position_pnl(self, exchange: ExchangeName, symbol: Symbol) -> PositionPnLDetail | None:
        """Get PnL details for a specific position.

        Args:
            exchange: Exchange where position is held
            symbol: Symbol of the position

        Returns:
            Position PnL details or None if position not found
        """
        key = f"{exchange.value}:{symbol.value}"
        return self.position_pnls.get(key)

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class ReconciliationReport(BaseModel):
    """Type-safe reconciliation report.

    This model replaces dict[str, Any] returns in reconciliation processes
    to ensure type safety for reconciliation reporting.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All monetary values use Decimal type
    """

    reconciliation_timestamp: datetime = Field(description="UTC timestamp of reconciliation")

    reconciliation_successful: bool = Field(description="Whether reconciliation was successful")

    total_discrepancies: int = Field(description="Total number of discrepancies found")

    # Exchange-specific reconciliation results
    exchange_results: dict[ExchangeName, bool] = Field(
        description="Reconciliation results keyed by exchange name"
    )

    # Discrepancy details
    balance_discrepancies: list[str] = Field(description="List of balance discrepancy descriptions")

    position_discrepancies: list[str] = Field(
        description="List of position discrepancy descriptions"
    )

    # Optional diagnostic data
    processing_time_ms: int | None = Field(
        default=None, description="Time taken to complete reconciliation in milliseconds"
    )

    error_messages: list[str] | None = Field(
        default=None, description="List of error messages if reconciliation failed"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class ValidationStatistics(BaseModel):
    """Type-safe validation statistics.

    This model replaces dict[str, object] returns in validation processes
    to ensure type safety for validation metrics.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    """

    total_validations: int = Field(description="Total number of validations performed")

    successful_validations: int = Field(description="Number of successful validations")

    failed_validations: int = Field(description="Number of failed validations")

    validation_success_rate: Decimal = Field(description="Validation success rate (0-1)")

    last_validation_timestamp: datetime = Field(description="Timestamp of last validation")

    # Optional error tracking
    common_failure_reasons: list[str] | None = Field(
        default=None, description="Most common validation failure reasons"
    )

    average_validation_time_ms: Decimal | None = Field(
        default=None, description="Average validation processing time in milliseconds"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True
