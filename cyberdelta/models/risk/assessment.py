"""Risk assessment models for trading decisions.

This module provides models for risk assessment results and position sizing
calculations used in the trading decision process.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class PositionSize(BaseModel):
    """Calculated position size with risk constraints applied.

    This model represents the result of position sizing calculations,
    including the quantity to trade and various risk metrics.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All monetary values use Decimal type
    - Explicit units in field names (e.g., _usd)
    """

    quantity: Decimal = Field(description="Quantity to trade in base asset units")

    value_usd: Decimal = Field(description="Total value of the position in USD")

    percent_of_equity: Decimal = Field(
        description="Position size as percentage of total equity (0-100)"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class RiskAssessment(BaseModel):
    """Risk assessment result for a trading signal.

    This model contains the complete risk assessment for a trading signal,
    including approval status, calculated position size, and any limit
    violations that would prevent execution.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - Explicit approval with clear violations
    - All monetary values use Decimal type
    """

    signal_id: str = Field(description="ID of the signal being assessed")

    approved: bool = Field(description="Whether the signal is approved for execution")

    position_size: PositionSize = Field(
        description="Calculated position size (valid even if not approved)"
    )

    current_exposure: Decimal = Field(description="Current total exposure in USD before this trade")

    limit_violations: list[str] = Field(
        description="List of specific limit violations preventing approval"
    )

    # Optional risk metrics
    max_loss_usd: Decimal | None = Field(
        default=None, description="Maximum potential loss in USD if stop loss is set"
    )

    risk_reward_ratio: Decimal | None = Field(
        default=None, description="Risk/reward ratio if take profit and stop loss are set"
    )

    def get_rejection_reason(self) -> str | None:
        """Get a formatted rejection reason if not approved.

        Returns:
            Formatted string with all violations, or None if approved
        """
        if self.approved:
            return None

        if not self.limit_violations:
            return "Rejected: No specific violations recorded"

        return f"Rejected: {'; '.join(self.limit_violations)}"

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True
