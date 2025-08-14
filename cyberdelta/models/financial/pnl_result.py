"""PnL calculation result model with comprehensive financial data."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class PnLResult(BaseModel):
    """Comprehensive PnL calculation result.

    This model represents the complete result of any PnL calculation,
    whether unrealized, realized, or portfolio-level. It includes all
    necessary metadata for auditing and business logic decisions.

    Attributes:
        amount: Net PnL amount after all calculations
        currency: Currency of the PnL result
        gross_amount: PnL before fees (if applicable)
        fees_amount: Fee amount included/excluded from calculation
        includes_fees: Whether fees are included in the net amount
        calculation_method: Method used for calculation (mark_to_market, fifo, etc.)
        calculation_timestamp: When the calculation was performed
        position_size: Size of position used in calculation (if applicable)
        entry_price: Entry price used in calculation (if applicable)
        mark_price: Mark price used in calculation (if applicable)
        precision: Decimal precision used in calculation
    """

    amount: Decimal = Field(description="Net PnL amount")
    currency: str = Field(description="Currency of the result")
    gross_amount: Decimal | None = Field(None, description="PnL before fees")
    fees_amount: Decimal | None = Field(None, description="Fees included/excluded")
    includes_fees: bool = Field(description="Whether fees are included in amount")
    calculation_method: str = Field(description="Method used for calculation")
    calculation_timestamp: datetime = Field(description="When calculation was performed")

    # Optional calculation context
    position_size: Decimal | None = Field(None, description="Position size used")
    entry_price: Decimal | None = Field(None, description="Entry price used")
    mark_price: Decimal | None = Field(None, description="Mark price used")
    precision: int | None = Field(None, description="Decimal precision used")

    @property
    def is_profitable(self) -> bool:
        """Check if PnL is profitable."""
        return self.amount > Decimal(0)

    @property
    def is_loss(self) -> bool:
        """Check if PnL represents a loss."""
        return self.amount < Decimal(0)

    @property
    def is_breakeven(self) -> bool:
        """Check if PnL is breakeven (zero)."""
        return self.amount == Decimal(0)

    def to_display_string(self, precision: int = 2) -> str:
        """Format PnL for display with proper sign and currency.

        Returns:
            Formatted string with sign, amount and currency.
        """
        sign = "+" if self.amount >= 0 else ""
        formatted_amount = f"{self.amount:.{precision}f}"
        return f"{sign}{formatted_amount} {self.currency}"

    def get_profit_loss_percentage(self, notional_value: Decimal) -> Decimal | None:
        """Calculate PnL as percentage of notional value.

        Returns:
            PnL percentage or None if notional value is zero.
        """
        if notional_value == 0:
            return None
        return (self.amount / notional_value) * Decimal(100)
