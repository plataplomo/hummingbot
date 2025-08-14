"""Fee calculation result model for exchange-specific fee tracking."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class FeeResult(BaseModel):
    """Comprehensive fee calculation result.

    This model represents the complete result of fee calculations across
    different exchanges, including detailed breakdown of fee components
    and calculation metadata for auditing.

    Attributes:
        amount: Total fee amount
        currency: Currency of the fee
        calculation_method: Method used for fee calculation
        calculation_timestamp: When the calculation was performed
        exchange: Exchange where the fee applies
        fee_rate: Fee rate used in calculation
        is_maker: Whether this was a maker order (affects fees)
        notional_value: Notional value the fee was calculated on
        base_fee: Base fee before any discounts
        discount_amount: Discount amount applied (if any)
        discount_type: Type of discount applied (VIP, token, referral, etc.)
        precision: Decimal precision used in calculation
    """

    amount: Decimal = Field(description="Total fee amount")
    currency: str = Field(description="Currency of the fee")
    calculation_method: str = Field(description="Method used for fee calculation")
    calculation_timestamp: datetime = Field(description="When calculation was performed")

    # Fee calculation context
    exchange: str = Field(description="Exchange where fee applies")
    fee_rate: Decimal = Field(description="Fee rate used in calculation")
    is_maker: bool | None = Field(None, description="Whether this was a maker order")
    notional_value: Decimal | None = Field(None, description="Notional value used")

    # Fee breakdown
    base_fee: Decimal | None = Field(None, description="Base fee before discounts")
    discount_amount: Decimal | None = Field(None, description="Discount amount applied")
    discount_type: str | None = Field(None, description="Type of discount applied")

    # Calculation metadata
    precision: int | None = Field(None, description="Decimal precision used")

    @property
    def effective_fee_rate(self) -> Decimal:
        """Calculate effective fee rate after discounts."""
        if self.notional_value and self.notional_value > 0:
            return self.amount / self.notional_value
        return Decimal(0)

    @property
    def has_discount(self) -> bool:
        """Check if any discount was applied."""
        return self.discount_amount is not None and self.discount_amount > 0

    def to_display_string(self, precision: int = 6) -> str:
        """Format fee for display with currency.

        Returns:
            Formatted string with fee amount and currency.
        """
        formatted_amount = f"{self.amount:.{precision}f}"
        return f"{formatted_amount} {self.currency}"

    def get_discount_percentage(self) -> Decimal | None:
        """Calculate discount as percentage of base fee.

        Returns:
            Discount percentage or None if no discount or invalid base fee.
        """
        if self.base_fee and self.base_fee > 0 and self.discount_amount:
            return (self.discount_amount / self.base_fee) * Decimal(100)
        return None
