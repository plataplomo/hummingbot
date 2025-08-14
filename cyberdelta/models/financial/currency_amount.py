"""Type-safe currency amount model for financial calculations."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator

from cyberdelta.exceptions.financial import (
    CurrencyMismatchError,
    DivisionByZeroError,
    InvalidAmountError,
)


class CurrencyAmount(BaseModel):
    """Type-safe currency amount with arithmetic operations.

    This model ensures currency safety in financial calculations by
    preventing operations between different currencies and maintaining
    precision throughout calculations.

    Attributes:
        amount: The monetary amount
        currency: The currency code (e.g., "USD", "USDC", "BTC")
        precision: Decimal precision for the currency (optional)
    """

    model_config = {"frozen": True}  # Make hashable

    def __hash__(self) -> int:
        """Make CurrencyAmount hashable for use in sets and dict keys.

        Returns:
            Hash value based on amount, currency, and precision.
        """
        return hash((self.amount, self.currency, self.precision))

    amount: Decimal = Field(description="The monetary amount")
    currency: str = Field(description="Currency code")
    precision: int | None = Field(None, description="Decimal precision for currency")

    @field_validator("currency")
    @classmethod
    def currency_must_be_uppercase(cls, v: str) -> str:
        """Ensure currency is always uppercase.

        Returns:
            Uppercase currency code.
        """
        return v.upper()

    @field_validator("amount")
    @classmethod
    def amount_must_be_finite(cls, v: Decimal) -> Decimal:
        """Ensure amount is finite (not NaN or infinite).

        Returns:
            Validated finite decimal amount.

        Raises:
            InvalidAmountError: If amount is NaN or infinite.
        """
        if not v.is_finite():
            raise InvalidAmountError
        return v

    def __add__(self, other: CurrencyAmount) -> CurrencyAmount:
        """Add two currency amounts of the same currency.

        Returns:
            New CurrencyAmount with sum of amounts.

        Raises:
            CurrencyMismatchError: If currencies don't match.
        """
        if self.currency != other.currency:
            raise CurrencyMismatchError(
                currency1=self.currency, currency2=other.currency, operation="add"
            )
        return CurrencyAmount(
            amount=self.amount + other.amount, currency=self.currency, precision=self.precision
        )

    def __sub__(self, other: CurrencyAmount) -> CurrencyAmount:
        """Subtract two currency amounts of the same currency.

        Returns:
            New CurrencyAmount with difference of amounts.

        Raises:
            CurrencyMismatchError: If currencies don't match.
        """
        if self.currency != other.currency:
            raise CurrencyMismatchError(
                currency1=self.currency, currency2=other.currency, operation="subtract"
            )
        return CurrencyAmount(
            amount=self.amount - other.amount, currency=self.currency, precision=self.precision
        )

    def __mul__(self, multiplier: Decimal) -> CurrencyAmount:
        """Multiply currency amount by a scalar.

        Returns:
            New CurrencyAmount with multiplied amount.
        """
        return CurrencyAmount(
            amount=self.amount * multiplier, currency=self.currency, precision=self.precision
        )

    def __truediv__(self, divisor: Decimal) -> CurrencyAmount:
        """Divide currency amount by a scalar.

        Returns:
            New CurrencyAmount with divided amount.

        Raises:
            DivisionByZeroError: If divisor is zero.
        """
        if divisor == 0:
            raise DivisionByZeroError(context="currency amount division")
        return CurrencyAmount(
            amount=self.amount / divisor, currency=self.currency, precision=self.precision
        )

    def __neg__(self) -> CurrencyAmount:
        """Negate the currency amount.

        Returns:
            New CurrencyAmount with negated amount.
        """
        return CurrencyAmount(amount=-self.amount, currency=self.currency, precision=self.precision)

    def __abs__(self) -> CurrencyAmount:
        """Get absolute value of currency amount.

        Returns:
            New CurrencyAmount with absolute amount.
        """
        return CurrencyAmount(
            amount=abs(self.amount), currency=self.currency, precision=self.precision
        )

    def __eq__(self, other: object) -> bool:
        """Check equality with another CurrencyAmount.

        Returns:
            True if amounts and currencies are equal.
        """
        if not isinstance(other, CurrencyAmount):
            return False
        return self.amount == other.amount and self.currency == other.currency

    def __lt__(self, other: CurrencyAmount) -> bool:
        """Compare if this amount is less than another (same currency only).

        Returns:
            True if this amount is less than other.

        Raises:
            CurrencyMismatchError: If currencies don't match.
        """
        if self.currency != other.currency:
            raise CurrencyMismatchError(
                currency1=self.currency, currency2=other.currency, operation="compare"
            )
        return self.amount < other.amount

    def __le__(self, other: CurrencyAmount) -> bool:
        """Compare if this amount is less than or equal to another.

        Returns:
            True if this amount is less than or equal to other.
        """
        return self < other or self == other

    def __gt__(self, other: CurrencyAmount) -> bool:
        """Compare if this amount is greater than another.

        Returns:
            True if this amount is greater than other.

        Raises:
            CurrencyMismatchError: If currencies don't match.
        """
        if self.currency != other.currency:
            raise CurrencyMismatchError(
                currency1=self.currency, currency2=other.currency, operation="compare"
            )
        return self.amount > other.amount

    def __ge__(self, other: CurrencyAmount) -> bool:
        """Compare if this amount is greater than or equal to another.

        Returns:
            True if this amount is greater than or equal to other.
        """
        return self > other or self == other

    @property
    def is_positive(self) -> bool:
        """Check if amount is positive."""
        return self.amount > 0

    @property
    def is_negative(self) -> bool:
        """Check if amount is negative."""
        return self.amount < 0

    @property
    def is_zero(self) -> bool:
        """Check if amount is zero."""
        return self.amount == 0

    def quantize_to_precision(self, precision: int | None = None) -> CurrencyAmount:
        """Quantize amount to specified precision.

        Returns:
            New CurrencyAmount with quantized amount.
        """
        target_precision = precision or self.precision
        if target_precision is None:
            return self

        quantizer = Decimal(10) ** -target_precision
        return CurrencyAmount(
            amount=self.amount.quantize(quantizer),
            currency=self.currency,
            precision=target_precision,
        )

    def to_display_string(self, precision: int | None = None) -> str:
        """Format currency amount for display.

        Returns:
            Formatted string with amount and currency.
        """
        display_precision = precision or self.precision or 2
        formatted_amount = f"{self.amount:.{display_precision}f}"
        return f"{formatted_amount} {self.currency}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization.

        Returns:
            Dictionary representation of currency amount.
        """
        return {"amount": str(self.amount), "currency": self.currency, "precision": self.precision}

    @classmethod
    def zero(cls, currency: str, precision: int | None = None) -> CurrencyAmount:
        """Create a zero amount in specified currency.

        Returns:
            New CurrencyAmount with zero amount.
        """
        return cls(amount=Decimal(0), currency=currency, precision=precision)
