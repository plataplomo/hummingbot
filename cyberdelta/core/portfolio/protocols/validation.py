"""Validation protocols for portfolio module."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable


if TYPE_CHECKING:
    from cyberdelta.core.models import Trade
    from cyberdelta.core.portfolio.models.portfolio_state import PortfolioState


@runtime_checkable
class Validatable(Protocol):
    """Protocol for objects that can validate themselves.

    This protocol defines the interface for objects that can:
    - Perform self-validation
    - Return structured validation results
    - Integrate with Pydantic validation systems

    Note: Uses validate_state() to avoid conflict with Pydantic's validate() classmethod
    """

    def validate_state(self) -> ValidationResult:
        """Validate the object and return validation results.

        Returns:
            ValidationResult containing validation status and any errors/warnings
        """
        ...


@runtime_checkable
class ValidationServiceProtocol(Protocol):
    """Protocol for validation services."""

    async def validate_trade(self, trade: Trade) -> ValidationResult:
        """Validate a trade.

        Args:
            trade: Trade to validate

        Returns:
            ValidationResult with validation status
        """
        ...

    async def validate_portfolio_state(self, state: PortfolioState) -> ValidationResult:
        """Validate portfolio state.

        Args:
            state: Portfolio state to validate

        Returns:
            ValidationResult with validation status
        """
        ...


# Import ValidationResult from models to avoid circular imports
# This will be available after models are created
if TYPE_CHECKING:
    from cyberdelta.core.portfolio.models.base import ValidationResult
