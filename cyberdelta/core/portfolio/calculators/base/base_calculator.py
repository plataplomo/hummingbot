"""Base calculator class for all portfolio calculations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeVar

from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from collections.abc import Mapping

logger = get_logger(__name__)

# Type variables for generic calculator constraints
R_co = TypeVar("R_co", covariant=True, bound=object)  # Result type from calculation


class CalculatorProtocol[R_co](Protocol):
    """Protocol for all portfolio calculators.

    This protocol allows implementations to have specific typed parameters
    while maintaining a common interface contract.

    Args:
        R_co: The type of result this calculator produces
    """

    async def calculate(self, *args: object, **kwargs: object) -> R_co:
        """Perform the calculation with implementation-specific parameters."""
        ...

    def validate_inputs(self, *args: object, **kwargs: object) -> None:
        """Validate calculation inputs with implementation-specific parameters."""
        ...


class BaseCalculator[R_co]:
    """Base class for all portfolio calculators.

    Provides common functionality for financial calculations.
    Concrete implementations should define their own specific method signatures
    for calculate() and validate_inputs() methods.

    Args:
        R_co: The type of result this calculator produces
    """

    def __init__(self, name: str, config: Mapping[str, object] | None = None) -> None:
        """Initialize the base calculator.

        Args:
            name: Human-readable name for this calculator
            config: Optional configuration dictionary
        """
        self.name = name
        self.config = config or {}

        logger.debug(
            "calculator_created", calculator_name=name, calculator_type=self.__class__.__name__
        )

    # Note: Concrete implementations should define their own calculate and validate_inputs
    # methods with specific typed parameters. This base class provides common utilities
    # without requiring abstract method overrides that cause LSP violations.

    def get_calculator_info(self) -> dict[str, object]:
        """Get calculator information for debugging and monitoring.

        Returns:
            Dictionary with calculator name, config, and type information
        """
        return {
            "name": self.name,
            "type": self.__class__.__name__,
            "config": self.config,
        }

    def __repr__(self) -> str:
        """String representation of the calculator.

        Returns:
            String representation showing class name and calculator name
        """
        return f"{self.__class__.__name__}(name='{self.name}')"
