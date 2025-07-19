"""Base classes for portfolio components with strong typing and direct AppSettings access."""

from cyberdelta.core.portfolio.base.typed_calculator import (
    CalculationResult,
    TypedCalculator,
)
from cyberdelta.core.portfolio.base.typed_state_manager import (
    StateUpdate,
    TypedStateManager,
)


__all__ = [
    "CalculationResult",
    "StateUpdate",
    "TypedCalculator",
    "TypedStateManager",
]
