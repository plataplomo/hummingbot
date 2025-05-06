"""
Typing Utilities and TypeGuards
-------------------------------

This module provides custom typing utilities, including TypeGuards,
to assist with static analysis and type narrowing in complex validation scenarios.
"""

from collections.abc import Sequence
from decimal import Decimal
from typing import Any, TypeGuard, Union

# Type alias for types that can potentially be parsed into a Decimal
PotentialDecimalInput = Union[str, int, float, Decimal]

# Type alias for a sequence (list or tuple) expected to hold price/quantity pairs
LevelSequence = Sequence[Any]  # Using Sequence for broader compatibility


def is_sequence_of_any(val: object) -> TypeGuard[Sequence[Any]]:
    """
    Checks if the value is a Sequence (like list or tuple).

    Used as a TypeGuard to narrow the type for static analysis after validation,
    allowing subsequent checks like len().

    Args:
        val: The value to check.

    Returns:
        True if val is a Sequence, False otherwise.
    """
    # Broad check, relying on Sequence protocol primarily
    return isinstance(val, Sequence) and not isinstance(val, (str, bytes))


def is_potential_decimal_input(val: object) -> TypeGuard[PotentialDecimalInput]:
    """
    Checks if the value's type is suitable for attempting Decimal parsing.

    Args:
        val: The value to check.

    Returns:
        True if the value is a str, int, float, or Decimal, False otherwise.
    """
    return isinstance(val, (str, int, float, Decimal))
