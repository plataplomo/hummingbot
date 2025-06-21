"""Typing Utilities and TypeGuards.

-------------------------------

This module provides custom typing utilities, including TypeGuards,
to assist with static analysis and type narrowing in complex validation scenarios.
"""

from collections.abc import Sequence
from decimal import Decimal
from typing import Any, TypeGuard

# Type alias for types that can potentially be parsed into a Decimal
PotentialDecimalInput = str | int | float | Decimal

# Type alias for a sequence (list or tuple) expected to hold price/quantity pairs
LevelSequence = Sequence[Any]  # Using Sequence for broader compatibility


def is_sequence_of_any(val: object) -> TypeGuard[Sequence[Any]]:
    """Check if the value is a Sequence (like list or tuple).

    Used as a TypeGuard to narrow the type for static analysis after validation,
    allowing subsequent checks like len().

    Args:
        val: The value to check.

    Returns:
        True if val is a Sequence, False otherwise.

    """
    # Broad check, relying on Sequence protocol primarily
    return isinstance(val, Sequence) and not isinstance(val, str | bytes)


def is_potential_decimal_input(val: object) -> TypeGuard[PotentialDecimalInput]:
    """Check if the value's type is suitable for attempting Decimal parsing.

    Args:
        val: The value to check.

    Returns:
        True if the value is a str, int, float, or Decimal, False otherwise.

    """
    return isinstance(val, str | int | float | Decimal)


def is_dict_str_any(val: object) -> TypeGuard[dict[str, Any]]:
    """Check if the value is a dict with string keys and Any values.

    Used as a TypeGuard to narrow the type for static analysis after validation.

    Args:
        val: The value to check.

    Returns:
        True if val is a dict[str, Any], False otherwise.
    """
    return isinstance(val, dict)


def is_list_any(val: object) -> TypeGuard[list[Any]]:
    """Check if the value is a list of Any.

    Used as a TypeGuard to narrow the type for static analysis after validation.

    Args:
        val: The value to check.

    Returns:
        True if val is a list[Any], False otherwise.
    """
    return isinstance(val, list)
