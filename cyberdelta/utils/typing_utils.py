"""
CyberDeltaEngine: Typing Utilities
---------------------------------

Helper functions and type definitions related to static typing.
"""

from typing import TypeGuard, TypeVar

_T = TypeVar("_T")


def is_sized_list_or_tuple(val: object) -> TypeGuard[list[_T] | tuple[_T, ...]]:
    """
    Check if a value is a list or tuple using TypeGuard for sized sequences.

    This helps type checkers understand that `len()` can be safely called
    after this check, even if element types are unknown initially.

    Args:
        val: The value to check.

    Returns:
        True if the value is a list or tuple, False otherwise.
    """
    return isinstance(val, list | tuple)


# Prevent accidental execution
if __name__ == "__main__":
    raise RuntimeError(f"{__file__} is not intended to be run directly.")
