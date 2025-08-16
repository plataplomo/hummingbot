"""Data state enums for validation and error handling.

This module contains enums that define data presence and availability states
used across the API layer for validation and error handling.
"""

from enum import Enum


class DataPresenceState(Enum):
    """State of data presence for error handling.

    Replaces boolean has_data parameter.
    """

    PRESENT = "present"
    """Data is present (was has_data=True)."""

    ABSENT = "absent"
    """Data is absent (was has_data=False)."""

    @property
    def is_present(self) -> bool:
        """Check if data is present."""
        return self == DataPresenceState.PRESENT


class FieldPresenceState(Enum):
    """State of field presence in messages.

    Replaces boolean has_field parameters in validation checks.
    """

    PRESENT = "present"
    """Field is present in the message."""

    ABSENT = "absent"
    """Field is absent from the message."""

    @property
    def is_present(self) -> bool:
        """Check if field is present."""
        return self == FieldPresenceState.PRESENT


__all__ = [
    "DataPresenceState",
    "FieldPresenceState",
]
