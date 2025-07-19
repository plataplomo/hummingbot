"""Signal enumeration types for CyberDeltaEngine.

This module defines signal-related enums that are shared across
the entire system to avoid circular imports.
"""

from enum import Enum


class SignalType(Enum):
    """Enum representing the type of a trading signal.

    Used for strategy logic and event handling.
    - ENTER_LONG: Signal to enter a long position.
    - EXIT_LONG: Signal to exit a long position.
    - ENTER_SHORT: Signal to enter a short position.
    - EXIT_SHORT: Signal to exit a short position.
    - HOLD: Signal to maintain current state.
    - REBALANCE: Signal to adjust position to target.
    """

    ENTER_LONG = "ENTER_LONG"
    EXIT_LONG = "EXIT_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT_SHORT = "EXIT_SHORT"
    HOLD = "HOLD"  # Signal to maintain current state
    REBALANCE = "REBALANCE"  # Signal to adjust position to target
