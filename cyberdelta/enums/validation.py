"""Validation enumeration types for CyberDeltaEngine.

This module defines validation-related enums used by the unified
validation framework to categorize and organize validation rules.
"""

from enum import Enum


class ValidationCategory(Enum):
    """Validation rule categories for organized rule execution.

    Categories are executed in priority order to fail fast on critical errors.
    Inspired by Nautilus Trader's pre-trade risk check architecture.
    """

    PRECISION = "precision"  # Price/quantity precision checks (tick/lot size)
    LIMITS = "limits"  # Min/max value and quantity checks
    BALANCE = "balance"  # Sufficient funds and margin checks
    RISK = "risk"  # Risk limit and exposure checks
    MARKET = "market"  # Market status and liquidity checks
    STATE = "state"  # Order state transition checks


class TradingState(Enum):
    """Trading system state for context-aware validation.

    Different states may apply different validation rules or bypass certain checks.
    Used by the validation framework to adjust validation behavior based on
    current system state.
    """

    ACTIVE = "active"  # Normal trading operations
    HALTED = "halted"  # Trading halted, no new orders
    REDUCING = "reducing"  # Reduce-only mode, only closing positions
    RECONCILING = "reconciling"  # Startup reconciliation, relaxed validation
