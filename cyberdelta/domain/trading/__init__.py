"""Trading domain components.

This module provides decomposed trading components for order execution,
validation, fill processing, and simulation following CODING_STANDARDS.md.
"""

# Core trading service
# Execution components
from cyberdelta.domain.trading.execution import ExecutionEngine, OrderTracker

# Fill processing components
from cyberdelta.domain.trading.fills import FillHandler, FillProcessor

# Simulation components
from cyberdelta.domain.trading.simulation import SafeModeWrapper

# Validation components
from cyberdelta.domain.trading.trading_event_handlers import (
    TradingOrderEventHandler,
    TradingPositionEventHandler,
)
from cyberdelta.domain.trading.trading_service import TradingService
from cyberdelta.infrastructure.validation import ValidationService


__all__ = [
    "ExecutionEngine",
    "FillHandler",
    "FillProcessor",
    "OrderTracker",
    "SafeModeWrapper",
    "TradingOrderEventHandler",
    "TradingPositionEventHandler",
    "TradingService",
    "ValidationService",
]
