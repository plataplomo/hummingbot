"""Trading domain components.

This module provides decomposed trading components for order execution,
validation, fill processing, and simulation following CODING_STANDARDS.md.
"""

# Core trading service
# Execution components
from cyberdelta.domain.trading.execution import ExecutionEngine, OrderTracker

# Fill processing components
from cyberdelta.domain.trading.fills import FeeCalculator, FillHandler, FillProcessor

# Simulation components
from cyberdelta.domain.trading.simulation import SafeModeWrapper, SimulatedFill
from cyberdelta.domain.trading.trading_service import TradingService

# Validation components
from cyberdelta.domain.trading.validation import (
    ExchangeValidator,
    MarketValidator,
    OrderModificationValidator,
    OrderValidator,
    PortfolioValidator,
    RiskValidator,
)


__all__ = [
    "ExchangeValidator",
    "ExecutionEngine",
    "FeeCalculator",
    "FillHandler",
    "FillProcessor",
    "MarketValidator",
    "OrderModificationValidator",
    "OrderTracker",
    "OrderValidator",
    "PortfolioValidator",
    "RiskValidator",
    "SafeModeWrapper",
    "SimulatedFill",
    "TradingService",
]
