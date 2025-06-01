"""Core components of the CyberDelta trading engine."""

# Import CircuitBreakerSystem from the correct module
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

from .balance_monitor import BalanceMonitor
from .data_handler import DataHandler
from .engine import Engine

# NOTE: ExecutionHandler removed from __init__ to avoid circular import
# Import directly from cyberdelta.core.execution_handler instead
from .portfolio_tracker import PortfolioTracker
from .risk_manager import RiskManager
from .signal_generator import SignalGenerator
from .signal_queue import PrioritySignalQueue  # Correct name
from .strategy import Strategy

__all__ = [
    # Core Components
    "BalanceMonitor",
    "DataHandler",
    "Engine",
    # "ExecutionHandler",  # Removed to avoid circular import
    "CircuitBreakerSystem",
    "PortfolioTracker",
    "RiskManager",
    "SignalGenerator",
    # "SignalQueue",  # Incorrect name
    "PrioritySignalQueue",  # Correct name
    "Strategy",
]
