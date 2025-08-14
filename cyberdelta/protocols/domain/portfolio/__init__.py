"""Portfolio domain protocols.

This module provides a centralized import location for all portfolio
protocols while maintaining modular organization in separate files.
"""

from __future__ import annotations

from cyberdelta.exceptions import StorageError

from .balance_management import BalanceManagerProtocol
from .pnl_calculation import PnLCalculatorProtocol
from .position_management import PositionManagerProtocol
from .reconciliation import ReconciliationEngineProtocol
from .state_management import PortfolioStateManagerProtocol
from .storage import PortfolioStorageProtocol


__all__ = [
    "BalanceManagerProtocol",
    "PnLCalculatorProtocol",
    "PortfolioStateManagerProtocol",
    "PortfolioStorageProtocol",
    "PositionManagerProtocol",
    "ReconciliationEngineProtocol",
    "StorageError",
]
