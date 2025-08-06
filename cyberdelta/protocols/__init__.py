"""Centralized protocols for CyberDeltaEngine.

This module provides a unified location for all protocol definitions
used throughout the system, following clean architecture principles.
"""

from __future__ import annotations

# Domain protocols
from .domain.portfolio import (
    BalanceManagerProtocol,
    PnLCalculatorProtocol,
    PortfolioStateManagerProtocol,
    PortfolioStorageProtocol,
    PositionManagerProtocol,
    ReconciliationEngineProtocol,
    StorageError,
)

# No external protocols - use actual base classes from cyberdelta.apis.base
# Infrastructure protocols
from .infrastructure.monitoring import HealthCheckable


__all__ = [
    # Portfolio domain protocols
    "BalanceManagerProtocol",
    # Infrastructure protocols
    "HealthCheckable",
    "PnLCalculatorProtocol",
    "PortfolioStateManagerProtocol",
    "PortfolioStorageProtocol",
    "PositionManagerProtocol",
    "ReconciliationEngineProtocol",
    # Storage exceptions
    "StorageError",
]
