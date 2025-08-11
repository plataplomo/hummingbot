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
from .domain.trading import SymbolServiceProtocol, TradingEventHandlerProtocol
from .domain.workflows import WorkflowHandler

# Infrastructure protocols
from .infrastructure.monitoring import HealthCheckable, MetricsProvider

# Service protocols for event system
from .services import AlertService, PortfolioService, RiskService, StateService, TradingService


__all__ = [
    # All protocols and services (sorted)
    "AlertService",  # Service protocol
    "BalanceManagerProtocol",  # Portfolio domain protocol
    "HealthCheckable",  # Infrastructure protocol
    "MetricsProvider",  # Infrastructure protocol
    "PnLCalculatorProtocol",  # Portfolio domain protocol
    "PortfolioService",  # Service protocol
    "PortfolioStateManagerProtocol",  # Portfolio domain protocol
    "PortfolioStorageProtocol",  # Portfolio domain protocol
    "PositionManagerProtocol",  # Portfolio domain protocol
    "ReconciliationEngineProtocol",  # Portfolio domain protocol
    "RiskService",  # Service protocol
    "StateService",  # Service protocol
    "StorageError",  # Storage exception
    "SymbolServiceProtocol",  # Trading domain protocol
    "TradingEventHandlerProtocol",  # Trading domain protocol
    "TradingService",  # Service protocol
    "WorkflowHandler",  # Workflow domain protocol
]
