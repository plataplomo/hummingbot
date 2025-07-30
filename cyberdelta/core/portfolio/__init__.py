"""Portfolio management package for CyberDeltaEngine.

This package provides a modular, scalable architecture for portfolio state management,
P&L calculations, and trade processing. It replaces the monolithic legacy tracker
with a collection of specialized components that work together.

Key Components:
- Managers: State management for balances, positions, orders
- Calculators: Financial calculations for P&L, exposure, metrics
- Services: Infrastructure services for pricing, caching, persistence
- Screening: Data validation and business rules
- Events: Event handling for portfolio changes
"""

# Import main orchestrator for easy access
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager


__all__ = [
    "PortfolioStateManager",
]
