"""Financial domain - pure mathematical calculation utilities.

The financial domain is a utility layer for pure mathematical calculations only.
It does NOT replace business logic domains like Risk or Portfolio.

Calculator modules in /calculators/:
- mark_to_market_calculator: Standard mark-to-market PnL calculations
- fifo_calculator: FIFO accounting method for regulatory compliance
- performance_metrics_calculator: Performance metrics calculations
- factory: Configuration-driven calculator creation

Main modules:
- fee_calculator: Fee calculations (standalone)
- factory: Comprehensive financial services factory (main entry point)
"""

# Import calculators from the calculators submodule
from cyberdelta.domain.financial.calculators import (
    CalculatorFactory,
    FIFOCalculator,
    MarkToMarketCalculator,
    PerformanceMetricsCalculator,
)
from cyberdelta.domain.financial.factory import (
    FinancialServicesFactory,
    create_default_fee_calculator,
    create_default_pnl_calculator,
)
from cyberdelta.domain.financial.fee_calculator import FeeCalculator


__all__ = [
    "CalculatorFactory",
    "FIFOCalculator",
    "FeeCalculator",
    "FinancialServicesFactory",
    "MarkToMarketCalculator",
    "PerformanceMetricsCalculator",
    "create_default_fee_calculator",
    "create_default_pnl_calculator",
]
