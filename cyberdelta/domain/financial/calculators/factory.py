"""Factory for creating financial calculators based on configuration.

Provides configuration-driven calculator instantiation following CODING_STANDARDS.md.
"""

from __future__ import annotations

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.financial.calculators.fifo_calculator import FIFOCalculator
from cyberdelta.domain.financial.calculators.mark_to_market_calculator import MarkToMarketCalculator
from cyberdelta.domain.financial.calculators.performance_metrics_calculator import (
    PerformanceMetricsCalculator,
)
from cyberdelta.protocols.financial import FeeCalculatorProtocol


logger = get_logger(__name__)

# Type alias for PnL calculator types
PnLCalculatorType = MarkToMarketCalculator | FIFOCalculator


class CalculatorFactory:
    """Factory to create calculators based on configuration.

    Following CODING_STANDARDS.md:
    - All calculator selection based on AppSettings configuration
    - No hardcoded defaults or assumptions
    - Explicit configuration-driven behavior
    """

    @staticmethod
    def create_pnl_calculator(
        config: AppSettings, fee_calculator: FeeCalculatorProtocol | None = None
    ) -> PnLCalculatorType:
        """Create PnL calculator based on config.financial.pnl.calculation_method.

        Args:
            config: Application settings containing financial configuration
            fee_calculator: Optional fee calculator for fee-inclusive calculations

        Returns:
            Appropriate PnL calculator implementation based on configuration

        Raises:
            ValueError: If unknown calculation method in configuration
        """
        method = config.financial.pnl.calculation_method

        logger.info(
            "creating_pnl_calculator",
            method=method,
            has_fee_calculator=fee_calculator is not None,
        )

        # Map of supported calculation methods
        if method == "fifo":
            logger.debug("creating_fifo_calculator")
            return FIFOCalculator(config, fee_calculator)

        if method == "mark_to_market":
            logger.debug("creating_mark_to_market_calculator")
            return MarkToMarketCalculator(config, fee_calculator)

        # Check for not yet implemented methods
        not_implemented = ["lifo", "weighted_average"]
        if method in not_implemented:
            msg = f"{method.upper()} calculation method not yet implemented"
            logger.error("unsupported_pnl_calculation_method", method=method)
            raise ValueError(msg)

        # Following CODING_STANDARDS.md: No fallbacks, fail fast
        msg = f"Unknown PnL calculation method in configuration: {method}"
        logger.error("unknown_pnl_calculation_method", method=method)
        raise ValueError(msg)

    @staticmethod
    def create_performance_calculator(config: AppSettings) -> PerformanceMetricsCalculator:
        """Create performance metrics calculator.

        Args:
            config: Application settings containing financial configuration

        Returns:
            Performance metrics calculator instance
        """
        logger.info("creating_performance_calculator")
        return PerformanceMetricsCalculator(config)

    @staticmethod
    def create_all_calculators(
        config: AppSettings, fee_calculator: FeeCalculatorProtocol | None = None
    ) -> dict[str, object]:
        """Create all calculator types for convenience.

        Args:
            config: Application settings containing financial configuration
            fee_calculator: Optional fee calculator for fee-inclusive calculations

        Returns:
            Dictionary containing all calculator instances:
            - pnl: PnL calculator (type based on config)
            - performance: Performance metrics calculator
        """
        logger.info("creating_all_calculators")

        return {
            "pnl": CalculatorFactory.create_pnl_calculator(config, fee_calculator),
            "performance": CalculatorFactory.create_performance_calculator(config),
        }
