"""Comprehensive Financial Services Factory.

Single entry point for creating all financial calculation services with proper
dependency injection and configuration management.
"""

from __future__ import annotations

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.financial.calculators.factory import CalculatorFactory, PnLCalculatorType
from cyberdelta.domain.financial.calculators.performance_metrics_calculator import (
    PerformanceMetricsCalculator,
)
from cyberdelta.domain.financial.fee_calculator import FeeCalculator
from cyberdelta.protocols.financial import FeeCalculatorProtocol


logger = get_logger(__name__)


class FinancialServicesFactory:
    """Factory for creating all financial calculation services.

    Provides centralized creation of:
    - PnL calculators (Mark-to-Market, FIFO, etc.)
    - Fee calculators
    - Performance metrics calculators

    All creation is configuration-driven from AppSettings following CODING_STANDARDS.md.
    """

    @staticmethod
    def create_fee_calculator(config: AppSettings) -> FeeCalculatorProtocol:
        """Create fee calculator based on configuration.

        Args:
            config: Application settings containing fee configuration

        Returns:
            Fee calculator implementation
        """
        logger.info("creating_fee_calculator")
        return FeeCalculator()

    @staticmethod
    def create_pnl_calculator(config: AppSettings) -> PnLCalculatorType:
        """Create PnL calculator with fee calculator dependency.

        Args:
            config: Application settings containing financial configuration

        Returns:
            PnL calculator with fee calculation support
        """
        logger.info("creating_pnl_calculator_with_fees")

        # Create fee calculator first
        fee_calculator = FinancialServicesFactory.create_fee_calculator(config)

        # Create PnL calculator with fee dependency
        return CalculatorFactory.create_pnl_calculator(config, fee_calculator)

    @staticmethod
    def create_performance_calculator(config: AppSettings) -> PerformanceMetricsCalculator:
        """Create performance metrics calculator.

        Args:
            config: Application settings containing performance configuration

        Returns:
            Performance metrics calculator with type-safe interface
        """
        logger.info("creating_performance_calculator")
        return CalculatorFactory.create_performance_calculator(config)


# Convenience functions for common use cases
def create_default_pnl_calculator(config: AppSettings) -> PnLCalculatorType:
    """Create default PnL calculator for single-service usage.

    Args:
        config: Application settings

    Returns:
        Configured PnL calculator ready for use
    """
    return FinancialServicesFactory.create_pnl_calculator(config)


def create_default_fee_calculator(config: AppSettings) -> FeeCalculatorProtocol:
    """Create default fee calculator for single-service usage.

    Args:
        config: Application settings

    Returns:
        Configured fee calculator ready for use
    """
    return FinancialServicesFactory.create_fee_calculator(config)
