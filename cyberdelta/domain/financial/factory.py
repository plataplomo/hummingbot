"""Comprehensive Financial Services Factory.

Single entry point for creating all financial calculation services with proper
dependency injection and configuration management.
"""

from __future__ import annotations

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.financial.calculators.factory import CalculatorFactory, PnLCalculatorType
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
    def create_performance_calculator(config: AppSettings) -> object:
        """Create performance metrics calculator.

        Args:
            config: Application settings containing performance configuration

        Returns:
            Performance metrics calculator
        """
        logger.info("creating_performance_calculator")
        return CalculatorFactory.create_performance_calculator(config)

    @staticmethod
    def create_all_financial_services(config: AppSettings) -> dict[str, object]:
        """Create complete financial services suite.

        Args:
            config: Application settings containing all financial configuration

        Returns:
            Dictionary containing all financial services:
            - fee_calculator: Fee calculation service
            - pnl_calculator: PnL calculation service (with fee support)
            - performance_calculator: Performance metrics service
        """
        logger.info("creating_complete_financial_services_suite")

        # Create fee calculator first (used by others)
        fee_calculator = FinancialServicesFactory.create_fee_calculator(config)

        # Create all calculators with proper dependencies
        pnl_calculator = CalculatorFactory.create_pnl_calculator(config, fee_calculator)
        performance_calculator = CalculatorFactory.create_performance_calculator(config)

        services: dict[str, object] = {
            "fee_calculator": fee_calculator,
            "pnl_calculator": pnl_calculator,
            "performance_calculator": performance_calculator,
        }

        logger.info(
            "financial_services_created",
            services=list(services.keys()),
            pnl_calculator_type=type(pnl_calculator).__name__,
        )

        return services


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
