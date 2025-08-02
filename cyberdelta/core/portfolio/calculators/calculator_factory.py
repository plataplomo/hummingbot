"""Calculator factory with direct AppSettings access following risk module patterns."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic.dataclasses import dataclass

from cyberdelta.config import AppSettings
from cyberdelta.core.portfolio.calculators.performance_calculator import PerformanceCalculator
from cyberdelta.core.portfolio.calculators.pnl.realized_pnl_calculator import RealizedPnLCalculator
from cyberdelta.core.portfolio.exceptions import CalculatorCreationError


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.models.base import BaseStateModel
    from cyberdelta.core.portfolio.protocols import StateContainerProtocol


@dataclass
class CalculatorSet:
    """Set of all calculators with type safety."""

    performance: PerformanceCalculator
    realized_pnl: RealizedPnLCalculator


class CalculatorFactory:
    """Factory for creating calculators with direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access
    - No dependency injection
    - Protocol-based dependencies
    - Simple direct instantiation
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol[BaseStateModel],
    ) -> None:
        """Initialize the calculator factory.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for data access
        """
        self.app_settings = app_settings
        self.state_container = state_container

    def create_performance_calculator(self) -> PerformanceCalculator:
        """Create a performance calculator.

        Returns:
            Configured PerformanceCalculator instance
        """
        return PerformanceCalculator(
            app_settings=self.app_settings,
            state_container=self.state_container,
        )

    def create_realized_pnl_calculator(self) -> RealizedPnLCalculator:
        """Create a realized P&L calculator.

        Returns:
            Configured RealizedPnLCalculator instance
        """
        return RealizedPnLCalculator(
            app_settings=self.app_settings,
            state_container=self.state_container,
        )

    def create_all_calculators(self) -> CalculatorSet:
        """Create all available calculators.

        Returns:
            CalculatorSet containing all calculator instances with validation

        Raises:
            CalculatorCreationError: If any calculator creation fails
        """
        try:
            performance_calc = self.create_performance_calculator()
            realized_pnl_calc = self.create_realized_pnl_calculator()

            return CalculatorSet(
                performance=performance_calc,
                realized_pnl=realized_pnl_calc,
            )
        except Exception as e:
            raise CalculatorCreationError(error_details=str(e)) from e
