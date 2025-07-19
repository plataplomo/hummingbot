"""Calculator factory with direct AppSettings access following risk module patterns."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.config import AppSettings
from cyberdelta.core.portfolio.calculators.exposure_calculator import ExposureCalculator
from cyberdelta.core.portfolio.calculators.performance_calculator import PerformanceCalculator
from cyberdelta.core.portfolio.calculators.pnl.realized_pnl_calculator import RealizedPnLCalculator


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.protocols import StateContainerProtocol


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
        state_container: StateContainerProtocol[Any],
    ) -> None:
        """Initialize the calculator factory.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for data access
        """
        self.app_settings = app_settings
        self.state_container = state_container

    def create_exposure_calculator(self) -> ExposureCalculator:
        """Create an exposure calculator.

        Returns:
            Configured ExposureCalculator instance
        """
        return ExposureCalculator(
            app_settings=self.app_settings,
            state_container=self.state_container,
        )

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

    def create_all_calculators(self) -> dict[str, object]:
        """Create all available calculators.

        Returns:
            Dictionary mapping calculator names to instances
        """
        return {
            "exposure": self.create_exposure_calculator(),
            "performance": self.create_performance_calculator(),
            "realized_pnl": self.create_realized_pnl_calculator(),
        }
