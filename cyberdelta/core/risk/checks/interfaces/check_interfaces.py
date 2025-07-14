"""Interfaces for opportunity checking."""

from abc import abstractmethod
from typing import Protocol

from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class BaseCheckerInterface(Protocol):
    """Protocol for individual checkers."""

    @abstractmethod
    async def check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Check an opportunity against specific criteria.
        
        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check
            
        Returns:
            CheckResult indicating success/failure with details
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the checker."""
        ...


class OpportunityCheckerInterface(Protocol):
    """Protocol for opportunity checking orchestrator."""

    @abstractmethod
    async def check(
        self,
        opportunity: ArbitrageOpportunity,
    ) -> CheckResult:
        """Check an opportunity through the full pipeline.
        
        Args:
            opportunity: The arbitrage opportunity to check
            
        Returns:
            CheckResult indicating overall success/failure
        """
        ...

    @abstractmethod
    async def check_batch(
        self,
        opportunities: list[ArbitrageOpportunity],
    ) -> list[CheckResult]:
        """Check multiple opportunities in batch.
        
        Args:
            opportunities: List of arbitrage opportunities to check
            
        Returns:
            List of CheckResult objects
        """
        ...


class CheckPipelineInterface(Protocol):
    """Protocol for check pipeline orchestrator."""

    @abstractmethod
    async def run_pipeline(
        self,
        opportunity: ArbitrageOpportunity,
        checkers: list[BaseCheckerInterface],
    ) -> CheckResult:
        """Run a pipeline of checkers against an opportunity.
        
        Args:
            opportunity: The arbitrage opportunity to check
            checkers: List of checkers to run
            
        Returns:
            CheckResult indicating overall pipeline success/failure
        """
        ...

    @abstractmethod
    def add_checker(self, checker: BaseCheckerInterface) -> None:
        """Add a checker to the pipeline."""
        ...

    @abstractmethod
    def remove_checker(self, checker_name: str) -> None:
        """Remove a checker from the pipeline."""
        ...
