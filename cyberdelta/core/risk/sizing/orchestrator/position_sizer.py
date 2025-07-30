"""Position sizing orchestrator with direct AppSettings access."""

import asyncio
from collections.abc import Coroutine
from decimal import Decimal
from typing import Any

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.exceptions.base_exceptions import RiskSizingError
from cyberdelta.core.risk.sizing.interfaces.sizing_interfaces import (
    BaseSizerInterface,
)
from cyberdelta.core.risk.sizing.models.sizing_result import (
    SizedOpportunity,
    SizingContext,
    SizingResult,
)
from cyberdelta.core.risk.sizing.strategies.typed_base_sizer import TypedBaseSizer
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Type alias for sizers that can be used in the orchestrator
OrchestratorSizer = BaseSizerInterface | TypedBaseSizer


class PositionSizer:
    """Orchestrates position sizing using different strategies."""

    def __init__(
        self,
        sizer: OrchestratorSizer,
        app_settings: AppSettings,
    ) -> None:
        """Initialize the position sizer with direct AppSettings access.

        Args:
            sizer: Position sizing strategy
            app_settings: Application settings with enhanced risk configuration
        """
        self.sizer = sizer
        self.app_settings = app_settings
        self.sizing_settings = app_settings.risk.sizing
        self.logger = get_logger(self.__class__.__name__)

        # Capital management
        self.reserved_capital = Decimal(0)
        self.allocated_capital = Decimal(0)

        # Performance tracking
        self.sizing_count = 0
        self.total_sizing_time = 0.0
        self.sizing_success_rate = 0.0

        # Concurrency limits from AppSettings
        self.max_concurrent_sizing = 5  # Hardcoded as not in new config
        self.sizing_timeout = self.sizing_settings.sizing_timeout_seconds

    async def size_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        available_capital: Decimal,
    ) -> SizingResult:
        """Size a single opportunity.

        Args:
            opportunity: The arbitrage opportunity to size
            available_capital: Available capital for sizing

        Returns:
            SizingResult with position size and details
        """
        try:
            # Create sizing context
            context = SizingContext(
                sizing_method=self.sizer.sizing_method,
                available_capital=available_capital,
                config=self._get_sizing_config(),
                max_allocation_per_trade=self.sizing_settings.max_portfolio_allocation,
                min_allocation_per_trade=self.sizing_settings.kelly_min_allocation,
                max_leverage=self.sizing_settings.max_leverage,
            )

            # Add metadata
            context.add_metadata("opportunity_symbol", getattr(opportunity, "symbol", "unknown"))
            context.add_metadata(
                "opportunity_exchanges",
                {
                    "long": getattr(opportunity, "long_exchange", "unknown"),
                    "short": getattr(opportunity, "short_exchange", "unknown"),
                },
            )

            # Perform sizing
            self.logger.debug("Sizing opportunity", sizer_name=self.sizer.name)

            result = await asyncio.wait_for(
                self.sizer.size(opportunity, context),
                timeout=self.sizing_timeout,
            )

            # Update performance metrics
            self._update_performance_metrics(result)

        except TimeoutError:
            self.logger.exception("Sizing timeout", timeout_seconds=self.sizing_timeout)
            return SizingResult.error_result(
                message=f"Sizing timeout after {self.sizing_timeout}s",
                details={"timeout": self.sizing_timeout},
            )
        except Exception as e:
            self.logger.exception("Sizing error")
            return SizingResult.error_result(
                message=f"Sizing error: {e!s}",
                details={"exception": str(e)},
            )
        else:
            return result

    async def size_opportunities(
        self,
        opportunities: list[ArbitrageOpportunity],
        available_capital: Decimal,
    ) -> list[SizingResult]:
        """Size multiple opportunities with capital allocation.

        Args:
            opportunities: List of arbitrage opportunities to size
            available_capital: Total available capital

        Returns:
            List of SizingResult objects
        """
        if not opportunities:
            return []

        self.logger.info(
            "Sizing opportunities",
            opportunity_count=len(opportunities),
            available_capital_usd=float(available_capital),
        )

        # Strategy 1: Equal capital allocation
        if self.sizing_settings.method == "simple":
            return await self._size_with_equal_allocation(opportunities, available_capital)

        # Strategy 2: Kelly-based allocation (default)
        return await self._size_with_kelly_allocation(opportunities, available_capital)

    async def _size_with_equal_allocation(
        self,
        opportunities: list[ArbitrageOpportunity],
        available_capital: Decimal,
    ) -> list[SizingResult]:
        """Size opportunities with equal capital allocation.

        Args:
            opportunities: List of arbitrage opportunities to size.
            available_capital: Total available capital to allocate.

        Returns:
            List of SizingResult objects with equal capital allocation.
        """
        capital_per_opportunity = available_capital / len(opportunities)

        # Create sizing tasks
        tasks: list[Coroutine[Any, Any, SizingResult]] = []
        for opportunity in opportunities:
            task = self.size_opportunity(opportunity, capital_per_opportunity)
            tasks.append(task)

        # Execute sizing in parallel with concurrency limit
        results: list[SizingResult] = []
        for i in range(0, len(tasks), self.max_concurrent_sizing):
            batch = tasks[i : i + self.max_concurrent_sizing]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)

            # Handle exceptions
            for result in batch_results:
                if isinstance(result, Exception):
                    error_result = SizingResult.error_result(
                        message=f"Sizing exception: {result!s}",
                        details={"exception": str(result)},
                    )
                    results.append(error_result)
                elif isinstance(result, SizingResult):
                    results.append(result)
                else:
                    # Shouldn't happen, but handle gracefully
                    error_result = SizingResult.error_result(
                        message="Invalid result type from sizing",
                        details={"result_type": str(type(result))},
                    )
                    results.append(error_result)

        return results

    async def _size_with_kelly_allocation(
        self,
        opportunities: list[ArbitrageOpportunity],
        available_capital: Decimal,
    ) -> list[SizingResult]:
        """Size opportunities using Kelly-based allocation.

        Args:
            opportunities: List of arbitrage opportunities to size.
            available_capital: Total available capital to allocate.

        Returns:
            List of SizingResult objects with Kelly-based capital allocation.
        """
        # First pass: calculate Kelly fractions for all opportunities
        kelly_fractions: list[Decimal] = []
        total_kelly = Decimal(0)

        for opportunity in opportunities:
            # Use small amount for Kelly calculation
            small_capital = min(available_capital / 10, Decimal(1000))
            context = SizingContext(
                sizing_method=self.sizer.sizing_method,
                available_capital=small_capital,
                config=self._get_sizing_config(),
            )

            # Get Kelly fraction estimate
            try:
                result = await self.sizer.size(opportunity, context)
                if result.success and result.kelly_fraction:
                    kelly_fraction = result.kelly_fraction
                else:
                    kelly_fraction = Decimal("0.01")  # Default small fraction
            except (RiskSizingError, ValueError, TypeError, ArithmeticError):
                kelly_fraction = Decimal("0.01")  # Safe default

            kelly_fractions.append(kelly_fraction)
            total_kelly += kelly_fraction

        # Second pass: allocate capital based on Kelly fractions
        results: list[SizingResult] = []
        for i, opportunity in enumerate(opportunities):
            if total_kelly > 0:
                allocation_ratio = kelly_fractions[i] / total_kelly
                allocated_capital = available_capital * allocation_ratio
            else:
                allocated_capital = available_capital / len(opportunities)

            # Size with allocated capital
            result = await self.size_opportunity(opportunity, allocated_capital)
            results.append(result)

        return results

    async def _size_with_risk_weighted_allocation(
        self,
        opportunities: list[ArbitrageOpportunity],
        available_capital: Decimal,
    ) -> list[SizingResult]:
        """Size opportunities using risk-weighted allocation.

        Args:
            opportunities: List of arbitrage opportunities to size.
            available_capital: Total available capital to allocate.

        Returns:
            List of SizingResult objects with risk-weighted capital allocation.
        """
        # Calculate risk scores for all opportunities
        risk_scores: list[Decimal] = []
        total_risk_score = Decimal(0)

        for opportunity in opportunities:
            # Calculate risk score based on spread and volatility
            risk_score = self._calculate_risk_score(opportunity)
            risk_scores.append(risk_score)
            total_risk_score += risk_score

        # Allocate capital inversely proportional to risk
        results: list[SizingResult] = []
        for i, opportunity in enumerate(opportunities):
            if total_risk_score > 0:
                # Lower risk = higher allocation
                inverse_risk = (1 / risk_scores[i]) if risk_scores[i] > 0 else Decimal(1)
                total_inverse_risk = sum(
                    1 / score if score > 0 else Decimal(1) for score in risk_scores
                )
                allocation_ratio = inverse_risk / total_inverse_risk
                allocated_capital = available_capital * allocation_ratio
            else:
                allocated_capital = available_capital / len(opportunities)

            # Size with allocated capital
            result = await self.size_opportunity(opportunity, allocated_capital)
            results.append(result)

        return results

    def _calculate_risk_score(self, opportunity: ArbitrageOpportunity) -> Decimal:
        """Calculate risk score for an opportunity.

        Args:
            opportunity: The arbitrage opportunity to calculate risk score for.

        Returns:
            Risk score as a Decimal (higher value indicates higher risk).
        """
        risk_score = Decimal("1.0")  # Base risk score

        # Adjust based on spread (higher spread = lower risk)
        spread_percentage = getattr(opportunity, "spread_percentage", None)
        if spread_percentage:
            try:
                spread_decimal = Decimal(str(spread_percentage))
                # Inverse relationship: higher spread = lower risk score
                risk_score *= 1 / (1 + spread_decimal * 10)
            except (ValueError, TypeError):
                pass

        # Adjust based on volatility (higher volatility = higher risk)
        volatility = getattr(opportunity, "volatility", None)
        if volatility:
            try:
                volatility_decimal = Decimal(str(volatility))
                # Direct relationship: higher volatility = higher risk score
                risk_score *= 1 + volatility_decimal * 5
            except (ValueError, TypeError):
                pass

        return max(Decimal("0.01"), risk_score)  # Minimum risk score

    def create_sized_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        sizing_result: SizingResult,
    ) -> SizedOpportunity:
        """Create a SizedOpportunity from sizing result.

        Args:
            opportunity: The arbitrage opportunity
            sizing_result: The sizing result

        Returns:
            SizedOpportunity instance
        """
        return SizedOpportunity.from_sizing_result(
            opportunity=opportunity,
            sizing_result=sizing_result,
            sizing_method=self.sizer.sizing_method,
        )

    def _get_sizing_config(self) -> dict[str, Any]:
        """Get sizing configuration as dictionary.

        Returns:
            Dictionary containing sizing configuration parameters.
        """
        return {
            "sizing_method": self.sizing_settings.method,
            "kelly_multiplier": float(self.sizing_settings.kelly_multiplier),
            "max_allocation": float(self.sizing_settings.max_portfolio_allocation),
            "min_allocation": float(self.sizing_settings.kelly_min_allocation),
            "max_leverage": float(self.sizing_settings.max_leverage),
            "min_volatility": float(self.sizing_settings.min_volatility),
            "max_volatility": float(self.sizing_settings.max_volatility_bound),
            "enable_validation_factors": self.sizing_settings.enable_validation_factors,
            "base_validation_factor": float(self.sizing_settings.base_validation_factor),
        }

    def _update_performance_metrics(self, result: SizingResult) -> None:
        """Update performance metrics.

        Args:
            result: The sizing result to update metrics from.
        """
        self.sizing_count += 1

        if result.execution_time_ms:
            self.total_sizing_time += result.execution_time_ms

        # Update success rate
        if result.success:
            self.sizing_success_rate = (
                self.sizing_success_rate * (self.sizing_count - 1) + 1
            ) / self.sizing_count
        else:
            self.sizing_success_rate = (
                self.sizing_success_rate * (self.sizing_count - 1)
            ) / self.sizing_count

    def set_sizer(self, sizer: OrchestratorSizer) -> None:
        """Set the active sizer strategy.

        Args:
            sizer: The sizer strategy to use.
        """
        self.sizer = sizer
        self.logger.info("Set sizer strategy", sizer_name=sizer.name)

    def get_sizer(self) -> OrchestratorSizer:
        """Get the current sizer strategy.

        Returns:
            The current sizer strategy instance.
        """
        return self.sizer

    def get_performance_stats(self) -> dict[str, Any]:
        """Get performance statistics.

        Returns:
            Dictionary containing performance statistics.
        """
        avg_sizing_time = self.total_sizing_time / self.sizing_count if self.sizing_count > 0 else 0

        return {
            "sizing_count": self.sizing_count,
            "average_sizing_time_ms": avg_sizing_time,
            "success_rate": self.sizing_success_rate,
            "current_sizer": self.sizer.name,
            "current_method": self.sizer.sizing_method,
            "reserved_capital": float(self.reserved_capital),
            "allocated_capital": float(self.allocated_capital),
        }

    def reserve_capital(self, amount: Decimal) -> None:
        """Reserve capital for pending trades."""
        self.reserved_capital += amount
        self.logger.debug(
            "Reserved capital",
            amount_usd=float(amount),
            total_reserved_usd=float(self.reserved_capital),
        )

    def release_capital(self, amount: Decimal) -> None:
        """Release reserved capital."""
        self.reserved_capital = max(Decimal(0), self.reserved_capital - amount)
        self.logger.debug(
            "Released capital",
            amount_usd=float(amount),
            total_reserved_usd=float(self.reserved_capital),
        )

    def get_available_capital(self, total_capital: Decimal) -> Decimal:
        """Get available capital after reservations.

        Args:
            total_capital: Total capital amount.

        Returns:
            Available capital after subtracting reserved capital.
        """
        return max(Decimal(0), total_capital - self.reserved_capital)

    def reset_performance_metrics(self) -> None:
        """Reset performance metrics."""
        self.sizing_count = 0
        self.total_sizing_time = 0.0
        self.sizing_success_rate = 0.0
        self.logger.info("Reset performance metrics")

    def set_concurrency_limits(self, max_concurrent: int, timeout: float) -> None:
        """Set concurrency limits.

        Args:
            max_concurrent: Maximum concurrent sizing operations
            timeout: Timeout for sizing operations
        """
        self.max_concurrent_sizing = max_concurrent
        self.sizing_timeout = timeout
        self.logger.info(
            "Set concurrency limits", max_concurrent=max_concurrent, timeout_seconds=timeout
        )

    def __str__(self) -> str:
        """String representation.

        Returns:
            String representation of the PositionSizer.
        """
        return f"PositionSizer(sizer={self.sizer.name}, method={self.sizer.sizing_method})"

    def __repr__(self) -> str:
        """Detailed representation.

        Returns:
            Detailed string representation of the PositionSizer.
        """
        return (
            f"PositionSizer(sizer={self.sizer.name}, method={self.sizer.sizing_method}, "
            f"app_settings={self.app_settings})"
        )
