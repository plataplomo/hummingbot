from __future__ import annotations  # Enable postponed evaluation

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, getcontext
from typing import TYPE_CHECKING, Any, Protocol

from cyberdelta.core.models import Position  # Needed for runtime isinstance check

# from cyberdelta.core.models import ArbitrageOpportunity, Order, OrderSide, OrderType, TradeSignal
# REMOVING this runtime import
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config
from cyberdelta.validation.circuit_breaker import BreakerState, CircuitBreakerSystem

if TYPE_CHECKING:  # Keep ArbitrageOpportunity here for type hinting only
    from cyberdelta.core.models import ArbitrageOpportunity

logger = logging.getLogger(__name__)

# Set precision for Decimal
getcontext().prec = 28  # Default precision, adjust if needed

# Define ZERO and ONE constants for clarity
ZERO = Decimal("0")
ONE = Decimal("1")


# Protocol for funding rate validator
class FundingRateValidatorProtocol(Protocol):
    def get_symbol_metrics(
        self, exchange: str, symbol: str
    ) -> dict[str, float | Decimal | None]: ...


class SizedOpportunity:
    """
    An arbitrage opportunity with calculated position sizes and risk metrics.
    """

    def __init__(
        self,
        opportunity: ArbitrageOpportunity,
        long_size: Decimal,
        short_size: Decimal,
        allocation_percentage: Decimal,
        expected_profit: Decimal,
        expected_return: Decimal,
        risk_adjusted_return: Decimal,
    ) -> None:
        """
        Initialize a sized opportunity.

        Args:
            opportunity: The base arbitrage opportunity
            long_size: Position size for long side in USD (Decimal)
            short_size: Position size for short side in USD (Decimal)
            allocation_percentage: Percentage of total capital allocated (Decimal)
            expected_profit: Expected profit in USD (Decimal)
            expected_return: Expected return as percentage (Decimal)
            risk_adjusted_return: Risk-adjusted return (Decimal)
        """
        self.opportunity = opportunity
        self.long_size = Decimal(str(long_size))
        self.short_size = Decimal(str(short_size))
        self.allocation_percentage = Decimal(str(allocation_percentage))
        self.expected_profit = Decimal(str(expected_profit))
        self.expected_return = Decimal(str(expected_return))
        self.risk_adjusted_return = Decimal(str(risk_adjusted_return))

    def __str__(self) -> str:
        """String representation of the sized opportunity."""
        return (
            f"SizedOpportunity: {self.opportunity.symbol} - "
            f"Long: {self.opportunity.long_exchange} ${self.long_size:.2f}, "
            f"Short: {self.opportunity.short_exchange} ${self.short_size:.2f}, "
            f"Alloc: {self.allocation_percentage:.2f}%, "
            f"ExpProfit: ${self.expected_profit:.2f}, "
            f"ExpReturn: {(self.expected_return * Decimal('100')):.2f}%, "
            f"RiskAdjReturn: {self.risk_adjusted_return:.4f}"
        )


class RiskManager:
    """
    Assess and size trades based on risk parameters.

    Responsible for:
    - Validating opportunities against risk constraints
    - Applying position sizing based on Kelly criterion
    - Enforcing position limits and portfolio risk controls
    - Calculating risk metrics
    """

    def __init__(
        self,
        config: Config,
        portfolio_tracker: PortfolioTracker,
        circuit_breaker_system: CircuitBreakerSystem | None = None,
        funding_rate_validator: FundingRateValidatorProtocol | None = None,
    ) -> None:
        """
        Initialize the risk manager.

        Args:
            config: Application configuration
            portfolio_tracker: Portfolio state tracking
            circuit_breaker_system: Optional system for circuit breakers
            funding_rate_validator: Optional validator for funding rate predictions.
                Must implement get_symbol_metrics(exchange: str, symbol: str).
        """
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.config = config
        self.portfolio_tracker = portfolio_tracker
        self.circuit_breaker_system = circuit_breaker_system
        self.funding_rate_validator = funding_rate_validator

        # Load GLOBAL risk parameters from config (ensure Decimal where appropriate)
        # Use the specific keys expected by tests and intended logic
        self.max_position_size = Decimal(str(config.get("risk.global.max_position_usd", "1000.0")))
        self.max_total_exposure = Decimal(
            str(config.get("risk.global.max_total_exposure_usd", "5000.0"))
        )
        # Kelly fraction is used in calculations with Decimal, convert upfront?
        # Convert kelly_fraction to Decimal for consistency in calculations
        self.kelly_fraction: Decimal = Decimal(str(config.get("risk.kelly_fraction", 0.5)))
        self.max_collateral_per_exchange: Decimal = Decimal(
            str(config.get("risk.max_collateral_per_exchange", 0.8))
        )
        self.max_leverage = Decimal(
            str(
                config.get("risk.global.max_portfolio_leverage", "5.0")
            )  # Assuming this key based on tests
        )
        self.min_liquidation_buffer: Decimal = Decimal(
            str(config.get("risk.min_liquidation_buffer", 0.2))
        )

        # Portfolio-level risk management parameters - Ensure Decimal where needed
        self.max_exposure_per_asset: Decimal = Decimal(
            str(config.get("risk.max_exposure_per_asset", 0.2))
        )
        self.max_exposure_per_exchange: Decimal = Decimal(
            str(config.get("risk.max_exposure_per_exchange", 0.5))
        )
        # Correlated exposure parameters - Removed as correlation logic is removed
        # self.max_correlated_exposure: float = float(
        #     config.get("risk.max_correlated_exposure", 0.3)
        # )
        # self.correlation_threshold: float = float(config.get("risk.correlation_threshold", 0.7))
        self.circuit_breaker_recovery_factor: Decimal = Decimal(
            str(config.get("risk.circuit_breaker_recovery_factor", 0.3))
        )
        self.min_exchange_balance = Decimal(
            str(config.get("risk_manager.min_exchange_balance", 10.0))
        )

        # Validation metric thresholds - ensure Decimal
        self.max_acceptable_rmse = Decimal(str(config.get("risk.max_acceptable_rmse", 0.05)))
        self.max_acceptable_bias = Decimal(str(config.get("risk.max_acceptable_bias", 0.02)))
        self.min_validation_factor = Decimal(str(config.get("risk.min_validation_factor", 0.2)))

        # Exchange-specific risk modifiers (used to be more conservative on certain exchanges)
        self.exchange_risk_modifiers: dict[str, float] = {}
        for exchange_id in config.get("exchanges", {}).keys():
            if config.get(f"exchanges.{exchange_id}.enabled", False):
                self.exchange_risk_modifiers[exchange_id] = config.get(
                    f"exchanges.{exchange_id}.risk_modifier", 1.0
                )

        # Configurable parameters specific to sizing/validation logic
        # These might be redundant or need clarification vs the global ones above
        # For now, ensure they load with specific keys if used elsewhere
        self.max_single_position_exposure = Decimal(
            str(self.config.get("risk.strategy.max_single_position_exposure_ratio", 0.1))
        )
        # REMOVED REDUNDANT max_total_exposure assignment
        self.max_drawdown_limit = Decimal(
            str(self.config.get("risk.global.max_drawdown_limit_ratio", 0.2))
        )
        self.min_net_funding_differential = Decimal(
            str(self.config.get("strategy.min_net_funding_differential", 0.0001))
        )
        # Ensure max_leverage_per_trade is Decimal
        self.max_leverage_per_trade = Decimal(
            str(self.config.get("risk.strategy.max_leverage_per_trade", 5.0))
        )
        self.volatility_period = self.config.get(
            "strategy.volatility_period_days", 14
        )  # Period for volatility calc

        # Internal state
        # Mypy fix [var-annotated]: Add type hint
        self.current_drawdown_metrics: dict[str, Any] = {}

    def _calculate_kelly_size(
        self, opportunity: ArbitrageOpportunity, total_capital: Decimal
    ) -> Decimal:
        """
        Calculate Kelly-based position sizing.

        Args:
            opportunity: Arbitrage opportunity
            total_capital: Total available capital (Decimal)

        Returns:
            Calculated position size in USD (Decimal), or ZERO if invalid.
        """
        if total_capital <= ZERO:
            self.logger.warning("Total capital is zero or negative, cannot calculate Kelly size.")
            return ZERO

        # Ensure required fields are present and valid
        if (
            getattr(opportunity, "expected_return", None) is None
            or getattr(opportunity, "basis_volatility", None) is None
            or opportunity.long_exchange is None
            or opportunity.short_exchange is None
        ):
            self.logger.warning(
                f"Missing required data for Kelly calculation for {opportunity.symbol}. "
                f"Return: {getattr(opportunity, 'expected_return', 'N/A')}, "
                f"Vol: {getattr(opportunity, 'basis_volatility', 'N/A')}",
            )
            return ZERO

        try:
            # Convert expected return (percentage) and volatility to Decimal
            # Use getattr with default ZERO for safe conversion
            expected_return_raw = getattr(opportunity, "expected_return", ZERO)
            basis_volatility_raw = getattr(opportunity, "basis_volatility", ZERO)
            expected_return_dec = Decimal(str(expected_return_raw))
            basis_volatility_dec = Decimal(str(basis_volatility_raw))

            # Apply exchange-specific risk modifiers to volatility
            long_modifier = Decimal(
                str(self.exchange_risk_modifiers.get(opportunity.long_exchange, 1.0))
            )
            short_modifier = Decimal(
                str(self.exchange_risk_modifiers.get(opportunity.short_exchange, 1.0))
            )
            # Average or take max? Let's average for now.
            avg_modifier = (long_modifier + short_modifier) / Decimal("2.0")
            adjusted_volatility = basis_volatility_dec * avg_modifier

            if adjusted_volatility <= ZERO:
                self.logger.warning(
                    f"Adjusted basis volatility is non-positive ({adjusted_volatility}) "
                    f"for {opportunity.symbol}. Cannot calculate Kelly size.",
                )
                # Mypy L233: Unreachable code removed (was after return)
                return ZERO

            # Variance is volatility squared
            variance = adjusted_volatility**2
            # Mypy L240: Unreachable code removed (was after return)

            # Kelly formula: f* = edge / odds = expected_return / variance
            # We use a fraction of Kelly (self.kelly_fraction)
            if variance <= ZERO:  # Avoid division by zero
                self.logger.warning(
                    f"Variance is zero or negative ({variance}) for {opportunity.symbol}. "
                    "Cannot calculate Kelly fraction."
                )
                return ZERO

            kelly_fraction = expected_return_dec / variance
            optimal_fraction = kelly_fraction * self.kelly_fraction

            # Clamp fraction between 0 and 1 (or a max allocation limit)
            # Using max_single_position_exposure as the upper limit per trade
            clamped_fraction = max(ZERO, min(optimal_fraction, self.max_single_position_exposure))

            # Calculate position size in USD
            position_size = clamped_fraction * total_capital

            self.logger.debug(
                f"Kelly Calc for {opportunity.symbol}: Return={expected_return_dec:.4f}, "
                f"Vol={basis_volatility_dec:.4f}, Mod={avg_modifier:.2f}, "
                f"AdjVol={adjusted_volatility:.4f}, Var={variance:.6f}, "
                f"KellyF={kelly_fraction:.4f}, OptimalF={optimal_fraction:.4f}, "
                f"ClampedF={clamped_fraction:.4f}, Size=${position_size:.2f}"
            )

            return position_size.quantize(Decimal("0.01"))  # Round to cents

        except (InvalidOperation, TypeError, ValueError) as e:
            self.logger.error(
                f"Error calculating Kelly size for {opportunity.symbol}: {e}. "
                f"Return: {getattr(opportunity, 'expected_return', 'N/A')}, "
                f"Vol: {getattr(opportunity, 'basis_volatility', 'N/A')}",
            )
            # Mypy L248: adjusted_volatility not defined here. Return ZERO directly.
            return ZERO

    def _check_portfolio_constraints(
        self, proposed_size: Decimal, opportunity: ArbitrageOpportunity
    ) -> tuple[bool, str | None]:
        """
        Check if adding the proposed trade violates portfolio-level constraints.

        Args:
            proposed_size: The proposed size for one leg of the trade (in USD).
                           Assumes long and short sizes are roughly equal for exposure checks.
            opportunity: The arbitrage opportunity being considered.

        Returns:
            Tuple (is_valid, reason): True if constraints are met, False otherwise with reason.
        """
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital is None or total_capital <= ZERO:
            return False, "Total capital unavailable or zero"
            # Mypy L337: Unreachable code removed (was after return)

        # 1. Max Single Position Size (Absolute USD)
        if proposed_size > self.max_position_size:
            return (
                False,
                f"Proposed size ${proposed_size:.2f} exceeds max single position size "
                f"${self.max_position_size:.2f}",
            )

        # 2. Max Single Position Size (Relative to Capital)
        max_relative_size = total_capital * self.max_single_position_exposure
        if proposed_size > max_relative_size:
            return (
                False,
                f"Proposed size ${proposed_size:.2f} exceeds max relative position size "
                f"(${max_relative_size:.2f}, {self.max_single_position_exposure:.1%})",
            )
            # Mypy L357: Unreachable code removed (was after return)

        # 3. Max Total Exposure (Absolute USD)
        current_exposure = self.calculate_total_exposure()
        if (current_exposure + proposed_size) > self.max_total_exposure:
            return (
                False,
                f"Adding ${proposed_size:.2f} would exceed max total exposure "
                f"(${self.max_total_exposure:.2f}). Current: ${current_exposure:.2f}",
            )

        # 4. Max Portfolio Leverage
        # Leverage = Total Exposure / Total Capital
        if total_capital > ZERO:  # Avoid division by zero
            projected_leverage = (current_exposure + proposed_size) / total_capital
            if projected_leverage > self.max_leverage:
                return (
                    False,
                    f"Projected leverage {projected_leverage:.2f}x exceeds max leverage "
                    f"{self.max_leverage:.2f}x",
                )

        # 5. Max Exposure Per Exchange
        # Mypy L428: opportunity is defined in scope.
        # Assuming error was spurious or related to other issues.
        long_ex = opportunity.long_exchange
        short_ex = opportunity.short_exchange
        # TODO: Reinstate exchange exposure check when PortfolioTracker
        #       .get_total_exposure_by_exchange exists
        # long_ex_exposure = self.portfolio_tracker.get_total_exposure_by_exchange(long_ex)
        # short_ex_exposure = self.portfolio_tracker.get_total_exposure_by_exchange(short_ex)
        #
        # if (long_ex_exposure + proposed_size) > (
        #     total_capital * self.max_exposure_per_exchange
        # ):
        #     return (
        #         False,
        #         f"Adding ${proposed_size:.2f} to {long_ex} exceeds max exchange exposure "
        #         f"({self.max_exposure_per_exchange:.1%}). Current: ${long_ex_exposure:.2f}",
        #     )
        # if (short_ex_exposure + proposed_size) > (
        #     total_capital * self.max_exposure_per_exchange
        # ):
        #     return (
        #         False,
        #         f"Adding ${proposed_size:.2f} to {short_ex} exceeds max exchange exposure "
        #         f"({self.max_exposure_per_exchange:.1%}). Current: ${short_ex_exposure:.2f}",
        #     )

        # 6. Max Exposure Per Asset (Base asset of the symbol)
        # Assuming symbol format like "BTC-PERP" -> base asset "BTC"
        # TODO: Reinstate asset exposure check when
        # PortfolioTracker.get_total_exposure_by_asset exists
        # asset_exposure = self.portfolio_tracker.get_total_exposure_by_asset(base_asset)
        # if (asset_exposure + proposed_size) > (total_capital * self.max_exposure_per_asset):
        #      return (
        #         False,
        #         f"Adding ${proposed_size:.2f} to {base_asset} exceeds max asset exposure "
        #         f"({self.max_exposure_per_asset:.1%}). Current: ${asset_exposures:.2f}",
        #      )

        # 7. Min Exchange Balance Check
        # Assuming USD balance check
        long_balance = self.portfolio_tracker.get_exchange_balance(long_ex, "USD")
        short_balance = self.portfolio_tracker.get_exchange_balance(short_ex, "USD")
        if (
            long_balance is None
            or long_balance.available is None
            or long_balance.available < self.min_exchange_balance
        ):
            return (
                False,
                f"Insufficient available balance on {long_ex} "
                f"(Have: ${long_balance.available if long_balance else 'N/A'}, "
                f"Min: ${self.min_exchange_balance})",
            )
        if (
            short_balance is None
            or short_balance.available is None
            or short_balance.available < self.min_exchange_balance
        ):
            return (
                False,
                f"Insufficient available balance on {short_ex} "
                f"(Have: ${short_balance.available if short_balance else 'N/A'}, "
                f"Min: ${self.min_exchange_balance})",
            )

        # TODO: Add checks for collateral requirements, liquidation buffers if possible

        return True, None

    def _apply_portfolio_exposure_management(
        self, sized_opportunities: list[SizedOpportunity]
    ) -> list[SizedOpportunity]:
        """
        Apply portfolio-level exposure limits and diversification rules.
        (Currently a placeholder - could rank, filter, or resize based on correlation, etc.)

        Args:
            sized_opportunities: List of opportunities already sized individually.

        Returns:
            Filtered/resized list of opportunities adhering to portfolio constraints.
        """
        # Placeholder: Currently just returns the list as is.
        # Future implementations could:
        # - Rank opportunities by risk-adjusted return.
        # - Iteratively add opportunities, checking cumulative exposure limits.
        # - Reduce sizes proportionally if limits are hit.
        # - Apply diversification rules (e.g., limit exposure to single asset/sector).
        # - Consider correlations between opportunities (removed for now).

        self.logger.debug(
            f"Applying portfolio exposure management to {len(sized_opportunities)} opportunities."
        )

        # Example: Simple check against total exposure
        # (redundant with _check_portfolio_constraints?)
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital is None or total_capital <= ZERO:
            self.logger.warning("Cannot apply exposure management: Total capital unavailable.")
            return []
            # Mypy L509: Unreachable code removed (was after return)

        current_exposure = self.calculate_total_exposure()
        allowed_new_exposure = self.max_total_exposure - current_exposure
        cumulative_new_exposure = ZERO
        final_opportunities = []

        # Sort by risk-adjusted return (descending) to prioritize best opportunities
        # Ensure risk_adjusted_return is not None before sorting
        sorted_opportunities = sorted(
            [opp for opp in sized_opportunities if opp.risk_adjusted_return is not None],
            key=lambda x: x.risk_adjusted_return,
            reverse=True,
        )

        for opp in sorted_opportunities:
            # Assume long/short sizes are equal for exposure calculation simplicity
            opp_exposure = max(opp.long_size, opp.short_size)
            if (cumulative_new_exposure + opp_exposure) <= allowed_new_exposure:
                final_opportunities.append(opp)
                cumulative_new_exposure += opp_exposure
            else:
                # Option 1: Skip the opportunity
                self.logger.debug(
                    f"Skipping opportunity {opp.opportunity.symbol} due to cumulative "
                    f"exposure limit. Needed: ${opp_exposure:.2f}, Remaining: "
                    f"${(allowed_new_exposure - cumulative_new_exposure):.2f}",
                )
                # Option 2: Resize the opportunity (more complex)
                # remaining_alloc = allowed_new_exposure - cumulative_new_exposure
                # if remaining_alloc > ZERO:
                #     scale_factor = remaining_alloc / opp_exposure
                #     opp.long_size *= scale_factor
                #     opp.short_size *= scale_factor
                #     # Recalculate profit/return?
                #     final_opportunities.append(opp)
                #     cumulative_new_exposure += remaining_alloc

        if len(final_opportunities) < len(sized_opportunities):
            self.logger.info(
                f"Portfolio exposure management reduced opportunities from "
                f"{len(sized_opportunities)} to {len(final_opportunities)}.",
            )

        return final_opportunities

    def _apply_portfolio_level_controls(
        self, sized_opportunity: SizedOpportunity
    ) -> SizedOpportunity | None:
        """
        Apply portfolio-level controls like drawdown limits.
        (Currently focuses on drawdown, could include correlation etc. later)

        Args:
            sized_opportunity: The opportunity sized by Kelly criterion.

        Returns:
            The potentially adjusted SizedOpportunity, or None if rejected.
        """
        # 1. Drawdown Check
        if not self.check_drawdown():
            self.logger.warning(
                f"Portfolio drawdown limit exceeded. Rejecting opportunity "
                f"{sized_opportunity.opportunity.symbol}."
            )
            return None

        # 2. Correlation Check (Removed - Placeholder)
        # if self._check_correlation_limits(sized_opportunity):
        #     self.logger.warning(f"Opportunity {sized_opportunity.opportunity.symbol}
        #     rejected due to correlation limits.")
        #     return None

        # 3. Circuit Breaker Recovery Adjustment
        # If any relevant circuit breaker is in HALF_OPEN state, reduce size
        size_modifier = ONE
        if self.circuit_breaker_system:
            long_ex = sized_opportunity.opportunity.long_exchange
            short_ex = sized_opportunity.opportunity.short_exchange
            # Check relevant breakers (e.g., API error breakers for the specific exchanges)
            long_api_breaker = self.circuit_breaker_system.get_exchange_breaker(
                long_ex, "APIErrorBreaker"
            )
            short_api_breaker = self.circuit_breaker_system.get_exchange_breaker(
                short_ex, "APIErrorBreaker"
            )

            # Check if breaker exists and is in HALF_OPEN state
            is_long_half_open = (
                long_api_breaker and long_api_breaker.state == BreakerState.HALF_OPEN
            )
            is_short_half_open = (
                short_api_breaker and short_api_breaker.state == BreakerState.HALF_OPEN
            )

            if is_long_half_open or is_short_half_open:
                self.logger.info(
                    f"Applying circuit breaker recovery factor "
                    f"({self.circuit_breaker_recovery_factor}) to opportunity "
                    f"{sized_opportunity.opportunity.symbol} due to HALF_OPEN state.",
                )
                size_modifier = self.circuit_breaker_recovery_factor

        if size_modifier < ONE:
            sized_opportunity.long_size *= size_modifier
            sized_opportunity.short_size *= size_modifier
            # Recalculate expected profit based on reduced size?
            # This assumes profit scales linearly with size, which might not be true
            # due to fees, slippage etc. For simplicity, we adjust it linearly here.
            sized_opportunity.expected_profit *= size_modifier
            # Expected return percentage should ideally remain the same if profit scales linearly
            # Risk-adjusted return might also need recalculation if risk profile changes
            self.logger.info(
                f"Adjusted size for {sized_opportunity.opportunity.symbol} due to recovery: "
                f"Long ${sized_opportunity.long_size:.2f}, "
                f"Short ${sized_opportunity.short_size:.2f}",
            )

        # Potentially add other portfolio-level adjustments here

        return sized_opportunity

    def _get_validation_metrics(self, exchange: str, symbol: str) -> Decimal | None:
        """
        Retrieve validation metrics for funding rate predictions.
        Returns a factor in [0, 1] if valid, or None if validation cannot be performed.
        """
        if not self.funding_rate_validator:
            self.logger.error(
                "No funding rate validator configured. Rejecting opportunity for safety."
            )
            return None

        try:
            metrics = self.funding_rate_validator.get_symbol_metrics(exchange, symbol)
            if not metrics:
                self.logger.warning(
                    f"No validation metrics found for {exchange}/{symbol}. Rejecting for safety."
                )
                return None

            rmse = Decimal(str(metrics.get("rmse", self.max_acceptable_rmse + ONE)))
            bias = Decimal(str(metrics.get("bias", self.max_acceptable_bias + ONE)))

            if rmse > self.max_acceptable_rmse or abs(bias) > self.max_acceptable_bias:
                self.logger.warning(
                    f"Validation metrics for {exchange}/{symbol} exceed thresholds. "
                    f"RMSE={rmse}, Bias={bias}. Rejecting for safety."
                )
                return None

            return ONE  # Only allow full size if metrics are within thresholds

        except Exception as e:
            self.logger.error(
                f"Error retrieving validation metrics for {exchange}/{symbol}: {e}. Rejecting for safety."
            )
            return None

    def size_opportunity(self, opportunity: ArbitrageOpportunity) -> SizedOpportunity | None:
        """
        Validate and size a single arbitrage opportunity.

        Args:
            opportunity: The potential arbitrage opportunity.

        Returns:
            A SizedOpportunity object if valid and sized, otherwise None.
        """
        self.logger.debug(f"Sizing opportunity for {opportunity.symbol}")

        # 1. Basic Opportunity Validation
        if not self.validate_opportunity(opportunity):
            # Logging is done within validate_opportunity
            return None

        # 2. Get Total Capital
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital is None or total_capital <= ZERO:
            self.logger.warning("Cannot size opportunity: Total capital unavailable or zero.")
            return None

        # --- SIMPLE SIZING PATH (if enabled in config) ---
        use_simple = self.config.get("risk.use_simple_sizing_path", False)
        if isinstance(use_simple, str):
            use_simple = use_simple.lower() in ("true", "1", "yes")
        if use_simple:
            method = self.config.get("risk.simple_sizing_method", "fixed_fraction")
            max_position = Decimal(str(self.config.get("risk.global.max_position_usd", "1000.0")))
            size = ZERO
            if method == "fixed_fraction":
                fraction = Decimal(str(self.config.get("risk.simple_fixed_fraction", "0.05")))
                size = (total_capital * fraction).quantize(Decimal("0.01"))
            elif method == "fixed_usd":
                size = Decimal(str(self.config.get("risk.simple_fixed_usd_size", "100.0")))
            # Cap at max position size
            size = min(size, max_position)
            # Cap at available capital
            size = min(size, total_capital)
            # --- Apply Validation Factor (if validator exists) ---
            long_validation_factor = self._get_validation_metrics(
                opportunity.long_exchange, opportunity.symbol
            )
            short_validation_factor = self._get_validation_metrics(
                opportunity.short_exchange, opportunity.symbol
            )
            if long_validation_factor is None or short_validation_factor is None:
                self.logger.warning(
                    "Validation failed for one or both legs. Rejecting opportunity for safety."
                )
                return None
            validation_factor: Decimal = min(long_validation_factor, short_validation_factor)
            if validation_factor < ONE:
                self.logger.info(
                    f"Applying validation factor {validation_factor:.3f} to size for "
                    f"{opportunity.symbol} (simple path).",
                )
                size *= validation_factor
                size = size.quantize(Decimal("0.01"))
                if size <= ZERO:
                    self.logger.info(
                        f"Size reduced to zero or less after validation factor for "
                        f"{opportunity.symbol} (simple path). Rejecting.",
                    )
                    return None
                self.logger.debug(
                    f"Size after validation factor for {opportunity.symbol} (simple path): ${size:.2f}"
                )
            # Enforce portfolio constraints
            is_valid, reason = self._check_portfolio_constraints(size, opportunity)
            if not is_valid:
                self.logger.info(
                    f"Opportunity {opportunity.symbol} rejected due to portfolio "
                    f"constraints: {reason}",
                )
                return None
            # Calculate expected profit/return (use expected_return if present, else 0)
            original_expected_return = getattr(opportunity, "expected_return", ZERO)
            if not isinstance(original_expected_return, Decimal):
                original_expected_return = Decimal(str(original_expected_return))
            expected_profit = size * original_expected_return
            expected_return_pct = original_expected_return
            risk_adjusted_return = expected_return_pct  # Placeholder
            sized_opportunity = SizedOpportunity(
                opportunity=opportunity,
                long_size=size,
                short_size=size,
                allocation_percentage=(size / total_capital) if total_capital > ZERO else ZERO,
                expected_profit=expected_profit,
                expected_return=expected_return_pct,
                risk_adjusted_return=risk_adjusted_return,
            )
            final_sized_opportunity = self._apply_portfolio_level_controls(sized_opportunity)
            if final_sized_opportunity:
                self.logger.info(
                    f"Successfully sized opportunity (simple path): {final_sized_opportunity}"
                )
            return final_sized_opportunity

        # --- DEFAULT: KELLY SIZING PATH ---
        # 3. Calculate Initial Kelly Size
        initial_size_usd = self._calculate_kelly_size(opportunity, total_capital)
        if initial_size_usd <= ZERO:
            # Logging is done within _calculate_kelly_size
            return None
        self.logger.debug(f"Initial Kelly size for {opportunity.symbol}: ${initial_size_usd:.2f}")

        # 4. Apply Validation Metrics Modifier (if validator exists)
        long_validation_factor = self._get_validation_metrics(
            opportunity.long_exchange, opportunity.symbol
        )
        short_validation_factor = self._get_validation_metrics(
            opportunity.short_exchange, opportunity.symbol
        )
        if long_validation_factor is None or short_validation_factor is None:
            self.logger.warning(
                "Validation failed for one or both legs. Rejecting opportunity for safety."
            )
            return None
        validation_factor: Decimal = min(long_validation_factor, short_validation_factor)
        if validation_factor < ONE:
            self.logger.info(
                f"Applying validation factor {validation_factor:.3f} to size for "
                f"{opportunity.symbol}.",
            )
            initial_size_usd *= validation_factor
            if initial_size_usd <= ZERO:
                self.logger.info(
                    f"Size reduced to zero or less after validation factor for "
                    f"{opportunity.symbol}. Rejecting.",
                )
                return None
            self.logger.debug(
                f"Size after validation factor for {opportunity.symbol}: ${initial_size_usd:.2f}"
            )

        # 5. Check Portfolio Constraints
        # Use the initial_size_usd (potentially reduced by validation) for checks
        is_valid, reason = self._check_portfolio_constraints(initial_size_usd, opportunity)
        if not is_valid:
            self.logger.info(
                f"Opportunity {opportunity.symbol} rejected due to portfolio constraints: {reason}",
            )
            return None

        # 6. Determine Final Sizes (assuming equal USD size for long/short for now)
        final_long_size = initial_size_usd
        final_short_size = initial_size_usd

        # 7. Apply Leverage Constraints (per leg/trade) - Adjust size if needed
        # (Placeholder for future logic)

        # 8. Calculate Expected Profit & Return (Based on Final Size)
        original_expected_return = getattr(opportunity, "expected_return", ZERO)
        if not isinstance(original_expected_return, Decimal):
            original_expected_return = Decimal(str(original_expected_return))
        expected_profit = final_long_size * original_expected_return  # Approx profit in USD
        expected_return_pct = original_expected_return  # Percentage return remains same
        risk_adjusted_return = expected_return_pct  # Placeholder: Use simple return for now

        # 10. Create SizedOpportunity object
        sized_opportunity = SizedOpportunity(
            opportunity=opportunity,
            long_size=final_long_size,
            short_size=final_short_size,
            allocation_percentage=(
                (final_long_size / total_capital) if total_capital > ZERO else ZERO
            ),
            expected_profit=expected_profit,
            expected_return=expected_return_pct,
            risk_adjusted_return=risk_adjusted_return,
        )

        # 11. Apply Final Portfolio Controls (e.g., drawdown, recovery factor)
        final_sized_opportunity = self._apply_portfolio_level_controls(sized_opportunity)

        if final_sized_opportunity:
            self.logger.info(f"Successfully sized opportunity: {final_sized_opportunity}")
        else:
            # Logging done within _apply_portfolio_level_controls
            pass

        return final_sized_opportunity

    def validate_opportunity(self, opportunity: ArbitrageOpportunity) -> bool:
        """
        Perform initial validation checks on an opportunity before sizing.

        Args:
            opportunity: The arbitrage opportunity.

        Returns:
            True if the opportunity passes initial validation, False otherwise.
        """
        self.logger.debug(f"Validating opportunity for {opportunity.symbol}")

        # 1. Check Required Fields
        try:
            opportunity.validate_required_fields()
        except ValueError as e:
            self.logger.warning(f"Opportunity validation failed for {opportunity.symbol}: {e}")
            return False

        # 2. Check Profitability (Net Funding Differential vs Threshold)
        if not self.is_opportunity_profitable(opportunity):
            # Logging done within is_opportunity_profitable
            return False

        # 3. Check Exchange Connectivity / Circuit Breakers
        if self.circuit_breaker_system:
            can_long, long_reason = self.circuit_breaker_system.can_execute(
                opportunity.long_exchange
            )
            if not can_long:
                self.logger.warning(
                    f"Rejecting opportunity {opportunity.symbol}: Circuit breaker tripped for "
                    f"long exchange {opportunity.long_exchange}: {long_reason}",
                )
                return False
            can_short, short_reason = self.circuit_breaker_system.can_execute(
                opportunity.short_exchange
            )
            if not can_short:
                self.logger.warning(
                    f"Rejecting opportunity {opportunity.symbol}: Circuit breaker "
                    f"tripped for short exchange {opportunity.short_exchange}: "
                    f"{short_reason}",
                )
                return False

        # 4. Check Prices (Basic Sanity - e.g., positive)
        try:
            # Ensure prices are Decimal before comparison
            long_price = (
                Decimal(str(opportunity.long_price)) if opportunity.long_price is not None else None
            )
            short_price = (
                Decimal(str(opportunity.short_price))
                if opportunity.short_price is not None
                else None
            )
            if long_price is None or long_price <= ZERO:
                self.logger.warning(
                    f"Invalid long entry price ({long_price}) for {opportunity.symbol}"
                )
                return False
            if short_price is None or short_price <= ZERO:
                self.logger.warning(
                    f"Invalid short entry price ({short_price}) for {opportunity.symbol}"
                )
                return False
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Error converting prices for {opportunity.symbol}: {e}")
            return False

        # 5. Check Exchange Balances (Minimum required)
        long_exchange_balance_obj = self.portfolio_tracker.get_exchange_balance(
            opportunity.long_exchange,
            "USD",  # Assuming check against USD balance
        )
        short_exchange_balance_obj = self.portfolio_tracker.get_exchange_balance(
            opportunity.short_exchange, "USD"
        )

        try:
            long_balance_dec = (
                Decimal(str(long_exchange_balance_obj.available))
                if long_exchange_balance_obj and long_exchange_balance_obj.available is not None
                else ZERO
            )
            short_balance_dec = (
                Decimal(str(short_exchange_balance_obj.available))
                if short_exchange_balance_obj and short_exchange_balance_obj.available is not None
                else ZERO
            )
            min_balance_dec = (
                self.min_exchange_balance  # Already Decimal
            )

            if long_balance_dec < min_balance_dec:
                self.logger.warning(
                    f"Insufficient balance on {opportunity.long_exchange} "
                    f"(${long_balance_dec:.2f}) for opportunity {opportunity.symbol}. "
                    f"Min required: ${min_balance_dec:.2f}",
                )
                return False
            if short_balance_dec < min_balance_dec:
                self.logger.warning(
                    f"Insufficient balance on {opportunity.short_exchange} "
                    f"(${short_balance_dec:.2f}) for opportunity {opportunity.symbol}. "
                    f"Min required: ${min_balance_dec:.2f}",
                )
                return False
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Error converting balances for {opportunity.symbol}: {e}")
            return False

        # 6. Check Total Exposure vs Capital (Leverage Sanity)
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital is None or total_capital <= ZERO:
            self.logger.warning(
                f"Cannot validate leverage for {opportunity.symbol}: Capital unavailable."
            )
            # Allow opportunity if capital is unavailable? Or reject? Reject for safety.
            return False

        try:
            total_exposure_dec = (
                self.calculate_total_exposure()  # Already Decimal
            )
            max_exposure_dec = (
                total_capital * self.max_leverage  # Already Decimal
            )
            if total_exposure_dec > max_exposure_dec:
                self.logger.warning(
                    f"Portfolio leverage limit exceeded. Current Exposure: "
                    f"${total_exposure_dec:.2f}, Max Allowed: ${max_exposure_dec:.2f} "
                    f"(Capital: ${total_capital:.2f}, Max Leverage: {self.max_leverage}x). "
                    f"Rejecting opportunity {opportunity.symbol}.",
                )
                return False
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Error checking leverage for {opportunity.symbol}: {e}")
            return False

        # TODO: Add more validation steps as needed
        # - Liquidity checks?
        # - Max open orders check?
        # - Correlation with existing positions? (Removed for now)

        self.logger.debug(f"Opportunity {opportunity.symbol} passed initial validation.")
        return True

    def validate_opportunities(
        self, opportunities: list[ArbitrageOpportunity]
    ) -> list[SizedOpportunity]:
        """
        Validate a list of opportunities and return sized ones.

        Args:
            opportunities: List of potential arbitrage opportunities.

        Returns:
            List of validated and sized opportunities ready for execution.
        """
        self.logger.info(f"Validating and sizing {len(opportunities)} opportunities.")
        sized_opportunities: list[SizedOpportunity] = []

        # Perform initial validation and sizing for each opportunity individually
        for opportunity in opportunities:
            self.logger.debug(
                f"Processing opportunity: {opportunity.symbol} ({opportunity.long_exchange} vs "
                f"{opportunity.short_exchange})",
            )
            # Attempt to size the opportunity (includes validation)
            sized_opp = self.size_opportunity(opportunity)
            if sized_opp:
                sized_opportunities.append(sized_opp)
            # else: # No need for log here, size_opportunity already logs reasons for failure
            #      self.logger.debug(
            #          f"Opportunity for {opportunity.symbol} did not result in a sized position."
            #      )

        # TODO: Add post-processing? E.g., ranking, diversification limits across the
        # selected batch?
        # For now, return all successfully sized opportunities.
        self.logger.info(
            f"Validated and sized {len(sized_opportunities)} opportunities out of "
            f"{len(opportunities)}.",
        )
        return sized_opportunities

    def get_portfolio_exposure_summary(self) -> dict[str, Any]:
        """
        Calculate and return a summary of current portfolio exposure.

        Returns:
            Dictionary containing exposure metrics.
        """
        summary: dict[str, Any] = {}
        total_capital = self.portfolio_tracker.get_total_capital()
        total_exposure = self.calculate_total_exposure()

        summary["timestamp"] = datetime.now(UTC).isoformat()
        summary["total_capital_usd"] = (
            f"{total_capital:.2f}" if total_capital is not None else "N/A"
        )
        summary["total_exposure_usd"] = f"{total_exposure:.2f}"
        summary["max_total_exposure_limit_usd"] = f"{self.max_total_exposure:.2f}"

        if total_capital and total_capital > ZERO:
            leverage = total_exposure / total_capital
            summary["portfolio_leverage"] = f"{leverage:.2f}x"
            summary["max_portfolio_leverage_limit"] = f"{self.max_leverage:.2f}x"
        else:
            summary["portfolio_leverage"] = "N/A"
            summary["max_portfolio_leverage_limit"] = f"{self.max_leverage:.2f}x"

        # Drawdown (assuming PortfolioTracker provides this)
        drawdown = self.portfolio_tracker.get_current_drawdown()
        summary["portfolio_drawdown_pct"] = f"{drawdown:.2%}" if drawdown is not None else "N/A"

        # Mypy fix [attr-defined]: Get active exchanges from PortfolioTracker internal state
        # (or config)
        # Using balances keys as proxy for active exchanges with capital
        summary["active_exchanges"] = list(self.portfolio_tracker._balances.keys())
        summary["exposure_by_exchange"] = {}
        summary["exposure_by_asset"] = {}

        # Calculate exposure per exchange
        for exchange in summary["active_exchanges"]:
            # TODO: Reinstate when PortfolioTracker.get_total_exposure_by_exchange exists
            # exp = self.portfolio_tracker.get_total_exposure_by_exchange(exchange)
            exp = ZERO  # Placeholder
            # summary["exposure_by_exchange"] is already initialized as dict
            summary["exposure_by_exchange"][exchange] = f"{exp:.2f}"

        # Calculate exposure per asset
        all_positions = self.portfolio_tracker.get_all_positions()
        asset_exposures: dict[str, Decimal] = {}
        # Iterate over the list of (exchange_id, position) tuples
        if all_positions:  # Check if the list is not empty
            for exchange_id, position in all_positions:
                # Ensure position is a valid Position object with necessary attributes
                if (
                    isinstance(position, Position)  # Explicit type check recommended
                    and hasattr(position, "symbol")
                    and hasattr(position, "size")
                    and hasattr(position, "entry_price")
                    and position.symbol is not None  # Check attributes are not None
                    and position.size is not None
                    and position.entry_price is not None
                ):
                    try:
                        base_asset = position.symbol.split("-")[0]  # Simple split
                        position_value = abs(
                            Decimal(str(position.size)) * Decimal(str(position.entry_price))
                        )  # Use abs for total exposure
                        # asset_exposures is already initialized as dict
                        asset_exposures[base_asset] = (
                            asset_exposures.get(base_asset, ZERO) + position_value
                        )
                    except (InvalidOperation, TypeError, IndexError) as e:
                        logger.warning(
                            f"Could not calculate exposure for position on {exchange_id} "
                            f"(Symbol: {getattr(position, 'symbol', 'N/A')}, "
                            f"Size: {getattr(position, 'size', 'N/A')}, "
                            f"Price: {getattr(position, 'entry_price', 'N/A')}). Error: {e}"
                        )
                else:
                    logger.warning(
                        f"Skipping malformed or incomplete position data from {exchange_id} "
                        f"for exposure calculation: {position}"
                    )

        for asset, exp in asset_exposures.items():
            # summary["exposure_by_asset"] is already initialized as dict
            summary["exposure_by_asset"][asset] = f"{exp:.2f}"

        # Mypy fix [attr-defined]: Remove volatility references
        # summary["asset_volatility"] = "N/A" # self.portfolio_tracker.get_asset_volatility(...)
        # summary["historical_volatility"] = "N/A"
        # self.portfolio_tracker.get_historical_volatility(...)

        return summary

    def check_drawdown(self) -> bool:
        """Check if the current portfolio drawdown exceeds the limit."""
        current_drawdown = self.portfolio_tracker.get_current_drawdown()
        if current_drawdown is None:
            self.logger.warning("Drawdown information unavailable, cannot check limit.")
            return True  # Fail open or closed? Fail open for now.
            # Mypy L1321: Unreachable code removed (was after return)

        if current_drawdown >= self.max_drawdown_limit:
            self.logger.warning(
                f"Drawdown check failed: Current {current_drawdown:.2%} >= Limit "
                f"{self.max_drawdown_limit:.2%}",
            )
            return False
        return True

    def calculate_position_exposure(self, symbol: str) -> Decimal | None:
        """
        Calculate the total USD exposure for a specific symbol across all exchanges.

        Args:
            symbol: The trading symbol (e.g., "BTC-PERP").

        Returns:
            Total USD exposure as a Decimal, or None if price is unavailable.
        """
        total_exposure = ZERO
        found_position = False
        all_positions = self.portfolio_tracker.get_all_positions()

        if all_positions:  # Check if the list is not empty
            for exchange_id, position in all_positions:
                # Check if this position matches the requested symbol
                if isinstance(position, Position) and position.symbol == symbol:
                    found_position = True
                    # Ensure position object has expected attributes for calculation
                    if (
                        hasattr(position, "size")
                        and position.size is not None
                        and hasattr(position, "entry_price")
                        and position.entry_price is not None
                        # Mark price is optional but preferred
                    ):
                        try:
                            # Use mark price if available and valid, otherwise entry price
                            price_str = None
                            if hasattr(position, "mark_price") and position.mark_price is not None:
                                price_str = str(position.mark_price)
                            else:
                                price_str = str(position.entry_price)

                            price_to_use = Decimal(price_str)
                            position_value = abs(Decimal(str(position.size)) * price_to_use)
                            total_exposure += position_value
                        except (InvalidOperation, TypeError, AttributeError) as e:
                            logger.error(
                                f"Error calculating exposure for {symbol} on {exchange_id}: {e}"
                            )
                            # If calculation fails for any leg, we might not have
                            # total exposure. Consider returning None or logging
                            # more severely depending on requirements.
                    else:
                        logger.warning(
                            f"Missing size, entry_price, or mark_price for position {symbol} on "
                            f"{exchange_id}. Cannot calculate exposure for this leg."
                        )

        return total_exposure if found_position else ZERO  # Return 0 if no position found

    def calculate_total_exposure(self) -> Decimal:
        """Calculate the total USD exposure across all positions."""
        total_exposure = ZERO
        all_positions = self.portfolio_tracker.get_all_positions()
        if all_positions:  # Check if the list is not empty
            for exchange_id, position in all_positions:
                # Ensure position is valid and has necessary attributes
                if (
                    isinstance(position, Position)
                    and hasattr(position, "size")
                    and position.size is not None
                    and hasattr(position, "entry_price")
                    and position.entry_price is not None
                    # Mark price is optional but preferred
                ):
                    try:
                        # Use mark price if available and valid, otherwise entry price
                        price_str = None
                        if hasattr(position, "mark_price") and position.mark_price is not None:
                            price_str = str(position.mark_price)
                        else:
                            price_str = str(position.entry_price)

                        price_to_use = Decimal(price_str)
                        position_value = abs(Decimal(str(position.size)) * price_to_use)
                        total_exposure += position_value
                    except (InvalidOperation, TypeError, AttributeError) as e:
                        logger.error(
                            f"Error calculating exposure for position on {exchange_id} "
                            f"(Symbol: {getattr(position, 'symbol', 'N/A')}): {e}"
                        )
                        # Decide how to handle partial failure - continue or return error?
                        # Continuing for now, total exposure might be underestimated.
                else:
                    logger.warning(
                        f"Skipping malformed or incomplete position data from {exchange_id} "
                        f"in total exposure calculation: {position}"
                    )
                    #     )

        return total_exposure
        # Mypy L1360: Unreachable code removed (was after return)

    def calculate_required_margin(
        self, symbol: str, size: Decimal, price: Decimal, leverage: Decimal
    ) -> Decimal:
        """Calculate the required margin for a position."""
        if leverage <= ZERO:
            return size * price  # 1x leverage requires full notional value
        return (size * price) / leverage

    def evaluate_liquidation_risk(self, symbol: str) -> Decimal | None:
        """
        Evaluate the liquidation risk for a symbol based on current price and liquidation price.

        Args:
            symbol: The trading symbol.

        Returns:
            A risk factor (e.g., distance to liquidation as a percentage), or None if unavailable.
            Higher value might indicate higher risk (closer to liquidation).
        """
        # Find the position for the given symbol across all exchanges
        position = None
        all_positions = self.portfolio_tracker.get_all_positions()
        if all_positions:  # Check if the list is not empty
            for _exchange_id, pos in all_positions:  # Iterate through (exchange, position) tuples
                if isinstance(pos, Position) and pos.symbol == symbol:
                    # Found the first position matching the symbol
                    position = pos
                    break  # Found the position, stop searching

        # Check if position was found and has necessary attributes
        # Check if position was found and has necessary attributes
        # Check if position was found and has necessary price attributes
        if position is None or position.liquidation_price is None or position.mark_price is None:
            self.logger.debug(
                f"Liquidation risk check skipped for {symbol}: "
                "Position or required prices not found."
            )
            return None

        try:
            # Ensure prices are Decimal
            liq_price = Decimal(str(position.liquidation_price))
            mark_price = Decimal(str(position.mark_price))

            if mark_price <= ZERO:
                return None  # Avoid division by zero

            # Calculate distance to liquidation as a percentage of mark price
            distance_pct = abs(mark_price - liq_price) / mark_price

            # Lower distance_pct means higher risk. Invert to make higher value = higher risk?
            # Example: Risk Factor = 1 / distance_pct (handle distance_pct = 0)
            risk_factor = ONE / distance_pct if distance_pct > ZERO else Decimal("inf")
            return risk_factor

        except (InvalidOperation, TypeError, AttributeError) as e:
            logger.error(f"Error evaluating liquidation risk for {symbol}: {e}")
            return None

    def is_opportunity_profitable(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check if the net funding differential meets the minimum threshold."""
        if opportunity.net_funding_differential is None:
            self.logger.debug(f"Opportunity {opportunity.symbol} has no NFD. Skipping.")
            return False
        try:
            nfd = Decimal(str(opportunity.net_funding_differential))
            if nfd >= self.min_net_funding_differential:
                return True
            else:
                self.logger.debug(
                    f"Opportunity {opportunity.symbol} NFD {nfd:.6f} not above threshold "
                    f"{self.min_net_funding_differential:.6f}",
                )
                return False
        except InvalidOperation:
            self.logger.error(
                f"Invalid net_funding_differential for {opportunity.symbol}: "
                f"{opportunity.net_funding_differential}",
            )
            return False

    def adjust_order_size(self, symbol: str, requested_size: Decimal) -> Decimal:
        """Placeholder for adjusting order size based on liquidity, order book depth, etc."""
        # TODO: Implement logic based on order book, recent volume, etc.
        return requested_size  # No adjustment for now

    def perform_sanity_checks(self) -> bool:
        """
        Perform high-level sanity checks on the overall portfolio state.
        Designed to catch potentially catastrophic conditions.

        Returns:
            True if checks pass, False if a critical condition is detected.
        """
        self.logger.debug("Performing portfolio sanity checks...")

        # Check 1: Leverage
        total_capital = self.portfolio_tracker.get_total_capital()
        total_exposure = self.calculate_total_exposure()
        sanity_leverage_limit = self.max_leverage * Decimal("1.5")  # Example: 50% buffer

        if total_capital is not None and total_capital > ZERO:
            leverage = total_exposure / total_capital
            self.logger.info(f"Sanity Check: Current Leverage = {leverage:.2f}x")
            if leverage > sanity_leverage_limit:
                self.logger.error(
                    f"Sanity Check FAIL: Portfolio leverage {leverage:.2f} exceeds sanity "
                    f"limit {sanity_leverage_limit:.2f}!",
                )
                return False
        # else: # Log if values are None
        #     self.logger.warning(
        #         "Sanity Check: Could not check leverage due to unavailable capital or exposure."
        #     )

        # Check 2: Drawdown (if available)
        current_drawdown = self.portfolio_tracker.get_current_drawdown()
        sanity_drawdown_limit = self.max_drawdown_limit * Decimal("1.5")  # Example: 50% buffer

        if current_drawdown is not None:
            self.logger.info(f"Sanity Check: Current Drawdown = {current_drawdown:.2%}")
            if current_drawdown > sanity_drawdown_limit:
                self.logger.error(
                    f"Sanity Check FAIL: Portfolio drawdown {current_drawdown:.2%} exceeds "
                    f"sanity limit {sanity_drawdown_limit:.2%}!",
                )
                return False

        # Add more checks as needed (e.g., large single position loss, API error rates)

        self.logger.info("Portfolio sanity checks passed.")
        return True

    def update(self, data: dict[str, Any]) -> None:
        """Update risk manager state based on new data (e.g., market data, portfolio updates)."""
        # Placeholder for potential future state updates
        if "drawdown_metrics" in data:
            self.current_drawdown_metrics = data["drawdown_metrics"]
            self.logger.debug(
                f"RiskManager updated with drawdown metrics: {data['drawdown_metrics']}"
            )
        # Potentially update volatility estimates, correlations, etc.
        pass
