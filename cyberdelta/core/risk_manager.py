from __future__ import annotations  # Enable postponed evaluation

import logging
from collections.abc import Sequence
from decimal import ROUND_DOWN, Decimal, InvalidOperation, getcontext
from typing import Any, Protocol

from cyberdelta.core.models import SpotBalance  # Added import
from cyberdelta.core.models.spot_balance import SpotBalance

# from cyberdelta.core.portfolio_tracker import PortfolioTrackerProtocol # This line should be commented out or removed
# from cyberdelta.core.models import ArbitrageOpportunity, Order, OrderSide, OrderType, TradeSignal
# REMOVING this runtime import
from cyberdelta.utils.config import Config
from cyberdelta.validation.circuit_breaker import BreakerState
from cyberdelta.validation.funding_data import ArbitrageOpportunity

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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SizedOpportunity):
            return NotImplemented
        return (
            self.opportunity == other.opportunity
            and self.long_size == other.long_size
            and self.short_size == other.short_size
            and self.allocation_percentage == other.allocation_percentage
            and self.expected_profit == other.expected_profit
            and self.expected_return == other.expected_return
            and self.risk_adjusted_return == other.risk_adjusted_return
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.opportunity,  # Relies on ArbitrageOpportunity implementing __hash__
                self.long_size,
                self.short_size,
                self.allocation_percentage,
                self.expected_profit,
                self.expected_return,
                self.risk_adjusted_return,
            )
        )


# --- Custom Exception Hierarchy ---
class RiskManagerError(Exception):
    """Base exception for all RiskManager errors."""

    pass


class ConfigError(RiskManagerError):
    """Raised when configuration is missing or invalid."""

    pass


class ValidationError(RiskManagerError):
    """Raised when an opportunity or action fails validation checks."""

    pass


class ConstraintViolationError(RiskManagerError):
    """Raised when a portfolio constraint is violated."""

    pass


# --- Data Structures for Protocols ---
# Remove ExchangeBalance TypedDict as it's replaced by SpotBalance
# class ExchangeBalance(TypedDict, total=False):
#     available_quantity: Decimal


class Position(Protocol):
    symbol: str
    size: Decimal
    entry_price: Decimal
    mark_price: Decimal | None
    liquidation_price: Decimal | None


# --- Protocols for Dependency Injection ---
class PortfolioTrackerProtocol(Protocol):
    def get_total_capital(self) -> Decimal: ...
    def get_exchange_balance(
        self, exchange: str, asset: str
    ) -> SpotBalance | None: ...  # Changed to SpotBalance
    def get_all_positions(self) -> Sequence[tuple[str, Position]]: ...
    def get_current_drawdown(self) -> Decimal | None: ...
    def get_total_exposure_usd(self) -> Decimal: ...


class CircuitBreakerSystemProtocol(Protocol):
    def can_execute(self, exchange: str) -> tuple[bool, str | None]: ...
    def get_exchange_breaker(self, exchange: str, breaker_type: str) -> object: ...


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
        portfolio_tracker: PortfolioTrackerProtocol,
        circuit_breaker_system: CircuitBreakerSystemProtocol | None = None,
        funding_rate_validator: FundingRateValidatorProtocol | None = None,
    ) -> None:
        """
        Initialize the risk manager.

        Args:
            config: Application configuration
            portfolio_tracker: Portfolio state tracking (must implement PortfolioTrackerProtocol)
            circuit_breaker_system: Optional system for circuit breakers
                (must implement CircuitBreakerSystemProtocol)
            funding_rate_validator: Optional validator for funding rate predictions.
                Must implement get_symbol_metrics(exchange: str, symbol: str).
        Raises:
            ConfigError: If any required config value is missing or invalid.
        """
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.config = config
        self.portfolio_tracker = portfolio_tracker
        self.circuit_breaker_system = circuit_breaker_system
        self.funding_rate_validator = funding_rate_validator
        self.current_drawdown_metrics: dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """
        Load and validate all configuration values, storing them as attributes.
        Raises ConfigError if any required value is missing or invalid.
        """
        try:
            # General Risk Parameters
            self.max_position_size = Decimal(
                str(self.config.get("risk.global.max_position_usd", "2000.0"))
            )
            self.max_total_exposure_usd = Decimal(
                str(self.config.get("risk.global.max_total_exposure_usd", "5000.0"))
            )
            self.min_position_size = Decimal(
                str(self.config.get("risk.global.min_position_usd", "10.0"))
            )
            self.min_opportunity_profitability = Decimal(
                str(self.config.get("risk.strategy.min_net_funding_differential", "0.00001"))
            )
            self.max_collateral_per_exchange: Decimal = Decimal(
                str(self.config.get("risk.max_collateral_per_exchange", 0.8))
            )
            self.max_leverage = Decimal(
                str(self.config.get("risk.global.max_portfolio_leverage", "5.0"))
            )
            self.min_liquidation_buffer: Decimal = Decimal(
                str(self.config.get("risk.min_liquidation_buffer", 0.2))
            )
            self.max_exposure_per_asset: Decimal = Decimal(
                str(self.config.get("risk.max_exposure_per_asset", 0.2))
            )
            self.max_exposure_per_exchange: Decimal = Decimal(
                str(self.config.get("risk.max_exposure_per_exchange", 0.5))
            )
            self.circuit_breaker_recovery_factor: Decimal = Decimal(
                str(self.config.get("risk.circuit_breaker_recovery_factor", 0.3))
            )
            self.min_exchange_balance = Decimal(
                str(self.config.get("risk_manager.min_exchange_balance", 10.0))
            )
            self.max_acceptable_rmse = Decimal(
                str(self.config.get("risk.max_acceptable_rmse", 0.05))
            )
            self.max_acceptable_bias = Decimal(
                str(self.config.get("risk.max_acceptable_bias", 0.02))
            )
            self.min_validation_factor = Decimal(
                str(self.config.get("risk.min_validation_factor", 0.2))
            )
            self.exchange_risk_modifiers: dict[str, float] = {}
            exchanges_config_any = self.config.get("exchanges", {})  # Get as Any first
            if isinstance(exchanges_config_any, dict):
                exchanges_config: dict[str, Any] = exchanges_config_any  # Narrow type
                for exchange_id_raw in exchanges_config.keys():  # Now keys should be str
                    # exchange_id_raw is now str
                    exchange_id: str = exchange_id_raw

                    if self.config.get(f"exchanges.{exchange_id}.enabled", False):
                        modifier_val = self.config.get(
                            f"exchanges.{exchange_id}.risk_modifier", 1.0
                        )
                        # Validate modifier type
                        if isinstance(modifier_val, (float, int)):
                            self.exchange_risk_modifiers[exchange_id] = float(modifier_val)
                        else:
                            # Default to 1.0 if type is wrong
                            self.exchange_risk_modifiers[exchange_id] = 1.0
            else:
                logger.warning(
                    "'exchanges' config is not a dictionary, cannot load risk modifiers."
                )

            self.max_single_position_exposure = Decimal(
                str(self.config.get("risk.strategy.max_single_position_exposure_ratio", 0.1))
            )
            self.max_drawdown_limit = Decimal(
                str(self.config.get("risk.global.max_drawdown_limit_ratio", 0.2))
            )
            self.min_net_funding_differential = Decimal(
                str(self.config.get("risk.strategy.min_net_funding_differential", "0.0001"))
            )
            self.max_leverage_per_trade = Decimal(
                str(self.config.get("risk.strategy.max_leverage_per_trade", 5.0))
            )
            self.volatility_period = self.config.get("strategy.volatility_period_days", 14)
            self.min_acceptable_kelly = Decimal(
                str(self.config.get("risk.kelly.min_acceptable_fraction", "0.001"))
            )
            self.max_acceptable_kelly = Decimal(
                str(self.config.get("risk.kelly.max_acceptable_fraction", "0.25"))
            )
            self.min_volatility = Decimal(
                str(self.config.get("risk.kelly.min_volatility", "0.001"))
            )
            self.kelly_fraction = Decimal(str(self.config.get("risk.kelly.fraction", "0.5")))
        except (InvalidOperation, ValueError, TypeError, KeyError) as e:
            raise ConfigError(f"Invalid or missing configuration value: {e}") from e

    def _calculate_kelly_size(
        self, opportunity: ArbitrageOpportunity, total_capital: Decimal
    ) -> Decimal:
        """
        Calculate position size using Kelly Criterion.
        Assumes expected return and volatility are provided or can be derived.
        """
        # Expected return (Net Funding Differential as a decimal)
        expected_return = opportunity.net_funding_differential
        if expected_return <= ZERO:
            self.logger.info(
                f"Kelly sizing: Expected return for {opportunity.symbol} is not positive ({expected_return:.4f}). Cannot size."
            )
            return ZERO

        # Volatility of the *spread* or *arbitrage opportunity itself*.
        # Convert from float | None to Decimal, handling None and non-positive.
        raw_volatility = opportunity.basis_volatility
        volatility: Decimal

        if raw_volatility is None:
            self.logger.warning(
                f"Volatility for {opportunity.symbol} is None. Using min_volatility: {self.min_volatility}."
            )
            volatility = self.min_volatility
        else:
            try:
                volatility = Decimal(str(raw_volatility))
            except InvalidOperation:
                self.logger.error(
                    f"Could not convert raw volatility '{raw_volatility}' to Decimal for {opportunity.symbol}. Using min_volatility: {self.min_volatility}."
                )
                volatility = self.min_volatility

        if volatility <= ZERO:
            self.logger.warning(
                f"Calculated/fallback volatility for {opportunity.symbol} is zero or negative ({volatility}). Using min_volatility: {self.min_volatility} if positive, else cannot size."
            )
            volatility = (
                self.min_volatility
            )  # Try min_volatility again if initial conversion was bad
            if volatility <= ZERO:
                self.logger.error(
                    f"min_volatility for {opportunity.symbol} is also zero or negative ({self.min_volatility}). Cannot calculate Kelly size."
                )
                return ZERO

        if expected_return <= ZERO:  # Redundant check, but safe
            self.logger.info(
                f"Kelly fraction calculation: Expected return for {opportunity.symbol} is not positive "
                f"({expected_return:.8f}). Resulting size will be zero."
            )
            return ZERO

        try:
            # Using mu / sigma^2 variant where mu is expected_return and sigma is volatility
            kelly_fraction_raw = expected_return / (volatility**2)
        except InvalidOperation:
            self.logger.error(
                f"Invalid operation during Kelly calculation for {opportunity.symbol} "
                f"(ER: {expected_return}, Vol: {volatility}). Defaulting to zero size."
            )
            return ZERO

        # Apply configured Kelly fraction (e.g., half Kelly)
        adjusted_kelly_fraction = kelly_fraction_raw * self.kelly_fraction

        # Clamp Kelly fraction to acceptable min/max range from config
        clamped_kelly_fraction = max(
            self.min_acceptable_kelly, min(adjusted_kelly_fraction, self.max_acceptable_kelly)
        )

        # Fetch and apply validation factors (RMSE, Bias)
        # Replicating logic from _calculate_simple_size for validation factors
        long_validation_result = self._get_validation_metrics(
            opportunity.long_exchange, opportunity.symbol
        )
        short_validation_result = self._get_validation_metrics(
            opportunity.short_exchange, opportunity.symbol
        )

        if long_validation_result is None or short_validation_result is None:
            self.logger.warning(
                f"Kelly Sizing: Validation failed for one or both legs of {opportunity.symbol}. "
                "Cannot safely size using Kelly. Rejecting."
            )
            return ZERO  # Or handle differently, e.g., apply a penalty factor

        # Assuming _get_validation_metrics returns ONE (Decimal('1')) if metrics are good,
        # and we want to use the minimum confidence from both legs.
        # If it can return other decimal factors, this logic might need adjustment.
        validation_factor = min(long_validation_result, short_validation_result)

        # Placeholder for actual RMSE/Bias factors if _get_validation_metrics changes to return them
        rmse_factor = validation_factor  # Simplified: use combined validation_factor
        bias_factor = ONE  # Simplified: assume no bias factor or it's part of validation_factor

        final_kelly_fraction = clamped_kelly_fraction * rmse_factor * bias_factor

        # Final position size based on this fraction of total capital
        calculated_size = total_capital * final_kelly_fraction

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(
                f"Kelly Sizing for {opportunity.symbol}:\n"
                f"  Total Capital: ${total_capital:.2f}\n"
                f"  Expected Return (NFD): {expected_return:.8f}\n"
                f"  Volatility: {volatility:.8f}\n"
                f"  Raw Kelly Fraction (mu/sigma^2): {kelly_fraction_raw:.4f}\n"
                f"  Configured Kelly Multiplier: {self.kelly_fraction}\n"
                f"  Adjusted Kelly Fraction: {adjusted_kelly_fraction:.4f}\n"
                f"  Clamped Kelly Fraction (min: {self.min_acceptable_kelly}, max: {self.max_acceptable_kelly}): {clamped_kelly_fraction:.4f}\n"
                f"  RMSE Factor: {rmse_factor:.2f}, Bias Factor: {bias_factor:.2f}\n"
                f"  Final Effective Kelly Fraction: {final_kelly_fraction:.4f}\n"
                f"  Calculated Size: ${calculated_size:.2f}"
            )

        return calculated_size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    def _check_required_fields(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check that all required fields are present in the opportunity.
        (Note: Pydantic validation handles this implicitly on creation/assignment).
        """
        # Pydantic models validate on instantiation/assignment.
        # No explicit method call needed here.
        return True

    def _check_profitability(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check if the opportunity is profitable."""
        return self.is_opportunity_profitable(opportunity)

    def _check_circuit_breaker(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check circuit breaker status for both exchanges."""
        if not self.circuit_breaker_system:
            return True
        can_long, long_reason = self.circuit_breaker_system.can_execute(opportunity.long_exchange)
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
        return True

    def _check_price_sanity(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check that long and short prices are positive and valid."""
        try:
            # Remove redundant None checks - Pydantic guarantees Decimal
            long_price = Decimal(str(opportunity.long_price))
            short_price = Decimal(str(opportunity.short_price))

            if long_price <= ZERO:
                self.logger.warning(
                    f"Invalid long entry price ({long_price}) for {opportunity.symbol}"
                )
                return False
            if short_price <= ZERO:
                self.logger.warning(
                    f"Invalid short entry price ({short_price}) for {opportunity.symbol}"
                )
                return False
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Error converting prices for {opportunity.symbol}: {e}")
            return False
        return True

    def _check_exchange_balances(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check that both exchanges have sufficient available balance."""
        long_collateral_asset = str(
            self.config.get(f"exchanges.{opportunity.long_exchange}.collateral_asset", "USD")
        )
        short_collateral_asset = str(
            self.config.get(f"exchanges.{opportunity.short_exchange}.collateral_asset", "USD")
        )

        long_exchange_balance_obj = self.portfolio_tracker.get_exchange_balance(
            opportunity.long_exchange,
            long_collateral_asset,
        )
        short_exchange_balance_obj = self.portfolio_tracker.get_exchange_balance(
            opportunity.short_exchange,
            short_collateral_asset,
        )
        try:
            long_balance_available_raw = (
                long_exchange_balance_obj.available_quantity
                if long_exchange_balance_obj
                else ZERO  # If object itself is None, balance is ZERO
            )
            long_balance_dec = (
                Decimal(str(long_balance_available_raw))
                # No need for None check here as .get with default handles it
            )

            short_balance_available_raw = (
                short_exchange_balance_obj.available_quantity
                if short_exchange_balance_obj
                else ZERO  # If object itself is None, balance is ZERO
            )
            short_balance_dec = (
                Decimal(str(short_balance_available_raw))
                # No need for None check here as .get with default handles it
            )

        except (TypeError, InvalidOperation) as e:  # Removed KeyError as .get() handles it
            self.logger.error(
                f"Error accessing or converting balance for {opportunity.symbol}: {e}. "
                f"Balances: L={long_exchange_balance_obj}, S={short_exchange_balance_obj}"
            )
            return False

        min_bal = self.min_exchange_balance
        long_exchange_ok = long_balance_dec >= min_bal
        short_exchange_ok = short_balance_dec >= min_bal

        if not long_exchange_ok:
            self.logger.warning(
                f"Insufficient balance on {opportunity.long_exchange} (${long_balance_dec:.2f}) for opportunity {opportunity.symbol}. Min required: ${min_bal:.2f}"
            )
        if not short_exchange_ok:
            self.logger.warning(
                f"Insufficient balance on {opportunity.short_exchange} (${short_balance_dec:.2f}) for opportunity {opportunity.symbol}. Min required: ${min_bal:.2f}"
            )

        return long_exchange_ok and short_exchange_ok

    def _check_leverage(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check that portfolio leverage is within allowed limits."""
        total_capital = self.portfolio_tracker.get_total_capital()

        if total_capital is None:
            logger.warning("Cannot check leverage: Total capital is None.")
            return False

        if total_capital <= ZERO:
            logger.warning("Total capital is zero or negative. Cannot calculate leverage.")
            return False
        try:
            total_exposure_dec = self.calculate_total_exposure()
            max_exposure_dec = total_capital * self.max_leverage
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
        return True

    def _check_constraint_max_position_size(
        self, proposed_size: Decimal
    ) -> tuple[bool, str | None]:
        if proposed_size > self.max_position_size:
            return (
                False,
                f"Proposed size ${proposed_size:.2f} exceeds max single position size "
                f"${self.max_position_size:.2f}",
            )
        return True, None

    def _check_constraint_max_relative_size(
        self, proposed_size: Decimal, total_capital: Decimal
    ) -> tuple[bool, str | None]:
        max_relative_size = total_capital * self.max_single_position_exposure
        if proposed_size > max_relative_size:
            return (
                False,
                f"Proposed size ${proposed_size:.2f} exceeds max relative position size "
                f"(${max_relative_size:.2f}, {self.max_single_position_exposure:.1%})",
            )
        return True, None

    def _check_constraint_max_total_exposure(
        self, proposed_size: Decimal
    ) -> tuple[bool, str | None]:
        # Use the direct method from portfolio tracker for current exposure
        current_exposure = self.portfolio_tracker.get_total_exposure_usd()
        if (current_exposure + proposed_size) > self.max_total_exposure_usd:
            return (
                False,
                f"Adding ${proposed_size:.2f} would exceed max total exposure "
                f"(${self.max_total_exposure_usd:.2f}). Current: ${current_exposure:.2f}",
            )
        return True, None

    def _check_constraint_max_leverage(
        self, proposed_size: Decimal, total_capital: Decimal
    ) -> tuple[bool, str | None]:
        if total_capital > ZERO:
            projected_leverage = (self.calculate_total_exposure() + proposed_size) / total_capital
            if projected_leverage > self.max_leverage:
                return (
                    False,
                    f"Projected leverage {projected_leverage:.2f}x exceeds max leverage "
                    f"{self.max_leverage:.2f}x",
                )
        return True, None

    def _check_constraint_exchange_balance(
        self, opportunity: ArbitrageOpportunity, proposed_size: Decimal
    ) -> tuple[bool, str | None]:
        long_ex = opportunity.long_exchange
        short_ex = opportunity.short_exchange
        long_balance_obj = self.portfolio_tracker.get_exchange_balance(long_ex, "USD")
        short_balance_obj = self.portfolio_tracker.get_exchange_balance(short_ex, "USD")

        long_available = long_balance_obj.available_quantity if long_balance_obj else ZERO
        if long_available < self.min_exchange_balance:  # Ensure Decimal comparison
            return (
                False,
                f"Insufficient available balance on {long_ex} "
                f"(Have: ${long_available if long_balance_obj else 'N/A'}, "
                f"Min: ${self.min_exchange_balance})",
            )

        short_available = short_balance_obj.available_quantity if short_balance_obj else ZERO
        if short_available < self.min_exchange_balance:  # Ensure Decimal comparison
            return (
                False,
                f"Insufficient available balance on {short_ex} "
                f"(Have: ${short_available if short_balance_obj else 'N/A'}, "
                f"Min: ${self.min_exchange_balance})",
            )
        return True, None

    def validate_opportunity(self, opportunity: ArbitrageOpportunity) -> bool:
        """
        Perform initial validation checks on an opportunity before sizing.

        Args:
            opportunity: The arbitrage opportunity.

        Returns:
            True if the opportunity passes initial validation, False otherwise.
        """
        self.logger.debug(f"Validating opportunity for {opportunity.symbol}")

        if not self._check_required_fields(opportunity):
            return False
        if not self._check_profitability(opportunity):
            return False
        if not self._check_circuit_breaker(opportunity):
            return False
        if not self._check_price_sanity(opportunity):
            return False
        if not self._check_exchange_balances(opportunity):
            return False
        if not self._check_leverage(opportunity):
            return False

        # TODO: Add more validation steps as needed
        # - Liquidity checks?
        # - Max open orders check?
        # - Correlation with existing positions? (Removed for now)

        self.logger.debug(f"Opportunity {opportunity.symbol} passed initial validation.")
        return True

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
        if total_capital <= ZERO:
            self.logger.warning("Cannot apply exposure management: Total capital unavailable.")
            return []

        current_exposure = self.calculate_total_exposure()
        allowed_new_exposure = self.max_total_exposure_usd - current_exposure
        cumulative_new_exposure = ZERO
        final_opportunities: list[SizedOpportunity] = []

        # Sort by risk-adjusted return (descending) to prioritize best opportunities
        sorted_opportunities = sorted(
            sized_opportunities,
            key=lambda x: x.risk_adjusted_return,
            reverse=True,
        )

        for opp in sorted_opportunities:
            opp_exposure = max(opp.long_size, opp.short_size)
            if (cumulative_new_exposure + opp_exposure) <= allowed_new_exposure:
                final_opportunities.append(opp)
                cumulative_new_exposure += opp_exposure
            else:
                self.logger.debug(
                    f"Skipping opportunity {opp.opportunity.symbol} due to cumulative "
                    f"exposure limit. Needed: ${opp_exposure:.2f}, Remaining: "
                    f"${(allowed_new_exposure - cumulative_new_exposure):.2f}",
                )

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
                long_api_breaker
                and getattr(long_api_breaker, "state", None) == BreakerState.HALF_OPEN
            )
            is_short_half_open = (
                short_api_breaker
                and getattr(short_api_breaker, "state", None) == BreakerState.HALF_OPEN
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
        if self.funding_rate_validator is None:
            self.logger.debug("FundingRateValidator not configured, returning factor 1.0")
            return ONE

        try:
            metrics = self.funding_rate_validator.get_symbol_metrics(exchange, symbol)
        except Exception as e:
            self.logger.error(
                f"Error retrieving funding validation metrics for {symbol} on {exchange}: {e}",
                exc_info=True,
            )
            return None

        if metrics is None or not all(
            k in metrics and metrics[k] is not None for k in ["rmse", "bias"]
        ):
            self.logger.warning(
                f"Validation metrics for {exchange}/{symbol} are incomplete or missing. Rejecting for safety."
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

    def _check_portfolio_constraints(
        self, size: Decimal, opportunity: ArbitrageOpportunity
    ) -> tuple[bool, str | None]:
        """
        Check if the given size satisfies all portfolio constraints for the opportunity.

        Args:
            size: The proposed position size.
            opportunity: The arbitrage opportunity.

        Returns:
            A tuple (bool, str | None) where the bool indicates
                whether the constraints are satisfied,
            and the str is an optional reason for rejection if not satisfied.
        """
        # Ensure total_capital is fetched and valid before proceeding
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital is None or total_capital <= ZERO:
            msg = f"Cannot check constraints: Invalid total capital ({total_capital})."
            self.logger.warning(msg)
            return False, msg

        checks = [
            self._check_constraint_max_position_size(size),
            self._check_constraint_max_relative_size(size, total_capital),
            self._check_constraint_max_total_exposure(size),
            self._check_constraint_max_leverage(size, total_capital),
            self._check_constraint_exchange_balance(opportunity, size),
        ]

        for passed, reason in checks:
            if not passed:
                return False, reason
        return True, None

    def _calculate_simple_size(
        self, opportunity: ArbitrageOpportunity, total_capital: Decimal
    ) -> SizedOpportunity | None:
        """
        Calculate position size using the simple sizing path (fixed fraction or fixed USD).
        Applies validation factors and enforces portfolio constraints.

        Args:
            opportunity: The arbitrage opportunity to size.
            total_capital: The total available capital (Decimal).

        Returns:
            A SizedOpportunity object if valid and sized, otherwise None.
        """
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
                f"Validation metrics failed for {opportunity.symbol} (Simple Path). Rejecting opportunity."
            )
            return None

        # Current _get_validation_metrics returns ONE or None. If it returns ONE, no change to size.
        # If it could return other decimal factors, this logic would apply them:
        # validation_factor: Decimal = min(long_validation_factor, short_validation_factor)
        # if validation_factor < ONE:
        #     self.logger.info(
        #         f"Applying validation factor {validation_factor:.3f} to size for "
        #         f"{opportunity.symbol} (simple path)."
        #     )
        #     sized_amount_usd *= validation_factor
        #     sized_amount_usd = sized_amount_usd.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        #     if sized_amount_usd <= ZERO:
        #         self.logger.info(
        #             f"Size reduced to zero or less after validation factor for "
        #             f"{opportunity.symbol} (simple path). Rejecting."
        #         )
        #         return None
        #     self.logger.debug(
        #         f"Size after validation factor for {opportunity.symbol} (simple path): ${sized_amount_usd:.2f}"
        #     )
        # --- END INSERTED VALIDATION FACTOR LOGIC ---

        # Enforce portfolio constraints
        is_valid, reason = self._check_portfolio_constraints(size, opportunity)
        if not is_valid:
            self.logger.info(
                f"Opportunity {opportunity.symbol} rejected due to portfolio constraints: {reason}",
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

    def _validate_opportunity_pipeline(self, opportunity: ArbitrageOpportunity) -> None:
        """
        Run the validation pipeline for an opportunity. Raises ValidationError on failure.
        """
        # List of synchronous validator methods
        sync_validators = [
            self._check_required_fields,
            self._check_profitability,
            self._check_circuit_breaker,
            self._check_price_sanity,
            self._check_exchange_balances,
            # _check_leverage is now sync, called directly below
        ]
        for validator in sync_validators:
            if not validator(opportunity):  # Call synchronous validators directly
                raise ValidationError(
                    f"Sync validation failed at {validator.__name__} for {opportunity.symbol}"
                )

        # Call the now-synchronous _check_leverage
        if not self._check_leverage(opportunity):
            raise ValidationError(f"Validation failed at _check_leverage for {opportunity.symbol}")

    async def size_opportunity(self, opportunity: ArbitrageOpportunity) -> SizedOpportunity | None:
        """
        Determine the appropriate size for an arbitrage opportunity.
        """
        # self._reload_config_values()  # Ensure latest config is used

        try:
            self._validate_opportunity_pipeline(opportunity)
        except ValidationError as e:
            self.logger.info(f"Opportunity {opportunity.symbol} failed validation: {e}")
            return None

        if self.config.get("risk.use_simple_sizing_path", False):
            return await self._size_simple(opportunity)

        return await self._size_kelly(opportunity)

    async def _size_simple(self, opportunity: ArbitrageOpportunity) -> SizedOpportunity | None:
        """
        Size an opportunity using the simple sizing path asynchronously.
        """
        self.logger.debug(f"Sizing {opportunity.symbol} using simple path.")

        # if not await self._validate_single_opportunity_sizing_conditions(opportunity): # Commented out
        #     self.logger.info(
        #         f"Opportunity {opportunity.symbol} failed pre-sizing validation (simple path)."
        #     )
        #     return None

        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital <= ZERO:
            self.logger.warning(
                f"Cannot size {opportunity.symbol} (simple path): Capital is zero or negative."
            )
            return None

        initial_size = ZERO
        if self.config.get("risk.simple_sizing_method", "fixed_fraction") == "fixed_fraction":
            fraction = Decimal(str(self.config.get("risk.simple_fixed_fraction", "0.01")))
            initial_size = total_capital * fraction
        elif self.config.get("risk.simple_sizing_method", "fixed_fraction") == "fixed_usd":
            initial_size = Decimal(str(self.config.get("risk.simple_fixed_usd_size", "100.0")))
        else:
            self.logger.warning(
                f"Unknown simple_sizing_method: {self.config.get('risk.simple_sizing_method', 'fixed_fraction')}. Defaulting to zero size."
            )
            return None  # Or raise error

        # Apply max position cap for the specific opportunity
        max_size_for_opp = self._get_max_position_size_for_opportunity(opportunity)
        sized_amount_usd = min(initial_size, max_size_for_opp)

        # Ensure size does not exceed total capital
        sized_amount_usd = min(sized_amount_usd, total_capital)

        # Ensure it meets minimum position size
        if sized_amount_usd < self.min_position_size:
            self.logger.info(
                f"Proposed size ${sized_amount_usd:.2f} for {opportunity.symbol} is below min "
                f"position size ${self.min_position_size:.2f}. Rejecting."
            )
            return None

        # Check total portfolio exposure
        current_exposure = self.portfolio_tracker.get_total_exposure_usd()
        if current_exposure + sized_amount_usd > self.max_total_exposure_usd:
            self.logger.warning(
                f"Rejecting {opportunity.symbol}: Adding ${sized_amount_usd:.2f} would exceed max total "
                f"portfolio exposure of ${self.max_total_exposure_usd:.2f} (Current: ${current_exposure:.2f})."
            )
            return None

        # Create SizedOpportunity (simplified for this path)
        # For simple path, expected profit/return might be less critical or derived differently
        # Using NFD as a proxy for expected return for now.
        # Ensure allocation_percentage is calculated correctly based on total_capital
        allocation_percentage = (
            (sized_amount_usd / total_capital) * Decimal("100") if total_capital > ZERO else ZERO
        )

        sized_opp = SizedOpportunity(
            opportunity=opportunity,
            long_size=sized_amount_usd,
            short_size=sized_amount_usd,
            allocation_percentage=allocation_percentage,
            expected_profit=sized_amount_usd * opportunity.net_funding_differential,
            expected_return=opportunity.net_funding_differential,  # Simplified
            risk_adjusted_return=opportunity.net_funding_differential,  # Simplified
        )
        self.logger.info(f"Successfully sized opportunity (simple path): {sized_opp}")
        return sized_opp

    async def _size_kelly(self, opportunity: ArbitrageOpportunity) -> SizedOpportunity | None:
        """
        Size opportunity using Kelly Criterion and apply constraints.
        """
        self.logger.debug(f"Sizing {opportunity.symbol} using Kelly path.")

        # Step 1: Initial Validation (Pre-Sizing Checks)
        # if not await self._validate_single_opportunity_sizing_conditions(opportunity):
        #     self.logger.info(
        #         f"Opportunity {opportunity.symbol} failed pre-sizing validation (Kelly path)."
        #     )
        #     return None
        # _validate_opportunity_pipeline is now called at the start of size_opportunity

        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital <= ZERO:
            self.logger.warning(
                f"Cannot size {opportunity.symbol} (Kelly path): Capital is zero or negative."
            )
            return None

        # Step 2: Calculate Kelly Size
        initial_kelly_size = self._calculate_kelly_size(opportunity, total_capital)
        if initial_kelly_size <= ZERO:
            self.logger.info(
                f"Initial Kelly size for {opportunity.symbol} is zero or negative. Rejecting."
            )
            return None

        # Step 3: Apply Portfolio Constraints
        # constrained_size, rejection_reason = await self._apply_portfolio_constraints_async(
        #     initial_kelly_size, opportunity, total_capital
        # )
        # Using synchronous version as _apply_portfolio_constraints_async doesn't exist
        is_valid_constraints, rejection_reason = self._check_portfolio_constraints(
            initial_kelly_size,
            opportunity,  # Pass initial_kelly_size as 'size'
        )
        if not is_valid_constraints:
            self.logger.info(
                f"Opportunity {opportunity.symbol} rejected by portfolio constraints: {rejection_reason}"
            )
            return None

        constrained_size = initial_kelly_size  # If constraints pass, use initial kelly size for now
        # TODO: _check_portfolio_constraints should return the adjusted size or this logic needs refinement

        if (
            constrained_size <= ZERO
        ):  # Should be caught by _check_portfolio_constraints if it adjusted to zero
            self.logger.info(
                f"Constrained size for {opportunity.symbol} is zero or negative after applying portfolio constraints. Rejecting."
            )
            return None

        # Step 4: Create preliminary SizedOpportunity object
        # Allocation percentage based on constrained_size and total_capital
        allocation_percentage = (
            (constrained_size / total_capital) * Decimal("100") if total_capital > ZERO else ZERO
        )
        # Expected profit based on constrained_size and NFD
        expected_profit = constrained_size * opportunity.net_funding_differential

        # For risk_adjusted_return, we might use the NFD directly or a more complex metric.
        # For now, using NFD as a simplified proxy. Original expected_return also NFD here.
        prelim_sized_opp = SizedOpportunity(
            opportunity=opportunity,
            long_size=constrained_size,  # Assuming symmetric for now
            short_size=constrained_size,  # Assuming symmetric for now
            allocation_percentage=allocation_percentage,
            expected_profit=expected_profit,
            expected_return=opportunity.net_funding_differential,  # Original NFD
            risk_adjusted_return=opportunity.net_funding_differential,  # Simplified
        )

        # Step 5: Apply Final Portfolio Level Controls
        # final_sized_opp = await self._apply_portfolio_level_controls_async(
        # prelim_sized_opp, total_capital
        # )
        # Using synchronous version as _apply_portfolio_level_controls_async doesn't exist
        final_sized_opp = self._apply_portfolio_level_controls(
            prelim_sized_opp  # Pass prelim_sized_opp as 'sized_opportunity'
        )

        if final_sized_opp:
            self.logger.info(f"Successfully sized opportunity (Kelly path): {final_sized_opp}")
        else:
            self.logger.info(
                f"Opportunity {opportunity.symbol} rejected by final portfolio level controls."
            )

        return final_sized_opp

    async def validate_opportunities(
        self, opportunities: list[ArbitrageOpportunity]
    ) -> list[SizedOpportunity]:
        """
        Validate and size a list of opportunities, returning only valid and sized ones.
        This method might apply broader portfolio considerations not handled by
        single opportunity sizing, e.g., overall exposure management across multiple new trades.
        """
        self.logger.info(f"Validating and sizing {len(opportunities)} opportunities.")
        # _reload_config_values() # Reload config if it can change between batches

        valid_sized_opportunities: list[SizedOpportunity] = []
        for opp in opportunities:
            sized_opp = await self.size_opportunity(opp)  # Uses the full single-opp pipeline
            if sized_opp:
                valid_sized_opportunities.append(sized_opp)

        # TODO: Implement portfolio-level exposure management across the *batch* of valid_sized_opportunities.
        # This is where _apply_portfolio_exposure_management might be called on the batch.
        # For now, just logging how many were individually sized.
        self.logger.info(
            f"Validated and sized {len(valid_sized_opportunities)} opportunities out of {len(opportunities)}."
        )

        # Sort opportunities by a metric (e.g., risk-adjusted return) if needed for prioritization.
        # Example: Sort by risk-adjusted return, descending.
        # valid_sized_opportunities.sort(key=lambda so: so.risk_adjusted_return, reverse=True)

        return valid_sized_opportunities

    def _get_max_position_size_for_opportunity(self, opportunity: ArbitrageOpportunity) -> Decimal:
        """Get max position size, potentially overridden by symbol-specific config."""
        # Example: Check for symbol-specific override
        # config_path = f"risk.asset_specific.{opportunity.symbol}.max_position_usd"
        # symbol_max_size = self.config.get(config_path)
        # if symbol_max_size is not None:
        #     return Decimal(str(symbol_max_size))
        # For now, always use global max position size
        max_size = self.max_position_size
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Max position size for {opportunity.symbol}: {max_size}")
        return max_size

    def check_drawdown(self) -> bool:
        """Check if the current portfolio drawdown exceeds the limit."""
        current_drawdown = self.portfolio_tracker.get_current_drawdown()
        if current_drawdown is None:
            self.logger.warning(
                "Drawdown information unavailable, cannot check limit. Failing CLOSED for safety."
            )
            return False  # Fail closed: block trading if drawdown data is missing

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
                # Remove unnecessary isinstance and None checks for Position and its fields
                # (Pydantic will enforce)
                if position.symbol == symbol:
                    found_position = True
                    try:
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
        return total_exposure if found_position else ZERO  # Return 0 if no position found

    def calculate_total_exposure(self) -> Decimal:
        """Calculate the total USD exposure across all positions."""
        total_exposure = ZERO
        all_positions = self.portfolio_tracker.get_all_positions()
        if all_positions:  # Check if the list is not empty
            for exchange_id, position in all_positions:
                # Remove unnecessary isinstance and None checks for Position and its fields
                # (Pydantic will enforce)
                if position.symbol:
                    try:
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
                            f"Error calculating exposure for {position.symbol} on "
                            f"{exchange_id}: {e}"
                        )
        return total_exposure

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
            for _exchange_id, pos in all_positions:
                if pos.symbol:
                    position = pos
                    break  # Found the position, stop searching

        # Check if position was found and has necessary attributes
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
        """Check if the opportunity meets the minimum net funding differential."""
        # Pydantic model 'ArbitrageOpportunity' ensures net_funding_differential is Decimal.
        # Direct comparison is safe.
        nfd = opportunity.net_funding_differential

        # Ensure min_opportunity_profitability is a Decimal (should be by _load_config)
        # No need for isinstance check on self.min_opportunity_profitability as it's set in _load_config

        is_profitable = nfd >= self.min_opportunity_profitability
        if not is_profitable:
            self.logger.debug(
                f"Opportunity {opportunity.symbol} NFD {nfd:.8f} < Min Profitable NFD {self.min_opportunity_profitability:.8f}"
            )
        return is_profitable

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

        if total_capital > ZERO:
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
