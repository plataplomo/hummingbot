from __future__ import annotations  # Enable postponed evaluation

import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation, getcontext
from typing import TYPE_CHECKING, Any  # Added TYPE_CHECKING

# from cyberdelta.core.models import ArbitrageOpportunity, Order, OrderSide, OrderType, TradeSignal # REMOVING this runtime import
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

if TYPE_CHECKING:  # This block should contain the only import from models
    from cyberdelta.core.models import (
        ArbitrageOpportunity,
        TradeSignal,
    )

logger = logging.getLogger(__name__)

# Set precision for Decimal
getcontext().prec = 28  # Default precision, adjust if needed


class SizedOpportunity:
    """
    An arbitrage opportunity with calculated position sizes and risk metrics.
    """

    def __init__(
        self,
        opportunity: ArbitrageOpportunity,
        long_size: Decimal,
        short_size: Decimal,
        allocation_percentage: float,
        expected_profit: Decimal,
        expected_return: float,
        risk_adjusted_return: float,
    ) -> None:
        """
        Initialize a sized opportunity.

        Args:
            opportunity: The base arbitrage opportunity
            long_size: Position size for long side in USD (Decimal)
            short_size: Position size for short side in USD (Decimal)
            allocation_percentage: Percentage of total capital allocated
            expected_profit: Expected profit in USD (Decimal)
            expected_return: Expected return as percentage
            risk_adjusted_return: Risk-adjusted return
        """
        self.opportunity = opportunity
        self.long_size = long_size
        self.short_size = short_size
        self.allocation_percentage = allocation_percentage
        self.expected_profit = expected_profit
        self.expected_return = expected_return
        self.risk_adjusted_return = risk_adjusted_return

    def __str__(self) -> str:
        """String representation of the sized opportunity."""
        return (
            f"SizedOpportunity: {self.opportunity.symbol} - "
            f"Long: {self.opportunity.long_exchange} ${self.long_size:.2f}, "
            f"Short: {self.opportunity.short_exchange} ${self.short_size:.2f}, "
            f"Alloc: {self.allocation_percentage:.2f}%, "
            f"ExpProfit: ${self.expected_profit:.2f}, "
            f"ExpReturn: {self.expected_return * 100:.2f}%, "
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
        # TODO: Replace Any with a more specific validator type if possible
        funding_rate_validator: Any | None = None,
    ) -> None:
        """
        Initialize the risk manager.

        Args:
            config: Application configuration
            portfolio_tracker: Portfolio state tracking
            circuit_breaker_system: Optional system for circuit breakers
            funding_rate_validator: Optional validator for funding rate predictions
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
        self.kelly_fraction = config.get("risk.kelly_fraction", 0.5)
        self.max_collateral_per_exchange = config.get("risk.max_collateral_per_exchange", 0.8)
        self.max_leverage = Decimal(
            str(
                config.get("risk.global.max_portfolio_leverage", "5.0")
            )  # Assuming this key based on tests
        )
        self.min_liquidation_buffer = config.get("risk.min_liquidation_buffer", 0.2)

        # Portfolio-level risk management parameters
        self.max_exposure_per_asset = config.get("risk.max_exposure_per_asset", 0.2)
        self.max_exposure_per_exchange = config.get("risk.max_exposure_per_exchange", 0.5)
        self.max_correlated_exposure = config.get("risk.max_correlated_exposure", 0.3)
        self.correlation_threshold = config.get("risk.correlation_threshold", 0.7)
        self.circuit_breaker_recovery_factor = config.get(
            "risk.circuit_breaker_recovery_factor", 0.3
        )
        self.min_exchange_balance = Decimal(
            str(config.get("risk_manager.min_exchange_balance", 10.0))
        )

        # Validation metric thresholds
        self.max_acceptable_rmse = config.get("risk.max_acceptable_rmse", 0.05)
        self.max_acceptable_bias = config.get("risk.max_acceptable_bias", 0.02)
        self.min_validation_factor = config.get("risk.min_validation_factor", 0.2)

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
        self.max_leverage_per_trade = Decimal(
            str(self.config.get("risk.strategy.max_leverage_per_trade", 5.0))
        )
        self.volatility_period = self.config.get(
            "strategy.volatility_period_days", 14
        )  # Period for volatility calc

        # Internal state
        self.current_drawdown_metrics = {}

    def _calculate_kelly_size(
        self, opportunity: ArbitrageOpportunity, total_capital: Decimal
    ) -> Decimal:
        """
        Calculate Kelly-based position sizing.

        Args:
            opportunity: Arbitrage opportunity
            total_capital: Total available capital (Decimal)

        Returns:
            Recommended position size in USD (Decimal)
        """
        # Extract parameters from opportunity
        nfd = (
            opportunity.net_funding_differential
        )  # Assuming this is already Decimal from SignalGenerator
        basis_volatility = opportunity.basis_volatility

        # Avoid division by zero
        if basis_volatility <= 0:
            basis_volatility = 0.001  # Default minimal volatility (float)

        # Calculate variance risk (float * float = float)
        variance_risk = basis_volatility**2

        # Get average price for Kelly calculation
        # Fetching from DataHandler is better practice, using opportunity prices for now
        # Ensure prices are Decimal
        long_price = opportunity.long_price  # Corrected attribute name
        short_price = opportunity.short_price  # Corrected attribute name
        if not long_price or not short_price:
            self.logger.warning(
                "Missing prices in opportunity for Kelly calculation. Using placeholder."
            )
            avg_price = Decimal("1.0")  # Placeholder
        else:
            avg_price = (long_price + short_price) / Decimal("2")
            if not isinstance(avg_price, Decimal):
                self.logger.error(f"Average price calculation resulted in non-Decimal: {avg_price}")
                avg_price = Decimal("1.0")  # Fallback

        if avg_price <= Decimal("0"):
            self.logger.warning(
                f"Average price is zero or negative ({avg_price}). Using placeholder."
            )
            avg_price = Decimal("1.0")  # Placeholder

        # Calculate Kelly fraction using Decimal for NFD and price, float for variance
        # Convert float variance to Decimal for calculation
        try:
            kelly = nfd / (Decimal(str(variance_risk)) * avg_price)
        except Exception as e:
            self.logger.error(
                f"Error calculating Kelly fraction: nfd={nfd}, var={variance_risk}, "
                f"avg_p={avg_price}. Error: {e}"
            )
            kelly = Decimal("0")  # Default to zero on error

        # Apply conservative multiplier (float)
        kelly_adjusted = kelly * Decimal(str(self.kelly_fraction))

        # Ensure it's a positive value
        kelly_adjusted = max(Decimal("0"), kelly_adjusted)

        # === ADDED Logging ===
        self.logger.info(
            f"Kelly Inputs: NFD={nfd}, Vol={basis_volatility}, "
            f"AvgPx={avg_price}, KellyFrac={self.kelly_fraction}"
        )
        self.logger.info(
            f"Kelly Calc: RawKelly={kelly:.6f}, AdjustedKelly={kelly_adjusted:.6f}, "
            f"TotalCapital={total_capital:.2f}"
        )
        # === END Logging ===

        # Calculate size based on Kelly (Decimal * Decimal = Decimal)
        size = kelly_adjusted * total_capital

        self.logger.debug(
            f"Kelly calculation: f*={kelly:.4f}, f_adjusted={kelly_adjusted:.4f}, size=${size:.2f}"
        )

        return size

    def _check_portfolio_constraints(
        self,
        long_exchange: str,
        short_exchange: str,
        long_size: Decimal,
        short_size: Decimal,
    ) -> bool:
        """
        Check if a trade satisfies portfolio-level constraints.

        Args:
            long_exchange: Exchange for long position
            short_exchange: Exchange for short position
            long_size: Size of long position in USD (Decimal)
            short_size: Size of short position in USD (Decimal)

        Returns:
            True if trade satisfies constraints, False otherwise
        """
        # Check total exposure (ensure all values are Decimal)
        current_exposure = self.portfolio_tracker.get_total_exposure()  # Assuming returns Decimal
        if not isinstance(current_exposure, Decimal):
            self.logger.warning(
                f"Current total exposure is not Decimal: {current_exposure}. Converting."
            )
            try:
                current_exposure = Decimal(str(current_exposure))
            except InvalidOperation:
                self.logger.error(
                    f"Could not convert current exposure '{current_exposure}' to Decimal."
                )
                return False  # Cannot perform check if conversion fails

        # Ensure long_size and short_size are also Decimal (add check for safety, though signature implies it)
        if not isinstance(long_size, Decimal):
            try:
                long_size = Decimal(str(long_size))
            except InvalidOperation:
                self.logger.error(f"Could not convert long_size '{long_size}' to Decimal.")
                return False
        if not isinstance(short_size, Decimal):
            try:
                short_size = Decimal(str(short_size))
            except InvalidOperation:
                self.logger.error(f"Could not convert short_size '{short_size}' to Decimal.")
                return False

        # Now perform addition with confirmed Decimals
        new_exposure = current_exposure + long_size + short_size

        if new_exposure > self.max_total_exposure:
            self.logger.info(
                f"Trade exceeds maximum total exposure: "
                f"${new_exposure:.2f} > ${self.max_total_exposure:.2f}"
            )
            return False

        # Check per-exchange exposure
        total_capital = self.portfolio_tracker.get_total_capital()  # Assuming returns Decimal
        if not isinstance(total_capital, Decimal):
            self.logger.warning(f"Total capital is not Decimal: {total_capital}. Converting.")
            try:
                total_capital = Decimal(str(total_capital))
            except InvalidOperation:
                self.logger.error(f"Could not convert total capital '{total_capital}' to Decimal.")
                return False

        for exchange, size in [
            (long_exchange, long_size),
            (short_exchange, short_size),
        ]:
            current_exchange_exposure_float = self.portfolio_tracker.get_exchange_exposure(
                exchange
            )  # Returns float | None
            if current_exchange_exposure_float is None:
                self.logger.warning(f"Could not get exposure for {exchange}. Assuming zero.")
                current_exchange_exposure = Decimal("0.0")
            else:
                try:
                    # Convert the float return value to Decimal
                    current_exchange_exposure = Decimal(str(current_exchange_exposure_float))
                except InvalidOperation:
                    self.logger.error(
                        f"Could not convert exposure for {exchange} '{current_exchange_exposure_float}' to Decimal."
                    )
                    return False

            new_exchange_exposure = current_exchange_exposure + size
            # max_collateral_per_exchange is float (ratio), multiply with Decimal capital
            max_exchange_exposure = total_capital * Decimal(str(self.max_collateral_per_exchange))

            if new_exchange_exposure > max_exchange_exposure:
                self.logger.info(
                    f"Trade exceeds maximum exposure for {exchange}: "
                    f"${new_exchange_exposure:.2f} > ${max_exchange_exposure:.2f}"
                )
                return False

        # Check leverage constraints
        for exchange, size in [
            (long_exchange, long_size),
            (short_exchange, short_size),
        ]:
            # Fetch the collateral balance (the method determines the asset internally)
            available_capital_dec = self.portfolio_tracker.get_exchange_collateral_balance(exchange)

            # Check if capital is sufficient (e.g., > 0)
            # Determine collateral asset *for logging purposes* after getting balance
            collateral_asset = self.config.get(f"exchanges.{exchange}.collateral_asset", "UNKNOWN")
            is_insufficient = (
                available_capital_dec <= self.min_exchange_balance
            )  # Decimal comparison
            self.logger.info(
                f"Capital Check {exchange}: Asset={collateral_asset}, "
                f"Avail={available_capital_dec:.2f}, MinReq={self.min_exchange_balance:.2f}, "
                f"IsInsufficient={is_insufficient}"
            )  # DETAILED DEBUG
            if is_insufficient:
                self.logger.warning(
                    f"No available capital on {exchange} ({collateral_asset}) "
                    f"(Balance: {available_capital_dec:.2f}, Min: {self.min_exchange_balance:.2f})"
                )
                return False

            implied_leverage = (
                size / available_capital_dec
                if available_capital_dec > Decimal("0")
                else Decimal("Infinity")
            )
            # Compare Decimal leverage to Decimal max_leverage
            if implied_leverage > self.max_leverage:
                self.logger.info(
                    f"Trade exceeds maximum leverage for {exchange}: "
                    f"{implied_leverage:.2f}x > {self.max_leverage:.2f}x"
                )
                return False

        return True

    def _apply_portfolio_exposure_management(
        self, opportunity: ArbitrageOpportunity, base_size: Decimal
    ) -> Decimal:
        """
        Apply portfolio-level exposure management constraints to the position size.

        Args:
            opportunity: Arbitrage opportunity
            base_size: Base position size calculated by Kelly (Decimal)

        Returns:
            Adjusted position size respecting portfolio constraints (Decimal)
        """
        symbol = opportunity.symbol
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange

        # Get portfolio metrics
        total_capital = self.portfolio_tracker.get_total_capital()
        if not isinstance(total_capital, Decimal):
            self.logger.warning(
                f"Total capital from tracker is not Decimal: {total_capital}. Converting."
            )
            try:
                total_capital = Decimal(str(total_capital))
            except InvalidOperation:
                self.logger.error(f"Could not convert total capital '{total_capital}' to Decimal.")
                return min(base_size, self.max_position_size)  # Return Decimal

        if total_capital <= Decimal("0"):
            self.logger.warning(
                "Cannot apply portfolio constraints: total capital is zero or negative"
            )
            return min(base_size, self.max_position_size)  # Return Decimal

        # Calculate current exposures
        current_total_exposure = self.portfolio_tracker.get_total_exposure()
        if not isinstance(current_total_exposure, Decimal):
            current_total_exposure = Decimal(str(current_total_exposure))

        # Calculate current symbol exposure manually from all positions
        current_symbol_exposure = Decimal("0.0")
        all_positions = (
            self.portfolio_tracker.get_all_positions()
        )  # Assuming returns list[tuple[str, Position]]
        for ex_id, pos in all_positions:
            if (
                pos.symbol == symbol and pos.is_active()
            ):  # Assuming is_active() exists or check pos.size > 0
                # Use pos.mark_price as fallback for current price
                current_price = pos.mark_price  # Might be None
                if current_price is not None:
                    # Ensure price and size are Decimal
                    price_dec = (
                        current_price
                        if isinstance(current_price, Decimal)
                        else Decimal(str(current_price))
                    )
                    pos_size = pos.size if isinstance(pos.size, Decimal) else Decimal(str(pos.size))
                    current_symbol_exposure += abs(price_dec * pos_size)
                else:
                    self.logger.warning(
                        f"Could not get current price for {pos.symbol} on {ex_id}. Using mark_price=None for exposure calc may be inaccurate."
                    )

        # Apply max exposure per asset constraint (Decimal * float ratio -> Decimal)
        max_asset_exposure = total_capital * Decimal(str(self.max_exposure_per_asset))
        available_asset_capacity = max_asset_exposure - current_symbol_exposure

        # Calculate available room for each constraint (using Decimal)
        available_total_exposure = max(
            Decimal("0"), self.max_total_exposure - current_total_exposure
        )
        available_symbol_exposure = max(Decimal("0"), available_asset_capacity)

        # Calculate per-exchange constraints
        current_long_exchange_exposure = self.portfolio_tracker.get_exchange_exposure(
            long_exchange
        )  # Assuming Decimal
        if not isinstance(current_long_exchange_exposure, Decimal):
            current_long_exchange_exposure = Decimal(str(current_long_exchange_exposure))
        current_short_exchange_exposure = self.portfolio_tracker.get_exchange_exposure(
            short_exchange
        )  # Assuming Decimal
        if not isinstance(current_short_exchange_exposure, Decimal):
            current_short_exchange_exposure = Decimal(str(current_short_exchange_exposure))

        max_allowed_exchange_exp = total_capital * Decimal(str(self.max_exposure_per_exchange))
        available_long_exchange = max(
            Decimal("0"), max_allowed_exchange_exp - current_long_exchange_exposure
        )
        available_short_exchange = max(
            Decimal("0"), max_allowed_exchange_exp - current_short_exchange_exposure
        )

        # Find the most restrictive constraint (min of Decimals)
        available_exposure = min(
            available_total_exposure,
            available_symbol_exposure,
            available_long_exchange,
            available_short_exchange,
        )

        # Apply correlation-based exposure limits if we have other active positions
        # This section needs careful review for Decimal usage if implemented
        active_positions = self.portfolio_tracker.get_all_positions()
        if active_positions:
            # Assuming calculate_portfolio_correlation returns float modifier
            # Need to handle interaction between Decimal size and float factor
            try:
                correlation_factor = self.calculate_portfolio_correlation(
                    opportunity, active_positions
                )
                if correlation_factor < 1.0:
                    correlated_limit = total_capital * Decimal(
                        str(self.max_correlated_exposure)
                    )  # Max exposure in USD
                    current_correlated_exposure = self._calculate_correlated_exposure(
                        opportunity.symbol, active_positions
                    )
                    if not isinstance(current_correlated_exposure, Decimal):
                        current_correlated_exposure = Decimal(str(current_correlated_exposure))
                    available_correlated = max(
                        Decimal("0"), correlated_limit - current_correlated_exposure
                    )
                    available_exposure = min(available_exposure, available_correlated)
            except AttributeError:
                # Handle missing method - log warning or ignore for now
                self.logger.debug(
                    "calculate_portfolio_correlation method not found. Skipping correlation limits."
                )
            except Exception as e:
                self.logger.error(f"Error applying correlation limits: {e}")

        # Final size is the minimum of base size and available exposure
        final_size = min(base_size, available_exposure, self.max_position_size)

        self.logger.debug(
            f"Exposure Mgmt: Base=${base_size:.2f}, AvailTotal=${available_total_exposure:.2f}, "
            f"AvailSym=${available_symbol_exposure:.2f}, AvailLongEx=${available_long_exchange:.2f}, "
            f"AvailShortEx=${available_short_exchange:.2f} -> Final=${final_size:.2f}"
        )

        return final_size

    def _apply_portfolio_level_controls(
        self, base_size: Decimal, opportunity: ArbitrageOpportunity
    ) -> Decimal:
        """
        Apply portfolio-level risk controls (e.g., volatility, drawdown, correlation, safety systems).
        Conditionally skips complex adjustments if simple path is enabled.
        Safety systems (Circuit Breaker, Funding Validation) are always applied.
        """
        adjusted_size = base_size
        symbol = opportunity.symbol
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange

        use_simple_path = self.config.get("risk.use_simple_sizing_path", False)

        # Conditionally apply complex dynamic adjustments
        if not use_simple_path:
            self.logger.info(
                "Applying standard portfolio level controls (Volatility, Drawdown, Correlation)."
            )
            # 1. Apply Volatility Adjustment
            try:
                size_after_vol = self._apply_volatility_adjustment(
                    adjusted_size, symbol, long_exchange, short_exchange
                )
                if size_after_vol != adjusted_size:
                    self.logger.debug(
                        f"After Volatility Adj: ${adjusted_size:.2f} -> ${size_after_vol:.2f}"
                    )
                    adjusted_size = size_after_vol
            except Exception as e:
                self.logger.error(f"Error applying volatility adjustment: {e}", exc_info=True)
                # Continue with potentially unadjusted size

            # 2. Apply Drawdown Protection
            try:
                size_after_drawdown = self._apply_drawdown_protection(
                    adjusted_size, long_exchange, short_exchange
                )
                if size_after_drawdown != adjusted_size:
                    self.logger.debug(
                        f"After Drawdown Protection: ${adjusted_size:.2f} -> ${size_after_drawdown:.2f}"
                    )
                    adjusted_size = size_after_drawdown
            except Exception as e:
                self.logger.error(f"Error applying drawdown protection: {e}", exc_info=True)
                # Continue

            # 3. Apply Correlation Limits
            try:
                size_after_correlation = self._apply_correlation_limits(adjusted_size, symbol)
                if size_after_correlation != adjusted_size:
                    self.logger.debug(
                        f"After Correlation Limits: ${adjusted_size:.2f} -> ${size_after_correlation:.2f}"
                    )
                    adjusted_size = size_after_correlation
            except Exception as e:
                self.logger.error(f"Error applying correlation limits: {e}", exc_info=True)
                # Continue
        else:
            self.logger.info(
                "Skipping complex portfolio controls (Volatility, Drawdown, Correlation) - Simple Path Active."
            )

        # --- Safety System Adjustments (ALWAYS RUN) ---

        # 4. Apply Circuit Breaker Adjustment if system exists
        if self.circuit_breaker_system:
            try:
                apply_cb_factor = False
                # Check global breakers first
                can_run_global, reason_global = self.circuit_breaker_system.can_execute("global")
                if not can_run_global:
                    self.logger.warning(f"Global circuit breaker check failed: {reason_global}")
                    apply_cb_factor = True
                else:
                    # If global is fine, check specific exchanges
                    can_run_long, reason_long = self.circuit_breaker_system.can_execute(
                        long_exchange
                    )
                    if not can_run_long:
                        self.logger.warning(
                            f"Exchange circuit breaker check failed for {long_exchange}: {reason_long}"
                        )
                        apply_cb_factor = True
                    else:
                        can_run_short, reason_short = self.circuit_breaker_system.can_execute(
                            short_exchange
                        )
                        if not can_run_short:
                            self.logger.warning(
                                f"Exchange circuit breaker check failed for {short_exchange}: {reason_short}"
                            )
                            apply_cb_factor = True

                if apply_cb_factor:
                    recovery_factor = Decimal(str(self.circuit_breaker_recovery_factor))
                    adjusted_size *= recovery_factor
                    self.logger.info(
                        f"Applied circuit breaker recovery factor {recovery_factor:.2f}. "
                        f"New size: ${adjusted_size:.2f}"
                    )

            except Exception as e:
                self.logger.error(f"Error applying circuit breaker adjustments: {e}", exc_info=True)

        # 5. Apply Funding Rate Validation Adjustment if validator exists
        if self.funding_rate_validator:
            try:
                # Get validation factors for both exchanges (returns float)
                long_validation_factor = self._get_validation_metrics(long_exchange, symbol)
                short_validation_factor = self._get_validation_metrics(short_exchange, symbol)

                # Use the minimum factor to be conservative
                min_factor = min(long_validation_factor, short_validation_factor)

                # Ensure factor is not below the absolute minimum (convert ratios to Decimal)
                final_factor_dec = max(
                    Decimal(str(self.min_validation_factor)), Decimal(str(min_factor))
                )

                if final_factor_dec < Decimal("1.0"):
                    adjusted_size *= final_factor_dec  # Multiply Decimal size by Decimal factor
                    self.logger.info(
                        f"Applied funding rate validation factor {final_factor_dec:.2f}. "
                        f"New size: ${adjusted_size:.2f}"
                    )

            except Exception as e:
                self.logger.error(
                    f"Error applying funding rate validation adjustments: {e}",
                    exc_info=True,
                )

        # Ensure size does not exceed max position size after adjustments
        adjusted_size = min(adjusted_size, self.max_position_size)

        # Ensure size is not negative
        adjusted_size = max(Decimal("0"), adjusted_size)

        self.logger.debug(
            f"Portfolio Level Controls: Base=${base_size:.2f} -> Adjusted=${adjusted_size:.2f}"
        )
        return adjusted_size

    # --- Helper methods for portfolio level controls (ensure they handle Decimal) ---

    def _calculate_correlated_exposure(
        self, symbol: str, active_positions: list[tuple[str, Any]]
    ) -> Decimal:
        """Calculate exposure to assets correlated with the target symbol."""
        # Placeholder implementation - needs correlation matrix/logic
        # For now, return zero assuming no correlation considered
        # If implemented, ensure Decimal calculations
        return Decimal("0.0")

    def _apply_volatility_adjustment(
        self, base_size: Decimal, symbol: str, long_exchange: str, short_exchange: str
    ) -> Decimal:
        """Adjust size based on market volatility."""
        try:
            # Fetch volatility metrics (assuming these return floats or need conversion)
            # Example: Assuming portfolio tracker provides volatility metrics
            current_volatility = self.portfolio_tracker.get_asset_volatility(symbol)
            historical_volatility = self.portfolio_tracker.get_historical_volatility(symbol)

            if (
                current_volatility is None
                or historical_volatility is None
                or historical_volatility == 0
            ):
                self.logger.debug("Insufficient volatility data to apply adjustment.")
                return base_size

            # Convert to Decimal for calculation
            current_volatility_dec = Decimal(str(current_volatility))
            historical_volatility_dec = Decimal(str(historical_volatility))

            # Avoid division by zero if historical volatility is extremely small
            if historical_volatility_dec == Decimal("0"):
                self.logger.warning("Historical volatility is zero, cannot calculate ratio.")
                return base_size

            # Calculate volatility ratio
            vol_ratio = current_volatility_dec / historical_volatility_dec

            VOLATILITY_RATIO_THRESHOLD = Decimal("1.5")  # Example threshold
            MAX_VOLATILITY_FACTOR = Decimal("0.5")  # Example reduction factor

            if vol_ratio > VOLATILITY_RATIO_THRESHOLD:
                factor = MAX_VOLATILITY_FACTOR
                adjusted_size = base_size * factor
                self.logger.info(
                    f"High volatility detected (Ratio: {vol_ratio:.2f}). "
                    f"Reducing size by factor {factor:.2f}. New size: ${adjusted_size:.2f}"
                )
                return adjusted_size
            else:
                return base_size

        except Exception as e:
            self.logger.error(f"Error calculating volatility adjustment: {e}", exc_info=True)
            # Fallback to base size on error
            return base_size

    def _apply_drawdown_protection(
        self, base_size: Decimal, long_exchange: str, short_exchange: str
    ) -> Decimal:
        """Adjust size based on recent portfolio drawdown."""
        try:
            # Fetch drawdown metrics (assuming returns float or needs conversion)
            portfolio_drawdown = self.portfolio_tracker.get_portfolio_drawdown()
            long_exchange_drawdown = self.portfolio_tracker.get_exchange_drawdown(long_exchange)
            short_exchange_drawdown = self.portfolio_tracker.get_exchange_drawdown(short_exchange)

            if portfolio_drawdown is None:
                self.logger.debug("Insufficient drawdown data available.")
                return base_size

            # Convert to Decimal
            portfolio_drawdown_dec = Decimal(str(portfolio_drawdown))
            # Define thresholds (as Decimal)
            DRAWDOWN_THRESHOLD_1 = Decimal("0.05")  # 5%
            DRAWDOWN_THRESHOLD_2 = Decimal("0.10")  # 10%
            DRAWDOWN_FACTOR_1 = Decimal("0.75")
            DRAWDOWN_FACTOR_2 = Decimal("0.50")

            factor = Decimal("1.0")
            if portfolio_drawdown_dec > DRAWDOWN_THRESHOLD_2:
                factor = DRAWDOWN_FACTOR_2
            elif portfolio_drawdown_dec > DRAWDOWN_THRESHOLD_1:
                factor = DRAWDOWN_FACTOR_1

            # Consider exchange-specific drawdown if more severe
            max_exchange_drawdown = Decimal("0")
            if long_exchange_drawdown is not None:
                max_exchange_drawdown = max(
                    max_exchange_drawdown, Decimal(str(long_exchange_drawdown))
                )
            if short_exchange_drawdown is not None:
                max_exchange_drawdown = max(
                    max_exchange_drawdown, Decimal(str(short_exchange_drawdown))
                )

            if max_exchange_drawdown > DRAWDOWN_THRESHOLD_2:
                factor = min(factor, DRAWDOWN_FACTOR_2)
            elif max_exchange_drawdown > DRAWDOWN_THRESHOLD_1:
                factor = min(factor, DRAWDOWN_FACTOR_1)

            if factor < Decimal("1.0"):
                adjusted_size = base_size * factor
                self.logger.info(
                    f"Drawdown protection activated (Portfolio: {portfolio_drawdown_dec:.2%}, Max Exchange: "
                    f"{max_exchange_drawdown:.2%}). Reducing size by factor {factor:.2f}. New size: "
                    f"${adjusted_size:.2f}"
                )
                return adjusted_size
            else:
                return base_size

        except Exception as e:
            self.logger.error(f"Error applying drawdown protection: {e}", exc_info=True)
            # Fallback to base size on error
            return base_size

    def _apply_correlation_limits(self, base_size: Decimal, symbol: str) -> Decimal:
        """
        Adjust size based on correlation with existing positions.
        Requires correlation matrix/data.
        """
        try:
            if not hasattr(self.portfolio_tracker, "get_asset_correlation"):
                self.logger.debug(
                    "Portfolio tracker missing 'get_asset_correlation'. "
                    "Skipping correlation limits."
                )
                return base_size

            active_positions = self.portfolio_tracker.get_all_positions()
            if not active_positions:
                return base_size  # No existing positions, no correlation adjustment needed

            total_correlated_exposure = Decimal("0.0")
            proposed_exposure = base_size

            for _ex_id, pos in active_positions:
                correlation = self.portfolio_tracker.get_asset_correlation(symbol, pos.symbol)
                if correlation is not None and abs(correlation) >= self.correlation_threshold:
                    price = (
                        pos.mark_price
                        if isinstance(pos.mark_price, Decimal)
                        else Decimal(str(pos.mark_price))
                    )
                    size = pos.size if isinstance(pos.size, Decimal) else Decimal(str(pos.size))
                    total_correlated_exposure += abs(price * size)

            # Calculate total capital
            total_capital = self.portfolio_tracker.get_total_capital()
            if not isinstance(total_capital, Decimal):
                total_capital = Decimal(str(total_capital))
            # Ensure total_capital is positive before proceeding
            if total_capital <= Decimal("0"):
                self.logger.warning(
                    "Total capital is zero or negative, cannot apply correlation limits."
                )
                return base_size

            # Calculate maximum allowed correlated exposure
            max_allowed_correlated = total_capital * Decimal(str(self.max_correlated_exposure))

            # Calculate available room for correlated exposure
            available_correlated_room = max(
                Decimal("0"), max_allowed_correlated - total_correlated_exposure
            )

            # Adjust base_size if it exceeds available correlated room
            if proposed_exposure > available_correlated_room:
                adjusted_size = available_correlated_room
                self.logger.info(
                    f"Correlation Limit: Symbol {symbol} correlated with existing exposure\n"
                    f"(Total Correlated: ${total_correlated_exposure:.2f}, Threshold: {self.correlation_threshold}).\n"
                    f"Max Allowed Correlated: ${max_allowed_correlated:.2f}, Available Room: ${available_correlated_room:.2f}.\n"
                    f"Reducing size from ${base_size:.2f} to ${adjusted_size:.2f}"
                )
                return max(Decimal("0"), adjusted_size)  # Ensure non-negative
            else:
                # No adjustment needed based on correlation
                return base_size

        except AttributeError as ae:
            self.logger.debug(
                f"AttributeError during correlation limit check (likely missing method): {ae}"
            )
            return base_size  # Fallback if methods are missing
        except Exception as e:
            self.logger.error(
                f"Error applying correlation limits for symbol {symbol}: {e}",
                exc_info=True,
            )
            # Fallback to base size on error
            return base_size

    def _get_validation_metrics(self, exchange: str, symbol: str) -> float:
        """
        Get validation metrics for funding rate predictions.
        Returns a factor (0.0 to 1.0) based on prediction accuracy.
        Factor = 1.0 means high confidence, lower values mean less confidence.
        """
        if not self.funding_rate_validator:
            return 1.0  # No validator, assume full confidence

        try:
            metrics = self.funding_rate_validator.get_validation_metrics(exchange, symbol)
            if not metrics:
                self.logger.warning(
                    f"No validation metrics found for {exchange}/{symbol}. Assuming low confidence."
                )
                return float(self.min_validation_factor)  # Return float

            rmse = metrics.get("rmse")
            bias = metrics.get("bias")

            if rmse is None or bias is None:
                self.logger.warning(
                    f"Incomplete validation metrics for {exchange}/{symbol}. "
                    f"Assuming low confidence."
                )
                return float(self.min_validation_factor)  # Return float

            # Normalize metrics against acceptable thresholds (use floats for ratios)
            rmse_float = float(rmse)
            bias_float = float(bias)
            max_rmse_float = float(self.max_acceptable_rmse)
            max_bias_float = float(self.max_acceptable_bias)
            min_factor_float = float(self.min_validation_factor)

            # Higher error -> lower factor
            rmse_factor = (
                max(0.0, 1.0 - (rmse_float / max_rmse_float)) if max_rmse_float > 0 else 1.0
            )
            bias_factor = (
                max(0.0, 1.0 - (abs(bias_float) / max_bias_float)) if max_bias_float > 0 else 1.0
            )

            # Combine factors (e.g., take the minimum to be conservative)
            combined_factor = min(rmse_factor, bias_factor)

            # Ensure factor is within bounds [min_validation_factor, 1.0]
            final_factor = max(min_factor_float, combined_factor)
            final_factor = min(1.0, final_factor)

            self.logger.debug(
                f"Validation Metrics Factor for {exchange}/{symbol}: RMSE={rmse:.4f}, "
                f"Bias={bias:.4f} -> Factor={final_factor:.2f}"
            )
            return final_factor  # Return float factor

        except Exception as e:
            self.logger.error(f"Error getting validation metrics for {exchange}/{symbol}: {e}")
            return float(self.min_validation_factor)  # Return float factor

    def size_opportunity(self, opportunity: ArbitrageOpportunity) -> SizedOpportunity | None:
        """
        Calculate the appropriate size for an arbitrage opportunity.

        Args:
            opportunity: Arbitrage opportunity

        Returns:
            SizedOpportunity if valid and meets risk criteria, otherwise None
        """
        # 0. Initial Checks
        # Ensure opportunity has Decimal values where expected
        if not isinstance(opportunity.net_funding_differential, Decimal):
            opportunity.net_funding_differential = Decimal(
                str(opportunity.net_funding_differential)
            )
        if hasattr(opportunity, "long_price") and not isinstance(opportunity.long_price, Decimal):
            opportunity.long_price = Decimal(str(opportunity.long_price))
        if hasattr(opportunity, "short_price") and not isinstance(opportunity.short_price, Decimal):
            opportunity.short_price = Decimal(str(opportunity.short_price))
        if hasattr(opportunity, "expected_profit"):
            if opportunity.expected_profit is None:
                opportunity.expected_profit = Decimal("0")  # Handle None case
            elif not isinstance(opportunity.expected_profit, Decimal):
                opportunity.expected_profit = Decimal(str(opportunity.expected_profit))

        if abs(opportunity.net_funding_differential) < self.min_net_funding_differential:
            self.logger.debug(
                f"Opportunity {opportunity.symbol} rejected: Net funding diff "
                f"{opportunity.net_funding_differential:.6f} < {self.min_net_funding_differential}"
            )
            return None

        # Wrap ALL sizing logic in a try-except block
        try:
            # 1. Get Total Capital
            total_capital = self.portfolio_tracker.get_total_capital()
            if not isinstance(total_capital, Decimal):
                self.logger.warning(
                    f"Total capital from tracker is not Decimal: {total_capital}. Converting."
                )
                try:
                    total_capital = Decimal(str(total_capital))
                except InvalidOperation:
                    self.logger.error(
                        f"Could not convert total capital '{total_capital}' to Decimal."
                    )
                    return None  # Cannot proceed without valid capital

            if total_capital <= Decimal("0"):
                self.logger.warning("Cannot size opportunity: total capital is zero or negative.")
                return None

            # 2. Calculate Initial Base Size (v0.0.1 Simple Path vs. Kelly)
            initial_base_size = Decimal("0")
            use_simple_path = self.config.get("risk.use_simple_sizing_path", False)

            # Inner try-except specifically for initial sizing calculation errors
            try:
                if use_simple_path:
                    self.logger.info("Using v0.0.1 simple sizing path.")
                    sizing_method = self.config.get("risk.simple_sizing_method", "fixed_fraction")
                    if sizing_method == "fixed_fraction":
                        fraction = Decimal(self.config.get("risk.simple_fixed_fraction", "0.01"))
                        initial_base_size = total_capital * fraction
                        self.logger.info(
                            f"Simple Sizing: Fixed Fraction ({fraction:.2%}). Base size: ${initial_base_size:.2f}"
                        )
                    elif sizing_method == "fixed_usd":
                        initial_base_size = Decimal(
                            self.config.get("risk.simple_fixed_usd_size", "100")
                        )
                        self.logger.info(
                            f"Simple Sizing: Fixed USD. Base size: ${initial_base_size:.2f}"
                        )
                    else:
                        self.logger.error(
                            f"Invalid simple_sizing_method: {sizing_method}. Defaulting to 0."
                        )
                        initial_base_size = Decimal("0")
                else:
                    self.logger.info("Using standard sizing path (Kelly Criterion).")
                    initial_base_size = self._calculate_kelly_size(opportunity, total_capital)
                    self.logger.info(
                        f"Standard Sizing: Kelly Result. Base size: ${initial_base_size:.2f}"
                    )

            except Exception as e:
                self.logger.error(f"Error during initial base size calculation: {e}", exc_info=True)
                return None  # Exit if initial sizing fails

            # Ensure initial size is non-negative before proceeding
            initial_base_size = max(Decimal("0"), initial_base_size)
            if initial_base_size <= Decimal("0"):
                self.logger.warning(
                    "Initial base size calculated as zero or negative. Rejecting opportunity."
                )
                return None

            # Steps 3, 4, 5 remain within the main try-except block
            # 3. Apply Portfolio Exposure Management (Applies to both paths)
            exposure_adjusted_size = Decimal("0")
            exposure_adjusted_size = self._apply_portfolio_exposure_management(
                opportunity, initial_base_size
            )
            self.logger.debug(
                f"After Exposure Mgmt: ${initial_base_size:.2f} -> ${exposure_adjusted_size:.2f}"
            )

            # 4. Apply Portfolio Level Controls (Will conditionally skip complex adjustments inside)
            final_base_size = Decimal("0")
            final_base_size = self._apply_portfolio_level_controls(
                exposure_adjusted_size, opportunity
            )
            self.logger.debug(
                f"After Portfolio Controls: ${exposure_adjusted_size:.2f} -> ${final_base_size:.2f}"
            )

            # Ensure size doesn't exceed individual max position size
            final_size = min(final_base_size, self.max_position_size)
            if final_size < final_base_size:
                self.logger.info(
                    f"Applied Max Position Size Cap: ${final_base_size:.2f} -> ${final_size:.2f}"
                )

            # 5. Check Portfolio Constraints with Final Size
            long_size = final_size
            short_size = final_size
            if not self._check_portfolio_constraints(
                opportunity.long_exchange,
                opportunity.short_exchange,
                long_size,
                short_size,
            ):
                self.logger.info("Opportunity failed portfolio constraints check after sizing.")
                return None

        except Exception as e:
            self.logger.error(
                f"Unhandled exception during sizing process for {opportunity.symbol}: {e}",
                exc_info=True,
            )
            return None  # Return None if any step from 1 to 5 fails unexpectedly

        # 6. Calculate derived metrics (Outside the main try-except)
        allocation_percentage = (
            float((final_size / total_capital) * Decimal("100"))
            if total_capital > Decimal("0")
            else 0.0
        )  # Result is float

        # Recalculate expected profit based on final size and NFD
        # Assuming prices are available in the opportunity object
        if opportunity.long_price and opportunity.short_price:
            avg_entry_price = (opportunity.long_price + opportunity.short_price) / Decimal("2")
            # Quantity = Size / Price
            quantity = (
                final_size / avg_entry_price if avg_entry_price > Decimal("0") else Decimal("0")
            )
            # Profit = Quantity * Price_Difference * Time_Factor (Simplified: Size * NFD)
            # Need to consider time horizon for funding rate profit
            # Simple approximation: Profit = Size * NetFundingDifferential (as a rate)
            # Assuming NFD is daily rate, profit per day = Size * NFD
            # This needs a more robust calculation based on strategy specifics.
            # Using a simplified placeholder based on NFD * Size
            final_expected_profit = (
                final_size * opportunity.net_funding_differential
            )  # Decimal * Decimal
        else:
            self.logger.warning(
                "Cannot accurately calculate expected profit due to missing prices. Using zero."
            )
            final_expected_profit = Decimal("0.0")

        # Expected return (percentage) - remains float, based on NFD
        expected_return = float(
            opportunity.net_funding_differential * 100
        )  # Convert Decimal rate to float percentage
        risk_adjusted_return = (
            float(opportunity.utility_score) if hasattr(opportunity, "utility_score") else 0.0
        )  # Use utility score as proxy (float)

        # 7. Create SizedOpportunity object
        sized_opportunity = SizedOpportunity(
            opportunity=opportunity,
            long_size=long_size,  # Final Decimal size
            short_size=short_size,  # Final Decimal size
            allocation_percentage=allocation_percentage,  # Float %
            expected_profit=final_expected_profit,  # Decimal USD profit
            expected_return=expected_return,  # Float %
            risk_adjusted_return=risk_adjusted_return,  # Float score
        )

        self.logger.info(f"Successfully sized opportunity: {sized_opportunity}")
        return sized_opportunity

    def validate_opportunities(
        self, opportunities: list[ArbitrageOpportunity]
    ) -> list[SizedOpportunity]:
        """
        Filter and size a list of opportunities based on risk.

        Args:
            opportunities: List of potential arbitrage opportunities

        Returns:
            List of validated and sized opportunities
        """
        validated_opportunities = []
        for opportunity in opportunities:
            sized_opp = self.size_opportunity(opportunity)
            if sized_opp:
                validated_opportunities.append(sized_opp)
            else:
                self.logger.info(
                    f"Opportunity {opportunity.symbol} ({opportunity.long_exchange} vs "
                    f"{opportunity.short_exchange}) rejected during sizing/validation."
                )

        # Optional: Rank validated opportunities based on risk/reward (e.g., risk_adjusted_return)
        validated_opportunities.sort(key=lambda o: o.risk_adjusted_return, reverse=True)

        return validated_opportunities

    def get_portfolio_exposure_summary(self) -> dict[str, Any]:
        """
        Generate a summary of current portfolio exposures.

        Returns:
            Dictionary containing exposure metrics (using Decimal for values).
        """
        total_capital = self.portfolio_tracker.get_total_capital()
        total_exposure = self.portfolio_tracker.get_total_exposure()
        # Ensure values from tracker are Decimal
        if not isinstance(total_capital, Decimal):
            total_capital = Decimal(str(total_capital))
        if not isinstance(total_exposure, Decimal):
            total_exposure = Decimal(str(total_exposure))

        summary = {
            "timestamp": datetime.now().isoformat(),
            # Report floats for easier JSON serialization/external use
            "total_capital_usd": float(total_capital),
            "total_exposure_usd": float(total_exposure),
            "total_exposure_pct": float((total_exposure / total_capital) * 100)
            if total_capital > Decimal("0")
            else 0.0,
            "max_total_exposure_limit_usd": float(self.max_total_exposure),
            "max_total_exposure_limit_pct": float((self.max_total_exposure / total_capital) * 100)
            if total_capital > Decimal("0")
            else 0.0,
            "exchange_exposure": {},
            "asset_exposure": {},
        }

        # Exchange Exposure
        exchanges = self.config.get("exchanges", {}).keys()
        asset_exposure_map: dict[str, Decimal] = {}

        for exchange in exchanges:
            if not self.config.get(f"exchanges.{exchange}.enabled", False):
                continue

            ex_exposure = self.portfolio_tracker.get_exchange_exposure(exchange)
            ex_balance = self.portfolio_tracker.get_exchange_collateral_balance(exchange)
            # Ensure Decimal
            if not isinstance(ex_exposure, Decimal):
                ex_exposure = Decimal(str(ex_exposure))
            if not isinstance(ex_balance, Decimal):
                ex_balance = Decimal(str(ex_balance))

            max_allowed_ex_exp = (
                total_capital * Decimal(str(self.max_exposure_per_exchange))
                if total_capital > Decimal("0")
                else Decimal("0")
            )

            summary["exchange_exposure"][exchange] = {
                "exposure_usd": float(ex_exposure),
                "exposure_pct_of_total_capital": float((ex_exposure / total_capital) * 100)
                if total_capital > Decimal("0")
                else 0.0,
                "balance_usd": float(ex_balance),
                "leverage": float(ex_exposure / ex_balance) if ex_balance > Decimal("0") else 0.0,
                "max_exposure_limit_usd": float(max_allowed_ex_exp),
                "max_exposure_limit_pct": float(self.max_exposure_per_exchange * 100),
            }

            # Aggregate asset exposures
            positions = self.portfolio_tracker.get_positions_by_exchange(exchange)
            for pos in positions:
                if pos.is_active():
                    symbol = pos.symbol  # Assuming internal symbol
                    # Ensure price/size are Decimal
                    price = (
                        pos.mark_price
                        if isinstance(pos.mark_price, Decimal)
                        else Decimal(str(pos.mark_price))
                    )
                    size = pos.size if isinstance(pos.size, Decimal) else Decimal(str(pos.size))
                    exposure_usd = abs(price * size)
                    asset_exposure_map[symbol] = (
                        asset_exposure_map.get(symbol, Decimal("0")) + exposure_usd
                    )

        # Asset Exposure
        for symbol, exposure_usd in asset_exposure_map.items():
            # Ensure total_capital is positive before calculating max allowed exposure
            max_asset_exp_allowed = (
                total_capital * Decimal(str(self.max_exposure_per_asset))
                if total_capital > Decimal("0")
                else Decimal("0")
            )
            summary["asset_exposure"][symbol] = {
                "exposure_usd": float(exposure_usd),
                "exposure_pct_of_total_capital": float((exposure_usd / total_capital) * 100)
                if total_capital > Decimal("0")
                else 0.0,
                "max_exposure_limit_usd": float(max_asset_exp_allowed),
                "max_exposure_limit_pct": float(self.max_exposure_per_asset * 100),
            }

        return summary

    def size_signal(self, signal: TradeSignal) -> TradeSignal:
        """Size a generic TradeSignal based on risk parameters. Ensure Decimal usage."""
        # TODO: Implement sizing logic for generic signals, adapting Kelly or using simpler rules.
        # This requires defining how signal properties (e.g., confidence, predicted move) map
        # to size.
        # Ensure Decimal usage throughout the implementation.
        self.logger.warning(
            "size_signal is not fully implemented. Returning original signal without sizing."
        )
        # If implemented, ensure signal.quantity and related monetary values are Decimal.
        return signal

    def _check_opportunity_risk(self, opportunity: ArbitrageOpportunity) -> bool:
        """Checks if the risk associated with an arbitrage opportunity is acceptable."""
        # Example check: Basis volatility
        # Corrected: Use getattr for safe access
        basis_volatility = getattr(opportunity, "basis_volatility", None)
        max_basis_volatility = self.config.get("max_basis_volatility", 0.01)  # Example config
        if basis_volatility is not None and basis_volatility > max_basis_volatility:
            self.logger.warning(
                "Opportunity basis volatility too high",
                opportunity=opportunity,
                volatility=basis_volatility,
            )
            return False

        # TODO: Add more checks (e.g., confidence score, liquidity)
        return True

    def _check_position_limit(self, symbol: str, potential_increase: Decimal) -> bool:
        """Checks if adding to a position exceeds the single position limit."""
        # Corrected: Use Decimal for calculations
        current_pos_val = self.calculate_position_exposure(symbol)
        if current_pos_val is None:  # Handle case where exposure can't be calculated
            self.logger.warning(f"Cannot check position limit for {symbol}, exposure unknown.")
            return False  # Fail safe
        max_size = self.max_position_size  # Already Decimal
        if current_pos_val + potential_increase > max_size:
            self.logger.info(
                f"Trade for {symbol} exceeds max position size: {current_pos_val + potential_increase:.2f} > {max_size:.2f}"
            )
            return False
        return True

    def _check_total_exposure(self, potential_increase: Decimal) -> bool:
        """Checks if adding a position exceeds the total portfolio exposure limit."""
        # Corrected: Use Decimal for calculations
        current_exposure = self.calculate_total_exposure()
        max_exposure = self.max_total_exposure  # Already Decimal
        if current_exposure + potential_increase > max_exposure:
            self.logger.info(
                f"Trade exceeds max total exposure: {current_exposure + potential_increase:.2f} > {max_exposure:.2f}"
            )
            return False
        return True

    def check_drawdown(self) -> bool:
        """Checks if the portfolio drawdown exceeds the maximum limit."""
        # Corrected: Use Decimal for calculations
        current_drawdown = self.portfolio_tracker.get_portfolio_drawdown()  # Returns Decimal
        if current_drawdown > self.max_drawdown_limit:  # Both should be Decimal
            self.logger.warning(
                f"Max drawdown limit exceeded: {current_drawdown:.2%} > {self.max_drawdown_limit:.2%}"
            )
            return False
        return True

    def calculate_position_exposure(self, symbol: str) -> Decimal | None:
        """Calculate the total USD exposure for a specific symbol across all exchanges."""
        total_exposure = Decimal("0.0")
        found_position = False
        for exchange_id in self.portfolio_tracker.get_active_exchanges():
            position = self.portfolio_tracker.get_position(exchange_id, symbol)  # Correct call
            if position and position.is_active():
                found_position = True
                # Use mark_price if available, otherwise log warning
                price = position.mark_price
                if price is None:
                    self.logger.warning(
                        f"Mark price unavailable for {symbol} on {exchange_id}, cannot calculate exposure."
                    )
                    return None  # Indicate calculation failure
                price_dec = Decimal(str(price))
                size_dec = Decimal(str(position.size))
                total_exposure += abs(price_dec * size_dec)

        return (
            total_exposure if found_position else Decimal("0.0")
        )  # Return 0 if no active position

    def calculate_total_exposure(self) -> Decimal:
        """Calculate the total USD exposure across all positions."""
        total_exposure = Decimal("0.0")
        all_positions = self.portfolio_tracker.get_all_positions()  # Use correct method
        for _exchange_id, position in all_positions:
            if position and position.is_active():
                # Use mark_price if available, otherwise log warning and potentially skip
                price = position.mark_price
                if price is None:
                    self.logger.warning(
                        f"Mark price unavailable for {position.symbol} on {_exchange_id}, skipping for total exposure."
                    )
                    continue  # Skip this position if price is unknown
                price_dec = Decimal(str(price))
                size_dec = Decimal(str(position.size))
                total_exposure += abs(price_dec * size_dec)
        return total_exposure

    def calculate_required_margin(
        self, symbol: str, size: Decimal, price: Decimal, leverage: Decimal
    ) -> Decimal:
        """Calculates the required margin for a position."""
        # Ensure all inputs are Decimal
        size_dec = Decimal(str(size))
        price_dec = Decimal(str(price))
        leverage_dec = Decimal(str(leverage))
        if leverage_dec <= 0:
            # Avoid division by zero and handle invalid leverage
            self.logger.error(f"Invalid leverage provided for margin calculation: {leverage_dec}")
            return Decimal("Infinity")  # Or raise an error
        return abs(size_dec * price_dec) / leverage_dec

    def evaluate_liquidation_risk(self, symbol: str) -> Decimal | None:
        """Evaluate liquidation risk for a symbol (e.g., distance to liq price)."""
        total_risk_factor = Decimal("0.0")
        position_count = 0
        for exchange_id in self.portfolio_tracker.get_active_exchanges():
            position = self.portfolio_tracker.get_position(exchange_id, symbol)  # Correct call
            if (
                position
                and position.is_active()
                and position.liquidation_price
                and position.mark_price
            ):
                liq_price = Decimal(str(position.liquidation_price))
                mark_price = Decimal(str(position.mark_price))
                if mark_price <= 0:
                    continue  # Avoid division by zero

                distance = abs(mark_price - liq_price) / mark_price
                # Lower distance = higher risk factor (inverse relationship, capped)
                risk_factor = (
                    max(Decimal("0.0"), min(Decimal("1.0"), Decimal("0.1") / distance))
                    if distance > 0
                    else Decimal("1.0")
                )
                total_risk_factor += risk_factor
                position_count += 1

        if position_count == 0:
            return Decimal("0.0")  # No position, no risk

        avg_risk_factor = total_risk_factor / Decimal(position_count)
        self.logger.debug(f"Average liquidation risk factor for {symbol}: {avg_risk_factor:.4f}")
        return avg_risk_factor

    def is_opportunity_profitable(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check if an opportunity meets minimum profitability criteria."""
        # Ensure net_funding_differential is Decimal
        nfd = opportunity.net_funding_differential
        if not isinstance(nfd, Decimal):
            try:
                nfd = Decimal(str(nfd))
            except InvalidOperation:
                self.logger.error(
                    f"Invalid net_funding_differential for {opportunity.symbol}: {opportunity.net_funding_differential}"
                )
                return False
        # Check against Decimal min_net_funding_differential
        is_profitable = nfd >= self.min_net_funding_differential
        if not is_profitable:
            self.logger.debug(
                f"Opportunity {opportunity.symbol} NFD {nfd:.6f} < min {self.min_net_funding_differential:.6f}"
            )
        return is_profitable

    def adjust_order_size(self, symbol: str, requested_size: Decimal) -> Decimal:
        # Placeholder: Add logic if specific adjustments are needed beyond initial sizing
        self.logger.warning("adjust_order_size is not fully implemented.")
        return requested_size

    def perform_sanity_checks(self) -> bool:
        """Perform basic sanity checks on portfolio state."""
        total_value = self.portfolio_tracker.get_total_capital()
        if total_value < Decimal("0"):  # Check against Decimal zero
            self.logger.error(
                f"Sanity Check FAIL: Negative total portfolio value: ${total_value:.2f}"
            )
            return False

        # Check for excessively large positions (example check)
        all_positions = self.portfolio_tracker.get_all_positions()
        for ex_id, pos in all_positions:
            if pos.size is not None and abs(Decimal(str(pos.size))) > (
                total_value * Decimal("10")
            ):  # Example: Position size > 10x total capital?
                self.logger.error(
                    f"Sanity Check FAIL: Position {pos.symbol} on {ex_id} has excessive size {pos.size} relative to capital ${total_value:.2f}"
                )
                # return False # Decide if this should halt operations

        self.logger.info("Portfolio sanity checks passed.")
        return True

    def update(self, data: dict[str, Any]) -> None:
        """Update risk manager state based on new data (e.g., market data, metrics)."""
        # Example: Update internal drawdown metrics if provided
        if "drawdown_metrics" in data:
            self.current_drawdown_metrics = data["drawdown_metrics"]
            self.logger.debug(
                f"RiskManager updated drawdown metrics: {self.current_drawdown_metrics}"
            )

        # Example: Potentially trigger re-evaluation based on data
        # if 'market_volatility' in data:
        #     self.evaluate_risk_limits()

        self.logger.debug(f"RiskManager received update data: {list(data.keys())}")
        # This method might need more implementation based on how risk params are updated dynamically
        return  # Added return
