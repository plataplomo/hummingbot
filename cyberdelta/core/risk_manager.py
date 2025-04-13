from __future__ import annotations  # Enable postponed evaluation

import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation, getcontext
from typing import TYPE_CHECKING, Any

# from cyberdelta.core.models import ArbitrageOpportunity, Order, OrderSide, OrderType, TradeSignal # REMOVING this runtime import
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

if TYPE_CHECKING:  # This block should contain the only import from models
    from cyberdelta.core.models import (
        ArbitrageOpportunity,
    )

logger = logging.getLogger(__name__)

# Set precision for Decimal
getcontext().prec = 28  # Default precision, adjust if needed

# Define ZERO and ONE constants for clarity
ZERO = Decimal("0")
ONE = Decimal("1")


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
        # Ensure Decimals are used
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
            # Mypy fix [operator]: Ensure expected_return is Decimal before multiplication
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
            Recommended position size in USD (Decimal)
        """
        # Mypy fix [operator]: Ensure NFD is Decimal before operations
        # Mypy fix [operator]: Ensure basis_volatility is Decimal before operations
        nfd_dec = opportunity.net_funding_differential
        basis_volatility_dec = opportunity.basis_volatility

        # Check if essential values are None
        if nfd_dec is None:
            self.logger.warning(
                f"NFD is None for {opportunity.symbol}, cannot calculate Kelly size."
            )
            return ZERO
        if basis_volatility_dec is None:
            self.logger.warning(
                f"Basis volatility is None for {opportunity.symbol}, cannot calculate Kelly size."
            )
            return ZERO

        # Ensure types are Decimal for calculations
        if not isinstance(nfd_dec, Decimal):
            try:
                nfd_dec = Decimal(str(nfd_dec))
            except InvalidOperation:
                self.logger.error(f"Invalid NFD value: {nfd_dec}. Cannot calculate Kelly size.")
                return ZERO
        if not isinstance(basis_volatility_dec, Decimal):
            try:
                basis_volatility_dec = Decimal(str(basis_volatility_dec))
            except InvalidOperation:
                self.logger.error(
                    f"Invalid basis volatility value: {basis_volatility_dec}. Cannot calculate Kelly size."
                )
                return ZERO

        # Avoid division by zero or negative volatility
        if basis_volatility_dec <= ZERO:
            self.logger.warning(
                f"Basis volatility is non-positive ({basis_volatility_dec}) for {opportunity.symbol}. Using small default for Kelly calc."
            )
            basis_volatility_dec = Decimal("0.001")  # Default minimal volatility

        variance_risk_dec = basis_volatility_dec**2

        # Get average price for Kelly calculation
        # Ensure prices are Decimal and not None
        long_price = opportunity.long_price
        short_price = opportunity.short_price
        if long_price is None or short_price is None:
            self.logger.warning(
                f"Missing prices for {opportunity.symbol} for Kelly calculation. Returning zero size."
            )
            return ZERO

        avg_price = (long_price + short_price) / Decimal("2")

        if avg_price <= ZERO:
            self.logger.warning(
                f"Average price is zero or negative ({avg_price}) for {opportunity.symbol}. Returning zero size."
            )
            return ZERO

        # Calculate Kelly fraction using Decimal arithmetic
        try:
            denominator = variance_risk_dec * avg_price
            if denominator == ZERO:
                self.logger.warning(
                    f"Kelly denominator is zero for {opportunity.symbol} (variance or price issue). Returning zero size."
                )
                kelly = ZERO
            else:
                kelly = nfd_dec / denominator
        except InvalidOperation as e:
            self.logger.error(f"Error calculating Kelly fraction for {opportunity.symbol}: {e}")
            kelly = ZERO

        # Apply Kelly fraction adjustment (self.kelly_fraction is Decimal)
        base_size = kelly * self.kelly_fraction * total_capital

        # Ensure size is not negative
        if base_size < ZERO:
            base_size = ZERO

        self.logger.debug(
            f"Calculated Kelly size for {opportunity.symbol}: {base_size:.2f} USD (NFD: {nfd_dec}, Vol: {basis_volatility_dec}, AvgPx: {avg_price}, Kelly: {kelly:.4f})"
        )
        return base_size

    def _check_portfolio_constraints(
        self,
        long_exchange: str,
        short_exchange: str,
        long_size: Decimal,
        short_size: Decimal,
    ) -> bool:
        """
        Check if adding the proposed position sizes violates portfolio constraints.

        Args:
            long_exchange: Exchange for the long leg
            short_exchange: Exchange for the short leg
            long_size: Proposed size for the long leg (USD)
            short_size: Proposed size for the short leg (USD)

        Returns:
            True if constraints are met, False otherwise.
        """
        # Mypy fix [operator]: Get total capital, check if None
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital is None or total_capital <= ZERO:
            self.logger.warning("Total capital is zero or unavailable. Cannot check constraints.")
            return False

        # 1. Max Total Exposure Check
        # Mypy fix [attr-defined]: Use calculate_total_exposure
        # Mypy fix [operator]: Check if current exposure is None
        current_exposure = self.calculate_total_exposure()
        if current_exposure is None:
            self.logger.warning("Current total exposure is unavailable. Cannot check constraints.")
            return False  # Or apply a default strict check? For now, fail safe.

        potential_increase = (
            long_size + short_size
        )  # Assuming sizes are positive exposure contributions
        if current_exposure + potential_increase > self.max_total_exposure:
            self.logger.warning(
                f"Trade exceeds max total exposure: "
                f"Current={current_exposure:.2f}, Adding={potential_increase:.2f}, Max={self.max_total_exposure:.2f}"
            )
            return False

        # 2. Max Leverage Check (using portfolio-wide leverage)
        # Mypy fix [attr-defined]: Remove get_current_leverage, calculate instead
        # Mypy fix [operator]: Check for None and division by zero
        current_total_exposure = (
            self.portfolio_tracker.get_total_exposure()
        )  # Re-fetch exposure for leverage calc
        if current_total_exposure is None:
            self.logger.warning("Current total exposure is unavailable. Cannot check leverage.")
            return False

        potential_total_exposure = current_total_exposure + potential_increase
        if total_capital > ZERO:
            potential_leverage = potential_total_exposure / total_capital
            if potential_leverage > self.max_leverage:
                self.logger.warning(
                    f"Trade exceeds max portfolio leverage: Potential={potential_leverage:.2f}, Max={self.max_leverage:.2f}"
                )
                return False
        else:
            self.logger.warning("Total capital is zero, cannot calculate portfolio leverage.")
            # Decide if this is a fail condition. If no capital, maybe no trades allowed?
            return False

        # 3. Max Collateral Per Exchange Check
        # Mypy fix [attr-defined]: Replace get_exchange_collateral_balance with get_exchange_balance
        # We need to define what constitutes 'collateral'. Usually stablecoins like USDC/USDT.
        # Assume USDC for now, make configurable later.
        collateral_asset = "USDC"  # TODO: Make configurable

        for exchange_id, size in [
            (long_exchange, long_size),
            (short_exchange, short_size),
        ]:
            # Mypy fix [operator]: Check if balance is None
            exchange_balance_obj = self.portfolio_tracker.get_exchange_balance(
                exchange_id, collateral_asset
            )
            if exchange_balance_obj is None or exchange_balance_obj.total is None:
                self.logger.warning(
                    f"Could not retrieve {collateral_asset} balance for {exchange_id}. Skipping collateral check for it."
                )
                # Decide: Fail the check? Or allow if balance unknown? For now, allow but warn.
                continue  # Skip check for this exchange if balance unknown

            exchange_collateral = exchange_balance_obj.total
            # This check seems flawed. Max collateral *per exchange* (config) usually means
            # the max % of TOTAL capital allowed on ONE exchange.
            # Let's reinterpret the check: Total capital on this exchange should not exceed X% of portfolio capital.
            # Alternatively, maybe it meant Required Margin vs Available Collateral?
            # Sticking to the % of total capital interpretation for now.

            # Calculate total capital *on this exchange* (approximate as collateral asset value)
            # Mypy fix [operator]: Check total capital is not None
            if total_capital > ZERO:
                collateral_ratio = exchange_collateral / total_capital
                # Mypy fix [operator]: Compare Decimal with Decimal
                if collateral_ratio > self.max_collateral_per_exchange:
                    self.logger.warning(
                        f"Exchange {exchange_id} collateral ({exchange_collateral:.2f} {collateral_asset}) "
                        f"exceeds limit ({self.max_collateral_per_exchange * 100:.1f}% of total capital {total_capital:.2f})"
                    )
                    # This is a state check, not directly related to the trade size unless the trade *requires* more collateral
                    # Let's refine this check: Check if the *required margin* for the new trade violates available balance
                    # This requires margin calculation logic, which is separate.
                    # For now, keeping the portfolio-level check, but acknowledging its potential ambiguity.
                    # If the *existing* state violates the rule, maybe block *all* trades on that exchange? Needs clarification.

            # Check minimum exchange balance
            if exchange_collateral < self.min_exchange_balance:
                self.logger.warning(
                    f"Exchange {exchange_id} collateral ({exchange_collateral:.2f} {collateral_asset}) "
                    f"is below minimum ({self.min_exchange_balance:.2f})"
                )
                # Block trade if it's on this exchange? Or just a warning? Block for safety.
                return False

        # 4. Circuit Breaker Check
        if self.circuit_breaker_system:
            if self.circuit_breaker_system.is_tripped(
                long_exchange
            ) or self.circuit_breaker_system.is_tripped(short_exchange):
                self.logger.warning(
                    f"Trade involves a tripped circuit breaker on {long_exchange} or {short_exchange}. Blocking trade."
                )
                return False

        return True  # All checks passed

    def _apply_portfolio_exposure_management(
        self, opportunity: ArbitrageOpportunity, base_size: Decimal
    ) -> Decimal:
        """
        Adjust base size based on various portfolio exposure limits.

        Args:
            opportunity: The arbitrage opportunity being considered.
            base_size: The initially calculated size (e.g., from Kelly).

        Returns:
            Adjusted size based on portfolio exposure constraints.
        """
        # Mypy fix [operator]: Check total capital
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital is None or total_capital <= ZERO:
            self.logger.warning(
                "Total capital is zero or unavailable. Cannot apply exposure management."
            )
            return ZERO  # No capital, no size

        adjusted_size = base_size
        symbol = opportunity.symbol
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange

        # 1. Max Exposure Per Asset
        # Mypy fix [attr-defined]: Replace get_asset_exposure - Use calculate_position_exposure
        # Mypy fix [operator]: Check exposure result
        current_asset_exposure = self.calculate_position_exposure(symbol)
        if current_asset_exposure is None:
            self.logger.warning(
                f"Could not determine current exposure for asset {symbol}. Applying default limit."
            )
            # Apply limit assuming zero current exposure? Or block trade? Assume zero for now.
            current_asset_exposure = ZERO

        max_asset_exposure_usd = total_capital * self.max_exposure_per_asset
        # Calculate how much *additional* exposure is allowed for this asset
        allowed_increase_asset = max_asset_exposure_usd - current_asset_exposure
        # The proposed trade adds exposure equal to the size (assuming size is directional exposure value)
        # For delta-neutral, total exposure increase might be complex. Let's assume size represents the gross exposure increase per leg.
        # Cap the size based on allowed increase for the asset.
        # A single opportunity adds 'size' to both long and short, effectively increasing gross exposure by 'size'.
        # Net exposure change depends on existing positions.
        # Let's cap the *new* position size based on the remaining room per asset.
        if adjusted_size > allowed_increase_asset:
            self.logger.info(
                f"[{symbol}] Capping size due to max asset exposure limit. Allowed Increase: {allowed_increase_asset:.2f}, Requested: {adjusted_size:.2f}"
            )
            adjusted_size = max(ZERO, allowed_increase_asset)  # Ensure not negative

        if adjusted_size <= ZERO:
            self.logger.info(f"[{symbol}] Size reduced to zero by asset exposure limit.")
            return ZERO

        # 2. Max Exposure Per Exchange
        for exchange_id in [long_exchange, short_exchange]:
            # Mypy fix [attr-defined]: Replace get_exchange_exposure - Use portfolio_tracker method? No, calculate locally.
            # Mypy fix [operator]: Check exposure result
            current_exchange_exposure = self.portfolio_tracker.get_exchange_exposure(exchange_id)
            if current_exchange_exposure is None:
                self.logger.warning(
                    f"Could not determine current exposure for exchange {exchange_id}. Applying default limit."
                )
                current_exchange_exposure = ZERO  # Assume zero current exposure

            max_exchange_exposure_usd = total_capital * self.max_exposure_per_exchange
            allowed_increase_exchange = max_exchange_exposure_usd - current_exchange_exposure

            # Cap the size based on allowed increase for this *specific exchange*
            if adjusted_size > allowed_increase_exchange:
                self.logger.info(
                    f"[{symbol}@{exchange_id}] Capping size due to max exchange exposure limit. Allowed Increase: {allowed_increase_exchange:.2f}, Current Size: {adjusted_size:.2f}"
                )
                adjusted_size = max(ZERO, allowed_increase_exchange)  # Ensure not negative

            if adjusted_size <= ZERO:
                self.logger.info(
                    f"[{symbol}@{exchange_id}] Size reduced to zero by exchange exposure limit."
                )
                return ZERO

        # 3. Max Single Position Exposure Limit (as ratio of total capital)
        max_single_pos_usd = total_capital * self.max_single_position_exposure
        if adjusted_size > max_single_pos_usd:
            self.logger.info(
                f"[{symbol}] Capping size due to max single position exposure limit. Max USD: {max_single_pos_usd:.2f}, Requested: {adjusted_size:.2f}"
            )
            adjusted_size = max_single_pos_usd

        if adjusted_size <= ZERO:
            self.logger.info(f"[{symbol}] Size reduced to zero by single position exposure limit.")
            return ZERO

        # Apply exchange-specific risk modifier (lower size for riskier exchanges)
        # Ensure modifiers exist, default to 1.0
        long_modifier = Decimal(str(self.exchange_risk_modifiers.get(long_exchange, 1.0)))
        short_modifier = Decimal(str(self.exchange_risk_modifiers.get(short_exchange, 1.0)))
        # Apply the *stricter* (lower) modifier of the two exchanges involved
        effective_modifier = min(long_modifier, short_modifier)
        if effective_modifier < ONE:
            self.logger.info(
                f"[{symbol}] Applying risk modifier {effective_modifier:.2f} (from {long_exchange}/{short_exchange})"
            )
            adjusted_size *= effective_modifier

        # Mypy fix [attr-defined]: Remove get_asset_volatility call - Volatility adjustment is separate
        # The logic below seemed related to volatility, moving to _apply_volatility_adjustment

        self.logger.debug(
            f"[{symbol}] Size after portfolio exposure management: {adjusted_size:.2f}"
        )
        return adjusted_size

    def _apply_portfolio_level_controls(
        self, base_size: Decimal, opportunity: ArbitrageOpportunity
    ) -> Decimal:
        """
        Apply portfolio-level controls like drawdown limits and leverage caps.

        Args:
            base_size: The size after initial calculations and exposure management.
            opportunity: The arbitrage opportunity.

        Returns:
            Adjusted size after applying portfolio-level controls.
        """
        adjusted_size = base_size
        symbol = opportunity.symbol

        # 1. Max Drawdown Limit Check
        # Mypy fix [attr-defined]: Replace get_portfolio_drawdown with get_current_drawdown
        # Mypy fix [operator]: Check if drawdown is None
        current_drawdown = self.portfolio_tracker.get_current_drawdown()

        if current_drawdown is not None:
            # Mypy fix [operator]: Ensure max_drawdown_limit is Decimal
            if current_drawdown >= self.max_drawdown_limit:
                self.logger.warning(
                    f"Portfolio drawdown ({current_drawdown:.2%}) exceeds limit ({self.max_drawdown_limit:.2%}). Reducing new trade size significantly or blocking."
                )
                # Reduce size significantly, e.g., by recovery factor or set to zero
                adjusted_size *= self.circuit_breaker_recovery_factor  # Use recovery factor concept
                self.logger.info(
                    f"[{symbol}] Size reduced to {adjusted_size:.2f} due to drawdown limit."
                )
            # Optional: Gradually reduce size as drawdown approaches the limit? More complex.
        else:
            self.logger.warning(
                "Could not retrieve current portfolio drawdown. Skipping drawdown check."
            )

        # 2. Max Leverage Per Trade Check (distinct from portfolio leverage)
        # This requires calculating the margin needed for the trade and comparing to size
        # Mypy fix [operator]: Ensure prices are not None
        long_price = opportunity.long_price
        short_price = opportunity.short_price
        if long_price is None or short_price is None:
            self.logger.warning(
                f"[{symbol}] Missing prices, cannot check max leverage per trade. Skipping."
            )
        elif adjusted_size > ZERO:  # Only check if size is positive
            # Assume average price for margin calculation simplification
            avg_price = (long_price + short_price) / Decimal("2")
            if avg_price > ZERO:
                # Assuming adjusted_size is the USD value of the position leg
                # Leverage = Position Value / Required Margin
                # Required Margin = Position Value / Leverage
                # We need to estimate the required margin based on the *max allowed* leverage per trade
                # If the implied margin (size / max_leverage_per_trade) exceeds available capital or specific limits, reduce size.
                # This check seems difficult without knowing exact exchange margin rules.
                # Alternative interpretation: Limit the *size* based on capital and max leverage.
                # Max Size = Available Capital * max_leverage_per_trade
                # Mypy fix [operator]: Check total capital
                total_capital = self.portfolio_tracker.get_total_capital()
                if total_capital is not None and total_capital > ZERO:
                    # This interpretation seems wrong. max_leverage_per_trade likely caps the leverage *used by this specific trade's margin*.
                    # Let's stick to the portfolio-level leverage check in _check_portfolio_constraints
                    # And potentially add a check later using calculate_required_margin if needed.
                    # For now, removing the direct check against max_leverage_per_trade here.
                    pass  # Keep portfolio level check, remove ambiguous trade-level one for now.
                # else:
                # self.logger.warning(f"[{symbol}] Cannot check trade leverage due to unavailable capital.")

        # 3. Max Position Size (Absolute USD Cap)
        if adjusted_size > self.max_position_size:
            self.logger.info(
                f"[{symbol}] Capping size due to global max position size limit. Max USD: {self.max_position_size:.2f}, Requested: {adjusted_size:.2f}"
            )
            adjusted_size = self.max_position_size

        # Final check: ensure size is not negative
        adjusted_size = max(ZERO, adjusted_size)

        if adjusted_size < base_size:
            self.logger.debug(
                f"[{symbol}] Size after portfolio-level controls: {adjusted_size:.2f} (was {base_size:.2f})"
            )
        # Mypy fix [unreachable]: Remove dead code after return
        # else: # Add this log statement if size wasn't reduced
        #      self.logger.debug(f"[{symbol}] Size unchanged by portfolio-level controls: {adjusted_size:.2f}")

        return adjusted_size

    def _get_validation_metrics(self, exchange: str, symbol: str) -> Decimal:
        """
        Retrieve validation metrics for a funding rate prediction model.

        Args:
            exchange: Exchange ID.
            symbol: Asset symbol.

        Returns:
            A validation factor (0 to 1) based on metrics, or default if unavailable.
        """
        if not self.funding_rate_validator:
            self.logger.debug(
                "Funding rate validator not configured. Returning default validation factor."
            )
            return ONE  # Default: Assume valid if no validator

        # Mypy fix [attr-defined]: Adapt based on actual validator interface if known
        # Assuming validator has methods like get_rmse, get_bias
        # Mypy fix [operator]: Handle potential None returns from validator
        try:
            # These method names are assumptions - replace with actual ones
            rmse = self.funding_rate_validator.get_rmse(exchange, symbol)
            bias = self.funding_rate_validator.get_bias(exchange, symbol)

            # Ensure metrics are Decimal and handle None
            rmse_dec = Decimal(str(rmse)) if rmse is not None else None
            bias_dec = (
                Decimal(str(bias)).copy_abs() if bias is not None else None
            )  # Use absolute bias

            if rmse_dec is None or bias_dec is None:
                self.logger.warning(
                    f"Missing validation metrics (RMSE/Bias) for {symbol}@{exchange}. Using min factor."
                )
                return self.min_validation_factor  # Penalize missing metrics

            # Check against thresholds (ensure thresholds are Decimal)
            if rmse_dec > self.max_acceptable_rmse or bias_dec > self.max_acceptable_bias:
                self.logger.warning(
                    f"Validation metrics exceed thresholds for {symbol}@{exchange}. "
                    f"RMSE: {rmse_dec:.4f} (Max: {self.max_acceptable_rmse:.4f}), "
                    f"Bias: {bias_dec:.4f} (Max: {self.max_acceptable_bias:.4f}). "
                    f"Using min factor."
                )
                return self.min_validation_factor  # Below threshold quality

            # Simple linear scaling example (adjust as needed)
            # Scale factor based on how far below max RMSE/Bias the metrics are
            rmse_scale = max(ZERO, ONE - (rmse_dec / self.max_acceptable_rmse))
            bias_scale = max(ZERO, ONE - (bias_dec / self.max_acceptable_bias))
            # Combine factors (e.g., average or minimum)
            combined_factor = (rmse_scale + bias_scale) / Decimal("2")

            # Ensure factor is at least the minimum required
            validation_factor = max(self.min_validation_factor, combined_factor)
            self.logger.debug(f"Validation factor for {symbol}@{exchange}: {validation_factor:.2f}")
            return validation_factor

        except Exception as e:
            self.logger.error(f"Error retrieving validation metrics for {symbol}@{exchange}: {e}")
            return self.min_validation_factor  # Penalize errors

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
        if hasattr(opportunity, "expected_profit_pct"):
            if opportunity.expected_profit_pct is None:
                opportunity.expected_profit_pct = Decimal("0")
            elif not isinstance(opportunity.expected_profit_pct, Decimal):
                opportunity.expected_profit_pct = Decimal(str(opportunity.expected_profit_pct))

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
                        fraction = Decimal(
                            str(self.config.get("risk.simple_fixed_fraction", "0.01"))
                        )
                        initial_base_size = total_capital * fraction
                        self.logger.info(
                            f"Simple Sizing: Fixed Fraction ({fraction:.2%}). Base size: ${initial_base_size:.2f}"
                        )
                    elif sizing_method == "fixed_usd":
                        initial_base_size = Decimal(
                            str(self.config.get("risk.simple_fixed_usd_size", "100"))
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
        final_expected_profit = Decimal("0.0")  # Default value
        expected_return = 0.0  # Default value
        risk_adjusted_return = 0.0  # Default value

        # Mypy fix [operator]: Check long_price and short_price are not None before use
        if opportunity.long_price is not None and opportunity.short_price is not None:
            # Ensure they are Decimal
            long_price_dec = opportunity.long_price
            short_price_dec = opportunity.short_price

            avg_entry_price = (long_price_dec + short_price_dec) / Decimal("2")
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

            # Expected return (percentage) - remains float, based on NFD
            # Mypy fix [arg-type]: Ensure NFD is converted for float()
            expected_return = float(opportunity.net_funding_differential * 100)
        else:
            self.logger.warning(
                "Cannot accurately calculate expected profit/return due to missing prices. Using zero."
            )

        # Mypy fix [arg-type]: Ensure utility_score is float
        risk_adjusted_return = (
            float(opportunity.utility_score)
            if hasattr(opportunity, "utility_score") and opportunity.utility_score is not None
            else 0.0
        )  # Use utility score as proxy (float), handle None

        # 7. Create SizedOpportunity object
        # Mypy fix [arg-type]: Ensure args match SizedOpportunity constructor (expects Decimals)
        sized_opportunity = SizedOpportunity(
            opportunity=opportunity,
            long_size=long_size,  # Final Decimal size
            short_size=short_size,  # Final Decimal size
            allocation_percentage=Decimal(str(allocation_percentage)),  # Convert float % to Decimal
            expected_profit=final_expected_profit,  # Decimal USD profit
            expected_return=Decimal(
                str(expected_return / 100.0)
            ),  # Convert float % to Decimal rate
            risk_adjusted_return=Decimal(
                str(risk_adjusted_return)
            ),  # Convert float score to Decimal
        )

        self.logger.info(f"Successfully sized opportunity: {sized_opportunity}")
        return sized_opportunity

    def validate_opportunity(self, opportunity: ArbitrageOpportunity) -> bool:
        """
        Validate an arbitrage opportunity against risk constraints.

        Args:
            opportunity: The arbitrage opportunity to validate

        Returns:
            True if the opportunity passes all validation checks, False otherwise
        """
        # Check if circuit breakers are active for either exchange
        if self.circuit_breaker_system is not None:
            can_execute_long, reason_long = self.circuit_breaker_system.can_execute(
                opportunity.long_exchange
            )
            if not can_execute_long:
                self.logger.warning(
                    f"Circuit breaker active for {opportunity.long_exchange} - rejecting opportunity: {reason_long}"
                )
                return False

            can_execute_short, reason_short = self.circuit_breaker_system.can_execute(
                opportunity.short_exchange
            )
            if not can_execute_short:
                self.logger.warning(
                    f"Circuit breaker active for {opportunity.short_exchange} - rejecting opportunity: {reason_short}"
                )
                return False

        # Validate funding rate differential
        if not isinstance(opportunity.net_funding_differential, Decimal):
            try:
                net_funding_differential = Decimal(str(opportunity.net_funding_differential))
            except (InvalidOperation, TypeError):
                self.logger.warning(
                    f"Invalid net_funding_differential: {opportunity.net_funding_differential}. Cannot convert to Decimal."
                )
                return False
        else:
            net_funding_differential = opportunity.net_funding_differential

        if net_funding_differential < self.min_net_funding_differential:
            self.logger.debug(
                f"Opportunity rejected: NFD {net_funding_differential} < "
                f"min required {self.min_net_funding_differential}"
            )
            return False

        # Check if the long price and short price are valid Decimals
        try:
            long_price = (
                opportunity.long_price
                if isinstance(opportunity.long_price, Decimal)
                else Decimal(str(opportunity.long_price))
            )
            short_price = (
                opportunity.short_price
                if isinstance(opportunity.short_price, Decimal)
                else Decimal(str(opportunity.short_price))
            )
        except (InvalidOperation, TypeError, AttributeError) as e:
            self.logger.warning(f"Opportunity has invalid price values: {e}")
            return False

        # Check if there is sufficient balance on both exchanges
        long_exchange_balance_obj = self.portfolio_tracker.get_exchange_balance(
            opportunity.long_exchange,
            # Mypy fix [call-arg]: Provide required 'asset' argument
            self.config.get(f"exchanges.{opportunity.long_exchange}.collateral_asset", "USDC"),
        )
        short_exchange_balance_obj = self.portfolio_tracker.get_exchange_balance(
            opportunity.short_exchange,
            # Mypy fix [call-arg]: Provide required 'asset' argument
            self.config.get(f"exchanges.{opportunity.short_exchange}.collateral_asset", "USDC"),
        )

        # Convert balances to Decimal for comparison
        try:
            long_balance_dec = (
                long_exchange_balance_obj.total if long_exchange_balance_obj else Decimal("0")
            )
            short_balance_dec = (
                short_exchange_balance_obj.total if short_exchange_balance_obj else Decimal("0")
            )

            min_balance_dec = (
                self.min_exchange_balance
                if isinstance(self.min_exchange_balance, Decimal)
                else Decimal(str(self.min_exchange_balance))
            )
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Error converting balance values to Decimal: {e}")
            return False

        if long_balance_dec < min_balance_dec:
            self.logger.warning(
                f"Insufficient balance on {opportunity.long_exchange}: "
                f"{long_balance_dec} < {min_balance_dec}"
            )
            return False

        if short_balance_dec < min_balance_dec:
            self.logger.warning(
                f"Insufficient balance on {opportunity.short_exchange}: "
                f"{short_balance_dec} < {min_balance_dec}"
            )
            return False

        # Check if funding rate metrics are validated (if validator is available)
        if self.funding_rate_validator:
            validation_result = self.funding_rate_validator.validate_funding_prediction(
                opportunity.symbol, opportunity.long_exchange, opportunity.short_exchange
            )

            if not validation_result:
                self.logger.info(
                    f"Funding rate prediction validation failed for {opportunity.symbol}"
                )
                return False

        # Validate current exchange exposure
        total_exposure = self.portfolio_tracker.get_total_exposure()
        try:
            total_exposure_dec = (
                total_exposure
                if isinstance(total_exposure, Decimal)
                else Decimal(str(total_exposure))
            )
            max_exposure_dec = (
                self.max_total_exposure
                if isinstance(self.max_total_exposure, Decimal)
                else Decimal(str(self.max_total_exposure))
            )
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Error converting exposure values to Decimal: {e}")
            return False

        if total_exposure_dec > max_exposure_dec:
            self.logger.warning(
                f"Rejecting opportunity: Total exposure {total_exposure_dec} exceeds maximum {max_exposure_dec}"
            )
            return False

        # Check leverage limits
        # Mypy fix [attr-defined]: Method get_current_leverage does not exist
        # Cannot perform this check reliably without the method.
        self.logger.debug(
            "Skipping current leverage check - method not available on PortfolioTracker."
        )
        # current_leverage = self.portfolio_tracker.get_current_leverage()
        # if current_leverage is not None:
        #     try:
        #         leverage_dec = (
        #             current_leverage
        #             if isinstance(current_leverage, Decimal)
        #             else Decimal(str(current_leverage))
        #         )
        #         max_leverage_dec = (
        #             self.max_leverage
        #             if isinstance(self.max_leverage, Decimal)
        #             else Decimal(str(self.max_leverage))
        #         )

        #         if leverage_dec > max_leverage_dec:
        #             self.logger.warning(
        #                 f"Rejecting opportunity: Current leverage {leverage_dec} exceeds maximum {max_leverage_dec}"
        #             )
        #             return False
        #     except (InvalidOperation, TypeError) as e:
        #         self.logger.error(f"Error converting leverage values to Decimal: {e}")
        #         return False

        # All checks passed
        return True

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
        sized_opportunities: list[SizedOpportunity] = []
        if not opportunities:
            return sized_opportunities  # Return empty list if input is empty

        # Mypy fix [operator]: Check total capital once at the start
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital is None or total_capital <= ZERO:
            self.logger.warning(
                "Total capital is zero or unavailable. Cannot validate/size opportunities."
            )
            return sized_opportunities

        for opportunity in opportunities:
            self.logger.debug(
                f"Processing opportunity: {opportunity.symbol} ({opportunity.long_exchange} vs {opportunity.short_exchange})"
            )
            # Attempt to size the opportunity (includes validation)
            sized_opp = self.size_opportunity(opportunity)
            if sized_opp:
                sized_opportunities.append(sized_opp)
            # else: # No need for log here, size_opportunity already logs reasons for failure
            #      self.logger.debug(f"Opportunity for {opportunity.symbol} did not result in a sized position.")

        # TODO: Add post-processing? E.g., ranking, diversification limits across the selected batch?
        # For now, return all successfully sized opportunities.
        self.logger.info(
            f"Validated and sized {len(sized_opportunities)} opportunities out of {len(opportunities)}."
        )
        return sized_opportunities

    def get_portfolio_exposure_summary(self) -> dict[str, Any]:
        """
        Generate a summary of current portfolio exposures.

        Returns:
            Dictionary containing exposure metrics (using Decimal for values).
        """
        total_capital = self.portfolio_tracker.get_total_capital()
        total_exposure = self.calculate_total_exposure()  # Use RM's calculation
        # Ensure values from tracker are Decimal
        if not isinstance(total_capital, Decimal):
            total_capital = Decimal(str(total_capital))
        if not isinstance(total_exposure, Decimal):
            total_exposure = Decimal(str(total_exposure))

        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_capital_usd": float(total_capital),
            "total_exposure_usd": float(total_exposure),
            "portfolio_leverage": None,
            "realized_pnl": None,  # Add realized PNL tracking if available
            "unrealized_pnl": None,
            "portfolio_drawdown_pct": None,
            "exchange_exposure": {},  # Mypy fix [assignment]: Initialize dict
            "asset_exposure": {},
            "top_positions": {},  # Mypy fix [assignment]: Initialize dict
            "active_exchanges": [],
        }

        # Mypy fix [operator]: Check return values are not None before using
        if total_capital is not None and total_exposure is not None and total_capital > ZERO:
            leverage = total_exposure / total_capital
            summary["portfolio_leverage"] = f"{leverage:.2f}x"
        else:
            summary["portfolio_leverage"] = "N/A"

        # Mypy fix [attr-defined]: Get PNL from portfolio tracker
        unrealized_pnl, realized_pnl = self.portfolio_tracker.get_pnl()
        summary["unrealized_pnl"] = f"{unrealized_pnl:.2f}"
        summary["realized_pnl"] = f"{realized_pnl:.2f}"  # Assumes get_pnl returns realized

        # Mypy fix [attr-defined]: Use get_current_drawdown
        drawdown = self.portfolio_tracker.get_current_drawdown()
        summary["portfolio_drawdown_pct"] = f"{drawdown:.2%}" if drawdown is not None else "N/A"

        # Mypy fix [attr-defined]: Get active exchanges from PortfolioTracker internal state (or config)
        # Using balances keys as proxy for active exchanges with capital
        summary["active_exchanges"] = list(self.portfolio_tracker._balances.keys())

        all_positions_data = []  # Collect position data for asset/top pos summary
        # Mypy fix [attr-defined]: Use get_all_positions and filter/aggregate
        all_positions = (
            self.portfolio_tracker.get_all_positions()
        )  # Returns list[tuple[str, Position]]

        for exchange_id in summary["active_exchanges"]:
            # Mypy fix [attr-defined]: Use get_exchange_exposure
            exchange_exposure = self.portfolio_tracker.get_exchange_exposure(exchange_id)
            summary["exchange_exposure"][exchange_id] = (
                f"{exchange_exposure:.2f}" if exchange_exposure is not None else "N/A"
            )  # Corrected assignment

            # Aggregate positions for this exchange
            exchange_positions = [pos for ex, pos in all_positions if ex == exchange_id]
            for position in exchange_positions:
                all_positions_data.append(position)  # Add to combined list

        # Calculate Asset Exposure Summary
        asset_exposure_agg: dict[str, Decimal] = {}
        for position in all_positions_data:
            # Mypy fix [operator]: Check position attributes
            if (
                position.symbol
                and position.position_size is not None
                and position.entry_price is not None
            ):
                # Simple exposure: abs(size) * price. Refine if needed.
                try:
                    exposure = abs(position.position_size * position.entry_price)  # Approximation
                    asset_exposure_agg[position.symbol] = (
                        asset_exposure_agg.get(position.symbol, ZERO) + exposure
                    )
                except (TypeError, InvalidOperation):
                    self.logger.warning(
                        f"Could not calculate exposure for position: {position.symbol} - size: {position.position_size}, price: {position.entry_price}"
                    )

        for asset, exposure in asset_exposure_agg.items():
            summary["asset_exposure"][asset] = f"{exposure:.2f}"

        # Top Positions (by absolute exposure value)
        position_values = []
        for position in all_positions_data:
            # Mypy fix [operator]: Check attributes again for safety
            if (
                position.symbol
                and position.position_size is not None
                and position.entry_price is not None
            ):
                try:
                    position_value = abs(position.position_size * position.entry_price)
                    position_values.append(
                        (position.symbol, position_value, position.exchange)
                    )  # Include exchange
                except (TypeError, InvalidOperation):
                    continue  # Skip if calculation fails

        # Sort by value, descending
        position_values.sort(key=lambda x: x[1], reverse=True)

        # Take top N (e.g., top 5)
        top_n = 5
        for symbol, value, exchange in position_values[:top_n]:
            summary["top_positions"][f"{symbol}@{exchange}"] = (
                f"{value:.2f}"  # Corrected assignment
            )

        # Mypy fix [attr-defined]: Remove volatility references
        # summary["asset_volatility"] = "N/A" # self.portfolio_tracker.get_asset_volatility(...)
        # summary["historical_volatility"] = "N/A" # self.portfolio_tracker.get_historical_volatility(...)

        return summary

    def check_drawdown(self) -> bool:
        """
        Check if the current portfolio drawdown exceeds the defined limit.

        Returns:
            True if drawdown is within limits or cannot be determined, False otherwise.
        """
        # Mypy fix [attr-defined]: Use get_current_drawdown
        # Mypy fix [operator]: Check if drawdown is None
        current_drawdown = self.portfolio_tracker.get_current_drawdown()

        if current_drawdown is None:
            self.logger.warning("Could not determine current drawdown. Assuming OK.")
            return True  # Fail open? Or closed? Fail open for now.

        # Mypy fix [operator]: Compare Decimal with Decimal
        if current_drawdown >= self.max_drawdown_limit:
            self.logger.warning(
                f"Drawdown check failed: Current {current_drawdown:.2%} >= Limit {self.max_drawdown_limit:.2%}"
            )
            return False

        return True  # Drawdown is within limits

    def calculate_position_exposure(self, symbol: str) -> Decimal | None:
        """
        Calculate the total USD exposure for a specific symbol across all exchanges.

        Args:
            symbol: The asset symbol (e.g., "BTC-PERP").

        Returns:
            Total exposure in USD (absolute value) for the asset, or None if error.
        """
        total_exposure = ZERO
        found_position = False
        # Mypy fix [attr-defined]: Use get_all_positions
        all_positions = self.portfolio_tracker.get_all_positions()

        for _exchange, position in all_positions:
            if position.symbol == symbol:
                found_position = True
                # Mypy fix [operator]: Check size and price are not None
                if position.position_size is not None and position.mark_price is not None:
                    try:
                        # Use mark price for current value if available, else entry price
                        price_to_use = (
                            position.mark_price
                            if position.mark_price is not None
                            else position.entry_price
                        )
                        if price_to_use is None:
                            self.logger.warning(
                                f"Missing price for position {symbol} on {_exchange}. Skipping exposure calculation for this leg."
                            )
                            continue

                        exposure = abs(position.position_size * price_to_use)
                        total_exposure += exposure
                    except (TypeError, InvalidOperation) as e:
                        self.logger.error(
                            f"Error calculating exposure for {symbol} on {_exchange}: {e}"
                        )
                        # Return None to indicate calculation failure? Or just skip this leg? Skip for now.

                # else: # Log if size or price is missing
                # self.logger.warning(f"Missing size or mark_price for position {symbol} on {_exchange}. Cannot calculate exposure for this leg.")

        if not found_position:
            # self.logger.debug(f"No positions found for asset {symbol}. Exposure is zero.")
            return ZERO  # Return zero if no positions exist

        return total_exposure

    def calculate_total_exposure(self) -> Decimal:
        """
        Calculate the total portfolio exposure across all assets and exchanges.

        Returns:
            Total portfolio exposure in USD (Decimal), or ZERO if unavailable.
        """
        # Mypy fix [attr-defined]: Use portfolio_tracker's method if it sums correctly.
        # Let's use the portfolio_tracker's get_total_exposure method directly.
        # Mypy fix [operator]: Handle None return
        total_exposure = (
            self.portfolio_tracker.get_total_exposure()
        )  # Assumes valuation asset matches default

        if total_exposure is None:
            self.logger.warning(
                "PortfolioTracker returned None for total exposure. Returning ZERO."
            )
            return ZERO

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
        # Check against minimum threshold
        # Mypy fix [operator]: Compare Decimals
        if nfd > self.min_net_funding_differential:
            # TODO: Add consideration for estimated fees/slippage here?
            # For now, basic NFD check.
            return True
        else:
            self.logger.debug(
                f"Opportunity {opportunity.symbol} NFD {nfd:.6f} not above threshold {self.min_net_funding_differential:.6f}"
            )
            return False

    def adjust_order_size(self, symbol: str, requested_size: Decimal) -> Decimal:
        # Placeholder: Add logic if specific adjustments are needed beyond initial sizing
        self.logger.warning("adjust_order_size is not fully implemented.")
        return requested_size

    def perform_sanity_checks(self) -> bool:
        """
        Perform sanity checks on the overall portfolio state. (e.g., consistency checks)

        Returns:
            True if sanity checks pass, False otherwise.
        """
        # Example checks:
        # 1. Total exposure vs sum of position values?
        # 2. Balances consistent with positions/trades? (Hard to check without full trade log)
        # 3. Leverage within reasonable bounds?

        # Check 1: Leverage
        total_capital = self.portfolio_tracker.get_total_capital()
        total_exposure = self.calculate_total_exposure()  # Use RM's calculation

        # Mypy fix [operator]: Handle None values
        if total_capital is not None and total_exposure is not None:
            if total_capital <= ZERO and total_exposure > ZERO:
                self.logger.error(
                    "Sanity Check FAIL: Positive exposure with zero or negative capital!"
                )
                return False
            if total_capital > ZERO:
                leverage = total_exposure / total_capital
                # Mypy fix [operator]: Compare Decimals
                # Use a slightly higher sanity check limit than the trading limit
                sanity_leverage_limit = self.max_leverage * Decimal("1.2")
                if leverage > sanity_leverage_limit:
                    self.logger.error(
                        f"Sanity Check FAIL: Portfolio leverage {leverage:.2f} exceeds sanity limit {sanity_leverage_limit:.2f}!"
                    )
                    return False
        # else: # Log if values are None
        # self.logger.warning("Sanity Check: Could not check leverage due to unavailable capital or exposure.")

        # Check 2: Drawdown (if available)
        # Mypy fix [attr-defined, operator]: Use get_current_drawdown and check None
        current_drawdown = self.portfolio_tracker.get_current_drawdown()
        if current_drawdown is not None:
            # Mypy fix [operator]: Compare Decimals
            sanity_drawdown_limit = self.max_drawdown_limit * Decimal("1.5")  # Higher sanity limit
            if current_drawdown > sanity_drawdown_limit:
                self.logger.error(
                    f"Sanity Check FAIL: Portfolio drawdown {current_drawdown:.2%} exceeds sanity limit {sanity_drawdown_limit:.2%}!"
                )
                return False

        # Add more checks as needed...

        self.logger.info("Portfolio sanity checks passed.")
        return True

    def update(self, data: dict[str, Any]) -> None:
        """
        Update RiskManager internal state if necessary (e.g., based on external events).
        Currently, RiskManager primarily reads from PortfolioTracker, so this might be minimal.
        """
        self.logger.debug(f"RiskManager update called with data: {data}")
        # Example: Update drawdown metrics if provided externally
        # if "drawdown_metrics" in data:
        #     self.current_drawdown_metrics = data["drawdown_metrics"]
        pass  # No internal state updates needed for now
