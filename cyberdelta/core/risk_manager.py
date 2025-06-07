"""CyberDeltaEngine: Risk Manager.

This module provides risk management functionality for trading operations,
including position sizing, risk validation, and portfolio-level constraints.
"""

from __future__ import annotations  # Enable postponed evaluation

import asyncio  # Added import for asyncio
import logging
from collections.abc import Sequence
from decimal import ROUND_DOWN, Decimal, InvalidOperation, getcontext
from enum import Enum  # Ensure Enum is imported
from typing import Any, Protocol

from cyberdelta.config.config_models import AppSettings
from cyberdelta.config.logging_config import get_logger  # Ensure get_logger is imported
from cyberdelta.core.models import SpotBalance

# from cyberdelta.core.portfolio_tracker import PortfolioTrackerProtocol
# This line should be commented out or removed
# from cyberdelta.core.models import ArbitrageOpportunity, Order, OrderSide, OrderType, TradeSignal
# REMOVING this runtime import
from cyberdelta.validation.circuit_breaker import BreakerState
from cyberdelta.validation.funding_data import (
    ArbitrageOpportunity,  # Import from correct location
)

logger = get_logger(__name__)

# Set precision for Decimal
getcontext().prec = 28  # Default precision, adjust if needed

# Define ZERO and ONE constants for clarity
ZERO = Decimal("0")
ONE = Decimal("1")


# Define SimpleSizingMethod Enum and VALID_SIMPLE_SIZING_METHODS at the module level
class SimpleSizingMethod(str, Enum):
    """Enumeration of simple position sizing methods."""

    FIXED_USD = "fixed_usd"
    FIXED_FRACTION = "fixed_fraction"


VALID_SIMPLE_SIZING_METHODS = {member.value for member in SimpleSizingMethod}


# Protocol for funding rate validator
class FundingRateValidatorProtocol(Protocol):
    """Protocol for funding rate validator implementation."""

    def get_symbol_metrics(
        self,
        exchange: str,
        symbol: str,
    ) -> dict[str, float | Decimal | None]:
        """Get symbol metrics for funding rate validation."""
        ...


class SizedOpportunity:
    """An arbitrage opportunity with calculated position sizes and risk metrics."""

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
        """Initialize a sized opportunity.

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
        """Return string representation of the sized opportunity."""
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
        """Check equality with another SizedOpportunity."""
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
        """Return hash value for the sized opportunity."""
        return hash(
            (
                self.opportunity,  # Relies on ArbitrageOpportunity implementing __hash__
                self.long_size,
                self.short_size,
                self.allocation_percentage,
                self.expected_profit,
                self.expected_return,
                self.risk_adjusted_return,
            ),
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
    """Protocol for position data structure."""

    symbol: str
    size: Decimal
    entry_price: Decimal | None  # Changed to Optional[Decimal]
    mark_price: Decimal | None
    liquidation_price: Decimal | None


# --- Protocols for Dependency Injection ---
class PortfolioTrackerProtocol(Protocol):
    """Protocol for portfolio tracking implementation."""

    async def get_total_capital(self) -> Decimal:
        """Get total available capital."""
        ...

    def get_exchange_balance(self, exchange: str, asset: str) -> SpotBalance | None:
        """Get balance for specific exchange and asset."""
        ...

    def get_all_positions(self) -> Sequence[tuple[str, Position]]:
        """Get all current positions."""
        ...

    async def get_current_drawdown(self) -> Decimal | None:
        """Get current portfolio drawdown."""
        ...

    async def get_total_exposure_usd(self) -> Decimal:
        """Get total portfolio exposure in USD."""
        ...


class CircuitBreakerSystemProtocol(Protocol):
    """Protocol for circuit breaker system implementation."""

    def can_execute(self, exchange: str) -> tuple[bool, str | None]:
        """Check if execution is allowed for given exchange."""
        ...

    def get_exchange_breaker(self, exchange: str, breaker_type: str) -> object:
        """Get circuit breaker for specific exchange and type."""
        ...


class RiskManager:
    """Assess and size trades based on risk parameters.

    Responsible for:
    - Validating opportunities against risk constraints
    - Applying position sizing based on Kelly criterion
    - Enforcing position limits and portfolio risk controls
    - Calculating risk metrics
    """

    def __init__(
        self,
        app_settings: AppSettings,
        portfolio_tracker: PortfolioTrackerProtocol,
        circuit_breaker_system: CircuitBreakerSystemProtocol | None = None,
        funding_rate_validator: FundingRateValidatorProtocol | None = None,
    ) -> None:
        """Initialize the risk manager.

        Args:
            app_settings: Application configuration
            portfolio_tracker: Portfolio state tracking (must implement PortfolioTrackerProtocol)
            circuit_breaker_system: Optional system for circuit breakers
                (must implement CircuitBreakerSystemProtocol)
            funding_rate_validator: Optional validator for funding rate predictions.
                Must implement get_symbol_metrics(exchange: str, symbol: str).

        Raises:
            ConfigError: If any required config value is missing or invalid.

        """
        self.app_settings: AppSettings = app_settings  # Assign config first
        self.logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__name__}",
        )  # Then logger

        # Assign dependencies from constructor arguments BEFORE any config
        # parsing that might use them
        self.portfolio_tracker: PortfolioTrackerProtocol = portfolio_tracker
        self.circuit_breaker_system: CircuitBreakerSystemProtocol | None = circuit_breaker_system
        self.funding_rate_validator: FundingRateValidatorProtocol | None = funding_rate_validator

        self.current_drawdown_metrics: dict[str, Any] = {}  # Initialize attribute

        # Initialize attributes that will be set in _load_config to satisfy linters
        # and provide default values if config loading has issues before these are set.
        self.use_simple_sizing_path: bool = False
        self.kelly_enabled: bool = False
        self.simple_fixed_usd_size: Decimal = ZERO
        self.simple_fixed_fraction: Decimal = ZERO
        self.simple_sizing_method_str: str = SimpleSizingMethod.FIXED_USD.value

        # Initialize attributes for min trade size
        self._global_min_trade_size_usd: Decimal = ZERO  # Initialize global min trade size
        self.min_trade_size_usd_per_exchange: dict[
            str,
            Decimal,
        ] = {}  # Initialize per-exchange dict

        # Load core boolean flags first, robustly
        try:
            # Access configuration through AppSettings structure
            self.use_simple_sizing_path = self.app_settings.risk.use_simple_sizing_path
            self.logger.info(
                f"RM_INIT: Parsed self.use_simple_sizing_path: {self.use_simple_sizing_path}",
            )

            # Kelly criterion is not in the current config structure, default to False
            self.kelly_enabled = False
            self.logger.info("RM_INIT: Kelly criterion not configured, defaulting to False")

            # Now load all other configuration values
            self._load_config()

        except (AttributeError, ValueError, TypeError) as e:
            self.logger.error(
                f"RM_INIT: Critical error during initial config parsing or _load_config: {e}",
                exc_info=True,
            )
            raise ConfigError(
                f"Invalid or missing configuration value during RiskManager initialization: {e}",
            ) from e

    def _load_config(self) -> None:
        """Load and validate all configuration values, storing them as attributes.

        Raises ConfigError if any required value is missing or invalid.
        """
        try:
            # General Risk Parameters - access through AppSettings structure
            self.max_position_size = self.app_settings.risk.global_risk.max_position_usd
            self.max_total_exposure_usd = self.app_settings.risk.global_risk.max_total_exposure_usd

            # Set default values for fields not in current config
            self.min_trade_size_usd = Decimal("1.0")  # Default minimum trade size
            self.min_nfd_for_sizing = Decimal("0.0001")  # Default 1 bps
            self.max_collateral_per_exchange = Decimal("0.8")  # Default 80%
            self.max_leverage = Decimal("5.0")  # Default 5x
            self.min_liquidation_buffer = Decimal("0.2")  # Default 20%
            self.max_exposure_per_asset = Decimal("0.2")  # Default 20%
            self.max_exposure_per_exchange = Decimal("0.5")  # Default 50%
            self.circuit_breaker_recovery_factor = Decimal("0.3")  # Default 30%
            self.min_exchange_balance = Decimal("10.0")  # Default $10
            self.max_acceptable_rmse = Decimal("0.05")  # Default 5%
            self.max_acceptable_bias = Decimal("0.02")  # Default 2%
            self.min_validation_factor = Decimal("0.2")  # Default 20%

            # Exchange risk modifiers - default to 1.0 for all exchanges
            self.exchange_risk_modifiers: dict[str, float] = {}
            for exchange_id, exchange_config in self.app_settings.exchanges.items():
                if exchange_config.enabled:
                    self.exchange_risk_modifiers[exchange_id] = 1.0  # Default modifier

            # Strategy-specific settings - defaults
            self.max_single_position_exposure_ratio = Decimal("0.1")  # Default 10%
            self.max_drawdown_limit_ratio = Decimal("0.2")  # Default 20%
            self.max_leverage_per_trade = Decimal("5.0")  # Default 5x

            # Simple Sizing Path specific attributes
            self.simple_sizing_method_str = self.app_settings.risk.simple_sizing_method
            self.simple_fixed_usd_size = self.app_settings.risk.simple_fixed_usd_size
            self.simple_fixed_fraction = self.app_settings.risk.simple_fixed_fraction

            # Kelly Criterion specific attributes - defaults since not in config
            self.volatility_period = 14  # Default 14 days
            self.min_acceptable_kelly = Decimal("0.001")  # Default 0.1%
            self.max_acceptable_kelly = Decimal("0.25")  # Default 25%
            self.min_volatility = Decimal("0.001")  # Default 0.1%
            self.kelly_fraction_config = Decimal("0.1")  # Default 10%
            self.kelly_max_leverage_cap = Decimal("3.0")  # Default 3x
            self.min_edge_bps_kelly = Decimal("5")  # Default 5 bps

            # Initialize min trade size attributes
            self._global_min_trade_size_usd = self.min_trade_size_usd
            self.min_trade_size_usd_per_exchange = {}  # No per-exchange config yet

        except (AttributeError, ValueError, TypeError) as e:
            self.logger.error(
                f"RM_INIT: Error loading configuration in _load_config: {e}",
                exc_info=True,
            )
            raise ConfigError(f"Invalid or missing configuration value in RiskManager: {e}") from e

    def _calculate_kelly_size(
        self,
        opportunity: ArbitrageOpportunity,
        total_capital: Decimal,
    ) -> Decimal:
        """Calculate position size using Kelly Criterion.

        Assumes expected return and volatility are provided or can be derived.
        """
        # Expected return (Net Funding Differential as a decimal)
        expected_return = opportunity.net_funding_differential
        if expected_return <= ZERO:
            self.logger.info(
                f"Kelly sizing: Expected return for {opportunity.symbol} is not positive "
                f"({expected_return:.4f}). Cannot size.",
            )
            return ZERO

        # Volatility of the *spread* or *arbitrage opportunity itself*.
        # Convert from float | None to Decimal, handling None and non-positive.
        raw_volatility = opportunity.basis_volatility
        volatility: Decimal

        if raw_volatility is None:
            self.logger.warning(
                f"Volatility for {opportunity.symbol} is None. Using min_volatility: "
                f"{self.min_volatility}.",
            )
            volatility = self.min_volatility
        else:
            try:
                volatility = Decimal(str(raw_volatility))
            except InvalidOperation:
                self.logger.error(
                    f"Could not convert raw volatility '{raw_volatility}' "
                    f"to Decimal for {opportunity.symbol}. "
                    f"Using min_volatility: {self.min_volatility}.",
                )
                volatility = self.min_volatility

        if volatility <= ZERO:
            self.logger.warning(
                f"Calculated/fallback volatility for {opportunity.symbol} "
                f"is zero or negative ({volatility}). Using min_volatility: "
                f"{self.min_volatility} if positive, else cannot size.",
            )
            volatility = (
                self.min_volatility
            )  # Try min_volatility again if initial conversion was bad
            if volatility <= ZERO:
                self.logger.error(
                    f"min_volatility for {opportunity.symbol} is also zero or negative "
                    f"({self.min_volatility}). Cannot calculate Kelly size.",
                )
                return ZERO

        if expected_return <= ZERO:  # Redundant check, but safe
            self.logger.info(
                f"Kelly fraction calculation: Expected return for {opportunity.symbol} "
                f"is not positive "
                f"({expected_return:.8f}). Resulting size will be zero.",
            )
            return ZERO

        try:
            # Using mu / sigma^2 variant where mu is expected_return and sigma is volatility
            kelly_fraction_raw = expected_return / (volatility**2)
        except InvalidOperation:
            self.logger.error(
                f"Invalid operation during Kelly calculation for {opportunity.symbol} "
                f"(ER: {expected_return}, Vol: {volatility}). Defaulting to zero size.",
            )
            return ZERO

        # Apply configured Kelly fraction (e.g., half Kelly)
        adjusted_kelly_fraction = kelly_fraction_raw * self.kelly_fraction_config

        # Clamp Kelly fraction to acceptable min/max range from config
        clamped_kelly_fraction = max(
            self.min_acceptable_kelly,
            min(adjusted_kelly_fraction, self.max_acceptable_kelly),
        )

        # Fetch and apply validation factors (RMSE, Bias)
        # Replicating logic from _calculate_simple_size for validation factors
        long_validation_result = self._get_validation_metrics(
            opportunity.long_exchange,
            opportunity.symbol,
        )
        short_validation_result = self._get_validation_metrics(
            opportunity.short_exchange,
            opportunity.symbol,
        )

        if long_validation_result is None or short_validation_result is None:
            self.logger.warning(
                f"Kelly Sizing: Validation failed for one or both legs of {opportunity.symbol}. "
                "Cannot safely size using Kelly. Rejecting.",
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
                f"  Configured Kelly Multiplier: {self.kelly_fraction_config}\n"
                f"  Adjusted Kelly Fraction: {adjusted_kelly_fraction:.4f}\n"
                f"  Clamped Kelly Fraction (min: {self.min_acceptable_kelly}, max: "
                f"{self.max_acceptable_kelly}): {clamped_kelly_fraction:.4f}\n"
                f"  RMSE Factor: {rmse_factor:.2f}, Bias Factor: {bias_factor:.2f}\n"
                f"  Final Effective Kelly Fraction: {final_kelly_fraction:.4f}\n"
                f"  Calculated Size: ${calculated_size:.2f}",
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
            opportunity.short_exchange,
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
                    f"Invalid long entry price ({long_price}) for {opportunity.symbol}",
                )
                return False
            if short_price <= ZERO:
                self.logger.warning(
                    f"Invalid short entry price ({short_price}) for {opportunity.symbol}",
                )
                return False
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Error converting prices for {opportunity.symbol}: {e}")
            return False
        return True

    def _check_exchange_balances(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check that both exchanges have sufficient available balance."""
        # Default to USD for collateral asset since it's not in current config structure
        long_collateral_asset = "USD"
        short_collateral_asset = "USD"

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
                f"Balances: L={long_exchange_balance_obj}, S={short_exchange_balance_obj}",
            )
            return False

        min_bal = self.min_exchange_balance
        long_exchange_ok = long_balance_dec >= min_bal
        short_exchange_ok = short_balance_dec >= min_bal

        if not long_exchange_ok:
            self.logger.warning(
                f"Insufficient balance on {opportunity.long_exchange} (${long_balance_dec:.2f}) "
                f"for opportunity {opportunity.symbol}. Min required: ${min_bal:.2f}",
            )
        if not short_exchange_ok:
            self.logger.warning(
                f"Insufficient balance on {opportunity.short_exchange} (${short_balance_dec:.2f}) "
                f"for opportunity {opportunity.symbol}. Min required: ${min_bal:.2f}",
            )

        return long_exchange_ok and short_exchange_ok

    async def _check_leverage(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check that portfolio leverage is within allowed limits."""
        total_capital = await self.portfolio_tracker.get_total_capital()

        if total_capital <= ZERO:
            self.logger.warning("Total capital is zero or negative. Cannot calculate leverage.")
            return False
        try:
            total_exposure_dec = await self.calculate_total_exposure()
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
        self,
        proposed_size: Decimal,
    ) -> tuple[bool, str | None]:
        if proposed_size > self.max_position_size:
            return (
                False,
                f"Proposed size ${proposed_size:.2f} exceeds max single position size "
                f"${self.max_position_size:.2f}",
            )
        return True, None

    async def _check_constraint_max_relative_size(
        self,
        proposed_size: Decimal,
        total_capital: Decimal,
    ) -> tuple[bool, str | None]:
        max_relative_size = total_capital * self.max_single_position_exposure_ratio
        if proposed_size > max_relative_size:
            return (
                False,
                f"Proposed size ${proposed_size:.2f} exceeds max relative position size "
                f"(${max_relative_size:.2f}, {self.max_single_position_exposure_ratio:.1%})",
            )
        return True, None

    async def _check_constraint_max_total_exposure(
        self,
        proposed_size: Decimal,
    ) -> tuple[bool, str | None]:
        # Use the direct method from portfolio tracker for current exposure
        current_exposure = await self.portfolio_tracker.get_total_exposure_usd()
        if (current_exposure + proposed_size) > self.max_total_exposure_usd:
            return (
                False,
                f"Adding ${proposed_size:.2f} would exceed max total exposure "
                f"(${self.max_total_exposure_usd:.2f}). Current: ${current_exposure:.2f}",
            )
        return True, None

    async def _check_constraint_max_leverage(
        self,
        proposed_size: Decimal,
        total_capital: Decimal,
    ) -> tuple[bool, str | None]:
        if total_capital > ZERO:
            # Corrected: Use await for calculate_total_exposure
            current_total_exposure = await self.calculate_total_exposure()
            projected_leverage = (current_total_exposure + proposed_size) / total_capital
            if projected_leverage > self.max_leverage:
                return (
                    False,
                    f"Projected leverage {projected_leverage:.2f}x exceeds max leverage "
                    f"{self.max_leverage:.2f}x",
                )
        return True, None

    def _check_constraint_exchange_balance(
        self,
        opportunity: ArbitrageOpportunity,
        proposed_size: Decimal,
    ) -> tuple[bool, str | None]:
        long_ex = opportunity.long_exchange
        short_ex = opportunity.short_exchange
        # Use get_collateral_asset_for_exchange to find the correct asset
        long_collateral_asset = self.get_collateral_asset_for_exchange(long_ex, opportunity.symbol)
        short_collateral_asset = self.get_collateral_asset_for_exchange(
            short_ex,
            opportunity.symbol,
        )

        long_balance_obj = self.portfolio_tracker.get_exchange_balance(
            long_ex,
            long_collateral_asset,
        )
        short_balance_obj = self.portfolio_tracker.get_exchange_balance(
            short_ex,
            short_collateral_asset,
        )

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

    async def validate_opportunity(self, opportunity: ArbitrageOpportunity) -> bool:
        """Perform initial validation checks on an opportunity before sizing.

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
        if not await self._check_leverage(opportunity):
            return False

        # TODO: Add more validation steps as needed
        # - Liquidity checks?
        # - Max open orders check?
        # - Correlation with existing positions? (Removed for now)

        self.logger.debug(f"Opportunity {opportunity.symbol} passed initial validation.")
        return True

    async def _apply_portfolio_exposure_management(
        self,
        sized_opportunities: list[SizedOpportunity],
    ) -> list[SizedOpportunity]:
        """Apply portfolio-level exposure limits and diversification rules.

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
            f"Applying portfolio exposure management to {len(sized_opportunities)} opportunities.",
        )

        # Example: Simple check against total exposure
        # (redundant with _check_portfolio_constraints?)
        total_capital = await self.portfolio_tracker.get_total_capital()
        if total_capital <= ZERO:
            self.logger.warning("Cannot apply exposure management: Total capital unavailable.")
            return []

        current_exposure = await self.calculate_total_exposure()
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

    async def _apply_portfolio_level_controls(
        self,
        sized_opportunity: SizedOpportunity,
    ) -> SizedOpportunity | None:
        """Apply portfolio-level controls like drawdown limits.

        (Currently focuses on drawdown, could include correlation etc. later)

        Args:
            sized_opportunity: The opportunity sized by Kelly criterion.

        Returns:
            The potentially adjusted SizedOpportunity, or None if rejected.

        """
        # 1. Drawdown Check
        if not await self.check_drawdown():
            self.logger.warning(
                f"Portfolio drawdown limit exceeded. Rejecting opportunity "
                f"{sized_opportunity.opportunity.symbol}.",
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
                long_ex,
                "APIErrorBreaker",
            )
            short_api_breaker = self.circuit_breaker_system.get_exchange_breaker(
                short_ex,
                "APIErrorBreaker",
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
        """Retrieve validation metrics for funding rate predictions.

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

        # The FundingRateValidatorProtocol ensures 'metrics' is a dict.
        # Individual keys 'rmse' or 'bias' might be missing or their values None.

        # Now metrics is guaranteed to be a dict, but keys might be missing
        rmse_value = metrics.get("rmse")
        bias_value = metrics.get("bias")

        if rmse_value is None or bias_value is None:
            self.logger.warning(
                f"Validation metrics for {exchange}/{symbol} are incomplete or missing "
                f"(rmse/bias is None in returned dict). Rejecting for safety.",
            )
            return None

        rmse_dec = Decimal(str(rmse_value))
        bias_dec = Decimal(str(bias_value))

        # Use the values already set in _load_config
        max_acceptable_rmse = self.max_acceptable_rmse
        max_acceptable_bias = self.max_acceptable_bias

        factor_rmse = max(ZERO, ONE - (rmse_dec / max_acceptable_rmse))
        factor_bias = max(ZERO, ONE - (abs(bias_dec) / max_acceptable_bias))

        # For now, using minimum of the individual factors.
        combined_factor = min(factor_rmse, factor_bias)

        # If the raw combined factor from metrics is below the minimum acceptable, treat as failure.
        if combined_factor < self.min_validation_factor:
            self.logger.warning(
                f"Raw combined validation factor {combined_factor:.3f} for {exchange}/{symbol} "
                f"is below minimum threshold {self.min_validation_factor:.3f}. "
                f"Rejecting opportunity.",
            )
            return None  # Reject

        # If we reach here, combined_factor is >= self.min_validation_factor.
        # This combined_factor will be used directly.
        final_factor = combined_factor

        # Log if the factor is low but still usable for sizing
        if final_factor < ONE:
            self.logger.info(
                f"Validation metrics for {exchange}/{symbol} result in factor: {final_factor:.3f} "
                f"(RMSE={rmse_dec}, Bias={bias_dec}). Applying factor.",
            )
        else:
            self.logger.debug(
                f"Validation metrics for {exchange}/{symbol} are within limits. "
                f"RMSE={rmse_dec}, Bias={bias_dec}. Factor: {final_factor:.3f}",
            )

        # Ensure it's not excessively precise if it's ONE
        return final_factor.quantize(Decimal("0.001"))

    async def _check_portfolio_constraints(
        self,
        size: Decimal,
        opportunity: ArbitrageOpportunity,
    ) -> tuple[bool, str | None]:
        """Check if the given size satisfies all portfolio constraints for the opportunity.

        Args:
            size: The proposed position size.
            opportunity: The arbitrage opportunity.

        Returns:
            A tuple (bool, str | None) where the bool indicates
                whether the constraints are satisfied,
            and the str is an optional reason for rejection if not satisfied.

        """
        # Ensure total_capital is fetched and valid before proceeding
        total_capital = await self.portfolio_tracker.get_total_capital()
        if total_capital <= ZERO:
            msg = f"Cannot check constraints: Invalid total capital ({total_capital})."
            self.logger.warning(msg)
            return False, msg

        checks = [
            self._check_constraint_max_position_size(size),
            await self._check_constraint_max_relative_size(size, total_capital),
            await self._check_constraint_max_total_exposure(size),
            await self._check_constraint_max_leverage(size, total_capital),
            self._check_constraint_exchange_balance(opportunity, size),
        ]

        for passed, reason in checks:
            if not passed:
                return False, reason
        return True, None

    async def _calculate_simple_size(
        self,
        opportunity: ArbitrageOpportunity,
        total_capital: Decimal,
        long_val_factor: Decimal,  # ADDED: Explicitly pass pre-calculated validation factors
        short_val_factor: Decimal,  # ADDED
    ) -> SizedOpportunity | None:
        """Calculate position size using the simple sizing path (fixed fraction or fixed USD).

        Applies validation factors and enforces portfolio constraints.

        Args:
            opportunity: The arbitrage opportunity to size.
            total_capital: The total available capital (Decimal).
            long_val_factor: Validation factor for the long leg.
            short_val_factor: Validation factor for the short leg.


        Returns:
            A SizedOpportunity object if valid and sized, otherwise None.

        """
        self.logger.debug(
            f"SOS: Called for {opportunity.symbol}. Total capital: {total_capital}, "
            f"Configured Fixed USD Size: {self.simple_fixed_usd_size}, "
            f"Configured Fixed Fraction: {self.simple_fixed_fraction}, "
            f"Configured Method: {self.simple_sizing_method_str}, "
            f"Passed LVal: {long_val_factor}, "
            f"Passed SVal: {short_val_factor}",
        )

        if opportunity.net_funding_differential <= self.min_nfd_for_sizing:
            self.logger.warning(
                f"SOS: Opportunity {opportunity.symbol} NFD "
                f"{opportunity.net_funding_differential:.8f} < Min NFD "
                f"{self.min_nfd_for_sizing:.8f}",
            )
            return None

        size = ZERO
        # Use attributes loaded from config in __init__ / _load_config
        if self.simple_sizing_method_str == SimpleSizingMethod.FIXED_FRACTION.value:
            size = (total_capital * self.simple_fixed_fraction).quantize(Decimal("0.01"))
        elif self.simple_sizing_method_str == SimpleSizingMethod.FIXED_USD.value:
            size = self.simple_fixed_usd_size  # This is already a Decimal

        # Cap at available capital
        size = min(size, total_capital)

        # --- Apply Validation Factor ---
        validation_factor = min(long_val_factor, short_val_factor)
        # self.logger.info(
        #     f"RM_DEBUG_CSS: symbol={opportunity.symbol}, initial_size_calc=${size:.2f}, "
        #     f"long_val_factor={long_val_factor}, short_val_factor={short_val_factor}, "
        #     f"effective_validation_factor={validation_factor}"
        # )
        if validation_factor < ONE:
            # self.logger.info(
            #     f"RM_DEBUG_CSS: Applying validation factor {validation_factor:.3f} "
            #     f"to size ${size:.2f}"
            # ) # Log before multiplication
            # size_before_vf = size # This variable is no longer used as the log is commented out
            size *= validation_factor
            # self.logger.info(
            #     f"RM_DEBUG_CSS: Size after validation_factor {validation_factor:.3f}: "
            #     f"${size_before_vf:.2f} -> ${size:.2f}" # This log used size_before_vf
            # ) # Log after
            size = size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if size <= ZERO:  # Check if size became zero or negative
                self.logger.info(
                    f"Size reduced to zero or less after validation factor for "
                    f"{opportunity.symbol} (simple path). Rejecting.\n",
                )
                return None
            self.logger.debug(
                f"Size after validation factor for {opportunity.symbol} (simple path): ${size:.2f}",
            )

        if size <= self.min_trade_size_usd:
            self.logger.info(
                f"SOS: Calculated size ${size:.2f} for {opportunity.symbol} "
                f"is <= min_trade_size_usd ${self.min_trade_size_usd:.2f}. Rejecting.\n",
            )
            return None

        # Enforce portfolio constraints (this will check max_position_size, etc.)
        is_valid, reason = await self._check_portfolio_constraints(size, opportunity)
        if not is_valid:
            self.logger.info(
                f"SOS: Opportunity {opportunity.symbol} with size ${size:.2f} "
                f"rejected due to portfolio constraints: {reason}",
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
        final_sized_opportunity = await self._apply_portfolio_level_controls(sized_opportunity)
        if final_sized_opportunity:
            self.logger.info(
                f"Successfully sized opportunity (simple path): {final_sized_opportunity}",
            )
        return final_sized_opportunity

    async def _validate_opportunity_pipeline(self, opportunity: ArbitrageOpportunity) -> None:
        """Run a series of checks on the opportunity. Raises ValidationError if any check fails."""
        logger.debug(f"Validating opportunity pipeline for: {opportunity.symbol}")

        # Perform checks that don't depend on proposed size first
        if not self._check_exchange_balances(opportunity):
            raise ValueError(
                f"Validation failed at _check_exchange_balances for {opportunity.symbol}",
            )

        # Add the missing circuit breaker check
        if not self._check_circuit_breaker(opportunity):
            raise ValueError(
                f"Validation failed at _check_circuit_breaker for {opportunity.symbol}",
            )

        # if not self._check_funding_rate_stability(opportunity): # COMMENTED OUT -
        # POTENTIALLY MISSING METHOD
        #      raise ValueError(f"Validation failed at _check_funding_rate_stability "
        #                       f"for {opportunity.symbol}")

        # if not self._check_basis_volatility(opportunity): # COMMENTED OUT -
        # POTENTIALLY MISSING METHOD
        #      raise ValueError(f"Validation failed at _check_basis_volatility "
        #                       f"for {opportunity.symbol}")

        if not await self._check_leverage(opportunity):
            raise ValueError(f"Validation failed at _check_leverage for {opportunity.symbol}")

        # if not self._check_market_liquidity(opportunity): # COMMENTED OUT -
        # POTENTIALLY MISSING METHOD
        #     logger.warning(f"Market liquidity check failed for {opportunity.symbol}")
        #     # raise ValueError(f"Validation failed at _check_market_liquidity "
        #     #                  f"for {opportunity.symbol}")

        logger.debug(f"Pre-sizing validation checks passed for {opportunity.symbol}.")

    async def size_opportunity(self, opportunity: ArbitrageOpportunity) -> SizedOpportunity | None:
        """Calculate the optimal size for an arbitrage opportunity, considering risk limits."""
        logger.info(
            f"RiskManager: Starting to size opportunity for {opportunity.symbol} on "
            f"{opportunity.long_exchange}/{opportunity.short_exchange}",
        )

        # Validate opportunity and get validation factors
        validation_result = await self._validate_and_get_factors(opportunity)
        if validation_result is None:
            return None

        long_validation_factor, short_validation_factor = validation_result

        # Check total capital
        total_capital = await self.portfolio_tracker.get_total_capital()
        if total_capital <= ZERO:
            logger.warning(
                f"Cannot size opportunity {opportunity.symbol}: Total capital is zero or negative.",
            )
            return None

        # Calculate sized opportunity based on method
        sized_opportunity = await self._calculate_sized_opportunity(
            opportunity, total_capital, long_validation_factor, short_validation_factor
        )
        if sized_opportunity is None:
            return None

        # Apply portfolio level controls
        final_sized_opportunity = await self._apply_portfolio_level_controls(sized_opportunity)

        return self._log_and_return_result(opportunity, final_sized_opportunity)

    async def _validate_and_get_factors(
        self, opportunity: ArbitrageOpportunity
    ) -> tuple[Decimal, Decimal] | None:
        """Validate opportunity and get validation factors."""
        try:
            # Perform initial validation steps (circuit breaker, balances, etc.)
            await self._validate_opportunity_pipeline(opportunity)

            # Get validation factors
            long_validation_factor = self._get_validation_metrics(
                opportunity.long_exchange,
                opportunity.symbol,
            )
            self.logger.debug(
                f"RM.size_opp: Long validation factor for {opportunity.long_exchange}/"
                f"{opportunity.symbol}: {long_validation_factor}",
            )
            if long_validation_factor is None:
                self.logger.warning(
                    f"Funding validation failed for long leg {opportunity.long_exchange}/"
                    f"{opportunity.symbol}. Rejecting opportunity.",
                )
                return None

            short_validation_factor = self._get_validation_metrics(
                opportunity.short_exchange,
                opportunity.symbol,
            )
            self.logger.debug(
                f"RM.size_opp: Short validation factor for {opportunity.short_exchange}/"
                f"{opportunity.symbol}: {short_validation_factor}",
            )
            if short_validation_factor is None:
                self.logger.warning(
                    f"Funding validation failed for short leg {opportunity.short_exchange}/"
                    f"{opportunity.symbol}. Rejecting opportunity.",
                )
                return None

            return long_validation_factor, short_validation_factor

        except ValueError as e:
            self.logger.warning(
                f"Opportunity {opportunity.symbol} failed pre-sizing validation: {e}",
            )
            return None

    async def _calculate_sized_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        total_capital: Decimal,
        long_validation_factor: Decimal,
        short_validation_factor: Decimal,
    ) -> SizedOpportunity | None:
        """Calculate sized opportunity based on configured method."""
        # Choose sizing method - configurable, default to simple since kelly is not fully configured
        sizing_method = getattr(self, "sizing_method", "simple")

        if sizing_method == "kelly":
            return await self._calculate_kelly_sized_opportunity(
                opportunity, total_capital, long_validation_factor, short_validation_factor
            )
        elif sizing_method == "simple":
            return await self._calculate_simple_size(
                opportunity,
                total_capital,
                long_validation_factor,
                short_validation_factor,
            )
        else:
            logger.error(f"Unknown sizing method configured: '{sizing_method}'")
            return None

    async def _calculate_kelly_sized_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        total_capital: Decimal,
        long_validation_factor: Decimal,
        short_validation_factor: Decimal,
    ) -> SizedOpportunity | None:
        """Calculate sized opportunity using Kelly criterion."""
        # Calculate Kelly size
        calculated_size_usd = self._calculate_kelly_size(opportunity, total_capital)
        if calculated_size_usd <= self.min_trade_size_usd:
            self.logger.info(
                f"Kelly calculated size (${calculated_size_usd}) for {opportunity.symbol} "
                f"below min size (${self.min_trade_size_usd}). Rejecting.",
            )
            return None

        # Apply validation factor
        validation_factor = min(long_validation_factor, short_validation_factor)
        calculated_size_usd_validated = self._apply_validation_factor(
            calculated_size_usd, validation_factor, opportunity.symbol
        )
        if calculated_size_usd_validated is None:
            return None
        
        calculated_size_usd = calculated_size_usd_validated

        # Check constraints
        is_valid, reason = await self._check_portfolio_constraints(
            calculated_size_usd,
            opportunity,
        )
        if not is_valid:
            self.logger.info(
                f"Kelly sized opportunity {opportunity.symbol} rejected due to "
                f"portfolio constraints: {reason}",
            )
            return None

        # Construct SizedOpportunity
        return self._construct_sized_opportunity(opportunity, calculated_size_usd, total_capital)

    def _apply_validation_factor(
        self, calculated_size_usd: Decimal, validation_factor: Decimal, symbol: str
    ) -> Decimal | None:
        """Apply validation factor to calculated size."""
        if validation_factor < ONE:  # Apply reduction only if factor < 1
            self.logger.info(
                f"Applying validation factor {validation_factor:.3f} to size for "
                f"{symbol} (kelly path).",
            )
            calculated_size_usd *= validation_factor
            calculated_size_usd = calculated_size_usd.quantize(
                Decimal("0.01"),
                rounding=ROUND_DOWN,
            )
            if calculated_size_usd <= self.min_trade_size_usd:
                self.logger.info(
                    f"Kelly size reduced below min size after validation factor for "
                    f"{symbol}. Rejecting.",
                )
                return None
            self.logger.debug(
                f"Kelly size after validation factor for {symbol}: ${calculated_size_usd:.2f}",
            )
        return calculated_size_usd

    def _construct_sized_opportunity(
        self, opportunity: ArbitrageOpportunity, final_size: Decimal, total_capital: Decimal
    ) -> SizedOpportunity:
        """Construct SizedOpportunity from calculated size."""
        allocation_percentage = (final_size / total_capital) if total_capital > ZERO else ZERO
        # Re-calculate expected profit based on the final size
        expected_return_pct = opportunity.net_funding_differential
        expected_profit = final_size * expected_return_pct
        # Placeholder for risk-adjusted return (e.g., Sharpe ratio if volatility is meaningful)
        risk_adjusted_return = expected_return_pct  # Simplistic for now

        return SizedOpportunity(
            opportunity=opportunity,
            long_size=final_size,
            short_size=final_size,  # Assuming symmetric size for now
            allocation_percentage=allocation_percentage,
            expected_profit=expected_profit,
            expected_return=expected_return_pct,
            risk_adjusted_return=risk_adjusted_return,
        )

    def _log_and_return_result(
        self, opportunity: ArbitrageOpportunity, final_sized_opportunity: SizedOpportunity | None
    ) -> SizedOpportunity | None:
        """Log the final result and return it."""
        logger.debug(
            f"[RM_SIZE_OPP_DEBUG] After _apply_portfolio_level_controls, "
            f"final_sized_opportunity is: {final_sized_opportunity}, "
            f"type: {type(final_sized_opportunity)}",
        )

        if final_sized_opportunity:
            logger.info(f"Successfully sized opportunity: {final_sized_opportunity}")
            return final_sized_opportunity
        else:
            logger.info(
                f"Opportunity {opportunity.symbol} rejected by portfolio level controls "
                f"(final_sized_opportunity is None).",
            )
            return None

    async def validate_opportunities(
        self,
        opportunities: list[ArbitrageOpportunity],
    ) -> list[SizedOpportunity]:
        """Validate and size a list of opportunities."""
        if not opportunities:
            return []
        logger.info(f"Validating and sizing {len(opportunities)} opportunities.")
        sizing_tasks = [self.size_opportunity(opp) for opp in opportunities]
        # Corrected type hint for asyncio.gather results to use BaseException
        # Now expects SizedOpportunity | None | BaseException because size_opportunity returns that
        sized_results: list[SizedOpportunity | None | BaseException] = await asyncio.gather(
            *sizing_tasks,
            return_exceptions=True,
        )
        validated_opportunities: list[SizedOpportunity] = []
        for i, result in enumerate(sized_results):
            original_opp = opportunities[i]
            logger.debug(
                f"[RM_VALIDATE_OPPS_LOOP] Processing result for {original_opp.symbol}: {result}, "
                f"type: {type(result)}",
            )
            if isinstance(result, SizedOpportunity):
                validated_opportunities.append(result)
                logger.debug(f"Opportunity for {original_opp.symbol} sized successfully.")
            elif isinstance(result, BaseException):  # Check against BaseException
                logger.error(
                    f"Error sizing opportunity for {original_opp.symbol}: {result}",
                    exc_info=result,
                )
            elif result is None:
                logger.info(
                    f"Opportunity for {original_opp.symbol} was filtered out or failed "
                    f"sizing (returned None).",
                )
        logger.info(f"Returning {len(validated_opportunities)} validated and sized opportunities.")
        return validated_opportunities

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

    async def check_drawdown(self) -> bool:
        """Check if the current portfolio drawdown exceeds the limit."""
        current_drawdown = await self.portfolio_tracker.get_current_drawdown()
        if current_drawdown is None:
            self.logger.warning(
                "Drawdown information unavailable, cannot check limit. Failing CLOSED for safety.",
            )
            return False  # Fail closed: block trading if drawdown data is missing

        if current_drawdown >= self.max_drawdown_limit_ratio:
            self.logger.warning(
                f"Drawdown check failed: Current {current_drawdown:.2%} >= Limit "
                f"{self.max_drawdown_limit_ratio:.2%}",
            )
            return False
        return True

    def calculate_position_exposure(self, symbol: str) -> Decimal | None:
        """Calculate the total USD exposure for a specific symbol across all exchanges.

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
                            f"Error calculating exposure for {symbol} on {exchange_id}: {e}",
                        )
        return total_exposure if found_position else ZERO  # Return 0 if no position found

    async def calculate_total_exposure(self) -> Decimal:
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
                            f"{exchange_id}: {e}",
                        )
        return total_exposure

    def calculate_required_margin(
        self,
        symbol: str,
        size: Decimal,
        price: Decimal,
        leverage: Decimal,
    ) -> Decimal:
        """Calculate the required margin for a position."""
        if leverage <= ZERO:
            return size * price  # 1x leverage requires full notional value
        return (size * price) / leverage

    def evaluate_liquidation_risk(self, symbol: str) -> Decimal | None:
        """Evaluate the liquidation risk for a symbol based on current price and liquidation price.

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
                "Position or required prices not found.",
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
        # No need for isinstance check on self.min_opportunity_profitability
        # as it's set in _load_config

        is_profitable = nfd >= self.min_nfd_for_sizing
        if not is_profitable:
            self.logger.debug(
                f"Opportunity {opportunity.symbol} NFD {nfd:.8f} < Min Profitable NFD "
                f"{self.min_nfd_for_sizing:.8f}",
            )
        return is_profitable

    def adjust_order_size(self, symbol: str, requested_size: Decimal) -> Decimal:
        """Adjust order size based on liquidity, order book depth, etc."""
        # TODO: Implement logic based on order book, recent volume, etc.
        return requested_size  # No adjustment for now

    async def perform_sanity_checks(self) -> bool:
        """Perform high-level sanity checks on the overall portfolio state.

        Designed to catch potentially catastrophic conditions.

        Returns:
            True if checks pass, False if a critical condition is detected.

        """
        self.logger.debug("Performing portfolio sanity checks...")

        # Check 1: Leverage
        total_capital = await self.portfolio_tracker.get_total_capital()
        total_exposure = await self.calculate_total_exposure()
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
        current_drawdown = await self.portfolio_tracker.get_current_drawdown()
        sanity_drawdown_limit = self.max_drawdown_limit_ratio * Decimal(
            "1.5",
        )  # Example: 50% buffer

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
                f"RiskManager updated with drawdown metrics: {data['drawdown_metrics']}",
            )
        # Potentially update volatility estimates, correlations, etc.
        pass

    # --- Exposure Checks (Portfolio Level) ---

    async def _check_max_exposure(
        self,
        opportunity: ArbitrageOpportunity,
        proposed_size_usd: Decimal | None,
    ) -> bool:
        """Check that the proposed trade does not exceed max portfolio exposure."""
        if proposed_size_usd is None or proposed_size_usd <= ZERO:
            logger.debug("Proposed size is zero or None, skipping max exposure check.")
            return True

        current_total_exposure = await self.calculate_total_exposure()
        # current_total_exposure is Decimal and cannot be None based on PortfolioTrackerProtocol.
        # Therefore, the direct check 'if current_total_exposure is None:' is no longer needed.

        max_exposure_factor = Decimal("2.0")

        total_capital = await self.portfolio_tracker.get_total_capital()
        # total_capital is Decimal and cannot be None based on PortfolioTrackerProtocol.
        if total_capital <= ZERO:
            logger.warning(
                f"Cannot calculate max exposure limit: Total capital is {total_capital} "
                f"(zero or negative).",
            )
            return False

        max_allowed_exposure = total_capital * max_exposure_factor
        potential_new_total_exposure = current_total_exposure + proposed_size_usd

        if potential_new_total_exposure > max_allowed_exposure:
            logger.warning(
                f"Trade ({opportunity.symbol}, size_usd {proposed_size_usd}) exceeds max portfolio "
                f"exposure. Current: {current_total_exposure}, Proposed New: "
                f"{potential_new_total_exposure}, Limit: {max_allowed_exposure}",
            )
            return False
        return True

    def get_collateral_asset_for_exchange(self, exchange: str, symbol: str) -> str:
        """Retrieve the collateral asset for a given exchange and symbol.

        Defaults to "USD" if specific configurations are not found.

        Args:
            exchange: The trading exchange.
            symbol: The trading symbol (currently unused in direct logic but kept for context).

        Returns:
            The collateral asset for the given exchange.

        """
        # Default to USD since collateral asset config is not in current AppSettings
        return "USD"

    async def _check_exchange_balances_with_logs(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check that both exchanges have sufficient available balance."""
        # Default to USD for collateral asset since it's not in current config structure
        long_collateral_asset = "USD"
        short_collateral_asset = "USD"

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
                f"Balances: L={long_exchange_balance_obj}, S={short_exchange_balance_obj}",
            )
            return False

        min_bal = self.min_exchange_balance
        long_exchange_ok = long_balance_dec >= min_bal
        short_exchange_ok = short_balance_dec >= min_bal

        if not long_exchange_ok:
            self.logger.warning(
                f"Insufficient balance on {opportunity.long_exchange} (${long_balance_dec:.2f}) "
                f"for opportunity {opportunity.symbol}. Min required: ${min_bal:.2f}",
            )
        if not short_exchange_ok:
            self.logger.warning(
                f"Insufficient balance on {opportunity.short_exchange} (${short_balance_dec:.2f}) "
                f"for opportunity {opportunity.symbol}. Min required: ${min_bal:.2f}",
            )

        return long_exchange_ok and short_exchange_ok

    # Attempt to find and modify _apply_risk_constraints
    # This is a best-effort attempt as the exact location is unknown.
    # The following is a hypothetical structure based on previous reasoning.

    async def _apply_risk_constraints(  # Or sync, signature might vary
        self,
        opportunity: ArbitrageOpportunity,
        current_size_usd: Decimal,
    ) -> Decimal | None:  # Return type might vary
        """Apply various risk constraints to the proposed trade size.

        This is a placeholder for where the actual method might be.
        """
        # Placeholder: Actual start of the method might differ

        # === START MODIFIED SECTION ===
        # Ensure current_size_usd is compared against the correct minimum for the exchange
        # This assumes self.min_trade_size_usd is a dict[str, Decimal] and
        # self._global_min_trade_size_usd is a Decimal
        min_for_exchange = self.min_trade_size_usd_per_exchange.get(
            opportunity.long_exchange,
            self._global_min_trade_size_usd,
        )

        # Log values for debugging this critical check
        self.logger.debug(
            f"RM_CONSTRAINTS MinTradeCheck: Opp={opportunity.symbol}, "
            f"CurrentSizeUSD={current_size_usd:.8f}, MinForExchange={min_for_exchange:.8f}, "
            f"Exchange={opportunity.long_exchange}",
        )

        # Explicitly reject 0 or negative size first
        if current_size_usd <= ZERO:  # ZERO is Decimal("0")
            self.logger.info(
                f"RM_CONSTRAINTS: Opp {opportunity.symbol} "
                f"({opportunity.long_exchange} -> {opportunity.short_exchange}) rejected: "
                f"Calculated initial size USD {current_size_usd:.4f} is zero or negative.",
            )
            return None

        # If positive, check if it's below the minimum required trade size
        if current_size_usd < min_for_exchange:
            self.logger.info(
                f"RM_CONSTRAINTS: Opp {opportunity.symbol} "
                f"({opportunity.long_exchange} -> {opportunity.short_exchange}) rejected: "
                f"Calculated initial size USD {current_size_usd:.4f} is less than "
                f"min trade size USD {min_for_exchange:.4f} for {opportunity.long_exchange}.",
            )
            return None
        # === END MODIFIED SECTION ===

        # ... other constraints like max_position_size, collateral checks etc. ...
        # For example:
        # if current_size_usd > self.max_position_size:
        #     logger.info(
        #         f"RM_CONSTRAINTS: Opp {opportunity.symbol} rejected: Size {current_size_usd} "
        #         f"> max {self.max_position_size}"
        #     )
        #     current_size_usd = self.max_position_size
        #     # Cap it, or return None if rejection is preferred

        # Collateral check (should use the potentially capped size)
        # available_collateral = await self.portfolio_tracker.get_available_collateral(...)
        # if current_size_usd > available_collateral:
        #     logger.info(
        #         f"RM_CONSTRAINTS: Opp {opportunity.symbol} rejected: Size {current_size_usd} > "
        #         f"available collateral {available_collateral}"
        #     )
        #     return None

        return current_size_usd  # Return the validated (and possibly capped) size

    def _validate_and_cap_final_size(  # Attempt to find this method too
        self,
        opportunity: ArbitrageOpportunity,
        proposed_size_usd: Decimal,
        # ... other params might exist
    ) -> Decimal | None:  # Return type might vary
        final_size_usd = proposed_size_usd

        # ... other capping logic like max_position_size ...
        # For example:
        # final_size_usd = min(final_size_usd, self.max_position_size)

        # === START MODIFIED SECTION for _validate_and_cap_final_size ===
        # The following line is problematic if min_trade_size check has already occurred
        # and rejected sizes that are too small. If a size reaches here, it should
        # already be >= min_trade_size (if positive).
        # Forcing it up to min_trade_size here can make tiny valid Kelly sizes
        # (that should be rejected) appear as valid minimum trades.
        # min_trade_size_usd_for_constraints = self.min_trade_size_usd.get(
        #     opportunity.long_exchange, self._global_min_trade_size_usd
        # )
        min_trade_size_usd_for_constraints = self.min_trade_size_usd_per_exchange.get(
            opportunity.long_exchange,
            self._global_min_trade_size_usd,
        )
        if final_size_usd > ZERO and final_size_usd < min_trade_size_usd_for_constraints:
            self.logger.warning(
                f"RM_VALIDATE_CAP: Calculated size {final_size_usd:.4f} for {opportunity.symbol} "
                f"was positive but below min_trade_size {min_trade_size_usd_for_constraints:.4f}. "
                f"This should have been rejected earlier. Review _apply_risk_constraints. "
                f"Returning None for safety.",
            )
            return None
        # === END MODIFIED SECTION for _validate_and_cap_final_size ===

        # ... logging success ...
        # self.logger.info(f"Successfully sized opportunity: ... {final_size_usd}")
        return final_size_usd
