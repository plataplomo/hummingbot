import logging
from typing import Dict, Optional, Any, List, Tuple
from datetime import datetime, timedelta
import math
from decimal import Decimal, getcontext

from cyberdelta.core.signal_generator import ArbitrageOpportunity
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config
from cyberdelta.core.types import TradeSignal

logger = logging.getLogger(__name__)

# Set precision for Decimal
getcontext().prec = 28 # Default precision, adjust if needed

class SizedOpportunity:
    """
    An arbitrage opportunity with calculated position sizes and risk metrics.
    """
    
    def __init__(self, 
                 opportunity: ArbitrageOpportunity,
                 long_size: Decimal,
                 short_size: Decimal,
                 allocation_percentage: float,
                 expected_profit: Decimal,
                 expected_return: float,
                 risk_adjusted_return: float):
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
        return (f"SizedOpportunity: {self.opportunity.symbol} - "
                f"Long: {self.opportunity.long_exchange} ${self.long_size:.2f}, "
                f"Short: {self.opportunity.short_exchange} ${self.short_size:.2f}, "
                f"Alloc: {self.allocation_percentage:.2f}%, "
                f"ExpProfit: ${self.expected_profit:.2f}, "
                f"ExpReturn: {self.expected_return * 100:.2f}%, "
                f"RiskAdjReturn: {self.risk_adjusted_return:.4f}")


class RiskManager:
    """
    Assess and size trades based on risk parameters.
    
    Responsible for:
    - Validating opportunities against risk constraints
    - Applying position sizing based on Kelly criterion
    - Enforcing position limits and portfolio risk controls
    - Calculating risk metrics
    """
    
    def __init__(self, config: Config, portfolio_tracker: PortfolioTracker, circuit_breaker_system=None, funding_rate_validator=None):
        """
        Initialize the risk manager.
        
        Args:
            config: Application configuration
            portfolio_tracker: Portfolio state tracking
            circuit_breaker_system: Optional system for circuit breakers
            funding_rate_validator: Optional validator for funding rate predictions
        """
        self.config = config
        self.portfolio_tracker = portfolio_tracker
        self.circuit_breaker_system = circuit_breaker_system
        self.funding_rate_validator = funding_rate_validator
        
        # Load risk parameters from config
        self.max_position_size = Decimal(str(config.get('risk.max_position_size', 1000.0)))  # USD
        self.max_total_exposure = Decimal(str(config.get('risk.max_total_exposure', 5000.0)))  # USD
        self.kelly_fraction = config.get('risk.kelly_fraction', 0.5)  # Conservative multiplier
        self.max_collateral_per_exchange = config.get('risk.max_collateral_per_exchange', 0.8)  # 80% max on any exchange
        self.max_leverage = config.get('risk.max_leverage', 5.0)  # Maximum allowed leverage
        self.min_liquidation_buffer = config.get('risk.min_liquidation_buffer', 0.2)  # 20% buffer from liquidation
        
        # Portfolio-level risk management parameters
        self.max_exposure_per_asset = config.get('risk.max_exposure_per_asset', 0.2)  # 20% max exposure to any single asset
        self.max_exposure_per_exchange = config.get('risk.max_exposure_per_exchange', 0.5)  # 50% max exposure to any exchange
        self.max_correlated_exposure = config.get('risk.max_correlated_exposure', 0.3)  # 30% max exposure to correlated assets
        self.correlation_threshold = config.get('risk.correlation_threshold', 0.7)  # Correlation threshold for grouping assets
        self.circuit_breaker_recovery_factor = config.get('risk.circuit_breaker_recovery_factor', 0.3)  # 30% sizing during recovery
        self.min_exchange_balance = Decimal(str(config.get('risk_manager.min_exchange_balance', 10.0))) # Minimum balance
        
        # Validation metric thresholds
        self.max_acceptable_rmse = config.get('risk.max_acceptable_rmse', 0.05)  # 5% max acceptable RMSE for funding rate predictions
        self.max_acceptable_bias = config.get('risk.max_acceptable_bias', 0.02)  # 2% max acceptable bias
        self.min_validation_factor = config.get('risk.min_validation_factor', 0.2)  # Minimum factor when validation metrics are poor
        
        # Exchange-specific risk modifiers (can be used to be more conservative on certain exchanges)
        self.exchange_risk_modifiers: Dict[str, float] = {}
        for exchange_id in config.get('exchanges', {}).keys():
            if config.get(f'exchanges.{exchange_id}.enabled', False):
                self.exchange_risk_modifiers[exchange_id] = config.get(
                    f'exchanges.{exchange_id}.risk_modifier', 1.0)
    
    def _calculate_kelly_size(self, opportunity: ArbitrageOpportunity, total_capital: Decimal) -> Decimal:
        """
        Calculate Kelly-based position sizing.
        
        Args:
            opportunity: Arbitrage opportunity
            total_capital: Total available capital (Decimal)
            
        Returns:
            Recommended position size in USD (Decimal)
        """
        # Extract parameters from opportunity
        nfd = opportunity.net_funding_differential # Assuming this is already Decimal from SignalGenerator
        basis_volatility = opportunity.basis_volatility
        
        # Avoid division by zero
        if basis_volatility <= 0:
            basis_volatility = 0.001  # Default minimal volatility (float)
            
        # Calculate variance risk (float * float = float)
        variance_risk = basis_volatility ** 2
        
        # Get average price for Kelly calculation
        # Fetching from DataHandler is better practice, using opportunity prices for now
        # Ensure prices are Decimal
        long_price = opportunity.long_price # Corrected attribute name
        short_price = opportunity.short_price # Corrected attribute name
        if not long_price or not short_price:
             logger.warning("Missing prices in opportunity for Kelly calculation. Using placeholder.")
             avg_price = Decimal("1.0") # Placeholder
        else:
            avg_price = (long_price + short_price) / Decimal("2")
            if not isinstance(avg_price, Decimal):
                logger.error(f"Average price calculation resulted in non-Decimal: {avg_price}")
                avg_price = Decimal("1.0") # Fallback
               
        if avg_price <= Decimal("0"):
             logger.warning(f"Average price is zero or negative ({avg_price}). Using placeholder.")
             avg_price = Decimal("1.0") # Placeholder

        # Calculate Kelly fraction using Decimal for NFD and price, float for variance
        # Convert float variance to Decimal for calculation
        try:
            kelly = nfd / (Decimal(str(variance_risk)) * avg_price)
        except Exception as e:
            logger.error(f"Error calculating Kelly fraction: nfd={nfd}, var={variance_risk}, avg_p={avg_price}. Error: {e}")
            kelly = Decimal("0") # Default to zero on error

        # Apply conservative multiplier (float)
        kelly_adjusted = kelly * Decimal(str(self.kelly_fraction))
        
        # Ensure it's a positive value
        kelly_adjusted = max(Decimal("0"), kelly_adjusted)
        
        # === ADDED Logging ===
        logger.info(f"Kelly Inputs: NFD={nfd}, Vol={basis_volatility}, AvgPx={avg_price}, KellyFrac={self.kelly_fraction}")
        logger.info(f"Kelly Calc: RawKelly={kelly:.6f}, AdjustedKelly={kelly_adjusted:.6f}, TotalCapital={total_capital:.2f}")
        # === END Logging ===
        
        # Calculate size based on Kelly (Decimal * Decimal = Decimal)
        size = kelly_adjusted * total_capital
        
        logger.debug(f"Kelly calculation: f*={kelly:.4f}, f_adjusted={kelly_adjusted:.4f}, size=${size:.2f}")
        
        return size
    
    def _check_portfolio_constraints(self, 
                                    long_exchange: str, 
                                    short_exchange: str,
                                    long_size: Decimal,
                                    short_size: Decimal) -> bool:
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
        current_exposure = self.portfolio_tracker.get_total_exposure() # Assuming returns Decimal
        if not isinstance(current_exposure, Decimal):
             logger.warning(f"Current total exposure is not Decimal: {current_exposure}. Converting.")
             current_exposure = Decimal(str(current_exposure)) 
            
        new_exposure = current_exposure + long_size + short_size
        
        if new_exposure > self.max_total_exposure:
            logger.info(f"Trade exceeds maximum total exposure: ${new_exposure:.2f} > ${self.max_total_exposure:.2f}")
            return False
        
        # Check per-exchange exposure
        total_capital = self.portfolio_tracker.get_total_capital() # Assuming returns Decimal
        if not isinstance(total_capital, Decimal):
            logger.warning(f"Total capital is not Decimal: {total_capital}. Converting.")
            total_capital = Decimal(str(total_capital))
            
        for exchange, size in [(long_exchange, long_size), (short_exchange, short_size)]:
            current_exchange_exposure = self.portfolio_tracker.get_exchange_exposure(exchange) # Assuming returns Decimal
            if not isinstance(current_exchange_exposure, Decimal):
                logger.warning(f"Current exposure for {exchange} is not Decimal: {current_exchange_exposure}. Converting.")
                current_exchange_exposure = Decimal(str(current_exchange_exposure))
                
            new_exchange_exposure = current_exchange_exposure + size
            # max_collateral_per_exchange is float (ratio), multiply with Decimal capital
            max_exchange_exposure = total_capital * Decimal(str(self.max_collateral_per_exchange))
            
            if new_exchange_exposure > max_exchange_exposure:
                logger.info(f"Trade exceeds maximum exposure for {exchange}: "
                           f"${new_exchange_exposure:.2f} > ${max_exchange_exposure:.2f}")
                return False
        
        # Check leverage constraints
        for exchange, size in [(long_exchange, long_size), (short_exchange, short_size)]:
            # Correctly get collateral asset for the *specific* exchange
            collateral_asset = self.config.get(f'exchanges.{exchange}.collateral_asset', 'USD') 
            # Fetch the balance for the *correct* collateral asset
            available_capital_dec = self.portfolio_tracker.get_exchange_collateral_balance(exchange, collateral_asset=collateral_asset) 

            # Check if capital is sufficient (e.g., > 0)
            is_insufficient = available_capital_dec <= self.min_exchange_balance # Decimal comparison
            # Use collateral_asset in log
            logger.info(f"Capital Check {exchange}: Asset={collateral_asset}, Avail={available_capital_dec:.2f}, MinReq={self.min_exchange_balance:.2f}, IsInsufficient={is_insufficient}") # DETAILED DEBUG
            if is_insufficient:
                logger.warning(f"No available capital on {exchange} ({collateral_asset}) (Balance: {available_capital_dec:.2f}, Min: {self.min_exchange_balance:.2f})")
                return False
                
            implied_leverage = size / available_capital_dec if available_capital_dec > Decimal("0") else Decimal("Infinity")
            # Compare Decimal leverage to float max_leverage
            if implied_leverage > Decimal(str(self.max_leverage)):
                logger.info(f"Trade exceeds maximum leverage for {exchange}: "
                           f"{implied_leverage:.2f}x > {self.max_leverage:.2f}x")
                return False
        
        return True
    
    def _apply_portfolio_exposure_management(self, opportunity: ArbitrageOpportunity, base_size: Decimal) -> Decimal:
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
        total_capital = self.portfolio_tracker.get_total_capital() # Assuming Decimal
        if not isinstance(total_capital, Decimal):
            total_capital = Decimal(str(total_capital))
            
        if total_capital <= Decimal("0"):
            logger.warning("Cannot apply portfolio constraints: total capital is zero or negative")
            return min(base_size, self.max_position_size) # Return Decimal
        
        # Calculate current exposures
        current_total_exposure = self.portfolio_tracker.get_total_exposure() # Assuming Decimal
        if not isinstance(current_total_exposure, Decimal):
            current_total_exposure = Decimal(str(current_total_exposure))
        
        # Calculate current symbol exposure manually from all positions
        current_symbol_exposure = Decimal("0.0")
        all_positions = self.portfolio_tracker.get_all_positions() # Assuming returns list of Position objects
        for ex_id, pos in all_positions:
            if pos.symbol == symbol and pos.is_active():
                # Ensure mark_price and size are Decimal
                mark_price = pos.mark_price if isinstance(pos.mark_price, Decimal) else Decimal(str(pos.mark_price))
                pos_size = pos.size if isinstance(pos.size, Decimal) else Decimal(str(pos.size))
                current_symbol_exposure += abs(mark_price * pos_size)

        # Apply max exposure per asset constraint (Decimal * float ratio -> Decimal)
        max_asset_exposure = total_capital * Decimal(str(self.max_exposure_per_asset))
        available_asset_capacity = max_asset_exposure - current_symbol_exposure
        
        # Calculate available room for each constraint (using Decimal)
        available_total_exposure = max(Decimal("0"), self.max_total_exposure - current_total_exposure)
        available_symbol_exposure = max(Decimal("0"), available_asset_capacity)
        
        # Calculate per-exchange constraints
        current_long_exchange_exposure = self.portfolio_tracker.get_exchange_exposure(long_exchange) # Assuming Decimal
        if not isinstance(current_long_exchange_exposure, Decimal): current_long_exchange_exposure = Decimal(str(current_long_exchange_exposure))
        current_short_exchange_exposure = self.portfolio_tracker.get_exchange_exposure(short_exchange) # Assuming Decimal
        if not isinstance(current_short_exchange_exposure, Decimal): current_short_exchange_exposure = Decimal(str(current_short_exchange_exposure))
        
        max_allowed_exchange_exp = total_capital * Decimal(str(self.max_exposure_per_exchange)) 
        available_long_exchange = max(Decimal("0"), max_allowed_exchange_exp - current_long_exchange_exposure)
        available_short_exchange = max(Decimal("0"), max_allowed_exchange_exp - current_short_exchange_exposure)
        
        # Find the most restrictive constraint (min of Decimals)
        available_exposure = min(
            available_total_exposure,
            available_symbol_exposure,
            available_long_exchange,
            available_short_exchange
        )
        
        # Apply correlation-based exposure limits if we have other active positions
        # This section needs careful review for Decimal usage if implemented
        active_positions = self.portfolio_tracker.get_all_positions()
        if active_positions:
            # Assuming calculate_portfolio_correlation returns float modifier
            # Need to handle interaction between Decimal size and float factor
            try:
                correlation_factor = self.calculate_portfolio_correlation(opportunity, active_positions) 
                if correlation_factor < 1.0:
                    correlated_limit = total_capital * Decimal(str(self.max_correlated_exposure)) # Max exposure in USD
                    current_correlated_exposure = self._calculate_correlated_exposure(opportunity.symbol, active_positions)
                    if not isinstance(current_correlated_exposure, Decimal): current_correlated_exposure = Decimal(str(current_correlated_exposure))
                    available_correlated = max(Decimal("0"), correlated_limit - current_correlated_exposure)
                    available_exposure = min(available_exposure, available_correlated)
            except AttributeError:
                # Handle missing method - log warning or ignore for now
                logger.debug("calculate_portfolio_correlation method not found. Skipping correlation limits.")
            except Exception as e:
                logger.error(f"Error applying correlation limits: {e}")

        # Final size is the minimum of base size and available exposure
        final_size = min(base_size, available_exposure, self.max_position_size)
        
        logger.debug(f"Exposure Management: Base=${base_size:.2f}, AvailTotal=${available_total_exposure:.2f}, AvailSym=${available_symbol_exposure:.2f}, AvailLongEx=${available_long_exchange:.2f}, AvailShortEx=${available_short_exchange:.2f} -> Final=${final_size:.2f}")

        return final_size
    
    def _apply_portfolio_level_controls(self, 
                                      base_size: Decimal,
                                      opportunity: ArbitrageOpportunity) -> Decimal:
        """
        Apply portfolio level controls like volatility, drawdown, correlation.
        
        Args:
            base_size: Base position size (Decimal)
            opportunity: Arbitrage opportunity
            
        Returns:
            Adjusted position size (Decimal)
        """
        adjusted_size = base_size
        symbol = opportunity.symbol
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange

        # 1. Volatility Adjustment
        try:
            adjusted_size = self._apply_volatility_adjustment(
                adjusted_size, symbol, long_exchange, short_exchange
            )
        except Exception as e:
            logger.error(f"Error applying volatility adjustment: {e}", exc_info=True)

        # 2. Drawdown Protection
        try:
            adjusted_size = self._apply_drawdown_protection(
                adjusted_size, long_exchange, short_exchange
            )
        except Exception as e:
            logger.error(f"Error applying drawdown protection: {e}", exc_info=True)

        # 3. Correlation Limits (Ensure _calculate_portfolio_correlation exists and handles Decimal)
        try:
            adjusted_size = self._apply_correlation_limits(adjusted_size, symbol)
        except AttributeError:
             logger.debug("Skipping correlation limits due to missing method _apply_correlation_limits or related methods.")
        except Exception as e:
             logger.error(f"Error applying correlation limits: {e}", exc_info=True)

        # 4. Apply Circuit Breaker Adjustments if system exists
        if self.circuit_breaker_system:
            try:
                recovery_factor = Decimal("1.0")
                # Check global breaker
                if self.circuit_breaker_system.is_global_open():
                    recovery_factor = min(recovery_factor, Decimal(str(self.circuit_breaker_recovery_factor)))
                    logger.warning("Global circuit breaker is OPEN. Applying recovery factor.")
                
                # Check exchange-specific breakers
                for ex_name in [long_exchange, short_exchange]:
                    if self.circuit_breaker_system.is_exchange_open(ex_name):
                        recovery_factor = min(recovery_factor, Decimal(str(self.circuit_breaker_recovery_factor)))
                        logger.warning(f"Exchange circuit breaker for {ex_name} is OPEN. Applying recovery factor.")
                        break # Apply only once if any relevant breaker is open
                        
                adjusted_size *= recovery_factor
                if recovery_factor < Decimal("1.0"):
                     logger.info(f"Applied circuit breaker recovery factor {recovery_factor:.2f}. New size: ${adjusted_size:.2f}")

            except Exception as e:
                logger.error(f"Error applying circuit breaker adjustments: {e}", exc_info=True)

        # 5. Apply Funding Rate Validation Adjustment if validator exists
        if self.funding_rate_validator:
            try:
                # Get validation factors for both exchanges (returns float)
                long_validation_factor = self._get_validation_metrics(long_exchange, symbol)
                short_validation_factor = self._get_validation_metrics(short_exchange, symbol)
                
                # Use the minimum factor to be conservative
                min_factor = min(long_validation_factor, short_validation_factor)
                
                # Ensure factor is not below the absolute minimum (convert ratios to Decimal)
                final_factor_dec = max(Decimal(str(self.min_validation_factor)), Decimal(str(min_factor)))

                if final_factor_dec < Decimal("1.0"):
                    adjusted_size *= final_factor_dec # Multiply Decimal size by Decimal factor
                    logger.info(f"Applied funding rate validation factor {final_factor_dec:.2f}. New size: ${adjusted_size:.2f}")

            except Exception as e:
                logger.error(f"Error applying funding rate validation adjustments: {e}", exc_info=True)

        # Ensure size does not exceed max position size after adjustments
        adjusted_size = min(adjusted_size, self.max_position_size)

        # Ensure size is not negative
        adjusted_size = max(Decimal("0"), adjusted_size)

        logger.debug(f"Portfolio Level Controls: Base=${base_size:.2f} -> Adjusted=${adjusted_size:.2f}")
        return adjusted_size

    # --- Helper methods for portfolio level controls (ensure they handle Decimal) ---

    def _calculate_correlated_exposure(self, symbol: str, active_positions: List[Tuple[str, Any]]) -> Decimal:
        """Calculate exposure to assets correlated with the target symbol."""
        # Placeholder implementation - needs correlation matrix/logic
        # For now, return zero assuming no correlation considered
        # If implemented, ensure Decimal calculations
        return Decimal("0.0")

    def _apply_safety_system_adjustments(self, 
                                       opportunity: ArbitrageOpportunity, 
                                       base_size: Decimal) -> Decimal:
        """Apply adjustments based on safety systems like circuit breakers."""
        # NOTE: This method seems redundant as its logic is now incorporated into
        # _apply_portfolio_level_controls. Consider removing or refactoring.
        # For now, replicate the relevant logic from _apply_portfolio_level_controls.
        adjusted_size = base_size
        
        # Apply circuit breaker factor
        if self.circuit_breaker_system:
            try:
                recovery_factor = Decimal("1.0")
                if self.circuit_breaker_system.is_global_open():
                    recovery_factor = min(recovery_factor, Decimal(str(self.circuit_breaker_recovery_factor)))
                if self.circuit_breaker_system.is_exchange_open(opportunity.long_exchange):
                    recovery_factor = min(recovery_factor, Decimal(str(self.circuit_breaker_recovery_factor)))
                if self.circuit_breaker_system.is_exchange_open(opportunity.short_exchange):
                    recovery_factor = min(recovery_factor, Decimal(str(self.circuit_breaker_recovery_factor)))
                
                if recovery_factor < Decimal("1.0"):
                    adjusted_size *= recovery_factor
                    logger.warning(f"Safety System: Circuit breaker active. Applying factor {recovery_factor:.2f}. New size: ${adjusted_size:.2f}")
            except Exception as e:
                 logger.error(f"Error applying CB in safety adjustments: {e}")
                
        # Apply funding rate validation factor
        if self.funding_rate_validator:
            try:
                long_factor = self._get_validation_metrics(opportunity.long_exchange, opportunity.symbol)
                short_factor = self._get_validation_metrics(opportunity.short_exchange, opportunity.symbol)
                min_validation_factor_float = min(long_factor, short_factor)
                final_factor = max(Decimal(str(self.min_validation_factor)), Decimal(str(min_validation_factor_float)))
                if final_factor < Decimal("1.0"):
                    adjusted_size *= final_factor
                    logger.info(f"Safety System: Applied funding validation factor {final_factor:.2f}. New size: ${adjusted_size:.2f}")
            except Exception as e:
                logger.error(f"Error applying FV in safety adjustments: {e}")

        return max(Decimal("0"), adjusted_size)

    def _apply_volatility_adjustment(
        self,
        base_size: Decimal,
        symbol: str,
        long_exchange: str,
        short_exchange: str
    ) -> Decimal:
        """Adjust size based on market volatility."""
        try:
            # Fetch volatility metrics (assuming these return floats or need conversion)
            # Example: Assuming portfolio tracker provides volatility metrics
            current_volatility = self.portfolio_tracker.get_asset_volatility(symbol) 
            historical_volatility = self.portfolio_tracker.get_historical_volatility(symbol)
            
            if current_volatility is None or historical_volatility is None or historical_volatility == 0:
                logger.debug("Insufficient volatility data to apply adjustment.")
                return base_size

            # Convert to Decimal for calculation
            current_volatility_dec = Decimal(str(current_volatility))
            historical_volatility_dec = Decimal(str(historical_volatility))
            
            # Avoid division by zero if historical volatility is extremely small
            if historical_volatility_dec == Decimal("0"):
                logger.warning("Historical volatility is zero, cannot calculate ratio.")
                return base_size
                
            # Calculate volatility ratio
            vol_ratio = current_volatility_dec / historical_volatility_dec

            VOLATILITY_RATIO_THRESHOLD = Decimal("1.5") # Example threshold
            MAX_VOLATILITY_FACTOR = Decimal("0.5")     # Example reduction factor

            if vol_ratio > VOLATILITY_RATIO_THRESHOLD:
                factor = MAX_VOLATILITY_FACTOR
                adjusted_size = base_size * factor
                logger.info(f"High volatility detected (Ratio: {vol_ratio:.2f}). Reducing size by factor {factor:.2f}. New size: ${adjusted_size:.2f}")
                return adjusted_size
            else:
                return base_size
                
        except Exception as e:
            logger.error(f"Error calculating volatility adjustment: {e}", exc_info=True)
            # Fallback to base size on error
            return base_size

    def _apply_drawdown_protection(
        self, 
        base_size: Decimal,
        long_exchange: str,
        short_exchange: str
    ) -> Decimal:
        """Adjust size based on recent portfolio drawdown."""
        try:
            # Fetch drawdown metrics (assuming returns float or needs conversion)
            portfolio_drawdown = self.portfolio_tracker.get_portfolio_drawdown()
            long_exchange_drawdown = self.portfolio_tracker.get_exchange_drawdown(long_exchange)
            short_exchange_drawdown = self.portfolio_tracker.get_exchange_drawdown(short_exchange)
            
            if portfolio_drawdown is None:
                 logger.debug("Insufficient drawdown data available.")
                 return base_size

            # Convert to Decimal
            portfolio_drawdown_dec = Decimal(str(portfolio_drawdown))
            # Define thresholds (as Decimal)
            DRAWDOWN_THRESHOLD_1 = Decimal("0.05") # 5%
            DRAWDOWN_THRESHOLD_2 = Decimal("0.10") # 10%
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
                max_exchange_drawdown = max(max_exchange_drawdown, Decimal(str(long_exchange_drawdown)))
            if short_exchange_drawdown is not None: 
                max_exchange_drawdown = max(max_exchange_drawdown, Decimal(str(short_exchange_drawdown)))
                
            if max_exchange_drawdown > DRAWDOWN_THRESHOLD_2:
                factor = min(factor, DRAWDOWN_FACTOR_2)
            elif max_exchange_drawdown > DRAWDOWN_THRESHOLD_1:
                factor = min(factor, DRAWDOWN_FACTOR_1)

            if factor < Decimal("1.0"):
                adjusted_size = base_size * factor
                logger.info(f"Drawdown protection activated (Portfolio: {portfolio_drawdown_dec:.2%}, Max Exchange: {max_exchange_drawdown:.2%}). Reducing size by factor {factor:.2f}. New size: ${adjusted_size:.2f}")
                return adjusted_size
            else:
                return base_size

        except Exception as e:
            logger.error(f"Error applying drawdown protection: {e}", exc_info=True)
            # Fallback to base size on error
            return base_size

    def _apply_correlation_limits(
        self, 
        base_size: Decimal,
        symbol: str
    ) -> Decimal:
        """
        Adjust size based on correlation with existing positions.
        Requires correlation matrix/data.
        """
        # Placeholder: Assumes correlation logic exists and works with Decimal
        # This method needs actual implementation based on correlation data source
        try:
            # 1. Get current portfolio composition (active positions)
            active_positions = self.portfolio_tracker.get_all_positions() # List[Tuple[str, Position]]
            if not active_positions:
                return base_size # No existing positions, no correlation adjustment needed

            # 2. Calculate proposed position exposure
            proposed_exposure = base_size 

            # 3. Calculate total exposure to assets highly correlated with the new symbol
            correlated_exposure = Decimal("0.0")
            # Requires portfolio_tracker.get_asset_correlation(symbol1, symbol2)
            if not hasattr(self.portfolio_tracker, 'get_asset_correlation'):
                logger.debug("Portfolio tracker missing 'get_asset_correlation'. Skipping correlation limits.")
                return base_size
                
            for ex_id, pos in active_positions:
                # Check correlation between pos.symbol and symbol
                correlation = self.portfolio_tracker.get_asset_correlation(symbol, pos.symbol)
                if correlation is not None and correlation >= self.correlation_threshold:
                    # Add exposure of the correlated position
                    price = pos.mark_price if isinstance(pos.mark_price, Decimal) else Decimal(str(pos.mark_price))
                    size = pos.size if isinstance(pos.size, Decimal) else Decimal(str(pos.size))
                    correlated_exposure += abs(price * size)
            
            # 4. Determine maximum allowed correlated exposure
            total_capital = self.portfolio_tracker.get_total_capital()
            if not isinstance(total_capital, Decimal): total_capital = Decimal(str(total_capital))
            # Ensure total_capital is positive before proceeding
            if total_capital <= Decimal("0"):
                 logger.warning("Total capital is zero or negative, cannot apply correlation limits.")
                 return base_size
            max_allowed_correlated = total_capital * Decimal(str(self.max_correlated_exposure))

            # 5. Calculate available room for correlated exposure
            available_correlated_room = max(Decimal("0"), max_allowed_correlated - correlated_exposure)

            # 6. Adjust base_size if it exceeds available correlated room
            if proposed_exposure > available_correlated_room:
                adjusted_size = available_correlated_room
                logger.info(f"Correlation limit applied. Symbol: {symbol}, Current Correlated Exp: ${correlated_exposure:.2f}, Max Allowed: ${max_allowed_correlated:.2f}, Available: ${available_correlated_room:.2f}. Reducing size from ${base_size:.2f} to ${adjusted_size:.2f}")
                return max(Decimal("0"), adjusted_size) # Ensure non-negative
            else:
                # No adjustment needed based on correlation
                return base_size
                
        except AttributeError as ae:
             logger.debug(f"AttributeError during correlation limit check (likely missing method): {ae}")
             return base_size # Fallback if methods are missing
        except Exception as e:
            logger.error(f"Error applying correlation limits for symbol {symbol}: {e}", exc_info=True)
            # Fallback to base size on error
            return base_size


    def _get_validation_metrics(self, exchange: str, symbol: str) -> float:
        """
        Get validation metrics for funding rate predictions.
        Returns a factor (0.0 to 1.0) based on prediction accuracy.
        Factor = 1.0 means high confidence, lower values mean less confidence.
        """
        if not self.funding_rate_validator:
            return 1.0 # No validator, assume full confidence
            
        try:
            metrics = self.funding_rate_validator.get_validation_metrics(exchange, symbol)
            if not metrics:
                logger.warning(f"No validation metrics found for {exchange}/{symbol}. Assuming low confidence.")
                return float(self.min_validation_factor) # Return float
                
            rmse = metrics.get('rmse')
            bias = metrics.get('bias')
            
            if rmse is None or bias is None:
                 logger.warning(f"Incomplete validation metrics for {exchange}/{symbol}. Assuming low confidence.")
                 return float(self.min_validation_factor) # Return float
            
            # Normalize metrics against acceptable thresholds (use floats for ratios)
            rmse_float = float(rmse)
            bias_float = float(bias)
            max_rmse_float = float(self.max_acceptable_rmse)
            max_bias_float = float(self.max_acceptable_bias)
            min_factor_float = float(self.min_validation_factor)
            
            # Higher error -> lower factor
            rmse_factor = max(0.0, 1.0 - (rmse_float / max_rmse_float)) if max_rmse_float > 0 else 1.0
            bias_factor = max(0.0, 1.0 - (abs(bias_float) / max_bias_float)) if max_bias_float > 0 else 1.0
            
            # Combine factors (e.g., take the minimum to be conservative)
            combined_factor = min(rmse_factor, bias_factor)
            
            # Ensure factor is within bounds [min_validation_factor, 1.0]
            final_factor = max(min_factor_float, combined_factor)
            final_factor = min(1.0, final_factor)
            
            logger.debug(f"Validation Metrics Factor for {exchange}/{symbol}: RMSE={rmse:.4f}, Bias={bias:.4f} -> Factor={final_factor:.2f}")
            return final_factor # Return float factor
            
        except Exception as e:
            logger.error(f"Error getting validation metrics for {exchange}/{symbol}: {e}")
            return float(self.min_validation_factor) # Return float factor

    def size_opportunity(self, opportunity: ArbitrageOpportunity) -> Optional[SizedOpportunity]:
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
            opportunity.net_funding_differential = Decimal(str(opportunity.net_funding_differential))
        if hasattr(opportunity, 'long_price') and not isinstance(opportunity.long_price, Decimal):
            opportunity.long_price = Decimal(str(opportunity.long_price))
        if hasattr(opportunity, 'short_price') and not isinstance(opportunity.short_price, Decimal):
            opportunity.short_price = Decimal(str(opportunity.short_price))
        if hasattr(opportunity, 'expected_profit'):
             if opportunity.expected_profit is None:
                 opportunity.expected_profit = Decimal("0") # Handle None case
             elif not isinstance(opportunity.expected_profit, Decimal):
                 opportunity.expected_profit = Decimal(str(opportunity.expected_profit))
             
        # 1. Get Total Capital
        total_capital = self.portfolio_tracker.get_total_capital()
        if not isinstance(total_capital, Decimal):
             logger.warning(f"Total capital from tracker is not Decimal: {total_capital}. Converting.")
             total_capital = Decimal(str(total_capital))
             
        if total_capital <= Decimal("0"):
            logger.warning("Cannot size opportunity: total capital is zero or negative.")
            return None

        # 2. Kelly Criterion Sizing
        try:
            kelly_size = self._calculate_kelly_size(opportunity, total_capital)
        except Exception as e:
            logger.error(f"Error during Kelly sizing: {e}", exc_info=True)
            return None

        # 3. Apply Portfolio Exposure Management
        try:
            exposure_adjusted_size = self._apply_portfolio_exposure_management(opportunity, kelly_size)
        except Exception as e:
            logger.error(f"Error applying portfolio exposure management: {e}", exc_info=True)
            return None

        # 4. Apply Portfolio Level Controls (Volatility, Drawdown, Correlation, Safety Systems)
        try:
            final_base_size = self._apply_portfolio_level_controls(exposure_adjusted_size, opportunity)
        except Exception as e:
            logger.error(f"Error applying portfolio level controls: {e}", exc_info=True)
            return None
            
        # Ensure size doesn't exceed individual max position size (Decimal comparison)
        final_size = min(final_base_size, self.max_position_size)

        # Ensure size is not negative
        final_size = max(Decimal("0"), final_size)

        if final_size <= Decimal("0"):
            logger.info(f"Opportunity {opportunity.symbol} sized to zero or less after risk adjustments.")
            return None

        # 5. Check Portfolio Constraints with Final Size (Uses Decimal)
        long_size = final_size
        short_size = final_size
        try:
            if not self._check_portfolio_constraints(opportunity.long_exchange, opportunity.short_exchange, long_size, short_size):
                logger.info("Opportunity failed portfolio constraints check after sizing.")
                return None
        except Exception as e:
             logger.error(f"Error checking portfolio constraints: {e}", exc_info=True)
             return None

        # 6. Calculate derived metrics
        allocation_percentage = float((final_size / total_capital) * Decimal("100")) if total_capital > Decimal("0") else 0.0 # Result is float
        
        # Recalculate expected profit based on final size and NFD
        # Assuming prices are available in the opportunity object
        if opportunity.long_price and opportunity.short_price:
             avg_entry_price = (opportunity.long_price + opportunity.short_price) / Decimal("2")
             # Quantity = Size / Price
             quantity = final_size / avg_entry_price if avg_entry_price > Decimal("0") else Decimal("0")
             # Profit = Quantity * Price_Difference * Time_Factor (Simplified: Size * NFD)
             # Need to consider time horizon for funding rate profit
             # Simple approximation: Profit = Size * NetFundingDifferential (as a rate)
             # Assuming NFD is daily rate, profit per day = Size * NFD
             # This needs a more robust calculation based on strategy specifics.
             # Using a simplified placeholder based on NFD * Size
             final_expected_profit = final_size * opportunity.net_funding_differential # Decimal * Decimal
        else:
             logger.warning("Cannot accurately calculate expected profit due to missing prices. Using zero.")
             final_expected_profit = Decimal("0.0")

        # Expected return (percentage) - remains float, based on NFD
        expected_return = float(opportunity.net_funding_differential * 100) # Convert Decimal rate to float percentage
        risk_adjusted_return = float(opportunity.utility_score) if hasattr(opportunity, 'utility_score') else 0.0 # Use utility score as proxy (float)

        # 7. Create SizedOpportunity object
        sized_opportunity = SizedOpportunity(
            opportunity=opportunity,
            long_size=long_size, # Final Decimal size
            short_size=short_size, # Final Decimal size
            allocation_percentage=allocation_percentage, # Float %
            expected_profit=final_expected_profit, # Decimal USD profit
            expected_return=expected_return, # Float %
            risk_adjusted_return=risk_adjusted_return # Float score
        )
        
        logger.info(f"Successfully sized opportunity: {sized_opportunity}")
        return sized_opportunity

    def validate_opportunities(self, opportunities: List[ArbitrageOpportunity]) -> List[SizedOpportunity]:
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
                logger.info(f"Opportunity {opportunity.symbol} ({opportunity.long_exchange} vs {opportunity.short_exchange}) rejected during sizing/validation.")
        
        # Optional: Rank validated opportunities based on risk/reward (e.g., risk_adjusted_return)
        validated_opportunities.sort(key=lambda o: o.risk_adjusted_return, reverse=True)
        
        return validated_opportunities

    def get_portfolio_exposure_summary(self) -> Dict[str, Any]:
        """
        Generate a summary of current portfolio exposures.
        
        Returns:
            Dictionary containing exposure metrics (using Decimal for values).
        """
        total_capital = self.portfolio_tracker.get_total_capital()
        total_exposure = self.portfolio_tracker.get_total_exposure()
        # Ensure values from tracker are Decimal
        if not isinstance(total_capital, Decimal): total_capital = Decimal(str(total_capital)) 
        if not isinstance(total_exposure, Decimal): total_exposure = Decimal(str(total_exposure))
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            # Report floats for easier JSON serialization/external use
            "total_capital_usd": float(total_capital), 
            "total_exposure_usd": float(total_exposure),
            "total_exposure_pct": float((total_exposure / total_capital) * 100) if total_capital > Decimal("0") else 0.0,
            "max_total_exposure_limit_usd": float(self.max_total_exposure),
            "max_total_exposure_limit_pct": float((self.max_total_exposure / total_capital) * 100) if total_capital > Decimal("0") else 0.0,
            "exchange_exposure": {},
            "asset_exposure": {},
        }

        # Exchange Exposure
        exchanges = self.config.get('exchanges', {}).keys()
        asset_exposure_map: Dict[str, Decimal] = {}
        
        for exchange in exchanges:
            if not self.config.get(f'exchanges.{exchange}.enabled', False):
                continue
                
            ex_exposure = self.portfolio_tracker.get_exchange_exposure(exchange)
            ex_balance = self.portfolio_tracker.get_exchange_collateral_balance(exchange)
            # Ensure Decimal
            if not isinstance(ex_exposure, Decimal): ex_exposure = Decimal(str(ex_exposure))
            if not isinstance(ex_balance, Decimal): ex_balance = Decimal(str(ex_balance))
            
            max_allowed_ex_exp = total_capital * Decimal(str(self.max_exposure_per_exchange)) if total_capital > Decimal("0") else Decimal("0")

            summary["exchange_exposure"][exchange] = {
                "exposure_usd": float(ex_exposure),
                "exposure_pct_of_total_capital": float((ex_exposure / total_capital) * 100) if total_capital > Decimal("0") else 0.0,
                "balance_usd": float(ex_balance),
                "leverage": float(ex_exposure / ex_balance) if ex_balance > Decimal("0") else 0.0,
                "max_exposure_limit_usd": float(max_allowed_ex_exp),
                "max_exposure_limit_pct": float(self.max_exposure_per_exchange * 100)
            }

            # Aggregate asset exposures
            positions = self.portfolio_tracker.get_positions_by_exchange(exchange)
            for pos in positions:
                if pos.is_active():
                    symbol = pos.symbol # Assuming internal symbol
                    # Ensure price/size are Decimal
                    price = pos.mark_price if isinstance(pos.mark_price, Decimal) else Decimal(str(pos.mark_price))
                    size = pos.size if isinstance(pos.size, Decimal) else Decimal(str(pos.size))
                    exposure_usd = abs(price * size)
                    asset_exposure_map[symbol] = asset_exposure_map.get(symbol, Decimal("0")) + exposure_usd

        # Asset Exposure
        for symbol, exposure_usd in asset_exposure_map.items():
            # Ensure total_capital is positive before calculating max allowed exposure
            max_asset_exp_allowed = total_capital * Decimal(str(self.max_exposure_per_asset)) if total_capital > Decimal("0") else Decimal("0")
            summary["asset_exposure"][symbol] = {
                "exposure_usd": float(exposure_usd),
                "exposure_pct_of_total_capital": float((exposure_usd / total_capital) * 100) if total_capital > Decimal("0") else 0.0,
                "max_exposure_limit_usd": float(max_asset_exp_allowed),
                "max_exposure_limit_pct": float(self.max_exposure_per_asset * 100)
            }

        return summary
        
    def size_signal(self, signal: TradeSignal) -> TradeSignal:
        """ Size a generic TradeSignal based on risk parameters. Ensure Decimal usage. """
        # TODO: Implement sizing logic for generic signals, adapting Kelly or using simpler rules.
        # This requires defining how signal properties (e.g., confidence, predicted move) map to size.
        # Ensure Decimal usage throughout the implementation.
        logger.warning("size_signal is not fully implemented. Returning original signal without sizing.")
        # If implemented, ensure signal.quantity and related monetary values are Decimal.
        return signal
