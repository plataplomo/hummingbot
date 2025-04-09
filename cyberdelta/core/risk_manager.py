import logging
from typing import Dict, Optional, Any, List, Tuple
from datetime import datetime
import math

from cyberdelta.core.signal_generator import ArbitrageOpportunity
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)

class SizedOpportunity:
    """
    An arbitrage opportunity with calculated position sizes and risk metrics.
    """
    
    def __init__(self, 
                 opportunity: ArbitrageOpportunity,
                 long_size: float,
                 short_size: float,
                 allocation_percentage: float,
                 expected_profit: float,
                 expected_return: float,
                 risk_adjusted_return: float):
        """
        Initialize a sized opportunity.
        
        Args:
            opportunity: The base arbitrage opportunity
            long_size: Position size for long side in USD
            short_size: Position size for short side in USD
            allocation_percentage: Percentage of total capital allocated
            expected_profit: Expected profit in USD
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
                f"ExpReturn: {self.expected_return:.2f}%, "
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
        self.max_position_size = config.get('risk.max_position_size', 1000.0)  # USD
        self.max_total_exposure = config.get('risk.max_total_exposure', 5000.0)  # USD
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
    
    def _calculate_kelly_size(self, opportunity: ArbitrageOpportunity, total_capital: float) -> float:
        """
        Calculate Kelly-based position sizing.
        
        Args:
            opportunity: Arbitrage opportunity
            total_capital: Total available capital
            
        Returns:
            Recommended position size in USD
        """
        # Extract parameters from opportunity
        nfd = opportunity.net_funding_differential / 100  # Convert percentage to decimal
        basis_volatility = opportunity.basis_volatility
        
        # Avoid division by zero
        if basis_volatility <= 0:
            basis_volatility = 0.001  # Default minimal volatility
            
        # Calculate variance risk (square of volatility)
        variance_risk = basis_volatility ** 2
        
        # Get average price for Kelly calculation
        # In practice, this would come from ticker data
        avg_price = 1.0  # Placeholder; in real implementation, get from data handler
        
        # Calculate Kelly fraction
        kelly = nfd / (variance_risk * avg_price)
        
        # Apply conservative multiplier
        kelly_adjusted = kelly * self.kelly_fraction
        
        # Ensure it's a positive value
        kelly_adjusted = max(0, kelly_adjusted)
        
        # Calculate size based on Kelly
        size = kelly_adjusted * total_capital
        
        logger.debug(f"Kelly calculation: f*={kelly:.4f}, f_adjusted={kelly_adjusted:.4f}, size=${size:.2f}")
        
        return size
    
    def _check_portfolio_constraints(self, 
                                    long_exchange: str, 
                                    short_exchange: str,
                                    long_size: float,
                                    short_size: float) -> bool:
        """
        Check if a trade satisfies portfolio-level constraints.
        
        Args:
            long_exchange: Exchange for long position
            short_exchange: Exchange for short position
            long_size: Size of long position in USD
            short_size: Size of short position in USD
            
        Returns:
            True if trade satisfies constraints, False otherwise
        """
        # Check total exposure
        current_exposure = self.portfolio_tracker.get_total_exposure()
        new_exposure = current_exposure + long_size + short_size
        
        if new_exposure > self.max_total_exposure:
            logger.info(f"Trade exceeds maximum total exposure: ${new_exposure:.2f} > ${self.max_total_exposure:.2f}")
            return False
        
        # Check per-exchange exposure
        for exchange, size in [(long_exchange, long_size), (short_exchange, short_size)]:
            current_exchange_exposure = self.portfolio_tracker.get_exchange_exposure(exchange)
            new_exchange_exposure = current_exchange_exposure + size
            total_capital = self.portfolio_tracker.get_total_capital()
            max_exchange_exposure = total_capital * self.max_collateral_per_exchange
            
            if new_exchange_exposure > max_exchange_exposure:
                logger.info(f"Trade exceeds maximum exposure for {exchange}: "
                           f"${new_exchange_exposure:.2f} > ${max_exchange_exposure:.2f}")
                return False
        
        # Check leverage constraints
        for exchange, size in [(long_exchange, long_size), (short_exchange, short_size)]:
            available_capital = self.portfolio_tracker.get_exchange_balance(exchange)
            if available_capital <= 0:
                logger.warning(f"No available capital on {exchange}")
                return False
                
            implied_leverage = size / available_capital
            if implied_leverage > self.max_leverage:
                logger.info(f"Trade exceeds maximum leverage for {exchange}: "
                           f"{implied_leverage:.2f}x > {self.max_leverage:.2f}x")
                return False
        
        return True
    
    def _apply_portfolio_exposure_management(self, opportunity: ArbitrageOpportunity, base_size: float) -> float:
        """
        Apply portfolio-level exposure management constraints to the position size.
        
        Args:
            opportunity: Arbitrage opportunity
            base_size: Base position size calculated by Kelly
            
        Returns:
            Adjusted position size respecting portfolio constraints
        """
        symbol = opportunity.symbol
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange
        
        # Get portfolio metrics
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital <= 0:
            logger.warning("Cannot apply portfolio constraints: total capital is zero or negative")
            return min(base_size, self.max_position_size)
        
        # Calculate current exposures
        current_total_exposure = self.portfolio_tracker.get_total_exposure()
        current_symbol_exposure = self.portfolio_tracker.get_symbol_exposure(symbol)
        current_long_exchange_exposure = self.portfolio_tracker.get_exchange_exposure(long_exchange)
        current_short_exchange_exposure = self.portfolio_tracker.get_exchange_exposure(short_exchange)
        
        # Calculate exposure ratios (as percentage of total capital)
        total_exposure_ratio = current_total_exposure / total_capital
        symbol_exposure_ratio = current_symbol_exposure / total_capital
        long_exchange_ratio = current_long_exchange_exposure / total_capital
        short_exchange_ratio = current_short_exchange_exposure / total_capital
        
        # Calculate available room for each constraint
        available_total_exposure = max(0, self.max_total_exposure / total_capital - total_exposure_ratio) * total_capital
        available_symbol_exposure = max(0, self.max_exposure_per_asset - symbol_exposure_ratio) * total_capital
        available_long_exchange = max(0, self.max_exposure_per_exchange - long_exchange_ratio) * total_capital
        available_short_exchange = max(0, self.max_exposure_per_exchange - short_exchange_ratio) * total_capital
        
        # Find the most restrictive constraint
        available_exposure = min(
            available_total_exposure,
            available_symbol_exposure,
            available_long_exchange,
            available_short_exchange
        )
        
        # Apply correlation-based exposure limits if we have other active positions
        active_positions = self.portfolio_tracker.get_active_positions()
        if active_positions:
            # Get correlation data from portfolio tracker or data handler (simplified here)
            # In a real implementation, this would use historical correlation data
            correlated_exposure = self._calculate_correlated_exposure(symbol, active_positions)
            correlated_exposure_ratio = correlated_exposure / total_capital
            
            # Calculate available room for correlated exposure
            available_correlated = max(0, self.max_correlated_exposure - correlated_exposure_ratio) * total_capital
            
            # Update available exposure with correlation constraint
            available_exposure = min(available_exposure, available_correlated)
        
        # Determine maximum position size based on available exposure
        # For a balanced arbitrage position, we need room for both sides
        max_position_size = min(available_exposure / 2, self.max_position_size)
        
        # Adjust base size to respect the exposure limits
        adjusted_size = min(base_size, max_position_size)
        
        if adjusted_size < base_size:
            logger.info(f"Position size reduced from ${base_size:.2f} to ${adjusted_size:.2f} due to portfolio exposure limits")
        
        return adjusted_size
    
    def _calculate_correlated_exposure(self, symbol: str, active_positions: List[Dict[str, Any]]) -> float:
        """
        Calculate the existing exposure to assets correlated with the given symbol.
        
        Args:
            symbol: Trading symbol to check
            active_positions: List of active positions in the portfolio
            
        Returns:
            Total exposure to correlated assets in USD
        """
        # Simplified correlation calculation - in practice would use market data
        # For now, assume assets with the same base currency are correlated
        base_currency = symbol.split('/')[0] if '/' in symbol else symbol[:3]  # Simple currency extraction, improve for real implementation
        
        correlated_exposure = 0.0
        for position in active_positions:
            position_symbol = position.get('symbol', '')
            position_base = position_symbol.split('/')[0] if '/' in position_symbol else position_symbol[:3]
            
            # Check if assets share the same base currency or have known correlation
            if position_base == base_currency:
                # Consider this position correlated
                correlated_exposure += abs(position.get('size_usd', 0.0))
            # In a real implementation, could use actual correlation matrix here
        
        return correlated_exposure
    
    def _apply_safety_system_adjustments(self, 
                                       opportunity: ArbitrageOpportunity, 
                                       base_size: float) -> float:
        """
        Apply adjustments based on safety systems like circuit breakers and validators.
        
        Args:
            opportunity: Arbitrage opportunity
            base_size: Base position size
            
        Returns:
            Adjusted position size respecting safety systems
        """
        adjusted_size = base_size
        
        # Adjust based on circuit breaker status
        if self.circuit_breaker_system:
            # Check circuit breakers for symbol and exchanges
            symbol = opportunity.symbol
            long_exchange = opportunity.long_exchange
            short_exchange = opportunity.short_exchange
            
            # Check symbol circuit breaker
            symbol_state = self.circuit_breaker_system.get_status(f"symbol:{symbol}")
            long_state = self.circuit_breaker_system.get_status(f"exchange:{long_exchange}")
            short_state = self.circuit_breaker_system.get_status(f"exchange:{short_exchange}")
            
            # If any circuit breaker is fully open, return zero size
            if "OPEN" in [symbol_state, long_state, short_state]:
                logger.warning(f"Circuit breaker active, preventing trade for {symbol}")
                return 0.0
            
            # If any circuit breaker is in recovery mode, reduce size
            if "HALF_OPEN" in [symbol_state, long_state, short_state]:
                logger.info(f"Circuit breaker in recovery mode, reducing position size")
                adjusted_size *= self.circuit_breaker_recovery_factor
        
        # Adjust based on funding rate validation metrics
        if self.funding_rate_validator:
            # Get validation metrics for both exchanges
            long_metrics = self._get_validation_metrics(opportunity.long_exchange, symbol)
            short_metrics = self._get_validation_metrics(opportunity.short_exchange, symbol)
            
            # Use the worst metrics to adjust size
            validation_factor = min(long_metrics, short_metrics)
            adjusted_size *= validation_factor
            
            if validation_factor < 1.0:
                logger.info(f"Position size reduced to {validation_factor:.2f}x due to funding rate validation metrics")
        
        return adjusted_size
    
    def _get_validation_metrics(self, exchange: str, symbol: str) -> float:
        """
        Get validation metrics adjustment factor for an exchange/symbol pair.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            Adjustment factor (0.0-1.0) based on validation metrics
        """
        # Default to 1.0 (no adjustment) if validator is not available
        if not self.funding_rate_validator:
            return 1.0
            
        try:
            # Get validation metrics from validator
            metrics = self.funding_rate_validator.get_metrics(exchange, symbol)
            
            # If no metrics available, use conservative default
            if not metrics:
                return 0.8  # 20% reduction when we have no validation data
            
            # Calculate adjustment based on RMSE (root mean square error)
            # Higher RMSE = lower factor = smaller position
            rmse = metrics.get('rmse', 0.0)
            rmse_factor = max(0.0, 1.0 - (rmse / self.max_acceptable_rmse))
            
            # Calculate adjustment based on bias
            # Higher absolute bias = lower factor
            bias = abs(metrics.get('bias', 0.0))
            bias_factor = max(0.0, 1.0 - (bias / self.max_acceptable_bias))
            
            # Combine factors (weighted average)
            combined_factor = (rmse_factor * 0.7) + (bias_factor * 0.3)
            
            # Ensure we don't go below the minimum factor
            return max(self.min_validation_factor, combined_factor)
            
        except Exception as e:
            logger.warning(f"Error getting validation metrics: {e}")
            return 0.8  # Conservative default on error
    
    def size_opportunity(self, opportunity: ArbitrageOpportunity) -> Optional[SizedOpportunity]:
        """
        Calculate appropriate position size for an arbitrage opportunity.
        
        Args:
            opportunity: Arbitrage opportunity to size
            
        Returns:
            Sized opportunity or None if opportunity doesn't meet risk criteria
        """
        # Get portfolio state
        total_capital = self.portfolio_tracker.get_total_capital()
        
        if total_capital <= 0:
            logger.warning("Cannot size opportunity: total capital is zero or negative")
            return None
        
        # Apply exchange-specific risk modifiers
        long_exchange_modifier = self.exchange_risk_modifiers.get(opportunity.long_exchange, 1.0)
        short_exchange_modifier = self.exchange_risk_modifiers.get(opportunity.short_exchange, 1.0)
        
        # Use the more conservative modifier
        exchange_modifier = min(long_exchange_modifier, short_exchange_modifier)
        
        # Calculate initial size based on Kelly criterion
        raw_size = self._calculate_kelly_size(opportunity, total_capital)
        
        # Apply exchange risk modifier
        modified_size = raw_size * exchange_modifier
        
        # Apply portfolio exposure management
        exposure_adjusted_size = self._apply_portfolio_exposure_management(opportunity, modified_size)
        
        # Apply safety system adjustments (circuit breakers, validation metrics)
        safety_adjusted_size = self._apply_safety_system_adjustments(opportunity, exposure_adjusted_size)
        
        # Apply absolute position size cap
        capped_size = min(safety_adjusted_size, self.max_position_size)
        
        # Ensure equal sizes for delta neutrality
        long_size = capped_size
        short_size = capped_size
        
        # Validate against portfolio constraints
        if not self._check_portfolio_constraints(
            opportunity.long_exchange, opportunity.short_exchange, long_size, short_size):
            logger.info(f"Opportunity failed portfolio constraints: {opportunity}")
            return None
        
        # Calculate expected profit with actual sizes
        # Note: Earlier profit calculation in opportunity was for a fixed $1000 position
        total_size = long_size + short_size
        position_ratio = total_size / 1000.0  # Ratio to the reference $1000 used in opportunity
        expected_profit = opportunity.expected_profit * position_ratio
        
        # Calculate return metrics
        allocation_percentage = (total_size / total_capital) * 100
        expected_return = (expected_profit / total_size) * 100 if total_size > 0 else 0
        risk_adjusted_return = expected_return / (opportunity.basis_volatility + 0.0001)  # Avoid division by zero
        
        # Create sized opportunity
        sized_opportunity = SizedOpportunity(
            opportunity=opportunity,
            long_size=long_size,
            short_size=short_size,
            allocation_percentage=allocation_percentage,
            expected_profit=expected_profit,
            expected_return=expected_return,
            risk_adjusted_return=risk_adjusted_return
        )
        
        logger.info(f"Sized opportunity: {sized_opportunity}")
        
        return sized_opportunity
    
    def validate_opportunities(self, opportunities: List[ArbitrageOpportunity]) -> List[SizedOpportunity]:
        """
        Validate and size a list of arbitrage opportunities.
        
        Args:
            opportunities: List of arbitrage opportunities
            
        Returns:
            List of sized opportunities that pass risk criteria
        """
        sized_opportunities = []
        
        for opportunity in opportunities:
            sized_opportunity = self.size_opportunity(opportunity)
            if sized_opportunity:
                sized_opportunities.append(sized_opportunity)
        
        # Sort by risk-adjusted return (descending)
        sized_opportunities.sort(key=lambda o: o.risk_adjusted_return, reverse=True)
        
        logger.info(f"Validated {len(sized_opportunities)} of {len(opportunities)} opportunities")
        
        return sized_opportunities
        
    def get_portfolio_exposure_summary(self) -> Dict[str, Any]:
        """
        Get a summary of current portfolio exposure metrics.
        
        Returns:
            Dictionary with exposure metrics
        """
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital <= 0:
            return {
                "total_capital": 0,
                "total_exposure": 0,
                "total_exposure_ratio": 0,
                "exchanges": {},
                "symbols": {},
                "status": "INVALID"
            }
            
        total_exposure = self.portfolio_tracker.get_total_exposure()
        
        # Calculate exchange exposures
        exchange_exposures = {}
        for exchange in self.portfolio_tracker.get_active_exchanges():
            exposure = self.portfolio_tracker.get_exchange_exposure(exchange)
            exchange_exposures[exchange] = {
                "exposure": exposure,
                "ratio": exposure / total_capital if total_capital > 0 else 0,
                "available": max(0, self.max_exposure_per_exchange * total_capital - exposure)
            }
            
        # Calculate symbol exposures
        symbol_exposures = {}
        for symbol in self.portfolio_tracker.get_active_symbols():
            exposure = self.portfolio_tracker.get_symbol_exposure(symbol)
            symbol_exposures[symbol] = {
                "exposure": exposure,
                "ratio": exposure / total_capital if total_capital > 0 else 0,
                "available": max(0, self.max_exposure_per_asset * total_capital - exposure)
            }
            
        # Calculate overall status
        if total_exposure / total_capital > self.max_total_exposure:
            status = "OVEREXPOSED"
        elif total_exposure / total_capital > self.max_total_exposure * 0.9:
            status = "NEAR_LIMIT"
        else:
            status = "NORMAL"
            
        return {
            "total_capital": total_capital,
            "total_exposure": total_exposure,
            "total_exposure_ratio": total_exposure / total_capital if total_capital > 0 else 0,
            "available_exposure": max(0, self.max_total_exposure * total_capital - total_exposure),
            "exchanges": exchange_exposures,
            "symbols": symbol_exposures,
            "status": status,
            "timestamp": datetime.now().timestamp()
        }
