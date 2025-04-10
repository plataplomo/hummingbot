import logging
from typing import Dict, Optional, Any, List, Tuple
from datetime import datetime, timedelta
import math

from cyberdelta.core.signal_generator import ArbitrageOpportunity
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config
from cyberdelta.core.types import TradeSignal

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
        self.min_exchange_balance = config.get('risk_manager.min_exchange_balance', 10.0) # Minimum balance required on an exchange
        
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
            collateral_asset = self.config.get(f'exchanges.{exchange}.collateral_asset', 'USD') # Get collateral for the specific exchange
            available_capital = self.portfolio_tracker.get_exchange_balance(exchange, collateral_asset)
            # logger.info(f"Initial Validity - Capital Check for {exchange}: Asset={collateral_asset}, Available={available_capital:.2f}") # Original Debug Log
            
            # Check if capital is sufficient (e.g., > 0)
            is_insufficient = available_capital <= self.min_exchange_balance # Calculate bool
            logger.info(f"Capital Check {exchange}: Asset={collateral_asset}, Avail={available_capital:.2f}, MinReq={self.min_exchange_balance:.2f}, IsInsufficient={is_insufficient}") # DETAILED DEBUG
            if is_insufficient:
                logger.warning(f"No available capital on {exchange} (Balance: {available_capital:.2f}, Min: {self.min_exchange_balance:.2f})")
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
        
        # Calculate current symbol exposure manually from all positions
        current_symbol_exposure = 0.0
        all_positions = self.portfolio_tracker.get_all_positions()
        for ex_id, pos in all_positions:
            # NOTE: Need to ensure internal symbol vs exchange symbol consistency here
            # Assuming opportunity.symbol is the internal symbol and pos.symbol is also internal? 
            # Or does pos.symbol need conversion based on ex_id?
            # For now, assume direct comparison works based on how positions are stored.
            if pos.symbol == symbol and pos.is_active():
                 current_symbol_exposure += abs(pos.mark_price * pos.size) # Use abs for total exposure regardless of side

        # Apply max exposure per asset constraint
        max_asset_exposure = total_capital * self.max_exposure_per_asset
        available_asset_capacity = max_asset_exposure - current_symbol_exposure
        
        # Calculate available room for each constraint
        available_total_exposure = max(0, self.max_total_exposure / total_capital - current_total_exposure / total_capital) * total_capital
        available_symbol_exposure = max(0, available_asset_capacity)
        available_long_exchange = max(0, self.max_exposure_per_exchange - current_total_exposure / total_capital) * total_capital
        available_short_exchange = max(0, self.max_exposure_per_exchange - current_total_exposure / total_capital) * total_capital
        
        # Find the most restrictive constraint
        available_exposure = min(
            available_total_exposure,
            available_symbol_exposure,
            available_long_exchange,
            available_short_exchange
        )
        
        # Apply correlation-based exposure limits if we have other active positions
        # Use get_all_positions() as it returns active positions
        active_positions = self.portfolio_tracker.get_all_positions()
        if active_positions:
            correlation_factor = self.calculate_portfolio_correlation(opportunity, active_positions)
        
        # Determine maximum position size based on available exposure
        # For a balanced arbitrage position, we need room for both sides
        max_position_size = min(available_exposure / 2, self.max_position_size)
        
        # Adjust base size to respect the exposure limits
        adjusted_size = min(base_size, max_position_size)
        
        if adjusted_size < base_size:
            logger.info(f"Position size reduced from ${base_size:.2f} to ${adjusted_size:.2f} due to portfolio exposure limits")
        
        return adjusted_size
    
    def _apply_portfolio_level_controls(self, 
                                      base_size: float, 
                                      opportunity: ArbitrageOpportunity) -> float:
        """
        Apply comprehensive portfolio-level controls to manage overall risk
        
        Args:
            base_size: Base position size after previous adjustments
            opportunity: The arbitrage opportunity
            
        Returns:
            Position size adjusted for portfolio-level controls
        """
        # Configuration parameters
        MAX_LEVERAGE = self.config.get('risk.max_leverage', 3.0)
        TARGET_LEVERAGE = self.config.get('risk.target_leverage', 2.0)
        MAX_EXPOSURE_PER_STRATEGY = self.config.get('risk.max_exposure_per_strategy', 0.4)  # 40% max per strategy
        MAX_EXCHANGE_CONCENTRATION = self.config.get('risk.max_exchange_concentration', 0.6)  # 60% max on any exchange
        RISK_PARITY_FACTOR = self.config.get('risk.risk_parity_factor', 0.5)  # How strongly to enforce risk parity
        
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital <= 0:
            logger.warning("Cannot apply portfolio-level controls: total capital is zero or negative")
            return base_size
        
        symbol = opportunity.symbol
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange
        strategy_type = getattr(opportunity, 'strategy_type', 'funding_rate_arbitrage')
        
        # Get current portfolio state
        exchange_exposures = {}
        strategy_exposures = {}
        total_exposure = 0.0
        total_risk = 0.0
        
        # Collect current exposures and risk levels
        # Use get_all_positions() as it returns active positions
        active_positions = self.portfolio_tracker.get_all_positions()
        for ex_id, pos in active_positions:
            # Accumulate exposures by exchange
            # Assuming Position object has 'exchange' and 'size_usd' attributes
            # or these need calculation
            pos_exchange = getattr(pos, 'exchange', None) # Safely get exchange
            pos_size_usd = getattr(pos, 'mark_price', 0.0) * getattr(pos, 'size', 0.0)
            
            if pos_exchange:
                if pos_exchange not in exchange_exposures:
                    exchange_exposures[pos_exchange] = 0.0
                exchange_exposures[pos_exchange] += pos_size_usd
            
            # Accumulate exposures by strategy
            pos_strategy = getattr(pos, 'strategy_type', 'funding_rate_arbitrage')
            if pos_strategy not in strategy_exposures:
                strategy_exposures[pos_strategy] = 0.0
            strategy_exposures[pos_strategy] += pos_size_usd
            
            # Accumulate total exposure
            total_exposure += pos_size_usd
            
            # Accumulate risk (weighted by volatility)
            position_volatility = getattr(pos, 'volatility', 0.01)  # Default if not available
            total_risk += pos_size_usd * position_volatility
        
        # Calculate portfolio-level metrics
        current_leverage = total_exposure / total_capital if total_capital > 0 else 0
        
        # Get opportunity-specific metrics
        opportunity_volatility = opportunity.basis_volatility
        
        # Apply risk-parity adjustment if we have existing positions
        risk_parity_size = base_size
        if total_risk > 0 and opportunity_volatility > 0:
            # Calculate target risk contribution based on existing portfolio
            # Higher RISK_PARITY_FACTOR = stronger enforcement of risk parity
            target_risk_contrib = (total_risk / total_exposure) if total_exposure > 0 else opportunity_volatility
            risk_weight = target_risk_contrib / opportunity_volatility
            risk_adjustment = (risk_weight ** RISK_PARITY_FACTOR)  # Dampen the adjustment
            risk_parity_size = base_size * risk_adjustment
            logger.debug(f"Risk parity adjustment: {risk_adjustment:.2f}")
        
        # Apply leverage constraints
        leverage_adjusted_size = base_size
        if current_leverage >= TARGET_LEVERAGE:
            # We're at or above target leverage, reduce position size
            leverage_factor = max(0.0, (MAX_LEVERAGE - current_leverage) / (MAX_LEVERAGE - TARGET_LEVERAGE))
            leverage_adjusted_size = base_size * leverage_factor
            logger.debug(f"Leverage control: current={current_leverage:.2f}x, factor={leverage_factor:.2f}")
            
        # Apply strategy concentration limits
        strategy_adjusted_size = base_size
        current_strategy_exposure = strategy_exposures.get(strategy_type, 0.0)
        strategy_ratio = current_strategy_exposure / total_capital
        if strategy_ratio >= MAX_EXPOSURE_PER_STRATEGY:
            # Already at max for this strategy
            strategy_adjusted_size = 0.0
            logger.debug(f"Strategy {strategy_type} at maximum exposure ({strategy_ratio:.2%})")
        else:
            available_strategy_room = (MAX_EXPOSURE_PER_STRATEGY - strategy_ratio) * total_capital
            strategy_adjusted_size = min(base_size, available_strategy_room)
            
        # Apply exchange concentration limits
        exchange_adjusted_size = base_size
        for exchange in [long_exchange, short_exchange]:
            current_exchange_exposure = exchange_exposures.get(exchange, 0.0)
            exchange_ratio = current_exchange_exposure / total_capital
            if exchange_ratio >= MAX_EXCHANGE_CONCENTRATION:
                # Already at max for this exchange
                exchange_adjusted_size = 0.0
                logger.debug(f"Exchange {exchange} at maximum concentration ({exchange_ratio:.2%})")
                break
            else:
                available_exchange_room = (MAX_EXCHANGE_CONCENTRATION - exchange_ratio) * total_capital
                exchange_adjusted_size = min(exchange_adjusted_size, available_exchange_room)
        
        # Take the most restrictive adjustment
        final_size = min(
            risk_parity_size,
            leverage_adjusted_size,
            strategy_adjusted_size,
            exchange_adjusted_size,
            base_size  # Never increase size
        )
        
        # Log significant adjustments
        if final_size < base_size * 0.9:  # More than 10% reduction
            logger.info(f"Portfolio-level controls reduced position size: ${base_size:.2f} -> ${final_size:.2f}")
            
        return final_size
    
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
        Apply adjustments based on safety systems (circuit breakers, validation metrics).
        
        Args:
            opportunity: Arbitrage opportunity
            base_size: Base position size from portfolio constraints
            
        Returns:
            Adjusted position size
        """
        # Check circuit breaker status first
        cb_adjustment = 1.0
        
        # Apply circuit breaker adjustments if system available
        if self.circuit_breaker_system:
            # Check breakers related to the exchanges
            long_breaker_status = self.circuit_breaker_system.get_status(
                "exchange", opportunity.long_exchange)
            short_breaker_status = self.circuit_breaker_system.get_status(
                "exchange", opportunity.short_exchange)
            
            # Check symbol-specific breakers
            symbol_breaker_status = self.circuit_breaker_system.get_status(
                "symbol", opportunity.symbol)
            
            # Determine most restrictive breaker
            if any(status in ["OPEN", "HALF_OPEN"] for status in [long_breaker_status, short_breaker_status, symbol_breaker_status]):
                logger.info(f"Circuit breaker active for opportunity: {opportunity.symbol}, "
                           f"{opportunity.long_exchange}, {opportunity.short_exchange}")
                
                # In recovery mode (HALF_OPEN), allow smaller position sizes
                if any(status == "HALF_OPEN" for status in [long_breaker_status, short_breaker_status, symbol_breaker_status]):
                    cb_adjustment = self.circuit_breaker_recovery_factor
                else:
                    # Breaker fully open - no trades
                    return 0.0
                    
        # Next, check validation metrics
        validation_adjustment = 1.0
        
        # Get validation metrics for both sides
        long_validation = self._get_validation_metrics(opportunity.long_exchange, opportunity.symbol)
        short_validation = self._get_validation_metrics(opportunity.short_exchange, opportunity.symbol)
        
        # Use more conservative validation factor
        validation_adjustment = min(long_validation, short_validation)
        
        # Apply compound adjustment
        final_adjustment = min(cb_adjustment, validation_adjustment)
        
        return base_size * final_adjustment
        
    def _apply_volatility_adjustment(
        self, 
        base_size: float, 
        symbol: str, 
        long_exchange: str,
        short_exchange: str
    ) -> float:
        """
        Adjust position size based on recent market volatility
        
        Args:
            base_size: Base position size from Kelly calculation
            symbol: Trading symbol
            long_exchange: Exchange for long position
            short_exchange: Exchange for short position
            
        Returns:
            Volatility-adjusted position size
        """
        # Constants for volatility adjustment
        VOLATILITY_SCALING_FACTOR = self.config.get('risk.volatility_scaling_factor', 0.7)
        MAX_VOLATILITY_REDUCTION = self.config.get('risk.max_volatility_reduction', 0.8)  # Maximum 80% reduction
        VOLATILITY_RATIO_THRESHOLD = self.config.get('risk.volatility_ratio_threshold', 1.5)  # Threshold above which we reduce position
        
        # Assume data_handler is accessible through portfolio_tracker or directly
        # In a real implementation, this would be injected or accessible through a service
        data_handler = getattr(self, 'data_handler', None)
        if not data_handler:
            logger.warning("No data handler available for volatility adjustment")
            return base_size
        
        # Get recent volatility data for both exchanges
        try:
            # Get recent volatility (e.g., 3-day)
            long_recent_vol = data_handler.get_recent_volatility(long_exchange, symbol, days=3)
            short_recent_vol = data_handler.get_recent_volatility(short_exchange, symbol, days=3)
            
            # Get baseline volatility (e.g., 30-day average)
            long_baseline_vol = data_handler.get_historical_volatility(long_exchange, symbol, days=30)
            short_baseline_vol = data_handler.get_historical_volatility(short_exchange, symbol, days=30)
            
            # Skip adjustment if data is missing
            if any(vol is None for vol in [long_recent_vol, short_recent_vol, long_baseline_vol, short_baseline_vol]):
                logger.debug(f"Missing volatility data for {symbol} on {long_exchange} or {short_exchange}")
                return base_size
                
            # Skip adjustment if baseline volatility is zero or near-zero
            if long_baseline_vol < 0.0001 or short_baseline_vol < 0.0001:
                logger.debug(f"Baseline volatility too low for {symbol}")
                return base_size
                
            # Calculate volatility ratios
            long_vol_ratio = long_recent_vol / long_baseline_vol
            short_vol_ratio = short_recent_vol / short_baseline_vol
            
            # Use the higher ratio (more conservative approach)
            vol_ratio = max(long_vol_ratio, short_vol_ratio)
            
            # Calculate adjustment factor
            # Higher recent volatility = lower position size
            if vol_ratio > VOLATILITY_RATIO_THRESHOLD:
                # Volatility is above threshold, reduce position size
                # The reduction increases as vol_ratio increases
                reduction_factor = min(
                    MAX_VOLATILITY_REDUCTION,
                    VOLATILITY_SCALING_FACTOR * (vol_ratio - VOLATILITY_RATIO_THRESHOLD)
                )
                adjustment_factor = 1.0 - reduction_factor
                
                # Ensure adjustment factor is at least 0.2 (never reduce by more than 80%)
                adjustment_factor = max(1.0 - MAX_VOLATILITY_REDUCTION, adjustment_factor)
                
                logger.info(f"Reducing position size for {symbol} due to high volatility: "
                           f"ratio={vol_ratio:.2f}, adjustment={adjustment_factor:.2f}")
                
                return base_size * adjustment_factor
            else:
                # Volatility is within acceptable range, no adjustment needed
                return base_size
                
        except Exception as e:
            logger.error(f"Error calculating volatility adjustment: {e}", exc_info=True)
            return base_size  # In case of error, return unadjusted size

    def _apply_drawdown_protection(
        self, 
        base_size: float,
        long_exchange: str,
        short_exchange: str
    ) -> float:
        """
        Implement drawdown protection to reduce position sizes during periods of losses
        
        Args:
            base_size: Base position size after previous adjustments
            long_exchange: Exchange for long position
            short_exchange: Exchange for short position
            
        Returns:
            Position size adjusted for drawdown protection
        """
        # Configuration parameters
        MAX_DRAWDOWN = self.config.get('risk.max_drawdown', 0.2)  # 20% max drawdown
        DRAWDOWN_SCALING_FACTOR = self.config.get('risk.drawdown_scaling_factor', 2.0)  # How aggressively to scale down
        MIN_SIZING_FACTOR = self.config.get('risk.min_drawdown_sizing_factor', 0.1)  # Minimum 10% of normal size
        
        # Get current drawdown from portfolio tracker
        current_drawdown = self.portfolio_tracker.get_current_drawdown()
        
        # If drawdown exceeds threshold, reduce position size
        if current_drawdown > 0:
            # Calculate reduction factor based on drawdown relative to max allowed
            drawdown_ratio = current_drawdown / MAX_DRAWDOWN
            
            if drawdown_ratio > 0.5:  # If we're past half of max drawdown
                # Scale down position size based on drawdown ratio
                # As drawdown approaches MAX_DRAWDOWN, sizing approaches MIN_SIZING_FACTOR
                sizing_factor = 1.0 - ((drawdown_ratio - 0.5) * 2.0 * (1.0 - MIN_SIZING_FACTOR))
                
                # Ensure we never go below minimum sizing
                sizing_factor = max(MIN_SIZING_FACTOR, sizing_factor)
                
                logger.info(f"Reducing position size due to drawdown protection: "
                           f"current drawdown={current_drawdown:.2%}, sizing factor={sizing_factor:.2f}")
                
                return base_size * sizing_factor
                
        # If drawdown is within acceptable range, return base size
        return base_size

    def _apply_correlation_limits(
        self, 
        base_size: float, 
        symbol: str
    ) -> float:
        """
        Apply position size limits based on correlation with existing positions
        
        Args:
            base_size: Base position size after previous adjustments
            symbol: Trading symbol
            
        Returns:
            Position size adjusted for correlation limits
        """
        # Configuration parameters
        CORRELATION_THRESHOLD = self.config.get('risk.correlation_threshold', 0.7)  # Correlation threshold for limiting exposure
        MAX_CORRELATION_GROUP_EXPOSURE = self.config.get('risk.max_correlation_group_exposure', 0.3)  # Max 30% of capital in correlated assets
        
        # Skip if we have no data handler for correlation calculations
        data_handler = getattr(self, 'data_handler', None)
        if not data_handler:
            logger.warning("No data handler available for correlation calculation")
            return base_size
        
        # Get active positions from portfolio tracker
        active_positions = self.portfolio_tracker.get_active_positions()
        if not active_positions:
            # No active positions, so no correlation concerns
            return base_size
        
        # Get total capital
        total_capital = self.portfolio_tracker.get_total_capital()
        if total_capital <= 0:
            logger.warning("Total capital is zero or negative")
            return base_size
        
        try:
            # Calculate current exposure to correlated assets
            correlated_symbols = []
            correlated_exposure = 0.0
            
            # Identify correlated assets
            for pos_symbol, position in active_positions.items():
                if pos_symbol == symbol:
                    # Skip the current symbol itself
                    continue
                    
                # Calculate correlation between symbol and current position
                correlation = data_handler.get_correlation(symbol, pos_symbol, days=30)
                
                # If correlation exceeds threshold, add to correlated group
                if correlation is not None and abs(correlation) >= CORRELATION_THRESHOLD:
                    correlated_symbols.append(pos_symbol)
                    correlated_exposure += position.size_usd
                    logger.debug(f"Symbol {symbol} correlated with {pos_symbol}: {correlation:.2f}")
            
            # If we have correlated assets, apply exposure limits
            if correlated_symbols:
                # Calculate current correlation group exposure ratio
                correlated_ratio = correlated_exposure / total_capital
                
                # Calculate maximum additional exposure allowed
                max_additional = (MAX_CORRELATION_GROUP_EXPOSURE - correlated_ratio) * total_capital
                
                # Ensure it's not negative
                max_additional = max(0, max_additional)
                
                if max_additional < base_size:
                    logger.info(f"Reducing position size due to correlation limits: "
                               f"{base_size:.2f} -> {max_additional:.2f} "
                               f"(correlated with {', '.join(correlated_symbols)})")
                    return max_additional
            
            # No correlated assets or within limits
            return base_size
            
        except Exception as e:
            logger.error(f"Error applying correlation limits: {e}", exc_info=True)
            # In case of error, return unadjusted size but with a warning
            logger.warning("Using unadjusted position size due to correlation calculation error")
            return base_size

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
        
        # Apply volatility-based position scaling
        volatility_adjusted_size = self._apply_volatility_adjustment(
            modified_size,
            opportunity.symbol,
            opportunity.long_exchange,
            opportunity.short_exchange
        )
        
        # Apply drawdown protection
        drawdown_adjusted_size = self._apply_drawdown_protection(
            volatility_adjusted_size,
            opportunity.long_exchange,
            opportunity.short_exchange
        )
        
        # Apply correlation-based position limits
        correlation_adjusted_size = self._apply_correlation_limits(
            drawdown_adjusted_size,
            opportunity.symbol
        )
        
        # Apply portfolio exposure management
        exposure_adjusted_size = self._apply_portfolio_exposure_management(opportunity, correlation_adjusted_size)
        
        # Apply portfolio-level controls
        portfolio_adjusted_size = self._apply_portfolio_level_controls(exposure_adjusted_size, opportunity)
        
        # Apply safety system adjustments (circuit breakers, validation metrics)
        safety_adjusted_size = self._apply_safety_system_adjustments(opportunity, portfolio_adjusted_size)
        
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
        now = datetime.now()
        # Define a validity window (e.g., 5 minutes)
        validity_window = timedelta(minutes=5) 
        
        for opportunity in opportunities:
            # --- Add validity check --- 
            if now - opportunity.timestamp > validity_window:
                logger.debug(f"Skipping expired opportunity for {opportunity.symbol} from {opportunity.timestamp}")
                continue # Skip expired opportunities
            # -------------------------    
            
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

    def size_signal(self, signal: TradeSignal) -> TradeSignal:
        """
        Apply position sizing to a trade signal.
        
        Args:
            signal: The trade signal to size
            
        Returns:
            Sized trade signal with adjusted quantities
        """
        if signal is None:
            return None
            
        # Extract key information from the signal
        symbol = signal.symbol
        exchange_id = signal.exchange_id
        direction = signal.direction
        
        # Get total available capital
        total_capital = self.portfolio_tracker.get_total_capital()
        exchange_balance = self.portfolio_tracker.get_exchange_balance(exchange_id)
        
        # Check if we have any capital to work with
        if total_capital <= 0 or exchange_balance <= 0:
            logger.warning(f"Insufficient capital to size signal for {symbol} on {exchange_id}")
            return signal
            
        # Calculate base position size
        # For simplicity, we'll start with a fixed percentage of exchange balance
        # In practice, this would use the Kelly criterion or other risk-based sizing
        base_size_usd = exchange_balance * 0.1  # 10% of exchange balance
        
        # Apply position limits
        max_position_size = self.max_position_size
        
        # Apply exchange-specific risk modifier
        exchange_modifier = self.exchange_risk_modifiers.get(exchange_id, 1.0)
        adjusted_size_usd = min(base_size_usd, max_position_size) * exchange_modifier
        
        # Apply portfolio constraints
        # Check if adding this position would exceed total exposure limits
        current_exposure = self.portfolio_tracker.get_total_exposure()
        if current_exposure + adjusted_size_usd > self.max_total_exposure:
            # Scale down to fit within limits
            available_exposure = max(0, self.max_total_exposure - current_exposure)
            adjusted_size_usd = min(adjusted_size_usd, available_exposure)
            logger.info(f"Sizing limited by total exposure constraint: ${adjusted_size_usd:.2f}")
        
        # Apply symbol-specific exposure limit
        symbol_exposure = self.portfolio_tracker.get_symbol_exposure(symbol)
        max_symbol_exposure = total_capital * self.max_exposure_per_asset
        if symbol_exposure + adjusted_size_usd > max_symbol_exposure:
            available_symbol_exposure = max(0, max_symbol_exposure - symbol_exposure)
            adjusted_size_usd = min(adjusted_size_usd, available_symbol_exposure)
            logger.info(f"Sizing limited by symbol exposure constraint: ${adjusted_size_usd:.2f}")
        
        # Apply exchange-specific exposure limit
        exchange_exposure = self.portfolio_tracker.get_exchange_exposure(exchange_id)
        max_exchange_exposure = total_capital * self.max_exposure_per_exchange
        if exchange_exposure + adjusted_size_usd > max_exchange_exposure:
            available_exchange_exposure = max(0, max_exchange_exposure - exchange_exposure)
            adjusted_size_usd = min(adjusted_size_usd, available_exchange_exposure)
            logger.info(f"Sizing limited by exchange exposure constraint: ${adjusted_size_usd:.2f}")
        
        # Apply any safety system adjustments
        if self.circuit_breaker_system and self.circuit_breaker_system.is_active():
            adjusted_size_usd *= self.circuit_breaker_recovery_factor
            logger.info(f"Circuit breaker active - reducing size to ${adjusted_size_usd:.2f}")
        
        # Create a copy of the signal with the updated size
        sized_signal = TradeSignal(
            id=signal.id,
            timestamp=signal.timestamp,
            symbol=signal.symbol,
            exchange_id=signal.exchange_id,
            direction=signal.direction,
            price=signal.price,
            quantity=adjusted_size_usd / signal.price if signal.price > 0 else 0,  # Convert USD to quantity
            signal_type=signal.signal_type,
            confidence=signal.confidence,
            expiration=signal.expiration,
            metadata={
                **signal.metadata,  # Preserve existing metadata
                "original_quantity": signal.quantity,
                "sized_usd": adjusted_size_usd,
                "risk_manager_applied": True
            }
        )
        
        logger.info(f"Sized signal for {symbol} on {exchange_id}: ${adjusted_size_usd:.2f}")
        return sized_signal
