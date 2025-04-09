import logging
from typing import Dict, Optional, Any, List
from datetime import datetime

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
    
    def __init__(self, config: Config, portfolio_tracker: PortfolioTracker):
        """
        Initialize the risk manager.
        
        Args:
            config: Application configuration
            portfolio_tracker: Portfolio state tracking
        """
        self.config = config
        self.portfolio_tracker = portfolio_tracker
        
        # Load risk parameters from config
        self.max_position_size = config.get('risk.max_position_size', 1000.0)  # USD
        self.max_total_exposure = config.get('risk.max_total_exposure', 5000.0)  # USD
        self.kelly_fraction = config.get('risk.kelly_fraction', 0.5)  # Conservative multiplier
        self.max_collateral_per_exchange = config.get('risk.max_collateral_per_exchange', 0.8)  # 80% max on any exchange
        self.max_leverage = config.get('risk.max_leverage', 5.0)  # Maximum allowed leverage
        self.min_liquidation_buffer = config.get('risk.min_liquidation_buffer', 0.2)  # 20% buffer from liquidation
        
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
        
        # Apply absolute position size cap
        capped_size = min(modified_size, self.max_position_size)
        
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
        expected_return = (expected_profit / total_size) * 100
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
