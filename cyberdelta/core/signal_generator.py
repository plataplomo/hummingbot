import logging
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)

class ArbitrageOpportunity:
    """
    Represents a funding rate arbitrage opportunity between two exchanges.
    """
    
    def __init__(self,
                 symbol: str,
                 long_exchange: str,
                 short_exchange: str,
                 long_funding_rate: float,
                 short_funding_rate: float,
                 net_funding_differential: float,
                 timestamp: datetime,
                 expected_profit: float,
                 utility_score: float,
                 basis_volatility: float):
        """
        Initialize an arbitrage opportunity.
        
        Args:
            symbol: Trading symbol
            long_exchange: Exchange to take long position
            short_exchange: Exchange to take short position
            long_funding_rate: Funding rate on long exchange
            short_funding_rate: Funding rate on short exchange
            net_funding_differential: Difference between funding rates
            timestamp: When the opportunity was identified
            expected_profit: Expected profit after costs
            utility_score: Ranking score considering profit and risk
            basis_volatility: Volatility of the basis between exchanges
        """
        self.symbol = symbol
        self.long_exchange = long_exchange
        self.short_exchange = short_exchange
        self.long_funding_rate = long_funding_rate
        self.short_funding_rate = short_funding_rate
        self.net_funding_differential = net_funding_differential
        self.timestamp = timestamp
        self.expected_profit = expected_profit
        self.utility_score = utility_score
        self.basis_volatility = basis_volatility
        
    def __str__(self) -> str:
        """String representation of the opportunity."""
        return (f"ArbitrageOpportunity: {self.symbol} - "
                f"Long: {self.long_exchange} ({self.long_funding_rate:.4f}%), "
                f"Short: {self.short_exchange} ({self.short_funding_rate:.4f}%), "
                f"NFD: {self.net_funding_differential:.4f}%, "
                f"ExpProfit: ${self.expected_profit:.2f}, "
                f"Utility: {self.utility_score:.4f}")


class SignalGenerator:
    """
    Identifies funding rate arbitrage opportunities between exchanges.
    
    Responsible for:
    - Monitoring funding rates across exchanges
    - Calculating Net Funding Differential (NFD)
    - Computing expected profit metrics including costs
    - Generating and ranking arbitrage opportunities
    """
    
    def __init__(self, config: Config, data_handler: DataHandler):
        """
        Initialize the signal generator.
        
        Args:
            config: Application configuration
            data_handler: DataHandler for market data
        """
        self.config = config
        self.data_handler = data_handler
        
        # Parameters from config
        self.min_funding_differential = config.get(
            'strategy.funding_rate.min_funding_differential', 0.0001)
        self.min_profit_threshold = config.get(
            'strategy.funding_rate.min_profit_threshold', 5.0)
        
        # Historical funding rates for volatility calculation
        # exchange -> symbol -> List[(timestamp, rate)]
        self.historical_funding_rates: Dict[str, Dict[str, List[Tuple[datetime, float]]]] = {}
        
        # Historical basis data for volatility calculation
        # symbol -> List[(timestamp, basis)]
        self.historical_basis: Dict[str, List[Tuple[datetime, float]]] = {}
        
        # Sample period and count for funding rate history
        self.funding_sample_period = config.get(
            'strategy.funding_rate.funding_sample_period', 3600)  # seconds
        self.funding_sample_count = config.get(
            'strategy.funding_rate.funding_sample_count', 24)
        
        # Risk aversion parameter (λ) for utility function
        self.risk_aversion = config.get('strategy.funding_rate.risk_aversion', 1.0)
        
        # Initialize data structures
        self._initialize_data_structures()
        
    def _initialize_data_structures(self):
        """Initialize data structures for historical data."""
        # Get all configured exchanges and symbols
        exchanges = []
        for exchange_id in self.config.get('exchanges', {}).keys():
            if self.config.get(f'exchanges.{exchange_id}.enabled', False):
                exchanges.append(exchange_id)
        
        # Initialize historical funding rate storage
        for exchange in exchanges:
            self.historical_funding_rates[exchange] = {}
            symbols = self.config.get(f'exchanges.{exchange}.symbols', [])
            for symbol in symbols:
                self.historical_funding_rates[exchange][symbol] = []
                
                # Also initialize basis history for this symbol
                if symbol not in self.historical_basis:
                    self.historical_basis[symbol] = []
    
    def update_historical_data(self):
        """
        Update historical funding rate and basis data with latest information.
        Should be called regularly to maintain up-to-date volatility calculations.
        """
        now = datetime.now()
        
        # Get all configured exchanges and symbols
        exchanges = []
        all_symbols = set()
        
        for exchange_id in self.config.get('exchanges', {}).keys():
            if self.config.get(f'exchanges.{exchange_id}.enabled', False):
                exchanges.append(exchange_id)
                symbols = self.config.get(f'exchanges.{exchange_id}.symbols', [])
                all_symbols.update(symbols)
        
        # Update funding rate history
        for exchange in exchanges:
            for symbol in self.historical_funding_rates[exchange].keys():
                funding_data = self.data_handler.get_funding_rate(exchange, symbol)
                if funding_data:
                    rate, timestamp = funding_data
                    
                    # Add to historical data
                    history = self.historical_funding_rates[exchange][symbol]
                    history.append((timestamp, rate))
                    
                    # Trim to keep only recent samples
                    cutoff_time = now - timedelta(seconds=self.funding_sample_period * self.funding_sample_count)
                    while history and history[0][0] < cutoff_time:
                        history.pop(0)
        
        # Update basis history (price difference between exchanges)
        for symbol in all_symbols:
            # We need at least two exchanges with this symbol
            valid_exchanges = []
            for exchange in exchanges:
                if (exchange in self.historical_funding_rates and 
                    symbol in self.historical_funding_rates[exchange]):
                    valid_exchanges.append(exchange)
            
            if len(valid_exchanges) >= 2:
                # Calculate basis between the first two valid exchanges
                # In a real implementation, we would calculate for all exchange pairs
                exchange1, exchange2 = valid_exchanges[0], valid_exchanges[1]
                
                ticker1 = self.data_handler.get_ticker(exchange1, symbol)
                ticker2 = self.data_handler.get_ticker(exchange2, symbol)
                
                if ticker1 and ticker2:
                    # Calculate basis (price differential)
                    basis = ticker1.close - ticker2.close
                    
                    # Add to historical data
                    self.historical_basis[symbol].append((now, basis))
                    
                    # Trim to keep only recent samples
                    cutoff_time = now - timedelta(seconds=self.funding_sample_period * self.funding_sample_count)
                    while self.historical_basis[symbol] and self.historical_basis[symbol][0][0] < cutoff_time:
                        self.historical_basis[symbol].pop(0)
    
    def calculate_basis_volatility(self, symbol: str) -> float:
        """
        Calculate the standard deviation of the basis for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Basis volatility or 0 if insufficient data
        """
        if symbol not in self.historical_basis or len(self.historical_basis[symbol]) < 2:
            return 0.0
        
        # Extract basis values from historical data
        basis_values = [b[1] for b in self.historical_basis[symbol]]
        
        # Calculate standard deviation
        return np.std(basis_values)
    
    def estimate_slippage(self, symbol: str, order_size: float, exchange: str) -> float:
        """
        Estimate slippage based on order size and liquidity.
        
        Args:
            symbol: Trading symbol
            order_size: Size of the order in USD
            exchange: Exchange identifier
            
        Returns:
            Estimated slippage as a percentage
        """
        # Get orderbook from data handler
        orderbook = self.data_handler.get_orderbook(exchange, symbol)
        if not orderbook:
            # Default to a conservative estimate if no orderbook data
            return 0.001  # 0.1% default slippage
        
        # Extract available liquidity from orderbook
        # This is a simplified approach; real implementation would analyze bid/ask depth
        available_depth = orderbook.get('depth', 100000)  # Default depth
        
        # Calculate slippage with a scaling factor
        scaling_factor = 0.1  # β parameter
        slippage = scaling_factor * (order_size / available_depth)
        
        # Cap the slippage at reasonable limits
        return min(slippage, 0.01)  # Max 1% slippage
    
    def generate_opportunities(self) -> List[ArbitrageOpportunity]:
        """
        Generate a list of arbitrage opportunities sorted by utility score.
        
        Returns:
            List of arbitrage opportunities
        """
        opportunities = []
        now = datetime.now()
        
        # Get all configured exchanges
        exchanges = []
        for exchange_id in self.config.get('exchanges', {}).keys():
            if self.config.get(f'exchanges.{exchange_id}.enabled', False):
                exchanges.append(exchange_id)
        
        # Need at least two exchanges for arbitrage
        if len(exchanges) < 2:
            logger.warning("Fewer than 2 exchanges configured. Arbitrage not possible.")
            return []
        
        # Get common symbols across exchanges
        common_symbols = set()
        for exchange in exchanges:
            symbols = self.config.get(f'exchanges.{exchange}.symbols', [])
            if not common_symbols:
                common_symbols = set(symbols)
            else:
                common_symbols = common_symbols.intersection(symbols)
        
        logger.debug(f"Found {len(common_symbols)} common symbols across all exchanges")
        
        # For each common symbol, check all exchange pairs
        for symbol in common_symbols:
            for i, long_exchange in enumerate(exchanges):
                for short_exchange in exchanges[i+1:]:
                    # Get funding rates
                    long_rate_data = self.data_handler.get_funding_rate(long_exchange, symbol)
                    short_rate_data = self.data_handler.get_funding_rate(short_exchange, symbol)
                    
                    if not long_rate_data or not short_rate_data:
                        continue
                    
                    long_rate, long_timestamp = long_rate_data
                    short_rate, short_timestamp = short_rate_data
                    
                    # Calculate Net Funding Differential (NFD)
                    # For funding rates, positive means longs pay shorts
                    # We want to long the exchange with the negative funding rate (getting paid)
                    # and short the exchange with the positive funding rate (also getting paid)
                    nfd = short_rate - long_rate
                    
                    # Decide which direction to take based on funding rates
                    # If long_rate < short_rate, keep as is
                    # If long_rate > short_rate, swap roles
                    if long_rate > short_rate:
                        long_exchange, short_exchange = short_exchange, long_exchange
                        long_rate, short_rate = short_rate, long_rate
                        nfd = abs(nfd)
                    
                    # Check if NFD exceeds minimum threshold
                    if abs(nfd) < self.min_funding_differential:
                        continue
                    
                    # Calculate basis volatility
                    basis_volatility = self.calculate_basis_volatility(symbol)
                    
                    # Calculate expected profit
                    # Assume a standard position size for now; risk manager will size appropriately
                    position_size = 1000.0  # $1000 USD for calculation purposes
                    
                    # Estimate trading costs
                    long_slippage = self.estimate_slippage(symbol, position_size, long_exchange)
                    short_slippage = self.estimate_slippage(symbol, position_size, short_exchange)
                    
                    # Get exchange fee rates from config
                    long_fee_rate = self.config.get(f'exchanges.{long_exchange}.fee_rate', 0.0005)
                    short_fee_rate = self.config.get(f'exchanges.{short_exchange}.fee_rate', 0.0005)
                    
                    # Calculate total costs
                    total_costs = (position_size * (long_slippage + short_slippage + 
                                                   long_fee_rate + short_fee_rate))
                    
                    # Calculate expected profit
                    # Need to convert NFD to decimal (from percentage)
                    expected_profit = (position_size * (nfd / 100)) - total_costs
                    
                    # Skip if expected profit is below threshold
                    if expected_profit < self.min_profit_threshold:
                        continue
                    
                    # Calculate utility score using risk-adjusted formula
                    # U = ExpectedProfit - λ * σ[B]²
                    # where λ is the risk aversion parameter and σ[B] is basis volatility
                    utility_score = expected_profit - (self.risk_aversion * (basis_volatility ** 2))
                    
                    # Create opportunity object
                    opportunity = ArbitrageOpportunity(
                        symbol=symbol,
                        long_exchange=long_exchange,
                        short_exchange=short_exchange,
                        long_funding_rate=long_rate,
                        short_funding_rate=short_rate,
                        net_funding_differential=nfd,
                        timestamp=now,
                        expected_profit=expected_profit,
                        utility_score=utility_score,
                        basis_volatility=basis_volatility
                    )
                    
                    opportunities.append(opportunity)
                    logger.debug(f"Found opportunity: {opportunity}")
        
        # Sort opportunities by utility score (descending)
        opportunities.sort(key=lambda o: o.utility_score, reverse=True)
        
        if opportunities:
            logger.info(f"Generated {len(opportunities)} arbitrage opportunities")
            for i, opp in enumerate(opportunities[:3]):
                logger.info(f"Top opportunity {i+1}: {opp}")
        else:
            logger.info("No arbitrage opportunities found")
        
        return opportunities
