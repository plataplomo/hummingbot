"""Attribution analysis component for portfolio performance."""
from __future__ import annotations

from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Dict, List, Tuple, Optional

from cyberdelta.core.portfolio.analytics.performance import AttributionResult
from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import Symbol

logger = get_logger(__name__)


class AttributionAnalyzer:
    """Analyzer for performance attribution across different dimensions."""
    
    def __init__(self, portfolio_manager: PortfolioStateManager) -> None:
        """Initialize attribution analyzer.
        
        Args:
            portfolio_manager: Portfolio state manager for accessing portfolio data
        """
        self.portfolio_manager = portfolio_manager
        self._initialized = False
        self._trade_history: List[Dict] = []  # Would be populated from trade service
        
    async def initialize(self) -> None:
        """Initialize the attribution analyzer."""
        if self._initialized:
            return
            
        logger.info("Initializing attribution analyzer")
        self._initialized = True
        
    async def shutdown(self) -> None:
        """Shutdown the attribution analyzer."""
        if not self._initialized:
            return
            
        logger.info("Shutting down attribution analyzer")
        self._initialized = False
        
    async def analyze_attribution(
        self, 
        portfolio_state: PortfolioState, 
        period: timedelta,
        max_depth: int = 3
    ) -> AttributionResult:
        """Analyze performance attribution for specified period.
        
        Args:
            portfolio_state: Current portfolio state
            period: Time period for attribution analysis
            max_depth: Maximum depth of attribution analysis
            
        Returns:
            AttributionResult with P&L attribution across dimensions
        """
        logger.info(f"Analyzing attribution for period: {period}")
        
        # Calculate total P&L for period
        total_pnl = await self._calculate_period_pnl(portfolio_state, period)
        
        # Attribution by exchange
        by_exchange = await self._attribute_by_exchange(portfolio_state, period)
        
        # Attribution by symbol
        by_symbol = await self._attribute_by_symbol(portfolio_state, period)
        
        # Attribution by strategy (from metadata if available)
        by_strategy = await self._attribute_by_strategy(portfolio_state, period)
        
        # Attribution by time bucket
        by_time_bucket = await self._attribute_by_time(portfolio_state, period)
        
        # Find top winners and losers
        top_winners, top_losers = await self._find_top_performers(by_symbol)
        
        return AttributionResult(
            period=period,
            total_pnl=total_pnl,
            by_exchange=by_exchange,
            by_symbol=by_symbol,
            by_strategy=by_strategy,
            by_time_bucket=by_time_bucket,
            top_winners=top_winners,
            top_losers=top_losers
        )
        
    async def _calculate_period_pnl(
        self, 
        portfolio_state: PortfolioState, 
        period: timedelta
    ) -> Decimal:
        """Calculate total P&L for the period."""
        # This would need historical data to calculate properly
        # For now, sum unrealized P&L from positions
        total_pnl = Decimal("0")
        
        for exchange_positions in portfolio_state.positions.values():
            for position in exchange_positions.values():
                if hasattr(position, 'unrealized_pnl') and position.unrealized_pnl:
                    total_pnl += position.unrealized_pnl
                    
        return total_pnl
        
    async def _attribute_by_exchange(
        self, 
        portfolio_state: PortfolioState, 
        period: timedelta
    ) -> Dict[str, Decimal]:
        """Attribute P&L by exchange."""
        attribution = {}
        
        for exchange, positions in portfolio_state.positions.items():
            exchange_pnl = Decimal("0")
            
            for position in positions.values():
                if hasattr(position, 'unrealized_pnl') and position.unrealized_pnl:
                    exchange_pnl += position.unrealized_pnl
                    
            if exchange_pnl != 0:
                attribution[exchange] = exchange_pnl
                
        return attribution
        
    async def _attribute_by_symbol(
        self, 
        portfolio_state: PortfolioState, 
        period: timedelta
    ) -> Dict[Symbol, Decimal]:
        """Attribute P&L by symbol."""
        attribution = {}
        
        for exchange_positions in portfolio_state.positions.values():
            for position in exchange_positions.values():
                if hasattr(position, 'symbol') and hasattr(position, 'unrealized_pnl'):
                    symbol = position.symbol
                    pnl = position.unrealized_pnl or Decimal("0")
                    
                    if symbol in attribution:
                        attribution[symbol] += pnl
                    else:
                        attribution[symbol] = pnl
                        
        return attribution
        
    async def _attribute_by_strategy(
        self, 
        portfolio_state: PortfolioState, 
        period: timedelta
    ) -> Dict[str, Decimal]:
        """Attribute P&L by strategy from position metadata."""
        attribution = {}
        
        # Get trade history to analyze strategy performance
        try:
            from cyberdelta.core.portfolio.managers.trade_manager import TradeManager
            if hasattr(self.portfolio_manager, 'trade_manager') and isinstance(self.portfolio_manager.trade_manager, TradeManager):
                trades = await self.portfolio_manager.trade_manager.get_state()
                
                # Default strategy based on symbol patterns if no metadata
                strategy_mapping = {
                    "momentum": [],
                    "arbitrage": [],
                    "market_making": []
                }
                
                # Group trades by inferred strategy
                for trade in trades:
                    # Infer strategy from symbol/exchange patterns
                    strategy = self._infer_strategy_from_trade(trade)
                    if strategy not in strategy_mapping:
                        strategy_mapping[strategy] = []
                    strategy_mapping[strategy].append(trade)
                
                # Calculate P&L by strategy using FIFO
                for strategy, strategy_trades in strategy_mapping.items():
                    if not strategy_trades:
                        continue
                        
                    strategy_pnl = self._calculate_strategy_pnl(strategy_trades)
                    if strategy_pnl != Decimal("0"):
                        attribution[strategy] = strategy_pnl
                        
        except Exception as e:
            logger.warning(f"Failed to calculate strategy attribution: {e}")
            
        # If no data available, return empty attribution
        return attribution
        
    async def _attribute_by_time(
        self, 
        portfolio_state: PortfolioState, 
        period: timedelta
    ) -> Dict[str, Decimal]:
        """Attribute P&L by time buckets from actual trade history."""
        buckets = {}
        
        try:
            from cyberdelta.core.portfolio.managers.trade_manager import TradeManager
            if hasattr(self.portfolio_manager, 'trade_manager') and isinstance(self.portfolio_manager.trade_manager, TradeManager):
                trades = await self.portfolio_manager.trade_manager.get_state()
                
                if not trades:
                    return buckets
                
                # Filter trades within the period
                now = datetime.now(UTC)
                period_start = now - period
                period_trades = [
                    trade for trade in trades 
                    if trade.executed_at >= period_start
                ]
                
                # Group trades by hour buckets
                hourly_trades = {}
                for trade in period_trades:
                    bucket_key = trade.executed_at.strftime("%Y-%m-%d %H:00")
                    if bucket_key not in hourly_trades:
                        hourly_trades[bucket_key] = []
                    hourly_trades[bucket_key].append(trade)
                
                # Calculate P&L for each bucket
                for bucket_key, bucket_trades in hourly_trades.items():
                    bucket_pnl = self._calculate_strategy_pnl(bucket_trades)
                    if bucket_pnl != Decimal("0"):
                        buckets[bucket_key] = bucket_pnl
                        
        except Exception as e:
            logger.warning(f"Failed to calculate time attribution: {e}")
            
        return buckets
        
    async def _find_top_performers(
        self, 
        by_symbol: Dict[Symbol, Decimal]
    ) -> Tuple[List[Tuple[Symbol, Decimal]], List[Tuple[Symbol, Decimal]]]:
        """Find top winning and losing positions."""
        # Sort by P&L
        sorted_symbols = sorted(
            by_symbol.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        # Get top 5 winners and losers
        winners = [(s, p) for s, p in sorted_symbols if p > 0][:5]
        losers = [(s, p) for s, p in sorted_symbols if p < 0][-5:]
        
        return winners, losers
    
    def _infer_strategy_from_trade(self, trade) -> str:
        """Infer trading strategy from trade characteristics."""
        # Basic strategy inference from symbol patterns
        symbol = trade.symbol.upper()
        
        # Arbitrage: typically involves pairs or cross-exchange trades
        if any(term in symbol for term in ["PERP", "FUT", "-"]):
            return "arbitrage"
            
        # Market making: typically smaller sizes, frequent trades
        # (This is simplified - real implementation would need more context)
        if trade.quantity < Decimal("100"):  # Small trade size
            return "market_making"
            
        # Default to momentum for other trades
        return "momentum"
    
    def _calculate_strategy_pnl(self, trades) -> Decimal:
        """Calculate P&L for a group of trades using FIFO."""
        if not trades:
            return Decimal("0")
            
        # Group by symbol to track positions
        symbol_positions = {}
        total_pnl = Decimal("0")
        
        for trade in sorted(trades, key=lambda t: t.executed_at):
            symbol = trade.symbol
            
            if symbol not in symbol_positions:
                symbol_positions[symbol] = {
                    'size': Decimal("0"),
                    'cost': Decimal("0")
                }
            
            pos = symbol_positions[symbol]
            
            if trade.side == "buy":
                # Adding to position
                pos['cost'] += trade.price * trade.quantity + trade.fee
                pos['size'] += trade.quantity
            else:  # sell
                # Closing position
                if pos['size'] > 0:
                    avg_entry = pos['cost'] / pos['size'] if pos['size'] > 0 else Decimal("0")
                    trade_pnl = (trade.price - avg_entry) * trade.quantity - trade.fee
                    total_pnl += trade_pnl
                    
                    # Update position
                    pos['size'] -= trade.quantity
                    if pos['size'] > 0:
                        pos['cost'] = avg_entry * pos['size']
                    else:
                        pos['cost'] = Decimal("0")
        
        return total_pnl