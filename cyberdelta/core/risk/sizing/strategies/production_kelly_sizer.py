"""Production Kelly criterion sizer using real trade history and market data."""

from decimal import Decimal
from typing import Any, Optional
from datetime import datetime, timedelta, UTC
import numpy as np

from cyberdelta.config import AppSettings
from cyberdelta.core.risk.sizing.strategies.kelly_criterion_sizer import KellyCriterionSizer
from cyberdelta.core.risk.sizing.models.sizing_result import SizingContext
from cyberdelta.core.risk.exceptions.sizing_exceptions import KellyCalculationError
from cyberdelta.validation.funding_data import ArbitrageOpportunity
from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState
from cyberdelta.core.portfolio.config.risk_parameters import MarketDataProvider
from cyberdelta.config.structlog_config import get_logger


class ProductionKellySizer(KellyCriterionSizer):
    """Production Kelly sizer with real trade history analysis.
    
    Extends the base Kelly sizer to use:
    - Actual trade history for win/loss rates
    - Real market volatility data
    - Dynamic parameter adjustment based on performance
    - Strategy-specific Kelly calculations
    """
    
    def __init__(
        self, 
        app_settings: AppSettings,
        portfolio_state: Optional[PortfolioState] = None,
        market_data_provider: Optional[MarketDataProvider] = None
    ) -> None:
        """Initialize production Kelly sizer.
        
        Args:
            app_settings: Application settings
            portfolio_state: Current portfolio state for trade history
            market_data_provider: Provider for real market data
        """
        super().__init__(app_settings)
        
        self.portfolio_state = portfolio_state
        self.market_data_provider = market_data_provider
        self.logger = get_logger(self.__class__.__name__)
        
        # Trade history parameters
        self.history_lookback_days = 30  # Days to analyze for win/loss rates
        self.min_trades_required = 10    # Minimum trades for reliable statistics
        
        # Performance tracking
        self._win_rate_cache: dict[str, Decimal] = {}
        self._avg_win_cache: dict[str, Decimal] = {}
        self._avg_loss_cache: dict[str, Decimal] = {}
        self._last_cache_update: Optional[datetime] = None
        
    async def _calculate_base_size(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> Decimal:
        """Calculate position size using real trade history Kelly criterion.
        
        Args:
            opportunity: The arbitrage opportunity to size
            context: Context information for sizing
            
        Returns:
            Base position size in USD
        """
        # Get strategy identifier
        strategy_id = getattr(opportunity, "strategy_id", "default")
        
        # Calculate Kelly parameters from real trade history
        win_probability, avg_win, avg_loss = await self._calculate_kelly_parameters_from_history(
            strategy_id, 
            opportunity
        )
        
        # Get real market volatility
        volatility = await self._get_real_volatility(opportunity, context)
        
        # Calculate Kelly fraction using the binary outcome formula
        if avg_loss > 0:
            kelly_fraction = self.calculate_kelly_with_win_probability(
                win_probability, 
                avg_win, 
                avg_loss
            )
        else:
            # Fallback to standard Kelly if no loss data
            expected_return = avg_win * win_probability
            kelly_fraction = self._calculate_kelly_fraction(expected_return, volatility)
        
        # Apply dynamic Kelly multiplier based on recent performance
        dynamic_multiplier = await self._calculate_dynamic_multiplier(strategy_id)
        adjusted_kelly = kelly_fraction * dynamic_multiplier
        
        # Apply bounds
        bounded_kelly = self._apply_kelly_bounds(adjusted_kelly)
        
        # Calculate position size
        position_size = context.available_capital * bounded_kelly
        
        # Store detailed calculation metadata
        context.add_metadata("strategy_id", strategy_id)
        context.add_metadata("win_probability", float(win_probability))
        context.add_metadata("avg_win", float(avg_win))
        context.add_metadata("avg_loss", float(avg_loss))
        context.add_metadata("volatility", float(volatility))
        context.add_metadata("kelly_fraction", float(kelly_fraction))
        context.add_metadata("dynamic_multiplier", float(dynamic_multiplier))
        context.add_metadata("adjusted_kelly", float(adjusted_kelly))
        context.add_metadata("bounded_kelly", float(bounded_kelly))
        context.add_metadata("trade_history_size", await self._get_trade_count(strategy_id))
        
        return position_size
    
    async def _calculate_kelly_parameters_from_history(
        self, 
        strategy_id: str, 
        opportunity: ArbitrageOpportunity
    ) -> tuple[Decimal, Decimal, Decimal]:
        """Calculate Kelly parameters from real trade history.
        
        Args:
            strategy_id: Strategy identifier
            opportunity: Current opportunity
            
        Returns:
            Tuple of (win_probability, avg_win, avg_loss)
        """
        # Check cache
        if self._is_cache_valid():
            if strategy_id in self._win_rate_cache:
                return (
                    self._win_rate_cache[strategy_id],
                    self._avg_win_cache.get(strategy_id, Decimal("0.03")),
                    self._avg_loss_cache.get(strategy_id, Decimal("0.02"))
                )
        
        # Get trade history
        trades = await self._get_trade_history(strategy_id)
        
        if len(trades) < self.min_trades_required:
            # Not enough history - use conservative defaults
            self.logger.warning(
                "insufficient_trade_history",
                strategy_id=strategy_id,
                trade_count=len(trades),
                min_required=self.min_trades_required
            )
            return self._get_default_kelly_parameters(opportunity)
        
        # Calculate statistics from trade history
        wins = []
        losses = []
        
        for trade in trades:
            pnl = trade.get("pnl", 0)
            if pnl > 0:
                wins.append(float(pnl))
            elif pnl < 0:
                losses.append(abs(float(pnl)))
        
        # Calculate parameters
        total_trades = len(trades)
        win_count = len(wins)
        
        win_probability = Decimal(str(win_count / total_trades)) if total_trades > 0 else Decimal("0.5")
        avg_win = Decimal(str(np.mean(wins))) if wins else Decimal("0.03")
        avg_loss = Decimal(str(np.mean(losses))) if losses else Decimal("0.02")
        
        # Normalize to percentages if needed
        if avg_win > 1:
            avg_win = avg_win / Decimal("100")
        if avg_loss > 1:
            avg_loss = avg_loss / Decimal("100")
        
        # Update cache
        self._win_rate_cache[strategy_id] = win_probability
        self._avg_win_cache[strategy_id] = avg_win
        self._avg_loss_cache[strategy_id] = avg_loss
        self._last_cache_update = datetime.now(UTC)
        
        return win_probability, avg_win, avg_loss
    
    async def _get_real_volatility(self, opportunity: ArbitrageOpportunity, context: SizingContext) -> Decimal:
        """Get real market volatility for the symbol.
        
        Args:
            opportunity: Trading opportunity
            context: Sizing context for volatility calculation
            
        Returns:
            Real volatility from market data
        """
        if not self.market_data_provider:
            # Fallback to parent's volatility calculation which uses opportunity data
            return await super()._calculate_volatility(opportunity, context)
        
        # Get symbol from opportunity
        symbol = getattr(opportunity, "symbol", None)
        if not symbol:
            # Try to construct from long/short symbols
            long_symbol = getattr(opportunity, "long_symbol", None)
            short_symbol = getattr(opportunity, "short_symbol", None)
            if long_symbol:
                symbol = long_symbol
            elif short_symbol:
                symbol = short_symbol
            else:
                return self._min_volatility * 10  # Conservative default
        
        try:
            # Get real volatility from market data provider
            volatility = await self.market_data_provider.get_symbol_volatility(symbol)
            
            # Ensure it's within bounds
            return min(max(volatility, self._min_volatility), self._max_volatility)
            
        except Exception as e:
            self.logger.error(
                "volatility_fetch_failed",
                symbol=symbol,
                error=str(e)
            )
            return self._min_volatility * 10  # Conservative default
    
    async def _calculate_dynamic_multiplier(self, strategy_id: str) -> Decimal:
        """Calculate dynamic Kelly multiplier based on recent performance.
        
        Args:
            strategy_id: Strategy identifier
            
        Returns:
            Dynamic multiplier (0.5 to 1.0)
        """
        # Get recent performance metrics
        recent_trades = await self._get_recent_trades(strategy_id, days=7)
        
        if len(recent_trades) < 3:
            # Not enough recent trades - use base multiplier
            return self._kelly_multiplier
        
        # Calculate recent win rate
        recent_wins = sum(1 for t in recent_trades if t.get("pnl", 0) > 0)
        recent_win_rate = recent_wins / len(recent_trades)
        
        # Calculate drawdown
        cumulative_pnl = 0
        max_pnl = 0
        max_drawdown = 0
        
        for trade in recent_trades:
            cumulative_pnl += trade.get("pnl", 0)
            max_pnl = max(max_pnl, cumulative_pnl)
            drawdown = (max_pnl - cumulative_pnl) / max_pnl if max_pnl > 0 else 0
            max_drawdown = max(max_drawdown, drawdown)
        
        # Adjust multiplier based on performance
        base_multiplier = self._kelly_multiplier
        
        # Reduce for poor win rate
        if recent_win_rate < 0.4:
            base_multiplier *= Decimal("0.7")
        elif recent_win_rate < 0.5:
            base_multiplier *= Decimal("0.85")
        
        # Reduce for high drawdown
        if max_drawdown > 0.2:
            base_multiplier *= Decimal("0.6")
        elif max_drawdown > 0.1:
            base_multiplier *= Decimal("0.8")
        
        # Ensure minimum multiplier
        return max(base_multiplier, Decimal("0.5"))
    
    async def _get_trade_history(self, strategy_id: str) -> list[dict[str, Any]]:
        """Get trade history for strategy.
        
        Args:
            strategy_id: Strategy identifier
            
        Returns:
            List of historical trades
        """
        if not self.portfolio_state:
            return []
        
        # Get closed positions from portfolio state
        # This is a simplified implementation - in production would query trade database
        trades = []
        
        # Check if portfolio state has trade history
        if hasattr(self.portfolio_state, 'trade_history'):
            all_trades = self.portfolio_state.trade_history
            
            # Filter by strategy and time
            cutoff_date = datetime.now(UTC) - timedelta(days=self.history_lookback_days)
            
            for trade in all_trades:
                if (trade.get("strategy_id") == strategy_id and 
                    trade.get("closed_at", datetime.now(UTC)) > cutoff_date):
                    trades.append(trade)
        
        return trades
    
    async def _get_recent_trades(self, strategy_id: str, days: int = 7) -> list[dict[str, Any]]:
        """Get recent trades for performance analysis.
        
        Args:
            strategy_id: Strategy identifier
            days: Number of days to look back
            
        Returns:
            List of recent trades
        """
        if not self.portfolio_state:
            return []
        
        trades = []
        cutoff_date = datetime.now(UTC) - timedelta(days=days)
        
        # Similar to _get_trade_history but with shorter timeframe
        if hasattr(self.portfolio_state, 'trade_history'):
            for trade in self.portfolio_state.trade_history:
                if (trade.get("strategy_id") == strategy_id and 
                    trade.get("closed_at", datetime.now(UTC)) > cutoff_date):
                    trades.append(trade)
        
        return trades
    
    async def _get_trade_count(self, strategy_id: str) -> int:
        """Get total trade count for strategy.
        
        Args:
            strategy_id: Strategy identifier
            
        Returns:
            Number of trades
        """
        trades = await self._get_trade_history(strategy_id)
        return len(trades)
    
    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid.
        
        Returns:
            True if cache is valid
        """
        if not self._last_cache_update:
            return False
        
        # Cache valid for 1 hour
        cache_age = datetime.now(UTC) - self._last_cache_update
        return cache_age.total_seconds() < 3600
    
    def _get_default_kelly_parameters(
        self, 
        opportunity: ArbitrageOpportunity
    ) -> tuple[Decimal, Decimal, Decimal]:
        """Get default Kelly parameters when history is insufficient.
        
        Args:
            opportunity: Current opportunity
            
        Returns:
            Conservative default parameters
        """
        # Conservative defaults based on opportunity type
        strategy_type = getattr(opportunity, "strategy_type", "unknown")
        
        if strategy_type == "arbitrage":
            # Arbitrage typically has higher win rate but smaller gains
            return Decimal("0.7"), Decimal("0.01"), Decimal("0.005")
        elif strategy_type == "momentum":
            # Momentum has moderate win rate and symmetric risk/reward
            return Decimal("0.55"), Decimal("0.03"), Decimal("0.025")
        else:
            # Conservative defaults
            return Decimal("0.5"), Decimal("0.02"), Decimal("0.02")