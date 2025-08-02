"""Performance calculation component."""
from __future__ import annotations

from datetime import datetime, UTC
from decimal import Decimal
from typing import Optional, List

from cyberdelta.core.analytics.performance import PerformanceSnapshot
from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.enums.trading import OrderSide

logger = get_logger(__name__)


class PerformanceCalculator:
    """Calculator for portfolio performance metrics."""
    
    def __init__(self, portfolio_manager: PortfolioStateManager) -> None:
        """Initialize performance calculator.
        
        Args:
            portfolio_manager: Portfolio state manager for accessing portfolio data
        """
        self.portfolio_manager = portfolio_manager
        self._initialized = False
        self._historical_values: List[Decimal] = []
        self._historical_returns: List[Decimal] = []
        
    async def initialize(self) -> None:
        """Initialize the performance calculator."""
        if self._initialized:
            return
            
        logger.info("Initializing performance calculator")
        self._initialized = True
        
    async def shutdown(self) -> None:
        """Shutdown the performance calculator."""
        if not self._initialized:
            return
            
        logger.info("Shutting down performance calculator")
        self._initialized = False
        
    async def calculate_snapshot(self, portfolio_state: Optional[PortfolioState] = None) -> PerformanceSnapshot:
        """Calculate a performance snapshot for the current portfolio state.
        
        Args:
            portfolio_state: Optional portfolio state, fetches current if not provided
            
        Returns:
            PerformanceSnapshot with current metrics
        """
        if not portfolio_state:
            portfolio_state = await self.portfolio_manager.get_portfolio_summary()
            
        # Calculate total value
        total_value = await self._calculate_total_value(portfolio_state)
        
        # Update historical tracking
        self._historical_values.append(total_value)
        if len(self._historical_values) > 252:  # Keep 1 year of daily data
            self._historical_values.pop(0)
            
        # Calculate P&L metrics
        daily_pnl = await self._calculate_daily_pnl(total_value)
        cumulative_pnl = await self._calculate_cumulative_pnl(total_value)
        realized_pnl = await self._calculate_realized_pnl(portfolio_state)
        unrealized_pnl = await self._calculate_unrealized_pnl(portfolio_state)
        
        # Calculate performance metrics
        win_rate = await self._calculate_win_rate(portfolio_state)
        sharpe_ratio = await self._calculate_sharpe_ratio()
        max_drawdown = await self._calculate_max_drawdown()
        
        # Count positions from active_positions field
        positions_count = portfolio_state.active_positions
        
        return PerformanceSnapshot(
            timestamp=datetime.now(UTC),
            total_value=total_value,
            daily_pnl=daily_pnl,
            cumulative_pnl=cumulative_pnl,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            win_rate=win_rate,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            positions_count=positions_count
        )
        
    async def _calculate_total_value(self, portfolio_state: PortfolioState) -> Decimal:
        """Calculate total portfolio value."""
        total = Decimal("0")
        
        # Add balance values - balances is flat dict in models.portfolio_state
        for balance in portfolio_state.balances.values():
            if hasattr(balance, 'available_quantity'):
                # Simplified - would need price data for non-USDC assets
                if balance.asset.value == "USDC":
                    total += Decimal(str(balance.available_quantity))
                        
        # Note: Position values would need to be calculated from a different source
        # as models.portfolio_state.PortfolioState doesn't have positions field
        # Using total_account_value field instead
        total = portfolio_state.total_account_value
                        
        return total
        
    async def _calculate_daily_pnl(self, current_value: Decimal) -> Decimal:
        """Calculate daily P&L."""
        if len(self._historical_values) < 2:
            return Decimal("0")
            
        previous_value = self._historical_values[-2]
        return current_value - previous_value
        
    async def _calculate_cumulative_pnl(self, current_value: Decimal) -> Decimal:
        """Calculate cumulative P&L from inception."""
        if not self._historical_values:
            return Decimal("0")
            
        initial_value = self._historical_values[0]
        return current_value - initial_value
        
    async def _calculate_realized_pnl(self, portfolio_state: PortfolioState) -> Decimal:
        """Calculate realized P&L from closed positions."""
        # Get trade history from trade manager if available
        try:
            from cyberdelta.core.portfolio.managers.trade_manager import TradeManager
            if hasattr(self.portfolio_manager, 'trade_manager') and isinstance(self.portfolio_manager.trade_manager, TradeManager):
                trades = await self.portfolio_manager.trade_manager.get_state()
                
                # Calculate realized P&L from trades
                realized_pnl = Decimal("0")
                
                # Group trades by symbol to track position lifecycle
                symbol_trades: dict[Symbol, list[Trade]] = {}
                for trade in trades:
                    if trade.symbol not in symbol_trades:
                        symbol_trades[trade.symbol] = []
                    symbol_trades[trade.symbol].append(trade)
                
                # Calculate P&L for each symbol
                for symbol, symbol_trade_list in symbol_trades.items():
                    # Sort trades by execution time
                    sorted_trades = sorted(symbol_trade_list, key=lambda t: t.executed_at)
                    
                    # Track position and calculate realized P&L using FIFO
                    position_size = Decimal("0")
                    position_cost = Decimal("0")
                    
                    for trade in sorted_trades:
                        trade_size = trade.quantity
                        trade_value = trade.price * trade.quantity
                        
                        if trade.side == OrderSide.BUY:
                            # Adding to position
                            position_cost += trade_value + trade.fee
                            position_size += trade_size
                        else:  # sell
                            # Closing position (partially or fully)
                            if position_size > 0:
                                # Calculate average entry price
                                avg_entry_price = position_cost / position_size if position_size > 0 else Decimal("0")
                                
                                # Calculate P&L for this sale
                                sale_proceeds = trade_value - trade.fee
                                sale_cost = avg_entry_price * trade_size
                                trade_pnl = sale_proceeds - sale_cost
                                realized_pnl += trade_pnl
                                
                                # Update position
                                position_size -= trade_size
                                if position_size > 0:
                                    position_cost = avg_entry_price * position_size
                                else:
                                    position_cost = Decimal("0")
                
                return realized_pnl
                
        except Exception as e:
            logger.warning(f"Failed to calculate realized P&L from trades: {e}")
        
        # Fallback: calculate from position history if available
        return Decimal("0")
        
    async def _calculate_unrealized_pnl(self, portfolio_state: PortfolioState) -> Decimal:
        """Calculate unrealized P&L from open positions."""
        # Use the total_unrealized_pnl field from PortfolioState
        return portfolio_state.total_unrealized_pnl
        
    async def _calculate_win_rate(self, portfolio_state: PortfolioState) -> Decimal:
        """Calculate win rate from trade history."""
        try:
            from cyberdelta.core.portfolio.managers.trade_manager import TradeManager
            if hasattr(self.portfolio_manager, 'trade_manager') and isinstance(self.portfolio_manager.trade_manager, TradeManager):
                trades = await self.portfolio_manager.trade_manager.get_state()
                
                if not trades:
                    return Decimal("0")
                
                # Calculate win rate from closed positions
                closed_positions: dict[Symbol, dict[str, Decimal | int]] = {}
                
                # Group trades by symbol and track positions
                for trade in sorted(trades, key=lambda t: t.executed_at):
                    symbol = trade.symbol
                    
                    if symbol not in closed_positions:
                        closed_positions[symbol] = {
                            'position_size': Decimal("0"),
                            'total_cost': Decimal("0"),
                            'realized_pnl': Decimal("0"),
                            'trades': 0,
                            'winning_trades': 0
                        }
                    
                    pos = closed_positions[symbol]
                    
                    if trade.side == OrderSide.BUY:
                        # Opening/adding to position
                        pos['total_cost'] += trade.price * trade.quantity + trade.fee
                        pos['position_size'] += trade.quantity
                    else:  # sell
                        # Closing position
                        if pos['position_size'] > 0:
                            avg_entry = pos['total_cost'] / pos['position_size']
                            trade_pnl = (trade.price - avg_entry) * trade.quantity - trade.fee
                            
                            pos['trades'] += 1
                            if trade_pnl > 0:
                                pos['winning_trades'] += 1
                            
                            # Update position
                            pos['position_size'] -= trade.quantity
                            if pos['position_size'] > 0:
                                pos['total_cost'] = avg_entry * pos['position_size']
                            else:
                                pos['total_cost'] = Decimal("0")
                
                # Calculate overall win rate
                total_trades = sum(pos['trades'] for pos in closed_positions.values())
                total_wins = sum(pos['winning_trades'] for pos in closed_positions.values())
                
                if total_trades > 0:
                    return Decimal(str(total_wins)) / Decimal(str(total_trades))
                    
        except Exception as e:
            logger.warning(f"Failed to calculate win rate from trades: {e}")
        
        # Return 0 if no trades or error
        return Decimal("0")
        
    async def _calculate_sharpe_ratio(self) -> Decimal:
        """Calculate Sharpe ratio."""
        if len(self._historical_values) < 2:
            return Decimal("0")
            
        # Calculate returns
        returns = []
        for i in range(1, len(self._historical_values)):
            prev_val = self._historical_values[i-1]
            curr_val = self._historical_values[i]
            if prev_val > 0:
                ret = (curr_val - prev_val) / prev_val
                returns.append(ret)
                
        if not returns:
            return Decimal("0")
            
        # Calculate average return
        avg_return = sum(returns) / len(returns)
        
        # Calculate standard deviation
        variance = sum((r - avg_return) ** 2 for r in returns) / len(returns)
        std_dev = variance.sqrt() if variance > 0 else Decimal("0")
        
        # Annualize (assuming daily data)
        annual_return = avg_return * Decimal("252")
        annual_std = std_dev * Decimal("252").sqrt()
        
        # Risk-free rate (2% annual)
        risk_free = Decimal("0.02")
        
        # Sharpe ratio
        if annual_std > 0:
            return (annual_return - risk_free) / annual_std
        else:
            return Decimal("0")
            
    async def _calculate_max_drawdown(self) -> Decimal:
        """Calculate maximum drawdown."""
        if len(self._historical_values) < 2:
            return Decimal("0")
            
        peak = self._historical_values[0]
        max_dd = Decimal("0")
        
        for value in self._historical_values[1:]:
            if value > peak:
                peak = value
            else:
                drawdown = (peak - value) / peak if peak > 0 else Decimal("0")
                max_dd = max(max_dd, drawdown)
                
        return max_dd