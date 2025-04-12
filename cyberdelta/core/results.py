"""
Results handler for backtesting.

This module provides tools for processing, analyzing, and saving backtest results.
"""

import json
import logging
import os
import pathlib
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class BacktestResultsHandler:
    """Handles processing, analysis, and saving of backtest results."""

    def __init__(
        self, 
        strategy_name: str, 
        initial_capital: Decimal,
        results_dir: str = "backtest_results",
    ) -> None:
        """
        Initialize the results handler.

        Args:
            strategy_name: Name of the strategy
            initial_capital: Initial capital for the backtest
            results_dir: Directory to save results
        """
        self.strategy_name = strategy_name
        self.initial_capital = initial_capital
        self.results_dir = results_dir
        
        # Create results directory if it doesn't exist
        pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)
        
        # Containers for results
        self.trades: List[Dict[str, Any]] = []
        self.positions: List[Dict[str, Any]] = []
        self.equity_curve: List[Dict[str, Any]] = []
        self.metrics: Dict[str, Any] = {}
        
        # Performance metrics
        self.returns_series: Optional[pd.Series] = None
    
    def add_trade(self, trade: Dict[str, Any]) -> None:
        """
        Add a trade to the results.

        Args:
            trade: Trade data dictionary
        """
        self.trades.append(trade)
    
    def add_position(self, position: Dict[str, Any]) -> None:
        """
        Add a position to the results.

        Args:
            position: Position data dictionary
        """
        self.positions.append(position)
    
    def add_equity_point(self, timestamp: datetime, equity: Decimal) -> None:
        """
        Add a point to the equity curve.

        Args:
            timestamp: Point timestamp
            equity: Equity value
        """
        self.equity_curve.append({
            "timestamp": timestamp,
            "equity": float(equity)  # Convert to float for JSON serialization
        })
    
    def calculate_returns(self) -> pd.Series:
        """
        Calculate returns series from equity curve.

        Returns:
            Series of period returns
        """
        if not self.equity_curve:
            return pd.Series()
            
        # Convert equity curve to DataFrame
        df = pd.DataFrame(self.equity_curve)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        
        # Calculate returns
        self.returns_series = df["equity"].pct_change().dropna()
        return self.returns_series
    
    def calculate_metrics(self) -> Dict[str, Any]:
        """
        Calculate performance metrics.

        Returns:
            Dictionary of performance metrics
        """
        if not self.returns_series:
            self.calculate_returns()
            
        if self.returns_series is None or len(self.returns_series) == 0:
            logger.warning("No returns data available to calculate metrics")
            self.metrics = {
                "total_trades": len(self.trades),
                "winning_trades": sum(1 for t in self.trades if t.get("pnl", 0) > 0),
                "total_return": 0.0,
                "annualized_return": 0.0,
                "sharpe_ratio": 0.0,
                "max_drawdown": 0.0,
            }
            return self.metrics
            
        # Calculate basic metrics
        self.metrics = {
            "total_trades": len(self.trades),
            "winning_trades": sum(1 for t in self.trades if t.get("pnl", 0) > 0),
            "losing_trades": sum(1 for t in self.trades if t.get("pnl", 0) <= 0),
            "win_rate": (sum(1 for t in self.trades if t.get("pnl", 0) > 0) / len(self.trades) * 100) if self.trades else 0.0,
        }
        
        # Calculate returns metrics
        total_return = ((1 + self.returns_series).prod() - 1) * 100  # as percentage
        annualized_return = ((1 + total_return / 100) ** (252 / len(self.returns_series)) - 1) * 100
        volatility = self.returns_series.std() * np.sqrt(252) * 100  # annualized, as percentage
        
        # Calculate drawdown
        cum_returns = (1 + self.returns_series).cumprod()
        running_max = cum_returns.cummax()
        drawdown = (cum_returns / running_max - 1) * 100  # as percentage
        max_drawdown = abs(drawdown.min())
        
        # Sharpe ratio (assuming risk-free rate of 0)
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0.0
        
        # Add more metrics
        self.metrics.update({
            "total_return": float(total_return),
            "annualized_return": float(annualized_return),
            "annualized_volatility": float(volatility),
            "sharpe_ratio": float(sharpe_ratio),
            "max_drawdown": float(max_drawdown),
            "num_trades": len(self.trades),
        })
        
        # Calculate additional trade metrics if we have trades
        if self.trades:
            pnl_values = [t.get("pnl", 0) for t in self.trades]
            winning_pnl = [p for p in pnl_values if p > 0]
            losing_pnl = [p for p in pnl_values if p <= 0]
            
            # Calculate averages
            avg_win = np.mean(winning_pnl) if winning_pnl else 0.0
            avg_loss = np.mean(losing_pnl) if losing_pnl else 0.0
            
            # Calculate profit factor
            total_profit = sum(winning_pnl)
            total_loss = abs(sum(losing_pnl))
            profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
            
            self.metrics.update({
                "avg_win": float(avg_win),
                "avg_loss": float(avg_loss),
                "profit_factor": float(profit_factor),
                "total_profit": float(total_profit),
                "total_loss": float(total_loss),
            })
        
        return self.metrics
    
    def save_results(self, filename: Optional[str] = None) -> str:
        """
        Save results to JSON file.

        Args:
            filename: Optional custom filename

        Returns:
            Path to saved file
        """
        # Calculate metrics if not already calculated
        if not self.metrics:
            self.calculate_metrics()
            
        # Create results object
        results = {
            "strategy_name": self.strategy_name,
            "initial_capital": float(self.initial_capital),
            "timestamp": datetime.now().isoformat(),
            "metrics": self.metrics,
            "trades": self.trades,
            "positions": self.positions,
            "equity_curve": self.equity_curve,
        }
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.strategy_name}_{timestamp}.json"
            
        # Ensure path is within results directory
        filepath = os.path.join(self.results_dir, filename)
        
        # Save to file
        try:
            with open(filepath, "w") as f:
                json.dump(results, f, indent=2)
            logger.info(f"Saved backtest results to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
            raise
    
    def format_results_for_output(self) -> Dict[str, Any]:
        """
        Format results for output.

        Returns:
            Dictionary with formatted results
        """
        # Calculate metrics if not already calculated
        if not self.metrics:
            self.calculate_metrics()
            
        # Format final equity
        final_equity = self.equity_curve[-1]["equity"] if self.equity_curve else float(self.initial_capital)
            
        # Create summary dictionary
        return {
            "success": True,
            "strategy_name": self.strategy_name,
            "initial_capital": float(self.initial_capital),
            "final_equity": final_equity,
            "total_return_pct": ((final_equity / float(self.initial_capital)) - 1) * 100,
            "metrics": self.metrics,
            "num_trades": len(self.trades),
        } 