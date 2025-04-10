"""
Strategy Performance Visualization Tools.

This module provides tools for visualizing and analyzing strategy performance data.
It provides a foundation for building both real-time performance monitoring dashboards
and historical performance analysis tools.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class VisualizationConfig:
    """Configuration for visualization components."""
    theme: str = "light"  # "light" or "dark"
    default_height: int = 600
    default_width: int = 800
    color_palette: List[str] = None
    template: str = "plotly_white"
    show_legend: bool = True
    
    def __post_init__(self):
        if self.color_palette is None:
            self.color_palette = px.colors.qualitative.Plotly
        
        if self.theme == "dark":
            self.template = "plotly_dark"


class PerformanceVisualizer:
    """
    Core class for generating strategy performance visualizations.
    
    This class provides methods for creating various visualizations of strategy
    performance data, including returns, drawdowns, trade analysis, and more.
    """
    
    def __init__(self, config: Optional[VisualizationConfig] = None):
        """
        Initialize the performance visualizer.
        
        Args:
            config: Configuration for visualizations
        """
        self.config = config or VisualizationConfig()
        
    def create_returns_chart(self, 
                             returns_data: pd.DataFrame, 
                             strategy_names: List[str] = None,
                             benchmark_data: pd.DataFrame = None,
                             title: str = "Cumulative Returns",
                             height: int = None,
                             width: int = None) -> go.Figure:
        """
        Create a cumulative returns chart for one or more strategies.
        
        Args:
            returns_data: DataFrame with datetime index and strategy returns as columns
            strategy_names: List of strategy names to include (if None, use all columns)
            benchmark_data: Optional benchmark returns for comparison
            title: Chart title
            height: Chart height
            width: Chart width
            
        Returns:
            Plotly figure object
        """
        height = height or self.config.default_height
        width = width or self.config.default_width
        
        if strategy_names is None:
            strategy_names = returns_data.columns.tolist()
        
        # Calculate cumulative returns
        cum_returns = (1 + returns_data[strategy_names]).cumprod() - 1
        
        # Create figure
        fig = go.Figure()
        
        # Add strategy returns
        for i, strategy in enumerate(strategy_names):
            color = self.config.color_palette[i % len(self.config.color_palette)]
            fig.add_trace(
                go.Scatter(
                    x=cum_returns.index,
                    y=cum_returns[strategy] * 100,  # Convert to percentage
                    mode='lines',
                    name=strategy,
                    line=dict(color=color, width=2),
                )
            )
        
        # Add benchmark if provided
        if benchmark_data is not None:
            cum_benchmark = (1 + benchmark_data).cumprod() - 1
            fig.add_trace(
                go.Scatter(
                    x=cum_benchmark.index,
                    y=cum_benchmark.iloc[:, 0] * 100,  # Convert to percentage
                    mode='lines',
                    name='Benchmark',
                    line=dict(color='gray', width=2, dash='dot'),
                )
            )
        
        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title='Date',
            yaxis_title='Cumulative Return (%)',
            template=self.config.template,
            height=height,
            width=width,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.5)" if self.config.theme == "light" else "rgba(0, 0, 0, 0.5)"
            ),
            hovermode="x unified"
        )
        
        # Add range slider
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        
        return fig
    
    def create_drawdown_chart(self, 
                              returns_data: pd.DataFrame, 
                              strategy_names: List[str] = None,
                              title: str = "Drawdown Analysis",
                              height: int = None,
                              width: int = None) -> go.Figure:
        """
        Create a drawdown chart for one or more strategies.
        
        Args:
            returns_data: DataFrame with datetime index and strategy returns as columns
            strategy_names: List of strategy names to include (if None, use all columns)
            title: Chart title
            height: Chart height
            width: Chart width
            
        Returns:
            Plotly figure object
        """
        height = height or self.config.default_height
        width = width or self.config.default_width
        
        if strategy_names is None:
            strategy_names = returns_data.columns.tolist()
        
        # Calculate drawdowns
        cum_returns = (1 + returns_data[strategy_names]).cumprod()
        rolling_max = cum_returns.cummax()
        drawdowns = (cum_returns / rolling_max - 1) * 100  # Convert to percentage
        
        # Create figure
        fig = go.Figure()
        
        # Add drawdown traces
        for i, strategy in enumerate(strategy_names):
            color = self.config.color_palette[i % len(self.config.color_palette)]
            fig.add_trace(
                go.Scatter(
                    x=drawdowns.index,
                    y=drawdowns[strategy],
                    mode='lines',
                    name=strategy,
                    line=dict(color=color, width=2),
                    fill='tozeroy',
                )
            )
        
        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title='Date',
            yaxis_title='Drawdown (%)',
            template=self.config.template,
            height=height,
            width=width,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.5)" if self.config.theme == "light" else "rgba(0, 0, 0, 0.5)"
            ),
            hovermode="x unified"
        )
        
        # Add range slider
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        
        return fig
    
    def create_trade_analysis_chart(self,
                                   trade_data: pd.DataFrame,
                                   title: str = "Trade Analysis",
                                   height: int = None,
                                   width: int = None) -> go.Figure:
        """
        Create a scatter plot of trades showing PnL vs duration.
        
        Args:
            trade_data: DataFrame with trade information
                Must include columns: 'duration', 'pnl', 'exit_time'
            title: Chart title
            height: Chart height
            width: Chart width
            
        Returns:
            Plotly figure object
        """
        height = height or self.config.default_height
        width = width or self.config.default_width
        
        # Create figure
        fig = go.Figure()
        
        # Split data into profitable and losing trades
        profitable = trade_data[trade_data['pnl'] > 0]
        losing = trade_data[trade_data['pnl'] <= 0]
        
        # Add profitable trades
        if not profitable.empty:
            size = np.sqrt(profitable['pnl'].abs()) * 5  # Scale marker size
            fig.add_trace(
                go.Scatter(
                    x=profitable['duration'],
                    y=profitable['pnl'],
                    mode='markers',
                    name='Profitable Trades',
                    marker=dict(
                        color='green',
                        size=size,
                        opacity=0.7,
                        line=dict(width=1, color='darkgreen')
                    ),
                    hovertext=profitable.apply(
                        lambda row: f"Time: {row['exit_time']}<br>PnL: ${row['pnl']:.2f}<br>Duration: {row['duration']} min",
                        axis=1
                    ),
                    hoverinfo='text'
                )
            )
        
        # Add losing trades
        if not losing.empty:
            size = np.sqrt(losing['pnl'].abs()) * 5  # Scale marker size
            fig.add_trace(
                go.Scatter(
                    x=losing['duration'],
                    y=losing['pnl'],
                    mode='markers',
                    name='Losing Trades',
                    marker=dict(
                        color='red',
                        size=size,
                        opacity=0.7,
                        line=dict(width=1, color='darkred')
                    ),
                    hovertext=losing.apply(
                        lambda row: f"Time: {row['exit_time']}<br>PnL: ${row['pnl']:.2f}<br>Duration: {row['duration']} min",
                        axis=1
                    ),
                    hoverinfo='text'
                )
            )
        
        # Add horizontal line at zero
        fig.add_shape(
            type="line",
            x0=0,
            y0=0,
            x1=1,
            y1=0,
            xref="paper",
            line=dict(
                color="gray",
                width=1,
                dash="dash",
            )
        )
        
        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title='Trade Duration (minutes)',
            yaxis_title='Trade PnL ($)',
            template=self.config.template,
            height=height,
            width=width,
            hovermode='closest'
        )
        
        return fig
    
    def create_funding_rate_heatmap(self,
                                   funding_data: pd.DataFrame,
                                   title: str = "Funding Rate Heatmap",
                                   height: int = None,
                                   width: int = None) -> go.Figure:
        """
        Create a heatmap of funding rates across assets and time.
        
        Args:
            funding_data: DataFrame with datetime index, assets as columns, funding rates as values
            title: Chart title
            height: Chart height
            width: Chart width
            
        Returns:
            Plotly figure object
        """
        height = height or self.config.default_height
        width = width or self.config.default_width
        
        # Pivot data if necessary (if not already in the right format)
        if 'asset' in funding_data.columns and 'funding_rate' in funding_data.columns:
            pivot_data = funding_data.pivot(index=funding_data.index, columns='asset', values='funding_rate')
        else:
            pivot_data = funding_data
            
        # Create figure
        fig = go.Figure(data=go.Heatmap(
            z=pivot_data.values.T,
            x=pivot_data.index,
            y=pivot_data.columns,
            colorscale='RdBu',
            zmid=0,  # Center colorscale at zero
            colorbar=dict(
                title=dict(
                    text='Funding Rate (%)',
                    side='right'
                )
            )
        ))
        
        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title='Date',
            yaxis_title='Asset',
            template=self.config.template,
            height=height,
            width=width
        )
        
        return fig
    
    def create_performance_dashboard(self,
                                   returns_data: pd.DataFrame,
                                   trade_data: pd.DataFrame = None,
                                   funding_data: pd.DataFrame = None,
                                   strategy_names: List[str] = None,
                                   benchmark_data: pd.DataFrame = None,
                                   height: int = None,
                                   width: int = None) -> go.Figure:
        """
        Create a comprehensive performance dashboard with multiple charts.
        
        Args:
            returns_data: DataFrame with datetime index and strategy returns as columns
            trade_data: DataFrame with trade information
            funding_data: DataFrame with funding rate information
            strategy_names: List of strategy names to include
            benchmark_data: Optional benchmark returns for comparison
            height: Dashboard height
            width: Dashboard width
            
        Returns:
            Plotly figure object
        """
        height = height or self.config.default_height * 2
        width = width or self.config.default_width * 1.5
        
        if strategy_names is None:
            strategy_names = returns_data.columns.tolist()
        
        # Create subplot grid
        fig = make_subplots(
            rows=2, 
            cols=2,
            subplot_titles=(
                "Cumulative Returns", 
                "Drawdown Analysis",
                "Trade Analysis" if trade_data is not None else "",
                "Funding Rate Heatmap" if funding_data is not None else ""
            ),
            specs=[
                [{"type": "xy"}, {"type": "xy"}],
                [{"type": "xy"}, {"type": "xy"}]
            ],
            vertical_spacing=0.1,
            horizontal_spacing=0.1
        )
        
        # 1. Cumulative Returns Chart
        cum_returns = (1 + returns_data[strategy_names]).cumprod() - 1
        
        for i, strategy in enumerate(strategy_names):
            color = self.config.color_palette[i % len(self.config.color_palette)]
            fig.add_trace(
                go.Scatter(
                    x=cum_returns.index,
                    y=cum_returns[strategy] * 100,
                    mode='lines',
                    name=f"{strategy} Returns",
                    line=dict(color=color, width=2),
                ),
                row=1, col=1
            )
        
        # Add benchmark if provided
        if benchmark_data is not None:
            cum_benchmark = (1 + benchmark_data).cumprod() - 1
            fig.add_trace(
                go.Scatter(
                    x=cum_benchmark.index,
                    y=cum_benchmark.iloc[:, 0] * 100,
                    mode='lines',
                    name='Benchmark',
                    line=dict(color='gray', width=2, dash='dot'),
                ),
                row=1, col=1
            )
        
        # 2. Drawdown Chart
        rolling_max = cum_returns.cummax()
        drawdowns = (cum_returns / rolling_max - 1) * 100
        
        for i, strategy in enumerate(strategy_names):
            color = self.config.color_palette[i % len(self.config.color_palette)]
            fig.add_trace(
                go.Scatter(
                    x=drawdowns.index,
                    y=drawdowns[strategy],
                    mode='lines',
                    name=f"{strategy} Drawdown",
                    line=dict(color=color, width=2),
                    fill='tozeroy',
                    showlegend=False
                ),
                row=1, col=2
            )
        
        # 3. Trade Analysis (if data provided)
        if trade_data is not None:
            # Split data into profitable and losing trades
            profitable = trade_data[trade_data['pnl'] > 0]
            losing = trade_data[trade_data['pnl'] <= 0]
            
            # Add profitable trades
            if not profitable.empty:
                size = np.sqrt(profitable['pnl'].abs()) * 5
                fig.add_trace(
                    go.Scatter(
                        x=profitable['duration'],
                        y=profitable['pnl'],
                        mode='markers',
                        name='Profitable Trades',
                        marker=dict(
                            color='green',
                            size=size,
                            opacity=0.7,
                            line=dict(width=1, color='darkgreen')
                        ),
                        hoverinfo='text',
                        hovertext=profitable.apply(
                            lambda row: f"PnL: ${row['pnl']:.2f}<br>Duration: {row['duration']} min",
                            axis=1
                        ),
                    ),
                    row=2, col=1
                )
            
            # Add losing trades
            if not losing.empty:
                size = np.sqrt(losing['pnl'].abs()) * 5
                fig.add_trace(
                    go.Scatter(
                        x=losing['duration'],
                        y=losing['pnl'],
                        mode='markers',
                        name='Losing Trades',
                        marker=dict(
                            color='red',
                            size=size,
                            opacity=0.7,
                            line=dict(width=1, color='darkred')
                        ),
                        hoverinfo='text',
                        hovertext=losing.apply(
                            lambda row: f"PnL: ${row['pnl']:.2f}<br>Duration: {row['duration']} min",
                            axis=1
                        ),
                    ),
                    row=2, col=1
                )
        
        # 4. Funding Rate Heatmap (if data provided)
        if funding_data is not None:
            # Pivot data if necessary
            if 'asset' in funding_data.columns and 'funding_rate' in funding_data.columns:
                pivot_data = funding_data.pivot(index=funding_data.index, columns='asset', values='funding_rate')
            else:
                pivot_data = funding_data
                
            fig.add_trace(
                go.Heatmap(
                    z=pivot_data.values.T,
                    x=pivot_data.index,
                    y=pivot_data.columns,
                    colorscale='RdBu',
                    zmid=0,
                    name='Funding Rates',
                ),
                row=2, col=2
            )
        
        # Update layout
        fig.update_layout(
            title="Strategy Performance Dashboard",
            template=self.config.template,
            height=height,
            width=width,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # Update axes titles
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_yaxes(title_text="Cumulative Return (%)", row=1, col=1)
        
        fig.update_xaxes(title_text="Date", row=1, col=2)
        fig.update_yaxes(title_text="Drawdown (%)", row=1, col=2)
        
        if trade_data is not None:
            fig.update_xaxes(title_text="Trade Duration (minutes)", row=2, col=1)
            fig.update_yaxes(title_text="Trade PnL ($)", row=2, col=1)
        
        if funding_data is not None:
            fig.update_xaxes(title_text="Date", row=2, col=2)
            fig.update_yaxes(title_text="Asset", row=2, col=2)
        
        return fig


class PerformanceMetricsCalculator:
    """
    Calculate various performance metrics for strategy evaluation.
    """
    
    @staticmethod
    def calculate_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 252) -> float:
        """
        Calculate the Sharpe ratio of a return series.
        
        Args:
            returns: Series of returns
            risk_free_rate: Annualized risk-free rate
            periods_per_year: Number of periods in a year
            
        Returns:
            Sharpe ratio
        """
        # Convert risk-free rate to per-period
        rf_per_period = (1 + risk_free_rate) ** (1 / periods_per_year) - 1
        
        # Calculate excess returns
        excess_returns = returns - rf_per_period
        
        # Calculate annualized Sharpe ratio
        return (excess_returns.mean() * periods_per_year) / (excess_returns.std() * np.sqrt(periods_per_year))
    
    @staticmethod
    def calculate_sortino_ratio(returns: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 252) -> float:
        """
        Calculate the Sortino ratio of a return series.
        
        Args:
            returns: Series of returns
            risk_free_rate: Annualized risk-free rate
            periods_per_year: Number of periods in a year
            
        Returns:
            Sortino ratio
        """
        # Convert risk-free rate to per-period
        rf_per_period = (1 + risk_free_rate) ** (1 / periods_per_year) - 1
        
        # Calculate excess returns
        excess_returns = returns - rf_per_period
        
        # Calculate downside deviation (using only negative returns)
        downside_returns = excess_returns[excess_returns < 0]
        downside_deviation = np.sqrt((downside_returns ** 2).sum() / len(returns)) * np.sqrt(periods_per_year)
        
        # Handle case where there are no negative returns
        if downside_deviation == 0:
            return np.inf if excess_returns.mean() > 0 else 0
        
        # Calculate annualized Sortino ratio
        return (excess_returns.mean() * periods_per_year) / downside_deviation
    
    @staticmethod
    def calculate_max_drawdown(returns: pd.Series) -> float:
        """
        Calculate the maximum drawdown of a return series.
        
        Args:
            returns: Series of returns
            
        Returns:
            Maximum drawdown as a positive percentage
        """
        # Calculate cumulative returns
        cum_returns = (1 + returns).cumprod()
        
        # Calculate running maximum
        running_max = cum_returns.cummax()
        
        # Calculate drawdown
        drawdown = (cum_returns / running_max - 1)
        
        # Return the maximum drawdown as a positive percentage
        return -drawdown.min() * 100
    
    @staticmethod
    def calculate_calmar_ratio(returns: pd.Series, periods_per_year: int = 252) -> float:
        """
        Calculate the Calmar ratio of a return series.
        
        Args:
            returns: Series of returns
            periods_per_year: Number of periods in a year
            
        Returns:
            Calmar ratio
        """
        # Calculate annualized return
        annualized_return = (1 + returns.mean()) ** periods_per_year - 1
        
        # Calculate maximum drawdown
        max_drawdown = PerformanceMetricsCalculator.calculate_max_drawdown(returns) / 100  # Convert to decimal
        
        # Handle case with no drawdown
        if max_drawdown == 0:
            return np.inf if annualized_return > 0 else 0
        
        # Calculate Calmar ratio
        return annualized_return / max_drawdown
    
    @staticmethod
    def calculate_win_rate(trades: pd.DataFrame) -> float:
        """
        Calculate the win rate of a series of trades.
        
        Args:
            trades: DataFrame with a 'pnl' column
            
        Returns:
            Win rate as a percentage
        """
        if len(trades) == 0:
            return 0
        
        # Count winning trades
        winning_trades = len(trades[trades['pnl'] > 0])
        
        # Calculate win rate
        return (winning_trades / len(trades)) * 100
    
    @staticmethod
    def calculate_profit_factor(trades: pd.DataFrame) -> float:
        """
        Calculate the profit factor of a series of trades.
        
        Args:
            trades: DataFrame with a 'pnl' column
            
        Returns:
            Profit factor
        """
        # Calculate gross profit and gross loss
        gross_profit = trades[trades['pnl'] > 0]['pnl'].sum()
        gross_loss = abs(trades[trades['pnl'] < 0]['pnl'].sum())
        
        # Handle case with no losses
        if gross_loss == 0:
            return np.inf if gross_profit > 0 else 0
        
        # Calculate profit factor
        return gross_profit / gross_loss
    
    def calculate_all_metrics(self, 
                             returns: pd.Series,
                             trades: pd.DataFrame = None,
                             risk_free_rate: float = 0.0,
                             periods_per_year: int = 252) -> Dict[str, float]:
        """
        Calculate all performance metrics.
        
        Args:
            returns: Series of returns
            trades: DataFrame with trade information
            risk_free_rate: Annualized risk-free rate
            periods_per_year: Number of periods in a year
            
        Returns:
            Dictionary of performance metrics
        """
        metrics = {
            'annualized_return': (1 + returns.mean()) ** periods_per_year - 1,
            'annualized_volatility': returns.std() * np.sqrt(periods_per_year),
            'sharpe_ratio': self.calculate_sharpe_ratio(returns, risk_free_rate, periods_per_year),
            'sortino_ratio': self.calculate_sortino_ratio(returns, risk_free_rate, periods_per_year),
            'max_drawdown': self.calculate_max_drawdown(returns),
            'calmar_ratio': self.calculate_calmar_ratio(returns, periods_per_year)
        }
        
        # Add trade-based metrics if trades are provided
        if trades is not None and not trades.empty:
            metrics.update({
                'win_rate': self.calculate_win_rate(trades),
                'profit_factor': self.calculate_profit_factor(trades),
                'avg_win': trades[trades['pnl'] > 0]['pnl'].mean() if not trades[trades['pnl'] > 0].empty else 0,
                'avg_loss': trades[trades['pnl'] < 0]['pnl'].mean() if not trades[trades['pnl'] < 0].empty else 0,
                'total_trades': len(trades),
                'winning_trades': len(trades[trades['pnl'] > 0]),
                'losing_trades': len(trades[trades['pnl'] < 0])
            })
        
        return metrics


# Example usage
if __name__ == "__main__":
    # This is for demonstration only
    import pandas as pd
    import numpy as np
    
    # Generate sample return data
    dates = pd.date_range(start='2020-01-01', end='2020-12-31', freq='D')
    np.random.seed(42)
    
    returns_data = pd.DataFrame({
        'Strategy1': np.random.normal(0.001, 0.02, len(dates)),
        'Strategy2': np.random.normal(0.0005, 0.015, len(dates)),
        'Strategy3': np.random.normal(0.0015, 0.025, len(dates))
    }, index=dates)
    
    # Generate sample trade data
    trade_data = pd.DataFrame({
        'strategy': np.random.choice(['Strategy1', 'Strategy2', 'Strategy3'], 100),
        'entry_time': np.random.choice(dates, 100),
        'exit_time': np.random.choice(dates, 100),
        'duration': np.random.randint(1, 1000, 100),
        'pnl': np.random.normal(50, 200, 100)
    })
    
    # Generate sample funding rate data
    assets = ['BTC', 'ETH', 'SOL', 'ADA', 'DOT']
    funding_data = pd.DataFrame({
        'asset': np.repeat(assets, len(dates)),
        'date': np.tile(dates, len(assets)),
        'funding_rate': np.random.normal(0, 0.01, len(dates) * len(assets))
    })
    funding_data.set_index('date', inplace=True)
    
    # Create visualizer
    visualizer = PerformanceVisualizer()
    
    # Create dashboard
    dashboard = visualizer.create_performance_dashboard(
        returns_data=returns_data,
        trade_data=trade_data,
        funding_data=funding_data.pivot(columns='asset', values='funding_rate')
    )
    
    # Show dashboard
    dashboard.show()
    
    # Calculate metrics
    calculator = PerformanceMetricsCalculator()
    metrics = calculator.calculate_all_metrics(returns_data['Strategy1'], trade_data[trade_data['strategy'] == 'Strategy1'])
    print(metrics) 