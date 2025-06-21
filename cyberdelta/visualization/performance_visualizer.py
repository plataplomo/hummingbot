"""Strategy Performance Visualization Tools.

This module provides tools for visualizing and analyzing strategy performance data.
It provides a foundation for building both real-time performance monitoring dashboards
and historical performance analysis tools.

NOTE: Many linter/type errors in this file are due to incomplete type stubs in pandas/plotly.
These do not represent real runtime risks and are not actionable in user code.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
import plotly.express as px  # type: ignore[import-untyped]
import plotly.graph_objects as go  # type: ignore[import-untyped]
from plotly.subplots import make_subplots  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


@dataclass
class VisualizationConfig:
    """Configuration for visualization components."""

    theme: str = "light"  # "light" or "dark"
    default_height: int = 600
    default_width: int = 800
    color_palette: list[str] | None = None
    template: str = "plotly_white"
    show_legend: bool = True

    def __post_init__(self) -> None:
        """Initialize default color palette and theme settings after dataclass creation.

        Sets up default Plotly color palette if none provided and applies dark theme
        template when dark theme is selected.
        """
        if self.color_palette is None:
            self.color_palette = px.colors.qualitative.Plotly

        if self.theme == "dark":
            self.template = "plotly_dark"


class PerformanceVisualizer:
    """Core class for generating strategy performance visualizations.

    This class provides methods for creating various visualizations of strategy
    performance data, including returns, drawdowns, trade analysis, and more.
    """

    def __init__(self, config: VisualizationConfig | None = None) -> None:
        """Initialize the performance visualizer.

        Args:
            config: Configuration for visualizations

        """
        self.config = config or VisualizationConfig()

    def create_returns_chart(
        self,
        returns_data: pd.DataFrame,
        strategy_names: list[str] | None = None,
        benchmark_data: pd.DataFrame | None = None,
        title: str = "Cumulative Returns",
        height: int | None = None,
        width: int | None = None,
    ) -> go.Figure:
        """Create a cumulative returns chart for one or more strategies.

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
        # Use a local variable to ensure type safety for strategy names
        names: list[str] = (
            strategy_names if strategy_names is not None else returns_data.columns.tolist()
        )
        if benchmark_data is None:
            benchmark_data = pd.DataFrame()
        height = height or self.config.default_height
        width = width or self.config.default_width

        # Calculate cumulative returns
        # NOTE: Type checker limitation: pandas stubs are incomplete for cumprod
        cum_returns = (1 + returns_data[names]).cumprod() - 1

        # Create figure
        fig = go.Figure()

        # Add strategy returns
        if self.config.color_palette is None:
            raise ValueError("color_palette must not be None")
        for i, strategy in enumerate(names):
            color_palette = self.config.color_palette
            color = color_palette[i % len(color_palette)]
            fig.add_trace(
                go.Scatter(
                    x=cum_returns.index,  # NOTE: pandas index type is partially unknown
                    y=cum_returns[strategy] * 100,  # Convert to percentage
                    mode="lines",
                    name=strategy,
                    line={"color": color, "width": 2},
                ),
            )

        # Add benchmark if provided
        if not benchmark_data.empty:
            # NOTE: Type checker limitation: pandas stubs are incomplete for cumprod
            cum_benchmark = (1 + benchmark_data).cumprod() - 1
            fig.add_trace(
                go.Scatter(
                    x=cum_benchmark.index,  # NOTE: pandas index type is partially unknown
                    y=cum_benchmark.iloc[:, 0] * 100,  # Convert to percentage
                    mode="lines",
                    name="Benchmark",
                    line={"color": "gray", "width": 2, "dash": "dot"},
                ),
            )

        # Update layout
        # NOTE: Type checker limitation: plotly stubs are incomplete for update_layout
        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Cumulative Return (%)",
            template=self.config.template,
            height=height,
            width=width,
            legend={
                "yanchor": "top",
                "y": 0.99,
                "xanchor": "left",
                "x": 0.01,
                "bgcolor": "rgba(255, 255, 255, 0.5)"
                if self.config.theme == "light"
                else "rgba(0, 0, 0, 0.5)",
            },
            hovermode="x unified",
        )

        # Add range slider
        # NOTE: Type checker limitation: plotly stubs are incomplete for update_xaxes
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector={
                "buttons": [
                    {"count": 1, "label": "1m", "step": "month", "stepmode": "backward"},
                    {"count": 6, "label": "6m", "step": "month", "stepmode": "backward"},
                    {"count": 1, "label": "YTD", "step": "year", "stepmode": "todate"},
                    {"count": 1, "label": "1y", "step": "year", "stepmode": "backward"},
                    {"step": "all"},
                ],
            },
        )

        return fig

    def create_drawdown_chart(
        self,
        returns_data: pd.DataFrame,
        strategy_names: list[str] | None = None,
        title: str = "Drawdown Analysis",
        height: int | None = None,
        width: int | None = None,
    ) -> go.Figure:
        """Create a drawdown chart for one or more strategies.

        Args:
            returns_data: DataFrame with datetime index and strategy returns as columns
            strategy_names: List of strategy names to include (if None, use all columns)
            title: Chart title
            height: Chart height
            width: Chart width

        Returns:
            Plotly figure object

        """
        # Use a local variable to ensure type safety for strategy names
        names: list[str] = (
            strategy_names if strategy_names is not None else returns_data.columns.tolist()
        )
        height = height or self.config.default_height
        width = width or self.config.default_width

        # Calculate drawdowns
        # NOTE: Type checker limitation: pandas stubs are incomplete for cumprod/cummax
        cum_returns = (1 + returns_data[names]).cumprod()
        rolling_max = cum_returns.cummax()
        drawdowns = (cum_returns / rolling_max - 1) * 100  # Convert to percentage

        # Create figure
        fig = go.Figure()

        # Add drawdown traces
        if self.config.color_palette is None:
            raise ValueError("color_palette must not be None")
        for i, strategy in enumerate(names):
            color_palette = self.config.color_palette
            color = color_palette[i % len(color_palette)]
            fig.add_trace(
                go.Scatter(
                    x=drawdowns.index,  # NOTE: pandas index type is partially unknown
                    y=drawdowns[strategy],
                    mode="lines",
                    name=strategy,
                    line={"color": color, "width": 2},
                    fill="tozeroy",
                ),
            )

        # Update layout
        # NOTE: Type checker limitation: plotly stubs are incomplete for update_layout/update_xaxes
        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Drawdown (%)",
            template=self.config.template,
            height=height,
            width=width,
            legend={
                "yanchor": "top",
                "y": 0.99,
                "xanchor": "left",
                "x": 0.01,
                "bgcolor": "rgba(255, 255, 255, 0.5)"
                if self.config.theme == "light"
                else "rgba(0, 0, 0, 0.5)",
            },
            hovermode="x unified",
        )
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector={
                "buttons": [
                    {"count": 1, "label": "1m", "step": "month", "stepmode": "backward"},
                    {"count": 6, "label": "6m", "step": "month", "stepmode": "backward"},
                    {"count": 1, "label": "YTD", "step": "year", "stepmode": "todate"},
                    {"count": 1, "label": "1y", "step": "year", "stepmode": "backward"},
                    {"step": "all"},
                ],
            },
        )

        return fig

    def create_trade_analysis_chart(
        self,
        trade_data: pd.DataFrame,
        title: str = "Trade Analysis",
        height: int | None = None,
        width: int | None = None,
    ) -> go.Figure:
        """Create a scatter plot of trades showing PnL vs duration.

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
        profitable = trade_data[trade_data["pnl"] > 0]
        losing = trade_data[trade_data["pnl"] <= 0]

        # Add profitable trades
        if not profitable.empty:
            size = np.sqrt(profitable["pnl"].abs()) * 5  # Scale marker size
            fig.add_trace(
                go.Scatter(
                    x=profitable["duration"],
                    y=profitable["pnl"],
                    mode="markers",
                    name="Profitable Trades",
                    marker={
                        "color": "green",
                        "size": size,
                        "opacity": 0.7,
                        "line": {"width": 1, "color": "darkgreen"},
                    },
                    hovertext=profitable.apply(
                        lambda row: f"Time: {row['exit_time']}<br>PnL: ${row['pnl']:.2f}<br>"
                        f"Duration: {row['duration']} min",
                        axis=1,
                    ),
                    hoverinfo="text",
                ),
            )

        # Add losing trades
        if not losing.empty:
            size = np.sqrt(losing["pnl"].abs()) * 5  # Scale marker size
            fig.add_trace(
                go.Scatter(
                    x=losing["duration"],
                    y=losing["pnl"],
                    mode="markers",
                    name="Losing Trades",
                    marker={
                        "color": "red",
                        "size": size,
                        "opacity": 0.7,
                        "line": {"width": 1, "color": "darkred"},
                    },
                    hovertext=losing.apply(
                        lambda row: f"Time: {row['exit_time']}<br>PnL: ${row['pnl']:.2f}<br>"
                        f"Duration: {row['duration']} min",
                        axis=1,
                    ),
                    hoverinfo="text",
                ),
            )

        # Add horizontal line at zero
        fig.add_shape(
            type="line",
            x0=0,
            y0=0,
            x1=1,
            y1=0,
            xref="paper",
            line={
                "color": "gray",
                "width": 1,
                "dash": "dash",
            },
        )

        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title="Trade Duration (minutes)",
            yaxis_title="Trade PnL ($)",
            template=self.config.template,
            height=height,
            width=width,
            hovermode="closest",
        )

        return fig

    def create_funding_rate_heatmap(
        self,
        funding_data: pd.DataFrame,
        title: str = "Funding Rate Heatmap",
        height: int | None = None,
        width: int | None = None,
    ) -> go.Figure:
        """Create a heatmap of funding rates across assets and time.

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
        # NOTE: Type checker limitation: pandas stubs are incomplete for pivot, values, index
        if "asset" in funding_data.columns and "funding_rate" in funding_data.columns:
            pivot_data = funding_data.pivot(index=None, columns="asset", values="funding_rate")
        else:
            pivot_data = funding_data

        # Create figure
        fig = go.Figure(
            data=go.Heatmap(
                z=pivot_data.values.T,
                x=pivot_data.index,
                y=pivot_data.columns,
                colorscale="RdBu",
                zmid=0,  # Center colorscale at zero
                colorbar={"title": {"text": "Funding Rate (%)", "side": "right"}},
            ),
        )

        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Asset",
            template=self.config.template,
            height=height,
            width=width,
        )

        return fig

    def create_performance_dashboard(
        self,
        returns_data: pd.DataFrame,
        trade_data: pd.DataFrame | None = None,
        funding_data: pd.DataFrame | None = None,
        strategy_names: list[str] | None = None,
        benchmark_data: pd.DataFrame | None = None,
        height: int | None = None,
        width: int | None = None,
    ) -> go.Figure:
        """Create a comprehensive performance dashboard with multiple charts.

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
        # Set up dashboard parameters
        height = height or int(self.config.default_height * 2)
        width = width or int(self.config.default_width * 1.5)
        names = strategy_names if strategy_names is not None else returns_data.columns.tolist()
        benchmark_data = benchmark_data if benchmark_data is not None else pd.DataFrame()

        # Create the subplot structure
        fig = self._create_dashboard_subplots(trade_data, funding_data)

        # Add all chart components
        self._add_cumulative_returns_chart(fig, returns_data, names, benchmark_data)
        self._add_drawdown_chart(fig, returns_data, names)

        if trade_data is not None:
            self._add_trade_analysis_chart(fig, trade_data)

        if funding_data is not None:
            self._add_funding_rate_heatmap(fig, funding_data)

        # Apply final layout
        self._apply_dashboard_layout(fig, height, width, trade_data, funding_data)

        return fig

    def _create_dashboard_subplots(
        self, trade_data: pd.DataFrame | None, funding_data: pd.DataFrame | None
    ) -> go.Figure:
        """Create the subplot structure for the dashboard."""
        return make_subplots(
            rows=2,
            cols=2,
            subplot_titles=(
                "Cumulative Returns",
                "Drawdown Analysis",
                "Trade Analysis" if trade_data is not None else "",
                "Funding Rate Heatmap" if funding_data is not None else "",
            ),
            specs=[[{"type": "xy"}, {"type": "xy"}], [{"type": "xy"}, {"type": "xy"}]],
            vertical_spacing=0.1,
            horizontal_spacing=0.1,
        )

    def _add_cumulative_returns_chart(
        self,
        fig: go.Figure,
        returns_data: pd.DataFrame,
        names: list[str],
        benchmark_data: pd.DataFrame,
    ) -> None:
        """Add cumulative returns chart to the dashboard."""
        # NOTE: Type checker limitation: pandas stubs are incomplete for cumprod/cummax
        cum_returns = (1 + returns_data[names]).cumprod() - 1

        if self.config.color_palette is None:
            raise ValueError("color_palette must not be None")
        color_palette = self.config.color_palette

        for i, strategy in enumerate(names):
            color = color_palette[i % len(color_palette)]
            fig.add_trace(
                go.Scatter(
                    x=cum_returns.index,
                    y=cum_returns[strategy] * 100,
                    mode="lines",
                    name=f"{strategy} Returns",
                    line={"color": color, "width": 2},
                ),
                row=1,
                col=1,
            )

        # Add benchmark if provided
        if not benchmark_data.empty:
            # NOTE: Type checker limitation: pandas stubs are incomplete for cumprod
            cum_benchmark = (1 + benchmark_data).cumprod() - 1
            fig.add_trace(
                go.Scatter(
                    x=cum_benchmark.index,
                    y=cum_benchmark.iloc[:, 0] * 100,
                    mode="lines",
                    name="Benchmark",
                    line={"color": "gray", "width": 2, "dash": "dot"},
                ),
                row=1,
                col=1,
            )

    def _add_drawdown_chart(
        self, fig: go.Figure, returns_data: pd.DataFrame, names: list[str]
    ) -> None:
        """Add drawdown chart to the dashboard."""
        # NOTE: Type checker limitation: pandas stubs are incomplete for cumprod/cummax
        cum_returns = (1 + returns_data[names]).cumprod() - 1
        rolling_max = cum_returns.cummax()
        drawdowns = (cum_returns / rolling_max - 1) * 100

        if self.config.color_palette is None:
            raise ValueError("color_palette must not be None")
        color_palette = self.config.color_palette

        for i, strategy in enumerate(names):
            color = color_palette[i % len(color_palette)]
            fig.add_trace(
                go.Scatter(
                    x=drawdowns.index,
                    y=drawdowns[strategy],
                    mode="lines",
                    name=f"{strategy} Drawdown",
                    line={"color": color, "width": 2},
                    fill="tozeroy",
                    showlegend=False,
                ),
                row=1,
                col=2,
            )

    def _add_trade_analysis_chart(self, fig: go.Figure, trade_data: pd.DataFrame) -> None:
        """Add trade analysis chart to the dashboard."""
        # Split data into profitable and losing trades
        profitable = trade_data[trade_data["pnl"] > 0]
        losing = trade_data[trade_data["pnl"] <= 0]

        # Add profitable trades
        if not profitable.empty:
            self._add_profitable_trades_scatter(fig, profitable)

        # Add losing trades
        if not losing.empty:
            self._add_losing_trades_scatter(fig, losing)

    def _add_profitable_trades_scatter(self, fig: go.Figure, profitable: pd.DataFrame) -> None:
        """Add profitable trades scatter plot."""
        size = np.sqrt(profitable["pnl"].abs()) * 5
        fig.add_trace(
            go.Scatter(
                x=profitable["duration"],
                y=profitable["pnl"],
                mode="markers",
                name="Profitable Trades",
                marker={
                    "color": "green",
                    "size": size,
                    "opacity": 0.7,
                    "line": {"width": 1, "color": "darkgreen"},
                },
                hoverinfo="text",
                hovertext=profitable.apply(
                    lambda row: f"PnL: ${row['pnl']:.2f}<br>Duration: {row['duration']} min",
                    axis=1,
                ),
            ),
            row=2,
            col=1,
        )

    def _add_losing_trades_scatter(self, fig: go.Figure, losing: pd.DataFrame) -> None:
        """Add losing trades scatter plot."""
        size = np.sqrt(losing["pnl"].abs()) * 5
        fig.add_trace(
            go.Scatter(
                x=losing["duration"],
                y=losing["pnl"],
                mode="markers",
                name="Losing Trades",
                marker={
                    "color": "red",
                    "size": size,
                    "opacity": 0.7,
                    "line": {"width": 1, "color": "darkred"},
                },
                hoverinfo="text",
                hovertext=losing.apply(
                    lambda row: f"PnL: ${row['pnl']:.2f}<br>Duration: {row['duration']} min",
                    axis=1,
                ),
            ),
            row=2,
            col=1,
        )

    def _add_funding_rate_heatmap(self, fig: go.Figure, funding_data: pd.DataFrame) -> None:
        """Add funding rate heatmap to the dashboard."""
        # Pivot data if necessary
        # NOTE: Type checker limitation: pandas stubs are incomplete for pivot, values, index
        if "asset" in funding_data.columns and "funding_rate" in funding_data.columns:
            pivot_data = funding_data.pivot(index=None, columns="asset", values="funding_rate")
        else:
            pivot_data = funding_data

        fig.add_trace(
            go.Heatmap(
                z=pivot_data.values.T,
                x=pivot_data.index,
                y=pivot_data.columns,
                colorscale="RdBu",
                zmid=0,
                name="Funding Rates",
            ),
            row=2,
            col=2,
        )

    def _apply_dashboard_layout(
        self,
        fig: go.Figure,
        height: int,
        width: int,
        trade_data: pd.DataFrame | None = None,
        funding_data: pd.DataFrame | None = None,
    ) -> None:
        """Apply final layout settings to the dashboard."""
        fig.update_layout(
            title="Strategy Performance Dashboard",
            template=self.config.template,
            height=height,
            width=width,
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
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


class PerformanceMetricsCalculator:
    """Calculator for strategy performance metrics.

    This class provides methods to calculate various performance metrics
    from strategy returns and trade data.
    """

    def __init__(self, annualization_factor: int = 252) -> None:
        """Initialize the calculator.

        Args:
            annualization_factor: Number of trading periods in a year
                (252 for daily returns, 12 for monthly, etc.)

        """
        self.annualization_factor = annualization_factor

    def calculate_sharpe_ratio(
        self,
        returns: pd.Series[float],
        risk_free_rate: float = 0.0,
    ) -> float:
        """Calculate the Sharpe ratio.

        Args:
            returns: Series of period returns
            risk_free_rate: Risk-free rate (annualized)

        Returns:
            Sharpe ratio (annualized)

        """
        if len(returns) < 2:
            return 0.0

        # Convert annual risk-free rate to period rate
        period_risk_free = risk_free_rate / self.annualization_factor

        excess_returns = returns - period_risk_free

        if excess_returns.std() == 0:
            return 0.0

        sharpe = excess_returns.mean() / excess_returns.std()

        # Annualize
        return float(sharpe * np.sqrt(self.annualization_factor))

    def calculate_sortino_ratio(
        self,
        returns: pd.Series[float],
        risk_free_rate: float = 0.0,
        target_return: float = 0.0,
    ) -> float:
        """Calculate the Sortino ratio.

        Args:
            returns: Series of period returns
            risk_free_rate: Risk-free rate (annualized)
            target_return: Minimum acceptable return (usually 0)

        Returns:
            Sortino ratio (annualized)

        """
        if len(returns) < 2:
            return 0.0

        # Convert annual rates to period rates
        period_risk_free = risk_free_rate / self.annualization_factor
        period_target = target_return / self.annualization_factor

        # Calculate excess returns
        excess_returns = returns - period_risk_free

        # Calculate downside deviation (below target)
        downside_returns = returns[returns < period_target]

        if len(downside_returns) == 0 or downside_returns.std() == 0:
            # No downside or zero downside deviation
            return float("inf") if excess_returns.mean() > 0 else 0.0

        # Sortino ratio
        sortino = excess_returns.mean() / downside_returns.std()

        # Annualize
        return float(sortino * np.sqrt(self.annualization_factor))

    def calculate_max_drawdown(self, returns: pd.Series[float]) -> float:
        """Calculate the maximum drawdown percentage.

        Args:
            returns: Series of period returns

        Returns:
            Maximum drawdown as a percentage (0-100)

        """
        if len(returns) < 2:
            return 0.0

        # Calculate cumulative returns
        cum_returns = (1 + returns).cumprod()

        # Calculate running maximum
        running_max = cum_returns.cummax()

        # Calculate drawdowns
        drawdowns = (cum_returns / running_max - 1) * 100  # As percentage

        # Find the maximum drawdown
        max_drawdown = abs(drawdowns.min())

        return float(max_drawdown)

    def calculate_calmar_ratio(self, returns: pd.Series[float], period: int = 36) -> float:
        """Calculate the Calmar ratio.

        Args:
            returns: Series of period returns
            period: Period in months for the Calmar ratio calculation

        Returns:
            Calmar ratio

        """
        if len(returns) < 2:
            return 0.0

        # Annualized return
        ann_return = self.calculate_annualized_return(returns) * 100  # As percentage

        # Maximum drawdown
        max_dd = self.calculate_max_drawdown(returns)

        if max_dd == 0:
            return float("inf") if ann_return > 0 else 0.0

        # Calmar ratio
        return ann_return / max_dd

    def calculate_annualized_return(self, returns: pd.Series[float]) -> float:
        """Calculate the annualized return.

        Args:
            returns: Series of period returns

        Returns:
            Annualized return (decimal)

        """
        if len(returns) < 1:
            return 0.0

        # Compound the returns
        # NOTE: Type checker limitation: pandas stubs are incomplete for prod
        # Convert to numpy array to avoid pandas type issues
        returns_array = np.asarray(returns.values, dtype=float)
        total_return = float(np.prod(1 + returns_array) - 1)

        # Annualize
        periods = len(returns)
        # NOTE: Operator safety: ensure periods > 0
        if periods == 0:
            return 0.0
        annualized_return = (1 + total_return) ** (self.annualization_factor / periods) - 1

        return float(annualized_return)

    def calculate_annualized_volatility(self, returns: pd.Series[float]) -> float:
        """Calculate the annualized volatility.

        Args:
            returns: Series of period returns

        Returns:
            Annualized volatility (decimal)

        """
        if len(returns) < 2:
            return 0.0

        # Annualize the standard deviation
        return float(returns.std() * np.sqrt(self.annualization_factor))

    def calculate_win_rate(self, trades: pd.DataFrame) -> float:
        """Calculate the win rate.

        Args:
            trades: DataFrame of trades with 'pnl' column

        Returns:
            Win rate as a percentage (0-100)

        """
        if len(trades) == 0:
            return 0.0

        # Count winning trades
        winning_trades = len(trades[trades["pnl"] > 0])

        # Calculate win rate
        win_rate = (winning_trades / len(trades)) * 100

        return win_rate

    def calculate_profit_factor(self, trades: pd.DataFrame) -> float:
        """Calculate the profit factor.

        Args:
            trades: DataFrame of trades with 'pnl' column

        Returns:
            Profit factor (gross profit / gross loss)

        """
        if len(trades) == 0:
            return 0.0

        # Separate winning and losing trades
        winning_trades = trades[trades["pnl"] > 0]
        losing_trades = trades[trades["pnl"] < 0]

        gross_profit = winning_trades["pnl"].sum() if len(winning_trades) > 0 else 0
        gross_loss = abs(losing_trades["pnl"].sum()) if len(losing_trades) > 0 else 0

        if gross_loss == 0:
            return float("inf") if gross_profit > 0 else 0.0

        return gross_profit / gross_loss

    def calculate_average_trade(self, trades: pd.DataFrame, win_loss: str = "all") -> float:
        """Calculate the average trade P&L.

        Args:
            trades: DataFrame of trades with 'pnl' column
            win_loss: Filter trades ('all', 'win', or 'loss')

        Returns:
            Average trade P&L

        """
        if len(trades) == 0:
            return 0.0

        if win_loss == "win":
            filtered_trades = trades[trades["pnl"] > 0]
        elif win_loss == "loss":
            filtered_trades = trades[trades["pnl"] < 0]
        else:
            filtered_trades = trades

        if len(filtered_trades) == 0:
            return 0.0

        return filtered_trades["pnl"].mean()

    def calculate_all_metrics(
        self,
        returns: pd.Series[float],
        trades: pd.DataFrame | None = None,
    ) -> dict[str, float]:
        """Calculate all performance metrics.

        Args:
            returns: Series of period returns
            trades: DataFrame of trades

        Returns:
            Dictionary of performance metrics

        """
        metrics = {}

        # Return-based metrics
        metrics["annualized_return"] = (
            self.calculate_annualized_return(returns) * 100
        )  # As percentage
        metrics["annualized_volatility"] = (
            self.calculate_annualized_volatility(returns) * 100
        )  # As percentage
        metrics["sharpe_ratio"] = self.calculate_sharpe_ratio(returns)
        metrics["sortino_ratio"] = self.calculate_sortino_ratio(returns)
        metrics["max_drawdown"] = self.calculate_max_drawdown(returns)
        metrics["calmar_ratio"] = self.calculate_calmar_ratio(returns)

        # Trade-based metrics (if trades provided)
        if trades is not None and len(trades) > 0:
            metrics["win_rate"] = self.calculate_win_rate(trades)
            metrics["profit_factor"] = self.calculate_profit_factor(trades)
            metrics["avg_win"] = self.calculate_average_trade(trades, "win")
            metrics["avg_loss"] = self.calculate_average_trade(trades, "loss")
            metrics["total_trades"] = len(trades)
            # Add counts for winning and losing trades
            metrics["winning_trades"] = len(trades[trades["pnl"] > 0])
            metrics["losing_trades"] = len(trades[trades["pnl"] <= 0])

        return metrics


# Example usage
if __name__ == "__main__":
    # This is for demonstration only
    import numpy as np
    import pandas as pd

    # Generate sample return data
    dates = pd.date_range(start="2020-01-01", end="2020-12-31", freq="D")
    np.random.seed(42)

    returns_data = pd.DataFrame(
        {
            "Strategy1": np.random.normal(0.001, 0.02, len(dates)),
            "Strategy2": np.random.normal(0.0005, 0.015, len(dates)),
            "Strategy3": np.random.normal(0.0015, 0.025, len(dates)),
        },
        index=dates,
    )

    # Generate sample trade data
    trade_data = pd.DataFrame(
        {
            "strategy": np.random.choice(["Strategy1", "Strategy2", "Strategy3"], 100),
            "entry_time": np.random.choice(dates, 100),
            "exit_time": np.random.choice(dates, 100),
            "duration": np.random.randint(1, 1000, 100),
            "pnl": np.random.normal(50, 200, 100),
        },
    )

    # Generate sample funding rate data
    assets = ["BTC", "ETH", "SOL", "ADA", "DOT"]
    funding_data = pd.DataFrame(
        {
            "asset": np.repeat(assets, len(dates)),
            "date": np.tile(dates, len(assets)),
            "funding_rate": np.random.normal(0, 0.01, len(dates) * len(assets)),
        },
    )
    funding_data.set_index("date", inplace=True)

    # Create visualizer
    visualizer = PerformanceVisualizer()

    # Create dashboard
    dashboard = visualizer.create_performance_dashboard(
        returns_data=returns_data,
        trade_data=trade_data,
        funding_data=funding_data.pivot(columns="asset", values="funding_rate"),
    )

    # Show dashboard
    dashboard.show()
