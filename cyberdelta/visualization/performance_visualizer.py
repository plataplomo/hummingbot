"""
Strategy Performance Visualization Tools.

This module provides tools for visualizing and analyzing strategy performance data.
It provides a foundation for building both real-time performance monitoring dashboards
and historical performance analysis tools.
"""

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)


@dataclass
class VisualizationConfig:
    """Configuration for visualization components."""

    theme: str = "light"  # "light" or "dark"
    default_height: int = 600
    default_width: int = 800
    color_palette: list[str] = None
    template: str = "plotly_white"
    show_legend: bool = True

    def __post_init__(self) -> None:
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

    def __init__(self, config: VisualizationConfig | None = None) -> None:
        """
        Initialize the performance visualizer.

        Args:
            config: Configuration for visualizations
        """
        self.config = config or VisualizationConfig()

    def create_returns_chart(
        self,
        returns_data: pd.DataFrame,
        strategy_names: list[str] = None,
        benchmark_data: pd.DataFrame = None,
        title: str = "Cumulative Returns",
        height: int = None,
        width: int = None,
    ) -> go.Figure:
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
                    mode="lines",
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
                    mode="lines",
                    name="Benchmark",
                    line=dict(color="gray", width=2, dash="dot"),
                )
            )

        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Cumulative Return (%)",
            template=self.config.template,
            height=height,
            width=width,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.5)"
                if self.config.theme == "light"
                else "rgba(0, 0, 0, 0.5)",
            ),
            hovermode="x unified",
        )

        # Add range slider
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="YTD", step="year", stepmode="todate"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all"),
                    ]
                )
            ),
        )

        return fig

    def create_drawdown_chart(
        self,
        returns_data: pd.DataFrame,
        strategy_names: list[str] = None,
        title: str = "Drawdown Analysis",
        height: int = None,
        width: int = None,
    ) -> go.Figure:
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
                    mode="lines",
                    name=strategy,
                    line=dict(color=color, width=2),
                    fill="tozeroy",
                )
            )

        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title="Drawdown (%)",
            template=self.config.template,
            height=height,
            width=width,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.5)"
                if self.config.theme == "light"
                else "rgba(0, 0, 0, 0.5)",
            ),
            hovermode="x unified",
        )

        # Add range slider
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="YTD", step="year", stepmode="todate"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all"),
                    ]
                )
            ),
        )

        return fig

    def create_trade_analysis_chart(
        self,
        trade_data: pd.DataFrame,
        title: str = "Trade Analysis",
        height: int = None,
        width: int = None,
    ) -> go.Figure:
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
                    marker=dict(
                        color="green",
                        size=size,
                        opacity=0.7,
                        line=dict(width=1, color="darkgreen"),
                    ),
                    hovertext=profitable.apply(
                        lambda row: f"Time: {row['exit_time']}<br>PnL: ${row['pnl']:.2f}<br>"
                        f"Duration: {row['duration']} min",
                        axis=1,
                    ),
                    hoverinfo="text",
                )
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
                    marker=dict(
                        color="red",
                        size=size,
                        opacity=0.7,
                        line=dict(width=1, color="darkred"),
                    ),
                    hovertext=losing.apply(
                        lambda row: f"Time: {row['exit_time']}<br>PnL: ${row['pnl']:.2f}<br>"
                        f"Duration: {row['duration']} min",
                        axis=1,
                    ),
                    hoverinfo="text",
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
            ),
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
        height: int = None,
        width: int = None,
    ) -> go.Figure:
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
        if "asset" in funding_data.columns and "funding_rate" in funding_data.columns:
            pivot_data = funding_data.pivot(
                index=funding_data.index, columns="asset", values="funding_rate"
            )
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
                colorbar=dict(title=dict(text="Funding Rate (%)", side="right")),
            )
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
        trade_data: pd.DataFrame = None,
        funding_data: pd.DataFrame = None,
        strategy_names: list[str] = None,
        benchmark_data: pd.DataFrame = None,
        height: int = None,
        width: int = None,
    ) -> go.Figure:
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
                "Funding Rate Heatmap" if funding_data is not None else "",
            ),
            specs=[[{"type": "xy"}, {"type": "xy"}], [{"type": "xy"}, {"type": "xy"}]],
            vertical_spacing=0.1,
            horizontal_spacing=0.1,
        )

        # 1. Cumulative Returns Chart
        cum_returns = (1 + returns_data[strategy_names]).cumprod() - 1

        for i, strategy in enumerate(strategy_names):
            color = self.config.color_palette[i % len(self.config.color_palette)]
            fig.add_trace(
                go.Scatter(
                    x=cum_returns.index,
                    y=cum_returns[strategy] * 100,
                    mode="lines",
                    name=f"{strategy} Returns",
                    line=dict(color=color, width=2),
                ),
                row=1,
                col=1,
            )

        # Add benchmark if provided
        if benchmark_data is not None:
            cum_benchmark = (1 + benchmark_data).cumprod() - 1
            fig.add_trace(
                go.Scatter(
                    x=cum_benchmark.index,
                    y=cum_benchmark.iloc[:, 0] * 100,
                    mode="lines",
                    name="Benchmark",
                    line=dict(color="gray", width=2, dash="dot"),
                ),
                row=1,
                col=1,
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
                    mode="lines",
                    name=f"{strategy} Drawdown",
                    line=dict(color=color, width=2),
                    fill="tozeroy",
                    showlegend=False,
                ),
                row=1,
                col=2,
            )

        # 3. Trade Analysis (if data provided)
        if trade_data is not None:
            # Split data into profitable and losing trades
            profitable = trade_data[trade_data["pnl"] > 0]
            losing = trade_data[trade_data["pnl"] <= 0]

            # Add profitable trades
            if not profitable.empty:
                size = np.sqrt(profitable["pnl"].abs()) * 5
                fig.add_trace(
                    go.Scatter(
                        x=profitable["duration"],
                        y=profitable["pnl"],
                        mode="markers",
                        name="Profitable Trades",
                        marker=dict(
                            color="green",
                            size=size,
                            opacity=0.7,
                            line=dict(width=1, color="darkgreen"),
                        ),
                        hoverinfo="text",
                        hovertext=profitable.apply(
                            lambda row: f"PnL: ${row['pnl']:.2f}<br>"
                            f"Duration: {row['duration']} min",
                            axis=1,
                        ),
                    ),
                    row=2,
                    col=1,
                )

            # Add losing trades
            if not losing.empty:
                size = np.sqrt(losing["pnl"].abs()) * 5
                fig.add_trace(
                    go.Scatter(
                        x=losing["duration"],
                        y=losing["pnl"],
                        mode="markers",
                        name="Losing Trades",
                        marker=dict(
                            color="red",
                            size=size,
                            opacity=0.7,
                            line=dict(width=1, color="darkred"),
                        ),
                        hoverinfo="text",
                        hovertext=losing.apply(
                            lambda row: f"PnL: ${row['pnl']:.2f}<br>"
                            f"Duration: {row['duration']} min",
                            axis=1,
                        ),
                    ),
                    row=2,
                    col=1,
                )

        # 4. Funding Rate Heatmap (if data provided)
        if funding_data is not None:
            # Pivot data if necessary
            if "asset" in funding_data.columns and "funding_rate" in funding_data.columns:
                pivot_data = funding_data.pivot(
                    index=funding_data.index, columns="asset", values="funding_rate"
                )
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

        # Update layout
        fig.update_layout(
            title="Strategy Performance Dashboard",
            template=self.config.template,
            height=height,
            width=width,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
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
        }
    )

    # Generate sample funding rate data
    assets = ["BTC", "ETH", "SOL", "ADA", "DOT"]
    funding_data = pd.DataFrame(
        {
            "asset": np.repeat(assets, len(dates)),
            "date": np.tile(dates, len(assets)),
            "funding_rate": np.random.normal(0, 0.01, len(dates) * len(assets)),
        }
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
