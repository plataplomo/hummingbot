"""
Real-Time Monitoring Dashboard for CyberDeltaEngine.

This module provides a web-based dashboard for real-time monitoring of
strategy performance using Dash and Plotly for visualization.
"""

import logging
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

# Ignore untyped library errors for dash/plotly until stubs are available/configured
import dash  # type: ignore
import dash_bootstrap_components as dbc  # type: ignore
import pandas as pd
import plotly.graph_objects as go  # type: ignore
from dash import Input, Output, dcc, html  # type: ignore

from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.monitoring.performance_tracker import PerformanceTracker
from cyberdelta.visualization.performance_visualizer import (
    PerformanceMetricsCalculator,
    PerformanceVisualizer,
    VisualizationConfig,
)

logger = logging.getLogger(__name__)


class RealTimeDashboard:
    """
    Real-time dashboard for monitoring strategy performance.

    This class creates and manages a Dash application that displays real-time
    performance metrics for running strategies.
    """

    def __init__(
        self,
        performance_tracker: PerformanceTracker,
        portfolio_tracker: PortfolioTracker,
        update_interval: int = 5000,  # milliseconds
        host: str = "127.0.0.1",
        port: int = 8050,
        debug: bool = False,
    ) -> None:
        """
        Initialize the dashboard.

        Args:
            performance_tracker: Tracker containing performance data
            portfolio_tracker: Tracker containing portfolio data
            update_interval: Dashboard update interval in milliseconds
            host: Host address to run the dashboard on
            port: Port to run the dashboard on
            debug: Whether to run in debug mode
        """
        self.performance_tracker = performance_tracker
        self.portfolio_tracker = portfolio_tracker
        self.update_interval = update_interval
        self.host = host
        self.port = port
        self.debug = debug

        # Initialize visualization components
        self.visualizer = PerformanceVisualizer(
            config=VisualizationConfig(theme="dark", template="plotly_dark")
        )
        self.metrics_calculator = PerformanceMetricsCalculator()

        # Initialize the Dash app
        self.app = dash.Dash(
            __name__,
            external_stylesheets=[dbc.themes.DARKLY],
            title="CyberDeltaEngine Dashboard",
        )
        self.setup_layout()
        self.setup_callbacks()

        # Cache for data to avoid repeated calculations
        self.data_cache: dict[str, Any] = {}
        self.last_update_time = datetime.now(UTC)

    def setup_layout(self) -> None:
        """Set up the dashboard layout."""
        self.app.layout = dbc.Container(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.H1(
                                    "CyberDeltaEngine Dashboard",
                                    className="text-center my-4",
                                ),
                                html.Div(
                                    id="last-update-time",
                                    className="text-center text-muted mb-4",
                                ),
                            ],
                            width=12,
                        )
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Card(
                                    [
                                        dbc.CardHeader("Strategy Selection"),
                                        dbc.CardBody(
                                            [
                                                dcc.Dropdown(
                                                    id="strategy-selector",
                                                    options=[],  # Will be populated in callback
                                                    multi=True,
                                                    placeholder="Select strategies to display",
                                                ),
                                                html.Div(className="mt-3"),
                                                dcc.RadioItems(
                                                    id="time-range-selector",
                                                    options=[
                                                        {
                                                            "label": "1 Hour",
                                                            "value": "1h",
                                                        },
                                                        {
                                                            "label": "1 Day",
                                                            "value": "1d",
                                                        },
                                                        {
                                                            "label": "1 Week",
                                                            "value": "1w",
                                                        },
                                                        {
                                                            "label": "1 Month",
                                                            "value": "1m",
                                                        },
                                                        {
                                                            "label": "All Time",
                                                            "value": "all",
                                                        },
                                                    ],
                                                    value="1d",
                                                    inline=True,
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                            width=12,
                        ),
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Card(
                                    [
                                        dbc.CardHeader("Performance Overview"),
                                        dbc.CardBody(
                                            [
                                                dcc.Graph(
                                                    id="performance-overview",
                                                    style={"height": "400px"},
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                            width=12,
                            className="mt-4",
                        ),
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Card(
                                    [
                                        dbc.CardHeader("Drawdown Analysis"),
                                        dbc.CardBody(
                                            [
                                                dcc.Graph(
                                                    id="drawdown-chart",
                                                    style={"height": "300px"},
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                            width=6,
                            className="mt-4",
                        ),
                        dbc.Col(
                            [
                                dbc.Card(
                                    [
                                        dbc.CardHeader("PnL Distribution"),
                                        dbc.CardBody(
                                            [
                                                dcc.Graph(
                                                    id="pnl-distribution",
                                                    style={"height": "300px"},
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                            width=6,
                            className="mt-4",
                        ),
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Card(
                                    [
                                        dbc.CardHeader("Trade Analysis"),
                                        dbc.CardBody(
                                            [
                                                dcc.Graph(
                                                    id="trade-analysis",
                                                    style={"height": "400px"},
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                            width=6,
                            className="mt-4",
                        ),
                        dbc.Col(
                            [
                                dbc.Card(
                                    [
                                        dbc.CardHeader("Funding Rate Heatmap"),
                                        dbc.CardBody(
                                            [
                                                dcc.Graph(
                                                    id="funding-rate-heatmap",
                                                    style={"height": "400px"},
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                            width=6,
                            className="mt-4",
                        ),
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Card(
                                    [
                                        dbc.CardHeader("Key Performance Metrics"),
                                        dbc.CardBody(id="performance-metrics-table"),
                                    ]
                                ),
                            ],
                            width=12,
                            className="mt-4",
                        ),
                    ]
                ),
                dcc.Interval(
                    id="interval-component",
                    interval=self.update_interval,  # in milliseconds
                    n_intervals=0,
                ),
            ],
            fluid=True,
            className="mt-3 mb-5",
        )

    def setup_callbacks(self) -> None:
        """Set up the dashboard callbacks."""

        # Update strategy dropdown options
        @self.app.callback(
            Output("strategy-selector", "options"),
            Output("strategy-selector", "value"),
            Input("interval-component", "n_intervals"),
        )
        def update_strategy_options(n_intervals: int) -> tuple[list[dict[str, str]], list[str]]:
            """Update the strategy selector dropdown options."""
            strategies = self.performance_tracker.get_tracked_strategies()
            options = [{"label": s, "value": s} for s in strategies]
            # Keep current selection if available
            current_selection = dash.callback_context.states.get("strategy-selector.value", [])
            valid_selection = [s for s in current_selection if s in strategies]
            return options, valid_selection

        # Update last update time display
        @self.app.callback(
            Output("last-update-time", "children"),
            Input("interval-component", "n_intervals"),
        )
        def update_time_display(n_intervals: int) -> str:
            """Update the last updated time display."""
            now = datetime.now(UTC)
            self.last_update_time = now
            return f"Last Updated: {self.last_update_time.strftime('%Y-%m-%d %H:%M:%S')}"

        # Update performance overview chart
        @self.app.callback(
            Output("performance-overview", "figure"),
            Input("strategy-selector", "value"),
            Input("time-range-selector", "value"),
            Input("interval-component", "n_intervals"),
        )
        def update_performance_overview(
            selected_strategies: list[str] | None,
            time_range: str,
            n_intervals: int,
        ) -> go.Figure:
            if not selected_strategies:
                return go.Figure().update_layout(template="plotly_dark")

            # Get returns data for selected strategies and time range
            returns_data = self._get_returns_data(selected_strategies, time_range)

            if returns_data.empty:
                return go.Figure().update_layout(
                    title="No data available for selected time range",
                    template="plotly_dark",
                )

            # Create returns chart using the visualizer
            return self.visualizer.create_returns_chart(
                returns_data=returns_data,
                strategy_names=selected_strategies,
                title="Cumulative Returns",
            )

        # Update drawdown chart
        @self.app.callback(
            Output("drawdown-chart", "figure"),
            Input("strategy-selector", "value"),
            Input("time-range-selector", "value"),
            Input("interval-component", "n_intervals"),
        )
        def update_drawdown_chart(
            selected_strategies: list[str] | None,
            time_range: str,
            n_intervals: int,
        ) -> go.Figure:
            if not selected_strategies:
                return go.Figure().update_layout(template="plotly_dark")

            # Get returns data for selected strategies and time range
            returns_data = self._get_returns_data(selected_strategies, time_range)

            if returns_data.empty:
                return go.Figure().update_layout(
                    title="No data available for selected time range",
                    template="plotly_dark",
                )

            # Create drawdown chart using the visualizer
            return self.visualizer.create_drawdown_chart(
                returns_data=returns_data,
                strategy_names=selected_strategies,
                title="Drawdown Analysis",
            )

        # Update PnL distribution chart
        @self.app.callback(
            Output("pnl-distribution", "figure"),
            Input("strategy-selector", "value"),
            Input("time-range-selector", "value"),
            Input("interval-component", "n_intervals"),
        )
        def update_pnl_distribution(
            selected_strategies: list[str] | None,
            time_range: str,
            n_intervals: int,
        ) -> go.Figure:
            if not selected_strategies:
                return go.Figure().update_layout(template="plotly_dark")

            # Get trade data for selected strategies and time range
            trade_data = self._get_trade_data(selected_strategies, time_range)

            if trade_data.empty:
                return go.Figure().update_layout(
                    title="No trade data available for selected time range",
                    template="plotly_dark",
                )

            # Create a histogram of PnL values
            fig = go.Figure()

            for strategy in selected_strategies:
                strategy_trades = trade_data[trade_data["strategy"] == strategy]
                if not strategy_trades.empty:
                    fig.add_trace(
                        go.Histogram(
                            x=strategy_trades["pnl"],
                            name=strategy,
                            opacity=0.7,
                            nbinsx=30,
                        )
                    )

            fig.update_layout(
                title="PnL Distribution",
                xaxis_title="PnL",
                yaxis_title="Count",
                barmode="overlay",
                template="plotly_dark",
            )

            return fig

        # Update trade analysis chart
        @self.app.callback(
            Output("trade-analysis", "figure"),
            Input("strategy-selector", "value"),
            Input("time-range-selector", "value"),
            Input("interval-component", "n_intervals"),
        )
        def update_trade_analysis(
            selected_strategies: list[str] | None,
            time_range: str,
            n_intervals: int,
        ) -> go.Figure:
            if not selected_strategies:
                return go.Figure().update_layout(template="plotly_dark")

            # Get trade data for selected strategies and time range
            trade_data = self._get_trade_data(selected_strategies, time_range)

            if trade_data.empty:
                return go.Figure().update_layout(
                    title="No trade data available for selected time range",
                    template="plotly_dark",
                )

            # Filter for the selected strategies
            filtered_data = trade_data[trade_data["strategy"].isin(selected_strategies)]

            # Create trade analysis chart using the visualizer
            return self.visualizer.create_trade_analysis_chart(
                trade_data=filtered_data, title="Trade Analysis (PnL vs Duration)"
            )

        # Update funding rate heatmap
        @self.app.callback(
            Output("funding-rate-heatmap", "figure"),
            Input("time-range-selector", "value"),
            Input("interval-component", "n_intervals"),
        )
        def update_funding_rate_heatmap(time_range: str, n_intervals: int) -> go.Figure:
            # Get funding rate data for time range
            funding_data = self._get_funding_rate_data(time_range)

            if funding_data.empty:
                return go.Figure().update_layout(
                    title="No funding rate data available for selected time range",
                    template="plotly_dark",
                )

            # Create funding rate heatmap using the visualizer
            return self.visualizer.create_funding_rate_heatmap(
                funding_data=funding_data, title="Funding Rate Heatmap"
            )

        # Update performance metrics table
        @self.app.callback(
            Output("performance-metrics-table", "children"),
            Input("strategy-selector", "value"),
            Input("time-range-selector", "value"),
            Input("interval-component", "n_intervals"),
        )
        def update_performance_metrics(
            selected_strategies: list[str] | None,
            time_range: str,
            n_intervals: int,
        ) -> html.Table:
            if not selected_strategies:
                return html.P("No strategies selected")

            # Get returns and trade data for selected strategies and time range
            returns_data = self._get_returns_data(selected_strategies, time_range)
            trade_data = self._get_trade_data(selected_strategies, time_range)

            if returns_data.empty:
                return html.P("No data available for selected time range")

            # Calculate metrics for each strategy
            metrics_rows = []

            for strategy in selected_strategies:
                if strategy in returns_data.columns:
                    strategy_returns = returns_data[strategy]
                    strategy_trades = (
                        trade_data[trade_data["strategy"] == strategy]
                        if not trade_data.empty
                        else None
                    )

                    metrics = self.metrics_calculator.calculate_all_metrics(
                        returns=strategy_returns, trades=strategy_trades
                    )

                    # Create a row for this strategy's metrics
                    metrics_rows.append(
                        html.Tr(
                            [
                                html.Td(strategy, className="fw-bold"),
                                html.Td(f"{metrics['annualized_return'] * 100:.2f}%"),
                                html.Td(f"{metrics['annualized_volatility'] * 100:.2f}%"),
                                html.Td(f"{metrics['sharpe_ratio']:.2f}"),
                                html.Td(f"{metrics['sortino_ratio']:.2f}"),
                                html.Td(f"{metrics['max_drawdown']:.2f}%"),
                                html.Td(f"{metrics['calmar_ratio']:.2f}"),
                                html.Td(
                                    f"{metrics.get('win_rate', 'N/A'):.2f}%"
                                    if isinstance(metrics.get("win_rate"), int | float)
                                    else "N/A"
                                ),
                                html.Td(
                                    f"{metrics.get('profit_factor', 'N/A'):.2f}"
                                    if isinstance(metrics.get("profit_factor"), int | float)
                                    else "N/A"
                                ),
                            ]
                        )
                    )

            # Create the table with all metrics
            table = dbc.Table(
                [
                    html.Thead(
                        html.Tr(
                            [
                                html.Th("Strategy"),
                                html.Th("Ann. Return"),
                                html.Th("Ann. Vol"),
                                html.Th("Sharpe Ratio"),
                                html.Th("Sortino Ratio"),
                                html.Th("Max DD"),
                                html.Th("Calmar Ratio"),
                                html.Th("Win Rate"),
                                html.Th("Profit Factor"),
                            ]
                        )
                    ),
                    html.Tbody(metrics_rows),
                ],
                bordered=True,
                hover=True,
                responsive=True,
                striped=True,
                className="small",
            )

            return table

    def _get_returns_data(self, strategies: list[str], time_range: str) -> pd.DataFrame:
        """
        Get returns data for selected strategies and time range.

        Args:
            strategies: List of strategy names
            time_range: Time range (1h, 1d, 1w, 1m, all)

        Returns:
            DataFrame with strategy returns
        """
        # Cache key
        cache_key = f"returns_{','.join(sorted(strategies))}_{time_range}"

        # Check if data is in cache and still fresh
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]

        # Get time range
        end_time = datetime.now(UTC)
        if time_range == "1h":
            start_time = end_time - timedelta(hours=1)
        elif time_range == "1d":
            start_time = end_time - timedelta(days=1)
        elif time_range == "1w":
            start_time = end_time - timedelta(weeks=1)
        elif time_range == "1m":
            start_time = end_time - timedelta(days=30)
        else:  # all time
            start_time = datetime(2020, 1, 1, tzinfo=UTC)  # Use a very early date

        # Get returns data from performance tracker
        returns_data = self.performance_tracker.get_returns_dataframe(
            strategy_names=strategies, start_time=start_time, end_time=end_time
        )

        # Cache the data
        self.data_cache[cache_key] = returns_data

        return returns_data

    def _get_trade_data(self, strategies: list[str], time_range: str) -> pd.DataFrame:
        """
        Get trade data for selected strategies and time range.

        Args:
            strategies: List of strategy names
            time_range: Time range (1h, 1d, 1w, 1m, all)

        Returns:
            DataFrame with trade data
        """
        # Cache key
        cache_key = f"trades_{','.join(sorted(strategies))}_{time_range}"

        # Check if data is in cache and still fresh
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]

        # Get time range
        end_time = datetime.now(UTC)
        if time_range == "1h":
            start_time = end_time - timedelta(hours=1)
        elif time_range == "1d":
            start_time = end_time - timedelta(days=1)
        elif time_range == "1w":
            start_time = end_time - timedelta(weeks=1)
        elif time_range == "1m":
            start_time = end_time - timedelta(days=30)
        else:  # all time
            start_time = datetime(2020, 1, 1, tzinfo=UTC)  # Use a very early date

        # Get trade data from performance tracker
        trade_data = self.performance_tracker.get_trades_dataframe(
            strategy_names=strategies, start_time=start_time, end_time=end_time
        )

        # Cache the data
        self.data_cache[cache_key] = trade_data

        return trade_data

    def _get_funding_rate_data(self, time_range: str) -> pd.DataFrame:
        """
        Get funding rate data for time range.

        Args:
            time_range: Time range (1h, 1d, 1w, 1m, all)

        Returns:
            DataFrame with funding rate data
        """
        # Cache key
        cache_key = f"funding_rates_{time_range}"

        # Check if data is in cache and still fresh
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]

        # Get time range
        end_time = datetime.now(UTC)
        if time_range == "1h":
            start_time = end_time - timedelta(hours=1)
        elif time_range == "1d":
            start_time = end_time - timedelta(days=1)
        elif time_range == "1w":
            start_time = end_time - timedelta(weeks=1)
        elif time_range == "1m":
            start_time = end_time - timedelta(days=30)
        else:  # all time
            start_time = datetime(2020, 1, 1, tzinfo=UTC)  # Use a very early date

        # Get funding rate data from performance tracker
        funding_data = self.performance_tracker.get_funding_rates_dataframe(
            start_time=start_time, end_time=end_time
        )

        # Cache the data
        self.data_cache[cache_key] = funding_data

        return funding_data

    def start(self, use_threading: bool = True) -> threading.Thread | None:
        """
        Start the dashboard server.

        Args:
            use_threading: Whether to run the server in a separate thread
        """
        logger.info(f"Starting dashboard server on http://{self.host}:{self.port}")
        if use_threading:
            self.server_thread = threading.Thread(target=self._run_server, daemon=True)
            self.server_thread.start()
            return self.server_thread
        else:
            self._run_server()

    def _run_server(self) -> None:
        """Run the dashboard server."""
        self.app.run_server(host=self.host, port=self.port, debug=self.debug)


def launch_dashboard(
    performance_tracker: PerformanceTracker,
    portfolio_tracker: PortfolioTracker,
    host: str = "127.0.0.1",
    port: int = 8050,
    debug: bool = False,
    use_threading: bool = True,
) -> RealTimeDashboard:
    """
    Launch the real-time dashboard.

    Args:
        performance_tracker: Tracker containing performance data
        portfolio_tracker: Tracker containing portfolio data
        host: Host address to run the dashboard on
        port: Port to run the dashboard on
        debug: Whether to run in debug mode
        use_threading: Whether to run the server in a separate thread

    Returns:
        The dashboard instance, and if use_threading is True, the dashboard thread
    """
    dashboard = RealTimeDashboard(
        performance_tracker=performance_tracker,
        portfolio_tracker=portfolio_tracker,
        host=host,
        port=port,
        debug=debug,
    )

    thread = dashboard.start(use_threading=use_threading)

    if use_threading:
        return dashboard, thread
    else:
        return dashboard
