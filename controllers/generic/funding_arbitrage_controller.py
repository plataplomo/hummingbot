"""
Funding Arbitrage Controller - Portfolio-level management for multi-token funding arbitrage
Manages capital allocation, opportunity ranking, and global risk across multiple funding arbitrage positions
"""
from dataclasses import dataclass
from decimal import Decimal

from pydantic import Field

from hummingbot.core.data_type.common import OrderType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.funding_arbitrage_executor import FundingArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


@dataclass
class FundingOpportunity:
    """Represents a funding arbitrage opportunity"""
    token: str
    long_exchange: str
    short_exchange: str
    long_funding_rate: Decimal
    short_funding_rate: Decimal
    spread: Decimal
    expected_profit: Decimal
    required_capital: Decimal

    @property
    def opportunity_id(self) -> str:
        return f"{self.token}_{self.long_exchange}_{self.short_exchange}"


class FundingArbitrageControllerConfig(ControllerConfigBase):
    """Configuration for the funding arbitrage controller"""
    controller_name: str = "funding_arbitrage_controller"
    controller_type: str = "generic"

    # Exchanges and tokens
    connectors: list[str] = Field(
        default=["backpack_perpetual", "binance_perpetual"],
        json_schema_extra={
            "prompt": "Enter connectors (comma-separated): ",
            "prompt_on_new": True,
        },
    )
    tokens: list[str] = Field(
        default=["BTC", "ETH", "SOL"],
        json_schema_extra={
            "prompt": "Enter tokens to trade (comma-separated): ",
            "prompt_on_new": True,
        },
    )

    # Position sizing and capital management
    position_size_quote: Decimal = Field(
        default=Decimal(50),
        json_schema_extra={
            "prompt": "Position size per trade in quote currency: ",
            "prompt_on_new": True,
        },
    )
    max_total_exposure: Decimal = Field(
        default=Decimal(500),
        json_schema_extra={
            "prompt": "Maximum total portfolio exposure: ",
            "prompt_on_new": True,
        },
    )
    max_positions: int = Field(
        default=5,
        json_schema_extra={
            "prompt": "Maximum number of concurrent positions: ",
            "prompt_on_new": True,
        },
    )

    # Entry conditions
    min_funding_rate_profitability: Decimal = Field(
        default=Decimal("0.002"),
        json_schema_extra={
            "prompt": "Minimum funding rate spread to enter (e.g. 0.002 for 0.2%): ",
            "prompt_on_new": True,
        },
    )
    trade_profitability_condition_to_enter: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Check immediate trade profitability before entering? ",
            "prompt_on_new": True,
        },
    )

    # Risk management
    profitability_to_take_profit: Decimal = Field(
        default=Decimal("0.01"),
        json_schema_extra={
            "prompt": "Take profit threshold (e.g. 0.01 for 1%): ",
            "prompt_on_new": True,
        },
    )
    funding_rate_diff_stop_loss: Decimal = Field(
        default=Decimal("-0.005"),
        json_schema_extra={
            "prompt": "Stop loss on funding rate differential: ",
            "prompt_on_new": True,
        },
    )
    position_time_limit: int = Field(
        default=86400,
        json_schema_extra={
            "prompt": "Maximum position hold time in seconds: ",
            "prompt_on_new": True,
        },
    )

    # Reconciliation safety
    enable_reconciliation: bool = Field(default=True)
    reconciliation_interval: int = Field(default=5)
    exposure_warning_time: int = Field(default=10)
    max_exposure_time: int = Field(default=30)
    emergency_cancel_unfilled: bool = Field(default=True)
    emergency_use_market_orders: bool = Field(default=True)

    # Leverage
    leverage: int = Field(
        default=10,
        json_schema_extra={
            "prompt": "Leverage to use: ",
            "prompt_on_new": True,
        },
    )


class FundingArbitrageController(ControllerBase):
    """
    Controller for managing multiple funding arbitrage positions across tokens and exchanges.

    Responsibilities:
    - Scan all token-exchange pairs for funding opportunities
    - Rank opportunities by expected profitability
    - Allocate capital intelligently across best opportunities
    - Enforce global risk limits and exposure controls
    - Coordinate reconciliation and emergency hedging
    """

    def __init__(self, config: FundingArbitrageControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config: FundingArbitrageControllerConfig = config

        # Track active opportunities
        self._active_opportunities: dict[str, FundingOpportunity] = {}
        self._allocated_capital: Decimal = Decimal(0)
        self._last_scan_time: float = 0
        self._scan_interval: float = 60.0  # Scan for opportunities every minute

    async def update_processed_data(self):
        """Scan market for funding arbitrage opportunities"""
        current_time = self.market_data_provider.time()

        # Only scan periodically to avoid excessive API calls
        if current_time - self._last_scan_time < self._scan_interval:
            return

        self._last_scan_time = current_time

        # Scan all token-exchange combinations for opportunities
        opportunities = await self._scan_funding_opportunities()

        # Rank opportunities by expected profitability
        ranked_opportunities = sorted(
            opportunities,
            key=lambda x: x.expected_profit,
            reverse=True,
        )

        # Store processed opportunities
        self.processed_data["opportunities"] = ranked_opportunities
        self.processed_data["available_capital"] = self._calculate_available_capital()

    async def _scan_funding_opportunities(self) -> list[FundingOpportunity]:
        """Scan all token-exchange pairs for funding rate arbitrage opportunities"""
        opportunities = []

        for token in self.config.tokens:
            # Get funding rates from all exchanges for this token
            funding_rates = {}
            for connector in self.config.connectors:
                try:
                    # Get funding rate for token on this exchange
                    symbol = f"{token}-USDT"
                    funding_info = self.market_data_provider.get_funding_info(
                        connector_name=connector,
                        trading_pair=symbol,
                    )
                    if funding_info:
                        funding_rates[connector] = funding_info.rate
                except Exception as e:
                    self.logger().warning(f"Failed to get funding for {token} on {connector}: {e}")

            # Find best long/short combination
            if len(funding_rates) >= 2:
                opportunity = self._find_best_opportunity(token, funding_rates)
                if opportunity and opportunity.spread >= self.config.min_funding_rate_profitability:
                    opportunities.append(opportunity)

        return opportunities

    def _find_best_opportunity(self, token: str, funding_rates: dict[str, Decimal]) -> FundingOpportunity | None:
        """Find the best long/short exchange combination for a token"""
        best_opportunity = None
        best_spread = Decimal(0)

        exchanges = list(funding_rates.keys())
        for i, long_exchange in enumerate(exchanges):
            for short_exchange in exchanges[i + 1:]:
                # Calculate spread (short funding - long funding)
                spread = funding_rates[short_exchange] - funding_rates[long_exchange]

                if spread > best_spread:
                    best_spread = spread
                    best_opportunity = FundingOpportunity(
                        token=token,
                        long_exchange=long_exchange,
                        short_exchange=short_exchange,
                        long_funding_rate=funding_rates[long_exchange],
                        short_funding_rate=funding_rates[short_exchange],
                        spread=spread,
                        expected_profit=spread * self.config.position_size_quote,
                        required_capital=self.config.position_size_quote * 2,  # Capital for both legs
                    )

        return best_opportunity

    def _calculate_available_capital(self) -> Decimal:
        """Calculate available capital for new positions"""
        # Get allocated capital from active executors
        allocated = Decimal(0)
        for executor_info in self.executors_info:
            if executor_info.status == RunnableStatus.RUNNING:
                allocated += self.config.position_size_quote * 2  # Both legs

        return self.config.max_total_exposure - allocated

    def determine_executor_actions(self) -> list[ExecutorAction]:
        """Determine which executors to create/stop based on opportunities and capital"""
        actions = []

        # Get current opportunities and available capital
        opportunities = self.processed_data.get("opportunities", [])
        available_capital = self.processed_data.get("available_capital", Decimal(0))

        # Count active positions
        active_executors = [e for e in self.executors_info if e.status == RunnableStatus.RUNNING]
        active_count = len(active_executors)

        # Check if we should close any positions (poor performance or better opportunities available)
        actions.extend(self._check_positions_to_close(active_executors, opportunities))

        # Create new executors for best opportunities within capital limits
        if active_count < self.config.max_positions:
            for opportunity in opportunities:
                # Check if we already have this position
                if self._is_opportunity_active(opportunity):
                    continue

                # Check capital availability
                if opportunity.required_capital > available_capital:
                    break  # No more capital for additional positions

                # Create executor for this opportunity
                action = self._create_executor_action(opportunity)
                if action:
                    actions.append(action)
                    available_capital -= opportunity.required_capital
                    active_count += 1

                    # Check position limit
                    if active_count >= self.config.max_positions:
                        break

        return actions

    def _is_opportunity_active(self, opportunity: FundingOpportunity) -> bool:
        """Check if we already have an active executor for this opportunity"""
        for executor_info in self.executors_info:
            if executor_info.status == RunnableStatus.RUNNING:
                config = executor_info.config
                if (config.token == opportunity.token and
                    config.long_connector_name == opportunity.long_exchange and
                        config.short_connector_name == opportunity.short_exchange):
                    return True
        return False

    def _check_positions_to_close(self, active_executors, new_opportunities) -> list[ExecutorAction]:
        """Check if any positions should be closed to make room for better opportunities"""
        actions = []

        # If we're at max positions and have better opportunities, close worst performers
        if len(active_executors) >= self.config.max_positions and new_opportunities:
            best_new_profit = new_opportunities[0].expected_profit if new_opportunities else Decimal(0)

            # Find worst performing executor
            worst_executor = None
            worst_performance = best_new_profit  # Only close if new opportunity is better

            for executor_info in active_executors:
                # Get executor's current performance
                if hasattr(executor_info, "net_pnl_pct"):
                    performance = executor_info.net_pnl_pct
                    if performance < worst_performance:
                        worst_performance = performance
                        worst_executor = executor_info

            # Close worst performer if found
            if worst_executor:
                actions.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=worst_executor.id,
                ))

        return actions

    def _create_executor_action(self, opportunity: FundingOpportunity) -> CreateExecutorAction | None:
        """Create an executor action for a funding arbitrage opportunity"""
        try:
            # Create triple barrier config for risk management
            triple_barrier = TripleBarrierConfig(
                take_profit=self.config.profitability_to_take_profit,
                stop_loss=abs(self.config.funding_rate_diff_stop_loss),
                time_limit=self.config.position_time_limit,
            )

            # Create executor config
            executor_config = FundingArbitrageExecutorConfig(
                controller_id=self.config.id,
                timestamp=self.market_data_provider.time(),
                token=opportunity.token,
                long_connector_name=opportunity.long_exchange,
                short_connector_name=opportunity.short_exchange,
                long_trading_pair=f"{opportunity.token}-USDT",
                short_trading_pair=f"{opportunity.token}-USDT",
                position_size_quote=self.config.position_size_quote,
                leverage=self.config.leverage,
                triple_barrier_config=triple_barrier,
                min_funding_rate_profitability=self.config.min_funding_rate_profitability,
                trade_profitability_condition_to_enter=self.config.trade_profitability_condition_to_enter,
                funding_rate_diff_stop_loss=self.config.funding_rate_diff_stop_loss,
                max_exposure_time=self.config.max_exposure_time,
                exposure_warning_time=self.config.exposure_warning_time,
                emergency_use_market_orders=self.config.emergency_use_market_orders,
                open_order_type=OrderType.LIMIT,
                close_order_type=OrderType.MARKET,
            )

            return CreateExecutorAction(
                executor_config=executor_config,
                controller_id=self.config.id,
            )

        except Exception as e:
            self.logger().error(f"Failed to create executor for {opportunity.token}: {e}")
            return None

    def to_format_status(self) -> list[str]:
        """Format controller status for display"""
        lines = []

        # Portfolio overview
        active_executors = [e for e in self.executors_info if e.status == RunnableStatus.RUNNING]
        allocated_capital = len(active_executors) * self.config.position_size_quote * 2
        available_capital = self.config.max_total_exposure - allocated_capital

        lines.extend([
            "=== Funding Arbitrage Controller Status ===",
            f"Active Positions: {len(active_executors)}/{self.config.max_positions}",
            f"Allocated Capital: ${allocated_capital:.2f}",
            f"Available Capital: ${available_capital:.2f}",
            f"Total Exposure Limit: ${self.config.max_total_exposure:.2f}",
        ])

        # Current opportunities
        opportunities = self.processed_data.get("opportunities", [])
        if opportunities:
            lines.append("\n=== Top Opportunities ===")
            for i, opp in enumerate(opportunities[:5], 1):
                lines.append(
                    f"{i}. {opp.token}: {opp.long_exchange} (long) / {opp.short_exchange} (short) | "
                    f"Spread: {opp.spread:.4%} | Expected Profit: ${opp.expected_profit:.2f}",
                )

        # Active positions performance
        if active_executors:
            lines.append("\n=== Active Positions ===")
            total_pnl = Decimal(0)
            for executor in active_executors:
                config = executor.config
                pnl = getattr(executor, "net_pnl_quote", Decimal(0))
                total_pnl += pnl
                lines.append(
                    f"- {config.token} ({config.long_connector_name}/{config.short_connector_name}): "
                    f"PnL: ${pnl:.2f}",
                )
            lines.append(f"Total Portfolio PnL: ${total_pnl:.2f}")

        return lines
