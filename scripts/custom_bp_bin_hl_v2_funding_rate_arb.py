"""
Funding Rate Arbitrage Strategy V2 - Refactored with Custom Executor
Properly implements V2 architecture with atomic position management
"""
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, Field, field_validator

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionMode
from hummingbot.core.event.events import FundingPaymentCompletedEvent
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.funding_arbitrage_executor.data_types import FundingArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class FundingRateArbitrageConfig(StrategyV2ConfigBase):
    """Configuration for the funding rate arbitrage strategy"""
    script_file_name: str = Path(__file__).name
    candles_config: list[CandlesConfig] = []  # noqa: RUF012
    controllers_config: list[str] = []  # noqa: RUF012

    # Market configuration
    connectors: set[str] = Field(
        default_factory=lambda: {"hyperliquid_perpetual", "binance_perpetual", "backpack_perpetual"},
        json_schema_extra={
            "prompt": "Enter the connectors separated by commas: ",
            "prompt_on_new": True,
        },
    )
    tokens: set[str] = Field(
        default_factory=lambda: {"WIF", "FET"},
        json_schema_extra={
            "prompt": "Enter the tokens separated by commas (e.g. WIF,FET): ",
            "prompt_on_new": True,
        },
    )

    # Position sizing
    position_size_quote: Decimal = Field(
        default=Decimal(100),
        json_schema_extra={
            "prompt": "Enter the position size in quote asset: ",
            "prompt_on_new": True,
        },
    )
    leverage: int = Field(
        default=20,
        gt=0,
        json_schema_extra={
            "prompt": "Enter the leverage (e.g. 20): ",
            "prompt_on_new": True,
        },
    )

    # Entry conditions
    min_funding_rate_profitability: Decimal = Field(
        default=Decimal("0.001"),
        json_schema_extra={
            "prompt": "Enter the min funding rate profitability to enter in a position (e.g. 0.001): ",
            "prompt_on_new": True,
        },
    )
    trade_profitability_condition_to_enter: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": "Do you want to check the trade profitability condition to enter? (True/False): ",
            "prompt_on_new": True,
        },
    )

    # Exit conditions
    profitability_to_take_profit: Decimal = Field(
        default=Decimal("0.01"),
        json_schema_extra={
            "prompt": "Enter the profitability to take profit: ",
            "prompt_on_new": True,
        },
    )
    funding_rate_diff_stop_loss: Decimal = Field(
        default=Decimal("-0.001"),
        json_schema_extra={
            "prompt": "Enter the funding rate difference to stop the position (e.g. -0.001): ",
            "prompt_on_new": True,
        },
    )

    # Safety parameters for unhedged exposure
    max_exposure_time: int = Field(
        default=30,
        json_schema_extra={
            "prompt": "Max seconds of unhedged exposure before emergency action (e.g. 30): ",
            "prompt_on_new": True,
        },
    )
    exposure_warning_time: int = Field(
        default=10,
        json_schema_extra={
            "prompt": "Seconds of exposure before warning (e.g. 10): ",
            "prompt_on_new": True,
        },
    )
    emergency_use_market_orders: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Use market orders for emergency hedge? (True/False): ",
            "prompt_on_new": True,
        },
    )

    # Order configuration
    open_order_type: OrderType = Field(
        default=OrderType.LIMIT,
        json_schema_extra={
            "prompt": "Order type for opening positions (LIMIT/MARKET): ",
            "prompt_on_new": True,
        },
    )
    close_order_type: OrderType = Field(
        default=OrderType.MARKET,
        json_schema_extra={
            "prompt": "Order type for closing positions (LIMIT/MARKET): ",
            "prompt_on_new": True,
        },
    )

    # Time limits
    position_time_limit: int = Field(
        default=86400,  # 24 hours
        json_schema_extra={
            "prompt": "Max time to hold positions in seconds (e.g. 86400 for 24h): ",
            "prompt_on_new": True,
        },
    )

    @field_validator("connectors", "tokens", mode="before")
    @classmethod
    def validate_sets(cls, v: Any) -> set[str]:
        if isinstance(v, str):
            return set(v.split(","))
        return v


class FundingRateArbitrage(StrategyV2Base):
    """
    V2-compliant funding rate arbitrage strategy using custom executor for atomic position management
    """

    # Exchange configuration
    quote_markets_map: ClassVar[dict[str, str]] = {
        "hyperliquid_perpetual": "USD",
        "binance_perpetual": "USDT",
        "backpack_perpetual": "USDC",
    }

    funding_payment_interval_map: ClassVar[dict[str, int]] = {
        "binance_perpetual": 60 * 60 * 8,
        "hyperliquid_perpetual": 60 * 60 * 1,
        "backpack_perpetual": 60 * 60 * 8,
    }

    funding_profitability_interval: ClassVar[int] = 60 * 60 * 24  # 24 hours

    @classmethod
    def get_trading_pair_for_connector(cls, token: str, connector: str) -> str:
        """Get the trading pair format for a specific connector"""
        return f"{token}-{cls.quote_markets_map.get(connector, 'USDT')}"

    @classmethod
    def init_markets(cls, config: BaseModel) -> dict[str, set[str]]:
        """Initialize markets for the strategy"""
        markets = {}
        if isinstance(config, FundingRateArbitrageConfig):
            for connector in config.connectors:
                trading_pairs = {
                    cls.get_trading_pair_for_connector(token, connector)
                    for token in config.tokens
                }
                markets[connector] = trading_pairs
        cls.markets = markets
        return markets

    def __init__(self, connectors: dict[str, ConnectorBase], config: FundingRateArbitrageConfig):
        super().__init__(connectors, config)
        self.config: FundingRateArbitrageConfig = config
        self.logger().warning("⚠️ DEPRECATED: This direct strategy is for testing only!")
        self.logger().warning("⚠️ For production, use v2_funding_arbitrage_with_controller.py")
        self.logger().info("🚀 FundingRateArbitrage V2 strategy initialized (TEST MODE)")

    def on_tick(self) -> None:
        """Main strategy tick - framework handles executor lifecycle"""
        super().on_tick()

    def start(self, clock, timestamp: float) -> None:
        """Start the strategy and apply initial settings"""
        self.logger().info("🚀 Strategy starting...")
        self.apply_initial_settings()
        self.logger().info("✅ Strategy started successfully")

    async def on_stop(self):
        """Stop the strategy - framework handles executor cleanup"""
        self.logger().info("🛑 Strategy stopping...")
        await super().on_stop()
        self.logger().info("✅ Strategy stopped")

    def apply_initial_settings(self):
        """Apply initial exchange settings"""
        self.logger().info("⚙️ Applying initial settings...")

        for connector_name, connector in self.connectors.items():
            if self.is_perpetual(connector_name):
                # Set position mode
                if connector_name in ["hyperliquid_perpetual", "backpack_perpetual"]:
                    position_mode = PositionMode.ONEWAY
                else:
                    position_mode = PositionMode.HEDGE

                self.logger().info(f"  Setting {connector_name} to {position_mode} mode")
                connector.set_position_mode(position_mode)

                # Set leverage
                trading_pairs = self.market_data_provider.get_trading_pairs(connector_name)
                self.logger().info(
                    f"  Setting leverage for {connector_name}: "
                    f"{self.config.leverage}x on {len(trading_pairs)} pairs",
                )

                for trading_pair in trading_pairs:
                    connector.set_leverage(trading_pair, self.config.leverage)

    def create_actions_proposal(self) -> list[CreateExecutorAction]:
        """
        Propose new executor actions based on funding rate opportunities.
        Uses custom FundingArbitrageExecutor for atomic position management.
        """
        create_actions: list[CreateExecutorAction] = []

        # Get active executors from framework
        active_executors = self.get_all_executors()

        # Get tokens currently being traded
        active_tokens = set()
        for executor_info in active_executors:
            # Check if this is a funding arbitrage executor
            if (
                executor_info.type == "funding_arbitrage_executor"
                and executor_info.is_active
                and hasattr(executor_info.config, "token")
            ):
                active_tokens.add(executor_info.config.token)

        # Check each token for opportunities
        for token in self.config.tokens:
            # Skip if already trading this token
            if token in active_tokens:
                continue

            # Find best opportunity
            opportunity = self.get_best_funding_arbitrage_opportunity(token)
            if not opportunity:
                continue

            connector_long, tp_long, connector_short, tp_short, funding_diff = opportunity

            # Check minimum profitability
            if abs(funding_diff) <= self.config.min_funding_rate_profitability:
                continue

            # Skip price divergence check if configured
            # The executor will handle detailed profitability calculations with fees
            if self.config.trade_profitability_condition_to_enter:
                # Simple price check - executor will do detailed analysis
                long_price = self.connectors[connector_long].get_mid_price(tp_long)
                short_price = self.connectors[connector_short].get_mid_price(tp_short)
                price_diff_pct = (short_price - long_price) / long_price
                if price_diff_pct < -Decimal("0.002"):  # More than 0.2% price divergence
                    self.logger().debug(
                        f"Skipping {token}: excessive price divergence {price_diff_pct:.4%}",
                    )
                    continue

            # Log opportunity
            self.logger().info(
                f"💰 ARBITRAGE: {token} | {connector_long} vs {connector_short} | "
                f"Funding: {funding_diff:.3%}",
            )

            # Create executor configuration
            executor_config = self.create_funding_arbitrage_executor_config(
                token, connector_long, tp_long, connector_short, tp_short, funding_diff,
            )

            # Create action to start the executor
            create_actions.append(CreateExecutorAction(
                executor_config=executor_config,
            ))

        return create_actions

    def stop_actions_proposal(self) -> list[StopExecutorAction]:
        """
        Propose stop actions for executors that meet exit criteria.
        The custom executor handles unhedged exposure internally.
        """
        stop_actions: list[StopExecutorAction] = []

        # Get all executors from framework
        executors = self.get_all_executors()

        for executor_info in executors:
            # Check if this is a funding arbitrage executor
            if executor_info.type != "funding_arbitrage_executor":
                continue

            if not executor_info.is_active:
                continue

            # Check if executor should stop based on strategy criteria
            should_stop = False
            stop_reason = ""

            # Get config for type checking
            config = executor_info.config

            # Check funding rate stop loss
            # Type narrow to FundingArbitrageExecutorConfig
            if isinstance(config, FundingArbitrageExecutorConfig):
                funding_diff = self.compute_funding_diff(
                    config.long_connector_name,
                    config.long_trading_pair,
                    config.short_connector_name,
                    config.short_trading_pair,
                )

                if funding_diff < self.config.funding_rate_diff_stop_loss:
                    should_stop = True
                    stop_reason = f"Funding below threshold: {funding_diff:.4%}"

            # Check take profit (executor tracks its own PnL)
            if executor_info.net_pnl_quote > self.config.profitability_to_take_profit * self.config.position_size_quote:
                should_stop = True
                stop_reason = f"Take profit: {executor_info.net_pnl_quote:.2f} quote"

            # Create stop action if needed
            if should_stop and hasattr(config, "token"):
                self.logger().info(
                    f"🛑 Closing {config.token}: {stop_reason}",
                )
                stop_actions.append(StopExecutorAction(executor_id=executor_info.id))

        return stop_actions

    def create_funding_arbitrage_executor_config(
        self,
        token: str,
        connector_long: str,
        trading_pair_long: str,
        connector_short: str,
        trading_pair_short: str,
        funding_diff: Decimal,
    ) -> FundingArbitrageExecutorConfig:
        """Create configuration for the funding arbitrage executor"""

        # Create triple barrier config for exit conditions
        triple_barrier_config = TripleBarrierConfig(
            stop_loss=Decimal("0.05"),  # 5% stop loss
            take_profit=None,  # Managed by strategy based on funding + PnL
            time_limit=self.config.position_time_limit,
            take_profit_order_type=self.config.close_order_type,
            stop_loss_order_type=OrderType.MARKET,  # Always market for stop loss
            time_limit_order_type=OrderType.MARKET,
        )

        # Create executor config
        return FundingArbitrageExecutorConfig(
            controller_id=f"funding_arb_{token}",
            timestamp=self.current_timestamp,  # Use framework timestamp
            token=token,
            long_connector_name=connector_long,
            long_trading_pair=trading_pair_long,
            short_connector_name=connector_short,
            short_trading_pair=trading_pair_short,
            position_size_quote=self.config.position_size_quote,
            leverage=self.config.leverage,
            open_order_type=self.config.open_order_type,
            entry_timeout=self.config.max_exposure_time,
            triple_barrier_config=triple_barrier_config,
            max_unhedged_exposure_time=self.config.max_exposure_time,
            warning_exposure_time=self.config.exposure_warning_time,
            emergency_use_market_orders=self.config.emergency_use_market_orders,
            min_funding_rate_diff=self.config.min_funding_rate_profitability,
            funding_rate_diff_stop_loss=self.config.funding_rate_diff_stop_loss,
        )

    def get_best_funding_arbitrage_opportunity(
        self, token: str,
    ) -> tuple[str, str, str, str, Decimal] | None:
        """Find the best funding arbitrage opportunity for a token"""
        best_opportunity = None
        highest_profitability = Decimal(0)

        for connector_long in self.connectors:
            for connector_short in self.connectors:
                if connector_long == connector_short:
                    continue

                trading_pair_long = self.get_trading_pair_for_connector(token, connector_long)
                trading_pair_short = self.get_trading_pair_for_connector(token, connector_short)

                # Calculate funding rate difference
                funding_diff = self.compute_funding_diff(
                    connector_long, trading_pair_long,
                    connector_short, trading_pair_short,
                )

                if abs(funding_diff) > highest_profitability:
                    highest_profitability = abs(funding_diff)
                    best_opportunity = (
                        connector_long, trading_pair_long,
                        connector_short, trading_pair_short,
                        funding_diff,
                    )

        return best_opportunity

    def compute_funding_diff(
        self,
        connector_long: str,
        trading_pair_long: str,
        connector_short: str,
        trading_pair_short: str,
    ) -> Decimal:
        """
        Compute funding rate difference for arbitrage profitability.
        Returns positive value if profitable to LONG on connector_long and SHORT on connector_short.
        """
        # Get normalized funding rates
        rate_long = self.get_normalized_funding_rate_in_seconds(connector_long, trading_pair_long)
        rate_short = self.get_normalized_funding_rate_in_seconds(connector_short, trading_pair_short)

        # Calculate difference over profitability interval
        funding_diff = (rate_short - rate_long) * self.funding_profitability_interval

        return funding_diff

    def get_normalized_funding_rate_in_seconds(self, connector_name: str, trading_pair: str) -> Decimal:
        """Get funding rate normalized to per-second basis"""
        funding_rate, _ = self.get_funding_rate(connector_name, trading_pair)
        payment_interval = self.funding_payment_interval_map.get(connector_name, 60 * 60 * 8)
        return funding_rate / payment_interval

    def get_funding_rate(self, connector_name: str, trading_pair: str) -> tuple[Decimal, int]:
        """Get funding rate and next payment timestamp"""
        connector = self.connectors[connector_name]
        funding_info = connector.get_funding_info(trading_pair)

        if funding_info and funding_info.rate is not None:
            return funding_info.rate, funding_info.next_funding_utc_timestamp

        return Decimal(0), 0

    def on_funding_payment_completed(self, event: FundingPaymentCompletedEvent, _: str):
        """Handle funding payment events - forward to appropriate executor"""
        # Note: In V2 architecture, executors are managed by the framework
        # We can track funding payments here for strategy-level metrics
        self.logger().info(
            f"💰 Funding payment: {event.market} {event.trading_pair} - Amount: {event.amount:.6f}",
        )

    @staticmethod
    def is_perpetual(exchange: str) -> bool:
        """Check if exchange is a perpetual market"""
        return "perpetual" in exchange

    def market_data_extra_info(self) -> list[str]:
        """Return market data information for display."""
        lines = []

        # Show active arbitrages summary
        executors = self.get_all_executors()
        active_funding_arbs = [
            e for e in executors
            if e.type == "funding_arbitrage_executor" and e.is_active
        ]

        if active_funding_arbs:
            lines.append(f"Active Arbitrages: {len(active_funding_arbs)}")
            for executor_info in active_funding_arbs[:3]:  # Show max 3
                config = executor_info.config
                if isinstance(config, FundingArbitrageExecutorConfig):
                    lines.append(
                        f"  {config.token}: "
                        f"{config.long_connector_name} vs "
                        f"{config.short_connector_name}",
                    )

        return lines

    def format_status(self) -> str:
        """Format strategy status for display"""
        lines = []
        lines.append("=== Funding Rate Arbitrage V2 ===")

        # Show active executors
        executors = self.get_all_executors()
        active_count = sum(
            1 for e in executors
            if e.type == "funding_arbitrage_executor" and e.is_active
        )

        if active_count > 0:
            lines.append(f"Active Arbitrages: {active_count}")

            for executor_info in executors:
                if executor_info.type == "funding_arbitrage_executor" and executor_info.is_active:
                    config = executor_info.config
                    if isinstance(config, FundingArbitrageExecutorConfig):
                        lines.append(
                            f"  {config.token}: "
                            f"{config.long_connector_name if hasattr(config, 'long_connector_name') else 'N/A'} vs "
                            f"{config.short_connector_name if hasattr(config, 'short_connector_name') else 'N/A'} | "
                            f"PnL: {executor_info.net_pnl_quote:.2f} | "
                            f"Trading: {'✅' if executor_info.is_trading else '⚠️'}",
                        )
        else:
            lines.append("No active arbitrages")

        return "\n".join(lines)
