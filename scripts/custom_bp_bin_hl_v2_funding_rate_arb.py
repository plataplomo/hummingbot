from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd
from pydantic import BaseModel, Field, field_validator

from hummingbot.client.config.client_config_map import ClientConfigEnum
from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.event.events import FundingPaymentCompletedEvent
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class FundingRateArbitrageConfig(StrategyV2ConfigBase):
    script_file_name: str = Path(__file__).name
    candles_config: list[CandlesConfig] = []
    controllers_config: list[str] = []
    markets: dict[str, set[str]] = Field(default_factory=dict)
    leverage: int = Field(
        default=20, gt=0,
        json_schema_extra={"prompt": "Enter the leverage (e.g. 20): ", "prompt_on_new": True},
    )
    min_funding_rate_profitability: Decimal = Field(
        default=Decimal("0.001"),
        json_schema_extra={
            "prompt": "Enter the min funding rate profitability to enter in a position (e.g. 0.001): ",
            "prompt_on_new": True,
        },
    )
    connectors: set[str] = Field(
        default_factory=lambda: {"hyperliquid_perpetual", "binance_perpetual", "backpack_perpetual"},
        json_schema_extra={
            "prompt": "Enter the connectors separated by commas: ",
            "prompt_on_new": True,
        },
    )
    tokens: set[str] = Field(
        default_factory=lambda: {"WIF", "FET"},
        json_schema_extra={"prompt": "Enter the tokens separated by commas (e.g. WIF,FET): ", "prompt_on_new": True},
    )
    position_size_quote: Decimal = Field(
        default=Decimal("100"),
        json_schema_extra={
            "prompt": "Enter the position size in quote asset: ",
            "prompt_on_new": True,
        },
    )
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
    trade_profitability_condition_to_enter: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": "Do you want to check the trade profitability condition to enter? (True/False): ",
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
    quote_markets_map: ClassVar[dict[str, str]] = {
        "hyperliquid_perpetual": "USD",
        "binance_perpetual": "USDT",
        "backpack_perpetual": "USDC",
    }
    funding_payment_interval_map: ClassVar[dict[str, int]] = {
        "binance_perpetual": 60 * 60 * 8,
        "hyperliquid_perpetual": 60 * 60 * 1,
        "backpack_perpetual": 60 * 60 * 8,  # Backpack has funding every 8 hours
    }
    funding_profitability_interval = 60 * 60 * 24

    @classmethod
    def get_trading_pair_for_connector(cls, token: str, connector: str) -> str:
        return f"{token}-{cls.quote_markets_map.get(connector, 'USDT')}"

    @classmethod
    def init_markets(cls, config: BaseModel) -> dict[str, set[str]]:
        markets = {}
        if isinstance(config, FundingRateArbitrageConfig):
            for connector in config.connectors:
                trading_pairs = {cls.get_trading_pair_for_connector(token, connector) for token in config.tokens}
                markets[connector] = trading_pairs
        cls.markets = markets
        return markets

    def __init__(self, connectors: dict[str, ConnectorBase], config: FundingRateArbitrageConfig):
        super().__init__(connectors, config)
        self.active_funding_arbitrages: dict[str, tuple] = {}
        self.stopped_funding_arbitrages: dict[str, list[tuple]] = {}

    def on_tick(self, timestamp: float, clock: Clock) -> None:  # noqa: ARG002
        pass

    def on_stop(self):
        self.close_open_positions()

    def close_open_positions(self):
        # Close all open positions across all connectors
        for connector_name, connector in self.connectors.items():
            if self.is_perpetual(connector_name):
                positions = connector.account_positions
                if positions:
                    for trading_pair, position in positions.items():
                        if position.position_side == PositionSide.LONG:
                            self.sell(connector_name, trading_pair, abs(position.amount), OrderType.MARKET, position_action=PositionAction.CLOSE)
                        elif position.position_side == PositionSide.SHORT:
                            self.buy(connector_name, trading_pair, abs(position.amount), OrderType.MARKET, position_action=PositionAction.CLOSE)

    def apply_initial_setting(self):
        for connector_name, connector in self.connectors.items():
            if self.is_perpetual(connector_name):
                # Both Hyperliquid and Backpack only support ONEWAY mode
                if connector_name in ["hyperliquid_perpetual", "backpack_perpetual"]:
                    position_mode = PositionMode.ONEWAY
                else:
                    position_mode = PositionMode.HEDGE
                connector.set_position_mode(position_mode)
                config = self.config
                if isinstance(config, FundingRateArbitrageConfig):
                    for trading_pair in self.market_data_provider.get_trading_pairs(connector_name):
                        connector.set_leverage(trading_pair, config.leverage)

    def on_funding_payment_completed(self, event: FundingPaymentCompletedEvent, _: str):
        if event.trading_pair in self.active_funding_arbitrages:
            self.active_funding_arbitrages[event.trading_pair]["funding_payment"] += event.amount

    def is_perpetual(self, exchange: str):
        return "perpetual" in exchange

    def get_funding_rate(self, connector_name: str, trading_pair: str):
        connector = self.connectors[connector_name]
        funding_info = connector.funding_info.get(trading_pair)
        if funding_info:
            funding_rate = funding_info.rate
            next_timestamp = funding_info.next_funding_utc_timestamp
            return funding_rate, next_timestamp
        return Decimal("0"), 0

    def get_funding_payment_in_quote(self, connector_name: str, trading_pair: str):
        config = self.config
        if not isinstance(config, FundingRateArbitrageConfig):
            return Decimal("0")
        connector = self.connectors[connector_name]
        funding_rate, _ = self.get_funding_rate(connector_name, trading_pair)
        mid_price = connector.get_mid_price(trading_pair)
        amount = config.position_size_quote / mid_price
        return mid_price * amount * funding_rate

    def get_arbitrage_profitability_pct(self, funding_diff: Decimal, connector_long: str, trading_pair_long: str,
                                        connector_short: str, trading_pair_short: str):
        config = self.config
        if not isinstance(config, FundingRateArbitrageConfig):
            return Decimal("0"), Decimal("0")
        connector_long_instance = self.connectors[connector_long]
        connector_short_instance = self.connectors[connector_short]
        mid_price_long = connector_long_instance.get_mid_price(trading_pair_long)
        mid_price_short = connector_short_instance.get_mid_price(trading_pair_short)
        amount_long = config.position_size_quote / mid_price_long
        amount_short = config.position_size_quote / mid_price_short
        long_spread = connector_long_instance.get_order_price_spread(trading_pair_long, True, amount_long)
        short_spread = connector_short_instance.get_order_price_spread(trading_pair_short, False, amount_short)
        estimated_arb_profit_open_long = -long_spread
        estimated_arb_profit_open_short = -short_spread
        estimated_arb_profit_close_long = long_spread
        estimated_arb_profit_close_short = short_spread
        config = self.config
        if not isinstance(config, FundingRateArbitrageConfig):
            return Decimal("0"), Decimal("0")
        estimated_arb_profit_funding = funding_diff * self.funding_profitability_interval / self.funding_payment_interval_map[
            connector_long] * config.position_size_quote
        estimated_arb_profit_open = estimated_arb_profit_open_long + estimated_arb_profit_open_short + estimated_arb_profit_funding
        estimated_arb_profit_close = estimated_arb_profit_close_long + estimated_arb_profit_close_short
        return estimated_arb_profit_open / config.position_size_quote, estimated_arb_profit_close / config.position_size_quote

    def create_positions_for_funding_arbitrage(self, connector_long: str, trading_pair_long: str,
                                               connector_short: str, trading_pair_short: str):
        config = self.config
        if not isinstance(config, FundingRateArbitrageConfig):
            return []
        estimated_arb_profit_open_long = self.connectors[connector_long].get_order_price_spread(trading_pair_long, True,
                                                                                                config.position_size_quote / self.connectors[connector_long].get_mid_price(trading_pair_long))
        estimated_arb_profit_open_short = self.connectors[connector_short].get_order_price_spread(trading_pair_short, False,
                                                                                                  config.position_size_quote / self.connectors[connector_short].get_mid_price(trading_pair_short))
        estimated_arb_profit_close_long = -estimated_arb_profit_open_long
        estimated_arb_profit_close_short = -estimated_arb_profit_open_short

        long_controller_id = self.create_position_controller(
            connector_long,
            trading_pair_long,
            TradeType.BUY,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=Decimal("0.1"),
                take_profit=Decimal("0.5"),
            ),
        )
        short_controller_id = self.create_position_controller(
            connector_short,
            trading_pair_short,
            TradeType.SELL,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=Decimal("0.1"),
                take_profit=Decimal("0.5"),
            ),
        )
        self.active_funding_arbitrages[f"{connector_long}_{trading_pair_long}_{connector_short}_{trading_pair_short}"] = (
            long_controller_id, short_controller_id, estimated_arb_profit_open_long,
            estimated_arb_profit_open_short, estimated_arb_profit_close_long, estimated_arb_profit_close_short,
            {"funding_payment": Decimal("0")})
        return [long_controller_id, short_controller_id]

    def create_position_controller(self, connector_name: str, trading_pair: str, side: TradeType,
                                   triple_barrier_config: TripleBarrierConfig):
        config = self.config
        if not isinstance(config, FundingRateArbitrageConfig):
            return ""
        connector = self.connectors[connector_name]
        mid_price = connector.get_mid_price(trading_pair)
        amount = config.position_size_quote / mid_price
        position_config = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=connector_name,
            trading_pair=trading_pair,
            side=side,
            entry_price=mid_price,
            amount=amount,
            leverage=config.leverage,
            triple_barrier_config=triple_barrier_config
        )
        return self.controller.start_executor(position_config)

    def create_actions_proposal(self) -> list[CreateExecutorAction]:
        create_actions: list[CreateExecutorAction] = []
        config = self.config
        if not isinstance(config, FundingRateArbitrageConfig):
            return []
        for token in config.tokens:
            for connector_long in self.connectors:
                for connector_short in self.connectors:
                    if connector_long != connector_short:
                        trading_pair_long = self.get_trading_pair_for_connector(token, connector_long)
                        trading_pair_short = self.get_trading_pair_for_connector(token, connector_short)
                        funding_diff = self.compute_funding_diff(connector_long, trading_pair_long, connector_short,
                                                                 trading_pair_short)
                        if abs(funding_diff) > config.min_funding_rate_profitability:
                            estimated_arb_profit_open, estimated_arb_profit_close = self.get_arbitrage_profitability_pct(
                                funding_diff, connector_long, trading_pair_long, connector_short, trading_pair_short)
                            if config.trade_profitability_condition_to_enter and estimated_arb_profit_open > 0:
                                executors = self.create_positions_for_funding_arbitrage(connector_long, trading_pair_long,
                                                                                        connector_short, trading_pair_short)
                                for executor in executors:
                                    create_actions.append(
                                        CreateExecutorAction(
                                            executor_config=self.controller.get_executor(executor).config,
                                        )
                                    )
        return create_actions

    def compute_funding_diff(self, connector_long: str, trading_pair_long: str, connector_short: str,
                             trading_pair_short: str):
        funding_rate_long, _ = self.get_funding_rate(connector_long, trading_pair_long)
        funding_rate_short, _ = self.get_funding_rate(connector_short, trading_pair_short)
        funding_in_quote_long = self.get_funding_payment_in_quote(connector_long, trading_pair_long)
        funding_in_quote_short = self.get_funding_payment_in_quote(connector_short, trading_pair_short)
        normalized_funding_long = funding_in_quote_long * self.funding_profitability_interval / self.funding_payment_interval_map[connector_long]
        normalized_funding_short = funding_in_quote_short * self.funding_profitability_interval / self.funding_payment_interval_map[connector_short]
        return normalized_funding_long - normalized_funding_short

    def stop_actions_proposal(self) -> list[StopExecutorAction]:
        stop_actions = []
        for arb_id, executors in self.active_funding_arbitrages.items():
            connector_long, trading_pair_long, connector_short, trading_pair_short = arb_id.split("_")[:4]
            funding_diff = self.compute_funding_diff(connector_long, trading_pair_long, connector_short, trading_pair_short)
            estimated_arb_profit_open_long = executors[2]
            estimated_arb_profit_open_short = executors[3]
            estimated_arb_profit_close_long = executors[4]
            estimated_arb_profit_close_short = executors[5]
            total_profitability_pct = (
                estimated_arb_profit_open_long + estimated_arb_profit_open_short +
                estimated_arb_profit_close_long + estimated_arb_profit_close_short
            )
            config = self.config
            if isinstance(config, FundingRateArbitrageConfig):
                if (total_profitability_pct > config.profitability_to_take_profit or
                        funding_diff < config.funding_rate_diff_stop_loss):
                    for executor_id in executors[:2]:
                        stop_actions.append(
                            StopExecutorAction(
                                controller_id=executor_id,
                            )
                        )
                    if arb_id not in self.stopped_funding_arbitrages:
                        self.stopped_funding_arbitrages[arb_id] = []
                    self.stopped_funding_arbitrages[arb_id].append(executors)
                    del self.active_funding_arbitrages[arb_id]
        return stop_actions

    def market_data_extra_info(self):
        lines = []
        for connector_name in self.connectors:
            if self.is_perpetual(connector_name):
                funding_info = {}
                positions = {}
                for trading_pair in self.market_data_provider.get_trading_pairs(connector_name):
                    funding_rate, _ = self.get_funding_rate(connector_name, trading_pair)
                    funding_info[trading_pair] = funding_rate
                    if hasattr(self.connectors[connector_name], "account_positions"):
                        positions[trading_pair] = self.connectors[connector_name].account_positions.get(trading_pair)
                funding_rate_status = []
                funding_rate_status.append(f"Exchange: {connector_name}")
                for trading_pair, rate in funding_info.items():
                    funding_rate_status.append(f"{trading_pair}: {rate:.6f}")
                    if positions.get(trading_pair):
                        position = positions[trading_pair]
                        funding_rate_status.append(f"  Position: {position.position_side} {position.amount}")
                lines.extend(funding_rate_status)
        return lines

    def format_status(self) -> str:
        lines = []
        for connector_name in self.connectors:
            # Format active arbitrages
            if self.active_funding_arbitrages:
                lines.append("\nActive Funding Arbitrages:")
                for arb_id in self.active_funding_arbitrages:
                    lines.append(f"  {arb_id}")

            # Show funding rates
            config = self.config
            if isinstance(config, FundingRateArbitrageConfig):
                funding_rate_status = []
                funding_rate_status.extend([
                    f"| Connector: {connector_name} | Tokens: {config.tokens} | ",
                    f"Min Funding Rate: {config.min_funding_rate_profitability * 100:.2f}% |",
                    f"| Trade PnL condition: {config.profitability_to_take_profit * 100:.2f}% |",
                ])

                # Show dataframes if they exist
                if self.active_funding_arbitrages or self.stopped_funding_arbitrages:
                    df_active = pd.DataFrame(self.active_funding_arbitrages)
                    df_stopped = pd.DataFrame(self.stopped_funding_arbitrages)
                    lines.append(format_df_for_printout(df=df_active, table_format=ClientConfigEnum.TABLE_FORMAT))
                    lines.append(format_df_for_printout(df=df_stopped, table_format=ClientConfigEnum.TABLE_FORMAT))
                else:
                    connector_long = connector_short = connector_name
                    trading_pair_long = trading_pair_short = list(self.market_data_provider.get_trading_pairs(connector_name))[0] if self.market_data_provider.get_trading_pairs(connector_name) else ""

                    if trading_pair_long and trading_pair_short:
                        estimated_arb_profit_open_long = self.connectors[connector_long].get_order_price_spread(
                            trading_pair_long, True, Decimal("100") / self.connectors[connector_long].get_mid_price(trading_pair_long)
                        )
                        estimated_arb_profit_open_short = self.connectors[connector_short].get_order_price_spread(
                            trading_pair_short, False, Decimal("100") / self.connectors[connector_short].get_mid_price(trading_pair_short)
                        )
                        estimated_arb_profit_close_long = -estimated_arb_profit_open_long
                        estimated_arb_profit_close_short = -estimated_arb_profit_open_short

                        funding_rate_status.extend([
                            f"No funding arbitrages in {connector_name}",
                            f"Open conditions: Trade profitability higher than "
                            f"{abs((estimated_arb_profit_open_short + estimated_arb_profit_open_long) * 100):.2f}% |",
                            f"| Close condition: Trade profitability higher than "
                            f"{abs((estimated_arb_profit_close_short + estimated_arb_profit_close_long) * 100):.2f}% |",
                        ])

                lines.extend(funding_rate_status)

        return "\n".join(lines)
