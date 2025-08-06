"""Pre-built test scenarios using symbols.

These scenarios provide complete test setups for common
testing patterns in the trading engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import (
    DerivativePosition,
    FundingRate,
    Order,
    SpotBalance,
    Ticker,
)
from cyberdelta.symbols import Symbol, exchanges, symbols


@dataclass
class FundingArbScenario:
    """Complete funding arbitrage test scenario."""

    symbols: dict[str, tuple[Symbol, Symbol]]
    funding_rates: dict[Symbol, Decimal]
    positions: dict[Symbol, Decimal]
    prices: dict[Symbol, Decimal] = field(default_factory=lambda: cast(dict[Symbol, Decimal], {}))
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class MarketMakingScenario:
    """Market making test scenario."""

    symbol: Symbol
    mid_price: Decimal
    spread: Decimal
    inventory: Decimal
    orders: list[Order]
    risk_limits: dict[str, Any]


@dataclass
class PositionManagementScenario:
    """Position management test scenario."""

    positions: dict[Symbol, DerivativePosition]
    balances: dict[str, SpotBalance]
    risk_metrics: dict[str, Decimal]
    orders: dict[Symbol, list[Order]]


class SymbolTestScenarios:
    """Pre-built test scenarios using symbols."""

    @staticmethod
    def create_funding_arbitrage_scenario(
        include_sol: bool = False,
    ) -> FundingArbScenario:
        """Create complete funding arbitrage test scenario.

        Args:
            include_sol: Whether to include SOL in the scenario

        Returns:
            FundingArbScenario: Complete test scenario
        """
        scenario_symbols = {
            "BTC": (symbols.BTC.hyperliquid(), symbols.BTC.backpack()),
            "ETH": (symbols.ETH.hyperliquid(), symbols.ETH.backpack()),
        }

        if include_sol:
            scenario_symbols["SOL"] = (symbols.SOL.hyperliquid(), symbols.SOL.backpack())

        # Funding rates with arbitrage opportunity
        funding_rates = {
            symbols.BTC.hyperliquid(): Decimal("0.01"),  # 1% on HL
            symbols.BTC.backpack(): Decimal("-0.005"),  # -0.5% on BP
            symbols.ETH.hyperliquid(): Decimal("0.008"),  # 0.8% on HL
            symbols.ETH.backpack(): Decimal("-0.002"),  # -0.2% on BP
        }

        if include_sol:
            funding_rates[symbols.SOL.hyperliquid()] = Decimal("0.015")
            funding_rates[symbols.SOL.backpack()] = Decimal("-0.003")

        # Active positions
        positions = {
            symbols.BTC.hyperliquid(): Decimal("1.0"),  # Long 1 BTC on HL
            symbols.BTC.backpack(): Decimal("-1.0"),  # Short 1 BTC on BP
            symbols.ETH.hyperliquid(): Decimal("10.0"),  # Long 10 ETH on HL
            symbols.ETH.backpack(): Decimal("-10.0"),  # Short 10 ETH on BP
        }

        # Current prices
        prices = {
            symbols.BTC.hyperliquid(): Decimal(50000),
            symbols.BTC.backpack(): Decimal(50010),
            symbols.ETH.hyperliquid(): Decimal(3000),
            symbols.ETH.backpack(): Decimal(3002),
        }

        if include_sol:
            prices[symbols.SOL.hyperliquid()] = Decimal(100)
            prices[symbols.SOL.backpack()] = Decimal("100.5")

        return FundingArbScenario(
            symbols=scenario_symbols,
            funding_rates=funding_rates,
            positions=positions,
            prices=prices,
        )

    @staticmethod
    def create_market_making_scenario(
        symbol: Symbol,
        mid_price: Decimal = Decimal(50000),
        spread_bps: int = 10,
    ) -> MarketMakingScenario:
        """Create market making test scenario.

        Args:
            symbol: Symbol to make market on
            mid_price: Mid price
            spread_bps: Spread in basis points

        Returns:
            MarketMakingScenario: Complete scenario
        """
        spread = mid_price * Decimal(spread_bps) / Decimal(10000)
        bid_price = mid_price - spread / 2
        ask_price = mid_price + spread / 2

        # Determine exchange from symbol
        exchange = (
            ExchangeName.HYPERLIQUID if symbol.exchange == "hyperliquid" else ExchangeName.BACKPACK
        )

        # Create orders
        orders = [
            Order(
                exchange_order_id="buy_1",
                exchange=exchange,
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                price=bid_price,
                quantity_requested=Decimal("0.1"),
                status=OrderStatus.OPEN,
                time_in_force=TimeInForce.GTC,
                created_at=datetime.now(UTC),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            ),
            Order(
                exchange_order_id="sell_1",
                exchange=exchange,
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                price=ask_price,
                quantity_requested=Decimal("0.1"),
                status=OrderStatus.OPEN,
                time_in_force=TimeInForce.GTC,
                created_at=datetime.now(UTC),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            ),
        ]

        risk_limits = {
            "max_position": Decimal("5.0"),
            "max_order_size": Decimal("1.0"),
            "min_spread_bps": spread_bps,
            "inventory_target": Decimal(0),
        }

        return MarketMakingScenario(
            symbol=symbol,
            mid_price=mid_price,
            spread=spread,
            inventory=Decimal("0.5"),
            orders=orders,
            risk_limits=risk_limits,
        )

    @staticmethod
    def create_position_management_scenario() -> PositionManagementScenario:
        """Create position management test scenario.

        Returns:
            PositionManagementScenario: Complete scenario
        """
        # Active positions
        positions = {
            symbols.BTC.hyperliquid(): DerivativePosition(
                exchange=ExchangeName.HYPERLIQUID,
                symbol=symbols.BTC.hyperliquid(),
                side=OrderSide.BUY,
                size=Decimal("2.5"),
                entry_price=Decimal(48000),
                mark_price=Decimal(50000),
                liquidation_price=Decimal(40000),
                unrealized_pnl=Decimal(5000),  # 2.5 * (50000 - 48000)
                realized_pnl=Decimal(1000),
                timestamp=datetime.now(UTC),
            ),
            symbols.ETH.backpack(): DerivativePosition(
                exchange=ExchangeName.BACKPACK,
                symbol=symbols.ETH.backpack(),
                side=OrderSide.SELL,
                size=Decimal("-15.0"),  # Short
                entry_price=Decimal(3100),
                mark_price=Decimal(3000),
                liquidation_price=Decimal(3500),
                unrealized_pnl=Decimal(1500),  # -15 * (3000 - 3100)
                realized_pnl=Decimal(500),
                timestamp=datetime.now(UTC),
            ),
        }

        # Balances
        balances = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset=exchanges.hyperliquid("USDC"),
                total_quantity=Decimal(55000),
                available_quantity=Decimal(50000),
                timestamp=datetime.now(UTC),
            ),
            "BTC": SpotBalance(
                exchange="hyperliquid",
                asset=exchanges.hyperliquid("BTC"),
                total_quantity=Decimal("0.1"),
                available_quantity=Decimal("0.1"),
                timestamp=datetime.now(UTC),
            ),
        }

        # Risk metrics
        risk_metrics = {
            "total_exposure": Decimal(170000),  # 125000 + 45000
            "net_exposure": Decimal(80000),  # 125000 - 45000
            "margin_usage": Decimal("0.08"),  # 4000 / 50000
            "leverage": Decimal("3.4"),  # 170000 / 50000
        }

        # Pending orders
        orders = {
            symbols.BTC.hyperliquid(): [
                Order(
                    exchange_order_id="btc_stop_loss",
                    exchange=ExchangeName.HYPERLIQUID,
                    symbol=symbols.BTC.hyperliquid(),
                    side=OrderSide.SELL,
                    order_type=OrderType.STOP_MARKET,
                    stop_price=Decimal(48000),
                    quantity_requested=Decimal("2.5"),
                    status=OrderStatus.OPEN,
                    time_in_force=TimeInForce.GTC,
                    created_at=datetime.now(UTC),
                    updated_at=None,
                    triggered_at=None,
                    strategy_name=None,
                    signal_id=None,
                ),
            ],
            symbols.ETH.backpack(): [
                Order(
                    exchange_order_id="eth_take_profit",
                    exchange=ExchangeName.BACKPACK,
                    symbol=symbols.ETH.backpack(),
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    price=Decimal(2900),
                    quantity_requested=Decimal("15.0"),
                    status=OrderStatus.OPEN,
                    time_in_force=TimeInForce.GTC,
                    created_at=datetime.now(UTC),
                    updated_at=None,
                    triggered_at=None,
                    strategy_name=None,
                    signal_id=None,
                ),
            ],
        }

        return PositionManagementScenario(
            positions=positions,
            balances=balances,
            risk_metrics=risk_metrics,
            orders=orders,
        )

    @staticmethod
    def create_multi_exchange_scenario() -> dict[str, Any]:
        """Create multi-exchange trading scenario.

        Returns:
            dict: Complete multi-exchange scenario
        """
        return {
            "symbols": {
                "hyperliquid": [
                    symbols.BTC.hyperliquid(),
                    symbols.ETH.hyperliquid(),
                    symbols.SOL.hyperliquid(),
                ],
                "backpack": [
                    symbols.BTC.backpack(),
                    symbols.ETH.backpack(),
                    symbols.SOL.backpack(),
                ],
            },
            "tickers": {
                symbols.BTC.hyperliquid(): Ticker(
                    symbol=symbols.BTC.hyperliquid(),
                    exchange=ExchangeName.HYPERLIQUID,
                    bid=Decimal(49990),
                    ask=Decimal(50010),
                    price=Decimal(50000),
                    volume=Decimal(1000),
                    timestamp=datetime.now(UTC),
                ),
                symbols.BTC.backpack(): Ticker(
                    symbol=symbols.BTC.backpack(),
                    exchange=ExchangeName.BACKPACK,
                    bid=Decimal(50000),
                    ask=Decimal(50020),
                    price=Decimal(50010),
                    volume=Decimal(800),
                    timestamp=datetime.now(UTC),
                ),
            },
            "funding_rates": {
                symbols.BTC.hyperliquid(): FundingRate(
                    symbol=symbols.BTC.hyperliquid(),
                    funding_rate=Decimal("0.0001"),
                    next_funding_time=datetime.now(UTC) + timedelta(hours=8),
                    timestamp=datetime.now(UTC),
                ),
                symbols.BTC.backpack(): FundingRate(
                    symbol=symbols.BTC.backpack(),
                    funding_rate=Decimal("-0.0002"),
                    next_funding_time=datetime.now(UTC) + timedelta(hours=8),
                    timestamp=datetime.now(UTC),
                ),
            },
            "opportunities": [
                {
                    "type": "price_arbitrage",
                    "symbols": (symbols.BTC.hyperliquid(), symbols.BTC.backpack()),
                    "spread": Decimal(10),  # $10 spread
                    "spread_pct": Decimal("0.02"),  # 0.02%
                },
                {
                    "type": "funding_arbitrage",
                    "symbols": (symbols.BTC.hyperliquid(), symbols.BTC.backpack()),
                    "rate_diff": Decimal("0.0003"),  # 0.03% rate difference
                    "annualized": Decimal("0.2628"),  # 26.28% annualized
                },
            ],
        }
