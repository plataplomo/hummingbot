"""Example: Integrating Market Orders with Trading Strategies.

This shows how to use market orders within CyberDeltaEngine's strategy framework
in an exchange-agnostic manner.
"""

from decimal import Decimal
from typing import Any

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.core.execution.orders import (
    InsufficientLiquidityError,
    MarketOrder,
    MarketOrderConfig,
    MarketOrderService,
)
from cyberdelta.core.models import Order, OrderSide, OrderType, SignalType, TimeInForce, TradeSignal
from cyberdelta.core.strategy import Strategy


class MarketOrderStrategy(Strategy):
    """Example strategy that uses market orders for immediate execution."""

    def __init__(self, name: str = "MarketOrderStrategy", symbol: str = "BTC") -> None:
        """Initialize the strategy."""
        super().__init__(name, symbol)
        self.market_orders: dict[str, MarketOrder] = {}
        self._initialize_market_orders()

    def _initialize_market_orders(self) -> None:
        """Initialize market order executors for each exchange."""
        # This would be called after exchanges are set up

    def add_exchange_market_order(self, exchange_name: str, exchange_api: ExchangeAPI) -> None:
        """Add a market order executor for an exchange.

        This method demonstrates the exchange-agnostic pattern - any exchange
        that implements ExchangeAPI can be added.
        """
        config = MarketOrderConfig(
            default_slippage_pct=Decimal("0.002"),  # 0.2%
            max_slippage_pct=Decimal("0.05"),  # 5% max
        )

        service = MarketOrderService(exchange_api=exchange_api, config=config)

        self.market_orders[exchange_name] = MarketOrder(
            exchange_api=exchange_api,
            market_order_service=service,
            config=config,
        )

    async def generate_signals(self) -> list[TradeSignal]:
        """Generate trading signals.

        This is where your strategy logic would determine when to trade.
        For this example, we'll create a simple signal.
        """
        signals: list[TradeSignal] = []

        # Example: Generate a buy signal when conditions are met
        if self._should_buy():
            signal = TradeSignal(
                symbol="BTC",
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal(50000),  # Required field - would get from market data
                quantity=Decimal("0.1"),
                exchange="hyperliquid",
                confidence=0.8,
                metadata={
                    "strategy": self.name,
                    "reason": "momentum_breakout",
                    "use_market_order": True,  # Flag for execution
                },
            )
            signals.append(signal)

        return signals

    def _should_buy(self) -> bool:
        """Example condition check."""
        # Your strategy logic here
        return True

    async def execute_signal_with_market_order(
        self,
        signal: TradeSignal,
        exchange_api: ExchangeAPI,
    ) -> Order | None:
        """Execute a signal using market orders.

        This method shows how to handle signal execution with market orders
        in an exchange-agnostic way.
        """
        # Handle single exchange or list of exchanges
        exchange_name = signal.exchange if isinstance(signal.exchange, str) else signal.exchange[0]

        # Check if we should use market order
        if signal.metadata and signal.metadata.get("use_market_order", False):
            if exchange_name not in self.market_orders:
                # Initialize market order for this exchange if not already done
                self.add_exchange_market_order(exchange_name, exchange_api)

            market_order = self.market_orders[exchange_name]

            try:
                # Execute market order
                return await market_order.execute_market_order(
                    symbol=signal.symbol,
                    side=signal.side,
                    quantity=signal.quantity or Decimal("0.1"),  # Default if None
                    max_slippage=(
                        signal.metadata.get("max_slippage", Decimal("0.01"))
                        if signal.metadata
                        else Decimal("0.01")
                    ),
                )

                # Market order executed successfully

            except InsufficientLiquidityError:
                # Insufficient liquidity for order
                # Could fall back to limit order here
                return None

        # If not using market order, return None (would use regular order placement)
        return None


class ArbitrageStrategyWithMarketOrders(Strategy):
    """Arbitrage strategy that uses market orders for speed."""

    def __init__(self, name: str = "MarketArbitrage", symbol: str = "BTC") -> None:
        """Initialize the arbitrage strategy."""
        super().__init__(name, symbol)
        self.market_orders: dict[str, MarketOrder] = {}

    def setup_market_orders(self, exchanges: dict[str, ExchangeAPI]) -> None:
        """Set up market order executors for all exchanges.

        This demonstrates how the same market order interface works
        across different exchanges.
        """
        for exchange_name, exchange_api in exchanges.items():
            # Custom config per exchange based on characteristics
            if exchange_name == "hyperliquid":
                config = MarketOrderConfig(
                    default_slippage_pct=Decimal("0.001"),  # Tighter for liquid exchange
                    use_all_mids_for_reference=True,
                )
            else:
                config = MarketOrderConfig(
                    default_slippage_pct=Decimal("0.002"),
                )

            service = MarketOrderService(exchange_api, config=config)
            self.market_orders[exchange_name] = MarketOrder(exchange_api, service, config)

    async def execute_arbitrage(
        self,
        opportunity: dict[str, Any],
        buy_exchange: str,
        sell_exchange: str,
    ) -> tuple[Order, Order]:
        """Execute arbitrage using market orders on both exchanges.

        The exchange-agnostic design allows us to execute the same
        market order logic on any exchange.
        """
        symbol: str = opportunity["symbol"]
        quantity: Decimal = opportunity["quantity"]

        # Execute simultaneously for speed
        import asyncio

        buy_task = self.market_orders[buy_exchange].execute_market_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
        )

        sell_task = self.market_orders[sell_exchange].execute_market_order(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
        )

        # Wait for both to complete
        buy_order, sell_order = await asyncio.gather(buy_task, sell_task)

        # Calculate profit
        if buy_order.average_fill_price and sell_order.average_fill_price:
            # Calculate profit
            _ = (sell_order.average_fill_price - buy_order.average_fill_price) * quantity

        return buy_order, sell_order


# Integration with Engine
class MarketOrderEngine:
    """Example of integrating market orders with the main engine."""

    def __init__(self) -> None:
        """Initialize the engine with market order support."""
        self.exchanges: dict[str, ExchangeAPI] = {}
        self.market_orders: dict[str, MarketOrder] = {}

    def add_exchange(self, name: str, exchange_api: ExchangeAPI) -> None:
        """Add an exchange with automatic market order support.

        This shows how any exchange implementing ExchangeAPI automatically
        gets market order functionality.
        """
        self.exchanges[name] = exchange_api

        # Automatically create market order executor
        config = MarketOrderConfig()
        service = MarketOrderService(exchange_api, config=config)
        self.market_orders[name] = MarketOrder(exchange_api, service, config)

    async def route_order(
        self,
        exchange: str,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        order_type: OrderType,
        price: Decimal | None = None,
    ) -> Order:
        """Route orders to appropriate execution method.

        This demonstrates how to integrate market orders into existing
        order routing logic.
        """
        exchange_api = self.exchanges.get(exchange)
        if not exchange_api:
            raise ValueError(f"Unknown exchange: {exchange}")

        if order_type == OrderType.MARKET:
            # Use market order executor
            return await self.market_orders[exchange].execute_market_order(
                symbol=symbol,
                side=side,
                quantity=quantity,
            )
        # Use regular order placement
        from cyberdelta.apis.models.service_args_models import PlaceOrderArgs

        args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force=TimeInForce.IOC,
        )
        return await exchange_api.place_order(args)
