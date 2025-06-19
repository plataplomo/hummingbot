"""Example: Integrating Market Orders with Trading Strategies.

This shows how to use market orders within CyberDeltaEngine's strategy framework
in an exchange-agnostic manner.
"""

from decimal import Decimal

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.core.execution.orders import (
    InsufficientLiquidityError,
    MarketOrder,
    MarketOrderConfig,
    MarketOrderService,
)
from cyberdelta.core.models import OrderSide, OrderType, TradeSignal
from cyberdelta.core.strategy import Strategy


class MarketOrderStrategy(Strategy):
    """Example strategy that uses market orders for immediate execution."""

    def __init__(self, name: str = "MarketOrderStrategy"):
        """Initialize the strategy."""
        super().__init__(name)
        self.market_orders: dict[str, MarketOrder] = {}
        self._initialize_market_orders()

    def _initialize_market_orders(self):
        """Initialize market order executors for each exchange."""
        # This would be called after exchanges are set up
        pass

    def add_exchange_market_order(self, exchange_name: str, exchange_api: ExchangeAPI):
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
            exchange_api=exchange_api, market_order_service=service, config=config
        )

    async def generate_signals(self) -> list[TradeSignal]:
        """Generate trading signals.

        This is where your strategy logic would determine when to trade.
        For this example, we'll create a simple signal.
        """
        signals = []

        # Example: Generate a buy signal when conditions are met
        if self._should_buy():
            signal = TradeSignal(
                symbol="BTC",
                side=OrderSide.BUY,
                quantity=Decimal("0.1"),
                order_type=OrderType.MARKET,  # Specify market order
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
        exchange_name = signal.exchange

        # Check if we should use market order
        if signal.metadata.get("use_market_order", False):
            if exchange_name not in self.market_orders:
                # Initialize market order for this exchange if not already done
                self.add_exchange_market_order(exchange_name, exchange_api)

            market_order = self.market_orders[exchange_name]

            try:
                # Execute market order
                order = await market_order.execute_market_order(
                    symbol=signal.symbol,
                    side=signal.side,
                    quantity=signal.quantity,
                    max_slippage=signal.metadata.get("max_slippage", Decimal("0.01")),
                )

                print(
                    f"Market order executed: {order.quantity_filled} @ {order.average_fill_price}"
                )
                return order

            except InsufficientLiquidityError as e:
                print(f"Insufficient liquidity for {signal.symbol}: {e}")
                # Could fall back to limit order here
                return None

        # If not using market order, return None (would use regular order placement)
        return None


class ArbitrageStrategyWithMarketOrders(Strategy):
    """Arbitrage strategy that uses market orders for speed."""

    def __init__(self, name: str = "MarketArbitrage"):
        """Initialize the arbitrage strategy."""
        super().__init__(name)
        self.market_orders: dict[str, MarketOrder] = {}

    def setup_market_orders(self, exchanges: dict[str, ExchangeAPI]):
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
        opportunity: dict,
        buy_exchange: str,
        sell_exchange: str,
    ) -> tuple[Order, Order]:
        """Execute arbitrage using market orders on both exchanges.

        The exchange-agnostic design allows us to execute the same
        market order logic on any exchange.
        """
        symbol = opportunity["symbol"]
        quantity = opportunity["quantity"]

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
            profit = (sell_order.average_fill_price - buy_order.average_fill_price) * quantity
            print(f"Arbitrage profit: ${profit}")

        return buy_order, sell_order


# Integration with Engine
class MarketOrderEngine:
    """Example of integrating market orders with the main engine."""

    def __init__(self):
        """Initialize the engine with market order support."""
        self.exchanges: dict[str, ExchangeAPI] = {}
        self.market_orders: dict[str, MarketOrder] = {}

    def add_exchange(self, name: str, exchange_api: ExchangeAPI):
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
        else:
            # Use regular order placement
            from cyberdelta.apis.models.service_args_models import PlaceOrderArgs

            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
            )
            return await exchange_api.place_order(args)
