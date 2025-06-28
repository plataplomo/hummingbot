"""Example: Exchange-Agnostic Market Order Integration.

This example demonstrates how to use the MarketOrder module in an exchange-agnostic way,
integrating it with CyberDeltaEngine's trading system.
"""

import asyncio
import time
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.hyperliquid import HyperliquidAPI
from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.execution.orders import (
    InsufficientLiquidityError,
    MarketOrder,
    MarketOrderConfig,
    MarketOrderService,
    PriceDeviationError,
)
from cyberdelta.core.execution.orders.market_order_metrics import MarketOrderMetrics
from cyberdelta.core.models import Order, OrderSide


logger = get_logger(__name__)


class ExchangeAgnosticMarketOrderExecutor:
    """Demonstrates exchange-agnostic market order execution."""

    def __init__(self) -> None:
        """Initialize the executor with exchange instances."""
        self.exchanges: dict[str, ExchangeAPI] = {}
        self.market_orders: dict[str, MarketOrder] = {}
        self.metrics = MarketOrderMetrics()

        # Load configuration
        config_manager = ConfigManager()
        secrets_manager = SecretsManager()

        # Initialize exchanges (both implement ExchangeAPI)
        self._initialize_exchanges(config_manager, secrets_manager)

    def _initialize_exchanges(
        self,
        config_manager: ConfigManager,
        secrets_manager: SecretsManager,
    ) -> None:
        """Initialize exchange instances."""
        # Get configs
        # Get configs from settings
        hl_config = (
            config_manager.settings.exchanges.get("hyperliquid")
            if config_manager.settings
            else None
        )
        bp_config = (
            config_manager.settings.exchanges.get("backpack") if config_manager.settings else None
        )

        # Get secrets
        # Get secrets from secrets_data exchanges dict
        hl_secrets = (
            secrets_manager.secrets_data.exchanges.get("hyperliquid")
            if secrets_manager.secrets_data
            else None
        )
        bp_secrets = (
            secrets_manager.secrets_data.exchanges.get("backpack")
            if secrets_manager.secrets_data
            else None
        )

        # Create exchange instances
        if hl_config and hl_secrets:
            self.exchanges["hyperliquid"] = HyperliquidAPI(hl_config, hl_secrets)

        if bp_config and bp_secrets:
            self.exchanges["backpack"] = BackpackAPI(bp_config, bp_secrets)

        # Create market order executors for each exchange
        for exchange_name, exchange_api in self.exchanges.items():
            self._create_market_order_executor(exchange_name, exchange_api)

    def _create_market_order_executor(self, exchange_name: str, exchange_api: ExchangeAPI) -> None:
        """Create a market order executor for a specific exchange."""
        # Exchange-specific configuration
        config = self._get_exchange_specific_config(exchange_name)

        # Create service (optionally with signal generator)
        service = MarketOrderService(
            exchange_api=exchange_api,
            signal_generator=None,  # Could add signal generator here
            config=config,
        )

        # Create executor
        self.market_orders[exchange_name] = MarketOrder(
            exchange_api=exchange_api,
            market_order_service=service,
            config=config,
        )

    def _get_exchange_specific_config(self, exchange_name: str) -> MarketOrderConfig:
        """Get exchange-specific market order configuration."""
        # You can customize settings per exchange
        if exchange_name == "hyperliquid":
            return MarketOrderConfig(
                default_slippage_pct=Decimal("0.001"),  # 0.1% for Hyperliquid
                max_slippage_pct=Decimal("0.05"),
                slippage_by_symbol={
                    "BTC": Decimal("0.0005"),  # Even tighter for BTC
                    "ETH": Decimal("0.0005"),
                    "SOL": Decimal("0.001"),
                    "default": Decimal("0.002"),
                },
            )
        if exchange_name == "backpack":
            return MarketOrderConfig(
                default_slippage_pct=Decimal("0.002"),  # 0.2% for Backpack
                max_slippage_pct=Decimal("0.05"),
            )
        return MarketOrderConfig()  # Default config

    async def execute_market_order(
        self,
        exchange_name: str,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        max_slippage: Decimal | None = None,
    ) -> Order:
        """Execute a market order on the specified exchange.

        This method is completely exchange-agnostic - it works with any exchange
        that implements the ExchangeAPI interface.

        Args:
            exchange_name: Name of the exchange to execute on
            symbol: Trading symbol
            side: Buy or sell
            quantity: Order quantity
            max_slippage: Optional maximum slippage override

        Returns:
            Order: Executed order with fill information

        Raises:
            ValueError: If exchange not found
            MarketOrderError: If execution fails
        """
        if exchange_name not in self.market_orders:
            raise ValueError(f"Exchange {exchange_name} not initialized")

        market_order = self.market_orders[exchange_name]

        # Execute the order

        start_time = time.time()

        try:
            order = await market_order.execute_market_order(
                symbol=symbol,
                side=side,
                quantity=quantity,
                max_slippage=max_slippage,
            )

            execution_time_ms = (time.time() - start_time) * 1000

            # Record metrics
            self.metrics.record_execution(
                symbol=symbol,
                side=side,
                requested_qty=quantity,
                filled_qty=order.quantity_filled or Decimal(0),
                expected_price=order.price or Decimal(0),  # In real use, calculate expected
                actual_price=order.average_fill_price or order.price or Decimal(0),
                expected_slippage=Decimal("0.001"),  # Would calculate from config
                status=order.status,
                execution_time_ms=execution_time_ms,
            )

            return order

        except (InsufficientLiquidityError, PriceDeviationError):
            # Market order failed, re-raise the exception
            raise

    async def execute_cross_exchange_arbitrage(
        self,
        symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        quantity: Decimal,
    ) -> tuple[Order, Order]:
        """Execute simultaneous market orders on two exchanges for arbitrage.

        This demonstrates how the exchange-agnostic design allows easy
        cross-exchange operations.

        Args:
            symbol: Trading symbol
            buy_exchange: Exchange to buy on
            sell_exchange: Exchange to sell on
            quantity: Order quantity

        Returns:
            Tuple of (buy_order, sell_order)
        """
        # Execute both orders simultaneously
        tasks = [
            self.execute_market_order(buy_exchange, symbol, OrderSide.BUY, quantity),
            self.execute_market_order(sell_exchange, symbol, OrderSide.SELL, quantity),
        ]

        buy_order, sell_order = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle results
        if isinstance(buy_order, Exception) or isinstance(sell_order, Exception):
            # Arbitrage failed - one or both orders failed
            if isinstance(buy_order, Exception):
                raise buy_order
            if isinstance(sell_order, Exception):
                raise sell_order

        return buy_order, sell_order  # type: ignore[return-value]

    def get_exchange_stats(self, exchange_name: str) -> dict[str, Any]:
        """Get market order statistics for a specific exchange."""
        stats: dict[str, Any] = {"overall": self.metrics.get_overall_stats(), "symbols": {}}

        # Get per-symbol stats
        # Get unique symbols from metrics
        symbols: set[str] = set()
        # Get symbols from metrics (accessing protected member for demo)
        metrics_list = getattr(self.metrics, "_metrics", [])
        for metric in metrics_list:
            if hasattr(metric, "symbol"):
                symbols.add(metric.symbol)

        # Get stats for each symbol
        for symbol in symbols:
            stats["symbols"][symbol] = self.metrics.get_symbol_stats(symbol)

        return stats


async def main() -> None:
    """Example usage of exchange-agnostic market orders."""
    # Initialize executor
    executor = ExchangeAgnosticMarketOrderExecutor()

    # Example 1: Execute a market order on Hyperliquid
    try:
        await executor.execute_market_order(
            exchange_name="hyperliquid",
            symbol="BTC",
            side=OrderSide.BUY,
            quantity=Decimal("0.01"),
        )
        # Hyperliquid order executed successfully
    except Exception as e:
        # Hyperliquid order failed
        logger.warning(
            "hyperliquid_order_failed",
            message="Hyperliquid order failed: %s",
            message_args=(e,),
        )

    # Example 2: Execute a market order on Backpack
    try:
        await executor.execute_market_order(
            exchange_name="backpack",
            symbol="BTC-USDC",
            side=OrderSide.SELL,
            quantity=Decimal("0.01"),
        )
        # Backpack order executed successfully
    except Exception as e:
        # Backpack order failed
        logger.warning(
            "backpack_order_failed",
            message="Backpack order failed: %s",
            message_args=(e,),
        )

    # Example 3: Cross-exchange arbitrage
    try:
        await executor.execute_cross_exchange_arbitrage(
            symbol="ETH",
            buy_exchange="hyperliquid",
            sell_exchange="backpack",
            quantity=Decimal("0.1"),
        )
        # Arbitrage executed successfully
    except Exception as e:
        # Arbitrage failed
        logger.warning("arbitrage_failed", message="Arbitrage failed: %s", message_args=(e,))

    # Show statistics
    for exchange in executor.exchanges:
        _ = executor.get_exchange_stats(exchange)
        # Statistics for exchange processed


if __name__ == "__main__":
    asyncio.run(main())
