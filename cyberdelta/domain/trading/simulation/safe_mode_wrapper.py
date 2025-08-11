"""Safe mode wrapper for paper trading without real exchange interaction.

This module provides a proper wrapper/decorator that intercepts ExchangeAPI calls
when safe mode is enabled, simulating operations without real execution.

IMPORTANT: Following clean_new_arch.md and CODING_STANDARDS.md:
- Wraps the REAL ExchangeAPI using composition pattern
- Uses exact same interface as ExchangeAPI (implements same methods)
- Configuration-driven simulation parameters from AppSettings
- Delegates to real API when safe mode disabled
- NO hardcoded simulation parameters
"""

from __future__ import annotations

import asyncio
import secrets
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args.account import (
    TransferArgs,
    UpdateAccountSettingsArgs,
    WithdrawArgs,
)
from cyberdelta.apis.models.service_args.market_data import (
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketArgs,
    GetMarketDataArgs,
    GetMarketsArgs,
)

# Import the actual service args models
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import CancelOrderResultStatus, OrderStatus
from cyberdelta.enums import ExchangeName, OrderSide, OrderType
from cyberdelta.models.account_settings import AccountSettings
from cyberdelta.models.derivative_position import DerivativePosition
from cyberdelta.models.margin_account import MarginAccountSummary
from cyberdelta.models.market.candle import Candle
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.market.funding_rate import FundingRate
from cyberdelta.models.market.market import Market
from cyberdelta.models.market.order import CancelOrderResult, Order
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker
from cyberdelta.models.operations import Transfer, Withdrawal
from cyberdelta.models.spot_balance import SpotBalance
from cyberdelta.symbols import bp_symbol, hl_symbol
from cyberdelta.symbols.models import Symbol


logger = get_logger(__name__)


# SimulatedFill removed - using proper Fill model from cyberdelta.models.market.fill


class SafeModeWrapper:
    """Proper wrapper for ExchangeAPI that simulates operations in safe mode.

    This follows composition pattern - it wraps an ExchangeAPI without inheriting.
    When safe mode is enabled, it intercepts calls and simulates them.
    When safe mode is disabled, it delegates to the real API.

    Configuration Usage:
    - Uses config.general.safe_mode to determine if active
    - Uses config.simulation.* for simulation parameters
    - Uses config.exchanges[exchange].* for exchange-specific config

    IMPORTANT: Following CODING_STANDARDS.md and clean_new_arch.md:
    - Proper composition pattern wrapping real ExchangeAPI
    - ALL simulation parameters from configuration
    - NO hardcoded trading behavior
    - Uses real ExchangeAPI interface
    - Delegates to real API when safe mode disabled
    """

    def __init__(
        self,
        config: AppSettings,
        real_api: ExchangeAPI,
        exchange_name: ExchangeName,
    ) -> None:
        """Initialize safe mode wrapper with real ExchangeAPI.

        Args:
            config: Application settings
            real_api: The real exchange API to wrap
            exchange_name: Name of the exchange being wrapped

        Raises:
            ValueError: If paper trading configuration is missing
        """
        self.app_config = config
        self._real_api = real_api
        self._exchange_name = exchange_name
        self._safe_mode = config.general.safe_mode

        # Simulation configuration from AppSettings
        self._simulation_config = config.simulation
        self._initial_balance = Decimal(str(self._simulation_config.initial_balance_usd))
        self._fill_probability = self._simulation_config.fill_probability
        # Use max slippage from config
        max_slippage = self._simulation_config.max_slippage_pct
        self._slippage_range = (0.0, max_slippage)
        self._limit_fill_delay = Decimal(str(self._simulation_config.limit_fill_delay_seconds))

        # Exchange-specific configuration
        exchange_config = config.exchanges.get(exchange_name.value)
        if not exchange_config:
            msg = f"Exchange configuration missing for {exchange_name.value}"
            raise ValueError(msg)

        # Use simulation fee rates
        self._maker_fee_rate = Decimal(str(self._simulation_config.maker_fee_rate))
        self._taker_fee_rate = Decimal(str(self._simulation_config.taker_fee_rate))

        # Simulated state
        self._simulated_orders: dict[str, Order] = {}
        self._simulated_balances: dict[str, SpotBalance] = {}
        self._simulated_positions: dict[str, DerivativePosition] = {}
        self._simulated_fills: list[Fill] = []

        # Initialize simulated balances with configured amounts
        self._initialize_balances()

        # Store task references for cleanup
        self._limit_fill_tasks: list[asyncio.Task[None]] = []

        if self._safe_mode:
            logger.warning(
                "safe_mode_wrapper_active",
                exchange=exchange_name.value,
                initial_balance=self._initial_balance,
                fill_probability=self._fill_probability,
                slippage_range=self._slippage_range,
                message="ALL TRADES WILL BE SIMULATED - NO REAL ORDERS",
            )

    # ===========================================
    # ExchangeAPI Methods Implementation
    # ===========================================

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place order - simulated in safe mode, real in normal mode.

        Returns:
            Order: The placed order (simulated or real)
        """
        if not self._safe_mode:
            return await self._real_api.place_order(args)

        # Simulate order placement with proper Order model
        order_id = f"SIM_{uuid.uuid4().hex[:8]}"
        client_order_id = args.client_order_id or f"CLIENT_{uuid.uuid4().hex[:8]}"

        order = Order(
            exchange_order_id=order_id,
            client_order_id=client_order_id,
            exchange=self._exchange_name,
            symbol=args.symbol,
            side=args.side,
            order_type=args.order_type,
            price=args.price,
            quantity_requested=args.quantity,
            time_in_force=args.time_in_force,
            status=OrderStatus.OPEN,
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            quantity_filled=Decimal(0),
        )

        self._simulated_orders[order_id] = order

        logger.info(
            "safe_mode_order_placed",
            order_id=order_id,
            symbol=args.symbol.value,
            side=args.side.value,
            order_type=args.order_type.value,
            price=args.price or None,
            quantity=args.quantity,
            safe_mode=True,
            message="SIMULATED ORDER - NOT SENT TO EXCHANGE",
        )

        # Simulate fills based on order type and configuration
        if args.order_type == OrderType.MARKET:
            await self._simulate_fill(order, immediate=True)
        else:
            # Schedule potential fill for limit orders based on config delay
            task = asyncio.create_task(self._simulate_limit_fill(order))
            self._limit_fill_tasks.append(task)

        return order

    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Cancel order - simulated in safe mode, real in normal mode.

        Returns:
            CancelOrderResult: Result of the cancellation attempt
        """
        if not self._safe_mode:
            return await self._real_api.cancel_order(args)

        order_id = args.order_id
        if order_id in self._simulated_orders:
            order = self._simulated_orders[order_id]
            if order.status == OrderStatus.OPEN:
                order.status = OrderStatus.CANCELED

                logger.info("safe_mode_order_cancelled", order_id=order_id, safe_mode=True)

                return CancelOrderResult(
                    order_id=order_id,
                    success=True,
                    message="Order cancelled in safe mode",
                    status=CancelOrderResultStatus.SUCCESS,
                )

        return CancelOrderResult(
            order_id=order_id,
            success=False,
            message="Order not found in safe mode",
            status=CancelOrderResultStatus.NOT_FOUND,
        )

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get balances - simulated in safe mode, real in normal mode.

        Returns:
            dict[str, SpotBalance]: Balance dictionary keyed by asset
        """
        if not self._safe_mode:
            return await self._real_api.get_balances()

        return self._simulated_balances.copy()

    async def get_positions(self, symbol: Symbol | None = None) -> list[DerivativePosition]:
        """Get positions - simulated in safe mode, real in normal mode.

        Returns:
            list[DerivativePosition]: List of open positions
        """
        if not self._safe_mode:
            return await self._real_api.get_positions(symbol)

        positions = list(self._simulated_positions.values())
        if symbol:
            positions = [p for p in positions if p.symbol == symbol]
        return positions

    async def get_account_summary(self) -> MarginAccountSummary:
        """Get account summary - simulated in safe mode, real in normal mode.

        Returns:
            MarginAccountSummary: Account summary information
        """
        if not self._safe_mode:
            return await self._real_api.get_account_summary()

        # Calculate simulated account summary from balances and positions
        total_equity = sum(
            (balance.total_quantity for balance in self._simulated_balances.values()), Decimal(0)
        )
        available_equity = sum(
            (balance.available_quantity for balance in self._simulated_balances.values()),
            Decimal(0),
        )

        # Create simulated account summary
        return MarginAccountSummary(
            exchange=self._exchange_name,
            timestamp=datetime.now(UTC),
            total_equity=total_equity,
            available_equity=available_equity,
        )

    async def get_open_orders(self, symbol: Symbol | None = None) -> list[Order]:
        """Get open orders - simulated in safe mode, real in normal mode.

        Returns:
            list[Order]: List of open orders
        """
        if not self._safe_mode:
            return await self._real_api.get_open_orders(symbol)

        open_orders = [
            order for order in self._simulated_orders.values() if order.status == OrderStatus.OPEN
        ]

        if symbol:
            open_orders = [order for order in open_orders if order.symbol == symbol]

        return open_orders

    async def cancel_all_orders(self, symbol: Symbol | None = None) -> list[CancelOrderResult]:
        """Cancel all orders - simulated in safe mode, real in normal mode.

        Returns:
            list[CancelOrderResult]: List of cancellation results
        """
        if not self._safe_mode:
            return await self._real_api.cancel_all_orders(symbol)

        results: list[CancelOrderResult] = []
        open_orders = await self.get_open_orders(symbol)

        for order in open_orders:
            if order.exchange_order_id:
                cancel_args = CancelOrderArgs(order_id=order.exchange_order_id)
            else:
                continue
            result = await self.cancel_order(cancel_args)
            results.append(result)

        return results

    # Market data methods - delegate to real API (safe to use real market data)
    async def get_ticker(self, symbol: Symbol) -> Ticker | None:
        """Get ticker - always use real market data.

        Returns:
            Ticker | None: Current ticker or None if not available
        """
        return await self._real_api.get_ticker(symbol)

    async def get_order_book(self, symbol: Symbol, depth: int = 20) -> OrderBook | None:
        """Get order book - always use real market data.

        Returns:
            OrderBook | None: Current order book or None if not available
        """
        return await self._real_api.get_order_book(symbol, depth)

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Get funding rates - always use real market data.

        Returns:
            List of funding rates from real API.
        """
        return await self._real_api.get_funding_rates(args)

    async def get_historical_funding_rates(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> list[FundingRate]:
        """Get historical funding rates - always use real market data.

        Returns:
            List of historical funding rates from real API.
        """
        return await self._real_api.get_historical_funding_rates(args)

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Get market data - always use real market data.

        Returns:
            List of candles from real API.
        """
        return await self._real_api.get_market_data(args)

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Get market - always use real market data.

        Returns:
            Market information from real API.
        """
        return await self._real_api.get_market(args)

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Get markets - always use real market data.

        Returns:
            List of markets from real API.
        """
        return await self._real_api.get_markets(args)

    # Order and trade history methods
    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Get order history - simulated in safe mode, real in normal mode.

        Returns:
            list[Order]: List of historical orders
        """
        if not self._safe_mode:
            return await self._real_api.get_order_history(args)

        # Return simulated order history
        orders = list(self._simulated_orders.values())

        # Apply symbol filter if specified
        if args.symbol:
            orders = [order for order in orders if order.symbol == args.symbol]

        # Sort by timestamp descending
        orders.sort(key=lambda x: x.created_at, reverse=True)

        return orders

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Fill]:
        """Get trade history - simulated in safe mode, real in normal mode.

        Returns:
            list[Fill]: List of historical trades
        """
        if not self._safe_mode:
            return await self._real_api.get_trade_history(args)

        # Convert simulated fills to Fill objects
        trades: list[Fill] = []
        for fill in self._simulated_fills:
            trade = Fill(
                id=f"trade_{uuid.uuid4().hex[:8]}",
                symbol=fill.symbol,
                executed_at=fill.executed_at,
                side=fill.side,
                order_id=fill.order_id,
                exchange=self._exchange_name,
                price=fill.price,
                quantity=fill.quantity,
                fee=fill.fee,
                fee_asset=self.app_config.calculation.base_currency,
                client_order_id=None,
            )
            trades.append(trade)

        # Apply symbol filter if specified
        if args.symbol:
            trades = [trade for trade in trades if trade.symbol == args.symbol]

        # Sort by timestamp descending
        trades.sort(key=lambda x: x.executed_at, reverse=True)

        return trades

    async def get_order_status(self, args: GetOrderArgs) -> Order | None:
        """Get order status - simulated in safe mode, real in normal mode.

        Returns:
            Order | None: Order status or None if not found
        """
        if not self._safe_mode:
            return await self._real_api.get_order_status(args)

        return self._simulated_orders.get(args.order_id)

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Get order - simulated in safe mode, real in normal mode.

        Returns:
            Order | None: Order or None if not found
        """
        if not self._safe_mode:
            return await self._real_api.get_order(args)

        return self._simulated_orders.get(args.order_id)

    # Account management methods (most should be disabled in safe mode)
    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Update account settings - disabled in safe mode.

        Returns:
            AccountSettings: Updated account settings

        Raises:
            ValueError: If called in safe mode
        """
        if not self._safe_mode:
            return await self._real_api.update_account_settings(args)

        logger.warning(
            "account_settings_update_blocked_safe_mode",
            safe_mode=True,
            message="Account settings updates are disabled in safe mode",
        )
        msg = "Account settings updates are disabled in safe mode"
        raise ValueError(msg)

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Transfer - disabled in safe mode.

        Returns:
            Transfer: Transfer result

        Raises:
            ValueError: If called in safe mode
        """
        if not self._safe_mode:
            return await self._real_api.transfer(args)

        logger.warning(
            "transfer_blocked_safe_mode",
            safe_mode=True,
            message="Transfers are disabled in safe mode",
        )
        msg = "Transfers are disabled in safe mode"
        raise ValueError(msg)

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Withdraw - disabled in safe mode.

        Returns:
            Withdrawal: Withdrawal result

        Raises:
            ValueError: If called in safe mode
        """
        if not self._safe_mode:
            return await self._real_api.withdraw(args)

        logger.warning(
            "withdrawal_blocked_safe_mode",
            safe_mode=True,
            message="Withdrawals are disabled in safe mode",
        )
        msg = "Withdrawals are disabled in safe mode"
        raise ValueError(msg)

    # Additional required methods
    async def place_batch_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
        """Place batch orders - simulated in safe mode, real in normal mode.

        Returns:
            list[Order]: List of placed orders
        """
        if not self._safe_mode:
            return await self._real_api.place_batch_orders(orders)

        results: list[Order] = []
        for order_args in orders:
            order = await self.place_order(order_args)
            results.append(order)

        return results

    async def cancel_batch_orders(
        self,
        cancel_args: list[CancelOrderArgs],
    ) -> list[CancelOrderResult]:
        """Cancel batch orders - simulated in safe mode, real in normal mode.

        Returns:
            list[CancelOrderResult]: List of cancellation results
        """
        if not self._safe_mode:
            return await self._real_api.cancel_batch_orders(cancel_args)

        results: list[CancelOrderResult] = []
        for cancel_arg in cancel_args:
            result = await self.cancel_order(cancel_arg)
            results.append(result)

        return results

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Get all open orders - simulated in safe mode, real in normal mode.

        Returns:
            list[Order]: List of all open orders
        """
        if not self._safe_mode:
            return await self._real_api.get_all_open_orders(args)

        return await self.get_open_orders()

    # ===========================================
    # Safe Mode Simulation Methods
    # ===========================================

    def _initialize_balances(self) -> None:
        """Initialize simulated balances with configured starting amounts."""
        base_currency = self.app_config.calculation.base_currency
        if self._exchange_name == ExchangeName.HYPERLIQUID:
            base_symbol = hl_symbol(base_currency)
        else:
            base_symbol = bp_symbol(base_currency)

        self._simulated_balances[base_currency] = SpotBalance(
            exchange=self._exchange_name,
            asset=base_symbol,
            timestamp=datetime.now(UTC),
            total_quantity=self._initial_balance,
            available_quantity=self._initial_balance,
        )

        logger.info(
            "safe_mode_balances_initialized",
            exchange=self._exchange_name.value,
            base_currency=base_currency,
            initial_balance=self._initial_balance,
        )

    async def _simulate_fill(self, order: Order, immediate: bool = False) -> None:
        """Simulate order fill based on configuration."""
        # Check fill probability from config
        # Always use secrets for secure randomness
        fill_check = secrets.SystemRandom().random()

        if not immediate and fill_check > self._fill_probability:
            logger.debug(
                "safe_mode_order_not_filled",
                order_id=order.exchange_order_id,
                fill_probability=self._fill_probability,
            )
            return

        # Calculate fill price with configured slippage
        if order.price:
            base_price = order.price
        else:
            # For market orders, get price from market data if available
            ticker = await self.get_ticker(order.symbol)
            base_price = ticker.price if ticker and ticker.price else Decimal(50000)

        # Apply slippage from config
        # Always use secrets for secure randomness
        slippage_factor = Decimal(
            str(secrets.SystemRandom().uniform(-self._slippage_range[1], self._slippage_range[1])),
        )

        if order.side == OrderSide.BUY:
            # Buyers pay slightly more (unfavorable slippage)
            fill_price = base_price * (Decimal(1) + abs(slippage_factor))
        else:
            # Sellers receive slightly less (unfavorable slippage)
            fill_price = base_price * (Decimal(1) - abs(slippage_factor))

        # Calculate fee from exchange config
        fee_rate = (
            self._taker_fee_rate if order.order_type == OrderType.MARKET else self._maker_fee_rate
        )
        fee = order.quantity_requested * fill_price * fee_rate

        # Create simulated fill
        if order.exchange_order_id is None:
            # For simulation, generate an order ID if not present
            order.exchange_order_id = f"SIM-{order.client_order_id}"

        fill = Fill(
            id=f"SIM_FILL_{uuid.uuid4().hex[:8]}",
            order_id=order.exchange_order_id,  # Now guaranteed to be non-None
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            exchange=self._exchange_name,
            price=fill_price,
            quantity=order.quantity_requested,
            fee=fee,
            fee_asset="USD" if fee > Decimal(0) else None,
            executed_at=datetime.now(UTC),
        )

        self._simulated_fills.append(fill)

        # Update order status
        order.status = OrderStatus.FILLED
        order.quantity_filled = order.quantity_requested
        order.average_fill_price = fill_price

        # Update simulated state
        await self._update_balance_from_fill(fill)
        await self._update_position_from_fill(fill)

        logger.info(
            "safe_mode_order_filled",
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            side=order.side.value,
            fill_price=fill_price,
            quantity=order.quantity_requested,
            fee=fee,
            slippage_factor=slippage_factor,
            safe_mode=True,
            message="SIMULATED FILL - NOT A REAL TRADE",
        )

    async def _simulate_limit_fill(self, order: Order) -> None:
        """Simulate potential limit order fill with configured delay."""
        # Wait for potential fill based on config
        fill_delay = float(self._limit_fill_delay)
        await asyncio.sleep(fill_delay)

        # Check if order still open (might have been cancelled)
        if order.status == OrderStatus.OPEN:
            await self._simulate_fill(order)

    async def _update_balance_from_fill(self, fill: Fill) -> None:
        """Update simulated balance after fill."""
        base_currency = self.app_config.calculation.base_currency

        if base_currency not in self._simulated_balances:
            if self._exchange_name == ExchangeName.HYPERLIQUID:
                base_symbol = hl_symbol(base_currency)
            else:
                base_symbol = bp_symbol(base_currency)
            self._simulated_balances[base_currency] = SpotBalance(
                exchange=self._exchange_name,
                asset=base_symbol,
                timestamp=datetime.now(UTC),
                total_quantity=Decimal(0),
                available_quantity=Decimal(0),
            )

        balance = self._simulated_balances[base_currency]

        # Calculate balance change
        cost: Decimal
        proceeds: Decimal
        if fill.side == OrderSide.BUY:
            # Buying: decrease balance by cost + fee
            cost = fill.quantity * fill.price + fill.fee
            proceeds = Decimal(0)  # Not used for buys
            new_total = balance.total_quantity - cost
            new_available = balance.available_quantity - cost
        else:
            # Selling: increase balance by proceeds - fee
            proceeds = fill.quantity * fill.price - fill.fee
            cost = Decimal(0)  # Not used for sells
            new_total = balance.total_quantity + proceeds
            new_available = balance.available_quantity + proceeds

        # Create new balance (SpotBalance is immutable)
        self._simulated_balances[base_currency] = SpotBalance(
            exchange=balance.exchange,
            asset=balance.asset,
            timestamp=datetime.now(UTC),
            total_quantity=new_total,
            available_quantity=new_available,
        )

        logger.debug(
            "safe_mode_balance_updated",
            asset=base_currency,
            total=new_total,
            available=new_available,
            change=-cost if fill.side == OrderSide.BUY else proceeds,
        )

    async def _update_position_from_fill(self, fill: Fill) -> None:
        """Update simulated position after fill."""
        symbol_key = fill.symbol.value

        if symbol_key not in self._simulated_positions:
            # Create new position
            self._simulated_positions[symbol_key] = DerivativePosition(
                exchange=self._exchange_name,
                symbol=fill.symbol,
                side=fill.side,
                size=fill.quantity,
                entry_price=fill.price,
                timestamp=datetime.now(UTC),
                unrealized_pnl=Decimal(0),
            )
        else:
            position = self._simulated_positions[symbol_key]

            # Calculate new position
            if position.side == fill.side:
                # Adding to position
                new_size = position.size + fill.quantity
                entry_price = (
                    position.entry_price if position.entry_price is not None else Decimal(0)
                )
                new_entry = (position.size * entry_price + fill.quantity * fill.price) / new_size

                # Create new position (DerivativePosition is immutable)
                self._simulated_positions[symbol_key] = DerivativePosition(
                    exchange=position.exchange,
                    symbol=position.symbol,
                    side=position.side,
                    size=new_size,
                    entry_price=new_entry,
                    timestamp=datetime.now(UTC),
                    unrealized_pnl=Decimal(0),
                )
            # Reducing position
            elif fill.quantity >= position.size:
                # Position closed
                entry_price = (
                    position.entry_price if position.entry_price is not None else Decimal(0)
                )
                realized_pnl = (fill.price - entry_price) * position.size
                if position.side == OrderSide.SELL:
                    realized_pnl = -realized_pnl

                del self._simulated_positions[symbol_key]

                logger.info(
                    "safe_mode_position_closed",
                    symbol=symbol_key,
                    realized_pnl=realized_pnl,
                )
            else:
                # Position reduced
                new_size = position.size - fill.quantity

                # Create new position with reduced size
                self._simulated_positions[symbol_key] = DerivativePosition(
                    exchange=position.exchange,
                    symbol=position.symbol,
                    side=position.side,
                    size=new_size,
                    entry_price=position.entry_price,
                    timestamp=datetime.now(UTC),
                    unrealized_pnl=Decimal(0),
                )

    def get_simulated_stats(self) -> dict[str, Any]:
        """Get statistics about simulated trading.

        Returns:
            dict[str, Any]: Dictionary containing simulation statistics
        """
        total_orders = len(self._simulated_orders)
        filled_orders = sum(
            1 for o in self._simulated_orders.values() if o.status == OrderStatus.FILLED
        )

        total_volume = sum(f.quantity * f.price for f in self._simulated_fills)
        total_fees = sum(f.fee for f in self._simulated_fills)

        return {
            "safe_mode": True,
            "total_orders": total_orders,
            "filled_orders": filled_orders,
            "fill_rate": filled_orders / total_orders if total_orders > 0 else 0,
            "total_fills": len(self._simulated_fills),
            "total_volume": total_volume,
            "total_fees": total_fees,
            "open_positions": len(self._simulated_positions),
            "current_balance": {
                asset: balance.total_quantity for asset, balance in self._simulated_balances.items()
            },
        }

    def is_safe_mode(self) -> bool:
        """Check if safe mode is active.

        Returns:
            bool: True if safe mode is active, False otherwise
        """
        return self._safe_mode

    async def cleanup(self) -> None:
        """Clean up any resources used by the wrapper."""
        # Cancel any pending limit fill tasks
        for task in self._limit_fill_tasks:
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete
        if self._limit_fill_tasks:
            await asyncio.gather(*self._limit_fill_tasks, return_exceptions=True)

        self._limit_fill_tasks.clear()
