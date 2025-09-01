"""
Funding Arbitrage Executor - Custom executor for atomic management of paired positions
This executor ensures hedging safety by managing both long and short positions as a single unit
"""
import asyncio
import logging
from decimal import Decimal
from typing import Any

from hummingbot.client.settings import AllConnectorSettings
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

from .data_types import FundingArbitrageExecutorConfig


class FundingArbitrageExecutor(ExecutorBase):
    """
    Custom executor that atomically manages paired long/short positions for funding arbitrage.
    Ensures both positions are hedged and handles emergency situations.
    """
    _logger = None

    @classmethod
    def logger(cls):
        """Get logger instance for this class."""
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy,
        config: FundingArbitrageExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 3,
    ):
        # Extract connectors list from config
        connectors = [config.long_connector_name, config.short_connector_name]
        super().__init__(strategy, connectors, config, update_interval)
        self.config: FundingArbitrageExecutorConfig = config
        self.max_retries = max_retries

        # Order tracking using TrackedOrder for proper fee tracking
        self._long_order: TrackedOrder | None = None
        self._short_order: TrackedOrder | None = None

        # Position state
        self._long_filled = False
        self._short_filled = False
        self._long_position_amount: Decimal = Decimal(0)
        self._short_position_amount: Decimal = Decimal(0)

        # Timing tracking
        self._entry_start_time: float | None = None
        self._exposure_start_time: float | None = None
        self._warning_logged = False

        # PnL tracking
        self._funding_payments_received: Decimal = Decimal(0)

    @property
    def is_active(self) -> bool:
        """Executor is active if we have any open positions or pending orders"""
        return (
            self._long_order is not None or
            self._short_order is not None or
            self._long_filled or
            self._short_filled
        )

    @property
    def is_trading(self) -> bool:
        """Returns true if both positions are filled and hedged"""
        return self._long_filled and self._short_filled

    def start(self):
        """Start the executor and begin control task"""
        super().start()
        self.logger().info(f"Starting FundingArbitrageExecutor for {self.config.token}")
        self._entry_start_time = self._strategy.current_timestamp

    def stop(self):
        """Stop the executor and close all positions"""
        self.logger().info(f"Stopping FundingArbitrageExecutor for {self.config.token}")
        # Set shutting down status to trigger position closing in control_task
        self._status = RunnableStatus.SHUTTING_DOWN
        super().stop()

    async def control_task(self):
        """Main control task managing both positions atomically - called periodically by base class"""
        if self._status == RunnableStatus.RUNNING:
            await self.control_positions()
        elif self._status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()

    async def control_positions(self):
        """Control the funding arbitrage positions during normal operation"""
        try:
            current_time = self._strategy.current_timestamp

            # Step 1: Place initial orders if not yet placed
            if not self._long_order and not self._short_order and not self._long_filled and not self._short_filled:
                await self.place_entry_orders()

            # Step 2: Check order status
            await self.update_order_status()

            # Step 3: Handle unhedged exposure with progressive reconciliation
            if self._has_unhedged_exposure():
                if self._exposure_start_time is None:
                    self._exposure_start_time = current_time
                    self._reconciliation_attempted = False
                    self._market_order_attempted = False

                exposure_duration = current_time - self._exposure_start_time

                # Warning threshold (15s) - just warn
                if exposure_duration > self.config.warning_exposure_time and not self._warning_logged:
                    self.logger().warning(
                        f"⚠️ Unhedged exposure for {exposure_duration:.1f}s on {self.config.token} "
                        f"(Long: {'FILLED' if self._long_filled else 'PENDING'}, "
                        f"Short: {'FILLED' if self._short_filled else 'PENDING'})",
                    )
                    self._warning_logged = True
                    # Start reconciliation - cancel and replace with aggressive limit
                    await self._attempt_reconciliation_with_limit()
                    self._reconciliation_attempted = True

                # Reconciliation threshold (60s) - use market order
                elif exposure_duration > self.config.max_unhedged_exposure_time and not self._market_order_attempted:
                    self.logger().error(
                        f"🚨 RECONCILIATION: Using MARKET ORDER after {exposure_duration:.1f}s "
                        f"for {self.config.token}",
                    )
                    await self._attempt_reconciliation_with_market()
                    self._market_order_attempted = True

                # Emergency close threshold (120s) - market order failed, close unhedged position only
                elif exposure_duration > self.config.max_unhedged_exposure_time * 2:
                    self.logger().error(
                        f"💀 EMERGENCY CLOSE: Market order reconciliation failed after {exposure_duration:.1f}s "
                        f"for {self.config.token}. Closing the unhedged position only.",
                    )
                    await self.emergency_hedge()
                    # Reset exposure tracking after emergency hedge
                    self._exposure_start_time = None
                    self._warning_logged = False
                    self._reconciliation_attempted = False
                    self._market_order_attempted = False

            # Step 4: Reset exposure tracking when hedged
            elif self._long_filled and self._short_filled:
                if self._exposure_start_time:
                    self.logger().info(f"✅ Positions hedged for {self.config.token}")
                    self._exposure_start_time = None
                    self._warning_logged = False

                # Check exit conditions
                if await self.should_exit_position():
                    await self.close_all_positions(close_type=CloseType.TAKE_PROFIT)
                    self._status = RunnableStatus.SHUTTING_DOWN

            # Step 5: Handle entry timeout
            if (self._entry_start_time and not self.is_trading and
                    current_time - self._entry_start_time > self.config.entry_timeout):
                self.logger().warning(
                    f"Entry timeout reached for {self.config.token}, cancelling unfilled orders",
                )
                await self.cancel_unfilled_orders()
                if self._long_filled or self._short_filled:
                    # We have partial fill, need to close
                    await self.close_all_positions(close_type=CloseType.TIME_LIMIT)
                self._status = RunnableStatus.SHUTTING_DOWN

        except Exception as e:
            self.logger().error(f"Error in control task for {self.config.token}: {e}")
            # Try to hedge if we have unhedged exposure
            if self._has_unhedged_exposure():
                await self.emergency_hedge()

    async def control_shutdown_process(self):
        """Handle the shutdown process"""
        # Close positions if needed
        if self.close_type != CloseType.POSITION_HOLD:
            if self._long_filled or self._short_filled:
                # Pass the close type to determine order type
                await self.close_all_positions(close_type=self.close_type)
        else:
            # Position hold - just cancel unfilled orders
            await self.cancel_unfilled_orders()

        # Mark as terminated when done
        if not self.is_active:
            self._status = RunnableStatus.TERMINATED

    def calculate_maker_price(self, connector, trading_pair: str, is_buy: bool, is_entry: bool = False) -> Decimal:
        """
        Calculate optimal maker price based on order book spread and tick size.

        Args:
            connector: The exchange connector
            trading_pair: The trading pair
            is_buy: True for buy orders, False for sell orders
            is_entry: True for entry orders (more aggressive), False for exit orders

        Returns:
            Optimal price for placing a maker order
        """
        try:
            # Get the order book for accurate spread analysis
            order_book = connector.get_order_book(trading_pair)
            bid = Decimal(str(order_book.get_price(False)))  # Best bid
            ask = Decimal(str(order_book.get_price(True)))   # Best ask
            spread = ask - bid
            spread_pct = spread / bid * Decimal(100)

            # Get tick size for proper price rounding
            reference_price = bid if is_buy else ask
            tick_size = connector.get_order_price_quantum(trading_pair, reference_price)

            # For entry orders, be more aggressive to ensure both sides fill
            if is_entry:
                # Use mid-price or slightly into the spread for entry orders
                mid_price = (bid + ask) / Decimal(2)
                if is_buy:
                    # Buy slightly above mid (but still maker side)
                    price = mid_price - spread * Decimal("0.2")  # 20% from mid toward bid
                    price = max(price, bid)  # But at least at bid
                else:
                    # Sell slightly below mid (but still maker side)
                    price = mid_price + spread * Decimal("0.2")  # 20% from mid toward ask
                    price = min(price, ask)  # But at most at ask
            else:
                # For exit orders, prioritize being a maker for fee savings
                if is_buy:
                    # For buy orders, place on bid side
                    if spread_pct < Decimal("0.1"):
                        # Tight spread: join the existing bid
                        price = bid
                    else:
                        # Wider spread: place 1 tick better than bid for priority
                        price = bid + tick_size
                        # But never cross to ask side
                        price = min(price, ask - tick_size)
                else:
                    # For sell orders, place on ask side
                    if spread_pct < Decimal("0.1"):
                        # Tight spread: join the existing ask
                        price = ask
                    else:
                        # Wider spread: place 1 tick better than ask for priority
                        price = ask - tick_size
                        # But never cross to bid side
                        price = max(price, bid + tick_size)

            return price

        except Exception as e:
            self.logger().warning(f"Failed to get order book for {trading_pair}, using fallback: {e}")
            # Fallback to simple price quotes if order book not available
            if is_buy:
                return connector.get_price_by_type(trading_pair, PriceType.BestBid)
            else:
                return connector.get_price_by_type(trading_pair, PriceType.BestAsk)

    async def place_entry_orders(self):
        """Place both entry orders atomically"""
        try:
            long_connector = self.connectors[self.config.long_connector_name]
            short_connector = self.connectors[self.config.short_connector_name]

            # Calculate order amounts
            long_mid_price = long_connector.get_mid_price(self.config.long_trading_pair)
            short_mid_price = short_connector.get_mid_price(self.config.short_trading_pair)

            long_amount = self.config.position_size_quote / long_mid_price
            short_amount = self.config.position_size_quote / short_mid_price

            # Get order prices based on order type
            if self.config.open_order_type == OrderType.LIMIT:
                # Use smart maker pricing for LIMIT orders (more aggressive for entry)
                long_price = self.calculate_maker_price(
                    long_connector,
                    self.config.long_trading_pair,
                    is_buy=True,
                    is_entry=True,
                )
                short_price = self.calculate_maker_price(
                    short_connector,
                    self.config.short_trading_pair,
                    is_buy=False,
                    is_entry=True,
                )
            else:
                # For MARKET orders, use the fill price
                long_price = long_connector.get_price_for_volume(
                    self.config.long_trading_pair, True, long_amount,
                ).result_price
                short_price = short_connector.get_price_for_volume(
                    self.config.short_trading_pair, False, short_amount,
                ).result_price

            self.logger().info(
                f"Placing entry orders for {self.config.token}: "
                f"Long {long_amount:.6f} @ {long_price:.4f}, "
                f"Short {short_amount:.6f} @ {short_price:.4f}",
            )

            # Place orders
            if self.config.open_order_type == OrderType.LIMIT:
                self._long_order = TrackedOrder()
                self._long_order.order_id = self.place_order(
                    connector_name=self.config.long_connector_name,
                    trading_pair=self.config.long_trading_pair,
                    order_type=OrderType.LIMIT,
                    side=TradeType.BUY,
                    amount=long_amount,
                    position_action=PositionAction.OPEN,
                    price=long_price,
                )

                self._short_order = TrackedOrder()
                self._short_order.order_id = self.place_order(
                    connector_name=self.config.short_connector_name,
                    trading_pair=self.config.short_trading_pair,
                    order_type=OrderType.LIMIT,
                    side=TradeType.SELL,
                    amount=short_amount,
                    position_action=PositionAction.OPEN,
                    price=short_price,
                )
            else:
                # Market orders
                self._long_order = TrackedOrder()
                self._long_order.order_id = self.place_order(
                    connector_name=self.config.long_connector_name,
                    trading_pair=self.config.long_trading_pair,
                    order_type=OrderType.MARKET,
                    side=TradeType.BUY,
                    amount=long_amount,
                    position_action=PositionAction.OPEN,
                )

                self._short_order = TrackedOrder()
                self._short_order.order_id = self.place_order(
                    connector_name=self.config.short_connector_name,
                    trading_pair=self.config.short_trading_pair,
                    order_type=OrderType.MARKET,
                    side=TradeType.SELL,
                    amount=short_amount,
                    position_action=PositionAction.OPEN,
                )

        except Exception as e:
            self.logger().error(f"Failed to place entry orders for {self.config.token}: {e}")
            raise

    async def update_order_status(self):
        """Check and update the status of our orders"""
        # Check long order
        if self._long_order and not self._long_filled:
            order = self.get_in_flight_order(
                self.config.long_connector_name,
                self._long_order.order_id,
            )
            if order and order.is_filled:
                self._long_filled = True
                self._long_position_amount = order.executed_amount_base
                self._long_order.order = order  # Store the InFlightOrder in TrackedOrder
                self.logger().info(f"Long position filled for {self.config.token}: {self._long_position_amount}")

        # Check short order
        if self._short_order and not self._short_filled:
            order = self.get_in_flight_order(
                self.config.short_connector_name,
                self._short_order.order_id,
            )
            if order and order.is_filled:
                self._short_filled = True
                self._short_position_amount = order.executed_amount_base
                self._short_order.order = order  # Store the InFlightOrder in TrackedOrder
                self.logger().info(f"Short position filled for {self.config.token}: {self._short_position_amount}")

    def _has_unhedged_exposure(self) -> bool:
        """Check if we have unhedged exposure (one position filled but not the other)"""
        return self._long_filled != self._short_filled

    async def _attempt_reconciliation_with_limit(self):
        """Try to reconcile positions with aggressive limit orders"""
        try:
            if self._long_filled and not self._short_filled:
                # Long filled, need to fill short
                self.logger().info(f"Reconciling: Cancelling and replacing SHORT order for {self.config.token}")

                # Cancel existing short order
                if self._short_order and self._short_order.order_id:
                    await self.cancel_order(
                        self.config.short_connector_name,
                        self.config.short_trading_pair,
                        self._short_order.order_id,
                    )

                # Place more aggressive limit order (improve price by 0.1%)
                mid_price = self.connectors[self.config.short_connector_name].get_mid_price(self.config.short_trading_pair)
                aggressive_price = mid_price * Decimal("0.999")  # 0.1% better price for faster fill (lower for sell)

                # Initialize TrackedOrder if needed
                if not self._short_order:
                    self._short_order = TrackedOrder()

                self._short_order.order_id = self.place_order(
                    connector_name=self.config.short_connector_name,
                    trading_pair=self.config.short_trading_pair,
                    order_type=OrderType.LIMIT,
                    side=TradeType.SELL,
                    amount=self._short_position_amount,
                    price=aggressive_price,
                    position_action=PositionAction.OPEN,
                )

            elif self._short_filled and not self._long_filled:
                # Short filled, need to fill long
                self.logger().info(f"Reconciling: Cancelling and replacing LONG order for {self.config.token}")

                # Cancel existing long order
                if self._long_order and self._long_order.order_id:
                    await self.cancel_order(
                        self.config.long_connector_name,
                        self.config.long_trading_pair,
                        self._long_order.order_id,
                    )

                # Place more aggressive limit order (improve price by 0.1%)
                mid_price = self.connectors[self.config.long_connector_name].get_mid_price(self.config.long_trading_pair)
                aggressive_price = mid_price * Decimal("1.001")  # 0.1% better price for faster fill (higher for buy)

                # Initialize TrackedOrder if needed
                if not self._long_order:
                    self._long_order = TrackedOrder()

                self._long_order.order_id = self.place_order(
                    connector_name=self.config.long_connector_name,
                    trading_pair=self.config.long_trading_pair,
                    order_type=OrderType.LIMIT,
                    side=TradeType.BUY,
                    amount=self._long_position_amount,
                    price=aggressive_price,
                    position_action=PositionAction.OPEN,
                )

        except Exception as e:
            self.logger().error(f"Failed to reconcile with limit order for {self.config.token}: {e}")

    async def _attempt_reconciliation_with_market(self):
        """Force reconciliation with market orders"""
        try:
            if self._long_filled and not self._short_filled:
                # Long filled, need to fill short with market order
                self.logger().info(f"MARKET ORDER: Forcing SHORT position for {self.config.token}")

                # Cancel existing short order if any
                if self._short_order and self._short_order.order_id:
                    await self.cancel_order(
                        self.config.short_connector_name,
                        self.config.short_trading_pair,
                        self._short_order.order_id,
                    )

                # Initialize TrackedOrder if needed
                if not self._short_order:
                    self._short_order = TrackedOrder()

                # Place market order
                self._short_order.order_id = self.place_order(
                    connector_name=self.config.short_connector_name,
                    trading_pair=self.config.short_trading_pair,
                    order_type=OrderType.MARKET,
                    side=TradeType.SELL,
                    amount=self._short_position_amount,
                    position_action=PositionAction.OPEN,
                )

            elif self._short_filled and not self._long_filled:
                # Short filled, need to fill long with market order
                self.logger().info(f"MARKET ORDER: Forcing LONG position for {self.config.token}")

                # Cancel existing long order if any
                if self._long_order and self._long_order.order_id:
                    await self.cancel_order(
                        self.config.long_connector_name,
                        self.config.long_trading_pair,
                        self._long_order.order_id,
                    )

                # Initialize TrackedOrder if needed
                if not self._long_order:
                    self._long_order = TrackedOrder()

                # Place market order
                self._long_order.order_id = self.place_order(
                    connector_name=self.config.long_connector_name,
                    trading_pair=self.config.long_trading_pair,
                    order_type=OrderType.MARKET,
                    side=TradeType.BUY,
                    amount=self._long_position_amount,
                    position_action=PositionAction.OPEN,
                )

        except Exception as e:
            self.logger().error(f"Failed to reconcile with MARKET order for {self.config.token}: {e}")

    async def emergency_hedge(self):
        """Emergency procedure to close ONLY the unhedged position (not all positions)"""
        self.logger().error(f"🚨 Executing emergency close of unhedged position for {self.config.token}")

        try:
            if self._long_filled and not self._short_filled:
                # Close long position immediately
                await self._close_long_position(use_market=True)
                # Cancel pending short order
                if self._short_order:
                    await self._cancel_order(
                        self.config.short_connector_name,
                        self.config.short_trading_pair,
                        self._short_order.order_id,
                    )

            elif self._short_filled and not self._long_filled:
                # Close short position immediately
                await self._close_short_position(use_market=True)
                # Cancel pending long order
                if self._long_order:
                    await self._cancel_order(
                        self.config.long_connector_name,
                        self.config.long_trading_pair,
                        self._long_order.order_id,
                    )

        except Exception as e:
            self.logger().error(f"Emergency hedge failed for {self.config.token}: {e}")

    async def should_exit_position(self) -> bool:
        """Check if we should exit the arbitrage position"""
        config = self.config.triple_barrier_config

        # Check funding rate stop loss
        current_funding_diff = self._get_current_funding_diff()
        if current_funding_diff < self.config.funding_rate_diff_stop_loss:
            self.logger().info(
                f"Funding rate stop loss triggered for {self.config.token}: "
                f"{current_funding_diff:.4%} < {self.config.funding_rate_diff_stop_loss:.4%}",
            )
            return True

        # Check take profit
        if config.take_profit:
            profit_pct = self.net_pnl_quote / self.config.position_size_quote
            if profit_pct >= config.take_profit:
                self.logger().info(
                    f"Take profit triggered for {self.config.token}: "
                    f"{profit_pct:.2%} >= {config.take_profit:.2%}",
                )
                return True

        # Check stop loss
        if config.stop_loss:
            loss_pct = self.net_pnl_quote / self.config.position_size_quote
            if loss_pct <= -config.stop_loss:
                self.logger().info(
                    f"Stop loss triggered for {self.config.token}: "
                    f"{loss_pct:.2%} <= -{config.stop_loss:.2%}",
                )
                return True

        # Check time limit
        if (config.time_limit and self._entry_start_time and
                self._strategy.current_timestamp - self._entry_start_time > config.time_limit):
            self.logger().info(f"Time limit reached for {self.config.token}")
            return True

        return False

    async def close_all_positions(self, close_type: CloseType | None = None):
        """Close both positions

        Args:
            close_type: The type of close operation (if None, uses default close_order_type from config)
        """
        self.logger().info(f"Closing all positions for {self.config.token}")

        # Determine if we should use market orders based on close type
        # Only true emergencies (failed states, insufficient balance) should force market orders
        emergency_close_types = [CloseType.FAILED, CloseType.INSUFFICIENT_BALANCE]

        if close_type in emergency_close_types:
            # Emergency scenarios always use market orders for quick exit
            use_market = True
        else:
            # For all other stops including EARLY_STOP (user stop command), use take_profit_order_type
            # (which should ideally be set from the controller's close_order_type)
            use_market = self.config.triple_barrier_config.take_profit_order_type == OrderType.MARKET

        # Close in parallel for speed
        tasks = []
        if self._long_filled:
            tasks.append(self._close_long_position(use_market))
        if self._short_filled:
            tasks.append(self._close_short_position(use_market))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Cancel any pending orders
        await self.cancel_unfilled_orders()

    async def _close_long_position(self, use_market: bool = False):
        """Close the long position"""
        if self._long_position_amount > 0:
            self.logger().info(f"Closing long position for {self.config.token}: {self._long_position_amount}")

            # Get the current price for LIMIT orders
            if not use_market:
                connector = self.connectors[self.config.long_connector_name]
                # For closing long, we're selling (is_buy=False)
                price = self.calculate_maker_price(
                    connector,
                    self.config.long_trading_pair,
                    is_buy=False,
                )
                self.place_order(
                    connector_name=self.config.long_connector_name,
                    trading_pair=self.config.long_trading_pair,
                    order_type=OrderType.LIMIT,
                    side=TradeType.SELL,
                    amount=self._long_position_amount,
                    position_action=PositionAction.CLOSE,
                    price=price,
                )
            else:
                self.place_order(
                    connector_name=self.config.long_connector_name,
                    trading_pair=self.config.long_trading_pair,
                    order_type=OrderType.MARKET,
                    side=TradeType.SELL,
                    amount=self._long_position_amount,
                    position_action=PositionAction.CLOSE,
                )

            self._long_filled = False
            self._long_position_amount = Decimal(0)

    async def _close_short_position(self, use_market: bool = False):
        """Close the short position"""
        if self._short_position_amount > 0:
            self.logger().info(f"Closing short position for {self.config.token}: {self._short_position_amount}")

            # Get the current price for LIMIT orders
            if not use_market:
                connector = self.connectors[self.config.short_connector_name]
                # For closing short, we're buying (is_buy=True)
                price = self.calculate_maker_price(
                    connector,
                    self.config.short_trading_pair,
                    is_buy=True,
                )
                self.place_order(
                    connector_name=self.config.short_connector_name,
                    trading_pair=self.config.short_trading_pair,
                    order_type=OrderType.LIMIT,
                    side=TradeType.BUY,
                    amount=self._short_position_amount,
                    position_action=PositionAction.CLOSE,
                    price=price,
                )
            else:
                self.place_order(
                    connector_name=self.config.short_connector_name,
                    trading_pair=self.config.short_trading_pair,
                    order_type=OrderType.MARKET,
                    side=TradeType.BUY,
                    amount=self._short_position_amount,
                    position_action=PositionAction.CLOSE,
                )

            self._short_filled = False
            self._short_position_amount = Decimal(0)

    async def cancel_unfilled_orders(self):
        """Cancel any unfilled orders"""
        if self._long_order and not self._long_filled:
            await self._cancel_order(
                self.config.long_connector_name,
                self.config.long_trading_pair,
                self._long_order.order_id,
            )
            self._long_order = None

        if self._short_order and not self._short_filled:
            await self._cancel_order(
                self.config.short_connector_name,
                self.config.short_trading_pair,
                self._short_order.order_id,
            )
            self._short_order = None

    async def _cancel_order(self, connector_name: str, trading_pair: str, order_id: str):
        """Cancel a specific order"""
        try:
            self._strategy.cancel(connector_name, trading_pair, order_id)
        except Exception as e:
            self.logger().warning(f"Failed to cancel order {order_id}: {e}")

    def _get_position_pnl(self, connector_name: str, trading_pair: str) -> Decimal:
        """Get PnL for a specific position"""
        connector = self.connectors.get(connector_name)
        if connector and hasattr(connector, "get_unrealized_pnl"):
            return connector.get_unrealized_pnl(trading_pair) or Decimal(0)
        return Decimal(0)

    def _get_current_funding_diff(self) -> Decimal:
        """Calculate current funding rate differential"""
        long_connector = self.connectors.get(self.config.long_connector_name)
        short_connector = self.connectors.get(self.config.short_connector_name)

        if not long_connector or not short_connector:
            return Decimal(0)

        # Get funding rates
        long_funding_rate = self._get_funding_rate(long_connector, self.config.long_trading_pair)
        short_funding_rate = self._get_funding_rate(short_connector, self.config.short_trading_pair)

        # Return differential (we earn from short, pay on long)
        return short_funding_rate - long_funding_rate

    def _get_funding_rate(self, connector, trading_pair: str) -> Decimal:
        """Get funding rate for a specific position"""
        try:
            if hasattr(connector, "get_funding_info"):
                funding_info = connector.get_funding_info(trading_pair)
                if funding_info and hasattr(funding_info, "rate"):
                    return Decimal(str(funding_info.rate))
        except Exception as e:
            self.logger().warning(f"Failed to get funding rate for {trading_pair}: {e}")
        return Decimal(0)

    def calculate_profitability(self) -> tuple[Decimal, Decimal]:
        """
        Calculate the profitability of the arbitrage including fees.
        Returns (open_profitability_pct, close_profitability_pct)
        """
        funding_diff = self._get_current_funding_diff()

        # Get connectors
        long_connector = self.connectors.get(self.config.long_connector_name)
        short_connector = self.connectors.get(self.config.short_connector_name)

        if not long_connector or not short_connector:
            return Decimal(0), Decimal(0)

        # Get mid prices
        mid_price_long = long_connector.get_mid_price(self.config.long_trading_pair)
        mid_price_short = short_connector.get_mid_price(self.config.short_trading_pair)

        # Calculate amounts
        amount_long = self.config.position_size_quote / mid_price_long
        amount_short = self.config.position_size_quote / mid_price_short

        # Calculate fees for opening positions
        fee_open_long = self._get_fee_with_fallback(
            long_connector,
            self.config.long_connector_name,
            self.config.long_trading_pair,
            OrderType.MARKET,
            TradeType.BUY,
            amount_long,
            mid_price_long,
            is_maker=False,
            position_action=PositionAction.OPEN,
        )

        fee_open_short = self._get_fee_with_fallback(
            short_connector,
            self.config.short_connector_name,
            self.config.short_trading_pair,
            OrderType.MARKET,
            TradeType.SELL,
            amount_short,
            mid_price_short,
            is_maker=False,
            position_action=PositionAction.OPEN,
        )

        # Calculate fees for closing positions
        fee_close_long = self._get_fee_with_fallback(
            long_connector,
            self.config.long_connector_name,
            self.config.long_trading_pair,
            OrderType.MARKET,
            TradeType.SELL,
            amount_long,
            mid_price_long,
            is_maker=False,
            position_action=PositionAction.CLOSE,
        )

        fee_close_short = self._get_fee_with_fallback(
            short_connector,
            self.config.short_connector_name,
            self.config.short_trading_pair,
            OrderType.MARKET,
            TradeType.BUY,
            amount_short,
            mid_price_short,
            is_maker=False,
            position_action=PositionAction.CLOSE,
        )

        # Calculate profitability
        open_profitability = funding_diff - fee_open_long - fee_open_short
        close_profitability = -funding_diff - fee_close_long - fee_close_short

        return open_profitability, close_profitability

    def _get_fee_with_fallback(
        self,
        connector_instance,
        connector_name: str,
        trading_pair: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal,
        is_maker: bool,
        position_action: PositionAction,
    ) -> Decimal:
        """Get fee with fallback using exchange's configured fee schema."""
        base_currency, quote_currency = trading_pair.split("-", 1)

        try:
            # Try to get fee from connector
            fee = connector_instance.get_fee(
                base_currency=base_currency,
                quote_currency=quote_currency,
                order_type=order_type,
                order_side=order_side,
                amount=amount,
                price=price,
                is_maker=is_maker,
                position_action=position_action,
            )
            return fee.percent
        except Exception as e:
            self.logger().debug(f"Failed to get fee from connector: {e}, using fallback")

            # Fallback to configured fee schema
            try:
                # Get the fee schema for the exchange
                connector_settings = AllConnectorSettings.get_connector_settings()
                if connector_name in connector_settings:
                    fee_schema = connector_settings[connector_name].trade_fee_schema
                    if fee_schema:
                        fee_percent = (
                            fee_schema.maker_percent_fee_decimal
                            if is_maker
                            else fee_schema.taker_percent_fee_decimal
                        )
                        return fee_percent
            except Exception as fallback_error:
                self.logger().warning(f"Failed to get fee from fallback: {fallback_error}")

        # Default fee if all else fails (0.1% taker fee as conservative estimate)
        return Decimal("0.001")

    def on_funding_payment_received(self, amount: Decimal):
        """Track funding payments received"""
        self._funding_payments_received += amount
        self.logger().info(f"Funding payment received for {self.config.token}: {amount:.6f}")

    def get_net_pnl_quote(self) -> Decimal:
        """
        Returns the net profit or loss in quote currency.
        Includes both realized PnL and funding payments.
        """
        # Use existing method that already calculates position PnL
        long_pnl = self._get_position_pnl(self.config.long_connector_name, self.config.long_trading_pair)
        short_pnl = self._get_position_pnl(self.config.short_connector_name, self.config.short_trading_pair)

        # Add funding payments received
        total_pnl = long_pnl + short_pnl + self._funding_payments_received - self.get_cum_fees_quote()

        return total_pnl

    def get_net_pnl_pct(self) -> Decimal:
        """
        Returns the net profit or loss in percentage.
        """
        net_pnl = self.get_net_pnl_quote()

        # Calculate based on position size
        if self.config.position_size_quote > 0:
            return (net_pnl / self.config.position_size_quote) * Decimal(100)

        return Decimal(0)

    def get_cum_fees_quote(self) -> Decimal:
        """
        Returns the cumulative fees in quote currency.
        """
        # Sum fees from both long and short orders using TrackedOrder properties
        orders = [self._long_order, self._short_order]
        return sum([order.cum_fees_quote for order in orders if order])

    @property
    def net_pnl_quote(self) -> Decimal:
        """Property wrapper for compatibility"""
        return self.get_net_pnl_quote()

    def to_dict(self) -> dict[str, Any]:
        """Convert executor state to dictionary for persistence"""
        return {
            "config": self.config.model_dump(),
            "long_filled": self._long_filled,
            "short_filled": self._short_filled,
            "long_position_amount": str(self._long_position_amount),
            "short_position_amount": str(self._short_position_amount),
            "funding_payments_received": str(self._funding_payments_received),
            "net_pnl_quote": str(self.get_net_pnl_quote()),
        }

    async def validate_sufficient_balance(self):
        """
        Validates that the executor has sufficient balance to place orders.
        For funding arbitrage, we need to check balance on both exchanges.
        """
        # Calculate required amounts
        position_size_quote = self.config.position_size_quote

        # For long position (spot/perp buy), we need quote currency
        long_connector = self.connectors[self.config.long_connector_name]
        _, long_quote = self.config.long_trading_pair.split("-", 1)
        long_quote_balance = long_connector.get_available_balance(long_quote)

        if long_quote_balance < position_size_quote:
            self.logger().error(
                f"Insufficient {long_quote} balance on {self.config.long_connector_name}: "
                f"have {long_quote_balance}, need {position_size_quote}",
            )
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            raise ValueError(f"Insufficient balance for long position on {self.config.long_connector_name}")

        # For short position (perp sell), check if we have enough margin or collateral
        short_connector = self.connectors[self.config.short_connector_name]
        short_base, short_quote = self.config.short_trading_pair.split("-", 1)

        # For perpetual short, we need sufficient margin/collateral
        if self.is_perpetual_connector(self.config.short_connector_name):
            # Check available margin or quote balance for collateral
            short_quote_balance = short_connector.get_available_balance(short_quote)
            # Assume we need at least position_size_quote / leverage for margin
            # Using conservative 10x leverage assumption if not specified
            required_margin = position_size_quote / Decimal(10)

            if short_quote_balance < required_margin:
                self.logger().error(
                    f"Insufficient margin on {self.config.short_connector_name}: "
                    f"have {short_quote_balance}, need at least {required_margin}",
                )
                self.close_type = CloseType.INSUFFICIENT_BALANCE
                raise ValueError(f"Insufficient margin for short position on {self.config.short_connector_name}")
        else:
            # For spot short, we would need the base currency to sell
            # But this is unusual for funding arbitrage (usually long spot, short perp)
            short_base_balance = short_connector.get_available_balance(short_base)
            required_base = position_size_quote / self.get_price(
                self.config.short_connector_name,
                self.config.short_trading_pair,
                PriceType.MidPrice,
            )

            if short_base_balance < required_base:
                self.logger().error(
                    f"Insufficient {short_base} balance on {self.config.short_connector_name}: "
                    f"have {short_base_balance}, need {required_base}",
                )
                self.close_type = CloseType.INSUFFICIENT_BALANCE
                raise ValueError(f"Insufficient balance for short position on {self.config.short_connector_name}")

        self.logger().info(
            f"Balance validation passed for {self.config.token} arbitrage: "
            f"Long {self.config.long_connector_name} has {long_quote_balance} {long_quote}, "
            f"Short {self.config.short_connector_name} has sufficient balance",
        )

    def early_stop(self, keep_position: bool = False):
        """
        This method allows the orchestrator to stop the executor early.

        :param keep_position: If True, keep positions open; if False, close positions
        """
        self.logger().info(
            f"Early stop requested for {self.config.token} arbitrage "
            f"(keep_position={keep_position})",
        )

        if keep_position:
            # Just stop the executor without closing positions
            self.close_type = CloseType.POSITION_HOLD
        else:
            # Close positions and stop
            self.close_type = CloseType.EARLY_STOP

        # Set status to shutting down to trigger position closing in control_task
        self._status = RunnableStatus.SHUTTING_DOWN
