"""
Funding Arbitrage Executor - Custom executor for atomic management of paired positions
This executor ensures hedging safety by managing both long and short positions as a single unit
"""
import asyncio
import logging
from decimal import Decimal
from typing import Any

from hummingbot.client.settings import AllConnectorSettings
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.event.events import MarketOrderFailureEvent, OrderFilledEvent
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

from .data_types import FundingArbitrageExecutorConfig, MissingLeg, PositionSide, ReconciliationState


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

        # Apply leverage settings to perpetual connectors
        self._apply_leverage_settings()

        # Order tracking using TrackedOrder for proper fee tracking
        self._long_order: TrackedOrder | None = None
        self._short_order: TrackedOrder | None = None

        # Close order tracking (separate from entry orders)
        self._long_close_order: TrackedOrder | None = None
        self._short_close_order: TrackedOrder | None = None
        self._close_orders_start_time: float | None = None

        # Position state
        self._long_filled = False
        self._short_filled = False
        self._long_position_amount: Decimal = Decimal(0)
        self._short_position_amount: Decimal = Decimal(0)

        # Timing tracking
        self._entry_start_time: float | None = None
        self._exposure_start_time: float | None = None
        self._warning_logged = False

        # Reconciliation state - tracks which leg needs reconciliation
        self._reconciliation_state = ReconciliationState()

        # Shutdown escalation flags
        self._market_close_attempted = False

        # PnL tracking
        self._funding_payments_received: Decimal = Decimal(0)

    def _apply_leverage_settings(self):
        """Apply leverage settings to perpetual connectors."""
        # Apply leverage to long connector if it's a perpetual
        if self.is_perpetual_connector(self.config.long_connector_name):
            long_connector = self.connectors[self.config.long_connector_name]
            # Perpetual connectors should always have set_leverage method
            # If they don't, it's a bug in the connector implementation
            try:
                long_connector.set_leverage(self.config.long_trading_pair, self.config.leverage)
                self.logger().info(
                    f"Set {self.config.long_connector_name} leverage to {self.config.leverage}x "
                    f"for {self.config.long_trading_pair}",
                )
            except AttributeError:
                self.logger().error(
                    f"Perpetual connector {self.config.long_connector_name} doesn't support set_leverage. "
                    "This is a connector implementation bug.",
                )

        # Apply leverage to short connector if it's a perpetual
        if self.is_perpetual_connector(self.config.short_connector_name):
            short_connector = self.connectors[self.config.short_connector_name]
            # Perpetual connectors should always have set_leverage method
            try:
                short_connector.set_leverage(self.config.short_trading_pair, self.config.leverage)
                self.logger().info(
                    f"Set {self.config.short_connector_name} leverage to {self.config.leverage}x "
                    f"for {self.config.short_trading_pair}",
                )
            except AttributeError:
                self.logger().error(
                    f"Perpetual connector {self.config.short_connector_name} doesn't support set_leverage. "
                    "This is a connector implementation bug.",
                )

    @property
    def is_active(self) -> bool:
        """Executor is active if we have any open positions or pending orders"""
        return (
            self._get_order("long") is not None or
            self._get_order("short") is not None or
            self._is_position_filled("long") or
            self._is_position_filled("short")
        )

    @property
    def is_trading(self) -> bool:
        """Returns true if both positions are filled and hedged"""
        return self._is_position_filled("long") and self._is_position_filled("short")

    @property
    def filled_amount_quote(self) -> Decimal:
        """
        Returns the filled amount in quote currency.
        Required by ExecutorBase for monitoring.
        """
        total = Decimal(0)

        sides: list[PositionSide] = ["long", "short"]
        for side in sides:
            if self._is_position_filled(side):
                amount = self._get_position_amount(side)
                if amount > 0:
                    connector_name, trading_pair, _ = self._get_position_config(side)
                    connector = self.connectors[connector_name]
                    price = connector.get_mid_price(trading_pair)
                    total += amount * price

        return total

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
            if await self._should_place_entry_orders():
                await self.place_entry_orders()

            # Step 2: Check order status
            await self.update_order_status()

            # Step 3: Handle unhedged exposure with progressive reconciliation
            if self._has_unhedged_exposure():
                await self._handle_unhedged_exposure(current_time)

            # Step 4: Handle hedged positions
            elif self._is_position_filled("long") and self._is_position_filled("short"):
                await self._handle_hedged_positions()

            # Step 5: Handle entry timeout
            if await self._check_entry_timeout(current_time):
                await self._handle_entry_timeout()

        except Exception as e:
            self.logger().error(f"Error in control task for {self.config.token}: {e}")
            # Try to hedge if we have unhedged exposure
            if self._has_unhedged_exposure():
                await self.emergency_hedge()

    async def _should_place_entry_orders(self) -> bool:
        """Check if we should place entry orders"""
        return (
            not self._get_order("long") and
            not self._get_order("short") and
            not self._is_position_filled("long") and
            not self._is_position_filled("short")
        )

    async def _handle_unhedged_exposure(self, current_time: float):
        """Handle unhedged exposure with progressive reconciliation"""
        # Identify which leg is missing
        missing_leg: MissingLeg = "short" if self._is_position_filled("long") else "long"

        # Initialize exposure tracking if needed
        if self._exposure_start_time is None:
            self._exposure_start_time = current_time
            self._reconciliation_state = ReconciliationState()
            self._reconciliation_state.missing_leg = missing_leg
            self.logger().info(
                f"Unhedged exposure detected for {self.config.token}: "
                f"{missing_leg} leg missing",
            )

        exposure_duration = current_time - self._exposure_start_time

        # Rate limit reconciliation attempts
        if self._is_reconciliation_rate_limited(current_time):
            return

        # Progressive reconciliation based on exposure duration
        await self._progressive_reconciliation(exposure_duration, missing_leg, current_time)

    def _is_reconciliation_rate_limited(self, current_time: float) -> bool:
        """Check if we should wait before next reconciliation attempt"""
        return bool(
            self._reconciliation_state.last_attempt_time and
            current_time - self._reconciliation_state.last_attempt_time < 2.0,
        )

    async def _progressive_reconciliation(
        self, exposure_duration: float, missing_leg: MissingLeg, current_time: float,
    ):
        """Execute progressive reconciliation strategy"""
        # Stage 1: Aggressive limit order
        if (exposure_duration > self.config.warning_exposure_time and
                self._reconciliation_state.stage == "none"):
            await self._execute_limit_reconciliation(exposure_duration, missing_leg, current_time)

        # Stage 2: Market order
        elif (exposure_duration > self.config.max_unhedged_exposure_time and
                self._reconciliation_state.stage == "limit"):
            await self._execute_market_reconciliation(exposure_duration, missing_leg, current_time)

        # Stage 3: Emergency hedge
        elif (exposure_duration > self.config.max_unhedged_exposure_time * 2 and
                self._reconciliation_state.stage != "emergency"):
            await self._execute_emergency_reconciliation(exposure_duration)

    async def _execute_limit_reconciliation(
        self, exposure_duration: float, missing_leg: MissingLeg, current_time: float,
    ):
        """Execute limit order reconciliation"""
        self.logger().warning(
            f"⚠️ Unhedged for {exposure_duration:.1f}s on {self.config.token}. "
            f"Attempting limit order reconciliation for {missing_leg} leg",
        )
        await self._reconcile_with_limit_order(missing_leg)
        self._reconciliation_state.stage = "limit"
        self._reconciliation_state.attempts += 1
        self._reconciliation_state.last_attempt_time = current_time

    async def _execute_market_reconciliation(
        self, exposure_duration: float, missing_leg: MissingLeg, current_time: float,
    ):
        """Execute market order reconciliation"""
        self.logger().error(
            f"🚨 CRITICAL: Unhedged for {exposure_duration:.1f}s on {self.config.token}. "
            f"Using MARKET ORDER for {missing_leg} leg",
        )
        await self._reconcile_with_market_order(missing_leg)
        self._reconciliation_state.stage = "market"
        self._reconciliation_state.attempts += 1
        self._reconciliation_state.last_attempt_time = current_time

    async def _execute_emergency_reconciliation(self, exposure_duration: float):
        """Execute emergency hedge"""
        self.logger().error(
            f"💀 EMERGENCY: Failed to hedge after {exposure_duration:.1f}s. "
            f"Closing the filled position for {self.config.token}",
        )
        await self.emergency_hedge()
        self._reconciliation_state.stage = "emergency"

    async def _handle_hedged_positions(self):
        """Handle fully hedged positions"""
        if self._exposure_start_time:
            self.logger().info(f"✅ Positions hedged for {self.config.token}")
            # Reset ALL tracking when positions become hedged
            self._exposure_start_time = None
            self._warning_logged = False
            self._reconciliation_state = ReconciliationState()

        # Check exit conditions
        if await self.should_exit_position():
            await self.close_all_positions(close_type=CloseType.TAKE_PROFIT)
            self._status = RunnableStatus.SHUTTING_DOWN

    async def _check_entry_timeout(self, current_time: float) -> bool:
        """Check if entry timeout has been reached"""
        return bool(
            self._entry_start_time and not self.is_trading and
            current_time - self._entry_start_time > self.config.entry_timeout,
        )

    async def _handle_entry_timeout(self):
        """Handle entry timeout condition"""
        self.logger().warning(
            f"Entry timeout reached for {self.config.token}, cancelling unfilled orders",
        )
        await self.cancel_unfilled_orders()
        if self._is_position_filled("long") or self._is_position_filled("short"):
            # We have partial fill, need to close
            await self.close_all_positions(close_type=CloseType.TIME_LIMIT)
        self._status = RunnableStatus.SHUTTING_DOWN

    async def control_shutdown_process(self):
        """Handle the shutdown process"""
        current_time = self._strategy.current_timestamp

        # Close positions if needed
        if self.close_type != CloseType.POSITION_HOLD:
            # Check if we actually have positions to close
            has_positions = (self._is_position_filled("long") and self._get_position_amount("long") > 0) or \
                            (self._is_position_filled("short") and self._get_position_amount("short") > 0)

            if has_positions:
                # Start close orders if not already started
                if self._close_orders_start_time is None:
                    # Pass the close type to determine order type
                    await self.close_all_positions(close_type=self.close_type)
                    self._close_orders_start_time = current_time
                    return  # Wait for next control cycle to check status

                # Check if close orders are complete
                long_closed = not self._is_position_filled("long") or self._get_position_amount("long") == 0
                short_closed = not self._is_position_filled("short") or self._get_position_amount("short") == 0

                if long_closed and short_closed:
                    # Both positions closed successfully
                    self.logger().info(f"All positions closed for {self.config.token}")
                    await self.cancel_unfilled_orders()
                    self._status = RunnableStatus.TERMINATED
                    return

                # Check for timeout and escalate if needed
                elapsed_time = current_time - self._close_orders_start_time

                # Use config-defined timeouts for progressive escalation
                warning_timeout = self.config.warning_exposure_time
                escalation_timeout = self.config.max_unhedged_exposure_time
                final_timeout = escalation_timeout * 2  # Double the max for final give-up

                if elapsed_time > warning_timeout and elapsed_time < escalation_timeout:
                    # First escalation - try market orders
                    if not self._market_close_attempted:
                        self._market_close_attempted = True

                        should_escalate = False

                        if (self._long_close_order and not long_closed and
                                not self.is_order_filled(self._long_close_order)):
                            self.logger().warning(
                                f"Long close order not filled after {warning_timeout}s, "
                                "escalating to MARKET",
                            )
                            should_escalate = True

                        if (self._short_close_order and not short_closed and
                                not self.is_order_filled(self._short_close_order)):
                            self.logger().warning(
                                f"Short close order not filled after {warning_timeout}s, "
                                "escalating to MARKET",
                            )
                            should_escalate = True

                        if should_escalate and self.config.emergency_use_market_orders:
                            # Cancel pending limit orders and place market orders
                            await self.cancel_close_orders()
                            # Use emergency market orders
                            await self.close_all_positions(close_type=CloseType.STOP_LOSS)

                elif elapsed_time > final_timeout:
                    # Final give-up after double the max exposure time
                    self.logger().error(
                        f"Failed to close positions after {elapsed_time:.1f}s for {self.config.token}. "
                        f"Marking as terminated. Manual intervention may be required.",
                    )
                    # Cancel any pending orders and terminate
                    await self.cancel_close_orders()
                    await self.cancel_unfilled_orders()
                    self._status = RunnableStatus.TERMINATED
            else:
                # No positions to close, just cancel unfilled entry orders and terminate
                self.logger().info(f"No positions to close for {self.config.token}")
                await self.cancel_unfilled_orders()
                self._status = RunnableStatus.TERMINATED
        else:
            # Position hold - just cancel unfilled entry orders
            await self.cancel_unfilled_orders()
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
            # Get order book data
            bid, ask, spread, spread_pct = self._get_order_book_data(connector, trading_pair)

            # Get market precision
            mid_price = (bid + ask) / Decimal(2)
            tick_size = connector.get_order_price_quantum(trading_pair, mid_price)
            spread_in_ticks = spread / tick_size if tick_size > 0 else Decimal(1000)

            self.logger().debug(
                f"Market precision for {trading_pair}: tick_size={tick_size}, "
                f"spread={spread:.6f} ({spread_in_ticks:.1f} ticks)",
            )

            # Calculate optimal price based on spread
            price = self._calculate_price_by_spread(
                bid, ask, spread, spread_in_ticks, tick_size, is_buy, is_entry,
            )

            # Apply exchange quantization and safety checks
            price = connector.quantize_order_price(trading_pair, price)
            price = self._apply_price_safety_checks(price, bid, ask, tick_size, is_buy)

            self.logger().info(
                f"Calculated {'entry' if is_entry else 'exit'} {'buy' if is_buy else 'sell'} price for {trading_pair}: "
                f"{price:.4f} (bid={bid:.4f}, ask={ask:.4f}, spread={spread_pct:.3f}%)",
            )
            return price

        except Exception as e:
            self.logger().error(f"Failed to get order book for {trading_pair}: {e}")
            raise ValueError(f"Cannot calculate maker price without order book data for {trading_pair}") from e

    def _get_order_book_data(self, connector, trading_pair: str) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        """Get order book data: bid, ask, spread, spread_pct"""
        order_book = connector.get_order_book(trading_pair)
        bid = Decimal(str(order_book.get_price(False)))  # Best bid
        ask = Decimal(str(order_book.get_price(True)))   # Best ask
        spread = ask - bid
        spread_pct = spread / bid * Decimal(100)

        self.logger().debug(
            f"Order book for {trading_pair}: bid={bid}, ask={ask}, spread={spread:.4f} ({spread_pct:.3f}%)",
        )
        return bid, ask, spread, spread_pct

    def _calculate_price_by_spread(
        self, bid: Decimal, ask: Decimal, spread: Decimal, spread_in_ticks: Decimal,
        tick_size: Decimal, is_buy: bool, is_entry: bool,
    ) -> Decimal:
        """Calculate price based on spread analysis"""
        if spread_in_ticks <= Decimal(2):
            # Ultra-tight spread (1-2 ticks): must join the book
            return bid if is_buy else ask
        elif spread_in_ticks <= Decimal(5):
            # Tight spread (3-5 ticks): place conservatively
            return self._calculate_tight_spread_price(bid, ask, tick_size, is_buy, is_entry)
        else:
            # Normal/wide spread (>5 ticks): use smart proportional placement
            return self._calculate_wide_spread_price(bid, ask, spread, spread_in_ticks, is_buy, is_entry)

    def _calculate_tight_spread_price(
        self, bid: Decimal, ask: Decimal, tick_size: Decimal, is_buy: bool, is_entry: bool,
    ) -> Decimal:
        """Calculate price for tight spreads (3-5 ticks)"""
        if is_entry:
            # Entry: one tick into spread for better fill probability
            return bid + tick_size if is_buy else ask - tick_size
        else:
            # Exit: join the book for maker fee
            return bid if is_buy else ask

    def _calculate_wide_spread_price(
        self, bid: Decimal, ask: Decimal, spread: Decimal, spread_in_ticks: Decimal,
        is_buy: bool, is_entry: bool,
    ) -> Decimal:
        """Calculate price for wide spreads (>5 ticks)"""
        if is_entry:
            # Entry orders: more aggressive (20-40% into spread)
            base_aggression = Decimal("0.3")
            aggressiveness = min(Decimal("0.4"), base_aggression * (spread_in_ticks / Decimal(10)))
        else:
            # Exit orders: conservative (10-20% into spread)
            base_aggression = Decimal("0.15")
            aggressiveness = min(Decimal("0.2"), base_aggression * (spread_in_ticks / Decimal(20)))

        return bid + (spread * aggressiveness) if is_buy else ask - (spread * aggressiveness)

    def _apply_price_safety_checks(
        self, price: Decimal, bid: Decimal, ask: Decimal, tick_size: Decimal, is_buy: bool,
    ) -> Decimal:
        """Apply final safety checks to ensure price doesn't cross spread"""
        if is_buy:
            # Buy orders must not cross to ask
            price = min(price, ask - tick_size)
            # But also not go below bid (would be worse price)
            price = max(price, bid)
        else:
            # Sell orders must not cross to bid
            price = max(price, bid + tick_size)
            # But also not go above ask (would be worse price)
            price = min(price, ask)
        return price

    async def place_entry_orders(self):
        """Place both entry orders atomically"""
        try:
            # Get connectors
            long_connector = self.connectors[self.config.long_connector_name]
            short_connector = self.connectors[self.config.short_connector_name]

            # Calculate order amounts and prices
            long_amount, short_amount = self._calculate_entry_amounts(long_connector, short_connector)
            long_price, short_price = await self._calculate_entry_prices(
                long_connector, short_connector, long_amount, short_amount,
            )

            self.logger().info(
                f"Placing entry orders for {self.config.token}: "
                f"Long {long_amount:.6f} @ {long_price:.4f}, "
                f"Short {short_amount:.6f} @ {short_price:.4f}",
            )

            # Place the orders
            await self._place_long_entry(long_amount, long_price)
            await self._place_short_entry(short_amount, short_price)

        except Exception as e:
            self.logger().error(f"Failed to place entry orders for {self.config.token}: {e}")
            raise

    def _calculate_entry_amounts(self, long_connector, short_connector) -> tuple[Decimal, Decimal]:
        """Calculate entry order amounts based on position size"""
        long_mid_price = long_connector.get_mid_price(self.config.long_trading_pair)
        short_mid_price = short_connector.get_mid_price(self.config.short_trading_pair)

        long_amount = self.config.position_size_quote / long_mid_price
        short_amount = self.config.position_size_quote / short_mid_price

        return long_amount, short_amount

    async def _calculate_entry_prices(
        self, long_connector, short_connector, long_amount: Decimal, short_amount: Decimal,
    ) -> tuple[Decimal, Decimal]:
        """Calculate entry order prices based on order type"""
        if self.config.open_order_type == OrderType.LIMIT:
            # Use smart maker pricing for LIMIT orders
            return self._calculate_limit_entry_prices(long_connector, short_connector)
        else:
            # For MARKET orders, use the fill price
            return self._calculate_market_entry_prices(
                long_connector, short_connector, long_amount, short_amount,
            )

    def _calculate_limit_entry_prices(self, long_connector, short_connector) -> tuple[Decimal, Decimal]:
        """Calculate limit order prices using smart maker pricing"""
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
        return long_price, short_price

    def _calculate_market_entry_prices(
        self, long_connector, short_connector, long_amount: Decimal, short_amount: Decimal,
    ) -> tuple[Decimal, Decimal]:
        """Calculate market order fill prices"""
        long_price = long_connector.get_price_for_volume(
            self.config.long_trading_pair, True, long_amount,
        ).result_price
        short_price = short_connector.get_price_for_volume(
            self.config.short_trading_pair, False, short_amount,
        ).result_price
        return long_price, short_price

    def _get_position_config(self, side: PositionSide) -> tuple[str, str, TradeType]:
        """Get configuration for a position side.

        Returns:
            Tuple of (connector_name, trading_pair, trade_type)
        """
        if side == "long":
            return (
                self.config.long_connector_name,
                self.config.long_trading_pair,
                TradeType.BUY,
            )
        else:
            return (
                self.config.short_connector_name,
                self.config.short_trading_pair,
                TradeType.SELL,
            )

    async def _place_entry_order(self, side: PositionSide, amount: Decimal, price: Decimal | None = None):
        """Place entry order for specified side.

        Args:
            side: "long" or "short" position side
            amount: Order amount
            price: Order price (for limit orders)
        """
        connector_name, trading_pair, trade_type = self._get_position_config(side)

        # Create tracked order
        tracked_order = TrackedOrder()

        # Place order based on order type
        if self.config.open_order_type == OrderType.LIMIT:
            tracked_order.order_id = self.place_order(
                connector_name=connector_name,
                trading_pair=trading_pair,
                order_type=OrderType.LIMIT,
                side=trade_type,
                amount=amount,
                position_action=PositionAction.OPEN,
                price=price,
            )
        else:
            tracked_order.order_id = self.place_order(
                connector_name=connector_name,
                trading_pair=trading_pair,
                order_type=OrderType.MARKET,
                side=trade_type,
                amount=amount,
                position_action=PositionAction.OPEN,
            )

        # Store tracked order
        if side == "long":
            self._long_order = tracked_order
        else:
            self._short_order = tracked_order

    async def _place_long_entry(self, amount: Decimal, price: Decimal | None = None):
        """Place long entry order (wrapper for compatibility)"""
        await self._place_entry_order("long", amount, price)

    async def _place_short_entry(self, amount: Decimal, price: Decimal | None = None):
        """Place short entry order (wrapper for compatibility)"""
        await self._place_entry_order("short", amount, price)

    async def _update_position_status(self, side: PositionSide) -> None:
        """Update the status of a position's order.

        Args:
            side: Position side to update ("long" or "short")
        """
        order_tracker = self._get_order(side, is_close=False)
        if not order_tracker or self._is_position_filled(side):
            return

        connector_name, _, _ = self._get_position_config(side)
        order = self.get_in_flight_order(connector_name, order_tracker.order_id)

        if order and order.is_filled:
            self._set_position_filled(side, True)
            self._set_position_amount(side, order.executed_amount_base)
            order_tracker.order = order  # Store the InFlightOrder in TrackedOrder

            amount = self._get_position_amount(side)
            self.logger().info(f"{side.capitalize()} position filled for {self.config.token}: {amount}")

    async def update_order_status(self):
        """Check and update the status of our orders"""
        await self._update_position_status("long")
        await self._update_position_status("short")

    def _has_unhedged_exposure(self) -> bool:
        """Check if we have unhedged exposure (one position filled but not the other)"""
        return self._is_position_filled("long") != self._is_position_filled("short")

    async def _reconcile_with_limit_order(self, missing_leg: MissingLeg):
        """
        Reconcile missing leg with an aggressive limit order.

        Args:
            missing_leg: "long" or "short" - which leg needs to be filled
        """
        try:
            if missing_leg == "short":
                # Long filled, need to fill short
                self.logger().info(f"Reconciling: Placing aggressive SHORT limit order for {self.config.token}")

                # Cancel existing short order
                if self._short_order and self._short_order.order_id:
                    await self._cancel_order(
                        self.config.short_connector_name,
                        self.config.short_trading_pair,
                        self._short_order.order_id,
                    )

                connector = self.connectors[self.config.short_connector_name]
                reconcile_amount = self._long_position_amount

                if reconcile_amount <= 0:
                    self.logger().error(f"Cannot reconcile: Long position amount is {reconcile_amount}")
                    return

                # Use smart pricing but more aggressive for reconciliation
                aggressive_price = self.calculate_maker_price(
                    connector,
                    self.config.short_trading_pair,
                    is_buy=False,
                    is_entry=True,  # Use entry pricing (more aggressive)
                )
                # Make it even more aggressive by 0.1%
                aggressive_price = aggressive_price * Decimal("0.999")

                # Initialize TrackedOrder if needed
                if not self._short_order:
                    self._short_order = TrackedOrder()

                self._short_order.order_id = self.place_order(
                    connector_name=self.config.short_connector_name,
                    trading_pair=self.config.short_trading_pair,
                    order_type=OrderType.LIMIT,
                    side=TradeType.SELL,
                    amount=reconcile_amount,
                    price=aggressive_price,
                    position_action=PositionAction.OPEN,
                )

            elif missing_leg == "long":
                # Short filled, need to fill long
                self.logger().info(f"Reconciling: Placing aggressive LONG limit order for {self.config.token}")

                # Cancel existing long order
                if self._long_order and self._long_order.order_id:
                    await self._cancel_order(
                        self.config.long_connector_name,
                        self.config.long_trading_pair,
                        self._long_order.order_id,
                    )

                connector = self.connectors[self.config.long_connector_name]
                reconcile_amount = self._short_position_amount

                if reconcile_amount <= 0:
                    self.logger().error(f"Cannot reconcile: Short position amount is {reconcile_amount}")
                    return

                # Use smart pricing but more aggressive for reconciliation
                aggressive_price = self.calculate_maker_price(
                    connector,
                    self.config.long_trading_pair,
                    is_buy=True,
                    is_entry=True,  # Use entry pricing (more aggressive)
                )
                # Make it even more aggressive by 0.1%
                aggressive_price = aggressive_price * Decimal("1.001")

                # Initialize TrackedOrder if needed
                if not self._long_order:
                    self._long_order = TrackedOrder()

                self._long_order.order_id = self.place_order(
                    connector_name=self.config.long_connector_name,
                    trading_pair=self.config.long_trading_pair,
                    order_type=OrderType.LIMIT,
                    side=TradeType.BUY,
                    amount=reconcile_amount,
                    price=aggressive_price,
                    position_action=PositionAction.OPEN,
                )

        except Exception as e:
            self.logger().error(f"Failed to reconcile with limit order for {self.config.token}: {e}")

    async def _reconcile_with_market_order(self, missing_leg: MissingLeg):
        """
        Force reconciliation with a market order.

        Args:
            missing_leg: "long" or "short" - which leg needs to be filled
        """
        try:
            if missing_leg == "short":
                # Long filled, need to fill short with market order
                self.logger().warning(f"MARKET ORDER: Forcing SHORT position for {self.config.token}")

                # Cancel existing short order if any
                if self._short_order and self._short_order.order_id:
                    await self._cancel_order(
                        self.config.short_connector_name,
                        self.config.short_trading_pair,
                        self._short_order.order_id,
                    )

                reconcile_amount = self._long_position_amount
                if reconcile_amount <= 0:
                    self.logger().error(f"Cannot reconcile: Long position amount is {reconcile_amount}")
                    return

                # Initialize TrackedOrder if needed
                if not self._short_order:
                    self._short_order = TrackedOrder()

                # Place market order
                self._short_order.order_id = self.place_order(
                    connector_name=self.config.short_connector_name,
                    trading_pair=self.config.short_trading_pair,
                    order_type=OrderType.MARKET,
                    side=TradeType.SELL,
                    amount=reconcile_amount,
                    position_action=PositionAction.OPEN,
                )

            elif missing_leg == "long":
                # Short filled, need to fill long with market order
                self.logger().warning(f"MARKET ORDER: Forcing LONG position for {self.config.token}")

                # Cancel existing long order if any
                if self._long_order and self._long_order.order_id:
                    await self._cancel_order(
                        self.config.long_connector_name,
                        self.config.long_trading_pair,
                        self._long_order.order_id,
                    )

                reconcile_amount = self._short_position_amount
                if reconcile_amount <= 0:
                    self.logger().error(f"Cannot reconcile: Short position amount is {reconcile_amount}")
                    return

                # Initialize TrackedOrder if needed
                if not self._long_order:
                    self._long_order = TrackedOrder()

                # Place market order
                self._long_order.order_id = self.place_order(
                    connector_name=self.config.long_connector_name,
                    trading_pair=self.config.long_trading_pair,
                    order_type=OrderType.MARKET,
                    side=TradeType.BUY,
                    amount=reconcile_amount,
                    position_action=PositionAction.OPEN,
                )

        except Exception as e:
            self.logger().error(f"Failed to reconcile with MARKET order for {self.config.token}: {e}")

    async def emergency_hedge(self):
        """Emergency procedure to close ONLY the unhedged position (not all positions)"""
        self.logger().error(f"🚨 Executing emergency close of unhedged position for {self.config.token}")

        try:
            if self._is_position_filled("long") and not self._is_position_filled("short"):
                # Close long position immediately
                self.logger().error(f"Closing unhedged LONG position for {self.config.token}")
                await self._close_long_position(use_market=True)
                # Cancel pending short order
                await self._cancel_position_order("short", is_close=False)

            elif self._is_position_filled("short") and not self._is_position_filled("long"):
                # Close short position immediately
                self.logger().error(f"Closing unhedged SHORT position for {self.config.token}")
                await self._close_short_position(use_market=True)
                # Cancel pending long order
                await self._cancel_position_order("long", is_close=False)

            # Mark executor for shutdown after emergency hedge
            self.close_type = CloseType.STOP_LOSS
            self._status = RunnableStatus.SHUTTING_DOWN

        except Exception as e:
            self.logger().error(f"Emergency hedge failed for {self.config.token}: {e}")
            # Still mark for shutdown even if emergency hedge fails
            self.close_type = CloseType.FAILED
            self._status = RunnableStatus.SHUTTING_DOWN

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
        emergency_close_types = [CloseType.FAILED, CloseType.INSUFFICIENT_BALANCE, CloseType.STOP_LOSS]

        if close_type in emergency_close_types:
            # Emergency scenarios always use market orders for quick exit
            use_market = True
        else:
            # For all other stops including EARLY_STOP (user stop command), use take_profit_order_type
            # (which should ideally be set from the controller's close_order_type)
            use_market = self.config.triple_barrier_config.take_profit_order_type == OrderType.MARKET

        # Only close positions that actually exist (based on event-driven state)
        tasks = []
        if self._is_position_filled("long") and self._get_position_amount("long") > 0:
            tasks.append(self._close_long_position(use_market))

        if self._is_position_filled("short") and self._get_position_amount("short") > 0:
            tasks.append(self._close_short_position(use_market))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            self.logger().info(f"No positions to close for {self.config.token}")

        # For non-emergency stops, the control_shutdown_process will handle monitoring
        # and escalation. For emergency stops, we've already used market orders.

    # ============== Position State Management Helpers ==============

    def _get_position_amount(self, side: PositionSide) -> Decimal:
        """Get the current position amount for a side."""
        return self._long_position_amount if side == "long" else self._short_position_amount

    def _set_position_amount(self, side: PositionSide, amount: Decimal) -> None:
        """Set the position amount for a side."""
        if side == "long":
            self._long_position_amount = amount
        else:
            self._short_position_amount = amount

    def _is_position_filled(self, side: PositionSide) -> bool:
        """Check if a position side is filled."""
        return self._long_filled if side == "long" else self._short_filled

    def _set_position_filled(self, side: PositionSide, filled: bool) -> None:
        """Set the filled status for a position side."""
        if side == "long":
            self._long_filled = filled
        else:
            self._short_filled = filled

    def _get_close_trade_type(self, side: PositionSide) -> TradeType:
        """Get the trade type for closing a position."""
        # Closing long = SELL, Closing short = BUY
        return TradeType.SELL if side == "long" else TradeType.BUY

    def _get_order(self, side: PositionSide, is_close: bool = False) -> TrackedOrder | None:
        """Get the tracked order for a side.

        Args:
            side: Position side ("long" or "short")
            is_close: Whether to get close order (True) or entry order (False)
        """
        if is_close:
            return self._long_close_order if side == "long" else self._short_close_order
        else:
            return self._long_order if side == "long" else self._short_order

    def _set_order(self, side: PositionSide, order: TrackedOrder | None, is_close: bool = False) -> None:
        """Set the tracked order for a side.

        Args:
            side: Position side ("long" or "short")
            order: The TrackedOrder to set (or None to clear)
            is_close: Whether to set close order (True) or entry order (False)
        """
        if is_close:
            if side == "long":
                self._long_close_order = order
            else:
                self._short_close_order = order
        else:
            if side == "long":
                self._long_order = order
            else:
                self._short_order = order

    async def _close_position(self, side: PositionSide, use_market: bool = False):
        """Close a position for the specified side.

        Args:
            side: "long" or "short" position to close
            use_market: Whether to use market order (True) or limit order (False)
        """
        position_amount = self._get_position_amount(side)
        if position_amount <= 0:
            return

        connector_name, trading_pair, _ = self._get_position_config(side)
        close_trade_type = self._get_close_trade_type(side)

        self.logger().info(f"Closing {side} position for {self.config.token}: {position_amount}")

        # Determine order type and price
        if use_market:
            order_type = OrderType.MARKET
            price = None
        else:
            order_type = OrderType.LIMIT
            connector = self.connectors[connector_name]
            # For closing: long->sell (is_buy=False), short->buy (is_buy=True)
            is_buy = close_trade_type == TradeType.BUY
            price = self.calculate_maker_price(connector, trading_pair, is_buy=is_buy)

        # Place the close order
        order_id = self.place_order(
            connector_name=connector_name,
            trading_pair=trading_pair,
            order_type=order_type,
            side=close_trade_type,
            amount=position_amount,
            position_action=PositionAction.CLOSE,
            price=price,
        )

        # Track the close order
        tracked_order = TrackedOrder(order_id)
        if side == "long":
            self._long_close_order = tracked_order
        else:
            self._short_close_order = tracked_order

    async def _close_long_position(self, use_market: bool = False):
        """Close the long position (wrapper for compatibility)"""
        await self._close_position("long", use_market)

    async def _close_short_position(self, use_market: bool = False):
        """Close the short position (wrapper for compatibility)"""
        await self._close_position("short", use_market)

    async def _cancel_position_order(self, side: PositionSide, is_close: bool = False) -> None:
        """Cancel an order for a specific position side.

        Args:
            side: Position side ("long" or "short")
            is_close: Whether to cancel close order (True) or entry order (False)
        """
        order = self._get_order(side, is_close)
        if not order or not order.order_id:
            return

        # Skip cancellation for filled entry orders
        if not is_close and self._is_position_filled(side):
            return

        connector_name, trading_pair, _ = self._get_position_config(side)
        await self._cancel_order(connector_name, trading_pair, order.order_id)
        self._set_order(side, None, is_close)

    async def cancel_unfilled_orders(self):
        """Cancel any unfilled entry orders (not close orders)"""
        await self._cancel_position_order("long", is_close=False)
        await self._cancel_position_order("short", is_close=False)

    async def cancel_close_orders(self):
        """Cancel any unfilled close orders"""
        await self._cancel_position_order("long", is_close=True)
        await self._cancel_position_order("short", is_close=True)

    def is_order_filled(self, tracked_order: TrackedOrder) -> bool:
        """Check if an order is filled"""
        if not tracked_order or not tracked_order.order_id:
            return False

        # Try to get the in-flight order from both connectors using proper encapsulation
        for connector_name in [self.config.long_connector_name, self.config.short_connector_name]:
            in_flight_order = None
            try:
                in_flight_order = self.get_in_flight_order(connector_name, tracked_order.order_id)
            except Exception as e:
                # Order might not be from this connector
                self.logger().debug(f"Order {tracked_order.order_id} not found in {connector_name}: {e}")

            if in_flight_order:
                return in_flight_order.is_filled

        # If not found in in-flight orders, check if it was filled and completed
        # TrackedOrder.order should always be an InFlightOrder if set
        if tracked_order.order:
            return tracked_order.order.is_filled
        return False

    async def _cancel_order(self, connector_name: str, trading_pair: str, order_id: str):
        """Cancel a specific order"""
        try:
            self._strategy.cancel(connector_name, trading_pair, order_id)
        except Exception as e:
            self.logger().warning(f"Failed to cancel order {order_id}: {e}")

    def _get_position_pnl(self, connector_name: str, trading_pair: str) -> Decimal:
        """Get PnL for a specific position"""
        connector = self.connectors.get(connector_name)
        if not connector:
            return Decimal(0)

        # Only perpetual connectors have unrealized PnL
        if self.is_perpetual_connector(connector_name):
            try:
                pnl = connector.get_unrealized_pnl(trading_pair)
                if pnl is None:
                    raise ValueError(f"Unable to get unrealized PnL for {trading_pair} on {connector_name}")
                return pnl
            except AttributeError as e:
                self.logger().error(f"Connector {connector_name} doesn't support get_unrealized_pnl")
                raise ValueError(
                    f"Perpetual connector {connector_name} missing required get_unrealized_pnl method",
                ) from e

        # For spot connectors, PnL is not applicable
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
        # Only perpetual connectors have funding rates
        connector_name = next((name for name, conn in self.connectors.items() if conn == connector), None)
        if not connector_name or not self.is_perpetual_connector(connector_name):
            return Decimal(0)

        try:
            funding_info = connector.get_funding_info(trading_pair)
            if funding_info:
                # FundingInfo should always have a rate attribute
                return Decimal(str(funding_info.rate))
        except (AttributeError, TypeError) as e:
            self.logger().warning(f"Failed to get funding rate for {trading_pair}: {e}")
        except Exception as e:
            self.logger().error(f"Unexpected error getting funding rate for {trading_pair}: {e}")
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
        fee_open_long = self._get_fee_from_connector(
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

        fee_open_short = self._get_fee_from_connector(
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
        fee_close_long = self._get_fee_from_connector(
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

        fee_close_short = self._get_fee_from_connector(
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

    def _get_fee_from_connector(
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
        """Get fee from connector or configured fee schema. Fails fast if unavailable."""
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
            self.logger().debug(f"Failed to get fee from connector: {e}, checking configuration")

            # Get from configured fee schema (not a fallback - this is actual config)
            connector_settings = AllConnectorSettings.get_connector_settings()
            if connector_name not in connector_settings:
                raise ValueError(f"No fee configuration found for {connector_name}") from e

            fee_schema = connector_settings[connector_name].trade_fee_schema
            if not fee_schema:
                raise ValueError(f"No fee schema configured for {connector_name}") from e

            fee_percent = (
                fee_schema.maker_percent_fee_decimal
                if is_maker
                else fee_schema.taker_percent_fee_decimal
            )

            if fee_percent is None:
                raise ValueError(
                    f"Fee percent not configured for {connector_name} ({'maker' if is_maker else 'taker'})",
                ) from e

            return fee_percent

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
        """
        Convert executor state to dictionary for persistence.
        Required by V2 architecture for state management.
        """
        return {
            "config": self.config.model_dump(),
            "long_filled": self._long_filled,
            "short_filled": self._short_filled,
            "long_position_amount": str(self._long_position_amount),
            "short_position_amount": str(self._short_position_amount),
            "funding_payments_received": str(self._funding_payments_received),
            "net_pnl_quote": str(self.get_net_pnl_quote()),
            "status": self._status.name,
            "close_type": self.close_type.name if self.close_type else None,
        }

    def get_custom_info(self) -> dict[str, Any]:
        """
        Returns custom info specific to funding arbitrage executor.
        Used by ExecutorBase.executor_info property.
        """
        return {
            "token": self.config.token,
            "long_exchange": self.config.long_connector_name,
            "short_exchange": self.config.short_connector_name,
            "long_filled": self._is_position_filled("long"),
            "short_filled": self._is_position_filled("short"),
            "long_amount": float(self._get_position_amount("long")),
            "short_amount": float(self._get_position_amount("short")),
            "position_hedged": self._is_position_filled("long") and self._is_position_filled("short"),
            "funding_rate_diff": float(self._get_current_funding_diff()),
            "leverage": self.config.leverage,
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

    def process_order_filled_event(self, _, market: ConnectorBase, event: OrderFilledEvent):
        """
        Handle order filled events to properly track position changes.
        Follows V2 architecture event handling pattern.

        Args:
            _: Event tag (unused but required by base class)
            market: The connector where the event occurred
            event: The order filled event
        """
        order_id = event.order_id

        # Validate the event is from one of our connectors
        if market not in [self.connectors[self.config.long_connector_name],
                          self.connectors[self.config.short_connector_name]]:
            return  # Not our event

        # Track entry order fills
        if self._long_order and self._long_order.order_id == order_id:
            self._long_filled = True
            self._long_position_amount = event.amount
            self.logger().info(
                f"Long position opened on {self.config.long_connector_name} for {self.config.token}: {event.amount}",
            )

        elif self._short_order and self._short_order.order_id == order_id:
            self._short_filled = True
            self._short_position_amount = event.amount
            self.logger().info(
                f"Short position opened on {self.config.short_connector_name} for {self.config.token}: {event.amount}",
            )

        # Track close order fills
        elif self._long_close_order and self._long_close_order.order_id == order_id:
            # Long position closed
            self._long_filled = False
            self._long_position_amount = Decimal(0)
            self.logger().info(f"Long position closed on {self.config.long_connector_name} for {self.config.token}")
            self._long_close_order = None

        elif self._short_close_order and self._short_close_order.order_id == order_id:
            # Short position closed
            self._short_filled = False
            self._short_position_amount = Decimal(0)
            self.logger().info(f"Short position closed on {self.config.short_connector_name} for {self.config.token}")
            self._short_close_order = None

        # TrackedOrder updates are handled by the base class event forwarders

    def process_order_failed_event(self, _, market: ConnectorBase, event: MarketOrderFailureEvent):
        """
        Handle order failure events to properly track position state.
        Follows V2 architecture event-driven pattern.

        Args:
            _: Event tag (unused but required by base class)
            market: The connector where the event occurred
            event: The order failure event
        """
        order_id = event.order_id
        # MarketOrderFailureEvent should always have these attributes, but be defensive
        error_message = str(getattr(event, "error_message", "Unknown error"))

        # Check if it's a ReduceOnly error (Binance specific)
        if "ReduceOnly" in error_message or "reduceOnly" in error_message:
            # This means there's no position to reduce
            # Update our state to reflect reality

            if self._long_close_order and self._long_close_order.order_id == order_id:
                # Long position doesn't exist on exchange
                self.logger().warning(
                    f"ReduceOnly error for long close order on {self.config.long_connector_name}. "
                    "No position exists, updating local state.",
                )
                self._long_filled = False
                self._long_position_amount = Decimal(0)
                self._long_close_order = None

            elif self._short_close_order and self._short_close_order.order_id == order_id:
                # Short position doesn't exist on exchange
                self.logger().warning(
                    f"ReduceOnly error for short close order on {self.config.short_connector_name}. "
                    "No position exists, updating local state.",
                )
                self._short_filled = False
                self._short_position_amount = Decimal(0)
                self._short_close_order = None
        else:
            # Other types of failures - log but don't change position state
            self.logger().error(
                f"Order {order_id} failed on {market.name}: {error_message}",
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

    def on_stop(self):
        """
        Called when the executor is stopped.
        Logs final state information.
        Required by V2 architecture.
        """
        # Log final state with error handling
        try:
            pnl_quote = self.get_net_pnl_quote()
            pnl_pct = self.get_net_pnl_pct()
            self.logger().info(
                f"FundingArbitrageExecutor stopped for {self.config.token}. "
                f"Final PnL: {pnl_quote:.4f} ({pnl_pct:.2f}%) | "
                f"Close type: {self.close_type.name if self.close_type else 'NONE'}",
            )
        except Exception as e:
            # Handle case where PnL calculation fails (e.g., no positions were opened)
            self.logger().info(
                f"FundingArbitrageExecutor stopped for {self.config.token}. "
                f"Close type: {self.close_type.name if self.close_type else 'NONE'} | "
                f"PnL calculation error: {e!s}",
            )
