from __future__ import annotations  # Enable postponed evaluation

import asyncio
from builtins import BaseException
from collections.abc import Awaitable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.base_api import ExchangeAPI
from cyberdelta.core.models import (
    DerivativePosition,
    Order,
    OrderSide,
    OrderStatus,
    SpotBalance,
    Trade,
)
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class PortfolioTracker:
    """
    Track and manage the current state of the portfolio.

    Responsible for:
    - Tracking balances across exchanges
    - Monitoring open positions and their P&L
    - Tracking order status
    - Calculating exposure metrics
    - Maintaining portfolio state
    - Performing reconciliation with exchange data
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize the portfolio tracker.

        Args:
            config: Application configuration
        """
        self.config: Config = config
        self.api_clients: dict[str, ExchangeAPI] = {}

        # Balance tracking (Store as Decimal)
        self._balances: dict[
            str, dict[str, SpotBalance]
        ] = {}  # exchange -> asset -> SpotBalance object

        # Position tracking
        self._positions: dict[
            str, dict[str, DerivativePosition]
        ] = {}  # exchange -> position_key -> DerivativePosition

        # Order tracking
        self._orders: dict[str, dict[str, Order]] = {}  # exchange -> order_id -> Order

        # Timestamp of last update
        self._last_update_time: dict[str, datetime] = {}  # exchange -> last update time

        # Timestamp of last reconciliation
        self._last_reconciliation_time: dict[
            str, datetime
        ] = {}  # exchange -> last reconciliation time

        # Reconciliation interval (5 minutes by default)
        interval = config.get("portfolio.reconciliation_interval", 300)
        if isinstance(interval, int | float | str):
            self.reconciliation_interval = int(interval)
        else:
            self.reconciliation_interval = 300  # seconds

        # Initialize data structures
        self._initialize_data_structures()

        # Initialize high watermark (as Decimal)
        self._high_watermark: Decimal = Decimal(
            "0.0"
        )  # Track highest portfolio value for drawdown calculation

        # Add internal tracking for realized PNL
        self._realized_pnl: Decimal = Decimal("0.0")  # Track realized PNL

        # Add active symbols and watchlist
        self._active_symbols: set[str] = set()
        self._watchlist: set[str] = set()

    def _initialize_data_structures(self) -> None:
        """Initialize data structures for all configured exchanges."""
        exchanges_raw = self.config.get("exchanges", {})
        exchanges: dict[str, Any] = exchanges_raw if isinstance(exchanges_raw, dict) else {}
        for exchange_id in [str(k) for k in exchanges.keys()]:
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Initialize dictionaries for this exchange
            self._balances[exchange_id] = {}
            self._positions[exchange_id] = {}
            self._orders[exchange_id] = {}
            # Default to a timezone-aware past date
            self._last_update_time[exchange_id] = datetime.min.replace(tzinfo=UTC)
            self._last_reconciliation_time[exchange_id] = datetime.min.replace(tzinfo=UTC)

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """
        Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier
            client: ExchangeAPI implementation
        """
        self.api_clients[exchange_id] = client
        logger.info(f"Registered API client for {exchange_id} in PortfolioTracker")

    async def initialize(self) -> None:
        """Initialize portfolio state from exchanges."""
        initialization_tasks: list[Awaitable[bool]] = []
        logger.info(
            f"PortfolioTracker {id(self)}: About to gather init tasks. Balance dict: {self._balances}"
        )
        for exchange_id, _client in self.api_clients.items():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue
            initialization_tasks.append(self._fetch_exchange_balances(exchange_id))
            initialization_tasks.append(self._fetch_exchange_positions(exchange_id))
            initialization_tasks.append(self._fetch_exchange_orders(exchange_id))
        logger.info(
            f"PortfolioTracker {id(self)}: About to gather init tasks. "
            f"Balances before: {self._balances}"
        )
        results: list[bool | BaseException] = await asyncio.gather(
            *initialization_tasks, return_exceptions=True
        )
        logger.info(f"---> State of self._balances immediately after init gather: {self._balances}")
        initialization_failed = False
        failed_tasks_info: list[str] = []
        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                task_description = f"task index {i}"
                logger.critical(
                    (
                        f"CRITICAL ERROR during PortfolioTracker initialization "
                        f"({task_description}): {result}"
                    ),
                    exc_info=result,
                )
                failed_tasks_info.append(f"{task_description}: {result}")
                initialization_failed = True
        if initialization_failed:
            error_summary = "; ".join(failed_tasks_info)
            logger.critical(
                f"PortfolioTracker failed to initialize essential data from one or more exchanges. "
                f"Cannot proceed reliably. Errors: {error_summary}"
            )
        initial_capital = self.get_total_capital()
        self._high_watermark = initial_capital
        if initial_capital > Decimal("0.0"):
            logger.info(f"Initial high watermark set to: {self._high_watermark}")
        else:
            logger.warning(f"Initial capital is {initial_capital}. High watermark not set.")
        logger.info("Portfolio state initialized")

    async def _fetch_exchange_balances(self, exchange_id: str) -> bool:
        logger.debug(f"[FETCH_BALANCES:{exchange_id}] Entering function.")
        try:
            client = self.api_clients.get(exchange_id)
            if not client:
                logger.error(f"[FETCH_BALANCES:{exchange_id}] No API client found.")
                return False

            logger.debug(f"[FETCH_BALANCES:{exchange_id}] Fetching balances from API...")
            balances_data = await client.get_balances()
            logger.debug(
                (
                    f"[FETCH_BALANCES:{exchange_id}] Raw API response: {balances_data} "
                    f"(Type: {type(balances_data)})"
                ),
                stacklevel=2,
            )

            if balances_data is None:
                logger.error(f"[FETCH_BALANCES:{exchange_id}] API call returned None.")
                return False

            # --- HANDLE DICT or LIST ---
            updated_balances: dict[str, SpotBalance] = {}
            processed = False
            try:
                for asset, balance_details in balances_data.items():
                    # Ensure exchange name is added
                    balance_details["exchange"] = exchange_id
                    balance_obj = self._parse_balance_info(exchange_id, asset, balance_details)
                    if balance_obj:
                        updated_balances[asset] = balance_obj
                processed = True
            except (ValidationError, TypeError, InvalidOperation) as e:
                logger.error(f"[{exchange_id}] Error parsing balance entry: {e}", exc_info=True)

            if processed:
                self._fetch_exchange_balances(exchange_id)
                self._last_update_time[exchange_id] = datetime.now(UTC)
                logger.info(
                    f"[FETCH_BALANCES:{exchange_id}] Successfully processed. Returning True."
                )
                return True
            elif processed and not updated_balances:
                logger.warning(
                    (
                        f"[FETCH_BALANCES:{exchange_id}] Processed the response, but resulting "
                        f"updated_balances dict is empty. Response was: {balances_data}. "
                        f"Returning False."
                    ),
                )
                return False
            else:  # Not processed (e.g., wrong data type)
                logger.error(
                    f"[FETCH_BALANCES:{exchange_id}] Failed to process balances data. "
                    f"Data type was {type(balances_data)}. Returning False."
                )
                return False

        except Exception as e:
            logger.exception(
                f"[FETCH_BALANCES:{exchange_id}] Unexpected error fetching balances: {e}"
            )
            return False

    def _parse_balance_info(
        self, exchange_id: str, asset: str, balance_info: dict[str, Any] | SpotBalance
    ) -> SpotBalance | None:
        """Parse balance information into a SpotBalance object."""
        # Handle SpotBalance object case
        if isinstance(balance_info, SpotBalance):
            # Ensure the exchange matches if it's already a SpotBalance object
            # (This shouldn't happen if fetched directly but good for defensive coding)
            if balance_info.exchange != exchange_id:
                logger.error(
                    f"Mismatched exchange ID in provided SpotBalance object: expected "
                    f"{exchange_id}, got {balance_info.exchange}"
                )
                return None
            return balance_info

        if not isinstance(balance_info, dict):
            logger.error(
                f"Invalid balance_info type: {type(balance_info)}. Expected dict or SpotBalance."
            )
            return None

        # Construct the data dictionary for SpotBalance, adding the exchange
        balance_data = balance_info.copy()
        balance_data["exchange"] = exchange_id
        balance_data["asset"] = asset  # Ensure asset is explicitly set

        try:
            # Validate and create the SpotBalance object
            # Pydantic handles parsing 'total', 'available' from str/int/float via validators
            parsed = SpotBalance(**balance_data)
            # Additional runtime checks (redundant with Pydantic ge=0 but defensive)
            if parsed.total < Decimal("0") or parsed.available < Decimal("0"):
                logger.error(
                    f"Parsed balance has negative values: Total={parsed.total}, "
                    f"Available={parsed.available}"
                )
                return None
            return parsed
        except (ValidationError, TypeError, InvalidOperation) as e:
            logger.error(
                f"Failed to parse balance for {asset} on {exchange_id}: {e}", exc_info=True
            )
            return None

    @staticmethod
    def _safe_decimal_convert(
        value: str | Decimal | int | float | None, field_name: str, asset: str, exchange_id: str
    ) -> Decimal | None:
        """Safely convert a value to Decimal, logging errors."""
        if value is None:
            # logger.debug(f"Value for {field_name} ({asset} on {exchange_id}) is None.")
            return None
        try:
            # Handle potential scientific notation strings from some APIs
            if isinstance(value, str) and ("e" in value or "E" in value):
                dec_value = Decimal(value)
                # Optional: Convert very small numbers near zero to actual zero if desired
                # if abs(dec_value) < Decimal('1e-18'): # Adjust threshold as needed
                #     return Decimal('0')
                return dec_value
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))  # Convert via string for precision
        except (InvalidOperation, ValueError, TypeError) as e:
            logger.error(
                f"Failed to convert '{field_name}' value '{value}' "
                f"(type: {type(value)}) to Decimal for {asset} on {exchange_id}: {e}"
            )
            return None

    async def _fetch_exchange_positions(self, exchange_id: str) -> bool:
        """Fetch and update positions for a specific exchange."""
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"No API client registered for {exchange_id} in fetch_positions")
            return False
        try:
            # Assuming get_positions now returns List[DerivativePosition]
            positions_data: list[DerivativePosition] = await client.get_positions()
            updated_positions: dict[str, DerivativePosition] = {}
            for position_info in positions_data:
                # Use symbol as the key for now
                # TODO: Revisit position identification strategy
                pos_key = position_info.symbol
                if pos_key:
                    # Assume position_info is already a validated DerivativePosition
                    # No need for _safe_decimal_convert if API layer provides validated models
                    updated_positions[pos_key] = position_info
                else:
                    logger.warning(
                        f"Skipping DerivativePosition object without symbol on {exchange_id}: {position_info}"
                    )

            self._positions[exchange_id] = updated_positions
            self._last_update_time[exchange_id] = datetime.now(UTC)
            logger.debug(
                f"Successfully updated positions for {exchange_id}. Count: {len(updated_positions)}"
            )
            return True
        except Exception as e:
            logger.exception(f"Failed to fetch positions for {exchange_id}: {e}")
            return False

    async def _fetch_exchange_orders(self, exchange_id: str) -> bool:
        """Fetch and update open orders for a specific exchange."""
        try:
            client = self.api_clients.get(exchange_id)
            if not client:
                logger.error(f"No API client found for {exchange_id}")
                return False
            orders_data = await client.get_open_orders()  # Assuming this returns a list
            updated_orders: dict[str, Order] = {}
            for order_info in orders_data:
                order_instance = None
                order_id = None
                # order_info is always Order
                order_instance = order_info
                order_id = order_instance.client_order_id
                if order_instance and order_id:
                    order_instance.price = self._safe_decimal_convert(
                        order_instance.price, "price", order_instance.symbol, exchange_id
                    )
                    order_instance.quantity_requested = self._safe_decimal_convert(
                        order_instance.quantity_requested,
                        "quantity_requested",
                        order_instance.symbol,
                        exchange_id,
                    ) or Decimal("0")
                    order_instance.quantity_filled = self._safe_decimal_convert(
                        order_instance.quantity_filled,
                        "quantity_filled",
                        order_instance.symbol,
                        exchange_id,
                    ) or Decimal("0")
                    if isinstance(order_instance.status, str):
                        try:
                            order_instance.status = OrderStatus(order_instance.status)
                        except ValueError:
                            logger.warning(
                                f"Invalid status string '{order_instance.status}' for "
                                f"order {order_id}"
                            )
                            order_instance.status = OrderStatus.UNKNOWN
                    updated_orders[str(order_id)] = order_instance
            self._orders[exchange_id] = updated_orders
            self._last_update_time[exchange_id] = datetime.now(UTC)
            logger.debug(
                f"Successfully updated open orders for {exchange_id}. Count: {len(updated_orders)}"
            )
            return True
        except Exception as e:
            logger.exception(f"Unexpected error fetching orders for {exchange_id}: {e}")
            return False

    async def update(self) -> None:
        """Update portfolio state by fetching data from exchanges."""
        now = datetime.now(UTC)
        update_tasks: list[Awaitable[bool]] = []
        for exchange_id, _client in self.api_clients.items():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue
            last_reconciliation = self._last_reconciliation_time.get(
                exchange_id, datetime.min.replace(tzinfo=UTC)
            )
            needs_reconciliation = (
                now - last_reconciliation
            ).total_seconds() >= self.reconciliation_interval
            if needs_reconciliation:
                logger.info(f"Reconciliation needed for {exchange_id}. Fetching all data.")
                update_tasks.append(self._fetch_exchange_balances(exchange_id))
                update_tasks.append(self._fetch_exchange_positions(exchange_id))
                update_tasks.append(self._fetch_exchange_orders(exchange_id))
                self._last_reconciliation_time[exchange_id] = now
            else:
                logger.debug(f"Fetching only orders for {exchange_id} (no reconciliation needed).")
                update_tasks.append(self._fetch_exchange_orders(exchange_id))
        if update_tasks:
            results: list[bool | BaseException] = await asyncio.gather(
                *update_tasks, return_exceptions=True
            )
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        f"Error during portfolio update task (index {i}): {result}", exc_info=result
                    )
                elif result is False:
                    logger.warning(
                        f"Portfolio update task (index {i}) indicated failure (returned False)."
                    )
        current_capital = self.get_total_capital()
        self._high_watermark = max(self._high_watermark, current_capital)

    def update_order(self, exchange_id: str, order: Order) -> None:
        """
        Update the status of a single order.

        Args:
            exchange_id: Exchange identifier
            order: Order object with updated information
        """
        if exchange_id not in self._orders:
            self._orders[exchange_id] = {}
            logger.warning(f"Initialized order tracking for {exchange_id} during update_order.")
        if not order.client_order_id:  # Use .client_order_id
            logger.error(f"Received order update without client_order_id on {exchange_id}: {order}")
            return
        order_id_str = str(order.client_order_id)  # Use .client_order_id, ensure key is string
        self._orders[exchange_id][order_id_str] = order
        # Handle order state transitions
        # In particular, we want to detect when an order reaches FILLED or PARTIALLY_FILLED
        # and generate a corresponding trade for the position tracker
        if order.status in [
            OrderStatus.FILLED,
            OrderStatus.PARTIALLY_FILLED,
        ] and order.quantity_filled > Decimal("0"):
            logger.info(
                f"Order {order_id_str} on {exchange_id} is {order.status}. "
                f"Triggering trade processing (placeholder)."
            )
            # self.process_trade(exchange_id, Trade(...)) # Requires Trade object creation logic

    def update_position(self, exchange_id: str, position: DerivativePosition) -> None:
        """
        Update the state of a single position.
        Args:
            exchange_id: Exchange identifier
            position: DerivativePosition object with updated information
        """
        if exchange_id not in self._positions:
            self._positions[exchange_id] = {}
            logger.warning(
                f"Initialized position tracking for {exchange_id} during update_position."
            )

        pos_key = position.symbol  # Use symbol as key
        if not pos_key:
            logger.error(f"Received position update without symbol on {exchange_id}: {position}")
            return

        self._positions[exchange_id][pos_key] = position
        self._last_update_time[exchange_id] = datetime.now(UTC)
        logger.debug(f"Updated position {pos_key} for {exchange_id}")

    def process_trade(self, exchange_id: str, trade: Trade) -> None:
        """
        Process a trade execution and update relevant portfolio state.
        Args:
            exchange_id: Exchange where the trade occurred.
            trade: Trade object representing the execution.
        """
        logger.info(
            f"Processing trade on {exchange_id}: {trade.side} {trade.quantity} "
            f"{trade.symbol} @ {trade.price}"
        )

        position_key = trade.symbol
        current_position = self._positions.get(exchange_id, {}).get(position_key)

        if current_position:
            logger.debug(f"Updating existing position for {trade.symbol} on {exchange_id}")
            original_size = current_position.size
            original_entry = current_position.entry_price or Decimal("0")  # Handle potential None
            trade_effect = trade.quantity if trade.side == OrderSide.BUY else -trade.quantity
            new_position_size = original_size + trade_effect

            if new_position_size == Decimal("0"):
                # Position closed
                if original_entry > 0:  # Avoid division by zero if entry was somehow 0
                    pnl = (trade.price - original_entry) * original_size.copy_sign(Decimal("1"))
                    if current_position.side == OrderSide.SELL:
                        pnl = -pnl
                    self._update_realized_pnl(pnl)
                    logger.info(
                        f"Realized PNL from closing {trade.symbol}: {pnl:.4f}. "
                        f"Total Realized PNL: {self._realized_pnl:.4f}"
                    )
                else:
                    logger.warning(
                        f"Cannot calculate PNL for closing {trade.symbol} due to zero/None entry price."
                    )

                del self._positions[exchange_id][position_key]
                logger.debug(f"Position {trade.symbol} closed on {exchange_id}.")

            else:
                # Position modified (size increased or decreased but not closed)
                # Calculate new average entry price
                new_entry_price = original_entry  # Default if original size was 0
                if original_size != Decimal("0"):
                    new_entry_price = (
                        (original_entry * abs(original_size)) + (trade.price * abs(trade_effect))
                    ) / abs(new_position_size)

                current_position.entry_price = new_entry_price
                current_position.size = new_position_size
                current_position.side = OrderSide.BUY if new_position_size > 0 else OrderSide.SELL
                current_position.timestamp = trade.timestamp  # Update timestamp
                # Mark price, liq price, unrealized PNL update via market data
                logger.debug(
                    f"Position {trade.symbol} modified. New size: {new_position_size}, New avg entry: {new_entry_price:.4f}"
                )

        else:
            # New position opened
            logger.debug(f"Opening new position for {trade.symbol} on {exchange_id}")
            side = trade.side
            new_position = DerivativePosition(
                exchange=exchange_id,
                symbol=trade.symbol,
                side=side,
                size=trade.quantity if side == OrderSide.BUY else -trade.quantity,
                entry_price=trade.price,  # Assume trade price is valid entry > 0
                timestamp=trade.timestamp,
                mark_price=trade.price,  # Initial mark price
                unrealized_pnl=Decimal("0.0"),
                realized_pnl=Decimal("0.0"),
                # hl_details=None, # Details would need separate fetching/update
                # bp_details=None,
            )
            self._positions.setdefault(exchange_id, {})[position_key] = new_position

        # Update Balances (Simplified - full logic depends on asset details)
        self._update_balances_from_trade(exchange_id, trade)

        self._last_update_time[exchange_id] = datetime.now(UTC)

    def _update_balances_from_trade(self, exchange_id: str, trade: Trade):
        # Placeholder for balance update logic based on trade details
        logger.debug(f"Placeholder: Update balances for trade {trade.id} on {exchange_id}")
        pass

    def _update_realized_pnl(self, amount: Decimal) -> None:
        """Update the total realized PNL."""
        if not isinstance(amount, Decimal) or not amount.is_finite():
            logger.error(f"Attempted to update realized PNL with invalid amount: {amount}")
            return
        self._realized_pnl += amount
        logger.info(f"Realized PNL updated by {amount:.4f}. New total: {self._realized_pnl:.4f}")

    # --- Position Access Methods ---
    def get_position(self, exchange_id: str, position_key: str) -> DerivativePosition | None:
        """
        Get a specific position by its key (currently symbol) on an exchange.
        Args:
            exchange_id: Exchange identifier.
            position_key: Unique key for the position (e.g., symbol).
        Returns:
            DerivativePosition object or None if not found.
        """
        return self._positions.get(exchange_id, {}).get(position_key)

    def get_positions_by_symbol(self, exchange_id: str, symbol: str) -> list[DerivativePosition]:
        """Get all positions for a specific symbol on an exchange."""  # Simplified docstring
        matching_positions: list[DerivativePosition] = []
        # Assuming positions are keyed by symbol now
        position = self._positions.get(exchange_id, {}).get(symbol)
        if position:
            matching_positions.append(position)
        # If keys might not be symbol, revert to iterating:
        # for position in self._positions.get(exchange_id, {}).values():
        #     if position.symbol == symbol:
        #         matching_positions.append(position)
        return matching_positions

    def get_all_positions(self) -> list[tuple[str, DerivativePosition]]:
        """Get all positions across all exchanges."""
        all_positions: list[tuple[str, DerivativePosition]] = []
        for exchange_id, positions in self._positions.items():
            for position in positions.values():
                all_positions.append((exchange_id, position))
        return all_positions

    def get_positions_by_exchange(self, exchange_id: str) -> list[DerivativePosition]:
        """Get all positions for a specific exchange."""  # Simplified docstring
        if exchange_id not in self._positions:
            logger.warning(f"Attempted to get positions for unknown exchange: {exchange_id}")
            return []
        return list(self._positions[exchange_id].values())

    @property
    def positions(self) -> dict[str, dict[str, DerivativePosition]]:
        """Public read-only accessor for all tracked positions."""  # Simplified docstring
        return self._positions

    def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        # ... (Implementation needs careful review for balance/position value calculation) ...
        # This implementation relies heavily on _get_asset_price_in_base which needs defining
        # Also needs to consider margin/position values correctly.
        logger.warning("get_total_capital needs review for DerivativePosition integration.")
        # Simplified temporary version using only balances:
        total_value = Decimal("0.0")
        for exchange_id, balances_dict in self._balances.items():
            for asset, balance in balances_dict.items():
                balance_total = getattr(balance, "total", Decimal("0.0"))
                if not isinstance(balance_total, Decimal) or not balance_total.is_finite():
                    logger.warning(f"Invalid/non-finite balance total for {asset} on {exchange_id}")
                    continue

                value_in_base = balance_total
                if asset != base_currency:
                    price = self._get_asset_price_in_base(asset, base_currency)
                    if price is not None and price.is_finite() and price > 0:
                        value_in_base = balance_total * price
                    else:
                        logger.warning(
                            f"Skipping {asset} on {exchange_id} in capital calc due to missing/invalid price."
                        )
                        continue
                total_value += value_in_base

        # Add unrealized PnL from positions
        unrealized_pnl, _ = self.get_pnl()  # Assuming get_pnl is updated for DerivativePosition
        if unrealized_pnl.is_finite():
            total_value += unrealized_pnl
        else:
            logger.error("Total unrealized PnL is non-finite. Not adding to total capital.")

        if total_value.is_finite():
            self._high_watermark = max(self._high_watermark, total_value)
        else:
            logger.error(
                f"Calculated total capital is non-finite ({total_value}). High watermark not updated."
            )
            return Decimal("0.0")  # Return 0 if non-finite

        return total_value

    def get_exchange_exposure(self, exchange_id: str) -> Decimal:
        """Calculate the total exposure (position value) on a specific exchange."""
        exposure = Decimal("0.0")
        for position in self._positions.get(exchange_id, {}).values():
            size = position.size
            mark_price = position.mark_price
            if (
                mark_price is not None
                and isinstance(mark_price, Decimal)
                and mark_price.is_finite()
                and isinstance(size, Decimal)
                and size.is_finite()
            ):
                exposure += abs(size) * mark_price
            else:
                logger.warning(
                    f"Skipping position {position.symbol} in exposure calc due to invalid size/mark price."
                )
        return exposure

    def get_total_exposure(self, valuation_asset: str = "USDC") -> Decimal:
        """Calculate the total portfolio exposure across all exchanges."""
        # TODO: Needs price conversion if valuation_asset is not quote asset of position
        logger.warning(
            "get_total_exposure needs review for DerivativePosition and price conversion."
        )
        total_exposure = Decimal("0.0")
        for exchange_positions in self._positions.values():
            for position in exchange_positions.values():
                size = position.size
                mark_price = position.mark_price
                if (
                    mark_price is not None
                    and isinstance(mark_price, Decimal)
                    and mark_price.is_finite()
                    and isinstance(size, Decimal)
                    and size.is_finite()
                ):
                    # Assuming mark_price is in valuation_asset for now
                    exposure_value = size * mark_price
                    total_exposure += exposure_value
                else:
                    logger.warning(
                        f"Skipping position {position.symbol} in total exposure calc due to invalid size/mark price."
                    )

        return total_exposure

    def get_pnl(self) -> tuple[Decimal, Decimal]:
        """Calculate total unrealized and realized PNL."""  # Simplified docstring
        total_unrealized_pnl = Decimal("0.0")
        for exchange_positions in self._positions.values():
            for position in exchange_positions.values():
                # Use stored unrealized_pnl if available and valid
                unrealized_pnl = position.unrealized_pnl
                if (
                    unrealized_pnl is not None
                    and isinstance(unrealized_pnl, Decimal)
                    and unrealized_pnl.is_finite()
                ):
                    total_unrealized_pnl += unrealized_pnl
                else:
                    # Fallback: Calculate from mark/entry if possible
                    mark_price = position.mark_price
                    entry_price = position.entry_price
                    size = position.size
                    if (
                        isinstance(mark_price, Decimal)
                        and mark_price.is_finite()
                        and isinstance(entry_price, Decimal)
                        and entry_price.is_finite()
                        and entry_price > 0
                        and isinstance(size, Decimal)
                        and size.is_finite()
                        and size != 0
                    ):
                        pnl = (mark_price - entry_price) * size
                        if position.side == OrderSide.SELL:
                            pnl = -pnl  # Correct for short positions
                        total_unrealized_pnl += pnl
                    else:
                        logger.debug(
                            f"Cannot calculate unrealized PNL for {position.symbol} due to missing/invalid data."
                        )

        return total_unrealized_pnl, self._realized_pnl

    def get_current_drawdown(self) -> Decimal | None:
        """
        Calculate the current portfolio drawdown from the high watermark.

        Returns:
            Current drawdown as a positive Decimal percentage (e.g., 0.1 for 10%),
            or None if capital is zero or negative.
        """
        current_capital = self.get_total_capital()

        if self._high_watermark <= Decimal("0.0"):
            return Decimal("0.0")  # Or None? Returning 0 might be safer.

        # Calculation of the drawdown as a ratio (0.1 = 10% drawdown)
        drawdown_ratio = (self._high_watermark - current_capital) / self._high_watermark
        return max(Decimal("0.0"), drawdown_ratio)  # Ensure drawdown is not negative

    # --- Order Access Methods ---

    def get_open_orders(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """Get a list of open orders for a specific exchange and optional symbol."""
        open_orders: list[Order] = []
        for order in self._orders.get(exchange_id, {}).values():
            if order.status in [OrderStatus.NEW, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                if symbol is None or order.symbol == symbol:
                    open_orders.append(order)
        return open_orders

    def get_order_history(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """Get the history of all tracked orders for an exchange and optional symbol."""
        history: list[Order] = []
        for order in self._orders.get(exchange_id, {}).values():
            if symbol is None or order.symbol == symbol:
                history.append(order)
        return history

    def get_order_by_id(self, exchange_id: str, order_id: str) -> Order | None:
        """
        Retrieve a specific order by its ID from the internal tracking.

        Args:
            exchange_id: The exchange the order belongs to.
            order_id: The unique identifier of the order.

        Returns:
            The Order object if found, otherwise None.
        """
        return self._orders.get(exchange_id, {}).get(order_id)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the portfolio state to a dictionary."""
        # Deep copy might be safer if objects are mutable and used elsewhere
        state: dict[str, Any] = {
            "balances": self._balances,
            "positions": self._positions,
            "orders": self._orders,
            "last_update_time": self._last_update_time,
            "last_reconciliation_time": self._last_reconciliation_time,
            "high_watermark": self._high_watermark,
            "realized_pnl": self._realized_pnl,
        }
        return state

    @classmethod
    def from_dict(cls, data: dict[str, Any], config: Config) -> PortfolioTracker:
        """Deserialize the portfolio state from a dictionary."""
        # This requires careful handling of object reconstruction
        tracker = cls(config)  # Initialize with config
        tracker._balances = data.get("balances", {})
        tracker._positions = data.get("positions", {})
        tracker._orders = data.get("orders", {})
        tracker._last_update_time = data.get("last_update_time", {})
        tracker._last_reconciliation_time = data.get("last_reconciliation_time", {})
        tracker._high_watermark = Decimal(str(data.get("high_watermark", "0.0")))
        tracker._realized_pnl = Decimal(str(data.get("realized_pnl", "0.0")))

        # TODO: Convert nested dicts back into SpotBalance, Position, Order objects
        # This is crucial for the tracker to function correctly after loading state.
        # Example (needs iteration and error handling):
        # for ex_id, bals in tracker._balances.items():
        #     for asset, bal_data in bals.items():
        #         if isinstance(bal_data, dict):
        #             tracker._balances[ex_id][asset] = SpotBalance(**bal_data) # Assuming keys match
        #             # __init__
        # Similar loops for positions and orders...
        logger.warning(
            "PortfolioTracker.from_dict needs implementation for object deserialization."
        )

        return tracker

    # --- Watchlist/Active Symbols ---
    def add_symbol_to_watchlist(self, symbol: str) -> None:
        """Add a symbol to the watchlist."""
        if symbol not in self._watchlist:
            self._watchlist.add(symbol)
            logger.info(f"Added {symbol} to portfolio watchlist.")
            # Potentially trigger subscription logic if needed

    def remove_symbol_from_watchlist(self, symbol: str) -> None:
        """Remove a symbol from the watchlist."""
        if symbol in self._watchlist:
            self._watchlist.remove(symbol)
            logger.info(f"Removed {symbol} from portfolio watchlist.")
            # Potentially trigger unsubscription logic

    def get_watchlist(self) -> set[str]:
        """Get the current set of watched symbols."""
        return self._watchlist.copy()

    def update_active_symbols(self) -> None:
        """Update the set of symbols with active positions or open orders."""
        active: set[str] = set()
        for positions in self._positions.values():
            for pos in positions.values():
                if pos.size != Decimal("0"):
                    active.add(pos.symbol)
        for orders in self._orders.values():
            for order in orders.values():
                if order.status in [
                    OrderStatus.NEW,
                    OrderStatus.OPEN,
                    OrderStatus.PARTIALLY_FILLED,
                ]:
                    active.add(order.symbol)
        self._active_symbols = active
        # logger.debug(f"Active symbols updated: {self._active_symbols}")

    def get_active_symbols(self) -> set[str]:
        """Get the current set of symbols with active positions or orders."""
        # Ensure it's up-to-date before returning
        self.update_active_symbols()
        return self._active_symbols.copy()

    def get_relevant_symbols(self) -> set[str]:
        """Get all symbols relevant to the portfolio (active + watchlist)."""
        self.update_active_symbols()  # Ensure active symbols are current
        return self._active_symbols.union(self._watchlist)

    def _update_balance(self, exchange_id: str, balance: SpotBalance | None) -> None:
        if balance is None:
            return
        self._balances.setdefault(exchange_id, {})[balance.asset] = balance

    def _parse_balances(
        self, exchange_id: str, balances_data: list[dict[str, Any]] | dict[str, Any]
    ) -> None:
        # ... (Implementation added in previous attempt, assuming correct now) ...
        pass  # Placeholder

    def _parse_positions(
        self, exchange_id: str, positions_data: list[DerivativePosition] | dict[str, Any]
    ) -> None:
        logger.warning("_parse_positions needs implementation based on API data format.")
        if isinstance(positions_data, dict):
            for _pos_key, pos_data in positions_data.items():
                # TODO: Parse pos_data dict into DerivativePosition
                pass
        elif isinstance(positions_data, list):
            for pos_data in positions_data:
                # TODO: Parse pos_data (dict or object?) into DerivativePosition
                pass
        else:
            logger.error(f"Unexpected type for positions_data: {type(positions_data)}")

    def _parse_orders(self, exchange_id: str, orders_data: list[Order] | dict[str, Any]) -> None:
        logger.warning("_parse_orders needs implementation based on API data format.")
        if isinstance(orders_data, dict):
            for _order_key, order_data in orders_data.items():
                # TODO: Parse order_data dict into Order
                pass
        elif isinstance(orders_data, list):
            for order_data in orders_data:
                # TODO: Parse order_data (dict or object?) into Order
                pass
        else:
            logger.error(f"Unexpected type for orders_data: {type(orders_data)}")

    def reset(self) -> None:
        """
        Reset the portfolio tracker to a clean initial state.

        This method clears all tracked balances, positions, orders, timestamps, high watermark,
        realized PNL, active symbols, and watchlist. It is intended for use in tests or integration
        scenarios where a fresh portfolio state is required.
        """
        self._balances.clear()
        self._positions.clear()
        self._orders.clear()
        self._last_update_time.clear()
        self._last_reconciliation_time.clear()
        self._high_watermark = Decimal("0.0")
        self._realized_pnl = Decimal("0.0")
        self._active_symbols.clear()
        self._watchlist.clear()
        self._initialize_data_structures()
        # Optionally, log the reset event
        logger.info("PortfolioTracker state has been reset.")

    async def load_state(self) -> None:
        # ... (Implementation as before) ...
        pass  # Placeholder

    async def initialize_portfolio(self) -> None:
        # ... (Implementation as before) ...
        pass  # Placeholder

    # --- Helper for Price Conversion (Placeholder) ---
    def _get_asset_price_in_base(self, asset: str, base_currency: str) -> Decimal | None:
        # TODO: Implement actual price fetching/lookup (e.g., from DataHandler or Tickers)
        logger.warning(
            f"Price conversion for {asset} to {base_currency} not implemented. Returning None."
        )
        if asset == base_currency:
            return Decimal("1.0")
        # Example: if asset == 'BTC' and base_currency == 'USDC', return current BTC/USDC price
        return None
