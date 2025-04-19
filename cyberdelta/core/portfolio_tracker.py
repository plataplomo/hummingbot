from __future__ import annotations  # Enable postponed evaluation

import asyncio
from builtins import BaseException
from collections.abc import Awaitable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.core.models import (
    Balance,
    Order,
    OrderSide,
    OrderStatus,
    Position,
    Trade,
)
from cyberdelta.core.risk_manager import ExchangeBalance
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
        self._balances: dict[str, dict[str, Balance]] = {}  # exchange -> asset -> Balance object

        # Position tracking
        self._positions: dict[str, dict[str, Position]] = {}  # exchange -> position_id -> Position

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
            updated_balances: dict[str, Balance] = {}
            processed = False
            if isinstance(balances_data, dict):
                logger.debug(f"[FETCH_BALANCES:{exchange_id}] Processing DICT.")
                for asset, balance_info in balances_data.items():
                    # asset is always str, balance_info is Balance or dict[str, Any]
                    if asset:
                        balance_instance = self._parse_balance_info(
                            exchange_id, asset, balance_info
                        )
                        if balance_instance:
                            updated_balances[asset] = balance_instance
                        else:
                            logger.warning(
                                f"[_fetch_exchange_balances:{exchange_id}] "
                                f"Failed to parse balance info for asset {asset}."
                            )
                            continue  # Skip this item if parsing failed
                    else:
                        logger.warning(
                            f"[_fetch_exchange_balances:{exchange_id}] "
                            f"Skipping balance item with missing asset or info: "
                            f"asset={asset}, info={balance_info}"
                        )
                        continue  # Skip this item
                processed = True
            else:
                # Defensive: balances_data is expected to be a list by type hint.
                # isinstance check removed.
                logger.debug(f"[FETCH_BALANCES:{exchange_id}] Processing LIST.")
                for balance_item in balances_data:
                    item_asset: str | None = None
                    parsed_balance: Balance | None = None
                    if type(balance_item) is Balance:
                        item_asset = balance_item.asset
                        parsed_balance = balance_item
                    else:
                        asset_candidate = (
                            balance_item.get("asset") if type(balance_item) is dict else None
                        )
                        if isinstance(asset_candidate, str):
                            item_asset = asset_candidate
                            parsed_balance = self._parse_balance_info(
                                exchange_id, item_asset, balance_item
                            )
                        else:
                            logger.warning(
                                f"[_fetch_exchange_balances:{exchange_id}] "
                                f"Skipping balance dict item missing or invalid 'asset' key: "
                                f"{balance_item}"
                            )
                            continue  # Skip this item
                    if parsed_balance and item_asset:
                        updated_balances[item_asset] = parsed_balance
                processed = True

            if processed and updated_balances:
                logger.info(
                    (
                        f"[FETCH_BALANCES:{exchange_id}] BEFORE assign: "
                        f"self._balances[{exchange_id}] = {self._balances.get(exchange_id)}. "
                        f"updated_balances = {updated_balances}"
                    ),
                )
                self._balances[exchange_id] = updated_balances
                logger.info(
                    (
                        f"[FETCH_BALANCES:{exchange_id}] AFTER assign: "
                        f"self._balances[{exchange_id}] = {self._balances.get(exchange_id)}"
                    ),
                )
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
        self, exchange_id: str, asset: str, balance_info: dict[str, Any] | Balance
    ) -> Balance | None:
        """Parse balance information into a Balance object."""
        # Handle Balance object case
        if isinstance(balance_info, Balance):
            try:
                # Already a Balance object, ensure Decimal types
                balance_info.total = self._safe_decimal_convert(
                    balance_info.total, "total", asset, exchange_id
                ) or Decimal("0")
                balance_info.free = self._safe_decimal_convert(
                    balance_info.free, "free", asset, exchange_id
                )
                balance_info.locked = self._safe_decimal_convert(
                    balance_info.locked, "locked", asset, exchange_id
                )
                return balance_info
            except (InvalidOperation, ValueError, TypeError) as e:
                logger.error(
                    f"Error converting Balance fields for {asset} on {exchange_id}: "
                    f"{e}. Data: {balance_info}"
                )
                return None

        # Defensive: balance_info is expected to be a dict by type hint; isinstance check removed.
        try:
            total = self._safe_decimal_convert(
                balance_info.get("total"), "total", asset, exchange_id
            )
            free = self._safe_decimal_convert(balance_info.get("free"), "free", asset, exchange_id)
            locked = self._safe_decimal_convert(
                balance_info.get("locked"), "locked", asset, exchange_id
            )

            if total is None:
                logger.warning(
                    f"Missing 'total' balance for {asset} on {exchange_id}. "
                    f"Cannot create Balance object."
                )
                return None

            # If free or locked is missing, try to infer or default to 0
            if free is None and locked is not None:
                free = total - locked
            elif locked is None and free is not None:
                locked = total - free
            elif free is None and locked is None:
                # Cannot determine free/locked, assume all is free if total > 0
                if total > Decimal("0"):
                    free = total
                    locked = Decimal("0")
                    logger.warning(
                        f"Missing 'free' and 'locked' for {asset} on {exchange_id}. "
                        f"Assuming all 'total' is free."
                    )
                else:
                    free = Decimal("0")
                    locked = Decimal("0")

            return Balance(asset=asset, total=total, free=free, locked=locked)
        except (InvalidOperation, ValueError, TypeError) as e:
            logger.error(
                f"Error parsing balance dict for {asset} on {exchange_id}: "
                f"{e}. Data: {balance_info}"
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
        logger.debug(f"Fetching positions for {exchange_id}")
        try:
            client = self.api_clients.get(exchange_id)
            if not client:
                logger.error(f"No API client found for {exchange_id}")
                return False

            positions_data = await client.get_positions()  # Type hint guarantees list[Position]
            # The 'if positions_data is None:' check was removed as it's unreachable
            # based on the ExchangeAPI.get_positions() type hint.
            # An empty list [] indicates no positions.
            updated_positions: dict[str, Position] = {}
            for position_info in positions_data:
                # position_info is always Position
                # If it's already a Position object, use its ID if available, else symbol
                pos_id = getattr(position_info, "id", None) or position_info.symbol
                if pos_id:
                    # Ensure Decimal types are correct
                    position_info.size = self._safe_decimal_convert(
                        position_info.size, "size", position_info.symbol, exchange_id
                    ) or Decimal("0")
                    position_info.entry_price = self._safe_decimal_convert(
                        position_info.entry_price,
                        "entry_price",
                        position_info.symbol,
                        exchange_id,
                    ) or Decimal("0")
                    position_info.mark_price = self._safe_decimal_convert(
                        position_info.mark_price,
                        "mark_price",
                        position_info.symbol,
                        exchange_id,
                    )
                    position_info.liquidation_price = self._safe_decimal_convert(
                        position_info.liquidation_price,
                        "liquidation_price",
                        position_info.symbol,
                        exchange_id,
                    )
                    position_info.unrealized_pnl = self._safe_decimal_convert(
                        position_info.unrealized_pnl,
                        "unrealized_pnl",
                        position_info.symbol,
                        exchange_id,
                    )
                    position_info.leverage = self._safe_decimal_convert(
                        position_info.leverage,
                        "leverage",
                        position_info.symbol,
                        exchange_id,
                    )
                    updated_positions[pos_id] = position_info
                else:
                    logger.warning(
                        f"Skipping Position object without symbol on {exchange_id}: {position_info}"
                    )
            self._positions[exchange_id] = updated_positions
            self._last_update_time[exchange_id] = datetime.now(UTC)
            logger.debug(
                f"Successfully updated positions for {exchange_id}. Count: {len(updated_positions)}"
            )
            return True

        except Exception as e:
            logger.exception(f"Unexpected error fetching positions for {exchange_id}: {e}")
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

    def update_position(self, exchange_id: str, position: Position) -> None:
        """
        Update the state of a single position.

        Args:
            exchange_id: Exchange identifier
            position: Position object with updated information
        """
        if exchange_id not in self._positions:
            self._positions[exchange_id] = {}
            logger.warning(
                f"Initialized position tracking for {exchange_id} during update_position."
            )

        # Use position ID if available, otherwise symbol as key
        position_key = getattr(position, "id", None) or position.symbol
        if not position_key:
            logger.error(
                f"Received position update without id or symbol on {exchange_id}: {position}"
            )
            return

        # Ensure key is string
        position_key_str = str(position_key)

        # logger.debug(
        #     f"Updating position {position_key_str} ({position.symbol}) on {exchange_id}. "
        #     f"New size: {position.size}"
        # )
        self._positions[exchange_id][position_key_str] = position
        self._last_update_time[exchange_id] = datetime.now(UTC)

    def process_trade(self, exchange_id: str, trade: Trade) -> None:
        """
        Process a trade execution and update relevant portfolio state.
        This is a simplified version; a dedicated ExecutionHandler might manage this.

        Args:
            exchange_id: Exchange where the trade occurred.
            trade: Trade object representing the execution.
        """
        logger.info(
            f"Processing trade on {exchange_id}: {trade.side} {trade.quantity} "
            f"{trade.symbol} @ {trade.price}"
        )

        # 1. Update Realized PNL (Requires knowing the cost basis of the closed portion)
        # This is complex and requires tracking position entry details (FIFO, LIFO, avg cost).
        # For simplicity, we'll only update realized PNL if a position is fully closed.

        # 2. Update Position
        position_key = trade.symbol  # Assuming positions are keyed by symbol for simplicity here
        current_position = self._positions.get(exchange_id, {}).get(position_key)

        if current_position:
            logger.debug(f"Updating existing position for {trade.symbol} on {exchange_id}")
            original_size = current_position.size
            trade_effect = trade.quantity if trade.side == OrderSide.BUY else -trade.quantity
            new_position_size = original_size + trade_effect
            # Calculate realized PNL for the closed position
            pnl = (trade.price - current_position.entry_price) * original_size.copy_sign(
                Decimal("1")
            )
            if current_position.side == OrderSide.SELL:
                pnl = -pnl
            self._update_realized_pnl(pnl)
            logger.info(
                f"Realized PNL from closing {trade.symbol}: {pnl}. "
                f"Total Realized PNL: {self._realized_pnl}"
            )
            del self._positions[exchange_id][position_key]
            # Recalculate average entry price (Weighted average)
            current_size = abs(current_position.size)
            new_size = abs(new_position_size)
            new_entry_price = (
                (current_position.entry_price * current_size) + (trade.price * abs(trade_effect))
            ) / new_size
            current_position.entry_price = new_entry_price
            logger.debug(f"Position {trade.symbol} updated. New avg entry: {new_entry_price}")
            current_position.size = new_size
            current_position.side = OrderSide.BUY if new_size > 0 else OrderSide.SELL
            logger.debug(f"Position {trade.symbol} updated. New size: {new_size}")
            # Mark price, liq price, unrealized PNL would be updated by market data streams
            # typically
        else:
            logger.debug(f"Opening new position for {trade.symbol} on {exchange_id}")
            side = trade.side
            new_position = Position(
                symbol=trade.symbol,
                size=trade.quantity if side == OrderSide.BUY else -trade.quantity,
                entry_price=trade.price,
                side=side,
                mark_price=trade.price,
                unrealized_pnl=Decimal("0.0"),
            )
            self._positions.setdefault(exchange_id, {})[position_key] = new_position

        # 3. Update Balances (Reduce cash/base asset, potentially update quote asset if relevant)
        # This requires knowing the trade fee and the base/quote assets.
        # Example: Buy BTC/USDC -> Decrease USDC, Increase BTC (or reflect in position value)
        _, quote_asset = self._split_symbol(trade.symbol)  # Simple split, might need refinement
        cost = trade.quantity * trade.price
        fee = trade.fee or Decimal("0.0")  # Assume fee is in quote currency

        if quote_asset and quote_asset in self._balances.get(exchange_id, {}):
            balance = self._balances[exchange_id][quote_asset]
            if trade.side == OrderSide.BUY:
                balance.total -= cost + fee
            else:  # Sell
                balance.total += cost - fee
            logger.debug(
                f"Updated {quote_asset} balance on {exchange_id} due to trade. "
                f"New total: {balance.total}"
            )

        self._last_update_time[exchange_id] = datetime.now(UTC)

    def _split_symbol(self, symbol: str) -> tuple[str, str]:
        """Basic symbol splitting (e.g., BTC/USDC -> BTC, USDC).

        Needs refinement for complex symbols.
        """
        # This is a placeholder. Implement robust symbol parsing based on expected formats.
        parts = symbol.split("/")
        if len(parts) == 2:
            return parts[0], parts[1]
        elif symbol.endswith("USDT"):  # Common convention
            return symbol[:-4], "USDT"
        elif symbol.endswith("USDC"):
            return symbol[:-4], "USDC"
        # Add more rules as needed
        logger.warning(f"Could not reliably split symbol '{symbol}' into base/quote.")
        return symbol, ""  # Fallback

    def update_balance(self, exchange_id: str, asset: str, amount: Decimal | Balance) -> None:
        """
        Update the balance for a specific asset on an exchange.

        Args:
            exchange_id: Exchange identifier
            asset: Asset symbol (e.g., 'USDC')
            amount: The new total balance (as Decimal) or a Balance object.
        """
        if exchange_id not in self._balances:
            self._balances[exchange_id] = {}
            logger.warning(f"Initialized balance tracking for {exchange_id} during update_balance.")

        if isinstance(amount, Balance):
            # If a full Balance object is provided, use it directly
            if amount.asset != asset:
                logger.error(
                    f"Asset mismatch in update_balance: expected {asset}, got {amount.asset}"
                )
                return
            # Ensure Decimal types
            amount.total = self._safe_decimal_convert(
                amount.total, "total", asset, exchange_id
            ) or Decimal("0")
            amount.free = self._safe_decimal_convert(amount.free, "free", asset, exchange_id)
            amount.locked = self._safe_decimal_convert(amount.locked, "locked", asset, exchange_id)
            self._balances[exchange_id][asset] = amount
            logger.debug(
                f"Updated balance for {asset} on {exchange_id} using Balance object. "
                f"New total: {amount.total}"
            )
        elif type(amount) in (Decimal, int, float, str):
            # If only a numerical amount is provided, update the total balance
            # This is less ideal as free/locked info is lost or becomes stale
            safe_amount = self._safe_decimal_convert(amount, "total", asset, exchange_id)
            if safe_amount is None:
                logger.error(
                    f"Invalid amount type/value provided for {asset} on {exchange_id}: {amount}"
                )
                return

            if asset in self._balances[exchange_id]:
                # Update existing balance object's total
                self._balances[exchange_id][asset].total = safe_amount
                # Mark free/locked as potentially stale if only total is updated?
                logger.warning(
                    f"Updating total balance for {asset} on {exchange_id} to {safe_amount}. "
                    f"Free/locked might be stale."
                )
            else:
                # Create a new balance object, assuming all is free
                self._balances[exchange_id][asset] = Balance(
                    asset=asset, total=safe_amount, free=safe_amount, locked=Decimal("0")
                )
                logger.debug(
                    f"Created new balance for {asset} on {exchange_id}. Total: {safe_amount}"
                )

        self._last_update_time[exchange_id] = datetime.now(UTC)

    def get_exchange_balance(self, exchange: str, asset: str) -> ExchangeBalance | None:
        """
        Get the exchange balance for a specific asset, as required by PortfolioTrackerProtocol.

        Args:
            exchange: Exchange identifier (protocol-compliant name)
            asset: Asset symbol

        Returns:
            ExchangeBalance TypedDict with at least 'available', or None if not found.
        """
        bal = self._balances.get(exchange, {}).get(asset)
        if bal is None:
            return None
        # Protocol requires at least 'available' (Decimal)
        return {
            "available": bal.available
            if hasattr(bal, "available") and bal.available is not None
            else bal.total
        }

    def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        """
        Calculate the total portfolio value in the specified base currency.

        Args:
            base_currency: The currency to express the total value in (e.g., 'USDC').

        Returns:
            Total portfolio value as a Decimal.
        """
        total_value = Decimal("0.0")
        for _exchange_id, balances in self._balances.items():
            for asset, balance in balances.items():
                # Skip check for None since total is never None in this implementation
                value_in_base = balance.total
                if asset != base_currency:
                    # Need a way to get the current price of 'asset' in 'base_currency'
                    # This functionality might belong elsewhere (e.g., DataHandler or a PriceOracle)
                    # For now, we assume 'total' is already in the base currency if asset !=
                    # base_currency
                    # Or we skip non-base currency assets if no conversion is available.
                    # Let's assume 'total' for non-base assets represents their value in
                    # base_currency.
                    logger.debug(
                        f"Assuming balance.total for {asset} ({value_in_base}) is "
                        f"already in {base_currency}"
                    )
                    pass  # Keep value_in_base as balance.total
                total_value += value_in_base
        # Update high watermark
        self._high_watermark = max(self._high_watermark, total_value)
        return total_value

    def get_exchange_exposure(self, exchange_id: str) -> Decimal:
        """Calculate the total exposure (position value) on a specific exchange."""
        exposure = Decimal("0.0")
        for position in self._positions.get(exchange_id, {}).values():
            size = position.size
            mark_price = position.mark_price
            # Defensive: mark_price may be None if not yet updated from exchange API.
            if mark_price is not None:
                exposure += abs(size) * mark_price
        return exposure

    def get_total_exposure(self, valuation_asset: str = "USDT") -> Decimal:
        """Calculate the total portfolio exposure across all exchanges."""
        total_exposure = Decimal("0.0")
        for exchange_positions in self._positions.values():
            for position in exchange_positions.values():
                size = position.size
                mark_price = position.mark_price
                if mark_price is not None:
                    total_exposure += size * mark_price
        return total_exposure

    def get_pnl(self) -> tuple[Decimal, Decimal]:
        """
        Calculate the total unrealized and realized PNL across the portfolio.

        Returns:
            A tuple containing (total_unrealized_pnl, total_realized_pnl).
        """
        total_unrealized_pnl = Decimal("0.0")
        for exchange_positions in self._positions.values():
            for position in exchange_positions.values():
                unrealized_pnl = position.unrealized_pnl
                mark_price = position.mark_price
                entry_price = position.entry_price
                size = position.size
                if unrealized_pnl is not None:
                    total_unrealized_pnl += unrealized_pnl
                elif mark_price is not None:
                    pnl = (mark_price - entry_price) * size
                    if position.side == OrderSide.SELL:
                        pnl = -pnl
                    total_unrealized_pnl += pnl
        return total_unrealized_pnl, self._realized_pnl

    def _update_realized_pnl(self, amount: Decimal) -> None:
        """Update the total realized PNL."""
        self._realized_pnl += amount
        logger.info(f"Realized PNL updated by {amount}. New total: {self._realized_pnl}")

    def get_position(self, exchange_id: str, position_id: str) -> Position | None:
        """
        Get a specific position by its ID on an exchange.

        Args:
            exchange_id: Exchange identifier.
            position_id: Unique identifier for the position.

        Returns:
            Position object or None if not found.
        """
        return self._positions.get(exchange_id, {}).get(position_id)

    def get_positions_by_symbol(self, exchange_id: str, symbol: str) -> list[Position]:
        """
        Get all positions for a specific symbol on an exchange.
        Note: Assumes storage is exchange -> position_id -> Position.
        Args:
            exchange_id: Exchange identifier.
            symbol: Trading symbol (e.g., 'BTC/USDC').
        Returns:
            A list of Position objects matching the symbol.
        """
        matching_positions: list[Position] = []
        for position in self._positions.get(exchange_id, {}).values():
            if position.symbol == symbol:
                matching_positions.append(position)
        return matching_positions

    def get_all_positions(self) -> list[tuple[str, Position]]:
        """Get all positions across all exchanges."""
        all_positions: list[tuple[str, Position]] = []
        for exchange_id, positions in self._positions.items():
            for position in positions.values():
                all_positions.append((exchange_id, position))
        return all_positions

    def get_positions_by_exchange(self, exchange_id: str) -> list[Position]:
        """
        Get all positions for a specific exchange.

        Args:
            exchange_id: The identifier of the exchange.

        Returns:
            A list of Position objects for the specified exchange.
        """
        if exchange_id not in self._positions:
            logger.warning(f"Attempted to get positions for unknown exchange: {exchange_id}")
            return []
        return list(self._positions[exchange_id].values())

    @property
    def positions(self) -> dict[str, dict[str, Position]]:
        """
        Public read-only accessor for all tracked positions.
        Returns:
            A dictionary mapping exchange_id to position_id to Position.
        Note:
            This property is intended for safe, read-only access (e.g., in tests).
            Modifying the returned dictionary or its contents may break encapsulation.
        """
        return self._positions

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

        # TODO: Convert nested dicts back into Balance, Position, Order objects
        # This is crucial for the tracker to function correctly after loading state.
        # Example (needs iteration and error handling):
        # for ex_id, bals in tracker._balances.items():
        #     for asset, bal_data in bals.items():
        #         if isinstance(bal_data, dict):
        #             tracker._balances[ex_id][asset] = Balance(**bal_data) # Assuming keys match
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

    def _update_balance(self, exchange_id: str, balance: Balance | None) -> None:
        if balance is None:
            return
        self._balances.setdefault(exchange_id, {})[balance.asset] = balance

    def _parse_balances(
        self, exchange_id: str, balances: list[Balance | dict[str, Any]] | dict[str, Any]
    ) -> None:
        if isinstance(balances, dict):
            for bal in balances.values():
                if isinstance(bal, Balance):
                    self._update_balance(exchange_id, bal)
                else:
                    self._update_balance(exchange_id, Balance(**bal))
        else:
            for bal in balances:
                if isinstance(bal, Balance):
                    self._update_balance(exchange_id, bal)
                else:
                    self._update_balance(exchange_id, Balance(**bal))

    def _parse_positions(
        self, exchange_id: str, positions: list[Position] | dict[str, Any]
    ) -> None:
        if isinstance(positions, dict):
            for _ in positions.values():
                pass
        else:
            for _ in positions:
                pass

    def _parse_orders(self, exchange_id: str, orders: list[Order] | dict[str, Any]) -> None:
        if isinstance(orders, dict):
            for _ in orders.values():
                pass
        else:
            for _ in orders:
                pass

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
        """
        Placeholder for loading persisted portfolio state from storage.
        In production, implement loading from disk or a database.
        """
        logger.info("PortfolioTracker.load_state called (no-op placeholder)")

    async def initialize_portfolio(self) -> None:
        """
        Initialize the portfolio by fetching balances, positions, and orders from exchanges.
        This is a wrapper for self.initialize() for compatibility with main.py.
        """
        logger.info(
            "PortfolioTracker.initialize_portfolio called; "
            "initializing portfolio state from exchanges."
        )
        await self.initialize()
