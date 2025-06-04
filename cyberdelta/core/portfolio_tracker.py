from __future__ import annotations  # Enable postponed evaluation

import asyncio
from builtins import BaseException
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.config_models import AppSettings, PortfolioTrackerConfig
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    Order,
    OrderSide,
    OrderStatus,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.symbol_mapper import SymbolMapper  # IMPORT IS PRESENT
from cyberdelta.utils.parsing import parse_datetime_utc

logger = get_logger(__name__)

# Temporarily define ExchangeType, Symbol as str TypeAlias to unblock linter
# TODO: Find or create the canonical definitions for these types
type Symbol = str
type ExchangeType = str


class PortfolioTracker:
    """Track and manage the current state of the portfolio.

    Responsible for:
    - Tracking balances across exchanges
    - Monitoring open positions and their P&L
    - Tracking order status
    - Calculating exposure metrics
    - Maintaining portfolio state
    - Performing reconciliation with exchange data
    """

    def __init__(
        self,
        app_settings: AppSettings,
        pt_config: PortfolioTrackerConfig,
        api_clients: dict[str, ExchangeAPI] | None = None,
        exchange_factories: dict[ExchangeType, Callable[..., ExchangeAPI]] | None = None,
        symbol_mapper: SymbolMapper | None = None,  # ADDED PARAMETER
    ) -> None:
        """Initialize the portfolio tracker.

        Args:
            app_settings: Application configuration
            pt_config: Portfolio tracker configuration
            api_clients: Dictionary of exchange API clients
            exchange_factories: Dictionary of exchange API factory functions
            symbol_mapper: Symbol mapper instance # ADDED DOC

        """
        self.logger = get_logger(__name__ + "." + self.__class__.__name__)
        self.app_settings: AppSettings = app_settings
        self.api_clients: dict[str, ExchangeAPI] = api_clients or {}
        self.exchange_factories = exchange_factories or {}
        self.symbol_mapper: SymbolMapper | None = symbol_mapper  # STORE AS SELF.SYMBOL_MAPPER
        self._lock = asyncio.Lock()

        # Internal state
        # Balances: dict[exchange_id, dict[asset_symbol, SpotBalance]]
        self.balances: defaultdict[str, defaultdict[str, SpotBalance]] = defaultdict(
            lambda: defaultdict(
                # Ensure a default SpotBalance that makes sense if an asset is queried
                # before it's set
                lambda: SpotBalance(
                    exchange="",  # Should be overridden or considered invalid
                    asset="",  # Should be overridden or considered invalid
                    total_quantity=Decimal(0),
                    available_quantity=Decimal(0),
                    timestamp=datetime.min.replace(tzinfo=UTC),  # Clearly old timestamp
                ),
            ),
        )
        # Positions: dict[exchange_id, dict[symbol, DerivativePosition]]
        self.positions: defaultdict[str, defaultdict[str, DerivativePosition]] = defaultdict(
            lambda: defaultdict(
                # Placeholder for a non-existent position
                lambda: DerivativePosition(
                    exchange="",
                    symbol="",
                    side=OrderSide.BUY,  # Default for a zero-size position; arbitrary
                    size=Decimal(0),
                    entry_price=None,  # Must be None if size is 0
                    timestamp=datetime.min.replace(tzinfo=UTC),
                ),
            ),
        )
        self.orders: defaultdict[str, dict[str, Order]] = defaultdict(
            dict,
        )  # client_order_id -> Order

        # Initialize last update times
        # Consolidate individual last update times
        self.last_update_time: defaultdict[str, datetime] = defaultdict(
            lambda: datetime.min.replace(tzinfo=UTC),
        )
        self.last_reconciliation_time: defaultdict[str, datetime] = defaultdict(
            lambda: datetime.min.replace(tzinfo=UTC),
        )
        # Removed individual _last_balance_update_times, _last_position_update_times, etc.

        self.tickers: dict[str, Ticker] = {}

        # Use the passed portfolio tracker config
        self.pt_config = pt_config

        self._initialize_from_config()

        # Reconciliation interval (5 minutes by default)
        self.reconciliation_interval = 300  # seconds

        # Initialize data structures
        self._initialize_data_structures()

        # Initialize high watermark (as Decimal)
        self.high_watermark: Decimal = Decimal(
            "0.0",
        )  # Track highest portfolio value for drawdown calculation

        # Add internal tracking for realized PNL
        self.realized_pnl: Decimal = Decimal("0.0")  # Track realized PNL

        # Add active symbols and watchlist
        self.active_symbols: set[str] = set()
        self.watchlist: set[str] = set()

    def _initialize_data_structures(self) -> None:
        """Initialize data structures for all configured exchanges."""
        for exchange_id in self.app_settings.exchanges.keys():
            if not self.app_settings.exchanges[exchange_id].enabled:
                continue

            # Ensure last_update_time and last_reconciliation_time have initial entries
            # if not already set by defaultdict lambda (which they are)
            _ = self.last_update_time[exchange_id]
            _ = self.last_reconciliation_time[exchange_id]

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier
            client: ExchangeAPI implementation

        """
        self.api_clients[exchange_id] = client
        self.logger.info(
            f"PT_REGISTER: Registered API client for {exchange_id}. \
            Current api_clients keys: {list(self.api_clients.keys())}",
        )

    async def initialize(self) -> None:
        """Initialize portfolio state from exchanges."""
        initialization_tasks: list[Awaitable[Any]] = []
        logger.info(
            f"PortfolioTracker {id(self)}: About to gather init tasks. "
            f"Balance dict: {self.balances}",
        )
        for exchange_id, client in self.api_clients.items():
            if not self.app_settings.exchanges[exchange_id].enabled:
                continue
            initialization_tasks.append(self._fetch_exchange_account_summary(client, exchange_id))
            initialization_tasks.append(self._fetch_exchange_balances(exchange_id))
            initialization_tasks.append(self._fetch_exchange_positions(exchange_id))
            initialization_tasks.append(self._fetch_exchange_orders(exchange_id))
        logger.info(
            f"PortfolioTracker {id(self)}: About to gather init tasks. "
            f"Balances before: {self.balances}",
        )
        results: list[bool | BaseException] = await asyncio.gather(
            *initialization_tasks,
            return_exceptions=True,
        )
        logger.info(f"---> State of self.balances immediately after init gather: {self.balances}")
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
                f"Cannot proceed reliably. Errors: {error_summary}",
            )

        # Set initial reconciliation time AFTER fetching
        now_utc = datetime.now(UTC)
        logger.info(
            f"PT INIT: Setting reconciliation times. "
            f"Current self.last_reconciliation_time before: {self.last_reconciliation_time}",
        )
        for exchange_id in self.api_clients:
            self.last_reconciliation_time[exchange_id] = now_utc
        logger.info(
            f"PT INIT: Reconciliation times set. "
            f"Current self.last_reconciliation_time after: {self.last_reconciliation_time}",
        )

        # Calculate initial portfolio capital and set high watermark
        initial_capital = await self.get_total_capital()
        self.high_watermark = initial_capital
        if initial_capital > Decimal("0.0"):
            logger.info(f"Initial high watermark set to: {self.high_watermark}")
        else:
            logger.warning(f"Initial capital is {initial_capital}. High watermark not set.")
        logger.info("Portfolio state initialized")

    async def _fetch_exchange_balances(self, exchange_id: str) -> bool:
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"No API client found for {exchange_id} in fetch_balances")
            return False
        try:
            balances_data = await client.get_balances()  # Returns dict[str, SpotBalance]

            updated_balances: dict[str, SpotBalance] = {}

            # If balances_data is empty dict
            if not balances_data:  # Empty dict received
                logger.info(f"[{exchange_id}] API returned an empty dictionary of balances.")
            else:
                for asset, balance_obj in balances_data.items():
                    if balance_obj.exchange == exchange_id:
                        updated_balances[asset] = balance_obj
                    else:
                        logger.warning(
                            f"[{exchange_id}] Skipping balance for asset {asset} "
                            f"due to mismatched exchange ID ({balance_obj.exchange}) "
                            f"in received SpotBalance object (from dict).",
                        )

            # Update internal state if new valid balances were found
            if updated_balances:
                async with self._lock:
                    current_assets_for_exchange = set(self.balances[exchange_id].keys())
                    newly_updated_asset_symbols = set(updated_balances.keys())

                    assets_to_remove = current_assets_for_exchange - newly_updated_asset_symbols
                    for asset_to_remove in assets_to_remove:
                        # Ensure asset actually exists before trying to delete to avoid
                        # KeyError if logic is imperfect
                        # Check against the inner dict for the specific exchange_id
                        if asset_to_remove in self.balances[exchange_id]:
                            del self.balances[exchange_id][asset_to_remove]
                            logger.debug(
                                f"[{exchange_id}] Removed stale balance "
                                f"for asset {asset_to_remove}.",
                            )

                    # Now, add/update balances from updated_balances
                    for asset, balance in updated_balances.items():
                        self.balances[exchange_id][asset] = balance  # Corrected access
                    self.last_update_time[exchange_id] = datetime.now(UTC)
                logger.info(
                    f"[FETCH_BALANCES:{exchange_id}] Balances updated "
                    f"successfully with {len(updated_balances)} items. "
                    f"Assets removed: {len(assets_to_remove)}.",
                )
            elif not updated_balances:
                # This implies that balances_data was not None,
                # and if it was a list or dict, it was empty,
                # and no balances were processed into updated_balances.
                # If balances_data was some other unexpected type,
                # it would have been caught by an earlier `else`
                # or the initial type hint for `client.get_balances()` would be violated.
                logger.warning(
                    f"[FETCH_BALANCES:{exchange_id}] No valid balances processed or "
                    f"API returned empty data. Type: {type(balances_data)}",
                )
                # Not returning False here, as an empty (but valid) response is not an error.

            return True

        except Exception as e:
            logger.exception(f"[FETCH_BALANCES:{exchange_id}] Error during balance fetch: {e}")
            return False

    def _parse_balance_info(
        self,
        exchange_id: str,
        asset: str,
        balance_info: dict[str, Any] | SpotBalance,
    ) -> SpotBalance | None:
        """Parse balance information into a SpotBalance object."""
        if isinstance(balance_info, SpotBalance):
            if balance_info.exchange != exchange_id:
                logger.error(
                    f"Mismatched exchange ID in provided SpotBalance object: expected "
                    f"{exchange_id}, got {balance_info.exchange}",
                )
                return None
            return balance_info

        # Since the type hint is dict[str, Any] | SpotBalance, and SpotBalance is handled above,
        # balance_info must be a dict here.

        # Construct the data dictionary for SpotBalance, adding the exchange
        balance_data = balance_info.copy()
        balance_data["exchange"] = exchange_id
        balance_data["asset"] = asset  # Ensure asset is explicitly set

        try:
            # Validate and create the SpotBalance object
            # Pydantic handles parsing 'total', 'available' from str/int/float via validators
            parsed = SpotBalance(**balance_data)
            # Additional runtime checks (redundant with Pydantic ge=0 but defensive)
            if parsed.total_quantity < Decimal("0") or parsed.available_quantity < Decimal("0"):
                logger.error(
                    f"Parsed balance has negative values: Total={parsed.total_quantity}, "
                    f"Available={parsed.available_quantity}",
                )
                return None
            return parsed
        except (ValidationError, TypeError, InvalidOperation) as e:
            logger.error(
                f"Failed to parse balance for {asset} on {exchange_id}: {e}",
                exc_info=True,
            )
            return None

    @staticmethod
    def _safe_decimal_convert(
        value: str | Decimal | int | float | None,
        field_name: str,
        asset: str,
        exchange_id: str,
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
                f"(type: {type(value)}) to Decimal for {asset} on {exchange_id}: {e}",
            )
            return None

    async def _fetch_exchange_positions(self, exchange_id: str) -> bool:
        """Fetch and update positions for a specific exchange."""
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"No API client registered for {exchange_id} in fetch_positions")
            return False
        try:
            positions_data_raw = await client.get_positions()  # -> list[DerivativePosition]

            # positions_data_raw is guaranteed to be a list by the ExchangeAPI interface
            processed_positions_list = positions_data_raw

            updated_positions: dict[str, DerivativePosition] = {}
            for position_info in processed_positions_list:
                # Use symbol as the key for now
                # TODO: Revisit position identification strategy
                pos_key = position_info.symbol
                if pos_key:
                    # Assume position_info is already a validated DerivativePosition
                    # No need for _safe_decimal_convert if API layer provides validated models
                    updated_positions[pos_key] = position_info
                else:
                    logger.warning(
                        f"Skipping DerivativePosition object without symbol on "
                        f"{exchange_id}: {position_info}",
                    )

            # This completely replaces the inner dict for the exchange_id
            self.positions[exchange_id] = defaultdict(
                lambda: DerivativePosition(
                    exchange="",
                    symbol="",
                    side=OrderSide.BUY,
                    size=Decimal(0),
                    entry_price=None,
                    timestamp=datetime.min.replace(tzinfo=UTC),
                ),
                updated_positions,
            )
            self.last_update_time[exchange_id] = datetime.now(UTC)
            logger.debug(
                f"Successfully updated positions for {exchange_id}. Count: {len(updated_positions)}",
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
            # ExchangeAPI.get_open_orders returns list[Order], Pyright should infer this.
            orders_data: list[Order] = await client.get_open_orders()
            updated_orders: dict[str, Order] = {}
            for order_info in orders_data:
                order_instance = None
                order_id = None
                # order_info is always Order
                order_instance = order_info
                order_id = order_instance.client_order_id
                if order_instance and order_id:
                    order_instance.price = self._safe_decimal_convert(
                        order_instance.price,
                        "price",
                        order_instance.symbol,
                        exchange_id,
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
                                f"order {order_id}",
                            )
                            order_instance.status = OrderStatus.UNKNOWN
                    updated_orders[str(order_id)] = order_instance
            self.orders[exchange_id] = updated_orders  # This replaces the inner dict
            self.last_update_time[exchange_id] = datetime.now(UTC)
            logger.debug(
                f"Successfully updated open orders for {exchange_id}. Count: {len(updated_orders)}",
            )
            return True
        except Exception as e:
            logger.exception(f"Unexpected error fetching orders for {exchange_id}: {e}")
            return False

    async def update(self) -> None:
        """Update portfolio state by fetching data from exchanges."""
        now = datetime.now(UTC)
        update_tasks: list[Awaitable[Any]] = []
        for exchange_id, client in self.api_clients.items():
            if not self.app_settings.exchanges[exchange_id].enabled:
                continue
            last_reconciliation = self.last_reconciliation_time.get(
                exchange_id,
                datetime.min.replace(tzinfo=UTC),
            )
            needs_reconciliation = (
                now - last_reconciliation
            ).total_seconds() >= self.reconciliation_interval
            if needs_reconciliation:
                logger.info(f"Reconciliation needed for {exchange_id}. Fetching all data.")
                update_tasks.append(self._fetch_exchange_account_summary(client, exchange_id))
                update_tasks.append(self._fetch_exchange_balances(exchange_id))
                update_tasks.append(self._fetch_exchange_positions(exchange_id))
                update_tasks.append(self._fetch_exchange_orders(exchange_id))
                self.last_reconciliation_time[exchange_id] = now
            else:
                logger.debug(f"Fetching only orders for {exchange_id} (no reconciliation needed).")
                update_tasks.append(self._fetch_exchange_orders(exchange_id))
        if update_tasks:
            results: list[bool | BaseException] = await asyncio.gather(
                *update_tasks,
                return_exceptions=True,
            )
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        f"Error during portfolio update task (index {i}): {result}",
                        exc_info=result,
                    )
                elif result is False:
                    logger.warning(
                        f"Portfolio update task (index {i}) indicated failure (returned False).",
                    )
        current_capital = await self.get_total_capital()
        self.high_watermark = max(self.high_watermark, current_capital)

    def update_order(self, exchange_id: str, order: Order) -> None:
        """Update the status of a single order.

        Args:
            exchange_id: Exchange identifier
            order: Order object with updated information

        """
        # self.orders is defaultdict(dict), so exchange_id will be auto-created if missing
        if not order.client_order_id:
            logger.error(f"Received order update without client_order_id on {exchange_id}: {order}")
            return
        order_id_str = str(order.client_order_id)
        self.orders[exchange_id][order_id_str] = order
        self.last_update_time[exchange_id] = datetime.now(UTC)  # Mark update for this exchange

        # Handle order state transitions
        # In particular, we want to detect when an order reaches FILLED or PARTIALLY_FILLED
        # and generate a corresponding trade for the position tracker
        if order.status in [
            OrderStatus.FILLED,
            OrderStatus.PARTIALLY_FILLED,
        ] and order.quantity_filled > Decimal("0"):
            logger.info(
                f"Order {order_id_str} on {exchange_id} is {order.status}. "
                f"Triggering trade processing (placeholder).",
            )
            # self.process_trade(exchange_id, Trade(...)) # Requires Trade object creation logic

    def update_position(self, exchange_id: str, position: DerivativePosition) -> None:
        """Update the state of a single position.

        Args:
            exchange_id: Exchange identifier
            position: DerivativePosition object with updated information

        """
        # self.positions is defaultdict(defaultdict), so exchange_id will be auto-created
        pos_key = position.symbol
        if not pos_key:
            logger.error(f"Received position update without symbol on {exchange_id}: {position}")
            return

        self.positions[exchange_id][pos_key] = position
        self.last_update_time[exchange_id] = datetime.now(UTC)
        logger.debug(f"Updated position {pos_key} for {exchange_id}")

    async def process_trade(self, exchange_id: str, trade: Trade) -> None:
        """Process a trade execution and update relevant portfolio state.
        - Updates positions.
        - Updates balances (placeholder, needs full implementation).
        - Updates realized P&L.

        Args:
            exchange_id: The exchange where the trade occurred.
            trade: The Trade object.

        """
        if not trade or not trade.symbol or not trade.quantity or trade.quantity <= Decimal(0):
            logger.warning(f"Ignoring invalid or zero-quantity trade on {exchange_id}: {trade}")
            return

        # Ensure symbol_mapper is available
        if not hasattr(self, "symbol_mapper") or self.symbol_mapper is None:
            logger.error("SymbolMapper not initialized in PortfolioTracker. Cannot process trade.")
            # Attempt to get it from config if possible, or raise
            # This indicates a setup issue if self.symbol_mapper is None.
            # For now, proceed with a basic fallback for base_symbol if absolutely necessary,
            # but this should be fixed by ensuring proper initialization.
            base_symbol = trade.symbol.split("-")[0].split("/")[0]  # Basic fallback
            logger.warning(
                f"SymbolMapper missing, using basic fallback for base symbol: {base_symbol}",
            )
        else:
            base_symbol = (
                self.symbol_mapper.get_internal_symbol(exchange_id, trade.symbol)
                or trade.symbol.split("-")[0].split("/")[0]
            )

        logger.info(
            f"Processing trade on {exchange_id}: {trade.side} {trade.quantity} "
            f"{base_symbol} (from {trade.symbol}) @ {trade.price}",
        )

        async def _update_position_async() -> None:
            async with self._lock:
                current_position = self.positions[exchange_id].get(base_symbol)
                trade_price = trade.price
                trade_quantity = trade.quantity
                trade_side = trade.side
                trade_timestamp = trade.executed_at  # USE executed_at

                realized_pnl_for_this_trade = Decimal("0")

                if not current_position or current_position.size == Decimal(0):
                    # Opening new position
                    logger.debug(
                        f"Opening new position for {base_symbol} on {exchange_id} "
                        f"from trade {trade.id}",
                    )
                    new_pos = DerivativePosition(
                        exchange=exchange_id,
                        symbol=base_symbol,
                        side=trade_side,
                        size=trade_quantity,
                        entry_price=trade_price,
                        timestamp=trade_timestamp,  # Uses corrected trade_timestamp
                        mark_price=trade_price,  # Initial mark price
                        # last_trade_id=trade.id # REMOVED
                    )
                    self.positions[exchange_id][base_symbol] = new_pos
                    self.active_symbols.add(base_symbol)
                else:
                    # Modifying existing position
                    logger.debug(
                        f"Modifying existing position for {base_symbol} on {exchange_id} "
                        f"from trade {trade.id}. Current size: {current_position.size}, "
                        f"side: {current_position.side}",
                    )
                    current_entry_price = current_position.entry_price or Decimal(
                        0,
                    )  # Default if somehow None

                    if current_position.side == trade_side:
                        # Increasing position
                        logger.debug(
                            f"Increasing position for {base_symbol}. Trade qty: {trade_quantity}",
                        )
                        if (
                            current_position.size < Decimal(0) and trade_side == OrderSide.SELL
                        ):  # Increasing short
                            new_avg_price = (
                                (abs(current_position.size) * current_entry_price)
                                + (trade_quantity * trade_price)
                            ) / (abs(current_position.size) + trade_quantity)
                            current_position.size -= trade_quantity
                        elif (
                            current_position.size > Decimal(0) and trade_side == OrderSide.BUY
                        ):  # Increasing long
                            new_avg_price = (
                                (current_position.size * current_entry_price)
                                + (trade_quantity * trade_price)
                            ) / (current_position.size + trade_quantity)
                            current_position.size += trade_quantity
                        else:  # This is the correct fallback else for the increasing position logic
                            logger.error(
                                f"Logical error in increasing position {base_symbol}. "
                                f"Current size {current_position.size}, "
                                f"side {current_position.side}. "
                                f"Trade qty {trade_quantity}, side {trade_side}",
                            )
                            new_avg_price = trade_price  # Fallback

                        current_position.entry_price = new_avg_price
                        logger.debug(
                            f"Position for {base_symbol} increased. New size: "
                            f"{current_position.size}, "
                            f"New avg entry: {current_position.entry_price}",
                        )

                    else:
                        # Decreasing or flipping position
                        logger.debug(
                            f"Reducing or flipping position for {base_symbol}. "
                            f"Trade qty: {trade_quantity}",
                        )

                        # Calculate PNL on the portion of the position affected by this trade
                        # PNL = (Exit Price - Entry Price) * Quantity
                        # For short positions, PNL = (Entry Price - Exit Price) * Quantity
                        qty_affected = min(trade_quantity, abs(current_position.size))

                        if current_entry_price != Decimal(0):  # Avoid PNL calc if entry was 0
                            if current_position.side == OrderSide.BUY:  # Closing/reducing a long
                                realized_pnl_for_this_trade = (
                                    trade_price - current_entry_price
                                ) * qty_affected
                            else:  # Closing/reducing a short
                                realized_pnl_for_this_trade = (
                                    current_entry_price - trade_price
                                ) * qty_affected

                            self._update_realized_pnl(
                                realized_pnl_for_this_trade,
                            )  # Update portfolio total PNL
                            current_position.realized_pnl = (
                                current_position.realized_pnl or Decimal(0)
                            ) + realized_pnl_for_this_trade
                            logger.info(
                                f"Trade {trade.id} for {base_symbol}: "
                                f"Realized PNL {realized_pnl_for_this_trade:.4f}. "
                                f"Position Realized PNL: {current_position.realized_pnl:.4f}",
                            )

                        if trade_quantity < abs(current_position.size):
                            # Reducing position, not closing or flipping
                            logger.debug(
                                f"Reducing position for {base_symbol}. New size: "
                                f"{
                                    current_position.size - qty_affected
                                    if current_position.side == OrderSide.BUY
                                    else current_position.size + qty_affected
                                }",
                            )
                            if current_position.side == OrderSide.BUY:
                                current_position.size -= trade_quantity
                            else:  # SELL side
                                current_position.size += trade_quantity
                            # Entry price remains the same

                        elif trade_quantity == abs(current_position.size):
                            # Closing position to flat
                            logger.debug(f"Closing position for {base_symbol} to flat.")
                            current_position.size = Decimal(0)
                            current_position.entry_price = None  # Flat position has no entry price
                            # current_position.side can be left as is or set to a default, e.g., BUY
                            self.active_symbols.discard(base_symbol)  # Symbol might become inactive

                        else:  # Flipping position (trade_quantity > abs(current_position.size))
                            remaining_qty = trade_quantity - abs(current_position.size)
                            logger.debug(
                                f"Flipping position for {base_symbol}. "
                                f"Remaining qty after closing: {remaining_qty}",
                            )
                            current_position.size = (
                                remaining_qty if trade_side == OrderSide.BUY else -remaining_qty
                            )
                            current_position.side = trade_side
                            current_position.entry_price = (
                                trade_price  # Entry price for the new portion
                            )
                            # PNL for the closed portion already calculated above

                    current_position.timestamp = trade_timestamp
                    current_position.mark_price = (
                        trade_price  # Update mark price to last trade price
                    )
                    # current_position.last_trade_id = trade.id # Ensure this is removed/commented
                    self.positions[exchange_id][base_symbol] = current_position

        await _update_position_async()

        # Placeholder: Balance update logic
        self._update_balances_from_trade(exchange_id, trade)
        logger.debug(f"Placeholder: Update balances for trade {trade.id} on {exchange_id}")

    def _update_balances_from_trade(self, exchange_id: str, trade: Trade) -> None:
        # Placeholder for balance update logic based on trade details
        logger.debug(f"Placeholder: Update balances for trade {trade.id} on {exchange_id}")
        pass

    def _update_realized_pnl(self, amount: Decimal) -> None:
        """Update the total realized PNL."""
        if not amount.is_finite():  # Check finiteness directly
            logger.error(f"Attempted to update realized PNL with invalid amount: {amount}")
            return
        self.realized_pnl += amount
        logger.info(f"Realized PNL updated by {amount:.4f}. New total: {self.realized_pnl:.4f}")

    # --- Position Access Methods ---
    def get_position(self, exchange_id: str, symbol: str) -> DerivativePosition | None:
        """Return the position for a specific symbol on a specific exchange."""
        if exchange_id not in self.positions:
            return None
        position = self.positions[exchange_id].get(symbol)  # Direct access to inner dict
        return position

    def get_positions_by_symbol(self, exchange_id: str, symbol: str) -> list[DerivativePosition]:
        """Retrieve all positions for a specific symbol on a given exchange."""
        if exchange_id not in self.positions:
            return []
        exchange_positions = self.positions[exchange_id]  # Direct access to inner dict
        # Iterate over values() as key is unused
        return [pos for pos in exchange_positions.values() if pos.symbol == symbol]

    def get_all_positions(self) -> Sequence[tuple[str, DerivativePosition]]:
        """Retrieve all derivative positions across all exchanges.
        Conforms to PortfolioTrackerProtocol
        (DerivativePosition implements Position protocol implicitly).
        """
        all_positions_list: list[tuple[str, DerivativePosition]] = []
        for exchange_id, symbol_positions_map in self.positions.items():
            for _symbol, position_obj in symbol_positions_map.items():
                # Only include positions with non-zero size
                if position_obj.size != Decimal(0):
                    all_positions_list.append((exchange_id, position_obj))
        return all_positions_list

    def get_positions_by_exchange(self, exchange_id: str) -> list[DerivativePosition]:
        """Get all positions for a specific exchange."""
        if exchange_id not in self.positions:
            logger.warning(f"Attempted to get positions for unknown exchange: {exchange_id}")
            return []
        # Filter out placeholders
        return [pos for pos in self.positions[exchange_id].values() if pos.size != Decimal(0)]

    async def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        """Calculates the total portfolio capital in the specified base currency."""
        logger.debug(f"Calculating total capital in {base_currency}...")
        total_value = Decimal("0.0")

        # 1. Calculate value of all spot balances
        for exchange_id, balances in self.balances.items():
            for asset, balance in balances.items():
                # Defensive check for zero quantity
                # (is None check removed as total_quantity is not Optional)
                if balance.total_quantity == Decimal("0"):
                    continue

                price = await self._get_asset_price_in_base(exchange_id, asset, base_currency)
                if price is not None:
                    try:
                        asset_value = balance.total_quantity * price
                        total_value += asset_value
                        logger.debug(
                            f"  [{exchange_id}] Spot Balance: {asset} {balance.total_quantity} "
                            f"@ {price} {base_currency} = {asset_value} {base_currency}",
                        )
                    except (TypeError, InvalidOperation) as e:
                        logger.error(
                            f"Error calculating value for spot balance {asset} "
                            f"on {exchange_id}: {e}",
                        )
                else:
                    logger.warning(
                        f"Could not determine price for spot asset {asset} "
                        f"on {exchange_id} in {base_currency}. "
                        f"Skipping in capital calculation.",
                    )

        # 2. Calculate the equity value of all derivative positions
        # This is simplified: Assumes margin is held in base_currency and PnL reflects value.
        # A more accurate calculation might need margin details per exchange.
        # We add unrealized PnL here as a proxy for position value change.
        _realized, unrealized = await self.get_pnl(base_currency)
        total_value += unrealized
        logger.debug(f"  Adding total unrealized PNL to capital: {unrealized} {base_currency}")

        # Update high watermark
        if total_value.is_finite() and total_value > self.high_watermark:
            self.high_watermark = total_value
            logger.debug(f"New high watermark reached: {self.high_watermark}")

        logger.info(f"Total portfolio capital calculated: {total_value} {base_currency}")
        return total_value if total_value.is_finite() else Decimal("0.0")

    async def get_exchange_exposure(
        self,
        exchange_id: str,
        valuation_asset: str = "USDC",
    ) -> Decimal:
        """Calculate the total market exposure for a given exchange in a valuation asset."""
        logger.debug(f"Calculating exposure for {exchange_id} in {valuation_asset}...")
        exchange_exposure = Decimal("0.0")
        positions = self.positions[exchange_id]  # Direct access to inner dict

        for position_key, position in positions.items():
            # Defensive checks (is None checks removed as size/symbol are not Optional)
            if position.size == Decimal("0"):
                continue  # Skip zero size positions

            # Get current market price
            mark_price = await self._get_asset_price_in_base(
                exchange_id,
                position.symbol,
                valuation_asset,
            )

            if mark_price is not None and mark_price.is_finite():
                try:
                    # Use absolute value of size for exposure calculation
                    position_value = abs(position.size) * mark_price
                    exchange_exposure += position_value
                    logger.debug(
                        f"  [{exchange_id}] Position Exposure: {position.symbol} "
                        f"size {position.size} @ mark {mark_price} {valuation_asset} = "
                        f"{position_value} {valuation_asset}",
                    )
                except (TypeError, InvalidOperation) as e:
                    logger.error(
                        f"Error calculating value for position {position_key} "
                        f"({position.symbol}) on {exchange_id}: {e}",
                    )
            else:
                logger.warning(
                    f"Could not determine mark price for position {position_key} "
                    f"({position.symbol}) on {exchange_id} in {valuation_asset}. "
                    f"Skipping in exposure calculation.",
                )

        logger.info(f"Total exposure for {exchange_id}: {exchange_exposure} {valuation_asset}")
        return exchange_exposure if exchange_exposure.is_finite() else Decimal("0.0")

    async def get_total_exposure_usd(self, valuation_asset: str = "USDC") -> Decimal:
        """Calculate the total market exposure across all exchanges."""
        logger.debug(f"Calculating total exposure across all exchanges in {valuation_asset}...")
        total_exposure = Decimal("0.0")
        for exchange_id in self.api_clients.keys():
            if self.app_settings.exchanges[exchange_id].enabled:
                total_exposure += await self.get_exchange_exposure(exchange_id, valuation_asset)

        logger.info(f"Total portfolio exposure calculated: {total_exposure} {valuation_asset}")
        return total_exposure if total_exposure.is_finite() else Decimal("0.0")

    async def get_pnl(self, base_currency: str = "USDC") -> tuple[Decimal, Decimal]:
        """Calculate the total realized and unrealized PNL across all exchanges.

        Args:
            base_currency: The currency to report PNL in.

        Returns:
            A tuple containing (total_realized_pnl, total_unrealized_pnl).

        """
        logger.debug(f"Calculating PNL in {base_currency}...")
        total_unrealized_pnl = Decimal("0.0")
        total_realized_pnl = self.realized_pnl  # Start with globally tracked realized PNL

        for exchange_id, positions in self.positions.items():
            for position_key, position in positions.items():
                # Add position's own realized PNL if it's valid
                # Re-adding None check for safety, along with finiteness
                if position.realized_pnl is not None and position.realized_pnl.is_finite():
                    pnl_quote_asset = (
                        position.symbol.split("_")[-1]
                        if "_" in position.symbol
                        else position.symbol.split("-")[-1]
                    )  # Simple guess
                    conversion_rate = await self._get_asset_price_in_base(
                        exchange_id,
                        pnl_quote_asset,
                        base_currency,
                    )

                    # Check conversion rate validity
                    if conversion_rate is not None and conversion_rate.is_finite():
                        try:
                            # Ensure we only add Decimal to Decimal
                            converted_pnl = position.realized_pnl * conversion_rate
                            if converted_pnl.is_finite():  # Final check before adding
                                total_realized_pnl += converted_pnl
                            else:
                                logger.warning(
                                    f"Converted realized PNL for {position_key} "
                                    f"is not finite ({converted_pnl}). Skipping addition.",
                                )
                        except (TypeError, InvalidOperation) as e:
                            logger.error(
                                f"Error during realized PNL conversion/addition "
                                f"for {position_key}: {e}",
                            )
                    else:
                        logger.warning(
                            f"Cannot convert realized PNL for {position_key} on {exchange_id} "
                            f"to {base_currency}. Realized PNL: {position.realized_pnl}, "
                            f"Conversion Rate: {conversion_rate}.",
                        )
                elif position.realized_pnl is not None:  # Log if it exists but isn't finite
                    logger.warning(
                        f"Position {position_key} realized PNL is not finite: "
                        f"{position.realized_pnl}",
                    )

                # Calculate unrealized PNL
                # DEFENSIVE CHECK: Check entry_price is not None *before* size check
                # because a non-zero size *requires* a non-None entry_price (model validation)
                if position.size == Decimal("0") or position.entry_price is None:
                    logger.debug(
                        f"Skipping unrealized PNL calc for {position_key} on {exchange_id} "
                        f"due to zero size or missing entry price.",
                    )
                    continue

                # 1. Get Mark Price in the requested Base Currency
                mark_price_in_base = await self._get_asset_price_in_base(
                    exchange_id,
                    position.symbol,
                    base_currency,
                )

                # 2. Get Entry Price (which is in the Quote currency of the symbol)
                entry_price_in_quote = position.entry_price

                # 3. Determine the Quote Currency from the symbol
                quote_currency = None
                if "-" in position.symbol:
                    quote_currency = position.symbol.split("-")[-1]
                elif "_" in position.symbol:
                    quote_currency = position.symbol.split("_")[-1]
                else:
                    logger.warning(
                        f"Cannot determine quote currency for symbol {position.symbol}, "
                        f"cannot calculate unrealized PNL accurately.",
                    )
                    continue  # Skip if we can't determine quote currency

                # 4. Convert Entry Price from Quote Currency to Base Currency
                entry_price_in_base: Decimal | None
                if quote_currency == base_currency:
                    entry_price_in_base = entry_price_in_quote
                else:
                    # Need conversion rate from quote to base
                    quote_to_base_rate = await self._get_asset_price_in_base(
                        exchange_id,
                        quote_currency,
                        base_currency,
                    )
                    if quote_to_base_rate is not None:
                        entry_price_in_base = entry_price_in_quote * quote_to_base_rate
                    else:
                        logger.warning(
                            f"Cannot convert entry price for {position.symbol} "
                            f"from {quote_currency} to {base_currency}. "
                            f"Skipping unrealized PNL.",
                        )
                        entry_price_in_base = None  # Explicitly set to None if conversion fails

                # 5. Calculate Unrealized PNL if possible
                # DEFENSIVE CHECK: Add explicit None checks for mark_price_in_base
                # and entry_price_in_base
                if (
                    mark_price_in_base is not None
                    and entry_price_in_base is not None
                    and mark_price_in_base.is_finite()
                    and entry_price_in_base.is_finite()  # Check finiteness here
                ):
                    try:
                        # Unrealized PNL = Size * (Mark Price in Base - Entry Price in Base)
                        unrealized_pnl = position.size * (mark_price_in_base - entry_price_in_base)
                        total_unrealized_pnl += unrealized_pnl
                        logger.debug(
                            f"  [{exchange_id}] Position PNL: {position.symbol} "
                            f"size {position.size}, entry_base {entry_price_in_base:.4f}, "
                            f"mark_base {mark_price_in_base:.4f} => Unrealized: "
                            f"{unrealized_pnl:.4f} {base_currency}",
                        )

                    except (TypeError, InvalidOperation) as e:
                        logger.error(
                            f"Error calculating unrealized PNL for {position_key} "
                            f"on {exchange_id}: {e}",
                        )
                else:
                    logger.warning(
                        f"Skipping unrealized PNL calculation for {position_key} "
                        f"({position.symbol}) on {exchange_id} due to missing/invalid "
                        f"converted prices (MarkBase={mark_price_in_base}, "
                        f"EntryBase={entry_price_in_base}).",
                    )

        finite_realized = total_realized_pnl if total_realized_pnl.is_finite() else Decimal("0.0")
        finite_unrealized = (
            total_unrealized_pnl if total_unrealized_pnl.is_finite() else Decimal("0.0")
        )

        logger.info(
            f"Total PNL calculated: Realized={finite_realized} {base_currency}, "
            f"Unrealized={finite_unrealized} {base_currency}",
        )
        return finite_realized, finite_unrealized

    async def get_current_drawdown(self, base_currency: str = "USDC") -> Decimal | None:
        """Calculates the current drawdown from the high watermark."""
        current_capital = await self.get_total_capital()

        if self.high_watermark <= Decimal("0.0"):
            logger.warning("High watermark is not positive. Cannot calculate drawdown.")
            return Decimal("0.0")

        if not current_capital.is_finite() or current_capital <= Decimal("0.0"):
            logger.warning(
                f"Current capital ({current_capital}) is not positive or finite. "
                f"Cannot calculate drawdown.",
            )
            return Decimal("0.0")

        drawdown = (self.high_watermark - current_capital) / self.high_watermark
        result = max(Decimal("0.0"), drawdown)
        logger.debug(
            f"Calculated Drawdown: HWM={self.high_watermark}, "
            f"Capital={current_capital}, Drawdown={result}",
        )
        return result

    # --- Order Access Methods ---

    def get_open_orders(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """Get all open orders for a given exchange and optionally a symbol."""
        if exchange_id not in self.orders:
            return []

        exchange_orders_dict = self.orders[exchange_id]

        open_orders_collected: list[Order] = []
        for order in exchange_orders_dict.values():
            is_status_open = order.status.is_open()
            is_symbol_match = symbol is None or order.symbol == symbol

            if is_status_open and is_symbol_match:
                open_orders_collected.append(order)

        return open_orders_collected

    def get_order_history(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """Get all orders (open and closed) for a given exchange and optionally a symbol."""
        all_orders: list[Order] = []
        exchange_orders = self.orders.get(exchange_id, {})
        for order in exchange_orders.values():
            if symbol is None or order.symbol == symbol:
                all_orders.append(order)
        return all_orders

    def get_order_by_id(self, exchange_id: str, order_id: str) -> Order | None:
        """Retrieve a specific order by its ID from the internal tracking.

        Args:
            exchange_id: The exchange the order belongs to.
            order_id: The unique identifier of the order.

        Returns:
            The Order object if found, otherwise None.

        """
        exchange_orders = self.orders.get(exchange_id, {})

        # First, try direct lookup using the provided order_id as a client_order_id
        order = exchange_orders.get(order_id)
        if order:
            return order

        # If not found, search for the order_id in the client_order_id format
        for client_order_id, order in exchange_orders.items():
            if client_order_id.endswith(f"-{order_id}"):
                return order

        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the portfolio state to a dictionary suitable for JSON.

        Note: Returns dict containing Pydantic models. Serialization handled by encoder.
        """
        # Convert defaultdicts to dict for serialization if necessary,
        # though Pydantic's default_encoders might handle it.
        # For explicit control:
        balances_dict = {ex: dict(assets) for ex, assets in self.balances.items()}
        positions_dict = {ex: dict(syms) for ex, syms in self.positions.items()}
        orders_dict = {ex: dict(ords) for ex, ords in self.orders.items()}
        last_update_dict = dict(self.last_update_time)
        last_reconciliation_dict = dict(self.last_reconciliation_time)

        return {
            "balances": balances_dict,
            "positions": positions_dict,
            "orders": orders_dict,
            "last_update_time": last_update_dict,
            "last_reconciliation_time": last_reconciliation_dict,
            "high_watermark": self.high_watermark,
            "realized_pnl": self.realized_pnl,
            # Active symbols and watchlist could be added if needed for persistence
            # "active_symbols": list(self.active_symbols),
            # "watchlist": list(self.watchlist),
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        app_settings: AppSettings,
        pt_config: PortfolioTrackerConfig,
    ) -> PortfolioTracker:
        """Deserialize the portfolio state from a dictionary."""
        tracker = cls(app_settings, pt_config)

        balances_data_get = data.get("balances", {})
        if isinstance(balances_data_get, dict):
            balances_data_typed = cast(dict[str, Any], balances_data_get)
            ex_id_str: str
            assets_dict_any: Any
            for ex_id_str, assets_dict_any in balances_data_typed.items():
                if isinstance(assets_dict_any, dict):
                    current_assets_items = cast(dict[str, Any], assets_dict_any)
                    k_asset_raw: str
                    bal_data_any: Any
                    for k_asset_raw, bal_data_any in current_assets_items.items():
                        asset_str = str(k_asset_raw)
                        if isinstance(bal_data_any, dict):
                            try:
                                # Ensure keys are str for model_validate
                                temp_bal_dict_for_comp = cast(dict[str, Any], bal_data_any)
                                validated_bal_dict: dict[str, Any] = {
                                    str(k): v for k, v in temp_bal_dict_for_comp.items()
                                }
                                tracker.balances[ex_id_str][asset_str] = SpotBalance.model_validate(
                                    validated_bal_dict,
                                )
                            except ValidationError as e:
                                logger.error(
                                    f"Error validating SpotBalance for {asset_str} "
                                    f"on {ex_id_str}: {e}",
                                )
                            except Exception as e:  # Catch other potential errors from str(k)
                                logger.warning(
                                    f"Type error processing balance {ex_id_str}/{asset_str} "
                                    f"in from_dict: {e}",
                                )
                        elif isinstance(bal_data_any, SpotBalance):  # If already an object
                            tracker.balances[ex_id_str][asset_str] = bal_data_any

        positions_data_get = data.get("positions", {})
        if isinstance(positions_data_get, dict):
            positions_data_typed = cast(dict[str, Any], positions_data_get)
            ex_id_str_pos: str
            syms_dict_any: Any
            for ex_id_str_pos, syms_dict_any in positions_data_typed.items():
                if isinstance(syms_dict_any, dict):
                    syms_dict_typed = cast(dict[str, Any], syms_dict_any)
                    sym_str: str
                    pos_data_any: Any
                    for sym_str, pos_data_any in syms_dict_typed.items():
                        if isinstance(pos_data_any, dict):
                            try:
                                # Ensure keys are str for model_validate
                                temp_pos_dict_for_comp = cast(dict[str, Any], pos_data_any)
                                validated_pos_dict_for_model: dict[str, Any] = {
                                    str(k): v for k, v in temp_pos_dict_for_comp.items()
                                }
                                tracker.positions[ex_id_str_pos][sym_str] = (
                                    DerivativePosition.model_validate(validated_pos_dict_for_model)
                                )
                            except ValidationError as e:
                                logger.error(
                                    f"Error validating DerivativePosition for {sym_str} "
                                    f"on {ex_id_str_pos}: {e}",
                                )
                            except Exception as e:  # Catch other potential errors from str(k)
                                logger.warning(
                                    f"Type error processing position {ex_id_str_pos}/{sym_str} "
                                    f"in from_dict: {e}",
                                )
                        elif isinstance(pos_data_any, DerivativePosition):
                            tracker.positions[ex_id_str_pos][sym_str] = pos_data_any

        orders_data_get = data.get("orders", {})
        if isinstance(orders_data_get, dict):
            orders_data_typed = cast(dict[str, Any], orders_data_get)
            ex_id_str_ord: str
            ords_dict_any: Any
            for ex_id_str_ord, ords_dict_any in orders_data_typed.items():
                if isinstance(ords_dict_any, dict):
                    ords_dict_typed = cast(dict[str, Any], ords_dict_any)
                    ord_id_str: str
                    order_data_any: Any
                    for ord_id_str, order_data_any in ords_dict_typed.items():
                        if isinstance(order_data_any, dict):
                            try:
                                # Ensure keys are str for model_validate
                                temp_order_dict_for_comp = cast(dict[str, Any], order_data_any)
                                validated_order_dict_for_model: dict[str, Any] = {
                                    str(k): v for k, v in temp_order_dict_for_comp.items()
                                }
                                tracker.orders[ex_id_str_ord][ord_id_str] = Order.model_validate(
                                    validated_order_dict_for_model,
                                )
                            except ValidationError as e:
                                logger.error(
                                    f"Error validating Order for {ord_id_str} "
                                    f"on {ex_id_str_ord}: {e}",
                                )
                            except Exception as e:  # Catch other potential errors from str(k)
                                logger.warning(
                                    f"Type error processing order {ex_id_str_ord}/{ord_id_str} "
                                    f"in from_dict: {e}",
                                )
                        elif isinstance(order_data_any, Order):
                            tracker.orders[ex_id_str_ord][ord_id_str] = order_data_any

        last_update_data_get = data.get("last_update_time", {})
        if isinstance(last_update_data_get, dict):
            last_update_data_typed = cast(dict[str, Any], last_update_data_get)
            ex_id_str_lut: str
            ts_data_any_lut: Any
            for ex_id_str_lut, ts_data_any_lut in last_update_data_typed.items():
                # Assuming ts_data_any_lut is already datetime or a parsable string/timestamp
                try:
                    if isinstance(ts_data_any_lut, datetime):
                        tracker.last_update_time[ex_id_str_lut] = ts_data_any_lut
                    else:  # Attempt parsing
                        # Ensure ts_data_any_lut is not None before passing to parse_datetime_utc
                        if ts_data_any_lut is not None:
                            parsed_ts = parse_datetime_utc(
                                ts_data_any_lut,
                                field_name=f"last_update_time.{ex_id_str_lut}",
                            )
                            if parsed_ts:
                                tracker.last_update_time[ex_id_str_lut] = parsed_ts
                        else:
                            logger.warning(
                                f"Received None for last_update_time for {ex_id_str_lut}, skipping.",
                            )
                except Exception as e:
                    logger.error(f"Error deserializing last_update_time for {ex_id_str_lut}: {e}")

        last_reconciliation_data_get = data.get("last_reconciliation_time", {})
        if isinstance(last_reconciliation_data_get, dict):
            last_reconciliation_data_typed = cast(dict[str, Any], last_reconciliation_data_get)
            ex_id_str_lrt: str
            ts_data_any_lrt: Any
            for ex_id_str_lrt, ts_data_any_lrt in last_reconciliation_data_typed.items():
                try:
                    if isinstance(ts_data_any_lrt, datetime):
                        tracker.last_reconciliation_time[ex_id_str_lrt] = ts_data_any_lrt
                    else:
                        # Ensure ts_data_any_lrt is not None before passing to parse_datetime_utc
                        if ts_data_any_lrt is not None:
                            parsed_ts = parse_datetime_utc(
                                ts_data_any_lrt,
                                field_name=f"last_reconciliation_time.{ex_id_str_lrt}",
                            )
                            if parsed_ts:
                                tracker.last_reconciliation_time[ex_id_str_lrt] = parsed_ts
                        else:
                            logger.warning(
                                f"Received None for last_reconciliation_time for "
                                f"{ex_id_str_lrt}, skipping.",
                            )
                except Exception as e:
                    logger.error(
                        f"Error deserializing last_reconciliation_time for {ex_id_str_lrt}: {e}",
                    )

        tracker.high_watermark = Decimal(str(data.get("high_watermark", "0.0")))
        tracker.realized_pnl = Decimal(str(data.get("realized_pnl", "0.0")))

        logger.info("PortfolioTracker state loaded from dict (object deserialization attempted).")
        return tracker

    # --- Watchlist/Active Symbols ---
    def add_symbol_to_watchlist(self, symbol: str) -> None:
        """Add a symbol to the watchlist."""
        if symbol not in self.watchlist:
            self.watchlist.add(symbol)
            logger.info(f"Added {symbol} to portfolio watchlist.")
            # Potentially trigger subscription logic if needed

    def remove_symbol_from_watchlist(self, symbol: str) -> None:
        """Remove a symbol from the watchlist."""
        if symbol in self.watchlist:
            self.watchlist.remove(symbol)
            logger.info(f"Removed {symbol} from portfolio watchlist.")
            # Potentially trigger unsubscription logic

    def get_watchlist(self) -> set[str]:
        """Get the current set of watched symbols."""
        return self.watchlist.copy()

    def update_active_symbols(self) -> None:
        """Update the set of symbols with active positions or open orders."""
        active: set[str] = set()
        for positions in self.positions.values():
            for pos in positions.values():
                if pos.size != Decimal("0"):
                    active.add(pos.symbol)
        for orders in self.orders.values():
            for order in orders.values():
                if order.status in [
                    OrderStatus.NEW,
                    OrderStatus.OPEN,
                    OrderStatus.PARTIALLY_FILLED,
                ]:
                    active.add(order.symbol)
        self.active_symbols = active
        # logger.debug(f"Active symbols updated: {self.active_symbols}")

    def get_active_symbols(self) -> set[str]:
        """Get the current set of symbols with active positions or orders."""
        # Ensure it's up-to-date before returning
        self.update_active_symbols()
        return self.active_symbols.copy()

    def get_relevant_symbols(self) -> set[str]:
        """Get all symbols relevant to the portfolio (active + watchlist)."""
        self.update_active_symbols()  # Ensure active symbols are current
        return self.active_symbols.union(self.watchlist)

    def _update_balance(self, exchange_id: str, balance: SpotBalance | None) -> None:
        if balance is None:
            return
        self.balances[exchange_id][balance.asset] = balance

    def _parse_positions(
        self,
        exchange_id: str,
        positions_data: list[DerivativePosition] | dict[str, Any],
    ) -> None:
        logger.warning("_parse_positions needs implementation based on API data format.")
        if isinstance(positions_data, dict):
            for _pos_key, _pos_data in positions_data.items():
                # TODO: Parse pos_data dict into DerivativePosition
                pass
        else:  # If it wasn't a dict, it must be a list[DerivativePosition] due to type hint
            for (
                _pos_data
            ) in positions_data:  # positions_data is now known to be list[DerivativePosition]
                # TODO: Parse _pos_data (DerivativePosition object) - likely no parsing needed
                pass
        # No else needed based on type hint

    def _parse_orders(self, exchange_id: str, orders_data: list[Order] | dict[str, Any]) -> None:
        async def _do_parse() -> None:
            async with self._lock:
                current_orders = self.orders.get(exchange_id, {})
                updated_count = 0
                new_count = 0

                items_to_process: list[Order | dict[str, Any]] = []
                if isinstance(orders_data, dict):
                    item_val: Any  # Values from dict[str, Any] are Any
                    for item_val in orders_data.values():
                        if isinstance(item_val, Order):
                            items_to_process.append(item_val)
                        elif isinstance(item_val, dict):
                            # Explicitly cast to the expected dict type for the list
                            items_to_process.append(cast(dict[str, Any], item_val))
                        else:
                            self.logger.warning(
                                f"Skipping unexpected value type in orders_data dict: "
                                f"{type(item_val)}",
                            )
                # If not a dict, it must be list[Order] per type hint
                else:  # orders_data is list[Order]
                    # orders_data is list[Order] here, no further isinstance check needed.
                    item_in_list: Order
                    for item_in_list in orders_data:
                        items_to_process.append(item_in_list)
                # Original final else for unexpected orders_data type is effectively unreachable
                # if input strictly matches type hint. This is acceptable.

                for order_data_item in items_to_process:
                    if isinstance(order_data_item, dict):
                        order_dict_data: dict[str, Any] = order_data_item  # Now this is safe
                        try:
                            order = Order.model_validate(order_dict_data)
                            # Process validated order
                            order.price = self._safe_decimal_convert(
                                order.price,
                                "price",
                                order.symbol,
                                exchange_id,
                            )
                            order.quantity_requested = self._safe_decimal_convert(
                                order.quantity_requested,
                                "quantity_requested",
                                order.symbol,
                                exchange_id,
                            ) or Decimal("0")
                            order.quantity_filled = self._safe_decimal_convert(
                                order.quantity_filled,
                                "quantity_filled",
                                order.symbol,
                                exchange_id,
                            ) or Decimal("0")
                            if isinstance(order.status, str):
                                try:
                                    order.status = OrderStatus(order.status)
                                except ValueError:
                                    self.logger.warning(
                                        f"Invalid status string '{order.status}' for "
                                        f"order {order.client_order_id}",
                                    )
                                    order.status = OrderStatus.UNKNOWN
                            current_orders[order.client_order_id] = order
                            updated_count += 1
                        except ValidationError as e:
                            # Ensure order_dict_data is used for logging if it's a dict
                            client_id_for_log = order_dict_data.get(
                                "clientOrderId",
                                order_dict_data.get("client_order_id", "UnknownClientOrderID"),
                            )
                            self.logger.error(
                                f"Error validating Order for {client_id_for_log} "
                                f"on {exchange_id}: {e}",
                            )
                    # If not dict, must be Order (items_to_process has Order | dict[str, Any],
                    # and non-Order/dict items were filtered).
                    # Pre-filtering ensures: if not dict here, it must be Order.
                    else:  # order_data_item must be an Order
                        order_obj_from_list: Order = order_data_item  # Renamed variable
                        # Process order object
                        order_obj_from_list.price = self._safe_decimal_convert(
                            order_obj_from_list.price,
                            "price",
                            order_obj_from_list.symbol,
                            exchange_id,
                        )
                        order_obj_from_list.quantity_requested = self._safe_decimal_convert(
                            order_obj_from_list.quantity_requested,
                            "quantity_requested",
                            order_obj_from_list.symbol,
                            exchange_id,
                        ) or Decimal("0")
                        order_obj_from_list.quantity_filled = self._safe_decimal_convert(
                            order_obj_from_list.quantity_filled,
                            "quantity_filled",
                            order_obj_from_list.symbol,
                            exchange_id,
                        ) or Decimal("0")
                        if isinstance(order_obj_from_list.status, str):
                            try:
                                order_obj_from_list.status = OrderStatus(order_obj_from_list.status)
                            except ValueError:
                                self.logger.warning(
                                    f"Invalid status string '{order_obj_from_list.status}' for "
                                    f"order {order_obj_from_list.client_order_id}",
                                )
                                order_obj_from_list.status = OrderStatus.UNKNOWN
                            current_orders[order_obj_from_list.client_order_id] = (
                                order_obj_from_list
                            )
                            new_count += 1
                    # No else needed as items_to_process is already filtered

                self.orders[exchange_id] = current_orders
                self.logger.info(
                    f"Parsed {len(items_to_process)} order items for {exchange_id}. "
                    f"{new_count} new (as Order objects), {updated_count} updated (from dict).",
                )

        asyncio.create_task(_do_parse())

    def reset(self) -> None:
        """Reset the portfolio tracker to a clean initial state.

        This method clears all tracked balances, positions, orders, timestamps, high watermark,
        realized PNL, active symbols, and watchlist. It is intended for use in tests or integration
        scenarios where a fresh portfolio state is required.
        """
        self.balances.clear()
        self.positions.clear()
        self.orders.clear()
        self.tickers.clear()
        self.last_update_time.clear()
        self.last_reconciliation_time.clear()
        self.high_watermark = Decimal("0.0")
        self.realized_pnl = Decimal("0.0")
        self.active_symbols.clear()
        self.watchlist.clear()
        self._initialize_data_structures()
        # Optionally, log the reset event
        logger.info("PortfolioTracker state has been reset.")

    async def load_state(self) -> None:
        # ... (Implementation as before) ...
        pass  # Placeholder

    async def initialize_portfolio(self) -> None:
        # ... (Implementation as before) ...
        pass  # Placeholder

    # --- Price Helper --- #
    async def _get_asset_price_in_base(
        self,
        exchange_id: str,
        asset: str,  # The asset whose price we want (e.g., 'BTC', 'ETH')
        base_currency: str,  # The currency to get the price in (e.g., 'USDC')
        price_override: Decimal
        | None = None,  # Allow overriding for specific cases like entry price conversion
    ) -> Decimal | None:
        """Helper to get the price of an asset in the base currency."""
        # TODO: Revisit price override logic - assumes override is already in base_currency
        if price_override is not None:
            logger.debug(
                f"[{exchange_id}] Using provided price override for {asset} "
                f"in {base_currency}: {price_override}",
            )
            return price_override

        if asset == base_currency:
            return Decimal("1.0")

        client = self.api_clients.get(exchange_id)
        if not client:
            logger.warning(
                f"[{exchange_id}] No API client to fetch price for {asset}->{base_currency}",
            )
            return None

        # DEBUG LOGGING START
        logger.info(
            f"_get_asset_price_in_base CALLED for {exchange_id}: asset='{asset}', "
            f"base_currency='{base_currency}', price_override={price_override}",
        )
        # DEBUG LOGGING END

        # 1. Direct match (e.g., BTC/USDC)
        symbol_direct = f"{asset.upper()}-{base_currency.upper()}"
        logger.debug(
            f"[{exchange_id}] _get_asset_price_in_base: Attempting direct "
            f"lookup for {symbol_direct}",
        )
        ticker_direct = await client.get_ticker(symbol_direct)
        logger.debug(
            f"[{exchange_id}] _get_asset_price_in_base: Received ticker_direct for "
            f"{symbol_direct}: {ticker_direct} (Type: {type(ticker_direct)})",
        )
        if ticker_direct:
            logger.debug(
                f"[{exchange_id}] _get_asset_price_in_base: ticker_direct.price "
                f"for {symbol_direct}: {getattr(ticker_direct, 'price', 'N/A')}",
            )
            # Debug logging for ticker price checks
            has_valid_price = (
                ticker_direct
                and ticker_direct.price is not None
                and ticker_direct.price > Decimal("0")
            )
            logger.debug(
                f"[{exchange_id}] Ticker direct price check for {symbol_direct}: {has_valid_price}",
            )

        if ticker_direct and ticker_direct.price is not None and ticker_direct.price > Decimal("0"):
            logger.debug(
                f"[{exchange_id}] _get_asset_price_in_base: ticker_direct.price "
                f"for {symbol_direct}: {ticker_direct.price}",
            )
            return ticker_direct.price

        # Try inverse pair: BASE-ASSET (e.g., USDC-BTC)
        symbol_inverse = f"{base_currency}-{asset}"
        logger.debug(
            f"[{exchange_id}] _get_asset_price_in_base: Attempting inverse "
            f"lookup for {symbol_inverse}",
        )
        ticker_inverse = await client.get_ticker(symbol_inverse)
        logger.debug(
            f"[{exchange_id}] _get_asset_price_in_base: Received ticker_inverse for "
            f"{symbol_inverse}: {ticker_inverse} (Type: {type(ticker_inverse)})",
        )
        if ticker_inverse:
            logger.debug(
                f"[{exchange_id}] _get_asset_price_in_base: ticker_inverse.price "
                f"for {symbol_inverse}: {getattr(ticker_inverse, 'price', 'N/A')}",
            )
            # Debug logging for inverse ticker price checks
            has_valid_inverse_price = (
                ticker_inverse
                and ticker_inverse.price is not None
                and ticker_inverse.price > Decimal("0")
            )
            logger.debug(
                f"[{exchange_id}] Ticker inverse price check for {symbol_inverse}: "
                f"{has_valid_inverse_price}",
            )

        if (
            ticker_inverse
            and ticker_inverse.price is not None
            and ticker_inverse.price > Decimal("0")
        ):
            price = Decimal("1.0") / ticker_inverse.price
            logger.debug(
                f"[{exchange_id}] _get_asset_price_in_base: ticker_inverse.price "
                f"for {symbol_inverse}: {ticker_inverse.price}, calculated: {price}",
            )
            return price

        # TODO: Implement simple triangulation if needed
        logger.warning(
            f"[{exchange_id}] Price conversion failed for {asset} to "
            f"{base_currency}. Returning None.",
        )
        return None

    def get_exchange_balance(self, exchange: str, asset: str) -> SpotBalance | None:
        """Retrieves the balance for a specific asset on a specific exchange.
        Conforms to PortfolioTrackerProtocol.
        """
        # Internally, self.balances uses exchange_id which is equivalent to 'exchange' here.
        if exchange in self.balances and asset in self.balances[exchange]:
            return self.balances[exchange][asset]
        self.logger.debug(f"Balance for {asset} on exchange '{exchange}' not found.")  # DEBUG log
        return None

    def _initialize_from_config(self) -> None:
        logger.info("Initializing PortfolioTracker from config...")
        # Initialize balances
        for exchange_id, balances in self.pt_config.initial_balances.items():
            for asset, quantity_str in balances.items():
                try:
                    quantity = Decimal(quantity_str)
                    self.balances[exchange_id][asset] = SpotBalance(
                        exchange=exchange_id,
                        asset=asset,
                        total_quantity=quantity,
                        available_quantity=quantity,  # Assume all available initially
                        timestamp=datetime.now(UTC),
                    )
                    logger.info(f"Initialized balance for {asset} on {exchange_id}: {quantity}")
                except InvalidOperation:
                    logger.error(
                        f"Invalid decimal value for initial balance of {asset} "
                        f"on {exchange_id}: {quantity_str}",
                    )
        # Initialize positions
        for pos_dict in self.pt_config.initial_positions:
            try:
                pos = DerivativePosition(**pos_dict)
                self.positions[pos.exchange][pos.symbol] = pos
                logger.info(
                    f"Initialized position: {pos.symbol} on {pos.exchange}, "
                    f"Side: {pos.side}, Size: {pos.size}",
                )
            except (ValidationError, TypeError) as e:
                logger.error(f"Failed to create DerivativePosition from config: {e}")
        logger.info("PortfolioTracker initialized.")

    async def _fetch_exchange_account_summary(
        self,
        client: ExchangeAPI,
        exchange_id: str,
    ) -> tuple[str, MarginAccountSummary | None] | None:
        try:
            summary = await client.get_account_summary()
            if summary:
                return exchange_id, summary
            else:
                logger.warning(f"No account summary found for {exchange_id}")
                return None
        except Exception as e:
            logger.exception(f"Error fetching account summary for {exchange_id}: {e}")
            return None

    def get_all_derivative_positions_for_exchange(
        self,
        exchange_id: str,
    ) -> dict[Symbol, DerivativePosition] | None:
        """Returns all derivative positions for a given exchange."""
        normalized_exchange_id = exchange_id.lower()

        # ADDED DETAILED LOGGING (NOW AS WARNING)
        self.logger.warning(
            f"PT_GET_ALL_DERIV_POS_CRITICAL_DEBUG: id(self)={id(self)}, "
            f"id(self.api_clients)={id(self.api_clients)}, "
            f"self.api_clients={self.api_clients}, "
            f"exchange_id='{exchange_id}', normalized_exchange_id='{normalized_exchange_id}'",
        )

        if normalized_exchange_id not in self.api_clients:
            self.logger.warning(
                f"PT_GET_ALL_DERIV_POS: Attempted to get positions for unknown or unregistered "
                f"exchange: '{exchange_id}' (normalized: '{normalized_exchange_id}').",
            )
            return None

        return self.positions[normalized_exchange_id]
