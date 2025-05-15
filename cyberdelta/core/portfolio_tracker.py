from __future__ import annotations  # Enable postponed evaluation

import asyncio
from builtins import BaseException
from collections import defaultdict
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from cyberdelta.apis.base.exchange_api import ExchangeAPI
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
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger
from cyberdelta.utils.parsing import parse_datetime_utc

logger = get_logger(__name__)

# Default timeout for how long balance/position data is considered fresh
DEFAULT_DATA_FRESHNESS_SECONDS = 60

# Temporarily define ExchangeId, ExchangeType, Symbol as str TypeAlias to unblock linter
# TODO: Find or create the canonical definitions for these types
type Symbol = str
type ExchangeId = str
type ExchangeType = str


class PortfolioTrackerConfig(BaseModel):
    data_freshness_seconds: int = Field(DEFAULT_DATA_FRESHNESS_SECONDS, gt=0)
    initial_balances: dict[ExchangeId, dict[str, str]] = Field(default_factory=dict)
    initial_positions: list[DerivativePosition] = Field(default_factory=list)


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

    def __init__(
        self,
        config: Config,
        api_clients: dict[str, ExchangeAPI] | None = None,
        exchange_factories: dict[ExchangeType, Callable[..., ExchangeAPI]] | None = None,
    ) -> None:
        """
        Initialize the portfolio tracker.

        Args:
            config: Application configuration
            api_clients: Dictionary of exchange API clients
            exchange_factories: Dictionary of exchange API factory functions
        """
        self.logger = get_logger(__name__ + "." + self.__class__.__name__)
        self.config: Config = config
        self.api_clients: dict[str, ExchangeAPI] = api_clients or {}
        self.exchange_factories = exchange_factories or {}
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
                )
            )
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
                )
            )
        )
        self.orders: defaultdict[str, dict[str, Order]] = defaultdict(
            dict
        )  # client_order_id -> Order

        # Initialize last update times
        # Consolidate individual last update times
        self.last_update_time: defaultdict[str, datetime] = defaultdict(
            lambda: datetime.min.replace(tzinfo=UTC)
        )
        self.last_reconciliation_time: defaultdict[str, datetime] = defaultdict(
            lambda: datetime.min.replace(tzinfo=UTC)
        )
        # Removed individual _last_balance_update_times, _last_position_update_times, etc.

        self.tickers: dict[str, Ticker] = {}

        pt_config_dict_raw = self.config.get("portfolio_tracker")
        # Ensure pt_config_dict is dict[str, Any]
        temp_pt_config_dict: dict[str, Any] = {}
        if isinstance(pt_config_dict_raw, dict):
            k_str: str
            v_any: Any
            for k_str, v_any in pt_config_dict_raw.items():  # Add type hints for k, v
                temp_pt_config_dict[str(k_str)] = v_any
        pt_config_dict = temp_pt_config_dict

        # Ensure data_freshness_seconds has a default if not in pt_config_dict
        # or if its value is not what PortfolioTrackerConfig expects.
        # PortfolioTrackerConfig will validate its type.
        if "data_freshness_seconds" not in pt_config_dict:
            pt_config_dict["data_freshness_seconds"] = DEFAULT_DATA_FRESHNESS_SECONDS

        try:
            self.pt_config = PortfolioTrackerConfig(**pt_config_dict)
        except ValidationError as e:
            logger.error(f"Invalid portfolio_tracker config: {e}. Using defaults.")
            # Fallback to default config if validation fails
            self.pt_config = PortfolioTrackerConfig(
                data_freshness_seconds=DEFAULT_DATA_FRESHNESS_SECONDS
            )

        self._initialize_from_config()

        # Reconciliation interval (5 minutes by default)
        interval = config.get("portfolio.reconciliation_interval", 300)
        if isinstance(interval, int | float | str):
            self.reconciliation_interval = int(interval)
        else:
            self.reconciliation_interval = 300  # seconds

        # Initialize data structures
        self._initialize_data_structures()

        # Initialize high watermark (as Decimal)
        self.high_watermark: Decimal = Decimal(
            "0.0"
        )  # Track highest portfolio value for drawdown calculation

        # Add internal tracking for realized PNL
        self.realized_pnl: Decimal = Decimal("0.0")  # Track realized PNL

        # Add active symbols and watchlist
        self.active_symbols: set[str] = set()
        self.watchlist: set[str] = set()

    def _initialize_data_structures(self) -> None:
        """Initialize data structures for all configured exchanges."""
        exchanges_raw = self.config.get("exchanges", {})
        exchanges: dict[str, Any] = exchanges_raw if isinstance(exchanges_raw, dict) else {}
        for exchange_id_str in [str(k) for k in exchanges.keys()]:
            exchange_id = exchange_id_str  # ensure it's a string
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Ensure last_update_time and last_reconciliation_time have initial entries
            # if not already set by defaultdict lambda (which they are)
            _ = self.last_update_time[exchange_id]
            _ = self.last_reconciliation_time[exchange_id]

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
        initialization_tasks: list[Awaitable[Any]] = []
        logger.info(
            f"PortfolioTracker {id(self)}: About to gather init tasks. "
            f"Balance dict: {self.balances}"
        )
        for exchange_id, client in self.api_clients.items():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue
            initialization_tasks.append(self._fetch_exchange_account_summary(client, exchange_id))
            initialization_tasks.append(self._fetch_exchange_balances(exchange_id))
            initialization_tasks.append(self._fetch_exchange_positions(exchange_id))
            initialization_tasks.append(self._fetch_exchange_orders(exchange_id))
        logger.info(
            f"PortfolioTracker {id(self)}: About to gather init tasks. "
            f"Balances before: {self.balances}"
        )
        results: list[bool | BaseException] = await asyncio.gather(
            *initialization_tasks, return_exceptions=True
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
                f"Cannot proceed reliably. Errors: {error_summary}"
            )

        # Set initial reconciliation time AFTER fetching
        now_utc = datetime.now(UTC)
        logger.info(
            f"PT INIT: Setting reconciliation times. "
            f"Current self.last_reconciliation_time before: {self.last_reconciliation_time}"
        )
        for exchange_id in self.api_clients:
            self.last_reconciliation_time[exchange_id] = now_utc
        logger.info(
            f"PT INIT: Reconciliation times set. "
            f"Current self.last_reconciliation_time after: {self.last_reconciliation_time}"
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
        logger.debug(f"[FETCH_BALANCES:{exchange_id}] Entering function.")
        try:
            client = self.api_clients.get(exchange_id)
            if not client:
                logger.error(f"[FETCH_BALANCES:{exchange_id}] No API client found.")
                return False

            logger.debug(f"[FETCH_BALANCES:{exchange_id}] Fetching balances from API...")
            balances_data_raw: (
                dict[str, SpotBalance] | list[SpotBalance] | None
            ) = await client.get_balances()
            logger.debug(
                (
                    f"[FETCH_BALANCES:{exchange_id}] Raw API response: {balances_data_raw} "
                    f"(Type: {type(balances_data_raw)})"
                ),
                stacklevel=2,
            )

            updated_balances: dict[str, SpotBalance] = {}

            if balances_data_raw is None:
                logger.info(
                    f"[FETCH_BALANCES:{exchange_id}] API call returned None. No balances to update."
                )
                # Assuming "zero balances" for now as it's safer for risk.
                async with self._lock:
                    if exchange_id in self.balances:  # Check if exchange_id itself exists
                        self.balances[exchange_id].clear()  # Clear assets for this exchange
                        logger.info(
                            f"[FETCH_BALANCES:{exchange_id}] Cleared existing balance entries "
                            f"as API returned None."
                        )
                        self.last_update_time[exchange_id] = datetime.now(UTC)  # Mark as updated
                return True

            elif isinstance(balances_data_raw, list):
                balances_list: list[Any] = (
                    balances_data_raw  # Temporarily Any to allow isinstance check
                )
                processed_new_balance = False
                for item_any in balances_list:
                    if not isinstance(item_any, SpotBalance):
                        logger.warning(
                            f"[{exchange_id}] Skipping non-SpotBalance item in "
                            f"balances list: {item_any}"
                        )
                        continue
                    item: SpotBalance = item_any  # Now item is confirmed SpotBalance

                    if item.exchange == exchange_id:
                        updated_balances[item.asset] = item
                        processed_new_balance = True
                    else:
                        logger.warning(
                            f"[{exchange_id}] Skipping balance for asset {item.asset} "
                            f"due to mismatched exchange ID ({item.exchange}) "
                            f"in received SpotBalance object."
                        )
                if not processed_new_balance and not balances_list:  # Empty list received
                    logger.info(
                        f"[FETCH_BALANCES:{exchange_id}] API returned an empty list of balances."
                    )
                    async with self._lock:
                        if exchange_id in self.balances:
                            self.balances[exchange_id].clear()
                            logger.info(
                                f"[FETCH_BALANCES:{exchange_id}] Cleared existing balance entries "
                                f"as API returned an empty list."
                            )
                            self.last_update_time[exchange_id] = datetime.now(UTC)

            else:  # balances_data_raw must be a dict here
                balances_dict: dict[str, SpotBalance] = balances_data_raw
                if not balances_dict:  # Empty dict received
                    logger.info(f"[{exchange_id}] API returned an empty dictionary of balances.")
                    async with self._lock:
                        if exchange_id in self.balances:
                            self.balances[exchange_id].clear()
                            logger.info(
                                f"[FETCH_BALANCES:{exchange_id}] Cleared existing balance entries "
                                f"as API returned an empty dict."
                            )
                            self.last_update_time[exchange_id] = datetime.now(UTC)

                for asset, balance_obj_any in balances_dict.items():
                    balance_obj: SpotBalance = balance_obj_any

                    if balance_obj.exchange == exchange_id:
                        updated_balances[asset] = balance_obj
                    else:
                        logger.warning(
                            f"[{exchange_id}] Skipping balance for asset {asset} "
                            f"due to mismatched exchange ID ({balance_obj.exchange}) "
                            f"in received SpotBalance object (from dict)."
                        )

            # Update internal state if new valid balances were found or if an empty list/dict
            # signified clearing
            if (
                updated_balances
                # If balances_data_raw was not None (handled above),
                # and not updated_balances, it implies balances_data_raw was an empty list/dict.
                # The isinstance check here is redundant given the flow.
                or (not updated_balances and not balances_data_raw)
            ):
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
                                f"for asset {asset_to_remove}."
                            )

                    # Now, add/update balances from updated_balances
                    for asset, balance in updated_balances.items():
                        self.balances[exchange_id][asset] = balance  # Corrected access
                    self.last_update_time[exchange_id] = datetime.now(UTC)
                logger.info(
                    f"[FETCH_BALANCES:{exchange_id}] Balances updated "
                    f"successfully with {len(updated_balances)} items. "
                    f"Assets removed: {len(assets_to_remove)}."
                )
            elif not updated_balances:
                # This implies that balances_data_raw was not None,
                # and if it was a list or dict, it was empty,
                # and no balances were processed into updated_balances.
                # If balances_data_raw was some other unexpected type,
                # it would have been caught by an earlier `else`
                # or the initial type hint for `client.get_balances()` would be violated.
                logger.warning(
                    f"[FETCH_BALANCES:{exchange_id}] No valid balances processed or "
                    f"API returned empty data. Type: {type(balances_data_raw)}"
                )
                # Not returning False here, as an empty (but valid) response is not an error.

            return True

        except Exception as e:
            logger.exception(f"[FETCH_BALANCES:{exchange_id}] Error during balance fetch: {e}")
            return False

    def _parse_balance_info(
        self, exchange_id: str, asset: str, balance_info: dict[str, Any] | SpotBalance
    ) -> SpotBalance | None:
        """Parse balance information into a SpotBalance object."""
        if isinstance(balance_info, SpotBalance):
            if balance_info.exchange != exchange_id:
                logger.error(
                    f"Mismatched exchange ID in provided SpotBalance object: expected "
                    f"{exchange_id}, got {balance_info.exchange}"
                )
                return None
            return balance_info

        # Since the type hint is dict[str, Any] | SpotBalance, and SpotBalance is handled above,
        # balance_info must be a dict here. The isinstance check below is
        # redundant if type hints are trusted.
        # if not isinstance(balance_info, dict): # Linter flags as unnecessary
        #     logger.error(
        #         f"Invalid balance_info type: {type(balance_info)}. Expected dict or SpotBalance."
        #     )
        #     return None

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
                    f"Available={parsed.available_quantity}"
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
                        f"Skipping DerivativePosition object without symbol on "
                        f"{exchange_id}: {position_info}"
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
            self.orders[exchange_id] = updated_orders  # This replaces the inner dict
            self.last_update_time[exchange_id] = datetime.now(UTC)
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
        update_tasks: list[Awaitable[Any]] = []
        for exchange_id, client in self.api_clients.items():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue
            last_reconciliation = self.last_reconciliation_time.get(
                exchange_id, datetime.min.replace(tzinfo=UTC)
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
        current_capital = await self.get_total_capital()
        self.high_watermark = max(self.high_watermark, current_capital)

    def update_order(self, exchange_id: str, order: Order) -> None:
        """
        Update the status of a single order.

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
        # self.positions is defaultdict(defaultdict), so exchange_id will be auto-created
        pos_key = position.symbol
        if not pos_key:
            logger.error(f"Received position update without symbol on {exchange_id}: {position}")
            return

        self.positions[exchange_id][pos_key] = position
        self.last_update_time[exchange_id] = datetime.now(UTC)
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

        # --- Use Base Symbol for Position Tracking ---
        base_symbol: str
        if "-" in trade.symbol:
            base_symbol = trade.symbol.split("-")[0]
        elif "_" in trade.symbol:
            base_symbol = trade.symbol.split("_")[0]
        else:
            base_symbol = trade.symbol  # Assume it's already base if no separator
        logger.debug(
            f"Using base symbol '{base_symbol}' for position tracking "
            f"from trade symbol '{trade.symbol}'"
        )
        position_key = base_symbol
        # -----------------------------------------

        exchange_positions = self.positions[
            exchange_id
        ]  # This is defaultdict[str, DerivativePosition]
        current_position = exchange_positions.get(position_key)  # DerivativePosition | None

        # The check for `current_position.size != Decimal(0)` or `is_active()` is good.
        # The placeholder has size=0 and entry_price=None.
        if current_position and current_position.is_active():  # is_active() checks size != 0
            logger.debug(f"Updating existing position for {base_symbol} on {exchange_id}")
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
                        f"Total Realized PNL: {self.realized_pnl:.4f}"
                    )
                else:
                    logger.warning(
                        f"Cannot calculate PNL for closing {trade.symbol} "
                        f"due to zero/None entry price."
                    )

                del self.positions[exchange_id][position_key]
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
                current_position.timestamp = trade.executed_at  # Use executed_at
                # Mark price, liq price, unrealized PNL update via market data
                logger.debug(
                    f"Position {trade.symbol} modified. New size: {new_position_size}, "
                    f"New avg entry: {new_entry_price:.4f}"
                )

        else:
            # New position opened
            logger.debug(f"Opening new position for {trade.symbol} on {exchange_id}")
            side = trade.side
            new_position = DerivativePosition(
                exchange=exchange_id,
                symbol=base_symbol,  # Use base_symbol
                side=side,
                size=trade.quantity if side == OrderSide.BUY else -trade.quantity,
                entry_price=trade.price,  # Assume trade price is valid entry > 0
                timestamp=trade.executed_at,  # Use executed_at
                mark_price=trade.price,  # Initial mark price
                liquidation_price=None,
                unrealized_pnl=Decimal("0.0"),
                realized_pnl=Decimal("0.0"),
            )
            self.positions[exchange_id][position_key] = new_position

        # Update Balances (Simplified - full logic depends on asset details)
        self._update_balances_from_trade(exchange_id, trade)

        self.last_update_time[exchange_id] = datetime.now(UTC)

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
        """Returns the position for a specific symbol on a specific exchange."""
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

    def get_all_positions(self) -> list[DerivativePosition]:
        """Returns a list of all derivative positions across all exchanges."""
        all_positions: list[DerivativePosition] = []
        for (
            _exchange_id_loop,
            positions_on_exchange_loop,
        ) in self.positions.items():  # Use _ if var not used
            # symbol_loop: str # Hint for loop variable
            # position_loop: DerivativePosition # Hint for loop variable
            for (
                _symbol_loop,
                position_loop,
            ) in positions_on_exchange_loop.items():  # Use _ if var not used
                if position_loop.size != Decimal(0):  # Exclude placeholders
                    all_positions.append(position_loop)
        return all_positions

    def get_positions_by_exchange(self, exchange_id: str) -> list[DerivativePosition]:
        """Get all positions for a specific exchange."""
        if exchange_id not in self.positions:
            logger.warning(f"Attempted to get positions for unknown exchange: {exchange_id}")
            return []
        # Filter out placeholders
        return [pos for pos in self.positions[exchange_id].values() if pos.size != Decimal(0)]

    async def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        """Calculate the total portfolio value in the specified base currency."""
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
                            f"@ {price} {base_currency} = {asset_value} {base_currency}"
                        )
                    except (TypeError, InvalidOperation) as e:
                        logger.error(
                            f"Error calculating value for spot balance {asset} "
                            f"on {exchange_id}: {e}"
                        )
                else:
                    logger.warning(
                        f"Could not determine price for spot asset {asset} "
                        f"on {exchange_id} in {base_currency}. "
                        f"Skipping in capital calculation."
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
        self, exchange_id: str, valuation_asset: str = "USDC"
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
                exchange_id, position.symbol, valuation_asset
            )

            if mark_price is not None and mark_price.is_finite():
                try:
                    # Use absolute value of size for exposure calculation
                    position_value = abs(position.size) * mark_price
                    exchange_exposure += position_value
                    logger.debug(
                        f"  [{exchange_id}] Position Exposure: {position.symbol} "
                        f"size {position.size} @ mark {mark_price} {valuation_asset} = "
                        f"{position_value} {valuation_asset}"
                    )
                except (TypeError, InvalidOperation) as e:
                    logger.error(
                        f"Error calculating value for position {position_key} "
                        f"({position.symbol}) on {exchange_id}: {e}"
                    )
            else:
                logger.warning(
                    f"Could not determine mark price for position {position_key} "
                    f"({position.symbol}) on {exchange_id} in {valuation_asset}. "
                    f"Skipping in exposure calculation."
                )

        logger.info(f"Total exposure for {exchange_id}: {exchange_exposure} {valuation_asset}")
        return exchange_exposure if exchange_exposure.is_finite() else Decimal("0.0")

    async def get_total_exposure_usd(self, valuation_asset: str = "USDC") -> Decimal:
        """Calculate the total market exposure across all exchanges."""
        logger.debug(f"Calculating total exposure across all exchanges in {valuation_asset}...")
        total_exposure = Decimal("0.0")
        for exchange_id in self.api_clients.keys():
            if self.config.get(f"exchanges.{exchange_id}.enabled", False):
                total_exposure += await self.get_exchange_exposure(exchange_id, valuation_asset)

        logger.info(f"Total portfolio exposure calculated: {total_exposure} {valuation_asset}")
        return total_exposure if total_exposure.is_finite() else Decimal("0.0")

    async def get_pnl(self, base_currency: str = "USDC") -> tuple[Decimal, Decimal]:
        """
        Calculate the total realized and unrealized PNL across all exchanges.

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
                        exchange_id, pnl_quote_asset, base_currency
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
                                    f"is not finite ({converted_pnl}). Skipping addition."
                                )
                        except (TypeError, InvalidOperation) as e:
                            logger.error(
                                f"Error during realized PNL conversion/addition "
                                f"for {position_key}: {e}"
                            )
                    else:
                        logger.warning(
                            f"Cannot convert realized PNL for {position_key} on {exchange_id} "
                            f"to {base_currency}. Realized PNL: {position.realized_pnl}, "
                            f"Conversion Rate: {conversion_rate}."
                        )
                elif position.realized_pnl is not None:  # Log if it exists but isn't finite
                    logger.warning(
                        f"Position {position_key} realized PNL is not finite: "
                        f"{position.realized_pnl}"
                    )

                # Calculate unrealized PNL
                # DEFENSIVE CHECK: Check entry_price is not None *before* size check
                # because a non-zero size *requires* a non-None entry_price (model validation)
                if position.size == Decimal("0") or position.entry_price is None:
                    logger.debug(
                        f"Skipping unrealized PNL calc for {position_key} on {exchange_id} "
                        f"due to zero size or missing entry price."
                    )
                    continue

                # 1. Get Mark Price in the requested Base Currency
                mark_price_in_base = await self._get_asset_price_in_base(
                    exchange_id, position.symbol, base_currency
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
                        f"cannot calculate unrealized PNL accurately."
                    )
                    continue  # Skip if we can't determine quote currency

                # 4. Convert Entry Price from Quote Currency to Base Currency
                entry_price_in_base: Decimal | None
                if quote_currency == base_currency:
                    entry_price_in_base = entry_price_in_quote
                else:
                    # Need conversion rate from quote to base
                    quote_to_base_rate = await self._get_asset_price_in_base(
                        exchange_id, quote_currency, base_currency
                    )
                    if quote_to_base_rate is not None:
                        entry_price_in_base = entry_price_in_quote * quote_to_base_rate
                    else:
                        logger.warning(
                            f"Cannot convert entry price for {position.symbol} "
                            f"from {quote_currency} to {base_currency}. "
                            f"Skipping unrealized PNL."
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
                            f"{unrealized_pnl:.4f} {base_currency}"
                        )

                    except (TypeError, InvalidOperation) as e:
                        logger.error(
                            f"Error calculating unrealized PNL for {position_key} "
                            f"on {exchange_id}: {e}"
                        )
                else:
                    logger.warning(
                        f"Skipping unrealized PNL calculation for {position_key} "
                        f"({position.symbol}) on {exchange_id} due to missing/invalid "
                        f"converted prices (MarkBase={mark_price_in_base}, "
                        f"EntryBase={entry_price_in_base})."
                    )

        finite_realized = total_realized_pnl if total_realized_pnl.is_finite() else Decimal("0.0")
        finite_unrealized = (
            total_unrealized_pnl if total_unrealized_pnl.is_finite() else Decimal("0.0")
        )

        logger.info(
            f"Total PNL calculated: Realized={finite_realized} {base_currency}, "
            f"Unrealized={finite_unrealized} {base_currency}"
        )
        return finite_realized, finite_unrealized

    async def get_current_drawdown(self, base_currency: str = "USDC") -> Decimal:
        """Calculate the current drawdown from the portfolio's high watermark."""
        current_capital = await self.get_total_capital()

        if self.high_watermark <= Decimal("0.0"):
            logger.warning("High watermark is not positive. Cannot calculate drawdown.")
            return Decimal("0.0")

        if not current_capital.is_finite() or current_capital <= Decimal("0.0"):
            logger.warning(
                f"Current capital ({current_capital}) is not positive or finite. "
                f"Cannot calculate drawdown."
            )
            return Decimal("0.0")

        drawdown = (self.high_watermark - current_capital) / self.high_watermark
        result = max(Decimal("0.0"), drawdown)
        logger.debug(
            f"Calculated Drawdown: HWM={self.high_watermark}, "
            f"Capital={current_capital}, Drawdown={result}"
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
        """
        Retrieve a specific order by its ID from the internal tracking.

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
    def from_dict(cls, data: dict[str, Any], config: Config) -> PortfolioTracker:
        """Deserialize the portfolio state from a dictionary."""
        tracker = cls(config)

        balances_data_get = data.get("balances", {})
        if isinstance(balances_data_get, dict):
            balances_data_typed: dict[str, Any] = balances_data_get
            ex_id_str: str
            assets_dict_any: Any
            for ex_id_str, assets_dict_any in balances_data_typed.items():
                if isinstance(assets_dict_any, dict):
                    asset_str: str
                    bal_data_any: Any
                    for asset_str, bal_data_any in assets_dict_any.items():
                        if isinstance(bal_data_any, dict):
                            try:
                                tracker.balances[ex_id_str][asset_str] = SpotBalance.model_validate(
                                    bal_data_any
                                )
                            except ValidationError as e:
                                logger.error(
                                    f"Error validating SpotBalance for {asset_str} "
                                    f"on {ex_id_str}: {e}"
                                )
                        elif isinstance(bal_data_any, SpotBalance):  # If already an object
                            tracker.balances[ex_id_str][asset_str] = bal_data_any

        positions_data_get = data.get("positions", {})
        if isinstance(positions_data_get, dict):
            positions_data_typed: dict[str, Any] = positions_data_get
            ex_id_str_pos: str
            syms_dict_any: Any
            for ex_id_str_pos, syms_dict_any in positions_data_typed.items():
                if isinstance(syms_dict_any, dict):
                    syms_dict_typed: dict[str, Any] = syms_dict_any
                    sym_str: str
                    pos_data_any: Any
                    for sym_str, pos_data_any in syms_dict_typed.items():
                        if isinstance(pos_data_any, dict):
                            try:
                                tracker.positions[ex_id_str_pos][sym_str] = (
                                    DerivativePosition.model_validate(pos_data_any)
                                )
                            except ValidationError as e:
                                logger.error(
                                    f"Error validating DerivativePosition for {sym_str} "
                                    f"on {ex_id_str_pos}: {e}"
                                )
                        elif isinstance(pos_data_any, DerivativePosition):
                            tracker.positions[ex_id_str_pos][sym_str] = pos_data_any

        orders_data_get = data.get("orders", {})
        if isinstance(orders_data_get, dict):
            orders_data_typed: dict[str, Any] = orders_data_get
            ex_id_str_ord: str
            ords_dict_any: Any
            for ex_id_str_ord, ords_dict_any in orders_data_typed.items():
                if isinstance(ords_dict_any, dict):
                    ords_dict_typed: dict[str, Any] = ords_dict_any
                    ord_id_str: str
                    order_data_any: Any
                    for ord_id_str, order_data_any in ords_dict_typed.items():
                        if isinstance(order_data_any, dict):
                            try:
                                tracker.orders[ex_id_str_ord][ord_id_str] = Order.model_validate(
                                    order_data_any
                                )
                            except ValidationError as e:
                                logger.error(
                                    f"Error validating Order for {ord_id_str} "
                                    f"on {ex_id_str_ord}: {e}"
                                )
                        elif isinstance(order_data_any, Order):
                            tracker.orders[ex_id_str_ord][ord_id_str] = order_data_any

        last_update_data_get = data.get("last_update_time", {})
        if isinstance(last_update_data_get, dict):
            last_update_data_typed: dict[str, Any] = last_update_data_get
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
                                ts_data_any_lut, field_name=f"last_update_time.{ex_id_str_lut}"
                            )
                            if parsed_ts:
                                tracker.last_update_time[ex_id_str_lut] = parsed_ts
                        else:
                            logger.warning(
                                f"Received None for last_update_time for {ex_id_str_lut}, skipping."
                            )
                except Exception as e:
                    logger.error(f"Error deserializing last_update_time for {ex_id_str_lut}: {e}")

        last_reconciliation_data_get = data.get("last_reconciliation_time", {})
        if isinstance(last_reconciliation_data_get, dict):
            last_reconciliation_data_typed: dict[str, Any] = last_reconciliation_data_get
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
                                f"{ex_id_str_lrt}, skipping."
                            )
                except Exception as e:
                    logger.error(
                        f"Error deserializing last_reconciliation_time for {ex_id_str_lrt}: {e}"
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
        self, exchange_id: str, positions_data: list[DerivativePosition] | dict[str, Any]
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
                order_data_iterable: (
                    list[Order] | dict[str, Any]
                )  # This should be list[Order | dict[str, Any]]
                if isinstance(orders_data, dict):
                    order_data_iterable = list(orders_data.values())  # Iterate over values if dict
                # If orders_data is not a dict, it must be a list[Order]
                # due to the type hint: list[Order] | dict[str, Any]
                # So, the isinstance(orders_data, list) check is redundant.
                else:  # orders_data must be a list[Order] here
                    order_data_iterable = orders_data  # Iterate directly if list
                # else: # This else branch is unreachable due to type hint
                #     logger.error(
                #         f"_parse_orders received unexpected type for orders_data: "
                #         f"{type(orders_data)}"
                #     )
                #     return

                order_data_item: Order | dict[str, Any]  # Type hint for loop variable
                for order_data_item in order_data_iterable:
                    if isinstance(order_data_item, dict):
                        # Order data is a dictionary, attempt to validate
                        order_dict_data: dict[str, Any] = order_data_item
                        try:
                            order = Order.model_validate(order_dict_data)
                            # Process validated order
                            order.price = self._safe_decimal_convert(
                                order.price, "price", order.symbol, exchange_id
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
                                    logger.warning(
                                        f"Invalid status string '{order.status}' for "
                                        f"order {order.client_order_id}"
                                    )
                                    order.status = OrderStatus.UNKNOWN
                            current_orders[order.client_order_id] = order
                            updated_count += 1
                        except ValidationError as e:
                            # Accessing order.client_order_id might fail if model_validate
                            # failed early
                            client_id_for_log = order_dict_data.get(
                                "clientOrderId",
                                order_dict_data.get("client_order_id", "UnknownClientOrderID"),
                            )
                            logger.error(
                                f"Error validating Order for {client_id_for_log} "
                                f"on {exchange_id}: {e}"
                            )
                    # If order_data_item is not a dict, it must be an Order object
                    # due to the type hint: Order | dict[str, Any]
                    # So, the isinstance(order_data_item, Order) check is redundant.
                    else:  # order_data_item must be an Order object here
                        # order_data_item is already an Order object
                        order = order_data_item
                        # Process order object
                        order.price = self._safe_decimal_convert(
                            order.price, "price", order.symbol, exchange_id
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
                                logger.warning(
                                    f"Invalid status string '{order.status}' for "
                                    f"order {order.client_order_id}"
                                )
                                order.status = OrderStatus.UNKNOWN
                            current_orders[order.client_order_id] = order
                            new_count += 1
                    # else: # This else branch is unreachable
                    #     # Mypy=[unreachable]
                    #     logger.warning(f"Invalid order format for {order_data_item}")
                # Explicitly update the dictionary for the exchange
                # This ensures the defaultdict behavior isn't bypassed if it was empty
                self.orders[exchange_id] = current_orders
                logger.info(
                    f"Parsed {len(orders_data)} orders for {exchange_id}. "
                    f"{new_count} new, {updated_count} updated."
                )

        asyncio.create_task(_do_parse())

    def reset(self) -> None:
        """
        Reset the portfolio tracker to a clean initial state.

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
                f"in {base_currency}: {price_override}"
            )
            return price_override

        if asset == base_currency:
            return Decimal("1.0")

        client = self.api_clients.get(exchange_id)
        if not client:
            logger.warning(
                f"[{exchange_id}] No API client to fetch price for {asset}->{base_currency}"
            )
            return None

        # DEBUG LOGGING START
        logger.info(
            f"_get_asset_price_in_base CALLED for {exchange_id}: asset='{asset}', "
            f"base_currency='{base_currency}', price_override={price_override}"
        )
        # DEBUG LOGGING END

        # 1. Direct match (e.g., BTC/USDC)
        symbol_direct = f"{asset.upper()}-{base_currency.upper()}"
        logger.debug(
            f"[{exchange_id}] _get_asset_price_in_base: Attempting direct "
            f"lookup for {symbol_direct}"
        )
        ticker_direct = await client.get_ticker(symbol_direct)
        logger.debug(
            f"[{exchange_id}] _get_asset_price_in_base: Received ticker_direct for "
            f"{symbol_direct}: {ticker_direct} (Type: {type(ticker_direct)})"
        )
        if ticker_direct:
            logger.debug(
                f"[{exchange_id}] _get_asset_price_in_base: ticker_direct.price "
                f"for {symbol_direct}: {getattr(ticker_direct, 'price', 'N/A')}"
            )
            # Debug logging for ticker price checks
            has_valid_price = (
                ticker_direct
                and ticker_direct.price is not None
                and ticker_direct.price > Decimal("0")
            )
            logger.debug(
                f"[{exchange_id}] Ticker direct price check for {symbol_direct}: {has_valid_price}"
            )

        if ticker_direct and ticker_direct.price is not None and ticker_direct.price > Decimal("0"):
            logger.debug(
                f"[{exchange_id}] _get_asset_price_in_base: ticker_direct.price "
                f"for {symbol_direct}: {ticker_direct.price}"
            )
            return ticker_direct.price

        # Try inverse pair: BASE-ASSET (e.g., USDC-BTC)
        symbol_inverse = f"{base_currency}-{asset}"
        logger.debug(
            f"[{exchange_id}] _get_asset_price_in_base: Attempting inverse "
            f"lookup for {symbol_inverse}"
        )
        ticker_inverse = await client.get_ticker(symbol_inverse)
        logger.debug(
            f"[{exchange_id}] _get_asset_price_in_base: Received ticker_inverse for "
            f"{symbol_inverse}: {ticker_inverse} (Type: {type(ticker_inverse)})"
        )
        if ticker_inverse:
            logger.debug(
                f"[{exchange_id}] _get_asset_price_in_base: ticker_inverse.price "
                f"for {symbol_inverse}: {getattr(ticker_inverse, 'price', 'N/A')}"
            )
            # Debug logging for inverse ticker price checks
            has_valid_inverse_price = (
                ticker_inverse
                and ticker_inverse.price is not None
                and ticker_inverse.price > Decimal("0")
            )
            logger.debug(
                f"[{exchange_id}] Ticker inverse price check for {symbol_inverse}: "
                f"{has_valid_inverse_price}"
            )

        if (
            ticker_inverse
            and ticker_inverse.price is not None
            and ticker_inverse.price > Decimal("0")
        ):
            price = Decimal("1.0") / ticker_inverse.price
            logger.debug(
                f"[{exchange_id}] _get_asset_price_in_base: ticker_inverse.price "
                f"for {symbol_inverse}: {ticker_inverse.price}, calculated: {price}"
            )
            return price

        # TODO: Implement simple triangulation if needed
        logger.warning(
            f"[{exchange_id}] Price conversion failed for {asset} to "
            f"{base_currency}. Returning None."
        )
        return None

    def get_exchange_balance(self, exchange_id: str, asset: str) -> SpotBalance | None:
        """Retrieve the SpotBalance for a specific asset on a specific exchange."""
        # --- BEGIN ADDED LOGGING ---
        logger.info(f"[PT_GET_EX_BAL_START] Called for exchange='{exchange_id}', asset='{asset}'")
        exchange_balances = self.balances.get(exchange_id)  # Get the inner dict for the exchange
        if exchange_balances is None:  # Exchange itself might not exist yet
            logger.info(f"[PT_GET_EX_BAL_RESULT] No balances found for exchange '{exchange_id}'.")
            return None
        balance_obj = exchange_balances.get(asset)  # Get balance from inner dict
        logger.info(
            f"[PT_GET_EX_BAL_RESULT] Found balance object for {asset} "
            f"on {exchange_id}: {balance_obj}"
        )
        # --- END ADDED LOGGING ---
        return balance_obj

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
                        f"on {exchange_id}: {quantity_str}"
                    )
        # Initialize positions
        for pos in self.pt_config.initial_positions:
            self.positions[pos.exchange][pos.symbol] = pos  # Corrected access
            logger.info(
                f"Initialized position: {pos.symbol} on {pos.exchange}, "
                f"Side: {pos.side}, Size: {pos.size}"
            )
        logger.info("PortfolioTracker initialized.")

    async def _fetch_exchange_account_summary(
        self, client: ExchangeAPI, exchange_id: str
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
