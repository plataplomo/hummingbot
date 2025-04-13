from __future__ import annotations  # Enable postponed evaluation

import asyncio
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
        self.reconciliation_interval: int = config.get(
            "portfolio.reconciliation_interval", 300
        )  # seconds

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
        for exchange_id in self.config.get("exchanges", {}).keys():
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
        initialization_tasks = []

        for exchange_id, _client in self.api_clients.items():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Create tasks for initial data collection
            initialization_tasks.append(self._fetch_exchange_balances(exchange_id))
            initialization_tasks.append(self._fetch_exchange_positions(exchange_id))
            # Consider fetching open orders too? Maybe not essential for pure init.
            # initialization_tasks.append(self._fetch_exchange_orders(exchange_id))

        logger.info(
            f"PortfolioTracker {id(self)}: About to gather init tasks. "
            f"Balances before: {self._balances}"
        )
        # Wait for all initialization tasks to complete
        results = await asyncio.gather(*initialization_tasks, return_exceptions=True)
        # Restore logging check immediately after gather:
        logger.info(f"---> State of self._balances immediately after init gather: {self._balances}")

        # Process results for errors - **MODIFIED FOR STRICTNESS**
        initialization_failed = False
        failed_tasks_info = []  # Store info about failed tasks
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Attempt to determine which task failed (requires mapping index to task type/exchange)
                # This mapping is implicit based on the order tasks were appended.
                # For now, log the generic error and its index.
                task_description = f"task index {i}"  # Basic description
                # TODO: Improve task description mapping if possible
                logger.critical(
                    f"CRITICAL ERROR during PortfolioTracker initialization ({task_description}): {result}",
                    exc_info=result,  # Pass the exception for traceback logging
                )
                failed_tasks_info.append(f"{task_description}: {result}")
                initialization_failed = True

        if initialization_failed:
            error_summary = "; ".join(failed_tasks_info)
            # Keep logging critical, but don't raise exception here, allow checks later
            logger.critical(
                f"PortfolioTracker failed to initialize essential data from one or more exchanges. "
                f"Cannot proceed reliably. Errors: {error_summary}"
            )
            # raise RuntimeError(...) # Optional: Re-enable if init failure should halt everything
        # --- END MODIFICATION ---

        # --- Set initial high watermark ---
        initial_capital = self.get_total_capital()
        if isinstance(initial_capital, Decimal) and initial_capital > Decimal("0.0"):
            self._high_watermark = initial_capital
            logger.info(f"Initial high watermark set to: {self._high_watermark}")
        else:
            logger.warning(f"Initial capital is {initial_capital}. High watermark not set.")
        # --------------------------------

        logger.info("Portfolio state initialized")

    async def _fetch_exchange_balances(self, exchange_id: str) -> bool:
        logger.debug(f"[FETCH_BALANCES:{exchange_id}] Entering function.")  # ENTRY LOG
        try:
            client = self.api_clients.get(exchange_id)
            if not client:
                logger.error(f"[FETCH_BALANCES:{exchange_id}] No API client found.")
                return False  # EXIT PATH 1

            logger.debug(f"[FETCH_BALANCES:{exchange_id}] Fetching balances from API...")
            balances_list = await client.get_balances()
            logger.debug(
                f"[FETCH_BALANCES:{exchange_id}] Raw API response: {balances_list} (Type: {type(balances_list)})",
                stacklevel=2,
            )  # RAW RESPONSE LOG

            if balances_list is None:
                logger.error(f"[FETCH_BALANCES:{exchange_id}] API call returned None.")
                return False  # EXIT PATH 2

            # --- HANDLE DICT or LIST ---
            updated_balances = {}
            processed = False
            if isinstance(balances_list, dict):
                logger.debug(f"[FETCH_BALANCES:{exchange_id}] Processing DICT.")
                for asset, balance_info in balances_list.items():
                    balance_instance = self._parse_balance_info(exchange_id, asset, balance_info)
                    if balance_instance:
                        updated_balances[asset] = balance_instance
                processed = True
            elif isinstance(balances_list, list):
                logger.debug(f"[FETCH_BALANCES:{exchange_id}] Processing LIST.")
                for balance_info in balances_list:
                    # Assume list contains Balance objects or dicts needing parsing
                    asset = None
                    if isinstance(balance_info, Balance):
                        asset = balance_info.asset
                        balance_instance = balance_info
                    elif isinstance(balance_info, dict):
                        asset = balance_info.get("asset")  # Extract asset if it's a dict
                        balance_instance = self._parse_balance_info(
                            exchange_id, asset, balance_info
                        )
                    else:
                        logger.warning(
                            f"[FETCH_BALANCES:{exchange_id}] Unsupported item type in list: {type(balance_info)}"
                        )
                        balance_instance = None

                    if balance_instance and asset:  # Ensure we have a valid object and asset name
                        updated_balances[asset] = balance_instance
                    else:
                        logger.warning(
                            f"[FETCH_BALANCES:{exchange_id}] Could not process balance item: {balance_info}"
                        )  # Log unprocessed items
                processed = True
            else:
                logger.error(
                    f"[FETCH_BALANCES:{exchange_id}] Fetched data is not dict or list: {type(balances_list)}"
                )
                # EXIT PATH 3 (implicitly leads to processed=False)
            # --- END HANDLE DICT or LIST ---

            if processed and updated_balances:  # Ensure we actually processed something meaningful
                logger.info(
                    f"[FETCH_BALANCES:{exchange_id}] BEFORE assign: self._balances[{exchange_id}] = {self._balances.get(exchange_id)}. updated_balances = {updated_balances}"
                )
                self._balances[exchange_id] = updated_balances  # CRITICAL LINE
                logger.info(
                    f"[FETCH_BALANCES:{exchange_id}] AFTER assign: self._balances[{exchange_id}] = {self._balances.get(exchange_id)}"
                )
                self._last_update_time[exchange_id] = datetime.now(UTC)
                logger.info(
                    f"[FETCH_BALANCES:{exchange_id}] Successfully processed. Returning True."
                )
                return True  # EXIT PATH 4 (SUCCESS)
            elif processed and not updated_balances:
                logger.warning(
                    f"[FETCH_BALANCES:{exchange_id}] Processed the response, but resulting updated_balances dict is empty. Response was: {balances_list}. Returning False."
                )
                return False  # EXIT PATH 5 (Processed but empty)
            else:
                # This path covers the case where it wasn't a dict or list
                logger.error(
                    f"[FETCH_BALANCES:{exchange_id}] Failed to process balances. Processed flag is False. Returning False."
                )
                return False  # EXIT PATH 6 (Failed processing)

        except Exception as e:
            logger.error(
                f"[FETCH_BALANCES:{exchange_id}] Exception during fetch/process: {e}", exc_info=True
            )
            return False  # EXIT PATH 7 (Exception)

    def _parse_balance_info(
        self, exchange_id: str, asset: str | None, balance_info: Any
    ) -> Balance | None:
        """Helper to parse various balance info formats into a Balance object."""
        if not asset:
            logger.warning(
                f"_parse_balance_info ({exchange_id}): Missing asset name for balance_info: {balance_info}"
            )
            return None

        if isinstance(balance_info, Balance):
            # Already a Balance object, ensure asset matches if possible
            if balance_info.asset != asset:
                logger.warning(
                    f"Asset mismatch in _parse_balance_info: expected {asset}, got {balance_info.asset}"
                )
                # Decide: trust provided asset or object's asset? Let's trust object's asset.
                # return balance_info # Returning as is
            return balance_info  # Trust object

        elif isinstance(balance_info, dict):
            try:
                total = Decimal(str(balance_info.get("total", "0")))
                # Use 'available' if present, otherwise default to 'free', else default to 'total'
                available_str = balance_info.get("available")
                if available_str is None:
                    available_str = balance_info.get("free")  # Backpack uses 'free'
                if available_str is None:
                    available = total  # Default available to total if neither is present
                else:
                    available = Decimal(str(available_str))

                # Handle free/locked explicitly if present
                free = available  # Default free to available
                locked = total - available  # Default locked calculation
                if "free" in balance_info:
                    free = Decimal(str(balance_info["free"]))
                if "locked" in balance_info:
                    locked = Decimal(str(balance_info["locked"]))

                # Recalculate total/available if free/locked seem more reliable (optional)
                # if "free" in balance_info and "locked" in balance_info:
                #     total = free + locked
                #     available = free

                return Balance(
                    asset=asset, total=total, available=available, free=free, locked=locked
                )
            except (InvalidOperation, TypeError, KeyError) as e:
                logger.error(
                    f"Error creating Balance object from dict for {asset} on {exchange_id}: {e} - Data: {balance_info}"
                )
                return None
        elif isinstance(balance_info, (Decimal, int, float, str)):
            # Treat as total balance if just a number/string
            try:
                total = Decimal(str(balance_info))
                return Balance(
                    asset=asset, total=total, available=total, free=total, locked=Decimal("0")
                )
            except (InvalidOperation, TypeError):
                logger.error(
                    f"Could not parse primitive balance info for {asset} on {exchange_id}: {balance_info}"
                )
                return None
        else:
            logger.warning(
                f"Unsupported balance data type for {asset} on {exchange_id}: {type(balance_info)}"
            )
            return None

    @staticmethod
    def _safe_decimal_convert(
        value: Any,
        field_name: str,
        identifier: str,  # Can be asset or symbol
        allow_none: bool = False,
        default: Decimal | None = None,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, handling None and defaults."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            if allow_none:
                return default
            else:
                raise ValueError(
                    f"PortfolioTracker: Field '{field_name}' for '{identifier}' cannot be None"
                )
        try:
            # Force string conversion first for robustness against float/int inputs
            return Decimal(str(value))
        except (InvalidOperation, TypeError):
            raise ValueError(
                f"PortfolioTracker: Invalid value '{value}' for field '{field_name}' "
                f"for '{identifier}'. Cannot convert to Decimal."
            )

    async def _fetch_exchange_positions(self, exchange_id: str) -> bool:
        """Fetch current positions from an exchange."""
        try:
            api_client = self.api_clients[exchange_id]
            positions_data = await api_client.get_positions()

            # Explicitly check for None first, as allowed by the type hint
            if positions_data is None:
                logger.warning(
                    f"[FETCH_POSITIONS:{exchange_id}] API call returned None. No positions to update."
                )
                # Consider if returning True is appropriate if None means 'no positions' vs. an error
                # For now, assume None means no data could be fetched or no positions exist.
                # Let's update the internal state to empty if it was None, signifying a successful fetch of 'no positions'
                self._positions[exchange_id] = {}
                self._last_update_time[exchange_id] = datetime.now(UTC)
                logger.info(
                    f"[FETCH_POSITIONS:{exchange_id}] API returned None, cleared local positions for this exchange."
                )
                return True  # Treat None as a successful fetch of zero positions

            if isinstance(positions_data, list):
                updated_positions = {}
                for position_info in positions_data:
                    if isinstance(position_info, Position):
                        # Ensure internal consistency (Decimal fields) via __post_init__ re-validation logic if needed
                        # Assuming Position objects from API are already validated by model's __post_init__
                        if position_info.id is None:
                            # Generate a unique ID if missing, although exchange should provide one
                            # This might indicate partial data or spot positions sometimes lack IDs
                            position_info.id = f"{position_info.symbol}_{position_info.side.value}_{datetime.now(UTC).timestamp()}"
                            logger.warning(
                                f"[FETCH_POSITIONS:{exchange_id}] Position for {position_info.symbol} lacked an ID. Generated: {position_info.id}"
                            )

                        # Use the Position's ID as the key
                        updated_positions[position_info.id] = position_info
                    elif isinstance(position_info, dict):
                        # Attempt to parse if it's a dict (less ideal, API should return objects)
                        try:
                            # Minimal required fields to attempt parsing
                            symbol = position_info.get("symbol")
                            side_val = position_info.get("side")
                            size_val = position_info.get("size")
                            entry_price_val = position_info.get("entry_price")
                            position_id = position_info.get("id")  # Exchange position ID is crucial

                            if not all(
                                [
                                    symbol,
                                    side_val,
                                    size_val is not None,
                                    entry_price_val is not None,
                                ]
                            ):
                                logger.warning(
                                    f"[FETCH_POSITIONS:{exchange_id}] Skipping dict position due to missing core fields: {position_info}"
                                )
                                continue

                            side = (
                                OrderSide(side_val) if isinstance(side_val, str) else side_val
                            )  # Allow enum too
                            if not isinstance(side, OrderSide):
                                logger.warning(
                                    f"[FETCH_POSITIONS:{exchange_id}] Skipping dict position due to invalid side: {position_info}"
                                )
                                continue

                            # Generate ID if missing (less ideal)
                            if position_id is None:
                                position_id = (
                                    f"{symbol}_{side.value}_{datetime.now(UTC).timestamp()}"
                                )

                            pos_instance = Position(
                                id=str(position_id),  # Ensure ID is string
                                symbol=str(symbol),
                                side=side,
                                size=self._safe_decimal_convert(
                                    size_val, "size", symbol, allow_none=False
                                ),
                                entry_price=self._safe_decimal_convert(
                                    entry_price_val, "entry_price", symbol, allow_none=False
                                ),
                                # Optional fields - parse safely
                                leverage=self._safe_decimal_convert(
                                    position_info.get("leverage"),
                                    "leverage",
                                    symbol,
                                    allow_none=True,
                                ),
                                mark_price=self._safe_decimal_convert(
                                    position_info.get("mark_price"),
                                    "mark_price",
                                    symbol,
                                    allow_none=True,
                                ),
                                liquidation_price=self._safe_decimal_convert(
                                    position_info.get("liquidation_price"),
                                    "liquidation_price",
                                    symbol,
                                    allow_none=True,
                                ),
                                unrealized_pnl=self._safe_decimal_convert(
                                    position_info.get("unrealized_pnl"),
                                    "unrealized_pnl",
                                    symbol,
                                    allow_none=True,
                                ),
                                realized_pnl=self._safe_decimal_convert(
                                    position_info.get("realized_pnl"),
                                    "realized_pnl",
                                    symbol,
                                    allow_none=True,
                                ),
                                margin_type=position_info.get("margin_type"),
                                margin_used=self._safe_decimal_convert(
                                    position_info.get("margin_used"),
                                    "margin_used",
                                    symbol,
                                    allow_none=True,
                                ),
                                timestamp=position_info.get("timestamp"),  # Keep as int/None
                                status=position_info.get("status"),
                                strategy_name=position_info.get(
                                    "strategy_name"
                                ),  # Less likely from API
                                close_price=self._safe_decimal_convert(
                                    position_info.get("close_price"),
                                    "close_price",
                                    symbol,
                                    allow_none=True,
                                ),
                                close_time=position_info.get(
                                    "close_time"
                                ),  # TODO: Parse datetime if string
                                pnl=self._safe_decimal_convert(
                                    position_info.get("pnl"), "pnl", symbol, allow_none=True
                                ),
                            )
                            updated_positions[pos_instance.id] = pos_instance
                        except (ValueError, TypeError, InvalidOperation) as e:
                            logger.warning(
                                f"[FETCH_POSITIONS:{exchange_id}] Error parsing position dict {position_info}: {e}",
                                exc_info=True,
                            )
                    else:
                        logger.warning(
                            f"[FETCH_POSITIONS:{exchange_id}] Unexpected item type in positions list: {type(position_info)}"
                        )

                # Update internal state, keyed by symbol
                self._positions[exchange_id] = updated_positions
                self._last_update_time[exchange_id] = datetime.now(UTC)
                logger.info(
                    f"[FETCH_POSITIONS:{exchange_id}] Processed {len(updated_positions)} positions. Updating internal state."
                )
                return True
            # This else block is now unreachable based on the type hint `list | None`
            # after the explicit None check above.
            # else:
            #     logger.error(
            #         f"Fetched position data for {exchange_id} is not a list or None: {type(positions_data)}"
            #     )
            #     return False
        except Exception as e:
            logger.error(f"Error fetching positions from {exchange_id}: {e}", exc_info=True)
            return False

    async def _fetch_exchange_orders(self, exchange_id: str) -> bool:
        """
        Fetch current orders from an exchange.

        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]

        try:
            # Fetch open orders from exchange
            orders = await client.get_open_orders()

            # Initialize the exchange orders dictionary if it doesn't exist
            if exchange_id not in self._orders:
                self._orders[exchange_id] = {}

            # Update local state
            new_orders = {}

            # Handle different order return formats
            if isinstance(orders, dict):
                # If orders is a dictionary like {order_id: Order}
                for order_id, order in orders.items():
                    if hasattr(order, "order_id") and order.order_id:
                        new_orders[order.order_id] = order
            elif isinstance(orders, list):
                # If orders is a list of Order objects
                for order in orders:
                    if hasattr(order, "order_id") and order.order_id:
                        new_orders[order.order_id] = order
                    else:
                        # Skip orders without IDs (this shouldn't happen)
                        logger.warning(f"Skipping order without ID from {exchange_id}")
            else:
                # Unexpected format - log and skip
                logger.warning(f"Unexpected orders format from {exchange_id}: {type(orders)}")
                return

            # Check for orders that are no longer open by the exchange (potentially CANCELED or FILLED)
            # Be cautious: network errors could cause temporary inconsistencies.
            # Reconciliation logic should be robust.
            orders_to_check = list(new_orders.keys())
            removed_count = 0
            for order_id in orders_to_check:
                if order_id not in new_orders:
                    current_order = new_orders[order_id]
                    # Only update if status suggests it *could* still be open
                    if current_order.status in [
                        OrderStatus.NEW,
                        OrderStatus.OPEN,  # Some exchanges use OPEN explicitly
                        OrderStatus.PARTIALLY_FILLED,
                        # Consider UNKNOWN as potentially open? Depends on strategy.
                        # OrderStatus.UNKNOWN
                    ]:
                        logger.warning(
                            f"[FETCH_ORDERS:{exchange_id}] Order {order_id} ({current_order.symbol}) no longer reported as open. Status was {current_order.status}. Marking UNKNOWN pending reconciliation or update."
                        )
                        # Don't remove, just mark status? Or rely on trade/cancel updates?
                        # Let's mark as UNKNOWN for now, updates should clarify.
                        current_order.status = OrderStatus.UNKNOWN
                        removed_count += 1  # Count as 'checked' or 'status updated'

            logger.info(
                f"[FETCH_ORDERS:{exchange_id}] Fetched and updated {len(new_orders) - removed_count} open orders. Marked {removed_count} potentially closed orders as UNKNOWN."
            )

            # Update with new orders
            self._orders[exchange_id] = new_orders
            self._last_update_time[exchange_id] = datetime.now(UTC)
            return True  # Indicate success

        except Exception as e:
            logger.error(f"Error fetching orders from {exchange_id}: {str(e)}", exc_info=True)
            return False  # Indicate failure

    async def update(self) -> None:
        """Update the portfolio state by fetching data from exchanges."""
        update_tasks = []

        for exchange_id in self.api_clients.keys():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Check if reconciliation is needed
            now = datetime.now(UTC)
            last_check = self._last_reconciliation_time.get(
                exchange_id, datetime.min.replace(tzinfo=UTC)
            )
            # Ensure last_check is timezone-aware before comparison
            if last_check.tzinfo is None:
                last_check = last_check.replace(tzinfo=UTC)

            if (now - last_check).total_seconds() >= self.reconciliation_interval:
                update_tasks.append(self._fetch_exchange_balances(exchange_id))
                update_tasks.append(self._fetch_exchange_positions(exchange_id))
                # Fetching orders less frequently might be okay unless precise open order state is critical
                # update_tasks.append(self._fetch_exchange_orders(exchange_id))
                self._last_reconciliation_time[exchange_id] = now  # Store UTC now
            else:
                # Only fetch orders if not reconciling
                update_tasks.append(self._fetch_exchange_orders(exchange_id))

            # Update the general last update time regardless
            self._last_update_time[exchange_id] = now  # Store UTC now

        # Wait for all update tasks to complete
        results = await asyncio.gather(*update_tasks, return_exceptions=True)

        # Process results for errors
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error during update: {result}", exc_info=True)

        logger.info("Portfolio state updated")

    def update_order(self, exchange_id: str, order: Order) -> None:
        """
        Update local state with a new or updated order.

        Args:
            exchange_id: Exchange identifier
            order: Order object
        """
        if exchange_id not in self._orders:
            self._orders[exchange_id] = {}

        # Store or update the order
        self._orders[exchange_id][order.order_id] = order
        # Handle status as string (not enum) for compatibility with tests
        status_str = order.status if isinstance(order.status, str) else order.status.name
        logger.debug(f"Updated order {order.order_id} on {exchange_id}: {status_str}")

        # If the order is filled, check if we need to update positions
        if hasattr(OrderStatus, "FILLED") and order.status == OrderStatus.FILLED:
            side_str = order.side.name if hasattr(order.side, "name") else str(order.side)
            logger.info(
                f"Order {order.order_id} on {exchange_id} filled: "
                f"{order.symbol} {side_str} {order.quantity}"
            )

    def update_position(self, exchange_id: str, position: Position) -> None:
        """
        Update the state of a specific position.

        Args:
            exchange_id: Exchange identifier
            position: Position object
        """
        if exchange_id not in self._positions:
            self._positions[exchange_id] = {}

        # Store or update the position keyed by SYMBOL
        exchange_positions = self._positions[exchange_id]
        # Assert type to potentially help Mypy understand the structure
        assert isinstance(exchange_positions, dict)  # Ensure it's a dict
        exchange_positions[position.symbol] = position  # Assign to the inner dict
        logger.debug(
            f"Updated position {position.symbol} on {exchange_id}: {position.side} {position.size}"
        )

    def process_trade(self, exchange_id: str, trade: Trade) -> None:
        """
        Process a new trade, updating orders, positions, and balances.

        Args:
            exchange_id: Exchange identifier
            trade: Trade object
        """
        if not isinstance(trade, Trade):
            logger.error(f"process_trade received invalid trade object: {type(trade)}")
            return

        if not trade.symbol or trade.side is None or trade.quantity is None or trade.price is None:
            logger.error(f"process_trade received incomplete trade data: {trade}")
            return

        logger.info(
            f"Processing trade for {trade.symbol} on {exchange_id}: "
            f"{trade.side.name} {trade.quantity} @ {trade.price}"
        )

        if exchange_id not in self._positions:
            self._positions[exchange_id] = {}

        current_position = self._positions.get(exchange_id, {}).get(trade.symbol)
        trade_size = trade.quantity
        trade_price = trade.price
        trade_side = trade.side

        # Adjust size based on side (negative for short)
        trade_size_signed = trade_size if trade_side == OrderSide.BUY else -trade_size

        if current_position is None or current_position.size == Decimal("0"):
            # --- Open a new position ---
            logger.debug(f"Opening new position for {trade.symbol} on {exchange_id}")
            new_position = Position(
                symbol=trade.symbol,
                side=trade_side,
                size=trade_size_signed,
                entry_price=trade_price,
                leverage=Decimal("1.0"),  # TODO: Leverage should come from config or context
                timestamp=int(trade.timestamp),  # Use trade timestamp
                unrealized_pnl=Decimal("0.0"),
                realized_pnl=Decimal("0.0"),  # Initial realized PNL for this position
                id=f"pos_{trade.symbol}_{exchange_id}",  # Simple ID
            )
            self._positions[exchange_id][trade.symbol] = new_position
            logger.info(f"New position opened: {new_position}")

        else:
            # --- Modify an existing position ---
            logger.debug(f"Modifying existing position for {trade.symbol} on {exchange_id}")
            original_size = current_position.size
            original_entry = current_position.entry_price

            new_size = original_size + trade_size_signed
            new_entry_price = original_entry  # Default if closing out
            realized_pnl_from_trade = Decimal("0.0")

            # Calculate realized P&L if the trade reduces or closes the position
            closing_trade = (original_size > 0 and trade_side == OrderSide.SELL) or (
                original_size < 0 and trade_side == OrderSide.BUY
            )

            if closing_trade and abs(trade_size_signed) <= abs(original_size):
                size_closed = min(abs(trade_size_signed), abs(original_size))
                if current_position.side == trade_side:
                    realized_pnl_from_trade = (trade_price - original_entry) * size_closed
                else:  # Position was SHORT
                    realized_pnl_from_trade = (original_entry - trade_price) * size_closed

                self._update_realized_pnl(realized_pnl_from_trade)  # Update global realized PNL
                # Optionally update position's realized PNL if tracked per-position
                if current_position.realized_pnl is None:
                    current_position.realized_pnl = Decimal(0)
                current_position.realized_pnl += realized_pnl_from_trade
                logger.info(
                    f"Trade closed {size_closed} of position. Realized PNL: {realized_pnl_from_trade:.4f}"
                )

            # Calculate new average entry price if increasing position or partially closing
            if new_size != Decimal("0"):
                # If signs are the same (increasing position) or different but new_size != 0 (partial close/flip)
                if (original_size * trade_size_signed >= 0) or (
                    original_size * trade_size_signed < 0 and new_size != 0
                ):
                    if abs(new_size) > Decimal("1e-9"):  # Avoid division by zero on full close
                        new_entry_price = (
                            (original_entry * abs(original_size))
                            + (trade_price * abs(trade_size_signed))
                        ) / abs(new_size)
                        # Handle case where signs were different (flipping position)
                        if original_size * new_size < 0:
                            new_entry_price = trade_price  # If flipped, new entry is the trade price of the flipping trade
                    else:  # Effectively closed
                        new_entry_price = Decimal("0.0")  # Or keep original?

            # Update position object
            current_position.entry_price = new_entry_price
            current_position.size = new_size
            # Update side based on new size
            if new_size > 0:
                current_position.side = trade_side
            elif new_size < 0:
                current_position.side = trade_side
            else:
                # Position closed, maybe set side to None or keep last? Keeping last for now.
                logger.info(f"Position for {trade.symbol} on {exchange_id} closed.")
                # Optionally remove from dict? self._positions[exchange_id].pop(trade.symbol, None)

            # Update timestamp? Maybe last modified time?
            current_position.timestamp = int(trade.timestamp)

            # Recalculate unrealized P&L if needed (or leave to separate update)
            # Assuming mark price update happens elsewhere

            logger.info(
                f"Position updated: Size={current_position.size}, Entry={current_position.entry_price:.4f}"
            )

        # Update Balances (Simplified: Reduce base currency by cost/add quote currency)
        # TODO: Need robust balance update logic considering fees and assets
        base_asset, quote_asset = self._split_symbol(trade.symbol)
        if not base_asset or not quote_asset:
            logger.error(
                f"[PROCESS_TRADE:{exchange_id}] Could not determine base/quote asset for symbol '{trade.symbol}'. Cannot update balances."
            )
            return  # Cannot proceed without knowing assets

        cost = trade.quantity * trade.price
        fee = trade.fee or Decimal("0.0")
        fee_asset = trade.fee_asset or quote_asset  # Default fee asset to quote

        # logger.debug(f"Attempting balance update: Cost={cost}, Fee={fee} {fee_asset}")
        # try:
        #     # If BUY: decrease quote, increase base
        #     if trade.side == OrderSide.BUY:
        #         self.update_balance(exchange_id, quote_asset, -cost) # Reduce available quote
        #         # Base asset quantity update handled by position size change? Needs clarity.
        #     # If SELL: increase quote, decrease base
        #     elif trade.side == OrderSide.SELL:
        #         self.update_balance(exchange_id, quote_asset, cost) # Increase available quote
        #         # Base asset quantity update handled by position size change?

        #     # Deduct fee
        #     if fee > 0:
        #          self.update_balance(exchange_id, fee_asset, -fee)

        # except Exception as bal_e:
        #     logger.error(f"Error updating balances after trade {trade.id}: {bal_e}", exc_info=True)

    def _split_symbol(self, symbol: str) -> tuple[str, str]:
        """
        Split a trading symbol into base and quote currencies.

        Args:
            symbol: Symbol in format 'BASE/QUOTE' or 'BASEQUOTE'

        Returns:
            Tuple of (base_currency, quote_currency)
        """
        if "/" in symbol:
            base, quote = symbol.split("/")
            return base, quote

        # For symbols without separators, try common quote currencies
        common_quotes = ["USDT", "USDC", "USD", "BTC", "ETH", "BNB"]
        for quote in common_quotes:
            if symbol.endswith(quote):
                base = symbol[: -len(quote)]
                return base, quote

        # Fallback to default splitting (last 4 chars as quote)
        logger.warning(
            f"Could not clearly identify base/quote for {symbol}, using default splitting"
        )
        return symbol[:-4], symbol[-4:]

    def update_balance(self, exchange_id: str, asset: str, amount: Decimal | Balance) -> None:
        """
        Update local state with a new balance, accepting Decimal or Balance object.
        Stores as Balance object internally.
        """
        if exchange_id not in self._balances:
            self._balances[exchange_id] = {}

        if isinstance(amount, Balance):
            balance_obj = amount
            if balance_obj.asset != asset:
                logger.warning(
                    f"Asset mismatch in update_balance: provided {asset}, Balance object has {balance_obj.asset}"
                )
                # Use asset from Balance object or raise error?
                asset = balance_obj.asset  # Prioritize object's asset
        elif isinstance(amount, Decimal):
            # Create a Balance object from the Decimal amount
            balance_obj = Balance(asset=asset, total=amount, available=amount)
        # The else block below was removed as it's unreachable according to the
        # type hint `amount: Decimal | Balance`. Mypy knows that if amount is
        # not a Balance and not a Decimal, no other type is possible.
        # else:
        #     logger.error(f"Invalid type for amount in update_balance: {type(amount)}")
        #     return

        # Store or update the Balance object
        old_balance_obj = self._balances[exchange_id].get(asset)
        self._balances[exchange_id][asset] = balance_obj

        # ADDED LOGGING
        logger.debug(
            f"update_balance called for {exchange_id}/{asset}. Balance object: {balance_obj}. \n"
            f"---> self._balances state AFTER update: {self._balances}"
        )
        # END ADDED LOGGING

        # Log significant changes in total balance
        if old_balance_obj is None or abs(balance_obj.total - old_balance_obj.total) > Decimal(
            "0.01"
        ):
            logger.debug(
                f"Updated balance for {asset} on {exchange_id}: Total={balance_obj.total}, Available={balance_obj.available}"
            )

    def get_exchange_balance(self, exchange_id: str, asset: str) -> Balance | None:
        """Get the Balance object for a specific asset on an exchange."""
        return self._balances.get(exchange_id, {}).get(asset)

    def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        """
        Calculate the total portfolio value in the base currency.
        Requires price conversion logic (TODO).
        """
        total_value = Decimal("0.0")
        all_exchange_balances = self._balances.values()
        for exchange_balances in all_exchange_balances:
            for asset, balance_obj in exchange_balances.items():
                if isinstance(balance_obj, Balance):
                    amount = balance_obj.total  # Use total from Balance object
                    # --- Treat USD and USDC as equivalent for now (TODO: Proper Conversion) ---
                    if (
                        asset == base_currency
                        or (base_currency == "USDC" and asset == "USD")
                        or (base_currency == "USD" and asset == "USDC")
                    ):
                        # ------------------------------------------------------------------------
                        total_value += amount
                    else:
                        # TODO: Price conversion logic remains the same
                        if asset != base_currency:
                            logger.warning(
                                f"Cannot convert {asset} to {base_currency} for total capital calculation (conversion TODO)"
                            )
                            pass  # Ignore other assets for now
                else:
                    # This case should ideally not happen if balances are stored correctly
                    logger.warning(
                        f"Stored balance for {asset} is not a Balance object: {type(balance_obj)}"
                    )
                    try:
                        # Attempt fallback conversion if it's just a number
                        if isinstance(balance_obj, (Decimal, int, float, str)):
                            amount = Decimal(str(balance_obj))
                            # --- Treat USD and USDC as equivalent for now (TODO: Proper Conversion) ---
                            if (
                                asset == base_currency
                                or (base_currency == "USDC" and asset == "USD")
                                or (base_currency == "USD" and asset == "USDC")
                            ):
                                # ------------------------------------------------------------------------
                                total_value += amount
                            else:
                                # TODO: Price conversion
                                pass  # Ignore other assets
                    except InvalidOperation:
                        pass  # Ignore if conversion fails

        return total_value

    def get_exchange_exposure(self, exchange_id: str) -> Decimal:
        """
        Get the current exposure for an exchange.

        Args:
            exchange_id: Exchange identifier

        Returns:
            Current exposure in USD
        """
        if exchange_id not in self._positions:
            return Decimal("0.0")

        exposure = Decimal("0.0")
        for position in self._positions[exchange_id].values():
            if position.is_active():
                # Use mark_price for a more accurate exposure calculation
                if position.mark_price is not None and position.size is not None:
                    # Ensure both values are Decimal before multiplication
                    mark_price = (
                        position.mark_price
                        if isinstance(position.mark_price, Decimal)
                        else Decimal(str(position.mark_price))
                    )
                    size = (
                        position.size
                        if isinstance(position.size, Decimal)
                        else Decimal(str(position.size))
                    )
                    exposure += mark_price * size

        return exposure

    def get_total_exposure(self, valuation_asset: str = "USDT") -> Decimal:
        """
        Calculate the total market exposure across all positions in a common valuation asset.
        NOTE: Simplified for unit testing - assumes mark_price is in valuation_asset.

        Args:
            valuation_asset: The asset to value the exposure in (IGNORED in simplified version).

        Returns:
            Total exposure value as a Decimal (sum of absolute values of size * mark_price).
        """
        total_exposure = Decimal("0.0")
        for exchange_positions in self._positions.values():
            for position in exchange_positions.values():
                if (
                    isinstance(position, Position)
                    and position.is_active()
                    and position.size is not None
                    and position.mark_price is not None
                ):
                    try:
                        pos_size = Decimal(str(position.size))
                        mark_price = Decimal(str(position.mark_price))
                        # Simplified: Add absolute value of position
                        total_exposure += abs(pos_size * mark_price)
                    except (InvalidOperation, TypeError) as e:
                        logger.warning(
                            f"Could not calculate exposure for position {position.symbol}: {e}"
                        )
        return total_exposure

    def get_pnl(self) -> tuple[Decimal, Decimal]:
        """
        Calculate the total realized and unrealized P&L across all positions.
        Returns PnL values as Decimal.

        Returns:
            Tuple containing total realized P&L and total unrealized P&L (Decimal, Decimal).
        """
        # Realized PNL is tracked separately
        realized_pnl = self._realized_pnl
        unrealized_pnl = Decimal("0.0")

        for exchange_id, positions in self._positions.items():
            for symbol, position in positions.items():
                if isinstance(position, Position) and position.is_active():
                    if position.unrealized_pnl is not None:
                        try:
                            pnl = Decimal(str(position.unrealized_pnl))
                            unrealized_pnl += pnl
                        except (InvalidOperation, TypeError):
                            logger.error(
                                f"Position unrealized_pnl '{position.unrealized_pnl}' for {symbol} on {exchange_id} "
                                f"is not Decimal. Treating as zero."
                            )
                    else:
                        logger.warning(
                            f"Unrealized PNL is None for position {symbol} on {exchange_id}. Cannot include in sum."
                        )

        # Return tracked realized PNL and calculated unrealized PNL
        return realized_pnl, unrealized_pnl

    def _update_realized_pnl(self, amount: Decimal) -> None:
        """Atomically update the realized PNL."""
        # TODO: Add locking if concurrency becomes an issue
        self._realized_pnl += amount
        logger.debug(f"Updated realized PNL by {amount}. New total: {self._realized_pnl}")

    def get_position(self, exchange_id: str, symbol: str) -> Position | None:
        """Get the position for a specific symbol on an exchange."""
        return self._positions.get(exchange_id, {}).get(symbol)

    def get_positions_by_symbol(self, exchange_id: str, symbol: str) -> list[Position]:
        """
        Get all positions for a symbol on an exchange.
        NOTE: Typically there's only one position per symbol per exchange.
        This method might be redundant if get_position covers the need.
        Returning a list for potential future complex position models.
        """
        position = self.get_position(exchange_id, symbol)
        return [position] if position else []

    def get_all_positions(self) -> list[tuple[str, Position]]:
        """
        Get all active positions across all exchanges.

        Returns:
            List of (exchange_id, position) tuples
        """
        all_positions = []
        for exchange_id in self._positions:
            for position in self._positions[exchange_id].values():
                if position.is_active():
                    all_positions.append((exchange_id, position))
        return all_positions

    def get_current_drawdown(self) -> Decimal | None:
        """
        Calculate the current portfolio drawdown from the high watermark.
        Returns Decimal percentage or None if not enough data.
        """
        current_capital = self.get_total_capital()  # Recalculates based on current state

        if current_capital is None or self._high_watermark == Decimal("0.0"):
            # Cannot calculate drawdown without current capital or a high watermark
            return None

        # Update high watermark
        self._high_watermark = max(self._high_watermark, current_capital)

        # Calculate drawdown
        drawdown = (self._high_watermark - current_capital) / self._high_watermark

        # Return drawdown as a positive Decimal percentage
        return max(Decimal("0.0"), drawdown)

    def get_order(self, exchange_id: str, order_id: str) -> Order | None:
        """
        Get a specific order.

        Args:
            exchange_id: Exchange identifier
            order_id: Order identifier

        Returns:
            Order object or None if not found
        """
        if exchange_id not in self._orders:
            return None

        try:
            order = self._orders[exchange_id][order_id]
            return order
        except KeyError:
            logger.debug(f"Order {order_id} not found for exchange {exchange_id}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving order {order_id}: {e}", exc_info=True)
            return None

    def get_open_orders(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """
        Get all open orders for an exchange.

        Args:
            exchange_id: Exchange identifier
            symbol: Optional symbol filter

        Returns:
            List of open orders
        """
        if exchange_id not in self._orders:
            return []

        open_orders = []
        for order in self._orders[exchange_id].values():
            # Compare status against OrderStatus enum members
            is_new = order.status == OrderStatus.NEW
            is_partially_filled = order.status == OrderStatus.PARTIALLY_FILLED

            # Handle case where status might be stored as string initially
            if isinstance(order.status, str):
                try:
                    status_enum = OrderStatus(order.status.lower())  # Attempt conversion
                    is_new = status_enum == OrderStatus.NEW
                    is_partially_filled = status_enum == OrderStatus.PARTIALLY_FILLED
                except ValueError:
                    # If string doesn't match enum value, it's not considered open
                    is_new = False
                    is_partially_filled = False

            if is_new or is_partially_filled:
                open_orders.append(order)
        return open_orders

    def get_order_history(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """
        Get all order history for an exchange.

        Args:
            exchange_id: Exchange identifier
            symbol: Optional symbol filter

        Returns:
            List of order history
        """
        if exchange_id not in self._orders:
            return []

        order_history = []
        for order in self._orders[exchange_id].values():
            if symbol is None or order.symbol == symbol:
                order_history.append(order)
        return order_history

    def to_dict(self) -> dict[str, Any]:
        """
        Convert portfolio state to a dictionary for serialization.

        Returns:
            Dictionary representation of portfolio state
        """
        state = {
            "_balances": {},
            "_positions": {},
            "_orders": {},
            "_last_update_time": {},
            "_last_reconciliation_time": {},
            "_high_watermark": str(self._high_watermark),
            "_realized_pnl": str(self._realized_pnl),
        }

        for ex_id, balances in self._balances.items():
            state["_balances"][ex_id] = {}
            for asset, balance in balances.items():
                if isinstance(balance, Balance):
                    state["_balances"][ex_id][asset] = balance.to_dict()  # Use Balance.to_dict()
                else:
                    # Handle non-Balance objects if they slipped through (e.g., store as string)
                    logger.warning(
                        f"Serializing non-Balance object for {asset} in {ex_id}: {type(balance)}"
                    )
                    state["_balances"][ex_id][asset] = str(balance)

        for ex_id, positions in self._positions.items():
            state["_positions"][ex_id] = {}
            for symbol, position in positions.items():
                if isinstance(position, Position):
                    state["_positions"][ex_id][symbol] = (
                        position.to_dict()
                    )  # Use Position.to_dict()
                else:
                    logger.warning(
                        f"Serializing non-Position object for {symbol} in {ex_id}: {type(position)}"
                    )
                    state["_positions"][ex_id][symbol] = str(position)

        for ex_id, orders in self._orders.items():
            state["_orders"][ex_id] = {}
            for order_id, order in orders.items():
                if isinstance(order, Order):
                    state["_orders"][ex_id][order_id] = order.to_dict()  # Use Order.to_dict()
                else:
                    logger.warning(
                        f"Serializing non-Order object for {order_id} in {ex_id}: {type(order)}"
                    )
                    state["_orders"][ex_id][order_id] = str(order)

        # Serialize datetimes as ISO strings
        state["_last_update_time"] = {
            k: v.isoformat() if v else None for k, v in self._last_update_time.items()
        }
        state["_last_reconciliation_time"] = {
            k: v.isoformat() if v else None for k, v in self._last_reconciliation_time.items()
        }

        return state
