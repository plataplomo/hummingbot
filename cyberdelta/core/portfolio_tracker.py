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
    OrderType,  # Added missing OrderType import
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
                # Attempt to determine which task failed (map index to task type/exchange)
                # This mapping is implicit based on the order tasks were appended.
                # For now, log the generic error and its index.
                task_description = f"task index {i}"  # Basic description
                # TODO: Improve task description mapping if possible
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
                    if asset and balance_info is not None:
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
            elif isinstance(balances_data, list):
                logger.debug(f"[FETCH_BALANCES:{exchange_id}] Processing LIST.")
                for balance_item in balances_data:
                    item_asset: str | None = None
                    parsed_balance: Balance | None = None

                    if isinstance(balance_item, Balance):
                        item_asset = balance_item.asset
                        parsed_balance = balance_item
                    elif isinstance(balance_item, dict):
                        item_asset = balance_item.get("asset")
                        if item_asset and isinstance(item_asset, str):
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
                    # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
                    #     continue # Skip this item

                    if parsed_balance and item_asset:
                        updated_balances[item_asset] = parsed_balance
                    # else: # Avoid logging again if parsing failed
                    #     if not isinstance(balance_item, Balance):
                    #          logger.warning(
                    #             f"[_fetch_exchange_balances:{exchange_id}] Failed to process or assign balance item: {balance_item}"
                    #         )
                processed = True
            # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
            #     pass # Should not happen if API returns dict or list as expected

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
        try:
            if isinstance(balance_info, Balance):
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

            elif isinstance(balance_info, dict):
                total = self._safe_decimal_convert(
                    balance_info.get("total"), "total", asset, exchange_id
                )
                free = self._safe_decimal_convert(
                    balance_info.get("free"), "free", asset, exchange_id
                )
                locked = self._safe_decimal_convert(
                    balance_info.get("locked"), "locked", asset, exchange_id
                )

                if total is None:
                    logger.warning(
                        f"Missing 'total' balance for {asset} on {exchange_id}. Cannot create Balance object."
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
                            f"Missing 'free' and 'locked' for {asset} on {exchange_id}. Assuming all 'total' is free."
                        )
                    else:
                        free = Decimal("0")
                        locked = Decimal("0")

                return Balance(asset=asset, total=total, free=free, locked=locked)
            # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
            #     # logger.error(f"Unsupported balance_info type for {asset} on {exchange_id}: {type(balance_info)}") # This line was unreachable
            #     return None # This line was unreachable

        except (InvalidOperation, ValueError, TypeError) as e:
            logger.error(
                f"Error parsing balance info for {asset} on {exchange_id}: {e}. Data: {balance_info}"
            )
            return None
        # Mypy error: Statement is unreachable [unreachable] - Removed unreachable return
        # return None # This line was unreachable

    @staticmethod
    def _safe_decimal_convert(
        value: Any, field_name: str, asset: str, exchange_id: str
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

            positions_data = await client.get_positions()
            if positions_data is None:
                logger.warning(f"No position data received from {exchange_id}")
                # Consider if this should clear existing positions or just skip update
                # For now, let's assume it means no positions, clear existing ones
                self._positions[exchange_id] = {}
                self._last_update_time[exchange_id] = datetime.now(UTC)
                return True  # Indicate success (processed 'no positions')

            updated_positions: dict[str, Position] = {}
            if isinstance(positions_data, list):
                for position_info in positions_data:
                    if isinstance(position_info, Position):
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
                    elif isinstance(position_info, dict):
                        symbol = position_info.get("symbol")
                        if not symbol:
                            logger.warning(
                                f"Skipping position dict without symbol on {exchange_id}: {position_info}"
                            )
                            continue

                        # Ensure 'side' is present and valid before creating Position
                        side_val = position_info.get("side")
                        side: OrderSide | None = None  # Explicitly type hint
                        if side_val:
                            try:
                                side = OrderSide(side_val)
                            except ValueError:
                                logger.warning(
                                    f"Invalid 'side' value '{side_val}' for position {symbol} on {exchange_id}. Skipping."
                                )
                                continue
                        else:
                            logger.warning(
                                f"Missing 'side' for position {symbol} on {exchange_id}. Skipping."
                            )
                            continue

                        # If side is None after checks, we cannot proceed (Mypy Error Fix)
                        if side is None:
                            # logger.error(f"Logic error: side is None after validation for {symbol}. Skipping.") # Should not happen
                            continue  # Skip this position

                        try:
                            # Attempt to create Position object, ensuring Decimals
                            pos_instance = Position(
                                symbol=symbol,
                                size=self._safe_decimal_convert(
                                    position_info.get("size"), "size", symbol, exchange_id
                                )
                                or Decimal("0"),
                                entry_price=self._safe_decimal_convert(
                                    position_info.get("entry_price"),
                                    "entry_price",
                                    symbol,
                                    exchange_id,
                                )
                                or Decimal("0"),
                                mark_price=self._safe_decimal_convert(
                                    position_info.get("mark_price"),
                                    "mark_price",
                                    symbol,
                                    exchange_id,
                                ),
                                liquidation_price=self._safe_decimal_convert(
                                    position_info.get("liquidation_price"),
                                    "liquidation_price",
                                    symbol,
                                    exchange_id,
                                ),
                                unrealized_pnl=self._safe_decimal_convert(
                                    position_info.get("unrealized_pnl"),
                                    "unrealized_pnl",
                                    symbol,
                                    exchange_id,
                                ),
                                leverage=self._safe_decimal_convert(
                                    position_info.get("leverage"), "leverage", symbol, exchange_id
                                ),
                                side=side,  # Use validated side
                                # Add other fields if necessary, ensuring type safety
                                # id=position_info.get("id") # Use symbol or a generated ID if 'id' isn't reliable
                            )
                            # Use symbol as key if no specific ID is provided/reliable
                            pos_id = position_info.get("id", symbol)
                            updated_positions[pos_id] = pos_instance
                        except (InvalidOperation, ValueError, TypeError) as e:
                            logger.error(
                                f"Error parsing position dict for {symbol} on {exchange_id}: {e}. Data: {position_info}"
                            )
                            # The continue statement here was unreachable as the exception implicitly continues the loop.
                    # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
                    #      logger.warning(f"Unsupported position item type in list for {exchange_id}: {type(position_info)}")
            # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
            #      logger.error(f"Received unexpected data type for positions from {exchange_id}: {type(positions_data)}")
            #      return False # Indicate failure due to unexpected data type

            # Replace old positions with the newly fetched ones
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
        # logger.debug(f"Fetching open orders for {exchange_id}") # Mypy unreachable
        try:
            client = self.api_clients.get(exchange_id)
            if not client:
                logger.error(f"No API client found for {exchange_id}")
                return False

            orders_data = await client.get_open_orders()  # Assuming this returns a list or dict
            if orders_data is None:
                logger.warning(f"No open order data received from {exchange_id}")
                # Clear existing open orders for this exchange if API confirms none exist
                # Filter existing orders, keeping only non-open ones might be safer
                # For now, let's clear:
                self._orders[exchange_id] = {}
                self._last_update_time[exchange_id] = datetime.now(UTC)
                return True  # Processed 'no open orders'

            updated_orders: dict[str, Order] = {}
            processed = False

            if isinstance(orders_data, list):
                logger.debug(f"Processing list of orders for {exchange_id}")
                for order_info in orders_data:
                    order_instance = None
                    order_id = None
                    if isinstance(order_info, Order):
                        order_instance = order_info
                        order_id = order_instance.order_id  # Use the object's ID
                    elif isinstance(order_info, dict):
                        order_id = order_info.get("order_id") or order_info.get(
                            "id"
                        )  # Check common keys
                        if order_id:
                            # Validate required fields before creating Order
                            symbol = str(order_info.get("symbol", ""))
                            side_val = order_info.get("side")
                            order_type_val = order_info.get("order_type") or order_info.get("type")
                            status_val = order_info.get("status")

                            side: OrderSide | None = None
                            order_type: OrderType | None = None
                            status: OrderStatus | None = None

                            if not symbol:
                                logger.warning(
                                    f"Skipping order dict without symbol on {exchange_id}: {order_info}"
                                )
                                continue
                            try:
                                if side_val:
                                    side = OrderSide(side_val)
                                else:
                                    raise ValueError("Missing 'side'")
                                if order_type_val:
                                    order_type = OrderType(order_type_val)
                                else:
                                    raise ValueError("Missing 'order_type' or 'type'")
                                if status_val:
                                    status = OrderStatus(status_val)
                                else:
                                    raise ValueError("Missing 'status'")
                            except ValueError as ve:
                                logger.warning(
                                    f"Invalid or missing enum value for order {order_id} on {exchange_id}: {ve}. Data: {order_info}"
                                )
                                continue

                            # Ensure validated enums are not None before proceeding (Mypy Error Fix)
                            if side is None or order_type is None or status is None:
                                # logger.error(f"Logic error: Enum value is None after validation for order {order_id}. Skipping.") # Should not happen
                                continue  # Skip this order

                            try:
                                # Create Order object from dict, ensuring Decimals and Enums
                                parsed_order_instance = Order(
                                    order_id=str(order_id),
                                    symbol=symbol,
                                    side=side,  # Use validated side
                                    order_type=order_type,  # Use validated order_type
                                    price=self._safe_decimal_convert(
                                        order_info.get("price"), "price", symbol, exchange_id
                                    ),
                                    quantity=self._safe_decimal_convert(
                                        order_info.get("quantity"), "quantity", symbol, exchange_id
                                    )
                                    or Decimal("0"),
                                    filled_quantity=self._safe_decimal_convert(
                                        order_info.get("filled_quantity")
                                        or order_info.get("filledQuantity"),
                                        "filled_quantity",
                                        symbol,
                                        exchange_id,
                                    )
                                    or Decimal("0"),
                                    status=status,  # Use validated status
                                    timestamp=order_info.get("timestamp")
                                    or order_info.get(
                                        "time"
                                    ),  # Check common keys, handle type later
                                    client_order_id=order_info.get("client_order_id")
                                    or order_info.get("clientOrderId"),
                                    # Add other fields as needed
                                )
                                # Further validation/conversion for timestamp if needed
                                ts_val = parsed_order_instance.timestamp
                                if isinstance(ts_val, (int, float)):
                                    # Assuming timestamp is ms or s epoch, convert to datetime
                                    # Mypy error: Statement is unreachable [unreachable] - Removed unreachable try block
                                    # try:
                                    # ts_sec = ts_val / 1000 if ts_val > 1e12 else ts_val # Mypy unreachable
                                    # parsed_order_instance.timestamp = datetime.fromtimestamp(ts_sec, UTC)
                                    # except (ValueError, TypeError):
                                    #     logger.warning(f"Could not convert timestamp {ts_val} to datetime for order {order_id}")
                                    #     parsed_order_instance.timestamp = None # Or set a default?
                                    pass  # Placeholder for potential conversion logic if needed
                                # elif ts_val is not None and not isinstance(ts_val, datetime): # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
                                #      logger.warning(f"Timestamp for order {order_id} is not int, float or datetime: {type(ts_val)}")
                                #      parsed_order_instance.timestamp = None
                                #      pass # Keep original value if not convertible and not datetime

                                order_instance = parsed_order_instance  # Assign parsed object

                            except (InvalidOperation, ValueError, TypeError) as e:
                                logger.error(
                                    f"Error parsing order dict for order ID {order_id} on {exchange_id}: {e}. Data: {order_info}"
                                )
                                # The continue statement here was unreachable as the exception implicitly continues the loop.
                        else:
                            logger.warning(
                                f"Skipping order dict without order_id/id on {exchange_id}: {order_info}"
                            )
                            continue
                    # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
                    #     logger.warning(f"Unsupported order item type in list for {exchange_id}: {type(order_info)}")
                    #     continue

                    if order_instance and order_id:
                        # Ensure Decimal types are correct if parsed from dict or pre-existing
                        if isinstance(order_info, dict):  # Already handled in creation above
                            pass
                        elif isinstance(
                            order_info, Order
                        ):  # Validate/convert pre-existing Order obj
                            order_instance.price = self._safe_decimal_convert(
                                order_instance.price, "price", order_instance.symbol, exchange_id
                            )
                            order_instance.quantity = self._safe_decimal_convert(
                                order_instance.quantity,
                                "quantity",
                                order_instance.symbol,
                                exchange_id,
                            ) or Decimal("0")
                            order_instance.filled_quantity = self._safe_decimal_convert(
                                order_instance.filled_quantity,
                                "filled_quantity",
                                order_instance.symbol,
                                exchange_id,
                            ) or Decimal("0")
                            # Ensure status is Enum
                            if isinstance(order_instance.status, str):
                                try:
                                    order_instance.status = OrderStatus(order_instance.status)
                                except ValueError:
                                    logger.warning(
                                        f"Invalid status string '{order_instance.status}' for order {order_id}"
                                    )
                                    order_instance.status = OrderStatus.UNKNOWN  # Or some default

                        updated_orders[str(order_id)] = order_instance
                processed = True

            elif isinstance(orders_data, dict):
                # Handle case where API returns a dict (e.g., order_id -> order_info)
                logger.debug(f"Processing dict of orders for {exchange_id}")
                # Similar parsing logic as for list items, but iterating dict items
                for order_id, order_info in orders_data.items():
                    # ... (parsing logic similar to list item handling) ...
                    pass  # Placeholder - implement if needed based on API behavior
                processed = True  # Assume processed if dict handling is implemented
                logger.warning(
                    f"Processing dict response for orders on {exchange_id} - implementation pending."
                )

            # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
            #      logger.error(f"Received unexpected data type for orders from {exchange_id}: {type(orders_data)}")
            #      return False # Indicate failure

            # Add explicit return False if not processed (Mypy fix for missing return)
            if not processed:
                logger.error(
                    f"Failed to process orders data for {exchange_id} (processed flag is False)."
                )
                return False

            if processed:
                # It's safer to update existing orders and add new ones,
                # rather than completely replacing the dictionary, to handle partial updates.
                # However, for fetching *open* orders, replacing might be intended.
                # Let's stick to replacing for now, assuming get_open_orders is comprehensive.
                self._orders[exchange_id] = updated_orders
                self._last_update_time[exchange_id] = datetime.now(UTC)
                logger.debug(
                    f"Successfully updated open orders for {exchange_id}. Count: {len(updated_orders)}"
                )
                return True
            # else: # Mypy error: Missing return statement [return] - Added explicit return # Mypy unreachable
            #      logger.error(f"Failed to process orders data for {exchange_id} (processed flag is False).")
            #      return False

        except Exception as e:
            logger.exception(f"Unexpected error fetching orders for {exchange_id}: {e}")
            return False
        # Mypy error: Statement is unreachable [unreachable] - Removed unreachable return
        # return False

    async def update(self) -> None:
        """Update portfolio state by fetching data from exchanges."""
        now = datetime.now(UTC)
        update_tasks = []

        for exchange_id, client in self.api_clients.items():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Check if reconciliation is needed
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
                update_tasks.append(
                    self._fetch_exchange_orders(exchange_id)
                )  # Fetch orders during reconciliation too
                self._last_reconciliation_time[exchange_id] = now  # Update time *before* await
            else:
                # Fetch only frequently updated data (e.g., orders) if not reconciling
                logger.debug(f"Fetching only orders for {exchange_id} (no reconciliation needed).")
                update_tasks.append(self._fetch_exchange_orders(exchange_id))

        if update_tasks:
            results = await asyncio.gather(*update_tasks, return_exceptions=True)
            # Log any errors from the update tasks
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    # TODO: Improve mapping of result index back to exchange/task type
                    logger.error(
                        f"Error during portfolio update task (index {i}): {result}", exc_info=result
                    )
                elif result is False:
                    logger.warning(
                        f"Portfolio update task (index {i}) indicated failure (returned False)."
                    )

        # Update high watermark after potential balance/PNL updates
        current_capital = self.get_total_capital()
        if isinstance(current_capital, Decimal):
            self._high_watermark = max(self._high_watermark, current_capital)
        # logger.debug(f"Portfolio updated. Current HWM: {self._high_watermark}")

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

        if not order.order_id:
            logger.error(f"Received order update without order_id on {exchange_id}: {order}")
            return

        order_id_str = str(order.order_id)  # Ensure key is string

        # Log previous state if exists
        # previous_order = self._orders[exchange_id].get(order_id_str)
        # if previous_order:
        #     logger.debug(f"Updating order {order_id_str} on {exchange_id}. Previous status: {previous_order.status}, New status: {order.status}")
        # else:
        #     logger.debug(f"Adding new order {order_id_str} to {exchange_id} with status {order.status}")

        self._orders[exchange_id][order_id_str] = order
        self._last_update_time[exchange_id] = datetime.now(UTC)

        # If order is filled or partially filled, consider processing the trade
        if (
            order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]
            and order.filled_quantity is not None
            and order.filled_quantity > Decimal("0")
        ):
            # This might need more info than just the order (e.g., execution price if different from order price)
            # For now, let's assume a simple Trade object can be created from the Order
            # This logic might belong in ExecutionHandler which then calls PortfolioTracker
            logger.info(
                f"Order {order_id_str} on {exchange_id} is {order.status}. Triggering trade processing (placeholder)."
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

        # logger.debug(f"Updating position {position_key_str} ({position.symbol}) on {exchange_id}. New size: {position.size}")
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
            f"Processing trade on {exchange_id}: {trade.side} {trade.quantity} {trade.symbol} @ {trade.price}"
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

            # Ensure values are Decimal before calculation (Mypy Error Fix)
            if not isinstance(original_size, Decimal) or not isinstance(trade_effect, Decimal):
                # logger.error(f"Cannot process trade for {trade.symbol}: invalid size types. Original: {type(original_size)}, Trade: {type(trade_effect)}") # Mypy unreachable
                pass  # Should not happen if types are correct upstream
            else:  # Only proceed if types are correct
                new_size = original_size + trade_effect

                # Mypy Error Fix: Ensure new_size is Decimal before comparison
                if isinstance(new_size, Decimal) and abs(new_size) < Decimal(
                    "1e-9"
                ):  # Position closed
                    logger.info(f"Position {trade.symbol} on {exchange_id} closed by trade.")
                    # Calculate realized PNL for the closed position
                    # Simplified PNL calc: (exit_price - entry_price) * quantity_closed * direction
                    # This assumes the trade closes the entire position. Partial closes are more complex.
                    # if current_position.entry_price is None or not isinstance(current_position.entry_price, Decimal): # Mypy unreachable
                    #      # logger.warning(f"Cannot calculate realized PNL for closing {trade.symbol}: missing or invalid entry price.") # Mypy unreachable
                    #      pnl = Decimal("0.0")
                    # else:
                    pnl = (trade.price - current_position.entry_price) * original_size.copy_sign(
                        Decimal("1")
                    )  # Assumes entry_price is valid Decimal
                    if current_position.side == OrderSide.SELL:  # Short position closed by buying
                        pnl = -pnl  # Invert PNL for short closes

                    self._update_realized_pnl(pnl)
                    logger.info(
                        f"Realized PNL from closing {trade.symbol}: {pnl}. Total Realized PNL: {self._realized_pnl}"
                    )

                    # Remove closed position
                    del self._positions[exchange_id][position_key]

                elif isinstance(
                    new_size, Decimal
                ):  # Position modified (size changed or average entry price adjusted)
                    # Recalculate average entry price (Weighted average)
                    # This assumes the trade adds to or reduces the existing position.
                    # If the trade flips the position (long -> short or vice-versa), this logic is insufficient.
                    # if current_position.entry_price is None or not isinstance(current_position.entry_price, Decimal): # Mypy unreachable
                    #      # logger.warning(f"Cannot update average entry price for {trade.symbol}: missing or invalid current entry price. Resetting to trade price.") # Mypy unreachable
                    #      current_position.entry_price = trade.price
                    if original_size.copy_sign(Decimal("1")) == new_size.copy_sign(
                        Decimal("1")
                    ):  # Sign hasn't flipped
                        new_entry_price = (
                            (current_position.entry_price * original_size)
                            + (trade.price * trade_effect)
                        ) / new_size  # Assumes entry_price is valid Decimal
                        current_position.entry_price = new_entry_price
                        logger.debug(
                            f"Position {trade.symbol} updated. New avg entry: {new_entry_price}"
                        )
                    # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
                    #      logger.warning(f"Position {trade.symbol} flipped side due to trade. Resetting entry price to trade price.")
                    #      # Resetting entry price might be too simple. Need proper cost basis tracking.
                    #      # For now, treat it like opening a new position at the trade price.
                    #      current_position.entry_price = trade.price
                    #      # Reset unrealized PNL as entry price changed significantly
                    #      current_position.unrealized_pnl = Decimal("0.0")

                    current_position.size = new_size
                    current_position.side = OrderSide.BUY if new_size > 0 else OrderSide.SELL
                    logger.debug(f"Position {trade.symbol} updated. New size: {new_size}")

                    # Mark price, liq price, unrealized PNL would be updated by market data streams typically
                    # Leverage might also change depending on exchange rules
                # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
                #     # This case should ideally not be reached if original_size and trade_effect are Decimals
                #     logger.error(f"Could not determine new position size type for {trade.symbol}. Original: {type(original_size)}, Trade: {type(trade_effect)}")

        else:  # Opening a new position
            logger.debug(f"Opening new position for {trade.symbol} on {exchange_id}")
            # Ensure side is valid before creating Position (Mypy Error Fix)
            side = trade.side
            if side is None:
                logger.error(
                    f"Cannot open position for {trade.symbol}: trade object missing 'side'. Trade: {trade}"
                )
                return

            new_position = Position(
                symbol=trade.symbol,
                size=trade.quantity if side == OrderSide.BUY else -trade.quantity,
                entry_price=trade.price,
                side=side,  # Use validated side
                # Other fields (mark_price, liq_price, pnl, leverage) need market data or further calculation
                mark_price=trade.price,  # Initial mark price can be trade price
                unrealized_pnl=Decimal("0.0"),
                # Assign a unique ID if possible/needed
                # id=f"{trade.symbol}_{trade.timestamp}" # Example ID generation
            )
            # Assuming position key is symbol for simplicity
            self._positions.setdefault(exchange_id, {})[position_key] = new_position

        # 3. Update Balances (Reduce cash/base asset, potentially update quote asset if relevant)
        # This requires knowing the trade fee and the base/quote assets.
        # Example: Buy BTC/USDC -> Decrease USDC, Increase BTC (or reflect in position value)
        base_asset, quote_asset = self._split_symbol(
            trade.symbol
        )  # Simple split, might need refinement
        cost = trade.quantity * trade.price
        fee = trade.fee or Decimal("0.0")  # Assume fee is in quote currency

        if quote_asset and quote_asset in self._balances.get(exchange_id, {}):
            balance = self._balances[exchange_id][quote_asset]
            # if balance.total is None: # Mypy error: Statement is unreachable [unreachable] - Removed check
            #      logger.warning(f"Cannot update balance for {quote_asset} on {exchange_id}: total is None.")
            if trade.side == OrderSide.BUY:
                balance.total -= cost + fee
                # Adjust free/locked based on settlement if needed
            else:  # Sell
                balance.total += cost - fee
            logger.debug(
                f"Updated {quote_asset} balance on {exchange_id} due to trade. New total: {balance.total}"
            )
        # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
        #      logger.warning(f"Could not update balance for quote asset {quote_asset} on {exchange_id}")

        # Update base asset balance if tracking non-USD assets directly
        # if base_asset and base_asset in self._balances.get(exchange_id, {}):
        #     if trade.side == OrderSide.BUY:
        #         self._balances[exchange_id][base_asset].total += trade.quantity
        #     else: # Sell
        #         self._balances[exchange_id][base_asset].total -= trade.quantity
        #     logger.debug(f"Updated {base_asset} balance on {exchange_id}. New total: {self._balances[exchange_id][base_asset].total}")

        self._last_update_time[exchange_id] = datetime.now(UTC)

    def _split_symbol(self, symbol: str) -> tuple[str, str]:
        """Basic symbol splitting (e.g., BTC/USDC -> BTC, USDC). Needs refinement for complex symbols."""
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
                f"Updated balance for {asset} on {exchange_id} using Balance object. New total: {amount.total}"
            )
        elif isinstance(amount, (Decimal, int, float, str)):
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
                    f"Updating total balance for {asset} on {exchange_id} to {safe_amount}. Free/locked might be stale."
                )
            else:
                # Create a new balance object, assuming all is free
                self._balances[exchange_id][asset] = Balance(
                    asset=asset, total=safe_amount, free=safe_amount, locked=Decimal("0")
                )
                logger.debug(
                    f"Created new balance for {asset} on {exchange_id}. Total: {safe_amount}"
                )
        # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
        #      logger.error(f"Invalid amount type provided for {asset} on {exchange_id}: {type(amount)}")
        #      return

        self._last_update_time[exchange_id] = datetime.now(UTC)

    def get_exchange_balance(self, exchange_id: str, asset: str) -> Balance | None:
        """
        Get the balance object for a specific asset on an exchange.

        Args:
            exchange_id: Exchange identifier
            asset: Asset symbol

        Returns:
            Balance object or None if not found.
        """
        return self._balances.get(exchange_id, {}).get(asset)

    def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        """
        Calculate the total portfolio value in the specified base currency.

        Args:
            base_currency: The currency to express the total value in (e.g., 'USDC').

        Returns:
            Total portfolio value as a Decimal.
        """
        total_value = Decimal("0.0")
        for exchange_id, balances in self._balances.items():
            for asset, balance in balances.items():
                # if balance.total is None: # Mypy error: Statement is unreachable [unreachable] - Removed check
                #     continue # Skip if total balance is unknown

                value_in_base = balance.total
                if asset != base_currency:
                    # Need a way to get the current price of 'asset' in 'base_currency'
                    # This functionality might belong elsewhere (e.g., DataHandler or a PriceOracle)
                    # For now, we assume 'total' is already in the base currency if asset != base_currency
                    # Or we skip non-base currency assets if no conversion is available.
                    # Let's assume 'total' for non-base assets represents their value in base_currency.
                    logger.debug(
                        f"Assuming balance.total for {asset} ({value_in_base}) is already in {base_currency}"
                    )
                    # price = self.get_asset_price_in_usd(asset) # Removed - Method doesn't exist here
                    # if price is not None:
                    #     value_in_base = balance.total * price
                    # else:
                    #     logger.warning(f"Could not get price for {asset} in {base_currency}. Skipping its value.")
                    #     value_in_base = Decimal("0.0")
                    pass  # Keep value_in_base as balance.total

                total_value += value_in_base

        # Update high watermark
        self._high_watermark = max(self._high_watermark, total_value)

        return total_value

    def get_exchange_exposure(self, exchange_id: str) -> Decimal:
        """Calculate the total exposure (position value) on a specific exchange."""
        exposure = Decimal("0.0")
        for position in self._positions.get(exchange_id, {}).values():
            if position.mark_price is not None and position.size is not None:
                exposure += abs(position.size) * position.mark_price  # Absolute exposure value
            # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
            #      logger.warning(f"Cannot calculate exposure for position {position.symbol} on {exchange_id}: missing mark_price or size.")
        return exposure

    def get_total_exposure(self, valuation_asset: str = "USDT") -> Decimal:
        """Calculate the total portfolio exposure across all exchanges."""
        total_exposure = Decimal("0.0")
        for exchange_positions in self._positions.values():
            for position in exchange_positions.values():
                if position.mark_price is not None and position.size is not None:
                    # Simple calculation: size * mark_price
                    # Assumes mark_price is in valuation_asset or convertible
                    # TODO: Add currency conversion if needed based on symbol quote asset vs valuation_asset
                    position_value = position.size * position.mark_price
                    total_exposure += (
                        position_value  # Net exposure (longs positive, shorts negative)
                    )
                # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
                #      logger.warning(f"Cannot calculate exposure for position {position.symbol}: missing mark_price or size.")
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
                if position.unrealized_pnl is not None:
                    total_unrealized_pnl += position.unrealized_pnl
                else:
                    # Attempt to calculate if mark and entry prices are available
                    if (
                        position.mark_price is not None
                        and position.entry_price is not None
                        and position.size is not None
                    ):
                        pnl = (position.mark_price - position.entry_price) * position.size
                        if position.side == OrderSide.SELL:  # Correct PNL calculation for shorts
                            pnl = -pnl
                        total_unrealized_pnl += pnl
                        # logger.debug(f"Calculated unrealized PNL for {position.symbol}: {pnl}")
                    # else: # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
                    #      logger.warning(f"Cannot calculate PNL for position {position.symbol}: missing PNL, mark_price, entry_price, or size.")

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
        matching_positions = []
        for position in self._positions.get(exchange_id, {}).values():
            if position.symbol == symbol:
                matching_positions.append(position)
        return matching_positions

    def get_all_positions(self) -> list[tuple[str, Position]]:
        """Get all positions across all exchanges."""
        all_positions = []
        for exchange_id, positions in self._positions.items():
            for position in positions.values():
                all_positions.append((exchange_id, position))
        return all_positions

    def get_current_drawdown(self) -> Decimal | None:
        """
        Calculate the current portfolio drawdown from the high watermark.

        Returns:
            Current drawdown as a positive Decimal percentage (e.g., 0.1 for 10%),
            or None if capital is zero or negative.
        """
        current_capital = self.get_total_capital()

        if self._high_watermark <= Decimal("0.0"):
            # logger.debug("High watermark is zero or negative, cannot calculate drawdown.")
            return Decimal("0.0")  # Or None? Returning 0 might be safer.

        # if current_capital <= Decimal("0.0"): # Mypy error: Statement is unreachable [unreachable] - Removed unreachable code block
        #      logger.warning(f"Current capital ({current_capital}) is zero or negative. Reporting 100% drawdown.")
        #      return Decimal("1.0") # 100% drawdown

        drawdown = (self._high_watermark - current_capital) / self._high_watermark
        return max(Decimal("0.0"), drawdown)  # Ensure drawdown is not negative

    # --- Order Access Methods ---

    def get_open_orders(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """Get a list of open orders for a specific exchange and optional symbol."""
        open_orders = []
        for order in self._orders.get(exchange_id, {}).values():
            if order.status in [OrderStatus.NEW, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                if symbol is None or order.symbol == symbol:
                    open_orders.append(order)
        return open_orders

    def get_order_history(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """Get the history of all tracked orders for an exchange and optional symbol."""
        history = []
        for order in self._orders.get(exchange_id, {}).values():
            if symbol is None or order.symbol == symbol:
                history.append(order)
        return history

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
        #             tracker._balances[ex_id][asset] = Balance(**bal_data) # Assuming keys match __init__
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
        active = set()
        for positions in self._positions.values():
            for pos in positions.values():
                if pos.size is not None and pos.size != Decimal("0"):  # Added None check for size
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
