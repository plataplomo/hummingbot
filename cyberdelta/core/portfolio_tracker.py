from __future__ import annotations  # Enable postponed evaluation

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import simplejson as json

from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.core.models import Balance, Order, OrderSide, OrderStatus, OrderType, Position
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger
from cyberdelta.utils.serialization import dump_json

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

        # Add active symbols and watchlist
        self._active_symbols: set[str] = set()
        self._watchlist: set[str] = set()

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
            initialization_tasks.append(self._fetch_exchange_orders(exchange_id))

        logger.info(
            f"PortfolioTracker {id(self)}: About to gather init tasks. "
            f"Balances before: {self._balances}"
        )
        # Wait for all initialization tasks to complete
        results = await asyncio.gather(*initialization_tasks, return_exceptions=True)
        logger.info(
            f"PortfolioTracker {id(self)}: Finished gathering init tasks. "
            f"Balances after: {self._balances}"
        )

        # Process results for errors - **MODIFIED FOR STRICTNESS**
        initialization_failed = False
        failed_tasks_info = [] # Store info about failed tasks
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Attempt to determine which task failed (requires mapping index to task type/exchange)
                # This mapping is implicit based on the order tasks were appended.
                # For now, log the generic error and its index.
                task_description = f"task index {i}" # Basic description
                # TODO: Improve task description mapping if possible
                logger.critical(
                    f"CRITICAL ERROR during PortfolioTracker initialization ({task_description}): {result}",
                    exc_info=result # Pass the exception for traceback logging
                )
                failed_tasks_info.append(f"{task_description}: {result}")
                initialization_failed = True

        if initialization_failed:
            error_summary = "; ".join(failed_tasks_info)
            raise RuntimeError(
                f"PortfolioTracker failed to initialize essential data from one or more exchanges. "
                f"Cannot proceed. Errors: {error_summary}"
            )
        # --- END MODIFICATION ---

        logger.info("Portfolio state initialized")

    async def _fetch_exchange_balances(self, exchange_id: str) -> bool:
        """Fetch current balances from an exchange and store as Balance objects."""
        try:
            api_client = self.api_clients[exchange_id]
            balances_data = await api_client.get_balances()

            # ---> ADD DEBUG LOGGING <---
            logger.debug(
                f"_fetch_exchange_balances ({exchange_id}): Received balances_data: "
                f"{balances_data} (Type: {type(balances_data)})"
            )
            # ---> END DEBUG LOGGING <---

            if isinstance(balances_data, dict):
                updated_balances = {}
                # ---> ADD DEBUG LOGGING <---
                logger.debug(f"_fetch_exchange_balances ({exchange_id}): Starting balance processing loop.")
                # ---> END DEBUG LOGGING <---
                for asset, balance_info in balances_data.items():
                    # ---> ADD DEBUG LOGGING <---
                    logger.debug(
                        f"_fetch_exchange_balances ({exchange_id}): Processing asset='{asset}', "
                        f"balance_info='{balance_info}', type='{type(balance_info)}'"
                    )
                    # ---> END DEBUG LOGGING <---
                    balance_instance = None # Initialize to None
                    if isinstance(balance_info, Balance): # Already a Balance object
                        logger.debug(f"_fetch_exchange_balances ({exchange_id}): Asset '{asset}' is already a Balance object.")
                        balance_instance = balance_info
                    elif isinstance(balance_info, dict): # Attempt to create from dict
                        logger.debug(f"_fetch_exchange_balances ({exchange_id}): Asset '{asset}' is dict, attempting Balance creation.")
                        try:
                            # Adapt based on expected dict structure from API
                            balance_instance = Balance(\
                                asset=asset,\
                                total=Decimal(balance_info.get('total', '0')),\
                                available=Decimal(balance_info.get('available', balance_info.get('total', '0'))) # Use total if available missing
                            )
                            logger.debug(f"_fetch_exchange_balances ({exchange_id}): Successfully created Balance for '{asset}' from dict.")
                        except (TypeError, KeyError, InvalidOperation) as e:
                            logger.error(f"Error creating Balance object from dict for {asset} on {exchange_id}: {e} - Data: {balance_info}", exc_info=True)
                    elif isinstance(balance_info, (int, float, str, Decimal)): # Attempt to create from raw value
                        logger.debug(f"_fetch_exchange_balances ({exchange_id}): Asset '{asset}' is value type, attempting Balance creation.")
                        try:
                            balance_instance = Balance(\
                               asset=asset,\
                               total=Decimal(balance_info),\
                               available=Decimal(balance_info)\
                            )
                            logger.debug(f"_fetch_exchange_balances ({exchange_id}): Successfully created Balance for '{asset}' from value.")
                        except (TypeError, InvalidOperation) as e:
                           logger.error(f"Error creating Balance object from value for {asset} on {exchange_id}: {e} - Value: {balance_info}", exc_info=True)
                    else:
                        logger.warning(f"Unsupported balance data type for {asset} on {exchange_id}: {type(balance_info)}")

                    # ---> ADD DEBUG LOGGING <---
                    if balance_instance:
                        updated_balances[asset] = balance_instance
                        logger.debug(f"_fetch_exchange_balances ({exchange_id}): Added Balance for '{asset}' to updated_balances.")
                    else:
                        logger.debug(f"_fetch_exchange_balances ({exchange_id}): No Balance object created/added for '{asset}'.")
                    # ---> END DEBUG LOGGING <---

                # ---> ADD DEBUG LOGGING <---
                logger.debug(f"_fetch_exchange_balances ({exchange_id}): Finished balance processing loop.")
                # ---> END DEBUG LOGGING <---

                # ---> ADD DEBUG LOGGING <---
                logger.debug(
                    f"_fetch_exchange_balances ({exchange_id}): Assigning updated_balances: "
                    f"{updated_balances}. Current self._balances: {self._balances}"
                )
                # --------> CRITICAL LINE <--------
                self._balances[exchange_id] = updated_balances
                # ---> ADD DEBUG LOGGING <---
                logger.debug(
                    f"_fetch_exchange_balances ({exchange_id}): After assignment, self._balances: "
                    f"{self._balances}"
                )
                # --------> END DEBUG LOGGING <---

                self._last_update_time[exchange_id] = datetime.now(UTC)
                logger.info(f"Fetched and processed balances for {exchange_id}")
                return True
            else:
                logger.error(f"Fetched balance data for {exchange_id} is not a dictionary: {type(balances_data)}")
                return False
        except Exception as e:
            logger.error(f"Error fetching balances from {exchange_id}: {e}", exc_info=True)
            return False

    async def _fetch_exchange_positions(self, exchange_id: str) -> bool:
        """Fetch current positions from an exchange."""
        try:
            api_client = self.api_clients[exchange_id]
            positions_data = await api_client.get_positions()

            if isinstance(positions_data, list):
                updated_positions = {}
                for position_info in positions_data:
                    if isinstance(position_info, Position): # Handle Position object directly
                        position_instance = position_info
                        if position_instance.symbol:
                             updated_positions[position_instance.symbol] = position_instance
                        else:
                             logger.warning(f"Fetched position object missing symbol on {exchange_id}: {position_instance}")
                    elif isinstance(position_info, dict): # Handle dict representation
                        try:
                            # Ensure necessary fields are present and create Position object
                            # Convert numeric strings/floats to Decimal
                            for key in ["size", "entry_price", "mark_price", "liquidation_price", "leverage", "unrealized_pnl"]:
                                if key in position_info and position_info[key] is not None:
                                    position_info[key] = Decimal(str(position_info[key]))
                            # Convert side string to enum
                            if "side" in position_info and isinstance(position_info["side"], str):
                                position_info["side"] = OrderSide(position_info["side"].lower())
                            
                            position_instance = Position(**position_info)
                            if position_instance.symbol:
                                updated_positions[position_instance.symbol] = position_instance
                            else:
                                logger.warning(f"Fetched position dict missing symbol on {exchange_id}: {position_info}")
                        except (TypeError, KeyError, InvalidOperation, ValueError) as e:
                            logger.error(f"Error creating Position object from dict on {exchange_id}: {e} - Data: {position_info}")
                    else:
                        logger.warning(f"Skipping unrecognized position data type on {exchange_id}: {type(position_info)}")

                # Update internal state, keyed by symbol
                self._positions[exchange_id] = updated_positions
                self._last_update_time[exchange_id] = datetime.now(UTC)
                logger.info(f"Fetched and processed positions for {exchange_id}")
                return True
            else:
                logger.error(f"Fetched position data for {exchange_id} is not a list: {type(positions_data)}")
                return False
        except Exception as e:
            logger.error(f"Error fetching positions from {exchange_id}: {e}", exc_info=True)
            return False

    async def _fetch_exchange_orders(self, exchange_id: str) -> None:
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
                    new_orders[order_id] = order
            elif isinstance(orders, list):
                # If orders is a list of Order objects
                for order in orders:
                    if hasattr(order, "id") and order.id:
                        new_orders[order.id] = order
                    else:
                        # Skip orders without IDs (this shouldn't happen)
                        logger.warning(f"Skipping order without ID from {exchange_id}")
            else:
                # Unexpected format - log and skip
                logger.warning(f"Unexpected orders format from {exchange_id}: {type(orders)}")
                return

            # Check for orders that are no longer open
            for order_id, old_order in self._orders[exchange_id].items():
                if order_id not in new_orders and old_order.status in [
                    OrderStatus.NEW,
                    OrderStatus.PARTIALLY_FILLED,
                ]:
                    logger.info(f"Order {order_id} on {exchange_id} is no longer open")

            # Update with new orders
            self._orders[exchange_id] = new_orders
            self._last_update_time[exchange_id] = datetime.now(UTC)

            logger.info(f"Updated {len(new_orders)} orders for {exchange_id}")

        except Exception as e:
            logger.error(f"Error fetching orders from {exchange_id}: {str(e)}", exc_info=True)

    async def update(self) -> None:
        """Update the portfolio state by fetching data from exchanges."""
        update_tasks = []

        for exchange_id in self.api_clients.keys():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Check if reconciliation is needed
            now = datetime.now(UTC)
            last_check = self._last_reconciliation_time.get(exchange_id, datetime.min.replace(tzinfo=UTC))
            # Ensure last_check is timezone-aware before comparison
            if last_check.tzinfo is None:
                 last_check = last_check.replace(tzinfo=UTC)

            if (now - last_check).total_seconds() >= self.reconciliation_interval:
                update_tasks.append(self._fetch_exchange_balances(exchange_id))
                update_tasks.append(self._fetch_exchange_positions(exchange_id))
                update_tasks.append(self._fetch_exchange_orders(exchange_id))
                self._last_reconciliation_time[exchange_id] = now # Store UTC now
            else:
                # Only fetch orders if not reconciling
                update_tasks.append(self._fetch_exchange_orders(exchange_id))

            # Update the general last update time regardless
            self._last_update_time[exchange_id] = now # Store UTC now

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
        self._orders[exchange_id][order.id] = order
        # Handle status as string (not enum) for compatibility with tests
        status_str = order.status if isinstance(order.status, str) else order.status.name
        logger.debug(f"Updated order {order.id} on {exchange_id}: {status_str}")

        # If the order is filled, check if we need to update positions
        if hasattr(OrderStatus, "FILLED") and order.status == OrderStatus.FILLED:
            side_str = order.side.name if hasattr(order.side, "name") else str(order.side)
            logger.info(
                f"Order {order.id} on {exchange_id} filled: "
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
        self._positions[exchange_id][position.symbol] = position
        logger.debug(
            f"Updated position {position.symbol} on {exchange_id}: "
            f"{position.side} {position.size}"
        )

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
                 logger.warning(f"Asset mismatch in update_balance: provided {asset}, Balance object has {balance_obj.asset}")
                 # Use asset from Balance object or raise error?
                 asset = balance_obj.asset # Prioritize object's asset
        elif isinstance(amount, Decimal):
             # Create a Balance object from the Decimal amount
             balance_obj = Balance(asset=asset, total=amount, available=amount)
        else:
             logger.error(f"Invalid type for amount in update_balance: {type(amount)}")
             return

        # Store or update the Balance object
        old_balance_obj = self._balances[exchange_id].get(asset)
        self._balances[exchange_id][asset] = balance_obj

        # Log significant changes in total balance
        if old_balance_obj is None or abs(balance_obj.total - old_balance_obj.total) > Decimal("0.01"):
             logger.debug(f"Updated balance for {asset} on {exchange_id}: Total={balance_obj.total}, Available={balance_obj.available}")

    def get_exchange_balance(self, exchange_id: str, asset: str) -> Balance | None:
        """Get the Balance object for a specific asset on an exchange."""
        return self._balances.get(exchange_id, {}).get(asset)

    def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        """
        Calculate the total portfolio value in the base currency.
        Requires price conversion logic (TODO).
        """
        total_value = Decimal("0.0")
        for exchange_balances in self._balances.values():
            for asset, balance_obj in exchange_balances.items():
                if isinstance(balance_obj, Balance):
                    amount = balance_obj.total # Use total from Balance object
                    if asset == base_currency:
                        total_value += amount
                    else:
                        # TODO: Price conversion logic remains the same
                        if asset != base_currency:
                             logger.warning(f"Cannot convert {asset} to {base_currency} for total capital calculation (conversion TODO)")
                             pass
                else:
                    # This case should ideally not happen if balances are stored correctly
                    logger.warning(f"Stored balance for {asset} is not a Balance object: {type(balance_obj)}")
                    try:
                        # Attempt fallback conversion if it's just a number
                        if isinstance(balance_obj, (Decimal, int, float, str)):
                            amount = Decimal(str(balance_obj))
                            if asset == base_currency:
                                 total_value += amount
                            else:
                                 # TODO: Price conversion
                                 pass
                    except InvalidOperation:
                         pass # Ignore if conversion fails

        return total_value

    def get_exchange_exposure(self, exchange_id: str) -> float:
        """
        Get the current exposure for an exchange.

        Args:
            exchange_id: Exchange identifier

        Returns:
            Current exposure in USD
        """
        if exchange_id not in self._positions:
            return 0.0

        exposure = 0.0
        for position in self._positions[exchange_id].values():
            if position.is_active():
                # Use mark_price for a more accurate exposure calculation
                exposure += position.mark_price * position.size

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
                            f"Could not calculate exposure for position {getattr(position, 'symbol', '?')}: {e}"
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
        current_capital = self.get_total_capital()

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
                     status_enum = OrderStatus(order.status.lower()) # Attempt conversion
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
            "balances": {},
            "positions": {},
            "orders": {},
            "last_update_time": {},
            "last_reconciliation_time": {},
            "high_watermark": str(self._high_watermark),
            "realized_pnl": str(self._realized_pnl),
        }

        for ex_id, balances in self._balances.items():
            state["balances"][ex_id] = {}
            for asset, balance in balances.items():
                if isinstance(balance, Balance):
                    state["balances"][ex_id][asset] = balance.to_dict() # Use Balance.to_dict()
                else:
                     # Handle non-Balance objects if they slipped through (e.g., store as string)
                     logger.warning(f"Serializing non-Balance object for {asset} in {ex_id}: {type(balance)}")
                     state["balances"][ex_id][asset] = str(balance)

        for ex_id, positions in self._positions.items():
            state["positions"][ex_id] = {}
            for symbol, position in positions.items():
                if isinstance(position, Position):
                    state["positions"][ex_id][symbol] = position.to_dict() # Use Position.to_dict()
                else:
                    logger.warning(f"Serializing non-Position object for {symbol} in {ex_id}: {type(position)}")
                    state["positions"][ex_id][symbol] = str(position)

        for ex_id, orders in self._orders.items():
            state["orders"][ex_id] = {}
            for order_id, order in orders.items():
                if isinstance(order, Order):
                    state["orders"][ex_id][order_id] = order.to_dict() # Use Order.to_dict()
                else:
                    logger.warning(f"Serializing non-Order object for {order_id} in {ex_id}: {type(order)}")
                    state["orders"][ex_id][order_id] = str(order)

        # Serialize datetimes as ISO strings
        state["last_update_time"] = {k: v.isoformat() if v else None for k, v in self._last_update_time.items()}
        state["last_reconciliation_time"] = {k: v.isoformat() if v else None for k, v in self._last_reconciliation_time.items()}

        return state

    def from_dict(self, state_dict: dict[str, Any]) -> None:
        """
        Load portfolio state from a dictionary.

        Args:
            state_dict: Dictionary representation of portfolio state
        """
        self._balances = {}
        for ex_id, balances_dict in state_dict.get("balances", {}).items():
            self._balances[ex_id] = {}
            for asset, balance_data in balances_dict.items():
                try:
                    # Ensure total and available are converted from string/float to Decimal
                    total = Decimal(str(balance_data.get("total", "0")))
                    available = Decimal(str(balance_data.get("available", "0")))
                    self._balances[ex_id][asset] = Balance(
                        asset=asset, total=total, available=available
                    )
                except (InvalidOperation, TypeError, KeyError) as e:
                    logger.error(
                        f"Error deserializing balance for {asset} on {ex_id}: {e} - Data: {balance_data}"
                    )

        self._positions = {}
        for ex_id, positions_dict in state_dict.get("positions", {}).items():
            self._positions[ex_id] = {}
            for _pos_id, pos_data in positions_dict.items():
                try:
                    # Convert numeric fields back to Decimal
                    for key in [
                        "size",
                        "entry_price",
                        "mark_price",
                        "liquidation_price",
                        "leverage",
                        "unrealized_pnl",
                        "realized_pnl",
                        "margin_used",
                    ]:
                        if key in pos_data and pos_data[key] is not None:
                            pos_data[key] = Decimal(str(pos_data[key]))
                        elif key in [
                            "liquidation_price",
                            "unrealized_pnl",
                            "realized_pnl",
                            "margin_used",
                        ]:
                            pos_data[key] = None  # Allow optional Decimals to be None
                    # Convert side back to enum
                    if "side" in pos_data and isinstance(pos_data["side"], str):
                        pos_data["side"] = OrderSide(pos_data["side"].lower())

                    self._positions[ex_id][_pos_id] = Position(**pos_data)
                except (InvalidOperation, TypeError, KeyError, ValueError) as e:
                    logger.error(
                        f"Error deserializing position {_pos_id} on {ex_id}: {e} - Data: {pos_data}"
                    )

        self._orders = {}
        for ex_id, orders_dict in state_dict.get("orders", {}).items():
            self._orders[ex_id] = {}
            for order_id, order_data in orders_dict.items():
                try:
                    # Convert numeric fields back to Decimal (handle None)
                    for key in ["price", "quantity", "filled_quantity", "avg_fill_price"]:
                        if key in order_data and order_data[key] is not None:
                            order_data[key] = Decimal(str(order_data[key]))
                        elif key in ["price", "avg_fill_price"]:
                            order_data[key] = None
                        else:
                            order_data[key] = Decimal("0.0")  # Default quantity/filled to 0 if None

                    # Convert enums back
                    if "status" in order_data:
                        order_data["status"] = OrderStatus(order_data["status"])
                    if "side" in order_data:
                        order_data["side"] = OrderSide(order_data["side"].lower())
                    if "type" in order_data:
                        order_data["type"] = OrderType(order_data["type"].lower())

                    self._orders[ex_id][order_id] = Order(**order_data)
                except (InvalidOperation, TypeError, KeyError, ValueError) as e:
                    logger.error(
                        f"Error deserializing order {order_id} on {ex_id}: {e} - Data: {order_data}"
                    )

        # Deserialize datetimes from ISO strings
        def parse_iso_datetime(dt_str): 
            if dt_str: 
                try: 
                    return datetime.fromisoformat(dt_str).replace(tzinfo=UTC) 
                except ValueError: return datetime.min.replace(tzinfo=UTC) # Fallback 
            return datetime.min.replace(tzinfo=UTC) # Fallback if None

        self._last_update_time = {
            k: parse_iso_datetime(v) 
            for k, v in state_dict.get("last_update_time", {}).items()
        }
        self._last_reconciliation_time = {
            k: parse_iso_datetime(v) 
            for k, v in state_dict.get("last_reconciliation_time", {}).items()
        }

        try:
            hwm_str = state_dict.get("high_watermark")
            self._high_watermark = Decimal(str(hwm_str)) if hwm_str is not None else Decimal("0.0")
        except (InvalidOperation, TypeError):
            logger.error(f"Error deserializing high_watermark: {state_dict.get('high_watermark')}")
            self._high_watermark = Decimal("0.0")

        try:
            realized_pnl_str = state_dict.get("realized_pnl")
            self._realized_pnl = Decimal(str(realized_pnl_str)) if realized_pnl_str is not None else Decimal("0.0")
        except (InvalidOperation, TypeError):
            logger.error(f"Error deserializing realized_pnl: {state_dict.get('realized_pnl')}")
            self._realized_pnl = Decimal("0.0")

        logger.info("Portfolio state deserialized from dict")

    def reset(self) -> None:
        """
        Reset the portfolio tracker state.
        """
        self._balances = {}
        self._positions = {}
        self._orders = {}
        self._last_update_time = {}
        self._last_reconciliation_time = {}
        self._high_watermark = Decimal("0.0")
        self._realized_pnl = Decimal("0.0")
        self._active_symbols = set()
        self._watchlist = set()
        self._initialize_data_structures()
        logger.info("PortfolioTracker state reset.")

    # --- STUB METHODS ---
    # Add stubs for methods expected by RiskManager but not yet implemented

    def get_asset_volatility(self, symbol: str) -> float | None:
        """STUB: Get current volatility for an asset. Returns float."""
        logger.debug(f"STUB: get_asset_volatility called for {symbol}. Returning default 0.01")
        # In a real implementation, fetch from DataHandler or calculate
        return 0.01  # Return float as RiskManager expects float for this specific calc for now

    def get_historical_volatility(self, symbol: str) -> float | None:
        """STUB: Get historical volatility for an asset. Returns float."""
        logger.debug(f"STUB: get_historical_volatility called for {symbol}. Returning default 0.01")
        # In a real implementation, fetch from DataHandler or calculate
        return 0.01  # Return float as RiskManager expects float for this specific calc for now

    def get_exchange_drawdown(self, exchange_id: str) -> float | None:
        """STUB: Get current drawdown for a specific exchange. Returns float percentage."""
        logger.debug(f"STUB: get_exchange_drawdown called for {exchange_id}. Returning default 0.0")
        # Requires tracking per-exchange high watermarks
        return 0.0  # Return float for now

    def get_asset_correlation(self, symbol1: str, symbol2: str) -> float | None:
        """STUB: Get correlation between two assets. Returns float."""
        logger.debug(
            f"STUB: get_asset_correlation called for {symbol1} and {symbol2}. Returning default 0.0"
        )
        # Requires correlation matrix/calculation logic
        return 0.0  # Return float for now

    def get_exchange_collateral_balance(self, exchange_id: str) -> Decimal:
        """Get the balance of the primary collateral asset for an exchange. Returns Decimal."""
        collateral_asset = self.config.get(f"exchanges.{exchange_id}.collateral_asset")
        if not collateral_asset:
            logger.warning(
                f"Collateral asset not defined for exchange {exchange_id}. "
                f"Cannot get specific balance."
            )
            # Fallback: sum all balances? Or return zero? Returning zero is safer.
            return Decimal("0.0")

        balance_obj = self._balances.get(exchange_id, {}).get(collateral_asset)
        if balance_obj and hasattr(balance_obj, "total") and balance_obj.total is not None:
            # Ensure it returns Decimal
            return (
                balance_obj.total
                if isinstance(balance_obj.total, Decimal)
                else Decimal(str(balance_obj.total))
            )
        else:
            logger.debug(
                f"Collateral asset {collateral_asset} balance not found for {exchange_id}. "
                f"Returning zero."
            )
            return Decimal("0.0")

    def get_active_exchanges(self) -> list[str]:
        """STUB: Returns a list of active exchange IDs."""
        return list(self.api_clients.keys())

    def get_active_symbols(self) -> list[str]:
        """STUB: Get a list of symbols with active positions or orders."""
        active_symbols = set()
        for exchange_positions in self._positions.values():
            for pos in exchange_positions.values():
                if pos.is_active():
                    active_symbols.add(pos.symbol)
        for exchange_orders in self._orders.values():
            for order in exchange_orders.values():
                if order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                    active_symbols.add(order.symbol)
        return list(active_symbols)

    def get_symbol_exposure(self, symbol: str) -> Decimal:
        """Calculate total exposure for a specific symbol across all exchanges. Returns Decimal."""
        symbol_exposure = Decimal("0.0")
        for _exchange_id, positions in self._positions.items():
            for _pos_id, pos in positions.items():
                if pos.symbol == symbol and pos.is_active():
                    price = pos.mark_price if pos.mark_price is not None else pos.entry_price
                    if price is not None and pos.size is not None:
                        symbol_exposure += abs(pos.size * price)
        return symbol_exposure

    def get_positions_by_exchange(self, exchange_id: str) -> list[Position]:
        """Get all positions for a specific exchange."""
        return list(self._positions.get(exchange_id, {}).values())

    def get_portfolio_drawdown(self) -> Decimal:
        """
        Calculate the current portfolio drawdown from its peak value.

        Returns:
            Current drawdown as a positive Decimal percentage (e.g., 0.1 for 10%).
            Returns Decimal('0') if high watermark is zero or current value is unknown/invalid.
        """
        # Use the internal method to avoid redundant logging if called frequently
        current_value = self.get_total_capital()  # Ensures calculation and high watermark update

        if not isinstance(current_value, Decimal):
            logger.error(
                f"get_total_capital returned non-Decimal value: {current_value}. "
                f"Cannot calculate drawdown."
            )
            return Decimal("0.0")

        if self._high_watermark <= Decimal("0.0"):
            # Don't log warning every time if peak hasn't been established yet
            # logger.debug("Cannot calculate drawdown: High watermark is zero or negative.")
            return Decimal("0.0")  # No drawdown if no peak value recorded

        # Drawdown calculation
        drawdown = (self._high_watermark - current_value) / self._high_watermark
        drawdown_percentage = max(Decimal("0.0"), drawdown)  # Drawdown cannot be negative

        # Log only if drawdown is significant or changes? Maybe not here.
        # logger.debug(f"Calculated drawdown: {drawdown_percentage:.4f} "
        # f"(Current: {current_value}, Peak: {self._high_watermark})")
        return drawdown_percentage  # Returns Decimal

    def get_asset_balance(self, exchange_id: str, asset: str) -> Balance:
        """
        Get the balance details for a specific asset on an exchange.

        Args:
            exchange_id: Exchange identifier.
            asset: Asset symbol (e.g., 'BTC', 'USDT').

        Returns:
            Balance object for the asset. Returns a zero Balance object if not found or invalid.
        """
        asset_upper = asset.upper()
        balance = self._balances[exchange_id].get(asset_upper)
        zero_balance = Balance(
            asset=asset_upper,
            total=Decimal("0.0"),
            free=Decimal("0.0"),
            locked=Decimal("0.0"),
        )

        if balance is None:
            # logger.debug(f"No balance found for {asset_upper} on {exchange_id}. "
            # f"Returning zero balance.")
            return zero_balance

        # Verify the retrieved data is actually a Balance object
        if not isinstance(balance, Balance):
            logger.warning(
                f"Stored balance data for {asset} on {exchange_id} is not a Balance object: "
                f"{type(balance)}. Returning zero balance."
            )
            return zero_balance

        try:
            # Create a new Balance object with converted Decimal values
            for field_name in ["total", "available"]:
                try:
                    setattr(balance, field_name, Decimal(str(getattr(balance, field_name))))
                except Exception as e:
                    logger.error(
                        f"Could not convert balance field '{field_name}' to Decimal for {asset} on {exchange_id}: {e}. "
                        f"Original: {balance}. Returning zero balance."
                    )
                    return zero_balance  # Return zero balance on conversion error
            return balance
        except Exception as e:
            logger.error(
                f"Could not convert balance field '{field_name}' to Decimal for {asset} on {exchange_id}: {e}. "
                f"Original: {balance}. Returning zero balance."
            )
            return zero_balance  # Return zero balance on conversion error

    def get_position_size(self, exchange_id: str, symbol: str) -> Decimal:
        """
        Get the current size of a position for a specific symbol on an exchange.
        Assumes the Position model stores signed size (positive for long, negative for short).

        Args:
            exchange_id: Exchange identifier.
            symbol: Trading pair symbol (e.g., 'BTC-PERP').

        Returns:
            Position size as a Decimal. Returns Decimal('0') if no active position exists or size is invalid.
        """
        total_size = Decimal("0.0")
        symbol_upper = symbol.upper()
        positions = self._positions.get(exchange_id, {})

        for position in positions.values():
            if not isinstance(position, Position):
                continue  # Skip invalid entries

            # Normalize symbol for comparison (e.g., BTC/USDT vs BTC-PERP)
            position_symbol_norm = position.symbol.upper().replace("-", "/")
            symbol_norm = symbol_upper.replace("-", "/")

            if position_symbol_norm == symbol_norm and position.is_active():
                # Ensure position.size is Decimal
                pos_size = position.size
                if pos_size is None:
                    continue  # Skip positions with null size

                if not isinstance(pos_size, Decimal):
                    try:
                        pos_size = Decimal(str(pos_size))
                    except Exception:
                        logger.error(
                            f"Position size '{pos_size}' for {symbol} on {exchange_id} "
                            f"is not Decimal. Skipping this position."
                        )
                        continue

                # Assuming size is signed in the Position model
                total_size += pos_size

        # logger.debug(f"Position size for {symbol_upper} on {exchange_id}: {total_size}")
        return total_size  # Returns Decimal

    def get_position_pnl(self, exchange_id: str, symbol: str) -> Decimal:
        """
        Get the unrealized P&L for a specific position.

        Args:
            exchange_id: Exchange identifier.
            symbol: Trading pair symbol.

        Returns:
            Unrealized P&L as a Decimal. Returns Decimal('0') if no active position or P&L data/invalid.
        """
        total_pnl = Decimal("0.0")
        symbol_upper = symbol.upper()
        positions = self._positions.get(exchange_id, {})

        for position in positions.values():
            if not isinstance(position, Position):
                continue  # Skip invalid entries

            # Normalize symbol for comparison
            position_symbol_norm = position.symbol.upper().replace("-", "/")
            symbol_norm = symbol_upper.replace("-", "/")

            if position_symbol_norm == symbol_norm and position.is_active():
                # Assuming position object has a 'pnl' attribute that is Decimal
                if hasattr(position, "pnl") and position.pnl is not None:
                    position_pnl = position.pnl
                    if not isinstance(position_pnl, Decimal):
                        try:
                            position_pnl = Decimal(str(position_pnl))
                        except Exception:
                            logger.error(
                                f"Position PNL '{position_pnl}' for {symbol} on {exchange_id} "
                                f"is not Decimal. Treating as zero for this position."
                            )
                            position_pnl = Decimal("0.0")
                    total_pnl += position_pnl  # Sum PNL
                else:
                    # Calculate PNL if not directly available (requires mark_price)
                    if (
                        hasattr(position, "mark_price")
                        and position.mark_price is not None
                        and hasattr(position, "entry_price")
                        and position.entry_price is not None
                        and hasattr(position, "size")
                        and position.size is not None
                    ):
                        try:
                            mark_price = Decimal(str(position.mark_price))
                            entry_price = Decimal(str(position.entry_price))
                            size = Decimal(str(position.size))  # Assuming signed size

                            # Basic PNL calculation for linear contracts
                            calculated_pnl = (mark_price - entry_price) * size
                            # TODO: Add logic for inverse contracts if needed
                            total_pnl += calculated_pnl
                        except Exception as e:
                            logger.error(
                                f"Could not calculate PNL for position {position.id} "
                                f"on {exchange_id}: {e}"
                            )
                    else:
                        logger.debug(
                            f"Cannot calculate PNL for position {position.id} on {exchange_id}: "
                            f"Missing mark_price, entry_price, or size."
                        )

        # logger.debug(f"Unrealized PNL for {symbol_upper} on {exchange_id} (summed): {total_pnl}")
        return total_pnl  # Returns Decimal

    # --- END STUB METHODS ---

    # Method to dump state to JSON string using the custom encoder
    def to_json(self) -> str:
        """Serialize the portfolio state to a JSON string."""
        state_dict = self.to_dict()
        return dump_json(state_dict)

    # Method to load state from JSON string
    @classmethod
    def from_json(cls, json_str: str, config: Config) -> PortfolioTracker:
        """Deserialize the portfolio state from a JSON string."""
        state_dict = json.loads(json_str)  # Use standard json.loads for initial parse
        instance = cls(config)
        instance.from_dict(state_dict)
        return instance

    def _add_to_watchlist(self, symbol: str) -> None:
        """Adds a symbol to the watchlist."""
        if symbol not in self.watch_list:
            # Changed: self.watch_list[symbol] = True # Incorrect for set/list
            self.watch_list.add(symbol)  # Assuming watch_list is a set
            logger.info(f"Added {symbol} to watchlist.")

    def _remove_from_watchlist(self, symbol: str) -> None:
        """Removes a symbol from the watchlist."""
        if symbol in self.watch_list:
            # Changed: del self.watch_list[symbol] # Incorrect for set/list
            self.watch_list.remove(symbol)  # Assuming watch_list is a set
            logger.info(f"Removed {symbol} from watchlist.")

    def add_asset(self, asset: str, total: Decimal, available: Decimal) -> None:
        """Adds or updates an asset balance."""
        if not isinstance(total, Decimal):
            total = Decimal(str(total))
        if not isinstance(available, Decimal):
            available = Decimal(str(available))
        free = available  # Default assumption
        locked = total - available
        self._balances[asset] = Balance(
            asset=asset, total=total, available=available, free=free, locked=locked
        )
        self.logger.info(f"Updated balance for {asset}", total=total, available=available)
        self._notify_observers()

    def calculate_total_value(self) -> Decimal:
        """Calculates the total value of all positions."""
        total_value = Decimal("0.0")
        for symbol, position in self._positions.items():
            for _pos_id, pos in position.items():
                if pos.size > Decimal("0"):
                    current_price = self.get_current_price(symbol)
                    if current_price is not None:
                        position_value = pos.size * current_price
                        total_value += position_value
        return total_value

    def calculate_average_entry_price(self, symbol: str) -> Decimal | None:
        """Calculates the average entry price for a given symbol."""
        total_cost = Decimal("0.0")
        total_quantity = Decimal("0.0")
        positions = self._positions.get(symbol, {})

        for _pos_id, position in positions.items():
            if position.size > Decimal("0"):
                total_cost += position.entry_price * position.size
                total_quantity += position.size

        if total_quantity > Decimal("0.0"):
            average_entry_price = total_cost / total_quantity
            return average_entry_price
        else:
            return None

    def calculate_asset_exposure(self, asset: str) -> Decimal:
        """Calculates the total exposure for a given asset across all positions."""
        exposure = Decimal("0.0")
        for symbol, position in self._positions.items():
            if asset in symbol.split("-")[0]:
                if position.size > Decimal("0"):
                    current_price = self.get_current_price(symbol)
                    if current_price:
                        position_value = position.size * current_price
                        exposure += position_value
        return exposure

    def get_position_value(self, symbol: str) -> Decimal | None:
        """Gets the current estimated value of a position."""
        if symbol in self._positions:
            position = self._positions[symbol]
            if position.size > Decimal("0"):
                current_price = self.get_current_price(symbol)
                if current_price:
                    # Need to handle optional mark_price
                    mark_price = (
                        position.mark_price if position.mark_price is not None else current_price
                    )
                    # Add None check for mark_price before multiplication
                    if mark_price is not None:
                        return position.size * mark_price
        return None

    def get_total_unrealized_pnl(self) -> Decimal:
        """Calculates the total unrealized PNL across all open positions."""
        total_pnl = Decimal("0.0")
        for symbol, position in self._positions.items():
            if position.size > Decimal("0"):
                current_price = self.get_current_price(symbol)
                if current_price:
                    # Ensure mark_price is checked for None
                    mark_price = position.mark_price
                    if mark_price is not None:
                        pnl = position.calculate_unrealized_pnl(mark_price)
                        if pnl is not None:
                            total_pnl += pnl
                    else:
                        self.logger.warning(f"Missing mark_price for PnL calc: {symbol}")
        return total_pnl

    def check_margin_levels(self) -> None:
        """Checks margin levels for all positions and exchanges."""
        # Implementation depends on exchange API capabilities
        self.logger.info("Checking margin levels (placeholder)...")
        for _symbol, position in self._positions.items():
            if position.size > Decimal("0"):
                # Fetch margin info from exchange API via api_client
                # Example: api_client.get_margin_info(position.symbol)
                pass  # Placeholder for margin check logic

    def calculate_max_trade_size(self, exchange_id: str) -> Decimal:
        """
        Calculates the maximum trade size for a symbol based on available capital and risk limits.

        Args:
            exchange_id: The exchange identifier.

        Returns:
            Maximum trade size as a Decimal.
        """
        # Factor 1: Maximum position size based on total capital
        total_capital = self.get_total_capital()
        max_size_from_capital = total_capital * self.config.get(
            "risk.max_position_pct_capital", Decimal("0.1")
        )

        # Factor 2: Maximum total exposure limit
        current_total_exposure = self.get_total_exposure()
        max_size_from_exposure = (
            self.config.get("risk.max_total_exposure", Decimal("10000")) - current_total_exposure
        )

        # Factor 3: Exchange specific exposure limit (if configured)
        max_exchange_exposure_pct = self.config.get(
            f"exchanges.{exchange_id}.max_exposure_pct", Decimal("0.1")
        )
        max_size_from_exchange = max_size_from_capital * max_exchange_exposure_pct

        # Calculate maximum trade size
        max_trade_size = min(max_size_from_capital, max_size_from_exposure, max_size_from_exchange)

        return max_trade_size

    def as_dict(self) -> dict[str, Any]:
        """Serialize portfolio state to a dictionary."""
        # Serialize balances, positions, orders, and other relevant state
        # Convert Decimal to str for JSON compatibility if using standard json
        serialized_balances = {
            ex: {asset: balance.to_dict() for asset, balance in bals.items()}
            for ex, bals in self._balances.items()
        }
        serialized_positions = {
            ex: {pid: pos.to_dict() for pid, pos in positions.items()}
            for ex, positions in self._positions.items()
        }
        serialized_orders = {
            ex: {oid: order.to_dict() for oid, order in orders.items()}
            for ex, orders in self._orders.items()
        }

        return {
            "balances": serialized_balances,
            "positions": serialized_positions,
            "orders": serialized_orders,
            "realized_pnl": str(self._realized_pnl), # Store as string
            "high_watermark": str(self._high_watermark), # Store as string
            # Add other state variables if needed
        }


# Example usage (consider moving to tests or main application logic)
# tracker = PortfolioTracker(config)
# ... populate tracker ...
# json_output = tracker.to_json(indent=4)
# print(json_output)
# loaded_tracker = PortfolioTracker.from_json(json_output, config)
