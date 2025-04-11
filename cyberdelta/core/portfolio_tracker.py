import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.core.models import Balance, Order, OrderStatus, Position
from cyberdelta.utils.config import Config
from cyberdelta.utils.serialization import dump_json

logger = logging.getLogger(__name__)


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
        self.config = config
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
        self.reconciliation_interval = config.get(
            "portfolio.reconciliation_interval", 300
        )  # seconds

        # Initialize data structures
        self._initialize_data_structures()

        # Initialize high watermark (as Decimal)
        self._high_watermark = Decimal(
            "0.0"
        )  # Track highest portfolio value for drawdown calculation

    def _initialize_data_structures(self) -> None:
        """Initialize data structures for all configured exchanges."""
        for exchange_id in self.config.get("exchanges", {}).keys():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Initialize dictionaries for this exchange
            self._balances[exchange_id] = {}
            self._positions[exchange_id] = {}
            self._orders[exchange_id] = {}
            self._last_update_time[exchange_id] = datetime.min
            self._last_reconciliation_time[exchange_id] = datetime.min

    def register_api_client(self, exchange_id: str, client: ExchangeAPI):
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

        for exchange_id, client in self.api_clients.items():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Create tasks for initial data collection
            initialization_tasks.append(self._fetch_exchange_balances(exchange_id))
            initialization_tasks.append(self._fetch_exchange_positions(exchange_id))
            initialization_tasks.append(self._fetch_exchange_orders(exchange_id))

        logger.info(
            f"PortfolioTracker {id(self)}: About to gather init tasks. Balances before: {self._balances}"
        )
        # Wait for all initialization tasks to complete
        results = await asyncio.gather(*initialization_tasks, return_exceptions=True)
        logger.info(
            f"PortfolioTracker {id(self)}: Finished gathering init tasks. Balances after: {self._balances}"
        )

        # Process results for errors
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error during initialization: {result}", exc_info=True)

        logger.info("Portfolio state initialized")

    async def _fetch_exchange_balances(self, exchange_id: str) -> None:
        """
        Fetch current balances from an exchange.

        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]

        try:
            # Fetch balances from exchange
            balances = await client.get_balances()
            logger.info(f"Raw balances received from {exchange_id}: {balances}")

            # Update local state - Ensure stored balances are Decimal
            updated_balances = {}
            for asset, balance_obj in balances.items():
                # Ensure the balance object itself uses Decimal internally if possible
                # Or at least store the object which should contain Decimal amounts
                if not isinstance(balance_obj, Balance):
                    logger.warning(
                        f"Balance data for {asset} on {exchange_id} is not a Balance object: {type(balance_obj)}"
                    )
                    # Attempt conversion if it's a dict
                    if isinstance(balance_obj, dict):
                        try:
                            # Ensure values are Decimal
                            total = Decimal(str(balance_obj.get("total", "0")))
                            free = Decimal(str(balance_obj.get("free", "0")))
                            locked = Decimal(str(balance_obj.get("locked", "0")))
                            updated_balances[asset] = Balance(
                                asset=asset, total=total, free=free, locked=locked
                            )
                        except Exception as conv_err:
                            logger.error(
                                f"Could not convert balance dict to Balance object for {asset}: {conv_err}"
                            )
                    continue  # Skip if not a Balance object or convertible dict
                else:
                    # Optional: Add checks here to ensure balance_obj.total etc. are Decimal
                    # if balance_obj.total is not None and not isinstance(balance_obj.total, Decimal):
                    #     balance_obj.total = Decimal(str(balance_obj.total))
                    # if balance_obj.free is not None and not isinstance(balance_obj.free, Decimal):
                    #     balance_obj.free = Decimal(str(balance_obj.free))
                    updated_balances[asset] = balance_obj

            self._balances[exchange_id] = updated_balances
            logger.info(
                f"Internal _balances state for {exchange_id} after update: {self._balances[exchange_id]}"
            )

            # Convert balances to dict with stringified Decimals before logging
            loggable_balances = {}
            for k, v in updated_balances.items():
                if hasattr(v, "to_dict"):
                    balance_dict = v.to_dict()
                    # Convert Decimal values in the dict to strings
                    for key, val in balance_dict.items():
                        if isinstance(val, Decimal):
                            balance_dict[key] = str(val)
                    loggable_balances[k] = balance_dict
                else:
                    # Handle cases where balance might not have to_dict (shouldn't happen ideally)
                    loggable_balances[k] = str(v)

            logger.info(f"Updated balances for {exchange_id}: {json.dumps(loggable_balances)}")

        except Exception as e:
            logger.error(f"Error fetching balances from {exchange_id}: {str(e)}", exc_info=True)

    async def _fetch_exchange_positions(self, exchange_id: str) -> None:
        """
        Fetch current positions from an exchange.

        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]

        try:
            # Fetch positions from exchange
            positions = await client.get_positions()

            # Initialize the exchange positions dictionary if it doesn't exist
            if exchange_id not in self._positions:
                self._positions[exchange_id] = {}

            # Update local state
            new_positions = {}

            # Handle different position return formats
            if isinstance(positions, dict):
                # If positions is a dictionary like {position_id: Position}
                for pos_id, position in positions.items():
                    new_positions[pos_id] = position
            elif isinstance(positions, list):
                # If positions is a list of Position objects
                for position in positions:
                    if hasattr(position, "id") and position.id:
                        new_positions[position.id] = position
                    else:
                        # Generate a position ID if none exists
                        # Ensure size and prices are Decimal
                        size = (
                            Decimal(str(position.size))
                            if position.size is not None
                            else Decimal("0")
                        )
                        entry_price = (
                            Decimal(str(position.entry_price))
                            if position.entry_price is not None
                            else Decimal("0")
                        )
                        mark_price = (
                            Decimal(str(position.mark_price))
                            if position.mark_price is not None
                            else Decimal("0")
                        )
                        position.size = size
                        position.entry_price = entry_price
                        position.mark_price = mark_price

                        pos_id = f"{position.symbol}_{position.side.value}_{len(new_positions)}"  # Use side.value
                        position.id = pos_id
                        new_positions[pos_id] = position
            else:
                # Unexpected format - log and skip
                logger.warning(f"Unexpected positions format from {exchange_id}: {type(positions)}")
                return

            # Check for closed positions that were previously open
            for pos_id, old_pos in self._positions[exchange_id].items():
                if pos_id not in new_positions and old_pos.is_active():
                    logger.info(f"Position {pos_id} on {exchange_id} is no longer active")

            # Update with new positions
            self._positions[exchange_id] = new_positions
            self._last_update_time[exchange_id] = datetime.now()

            logger.info(f"Updated {len(new_positions)} positions for {exchange_id}")

        except Exception as e:
            logger.error(f"Error fetching positions from {exchange_id}: {str(e)}", exc_info=True)

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
            self._last_update_time[exchange_id] = datetime.now()

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
            now = datetime.now()
            if (
                now - self._last_reconciliation_time.get(exchange_id, datetime.min)
            ).total_seconds() >= self.reconciliation_interval:
                # Time for full reconciliation
                update_tasks.append(self._fetch_exchange_balances(exchange_id))
                update_tasks.append(self._fetch_exchange_positions(exchange_id))
                update_tasks.append(self._fetch_exchange_orders(exchange_id))
                self._last_reconciliation_time[exchange_id] = now
            else:
                # Just update positions and orders for real-time tracking
                update_tasks.append(self._fetch_exchange_positions(exchange_id))
                update_tasks.append(self._fetch_exchange_orders(exchange_id))

        # Wait for all update tasks to complete
        await asyncio.gather(*update_tasks, return_exceptions=True)

    def update_order(self, exchange_id: str, order: Order):
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
                f"Order {order.id} on {exchange_id} filled: {order.symbol} {side_str} {order.quantity}"
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

        # Store or update the position
        self._positions[exchange_id][position.id] = position
        logger.debug(
            f"Updated position {position.id} on {exchange_id}: {position.symbol} {position.side} {position.quantity}"
        )

    def update_balance(self, exchange_id: str, asset: str, amount: float):
        """
        Update local state with a new balance.

        Args:
            exchange_id: Exchange identifier
            asset: Asset name
            amount: Balance amount
        """
        if exchange_id not in self._balances:
            self._balances[exchange_id] = {}

        # Store or update the balance
        old_balance = self._balances[exchange_id].get(asset, 0.0)
        self._balances[exchange_id][asset] = amount

        # Log significant changes
        if abs(amount - old_balance) > 0.01:
            logger.info(
                f"Balance change on {exchange_id}: {asset} {old_balance:.2f} -> {amount:.2f}"
            )

    def get_exchange_balance(
        self, exchange_id: str, asset: str = "USDC"
    ) -> Decimal:
        """
        Get the total balance for a specific asset on an exchange.

        Args:
            exchange_id: Exchange identifier
            asset: Asset name (default: USDC)

        Returns:
            Current balance
        """
        if exchange_id not in self._balances:
            return Decimal("0.0")

        balance = self._balances[exchange_id].get(asset, 0.0)
        # Handle balance as object or float
        if hasattr(balance, "free"):  # Check for and return FREE balance
            return balance.free
        # Fallback for simple float balances (if any)
        if isinstance(balance, (float, int)):
            return Decimal(str(balance))
        # Return 0.0 if balance object doesn't have 'free' or it's not a number
        logger.warning(
            f"Balance for {asset} on {exchange_id} has unexpected format: {type(balance)}"
        )
        return Decimal("0.0")

    def get_total_capital(self, valuation_asset: str = "USDT") -> Decimal:
        """
        Calculate the total capital across all exchanges in a common valuation asset.

        Args:
            valuation_asset: The asset to value the portfolio in (e.g., USDT, USD).

        Returns:
            Total portfolio value as a Decimal. Returns Decimal('0') if valuation fails.
        """
        total_value = Decimal("0.0")
        # This method needs a proper implementation using market data to convert assets.
        # For now, we'll return a placeholder or a simplified sum if possible.
        # A simplified approach (assuming all balances are already in valuation_asset or similar value):
        logger.info(f"Calculating total capital. Current _balances: {self._balances}")
        for exchange_id, exchange_balances in self._balances.items():
            logger.info(f"Processing balances for exchange: {exchange_id}")
            for asset, balance in exchange_balances.items():
                logger.info(
                    f"Checking asset {asset}. Balance type: {type(balance)}, Value: {balance}"
                )
                # Ensure balance object and its values are valid
                if not isinstance(balance, Balance) or balance.total is None:
                    logger.warning(
                        f"Invalid balance object for {asset} on {exchange_id}: {balance}. Skipping."
                    )
                    continue

                # Ensure balance.total is Decimal
                balance_total = balance.total
                if not isinstance(balance_total, Decimal):
                    try:
                        balance_total = Decimal(str(balance_total))
                    except Exception:
                        logger.error(
                            f"Could not convert balance total '{balance.total}' for {asset} to Decimal. Skipping."
                        )
                        continue

                if asset.upper() == valuation_asset.upper():  # Simple check
                    total_value += balance_total
                    logger.info(
                        f"Added {balance_total} for {asset} (matches valuation asset). New total: {total_value}"
                    )
                else:
                    # TODO: Add conversion logic using market data for other assets
                    # For now, we might just add the balance if it's a stablecoin assumed to be near 1:1 with USDT/USD
                    if asset.upper() in ["USDC", "USD", "BUSD"]:  # Example stablecoins
                        total_value += balance_total
                        logger.info(
                            f"Added {balance_total} for {asset} (stablecoin). New total: {total_value}"
                        )
                    else:
                        # Placeholder: Log that conversion is needed
                        logger.debug(
                            f"Asset {asset} requires price conversion to {valuation_asset} for total capital calculation."
                        )

        # Update high watermark
        self._high_watermark = max(self._high_watermark, total_value)
        logger.debug(
            f"Calculated total capital: {total_value} {valuation_asset}, High Watermark: {self._high_watermark}"
        )
        return total_value  # Ensure this returns Decimal

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

        Args:
            valuation_asset: The asset to value the exposure in.

        Returns:
            Total exposure value as a Decimal.
        """
        total_exposure = Decimal("0.0")
        # TODO: Implement exposure calculation using position sizes and market prices
        # Needs access to market data (e.g., from DataHandler)
        # Simplified example:
        for exchange_positions in self._positions.values():
            for position in exchange_positions.values():
                if (
                    not isinstance(position, Position)
                    or not position.is_active()
                    or position.size is None
                    or position.mark_price is None
                ):
                    continue  # Skip inactive or invalid positions

                # Ensure values are Decimal
                try:
                    pos_size = Decimal(str(position.size))
                    mark_price = Decimal(str(position.mark_price))
                except Exception:
                    logger.error(
                        f"Could not convert position size '{position.size}' or mark price '{position.mark_price}' to Decimal for {position.symbol}. Skipping."
                    )
                    continue

                # Simple exposure calculation (Size * Mark Price)
                # Assumes mark_price is in the quote currency, need conversion to valuation_asset
                exposure_value = pos_size * mark_price
                # TODO: Convert exposure_value to valuation_asset using market data

                # For now, assume quote asset is valuation asset if it matches (e.g., BTC/USDT exposure in USDT)
                # This is a MAJOR simplification
                symbol_parts = position.symbol.split("/")  # e.g., ['BTC', 'USDT']
                if len(symbol_parts) > 1 and symbol_parts[-1].upper() == valuation_asset.upper():
                    total_exposure += abs(exposure_value)
                else:
                    logger.debug(
                        f"Position {position.symbol} requires price conversion for exposure calculation in {valuation_asset}."
                    )

        logger.debug(f"Calculated total exposure (simplified): {total_exposure} {valuation_asset}")
        return total_exposure  # Ensure this returns Decimal

    def get_pnl(self) -> tuple[Decimal, Decimal]:
        """
        Calculate the total and unrealized P&L across all positions.
        Returns PnL values as Decimal.

        Returns:
            Tuple containing total P&L and unrealized P&L (Decimal, Decimal).
        """
        # Perform Decimal calculations for P&L
        total_pnl = Decimal("0.0")
        unrealized_pnl = Decimal("0.0")
        realized_pnl = Decimal("0.0")  # Needs proper tracking if required

        for exchange_id, positions in self._positions.items():
            for pos_id, position in positions.items():
                if position.is_active():
                    # Ensure values are Decimal
                    size = (
                        position.size
                        if isinstance(position.size, Decimal)
                        else Decimal(str(position.size))
                    )
                    entry_price = (
                        position.entry_price
                        if isinstance(position.entry_price, Decimal)
                        else Decimal(str(position.entry_price))
                    )
                    mark_price = (
                        position.mark_price
                        if isinstance(position.mark_price, Decimal)
                        else Decimal(str(position.mark_price))
                    )

                    # PnL = Size * (Mark Price - Entry Price)
                    # Handle potential None values gracefully
                    if mark_price is not None and entry_price is not None:
                        pos_pnl = size * (mark_price - entry_price)
                        unrealized_pnl += pos_pnl
                    else:
                        logger.warning(
                            f"Cannot calculate PnL for position {pos_id} due to missing prices."
                        )

        # Total PnL (simplified as unrealized for now)
        total_pnl = unrealized_pnl

        # Return as Tuple[Decimal, Decimal]
        return total_pnl, unrealized_pnl

    def get_position(self, exchange_id: str, position_id: str) -> Position | None:
        """
        Get a specific position.

        Args:
            exchange_id: Exchange identifier
            position_id: Position identifier

        Returns:
            Position object or None if not found
        """
        if exchange_id not in self._positions:
            return None

        return self._positions[exchange_id].get(position_id)

    def get_positions_by_symbol(self, exchange_id: str, symbol: str) -> list[Position]:
        """
        Get all positions for a symbol on an exchange.

        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol

        Returns:
            List of positions
        """
        if exchange_id not in self._positions:
            return []

        return [
            p for p in self._positions[exchange_id].values() if p.symbol == symbol and p.is_active()
        ]

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

        return self._orders[exchange_id].get(order_id)

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

        open_statuses = [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]

        if symbol:
            return [
                o
                for o in self._orders[exchange_id].values()
                if o.status in open_statuses and o.symbol == symbol
            ]
        else:
            return [o for o in self._orders[exchange_id].values() if o.status in open_statuses]

    def to_dict(self) -> dict[str, Any]:
        """
        Convert portfolio state to a dictionary for serialization.

        Returns:
            Dictionary representation of portfolio state
        """
        # Serialize balances (simple structure)
        balances_dict = {
            exchange_id: {
                asset: balance.to_dict() if hasattr(balance, "to_dict") else balance
                for asset, balance in assets.items()
            }
            for exchange_id, assets in self._balances.items()
        }

        # Serialize positions (may have complex objects)
        positions_dict = {
            exchange_id: {
                position_id: position.to_dict() if hasattr(position, "to_dict") else position
                for position_id, position in positions.items()
            }
            for exchange_id, positions in self._positions.items()
        }

        # Serialize orders (may have complex objects)
        orders_dict = {
            exchange_id: {
                order_id: order.to_dict() if hasattr(order, "to_dict") else order
                for order_id, order in orders.items()
            }
            for exchange_id, orders in self._orders.items()
        }

        # Serialize timestamps
        last_update_dict = {}
        if self._last_update_time:
            for exchange_id, timestamp in self._last_update_time.items():
                # Handle both datetime objects and other types
                if hasattr(timestamp, "isoformat"):
                    last_update_dict[exchange_id] = timestamp.isoformat()
                else:
                    last_update_dict[exchange_id] = timestamp

        # Same for reconciliation times
        last_recon_dict = {}
        if self._last_reconciliation_time:
            for exchange_id, timestamp in self._last_reconciliation_time.items():
                if hasattr(timestamp, "isoformat"):
                    last_recon_dict[exchange_id] = timestamp.isoformat()
                else:
                    last_recon_dict[exchange_id] = timestamp

        # Create the complete state dictionary
        state_dict = {
            "balances": balances_dict,
            "positions": positions_dict,
            "orders": orders_dict,
            "last_update_time": last_update_dict,
            "last_reconciliation_time": last_recon_dict,
            "high_watermark": getattr(self, "_high_watermark", Decimal("0.0")),
            "current_drawdown": self.get_current_drawdown(),
        }

        return state_dict

    def from_dict(self, state_dict: dict[str, Any]) -> None:
        """
        Load portfolio state from a dictionary.

        Args:
            state_dict: Dictionary representation of portfolio state
        """
        # Restore balances
        if "balances" in state_dict:
            for exchange_id, assets in state_dict["balances"].items():
                if exchange_id not in self._balances:
                    self._balances[exchange_id] = {}
                self._balances[exchange_id].update(assets)

        # Restore positions (full implementation would deserialize position objects)
        # This is a simplified example
        if "positions" in state_dict:
            for exchange_id, positions in state_dict["positions"].items():
                if exchange_id not in self._positions:
                    self._positions[exchange_id] = {}
                # In a real implementation, deserialize Position objects
                # self._positions[exchange_id].update(positions)

        # Restore orders (full implementation would deserialize order objects)
        # This is a simplified example
        if "orders" in state_dict:
            for exchange_id, orders in state_dict["orders"].items():
                if exchange_id not in self._orders:
                    self._orders[exchange_id] = {}
                # In a real implementation, deserialize Order objects
                # self._orders[exchange_id].update(orders)

        # Restore timestamps
        if "last_update_time" in state_dict:
            for exchange_id, timestamp_str in state_dict["last_update_time"].items():
                self._last_update_time[exchange_id] = datetime.fromisoformat(timestamp_str)

        if "last_reconciliation_time" in state_dict:
            for exchange_id, timestamp_str in state_dict["last_reconciliation_time"].items():
                self._last_reconciliation_time[exchange_id] = datetime.fromisoformat(timestamp_str)

        # Restore high watermark if available
        if "high_watermark" in state_dict:
            self._high_watermark = Decimal(state_dict["high_watermark"])

        logger.info("Portfolio state restored from dictionary")

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
                f"Collateral asset not defined for exchange {exchange_id}. Cannot get specific balance."
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
                f"Collateral asset {collateral_asset} balance not found for {exchange_id}. Returning zero."
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
        for exchange_id, positions in self._positions.items():
            for pos_id, pos in positions.items():
                if pos.symbol == symbol and pos.is_active():
                    price = (
                        pos.mark_price
                        if isinstance(pos.mark_price, Decimal)
                        else Decimal(str(pos.mark_price))
                    )
                    size = pos.size if isinstance(pos.size, Decimal) else Decimal(str(pos.size))
                    symbol_exposure += abs(price * size)
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
                f"get_total_capital returned non-Decimal value: {current_value}. Cannot calculate drawdown."
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
        # logger.debug(f"Calculated drawdown: {drawdown_percentage:.4f} (Current: {current_value}, Peak: {self._high_watermark})")
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
        balance = self._balances.get(exchange_id, {}).get(asset_upper)
        zero_balance = Balance(
            asset=asset_upper,
            total=Decimal("0.0"),
            free=Decimal("0.0"),
            locked=Decimal("0.0"),
        )

        if balance is None:
            # logger.debug(f"No balance found for {asset_upper} on {exchange_id}. Returning zero balance.")
            return zero_balance

        # Validate and ensure the returned balance uses Decimal
        if not isinstance(balance, Balance):
            logger.warning(
                f"Stored balance data for {asset} on {exchange_id} is not a Balance object: {type(balance)}. Returning zero balance."
            )
            return zero_balance

        try:
            # Create a new Balance object with converted Decimal values
            total = Decimal(str(balance.total)) if balance.total is not None else Decimal("0.0")
            free = Decimal(str(balance.free)) if balance.free is not None else Decimal("0.0")
            locked = Decimal(str(balance.locked)) if balance.locked is not None else Decimal("0.0")
            return Balance(asset=asset_upper, total=total, free=free, locked=locked)
        except Exception as e:
            logger.error(
                f"Failed to convert balance values to Decimal for {asset} on {exchange_id}: {e}. Original: {balance}. Returning zero balance."
            )
            return zero_balance

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
                            f"Position size '{pos_size}' for {symbol} on {exchange_id} is not Decimal. Skipping this position."
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
                                f"Position PNL '{position_pnl}' for {symbol} on {exchange_id} is not Decimal. Treating as zero for this position."
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
                                f"Could not calculate PNL for position {position.id} on {exchange_id}: {e}"
                            )
                    else:
                        logger.debug(
                            f"Cannot calculate PNL for position {position.id} on {exchange_id}: Missing mark_price, entry_price, or size."
                        )

        # logger.debug(f"Unrealized PNL for {symbol_upper} on {exchange_id} (summed): {total_pnl}")
        return total_pnl  # Returns Decimal

    # --- END STUB METHODS ---

    # Method to dump state to JSON string using the custom encoder
    def to_json(self, **kwargs: Any) -> str:  # noqa: ANN003
        """Serialize the portfolio state to a JSON string."""
        state_dict = self.to_dict()
        return dump_json(state_dict, **kwargs)

    # Method to load state from JSON string
    @classmethod
    def from_json(
        cls, json_str: str, config: Config, **kwargs: Any # noqa: ANN003
    ) -> "PortfolioTracker":
        """Deserialize the portfolio state from a JSON string."""
        state_dict = json.loads(
            json_str, **kwargs
        )  # Standard loads should work if dump used str for Decimal
        return cls.from_dict(state_dict, config)


# Example usage (consider moving to tests or main application logic)
# tracker = PortfolioTracker(config)
# ... populate tracker ...
# json_output = tracker.to_json(indent=4)
# print(json_output)
# loaded_tracker = PortfolioTracker.from_json(json_output, config)
