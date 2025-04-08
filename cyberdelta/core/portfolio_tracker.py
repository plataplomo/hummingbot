import logging
import asyncio
from typing import Dict, Optional, List
from collections import defaultdict

from ..core.models import Balance, Position, Order
from ..apis.base import ExchangeAPI

logger = logging.getLogger(__name__)

class PortfolioTracker:
    """Tracks portfolio state including balances, positions, and orders across exchanges."""

    def __init__(self, api_clients: Dict[str, ExchangeAPI]):
        self.api_clients = api_clients
        self.balances: Dict[str, Dict[str, Balance]] = defaultdict(dict) # {exchange: {asset: Balance}}
        self.positions: Dict[str, Dict[str, Position]] = defaultdict(dict) # {exchange: {symbol: Position}}
        self.open_orders: Dict[str, Dict[str, Order]] = defaultdict(dict) # {exchange: {order_id: Order}}
        self._lock = asyncio.Lock() # Protect state during updates
        self._stop_event = asyncio.Event()

    async def update_balance(self, exchange: str, asset: str, total: float, available: float):
        """Update the balance for a specific asset on an exchange."""
        async with self._lock:
            balance = Balance(asset=asset, total=total, available=available)
            self.balances[exchange][asset] = balance
            logger.debug(f"Updated balance for {asset} on {exchange}: {balance}")

    async def update_position(self, exchange: str, position_data: Position):
        """Update or add a position for a symbol on an exchange."""
        async with self._lock:
            self.positions[exchange][position_data.symbol] = position_data
            logger.debug(f"Updated position for {position_data.symbol} on {exchange}: {position_data}")

    async def remove_position(self, exchange: str, symbol: str):
        """Remove a position (e.g., when closed)."""
        async with self._lock:
            if symbol in self.positions.get(exchange, {}):
                del self.positions[exchange][symbol]
                logger.debug(f"Removed position for {symbol} on {exchange}.")

    async def update_order(self, exchange: str, order_data: Order):
        """Update or add an order."""
        async with self._lock:
            # Remove if filled/canceled/rejected, otherwise update
            if order_data.status in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
                if order_data.order_id in self.open_orders.get(exchange, {}):
                    del self.open_orders[exchange][order_data.order_id]
                    logger.debug(f"Removed order {order_data.order_id} ({order_data.status}) on {exchange}.")
            else:
                self.open_orders[exchange][order_data.order_id] = order_data
                logger.debug(f"Updated order {order_data.order_id} ({order_data.status}) on {exchange}.")

    async def load_initial_state(self):
        """Fetch initial balances, positions, and open orders from all active exchanges."""
        logger.info("Loading initial portfolio state...")
        fetch_tasks = []
        for exchange, client in self.api_clients.items():
            fetch_tasks.append(self._fetch_exchange_state(exchange, client))

        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        # Log any errors during initial fetch
        for i, exchange in enumerate(self.api_clients.keys()):
            if isinstance(results[i], Exception):
                logger.error(f"Failed to fetch initial state for {exchange}: {results[i]}")
        logger.info("Initial portfolio state loading complete.")

    async def _fetch_exchange_state(self, exchange: str, client: ExchangeAPI):
        """Fetch balances, positions, and orders for a single exchange."""
        logger.info(f"Fetching initial state for {exchange}...")
        try:
            # Fetch in parallel
            balances_task = asyncio.create_task(client.get_balances())
            positions_task = asyncio.create_task(client.get_positions())
            orders_task = asyncio.create_task(client.get_open_orders())

            balances = await balances_task
            positions = await positions_task
            orders = await orders_task

            async with self._lock:
                self.balances[exchange] = balances
                self.positions[exchange] = positions
                self.open_orders[exchange] = {o.order_id: o for o in orders}

            logger.info(f"Successfully fetched state for {exchange}. Bal: {len(balances)}, Pos: {len(positions)}, Ord: {len(orders)}")

        except NotImplementedError:
             logger.warning(f"State fetching not fully implemented for {exchange}. Skipping initial load.")
        except Exception as e:
            logger.error(f"Error fetching state for {exchange}: {e}", exc_info=True)
            raise # Re-raise to be caught by gather

    # --- Getters for state (thread-safe reads might be needed if accessed heavily) ---
    def get_balance(self, exchange: str, asset: str) -> Optional[Balance]:
        return self.balances.get(exchange, {}).get(asset)

    def get_all_balances(self, exchange: str) -> Dict[str, Balance]:
        return self.balances.get(exchange, {}).copy()

    def get_position(self, exchange: str, symbol: str) -> Optional[Position]:
        return self.positions.get(exchange, {}).get(symbol)

    def get_all_positions(self, exchange: str) -> Dict[str, Position]:
        return self.positions.get(exchange, {}).copy()

    def get_order(self, exchange: str, order_id: str) -> Optional[Order]:
        return self.open_orders.get(exchange, {}).get(order_id)

    def get_all_open_orders(self, exchange: str) -> Dict[str, Order]:
        return self.open_orders.get(exchange, {}).copy()

    # --- Run loop (optional - could be purely event-driven via WS updates) ---
    async def run(self):
        """Optionally run a loop for periodic state fetching or health checks."""
        logger.info("Portfolio Tracker starting run loop.")
        await self.load_initial_state()
        # If not using WebSockets for user data, poll periodically here
        poll_interval = 60 # seconds

        while not self._stop_event.is_set():
            # Example polling (only needed if WS user data is unreliable/unavailable)
            # logger.debug("Polling portfolio state...")
            # await self.load_initial_state()

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=poll_interval)
                break # Stop event was set
            except asyncio.TimeoutError:
                continue # Timeout reached, continue loop
            except asyncio.CancelledError:
                logger.info("Portfolio Tracker run loop cancelled.")
                break

        logger.info("Portfolio Tracker run loop finished.")

    def stop(self):
        """Signals the portfolio tracker to stop."""
        logger.info("Portfolio Tracker received stop signal.")
        self._stop_event.set()

# Need to import OrderStatus from models
from ..core.models import OrderStatus 