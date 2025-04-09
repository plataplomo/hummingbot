import logging
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import asyncio

from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.core.models import Position, Order, OrderStatus
from cyberdelta.utils.config import Config

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
    
    def __init__(self, config: Config):
        """
        Initialize the portfolio tracker.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.api_clients: Dict[str, ExchangeAPI] = {}
        
        # Balance tracking
        self._balances: Dict[str, Dict[str, float]] = {}  # exchange -> asset -> amount
        
        # Position tracking
        self._positions: Dict[str, Dict[str, Position]] = {}  # exchange -> position_id -> Position
        
        # Order tracking
        self._orders: Dict[str, Dict[str, Order]] = {}  # exchange -> order_id -> Order
        
        # Timestamp of last update
        self._last_update_time: Dict[str, datetime] = {}  # exchange -> last update time
        
        # Timestamp of last reconciliation
        self._last_reconciliation_time: Dict[str, datetime] = {}  # exchange -> last reconciliation time
        
        # Reconciliation interval (5 minutes by default)
        self.reconciliation_interval = config.get('portfolio.reconciliation_interval', 300)  # seconds
        
        # Initialize data structures
        self._initialize_data_structures()
    
    def _initialize_data_structures(self):
        """Initialize data structures for all configured exchanges."""
        for exchange_id in self.config.get('exchanges', {}).keys():
            if not self.config.get(f'exchanges.{exchange_id}.enabled', False):
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
    
    async def initialize(self):
        """Initialize portfolio state from exchanges."""
        initialization_tasks = []
        
        for exchange_id, client in self.api_clients.items():
            if not self.config.get(f'exchanges.{exchange_id}.enabled', False):
                continue
                
            # Create tasks for initial data collection
            initialization_tasks.append(self._fetch_exchange_balances(exchange_id))
            initialization_tasks.append(self._fetch_exchange_positions(exchange_id))
            initialization_tasks.append(self._fetch_exchange_orders(exchange_id))
            
        # Wait for all initialization tasks to complete
        await asyncio.gather(*initialization_tasks, return_exceptions=True)
        logger.info("Portfolio state initialized")
        
    async def _fetch_exchange_balances(self, exchange_id: str):
        """
        Fetch current balances from an exchange.
        
        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]
        
        try:
            # Fetch balances from exchange
            balances = await client.get_balances()
            
            # Update local state
            self._balances[exchange_id] = balances
            self._last_update_time[exchange_id] = datetime.now()
            
            logger.info(f"Updated balances for {exchange_id}: {json.dumps(balances)}")
            
        except Exception as e:
            logger.error(f"Error fetching balances from {exchange_id}: {str(e)}", exc_info=True)
    
    async def _fetch_exchange_positions(self, exchange_id: str):
        """
        Fetch current positions from an exchange.
        
        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]
        
        try:
            # Fetch positions from exchange
            positions = await client.get_positions()
            
            # Update local state
            new_positions = {}
            for position in positions:
                new_positions[position.id] = position
            
            # Check for closed positions that were previously open
            for pos_id, old_pos in self._positions[exchange_id].items():
                if pos_id not in new_positions and old_pos.is_active():
                    logger.info(f"Position {pos_id} on {exchange_id} is no longer active")
            
            # Update with new positions
            self._positions[exchange_id] = new_positions
            self._last_update_time[exchange_id] = datetime.now()
            
            logger.info(f"Updated {len(positions)} positions for {exchange_id}")
            
        except Exception as e:
            logger.error(f"Error fetching positions from {exchange_id}: {str(e)}", exc_info=True)
    
    async def _fetch_exchange_orders(self, exchange_id: str):
        """
        Fetch current orders from an exchange.
        
        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]
        
        try:
            # Fetch open orders from exchange
            orders = await client.get_open_orders()
            
            # Update local state
            new_orders = {}
            for order in orders:
                new_orders[order.id] = order
            
            # Check for orders that are no longer open
            for order_id, old_order in self._orders[exchange_id].items():
                if order_id not in new_orders and old_order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                    logger.info(f"Order {order_id} on {exchange_id} is no longer open")
            
            # Update with new orders
            self._orders[exchange_id] = new_orders
            self._last_update_time[exchange_id] = datetime.now()
            
            logger.info(f"Updated {len(orders)} orders for {exchange_id}")
            
        except Exception as e:
            logger.error(f"Error fetching orders from {exchange_id}: {str(e)}", exc_info=True)
    
    async def update(self):
        """Update portfolio state from all exchanges."""
        update_tasks = []
        
        for exchange_id in self.api_clients.keys():
            if not self.config.get(f'exchanges.{exchange_id}.enabled', False):
                continue
                
            # Check if reconciliation is needed
            now = datetime.now()
            if (now - self._last_reconciliation_time.get(exchange_id, datetime.min)).total_seconds() >= self.reconciliation_interval:
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
        logger.debug(f"Updated order {order.id} on {exchange_id}: {order.status.name}")
        
        # If the order is filled, check if we need to update positions
        if order.status == OrderStatus.FILLED:
            logger.info(f"Order {order.id} on {exchange_id} filled: {order.symbol} {order.side.name} {order.quantity}")
    
    def update_position(self, exchange_id: str, position: Position):
        """
        Update local state with a new or updated position.
        
        Args:
            exchange_id: Exchange identifier
            position: Position object
        """
        if exchange_id not in self._positions:
            self._positions[exchange_id] = {}
            
        # Store or update the position
        self._positions[exchange_id][position.id] = position
        logger.debug(f"Updated position {position.id} on {exchange_id}: {position.symbol} {position.side} {position.quantity}")
    
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
            logger.info(f"Balance change on {exchange_id}: {asset} {old_balance:.2f} -> {amount:.2f}")
    
    def get_exchange_balance(self, exchange_id: str, asset: str = 'USDC') -> float:
        """
        Get the current balance for an exchange.
        
        Args:
            exchange_id: Exchange identifier
            asset: Asset name (default: USDC)
            
        Returns:
            Current balance
        """
        if exchange_id not in self._balances:
            return 0.0
            
        return self._balances[exchange_id].get(asset, 0.0)
    
    def get_total_capital(self) -> float:
        """
        Get the total capital across all exchanges.
        
        Returns:
            Total capital in USD
        """
        total = 0.0
        for exchange_id in self._balances:
            for asset, amount in self._balances[exchange_id].items():
                # In a real implementation, we would convert non-USD assets
                # For simplicity, we assume all balances are in USDC/USD
                total += amount
        return total
    
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
                # Calculate exposure based on position size
                # entry_price * quantity represents the notional value
                exposure += position.entry_price * position.quantity
                
        return exposure
    
    def get_total_exposure(self) -> float:
        """
        Get the total exposure across all exchanges.
        
        Returns:
            Total exposure in USD
        """
        total = 0.0
        for exchange_id in self._positions:
            total += self.get_exchange_exposure(exchange_id)
        return total
    
    def get_pnl(self) -> Tuple[float, float]:
        """
        Calculate total realized and unrealized PnL.
        
        Returns:
            Tuple of (realized_pnl, unrealized_pnl)
        """
        realized_pnl = 0.0
        unrealized_pnl = 0.0
        
        for exchange_id in self._positions:
            for position in self._positions[exchange_id].values():
                # Realized PnL from closed positions
                if not position.is_active() and position.realized_pnl is not None:
                    realized_pnl += position.realized_pnl
                
                # Unrealized PnL for open positions
                # In a real implementation, we would use current market prices
                # This is a simplified version
                elif position.is_active() and position.entry_price > 0:
                    # Use last price from the position's symbol
                    # For now, we'll just use a placeholder value
                    current_price = position.entry_price  # Placeholder
                    unrealized_pnl += position.calculate_unrealized_pnl(current_price)
        
        return realized_pnl, unrealized_pnl
    
    def get_position(self, exchange_id: str, position_id: str) -> Optional[Position]:
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
    
    def get_positions_by_symbol(self, exchange_id: str, symbol: str) -> List[Position]:
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
            
        return [p for p in self._positions[exchange_id].values() if p.symbol == symbol and p.is_active()]
    
    def get_all_positions(self) -> List[Tuple[str, Position]]:
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
    
    def get_order(self, exchange_id: str, order_id: str) -> Optional[Order]:
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
    
    def get_open_orders(self, exchange_id: str, symbol: Optional[str] = None) -> List[Order]:
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
            return [o for o in self._orders[exchange_id].values() 
                   if o.status in open_statuses and o.symbol == symbol]
        else:
            return [o for o in self._orders[exchange_id].values() 
                   if o.status in open_statuses]
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert portfolio state to dictionary for serialization.
        
        Returns:
            Dictionary representation of portfolio state
        """
        # Convert balances to a dictionary
        balances_dict = {}
        for exchange_id, assets in self._balances.items():
            balances_dict[exchange_id] = assets.copy()
        
        # Convert positions to a dictionary
        positions_dict = {}
        for exchange_id, positions in self._positions.items():
            positions_dict[exchange_id] = {
                pos_id: pos.to_dict() for pos_id, pos in positions.items()
            }
        
        # Convert orders to a dictionary
        orders_dict = {}
        for exchange_id, orders in self._orders.items():
            orders_dict[exchange_id] = {
                order_id: order.to_dict() for order_id, order in orders.items()
            }
        
        # Return complete state
        return {
            'balances': balances_dict,
            'positions': positions_dict,
            'orders': orders_dict,
            'last_update_time': {
                exchange_id: ts.isoformat() 
                for exchange_id, ts in self._last_update_time.items()
            },
            'last_reconciliation_time': {
                exchange_id: ts.isoformat() 
                for exchange_id, ts in self._last_reconciliation_time.items()
            }
        }
    
    def from_dict(self, state_dict: Dict[str, Any]):
        """
        Restore portfolio state from a dictionary.
        
        Args:
            state_dict: Dictionary representation of portfolio state
        """
        # Restore balances
        if 'balances' in state_dict:
            for exchange_id, assets in state_dict['balances'].items():
                if exchange_id not in self._balances:
                    self._balances[exchange_id] = {}
                self._balances[exchange_id].update(assets)
        
        # Restore positions (full implementation would deserialize position objects)
        # This is a simplified example
        if 'positions' in state_dict:
            for exchange_id, positions in state_dict['positions'].items():
                if exchange_id not in self._positions:
                    self._positions[exchange_id] = {}
                # In a real implementation, deserialize Position objects
                # self._positions[exchange_id].update(positions)
        
        # Restore orders (full implementation would deserialize order objects)
        # This is a simplified example
        if 'orders' in state_dict:
            for exchange_id, orders in state_dict['orders'].items():
                if exchange_id not in self._orders:
                    self._orders[exchange_id] = {}
                # In a real implementation, deserialize Order objects
                # self._orders[exchange_id].update(orders)
        
        # Restore timestamps
        if 'last_update_time' in state_dict:
            for exchange_id, timestamp_str in state_dict['last_update_time'].items():
                self._last_update_time[exchange_id] = datetime.fromisoformat(timestamp_str)
        
        if 'last_reconciliation_time' in state_dict:
            for exchange_id, timestamp_str in state_dict['last_reconciliation_time'].items():
                self._last_reconciliation_time[exchange_id] = datetime.fromisoformat(timestamp_str)
        
        logger.info("Portfolio state restored from dictionary")
