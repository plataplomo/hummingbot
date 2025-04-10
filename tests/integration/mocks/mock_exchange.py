import asyncio
import logging
import time
import uuid
from typing import Dict, Any, Optional, List, Coroutine, Callable
from datetime import datetime, timezone, timedelta
import copy

from cyberdelta.apis.base import ExchangeAPI, APIError, APIErrorCode
from cyberdelta.core.models import (
    Order, OrderBook, Ticker, Trade, Position, Balance, FundingRate, 
    OrderType, OrderSide, OrderStatus, # ExchangeID, # REMOVE ExchangeID from here
    # Symbol      # REMOVE Symbol from here
)
# Correct the import to use the new typing module
from cyberdelta.core.typing import ExchangeID, Symbol
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)

# Type alias for WebSocket message handlers from base.py
MessageHandler = Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]

# Define a custom exception for mock API errors
class MockAPIError(Exception):
    pass

class MockExchangeAPI(ExchangeAPI):
    """
    Mock implementation of the ExchangeAPI for integration testing.
    Simulates basic exchange behavior, including order management, data fetching,
    and WebSocket interactions. Allows simulating errors and latency.
    """

    def __init__(self, 
                 exchange_name: str, 
                 config: Dict[str, Any], 
                 secrets: Dict[str, Optional[str]], 
                 config_obj: Optional[Config] = None): # Add optional config_obj parameter
        super().__init__(exchange_name, config, secrets)
        self.full_config = config_obj # Store the full config object if provided
        self._order_id_counter = 1
        self._orders: Dict[str, Order] = {} # Store orders by ID
        self._positions: Dict[str, Position] = {} # Store positions by symbol
        self._balances: Dict[str, Balance] = {} # Store balances by asset
        self._mock_tickers: Dict[str, Ticker] = {}
        self._mock_funding_rates: Dict[str, FundingRate] = {}
        
        # --- NEW: Fee attributes initialized from config ---
        default_fee = 0.001 # Default fee rate if not in config
        default_asset = 'USD' # Default fee asset if not in config
        if self.full_config:
            self.maker_fee = self.full_config.get(f'exchanges.{exchange_name}.maker_fee', default_fee)
            self.taker_fee = self.full_config.get(f'exchanges.{exchange_name}.taker_fee', default_fee)
            # Use 'collateral_asset' as the primary indicator for fee asset
            self.fee_asset = self.full_config.get(f'exchanges.{exchange_name}.collateral_asset', default_asset)
        else:
            # Fallback if full_config not provided (less ideal)
            self.maker_fee = config.get('maker_fee', default_fee)
            self.taker_fee = config.get('taker_fee', default_fee)
            self.fee_asset = config.get('collateral_asset', default_asset)
        # ---------------------------------------------------

        # Simulation parameters
        self._latency_ms: float = 10.0  # Default latency in milliseconds
        self._error_simulation: Optional[Dict[str, Any]] = None # Config to simulate errors
        self._fail_on_method: Optional[str] = None # Method name to fail on
        self._failure_exception: Exception = MockAPIError("Simulated API failure") # Exception to raise
        self._open_orders_behavior: str = "default" # Options: default, fill_immediately, partial_fill

        logger.info(f"Initialized MockExchangeAPI for {exchange_name} (Maker Fee: {self.maker_fee}, Taker Fee: {self.taker_fee}, Fee Asset: {self.fee_asset})")

    async def _simulate_latency(self):
        """Simulate network latency."""
        if self._latency_ms > 0:
            await asyncio.sleep(self._latency_ms / 1000.0)

    def configure_error(self, method_name: str, error_code: APIErrorCode, message: str = "Simulated Error", **kwargs):
        """Configure the mock to raise a specific APIError for a method."""
        self._error_simulation = {
            "method": method_name,
            "error": APIError(message, code=error_code, **kwargs)
        }
        logger.info(f"MockExchange {self.exchange_name} configured to raise {error_code} on {method_name}")

    def clear_error(self):
        """Clear any configured error simulation."""
        self._error_simulation = None
        logger.info(f"MockExchange {self.exchange_name} error simulation cleared")
        
    def _check_error_simulation(self, method_name: str):
        """Check if an error should be raised for the current method."""
        if self._error_simulation and self._error_simulation["method"] == method_name:
            logger.warning(f"Simulating error for {method_name} on {self.exchange_name}")
            raise self._error_simulation["error"]

    def configure_failure(self, method_name: Optional[str] = None, exception: Optional[Exception] = None):
        """Configure the mock to fail on a specific method call."""
        self._fail_on_method = method_name
        if exception:
            self._failure_exception = exception
        else:
            self._failure_exception = MockAPIError(f"Simulated API failure on {method_name}")

    def reset_failure(self):
        """Reset the failure configuration."""
        self._fail_on_method = None

    # --- Implement abstract methods ---

    async def _authenticate(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Mock authentication - always succeeds."""
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Authenticating request {method} {path}")
        # Return dummy headers or signature info if needed by _request implementation
        return {"mock_auth_header": "valid"} 

    async def _route_ws_message(self, message: Dict[str, Any]):
        """Route mock WebSocket messages (implementation needed)."""
        logger.debug(f"Mock {self.exchange_name}: Received WS message: {message}")
        # Basic routing logic can be added here based on message type
        pass 

    async def subscribe(self, topic: str, handler: MessageHandler):
        """Mock subscription - stores handler."""
        logger.info(f"Mock {self.exchange_name}: Subscribing to {topic}")
        self._ws_handlers[topic] = handler
        # In a real mock, you might start pushing simulated data for this topic

    async def _resubscribe(self):
        """Mock resubscription after reconnect."""
        logger.info(f"Mock {self.exchange_name}: Re-subscribing to {len(self._ws_handlers)} topics")
        # Simulate re-sending subscription requests if needed

    async def get_ticker(self, symbol: str) -> Optional[Ticker]:
        """Return a predefined mock ticker."""
        self._check_error_simulation("get_ticker")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting ticker for {symbol}")
        return self._mock_tickers.get(symbol)

    async def get_order_book(self, symbol: str, depth: Optional[int] = None) -> Optional[OrderBook]:
        """Return a predefined mock order book (basic implementation)."""
        self._check_error_simulation("get_order_book")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting order book for {symbol}")
        # Return a dummy OrderBook or None
        return OrderBook(symbol=symbol, bids=[], asks=[], timestamp=datetime.now(timezone.utc))

    async def get_recent_trades(self, symbol: str, limit: Optional[int] = None) -> List[Trade]:
        """Return an empty list of recent trades."""
        self._check_error_simulation("get_recent_trades")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting recent trades for {symbol}")
        return []

    async def get_funding_rate(self, symbol: str) -> Optional[FundingRate]:
        """Return a predefined mock funding rate."""
        self._check_error_simulation("get_funding_rate")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting funding rate for {symbol}")
        return self._mock_funding_rates.get(symbol)

    async def get_balances(self) -> Dict[str, Balance]:
        """Return the mock balances."""
        if self._fail_on_method == "get_balances":
             raise self._failure_exception
        await asyncio.sleep(0.01) # Simulate latency
        # Return a deep copy to prevent external modification
        return copy.deepcopy(self._balances)

    async def get_positions(self, symbols: Optional[List[Symbol]] = None) -> List[Position]:
        """Return mock positions, optionally filtered by symbols."""
        self._check_error_simulation("get_positions")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting positions")
        positions = [p for p in self._positions.values() if symbols is None or p.symbol in symbols]
        return positions

    async def place_order(self,
                        symbol: str,
                        side: OrderSide,
                        order_type: OrderType,
                        quantity: float,
                        price: Optional[float] = None,
                        client_order_id: Optional[str] = None,
                        time_in_force: Optional[str] = None, # Argument exists but not used by Order model
                        reduce_only: Optional[bool] = None, # Argument exists but not used by Order model
                        **kwargs) -> Order:
        """Simulate placing an order."""
        self._check_error_simulation("place_order")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Placing order: {symbol} {side.value} {quantity} @ {price if price else order_type.value}")
        
        order_id = f"mock_{self.exchange_name}_{self._order_id_counter}"
        self._order_id_counter += 1
        
        # --- Simulate more realistic fill for market orders --- 
        fill_price = price # Default to limit price if provided
        status = OrderStatus.OPEN if order_type == OrderType.LIMIT else OrderStatus.FILLED
        
        if order_type == OrderType.MARKET:
            # Get the exchange-specific symbol
            # Assuming a simple mapping for mock: internal symbol maps directly
            exchange_symbol = symbol # In real API, might need lookup config.get(f'exchanges.{self.exchange_name}.symbols.{symbol}')
            mock_ticker = self._mock_tickers.get(exchange_symbol)
            if mock_ticker:
                if side == OrderSide.BUY:
                    fill_price = mock_ticker.ask if mock_ticker.ask > 0 else mock_ticker.price
                else: # SELL
                    fill_price = mock_ticker.bid if mock_ticker.bid > 0 else mock_ticker.price
            else:
                logger.warning(f"Mock {self.exchange_name}: No ticker found for {exchange_symbol} to determine market fill price. Using default 40000.0")
                fill_price = 40000.0 # Fallback if no ticker set
        elif order_type == OrderType.LIMIT:
             if price is None:
                 raise ValueError("Limit price must be provided for LIMIT orders")
             fill_price = price # Use provided limit price for status OPEN
             status = OrderStatus.OPEN # Limit orders start as OPEN
        # ------------------------------------------------------

        filled_quantity = quantity if status == OrderStatus.FILLED else 0.0
        
        order = Order(
            id=order_id,
            client_order_id=client_order_id,
            symbol=symbol,
            price=fill_price, # Use the determined fill_price
            quantity=quantity,
            filled_quantity=filled_quantity,
            side=side,
            type=order_type,
            status=status,
            time=int(datetime.now(timezone.utc).timestamp() * 1000)
        )
        # self._orders[order_id] = order # Store initial order state before potentially modifying for fill behavior

        # --- Apply Fill Behavior --- 
        final_filled_quantity = 0.0
        final_status = status # Default to initial status (NEW for limit, FILLED for market)
        trade_to_record = None # Only create a trade if filled
        
        if self._open_orders_behavior == "fill_immediately" and status == OrderStatus.NEW:
            # Simulate immediate fill for limit orders if requested
            final_status = OrderStatus.FILLED
            final_filled_quantity = quantity
            order.status = final_status
            order.filled_quantity = final_filled_quantity
            logger.debug(f"Mock {self.exchange_name}: Simulating immediate fill for limit order {order_id}")
        
        elif self._open_orders_behavior == "partial_fill":
            # Simulate partial fill (e.g., 50%) regardless of initial status
            partial_fill_ratio = 0.5
            final_filled_quantity = quantity * partial_fill_ratio
            final_status = OrderStatus.PARTIALLY_FILLED
            order.status = final_status
            order.filled_quantity = final_filled_quantity
            logger.debug(f"Mock {self.exchange_name}: Simulating partial fill ({partial_fill_ratio*100}%) for order {order_id}")
        
        elif status == OrderStatus.FILLED:
            # If initially filled (e.g., market order), set final filled quantity
            final_filled_quantity = quantity
            final_status = OrderStatus.FILLED # Ensure it stays filled
            order.status = final_status # Update order object just in case
            order.filled_quantity = final_filled_quantity
            
        # --- Create Trade Record if Filled --- 
        if final_filled_quantity > 0:
             # Use the fill_price calculated earlier
            avg_fill_price = fill_price
            # Determine correct fee (taker for market/immediate fill, maker otherwise?)
            # Mock simplification: Assume taker fee for any filled amount initially
            trade_fee_rate = self.taker_fee

            trade_to_record = Trade(
                id=f"trade_{uuid.uuid4()}", # Unique trade ID
                symbol=symbol,
                price=avg_fill_price,
                quantity=final_filled_quantity,
                side=side,
                time=int(datetime.now(timezone.utc).timestamp() * 1000),
                # Use the determined fee rate and fee asset
                fee=abs(final_filled_quantity * avg_fill_price * trade_fee_rate),
                fee_asset=self.fee_asset # Use the class attribute
            )
            # Update mock balances and positions based *only* on the trade (filled amount)
            self._update_balance_and_position(trade_to_record)
            
        # --- Store Final Order State --- 
        self._orders[order_id] = order # Store potentially modified order (status, filled_qty)
                
        logger.debug(f"Mock {self.exchange_name}: Order created/updated: {order}")
        return order

    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Simulate cancelling an order."""
        self._check_error_simulation("cancel_order")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Cancelling order {order_id}")
        
        if order_id in self._orders:
            order = self._orders[order_id]
            if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                order.status = OrderStatus.CANCELLED
                logger.debug(f"Mock {self.exchange_name}: Order {order_id} cancelled.")
                return {"success": True, "orderId": order_id}
            else:
                logger.warning(f"Mock {self.exchange_name}: Order {order_id} cannot be cancelled (status: {order.status})")
                raise APIError(f"Order already {order.status.value}", code=APIErrorCode.ORDER_NOT_FOUND)
        else:
            logger.error(f"Mock {self.exchange_name}: Order {order_id} not found for cancellation.")
            raise APIError("Order not found", code=APIErrorCode.ORDER_NOT_FOUND)

    async def get_order(self, order_id: str) -> Optional[Order]:
        """Return a specific order by its ID from the mock store."""
        if self._fail_on_method == "get_order":
             raise self._failure_exception
        await asyncio.sleep(0.01) # Simulate latency
        return copy.deepcopy(self._orders.get(order_id))

    async def get_open_orders(self, symbol: Optional[Symbol] = None) -> List[Order]:
        """Return mock open orders, optionally filtered by symbol."""
        self._check_error_simulation("get_open_orders")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting open orders (symbol: {symbol})")
        open_orders = [
            o for o in self._orders.values() 
            if o.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]
        ]
        if symbol:
            open_orders = [o for o in open_orders if o.symbol == symbol]
        return open_orders
        
    # --- Mock-specific methods for test setup ---
    
    def set_mock_ticker(self, ticker: Ticker):
        """Set a ticker value for the mock to return."""
        logger.debug(f"Mock {self.exchange_name}: Setting mock ticker for {ticker.symbol}")
        self._mock_tickers[ticker.symbol] = ticker

    def set_mock_funding_rate(self, funding_rate: FundingRate):
        """Set a funding rate value for the mock to return."""
        logger.debug(f"Mock {self.exchange_name}: Setting mock funding rate for {funding_rate.symbol}")
        self._mock_funding_rates[funding_rate.symbol] = funding_rate
        
    def set_mock_balance(self, balance: Balance):
        """Set a balance value for the mock to return."""
        logger.debug(f"Mock {self.exchange_name}: Setting mock balance for {balance.asset}")
        self._balances[balance.asset] = balance

    def set_mock_position(self, position: Position):
        """Set a position value for the mock to return."""
        logger.debug(f"Mock {self.exchange_name}: Setting mock position for {position.symbol}")
        self._positions[position.symbol] = position

    def set_open_orders_behavior(self, behavior: str):
        """Set how open orders should behave (default, fill_immediately, partial_fill)."""
        if behavior not in ["default", "fill_immediately", "partial_fill"]:
            raise ValueError("Invalid open_orders_behavior")
        self._open_orders_behavior = behavior

    def set_latency(self, latency_ms: float):
        """Set the simulated latency for API calls."""
        self._latency_ms = max(0, latency_ms)
        logger.info(f"Mock {self.exchange_name} latency set to {self._latency_ms} ms")
        
    def reset(self):
        """Reset the mock state (orders, positions, etc.)."""
        self._order_id_counter = 1
        self._orders = {}
        self._positions = {}
        self._balances = {}
        self._mock_tickers = {}
        self._mock_funding_rates = {}
        self._latency_ms = 10.0
        self._error_simulation = None
        self._fail_on_method = None
        self._open_orders_behavior = "default"
        logger.info(f"MockExchangeAPI {self.exchange_name} reset.") 

    async def fetch_ticker(self, symbol: Symbol) -> Ticker:
        if self._fail_on_method == "fetch_ticker":
            raise self._failure_exception
        if self._mock_tickers and self._mock_tickers.get(symbol):
            return self._mock_tickers[symbol]
        # Simulate fetching if no specific mock is set
        return Ticker(
            symbol=symbol,
            bid=9990.0,
            ask=10010.0,
            last_price=10000.0,
            timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

    async def fetch_funding_rate(self, symbol: Symbol) -> FundingRate:
        if self._fail_on_method == "fetch_funding_rate":
            raise self._failure_exception
        if self._mock_funding_rates and self._mock_funding_rates.get(symbol):
            return self._mock_funding_rates[symbol]
        # Simulate fetching if no specific mock is set
        return FundingRate(
            symbol=symbol,
            funding_rate=0.0001,
            next_funding_time=int(
                (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp() * 1000
            ),
        )

    async def fetch_balances(self) -> Dict[str, Balance]:
        if self._fail_on_method == "fetch_balances":
            raise self._failure_exception
        await asyncio.sleep(0.01) # Simulate network latency
        return self._balances.copy()

    async def fetch_positions(self) -> Dict[Symbol, Position]:
        if self._fail_on_method == "fetch_positions":
            raise self._failure_exception
        await asyncio.sleep(0.01) # Simulate network latency
        return self._positions.copy()

    def _update_balance_and_position(self, trade: Trade):
        """Helper to update internal balances and positions based on a trade."""
        # Determine base asset from the symbol
        base_asset = trade.symbol.split('-')[0] if '-' in trade.symbol else trade.symbol

        # Determine the quote/margin asset for cost calculation
        # Prioritize USDC or USD if they exist as primary collateral
        if "USDC" in self._balances:
            quote_asset = "USDC"
        elif "USD" in self._balances:
            quote_asset = "USD"
        # Fallback: infer from symbol (less reliable for complex collateral)
        elif '-' in trade.symbol:
            quote_asset = trade.symbol.split('-')[1]
        else:
            quote_asset = "USD" # Default assumption if no hyphen and no primary collateral found

        logger.debug(f"Determined Quote Asset for cost calculation: {quote_asset}")

        # Ensure balances exist for involved assets
        if quote_asset not in self._balances:
            self.set_mock_balance(Balance(asset=quote_asset, total=10000.0, free=10000.0)) # Default initial balance
        if base_asset not in self._balances:
            self.set_mock_balance(Balance(asset=base_asset, total=0.0, free=0.0))

        cost = trade.quantity * trade.price
        # Fee deduction asset comes directly from the Trade object now
        fee_deduction_asset = trade.fee_asset
        fee = trade.fee # Fee amount is already calculated in Trade object

        logger.debug(f"Updating balance for {self.exchange_name}: Symbol={trade.symbol}, Side={trade.side.value}, Qty={trade.quantity}, Price={trade.price}, Cost={cost}, Fee={fee}, FeeAsset={fee_deduction_asset}, CalculatedQuoteAsset={quote_asset}")
        balance_before = self._balances.get(quote_asset)
        logger.debug(f"Balance BEFORE update ({quote_asset}): {balance_before}")
        # Ensure fee deduction asset balance exists
        if fee_deduction_asset not in self._balances:
             self.set_mock_balance(Balance(asset=fee_deduction_asset, total=10000.0, free=10000.0)) # Add if missing

        fee_balance_before = self._balances.get(fee_deduction_asset)
        logger.debug(f"Fee Balance BEFORE update ({fee_deduction_asset}): {fee_balance_before}")

        # Apply cost and fee updates (using trade.fee and trade.fee_asset)
        if trade.side == OrderSide.BUY:
            # Increase base asset
            self._balances[base_asset].total += trade.quantity
            self._balances[base_asset].free += trade.quantity
            # Decrease quote asset by cost
            self._balances[quote_asset].total -= cost
            self._balances[quote_asset].free -= cost
            # Decrease fee asset by fee
            self._balances[fee_deduction_asset].total -= fee
            self._balances[fee_deduction_asset].free -= fee
        else: # SELL
            # Decrease base asset
            self._balances[base_asset].total -= trade.quantity
            self._balances[base_asset].free -= trade.quantity
            # Increase quote asset by cost
            self._balances[quote_asset].total += cost
            self._balances[quote_asset].free += cost
            # Decrease fee asset by fee
            self._balances[fee_deduction_asset].total -= fee
            self._balances[fee_deduction_asset].free -= fee

        balance_after = self._balances.get(quote_asset)
        logger.debug(f"Balance AFTER update ({quote_asset}): {balance_after}")
        fee_balance_after = self._balances.get(fee_deduction_asset)
        logger.debug(f"Fee Balance AFTER update ({fee_deduction_asset}): {fee_balance_after}")

        # Update position (very basic) - Use 'size' instead of 'quantity'
        if trade.symbol not in self._positions:
            # Use 'size=0.0' in constructor
            self._positions[trade.symbol] = Position(symbol=trade.symbol, size=0.0, entry_price=0.0, mark_price=trade.price, side=trade.side) # Added mark_price and side

        pos = self._positions[trade.symbol]
        # Use pos.size for calculations
        if trade.side == OrderSide.BUY:
            new_size = pos.size + trade.quantity
        else:
            new_size = pos.size - trade.quantity

        # Simple average entry price update
        if new_size != 0:
            # Use pos.size here
            if pos.size == 0:
                 new_entry_price = trade.price
            # Use pos.size here 
            elif (pos.size > 0 and trade.side == OrderSide.BUY) or (pos.size < 0 and trade.side == OrderSide.SELL):
                # Averaging up/down - Use pos.size here
                new_entry_price = ((pos.size * pos.entry_price) + (trade.quantity * trade.price)) / new_size
            else:
                # Reducing position size - entry price doesn't change (usually PnL is realized)
                new_entry_price = pos.entry_price
        else:
            new_entry_price = 0.0 # Position closed

        # Set pos.size instead of pos.quantity
        pos.size = new_size
        pos.entry_price = new_entry_price
        pos.mark_price = trade.price # Update mark price on trade
        # Ensure side is correct, especially when opening/flipping
        if pos.size > 0:
            pos.side = OrderSide.BUY
        elif pos.size < 0:
            pos.side = OrderSide.SELL
            pos.size = abs(pos.size) # Store size as positive, side indicates direction
        else:
            # If size is zero, side doesn't strictly matter, maybe keep last or set default?
            pass # Keep existing side or reset?

        balance_after = self._balances.get(quote_asset)
        logger.debug(f"Balance AFTER update ({quote_asset}): {balance_after}")

    def get_trades(self) -> List[Trade]:
        # This logic was incorrect, need to store trades separately
        # return [trade for trade in self._orders.values() if trade.status == OrderStatus.FILLED]
        # Assuming trades are stored elsewhere or this method needs removal/rework
        # For now, return empty to avoid crashing tests if called
        return []

    def get_orders(self) -> Dict[str, Order]:
        return self._orders.copy() 