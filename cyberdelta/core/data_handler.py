import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta

from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.core.models import MarketData
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)

class DataHandler:
    """
    Centralized market data collection and management.
    
    Responsible for:
    - Establishing and maintaining connections to exchange APIs
    - Processing market data streams (WebSocket, REST)
    - Storing and managing latest market data
    - Validating and normalizing data
    - Tracking data freshness
    - Providing clean, consistent data access
    """
    
    def __init__(self, config: Config):
        """
        Initialize the DataHandler.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.api_clients: Dict[str, ExchangeAPI] = {}
        
        # Market data storage
        self.tickers: Dict[str, Dict[str, MarketData]] = {}  # exchange -> symbol -> ticker
        self.funding_rates: Dict[str, Dict[str, Tuple[float, datetime]]] = {}  # exchange -> symbol -> (rate, timestamp)
        self.orderbooks: Dict[str, Dict[str, Any]] = {}  # exchange -> symbol -> orderbook
        
        # Data freshness tracking
        self.last_update_time: Dict[str, Dict[str, Dict[str, datetime]]] = {}  # exchange -> data_type -> symbol -> timestamp
        
        # WebSocket connection management
        self.ws_connections: Dict[str, Any] = {}
        self.ws_tasks: Dict[str, asyncio.Task] = {}
        self.reconnect_attempts: Dict[str, int] = {}
        
        # Data staleness thresholds (in seconds)
        self.staleness_thresholds = {
            'ticker': 60,
            'funding_rate': 300,
            'orderbook': 60
        }
        
        self._setup_data_structures()
        
    def _setup_data_structures(self):
        """Initialize data structures for all configured exchanges and symbols."""
        for exchange_id in self.config.get('exchanges', {}).keys():
            if not self.config.get(f'exchanges.{exchange_id}.enabled', False):
                continue
                
            # Initialize data dictionaries for this exchange
            self.tickers[exchange_id] = {}
            self.funding_rates[exchange_id] = {}
            self.orderbooks[exchange_id] = {}
            self.last_update_time[exchange_id] = {
                'ticker': {},
                'funding_rate': {},
                'orderbook': {}
            }
            
            # Initialize reconnect attempt counter
            self.reconnect_attempts[exchange_id] = 0
            
    def register_api_client(self, exchange_id: str, client: ExchangeAPI):
        """
        Register an API client for an exchange.
        
        Args:
            exchange_id: Exchange identifier
            client: ExchangeAPI implementation
        """
        self.api_clients[exchange_id] = client
        logger.info(f"Registered API client for {exchange_id}")
        
    async def initialize(self):
        """Initialize connections and start data collection."""
        # Start WebSocket connections for all exchanges
        for exchange_id, client in self.api_clients.items():
            if not self.config.get(f'exchanges.{exchange_id}.enabled', False):
                continue
                
            # Start WebSocket connections
            self.ws_tasks[exchange_id] = asyncio.create_task(
                self._maintain_websocket_connection(exchange_id)
            )
            logger.info(f"Started WebSocket maintenance task for {exchange_id}")
        
        # Initial data collection (synchronous to ensure we have data before proceeding)
        await self._collect_initial_data()
        
    async def _collect_initial_data(self):
        """Collect initial data from all exchanges."""
        collection_tasks = []
        
        for exchange_id, client in self.api_clients.items():
            if not self.config.get(f'exchanges.{exchange_id}.enabled', False):
                continue
                
            # Get exchange symbols
            symbols = self.config.get(f'exchanges.{exchange_id}.symbols', [])
            
            # Create tasks for each initial data collection
            collection_tasks.append(self._collect_tickers(exchange_id, symbols))
            collection_tasks.append(self._collect_funding_rates(exchange_id, symbols))
            
        # Wait for all initial data collection to complete
        await asyncio.gather(*collection_tasks, return_exceptions=True)
        logger.info("Initial data collection completed")
        
    async def _maintain_websocket_connection(self, exchange_id: str):
        """
        Maintain a WebSocket connection to an exchange with exponential backoff reconnection.
        
        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]
        min_reconnect_delay = self.config.get(f'exchanges.{exchange_id}.websocket.reconnect_delay', 5)
        max_reconnect_delay = self.config.get(f'exchanges.{exchange_id}.websocket.max_reconnect_delay', 300)
        symbols = self.config.get(f'exchanges.{exchange_id}.symbols', [])
        
        while True:
            try:
                # Calculate reconnection delay with exponential backoff
                reconnect_delay = min(min_reconnect_delay * (2 ** self.reconnect_attempts[exchange_id]), 
                                    max_reconnect_delay)
                
                if self.reconnect_attempts[exchange_id] > 0:
                    logger.warning(f"Reconnecting to {exchange_id} WebSocket in {reconnect_delay} seconds "
                                   f"(attempt {self.reconnect_attempts[exchange_id]})")
                    await asyncio.sleep(reconnect_delay)
                
                # Connect to WebSocket
                self.ws_connections[exchange_id] = await client.connect_websocket()
                logger.info(f"Connected to {exchange_id} WebSocket")
                
                # Subscribe to channels
                await client.subscribe_to_tickers(symbols)
                await client.subscribe_to_orderbooks(symbols)
                
                # Exchange-specific subscriptions
                if exchange_id == "hyperliquid":
                    # Hyperliquid has hourly funding payments
                    await client.subscribe_to_funding_updates(symbols)
                
                # Reset reconnection attempts on successful connection
                self.reconnect_attempts[exchange_id] = 0
                
                # Process messages
                await self._process_websocket_messages(exchange_id)
                
            except asyncio.CancelledError:
                logger.info(f"WebSocket task for {exchange_id} was cancelled")
                break
            except Exception as e:
                logger.error(f"WebSocket connection error for {exchange_id}: {str(e)}", exc_info=True)
                # Increment reconnection attempts
                self.reconnect_attempts[exchange_id] += 1
    
    async def _process_websocket_messages(self, exchange_id: str):
        """
        Process messages from a WebSocket connection.
        
        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]
        ping_interval = self.config.get(f'exchanges.{exchange_id}.websocket.ping_interval', 30)
        last_ping_time = time.time()
        
        while True:
            # Check if ping is needed
            current_time = time.time()
            if current_time - last_ping_time > ping_interval:
                await client.ping_websocket()
                last_ping_time = current_time
            
            # Process incoming messages
            message = await client.receive_websocket_message()
            if message:
                await self._handle_websocket_message(exchange_id, message)
    
    async def _handle_websocket_message(self, exchange_id: str, message: Dict[str, Any]):
        """
        Handle a WebSocket message.
        
        Args:
            exchange_id: Exchange identifier
            message: WebSocket message
        """
        try:
            client = self.api_clients[exchange_id]
            message_type = client.get_message_type(message)
            
            if message_type == 'ticker':
                symbol, ticker_data = client.parse_ticker_message(message)
                self._update_ticker(exchange_id, symbol, ticker_data)
                
            elif message_type == 'orderbook':
                symbol, orderbook_data = client.parse_orderbook_message(message)
                self._update_orderbook(exchange_id, symbol, orderbook_data)
                
            elif message_type == 'funding':
                symbol, funding_data = client.parse_funding_message(message)
                self._update_funding_rate(exchange_id, symbol, funding_data)
                
            elif message_type == 'error':
                error_message = client.parse_error_message(message)
                logger.error(f"WebSocket error from {exchange_id}: {error_message}")
                
            else:
                # Unknown message type
                logger.debug(f"Received unknown message type from {exchange_id}: {message_type}")
                
        except Exception as e:
            logger.error(f"Error handling WebSocket message from {exchange_id}: {str(e)}", exc_info=True)
    
    def _update_ticker(self, exchange_id: str, symbol: str, ticker_data: Dict[str, Any]):
        """
        Update ticker data.
        
        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol
            ticker_data: Ticker data
        """
        # Create MarketData object
        market_data = MarketData(
            symbol=symbol,
            timestamp=ticker_data.get('timestamp', datetime.now()),
            open=ticker_data.get('open', 0.0),
            high=ticker_data.get('high', 0.0),
            low=ticker_data.get('low', 0.0),
            close=ticker_data.get('close', 0.0),
            volume=ticker_data.get('volume', 0.0),
            additional_data=ticker_data.get('additional_data', {})
        )
        
        # Update data
        self.tickers[exchange_id][symbol] = market_data
        self.last_update_time[exchange_id]['ticker'][symbol] = datetime.now()
        
    def _update_orderbook(self, exchange_id: str, symbol: str, orderbook_data: Dict[str, Any]):
        """
        Update orderbook data.
        
        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol
            orderbook_data: Orderbook data
        """
        self.orderbooks[exchange_id][symbol] = orderbook_data
        self.last_update_time[exchange_id]['orderbook'][symbol] = datetime.now()
    
    def _update_funding_rate(self, exchange_id: str, symbol: str, funding_data: Dict[str, Any]):
        """
        Update funding rate data.
        
        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol
            funding_data: Funding rate data
        """
        rate = funding_data.get('rate', 0.0)
        timestamp = funding_data.get('timestamp', datetime.now())
        
        self.funding_rates[exchange_id][symbol] = (rate, timestamp)
        self.last_update_time[exchange_id]['funding_rate'][symbol] = datetime.now()
        
        logger.info(f"Updated funding rate for {exchange_id}/{symbol}: {rate} at {timestamp}")
    
    async def _collect_tickers(self, exchange_id: str, symbols: List[str]):
        """
        Collect ticker data from REST API.
        
        Args:
            exchange_id: Exchange identifier
            symbols: List of trading symbols
        """
        client = self.api_clients[exchange_id]
        
        try:
            tickers = await client.get_tickers(symbols)
            
            for symbol, ticker_data in tickers.items():
                self._update_ticker(exchange_id, symbol, ticker_data)
                
            logger.debug(f"Collected {len(tickers)} tickers from {exchange_id}")
            
        except Exception as e:
            logger.error(f"Error collecting tickers from {exchange_id}: {str(e)}", exc_info=True)
    
    async def _collect_funding_rates(self, exchange_id: str, symbols: List[str]):
        """
        Collect funding rate data from REST API.
        
        Args:
            exchange_id: Exchange identifier
            symbols: List of trading symbols
        """
        client = self.api_clients[exchange_id]
        
        try:
            funding_rates = await client.get_funding_rates(symbols)
            
            for symbol, funding_data in funding_rates.items():
                rate = funding_data.get('rate', 0.0)
                timestamp = funding_data.get('timestamp', datetime.now())
                
                self.funding_rates[exchange_id][symbol] = (rate, timestamp)
                self.last_update_time[exchange_id]['funding_rate'][symbol] = datetime.now()
                
            logger.debug(f"Collected {len(funding_rates)} funding rates from {exchange_id}")
            
        except Exception as e:
            logger.error(f"Error collecting funding rates from {exchange_id}: {str(e)}", exc_info=True)
    
    async def update_all_data(self):
        """Collect all data from REST APIs to ensure freshness."""
        update_tasks = []
        
        for exchange_id, client in self.api_clients.items():
            if not self.config.get(f'exchanges.{exchange_id}.enabled', False):
                continue
                
            # Get exchange symbols
            symbols = self.config.get(f'exchanges.{exchange_id}.symbols', [])
            
            # Create tasks for data collection
            update_tasks.append(self._collect_tickers(exchange_id, symbols))
            update_tasks.append(self._collect_funding_rates(exchange_id, symbols))
            
        # Wait for all data collection to complete
        await asyncio.gather(*update_tasks, return_exceptions=True)
        logger.info("All data updated")
    
    def get_ticker(self, exchange_id: str, symbol: str) -> Optional[MarketData]:
        """
        Get the most recent ticker data for a symbol.
        
        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            MarketData object or None if not available
        """
        if exchange_id not in self.tickers or symbol not in self.tickers[exchange_id]:
            return None
            
        # Check data freshness
        last_update = self.last_update_time[exchange_id]['ticker'].get(symbol)
        if last_update:
            time_since_update = (datetime.now() - last_update).total_seconds()
            if time_since_update > self.staleness_thresholds['ticker']:
                logger.warning(f"Stale ticker data for {exchange_id}/{symbol}: {time_since_update:.1f}s old")
        
        return self.tickers[exchange_id][symbol]
        
    def get_funding_rate(self, exchange_id: str, symbol: str) -> Optional[Tuple[float, datetime]]:
        """
        Get the most recent funding rate for a symbol.
        
        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            Tuple of (rate, timestamp) or None if not available
        """
        if exchange_id not in self.funding_rates or symbol not in self.funding_rates[exchange_id]:
            return None
            
        # Check data freshness
        last_update = self.last_update_time[exchange_id]['funding_rate'].get(symbol)
        if last_update:
            time_since_update = (datetime.now() - last_update).total_seconds()
            if time_since_update > self.staleness_thresholds['funding_rate']:
                logger.warning(f"Stale funding rate data for {exchange_id}/{symbol}: {time_since_update:.1f}s old")
        
        return self.funding_rates[exchange_id][symbol]
        
    def get_orderbook(self, exchange_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the most recent orderbook for a symbol.
        
        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            Orderbook data or None if not available
        """
        if exchange_id not in self.orderbooks or symbol not in self.orderbooks[exchange_id]:
            return None
            
        # Check data freshness
        last_update = self.last_update_time[exchange_id]['orderbook'].get(symbol)
        if last_update:
            time_since_update = (datetime.now() - last_update).total_seconds()
            if time_since_update > self.staleness_thresholds['orderbook']:
                logger.warning(f"Stale orderbook data for {exchange_id}/{symbol}: {time_since_update:.1f}s old")
        
        return self.orderbooks[exchange_id][symbol]
    
    async def shutdown(self):
        """Properly close all connections."""
        # Cancel all WebSocket tasks
        for exchange_id, task in self.ws_tasks.items():
            try:
                task.cancel()
                await task
            except Exception as e:
                logger.error(f"Error canceling WebSocket task for {exchange_id}: {str(e)}")
        
        # Close WebSocket connections
        for exchange_id, ws in self.ws_connections.items():
            try:
                client = self.api_clients[exchange_id]
                await client.close_websocket()
            except Exception as e:
                logger.error(f"Error closing WebSocket for {exchange_id}: {str(e)}")
        
        logger.info("DataHandler shutdown complete")
