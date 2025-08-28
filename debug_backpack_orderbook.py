#!/usr/bin/env python3
"""Debug script for Backpack order book data source issues."""

import asyncio
import logging
from typing import Any, Dict

from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange
from hummingbot.connector.exchange.backpack.backpack_api_order_book_data_source import BackpackAPIOrderBookDataSource
from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack import backpack_web_utils as web_utils

# Set up logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class BackpackOrderBookDebugger:
    """Debug Backpack order book data source issues."""
    
    def __init__(self):
        self.api_key = "5qYV+z4HiGfVO02Sy4p9lWp/C6JmyqqiN9EBrYbBVKo="
        self.api_secret = "xrytCzv4u66CbKBKloQ1TcRYr8aHImyvICzq+odttog="
        self.test_trading_pair = "BTC-USDC"
        
    async def test_order_book_endpoints(self):
        """Test order book related endpoints."""
        logger.error("=== TESTING ORDER BOOK ENDPOINTS ===")
        
        # Test depth endpoint specifically
        test_symbols = ["BTC_USDC", "SOL_USDC", "ETH_USDC"]
        
        for symbol in test_symbols:
            try:
                logger.error(f"Testing depth for {symbol}...")
                
                throttler = web_utils.create_throttler()
                api_factory = web_utils.build_api_factory(throttler=throttler)
                rest_assistant = await api_factory.get_rest_assistant()
                
                from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTMethod
                
                url = f"https://api.backpack.exchange/api/v1/depth?symbol={symbol}"
                request = RESTRequest(method=RESTMethod.GET, url=url)
                response = await rest_assistant.call(request)
                
                if response.status == 200:
                    data = await response.json()
                    logger.error(f"  SUCCESS {symbol}: {type(data)}")
                    if isinstance(data, dict):
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])
                        logger.error(f"    Bids: {len(bids)}, Asks: {len(asks)}")
                        if bids and asks:
                            logger.error(f"    Best bid: {bids[0]}, Best ask: {asks[0]}")
                else:
                    error_text = await response.text()
                    logger.error(f"  FAILED {symbol}: HTTP {response.status} - {error_text}")
                    
            except Exception as e:
                logger.error(f"  ERROR {symbol}: {e}")
    
    async def test_websocket_connection(self):
        """Test WebSocket connection to Backpack."""
        logger.error("=== TESTING WEBSOCKET CONNECTION ===")
        
        try:
            import websockets
            
            # Test basic WebSocket connection
            logger.error("Testing WebSocket connection to wss://ws.backpack.exchange/...")
            
            async with websockets.connect("wss://ws.backpack.exchange/") as websocket:
                logger.error("WebSocket connection SUCCESS")
                
                # Test subscription to depth channel
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": ["depth.BTC_USDC"]
                }
                
                logger.error(f"Sending subscription: {subscribe_msg}")
                await websocket.send(str(subscribe_msg).replace("'", '"'))
                
                # Wait for response
                logger.error("Waiting for WebSocket response...")
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    logger.error(f"WebSocket response: {response}")
                except asyncio.TimeoutError:
                    logger.error("WebSocket response timeout")
                    
        except Exception as e:
            logger.error(f"WebSocket test FAILED: {e}")
    
    async def test_order_book_data_source(self):
        """Test BackpackAPIOrderBookDataSource directly."""
        logger.error("=== TESTING ORDER BOOK DATA SOURCE ===")
        
        try:
            # Create order book data source
            logger.error("Creating BackpackExchange with detailed logging...")
            
            try:
                config_map = ClientConfigMap()
                logger.error("Config map created")
                
                connector = BackpackExchange(
                    client_config_map=ClientConfigAdapter(config_map),
                    backpack_api_key=self.api_key,
                    backpack_api_secret=self.api_secret,
                    trading_pairs=[self.test_trading_pair],
                    trading_required=False
                )
                logger.error("Connector __init__ completed")
                
                # Check if base class constructor was called
                if hasattr(connector, '_throttler'):
                    logger.error("Base class _throttler exists")
                else:
                    logger.error("Base class _throttler missing - constructor failed")
                    
                # Try to manually create order book tracker to see what fails
                logger.error("Testing manual OrderBookTracker creation...")
                try:
                    from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
                    
                    manual_tracker = OrderBookTracker(
                        data_source=connector._orderbook_ds,
                        trading_pairs=connector.trading_pairs,
                        domain=connector.domain
                    )
                    logger.error(f"Manual OrderBookTracker SUCCESS: {type(manual_tracker)}")
                    
                    # Try to set it manually
                    connector._set_order_book_tracker(manual_tracker)
                    logger.error("Manual _set_order_book_tracker SUCCESS")
                    
                except Exception as tracker_error:
                    logger.error(f"Manual OrderBookTracker FAILED: {tracker_error}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    
                # Test manual base constructor replication
                logger.error("Testing manual base constructor steps...")
                try:
                    from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
                    
                    # Force create what base constructor should do
                    logger.error("Force creating OrderBookTracker...")
                    manual_ds = connector._create_order_book_data_source()
                    manual_tracker = OrderBookTracker(
                        data_source=manual_ds,
                        trading_pairs=connector.trading_pairs,
                        domain=connector.domain
                    )
                    
                    logger.error("Force setting tracker...")
                    connector._order_book_tracker = manual_tracker
                    
                    # Verify it exists now
                    if hasattr(connector, '_order_book_tracker'):
                        logger.error("Order book tracker NOW EXISTS after force creation")
                    else:
                        logger.error("Order book tracker STILL missing after force creation")
                        
                except Exception as force_error:
                    logger.error(f"Force creation FAILED: {force_error}")
                    
            except Exception as init_error:
                logger.error(f"Connector __init__ FAILED: {init_error}")
                return
            
            order_book_data_source = BackpackAPIOrderBookDataSource(
                trading_pairs=[self.test_trading_pair],
                connector=connector,
                api_factory=connector._web_assistants_factory
            )
            
            logger.error("Order book data source created successfully")
            
            # Test getting snapshot using the actual method
            logger.error(f"Testing _order_book_snapshot for {self.test_trading_pair}...")
            try:
                snapshot_msg = await order_book_data_source._order_book_snapshot(self.test_trading_pair)
                logger.error(f"Snapshot SUCCESS: {type(snapshot_msg)}")
                if hasattr(snapshot_msg, 'content'):
                    content = snapshot_msg.content
                    bids = content.get('bids', [])
                    asks = content.get('asks', [])
                    logger.error(f"Bids: {len(bids)}, Asks: {len(asks)}")
            except Exception as e:
                logger.error(f"Snapshot FAILED: {e}")
                
            # Test request snapshot directly
            logger.error(f"Testing _request_order_book_snapshot for {self.test_trading_pair}...")
            try:
                snapshot_data = await order_book_data_source._request_order_book_snapshot(self.test_trading_pair)
                logger.error(f"Request snapshot SUCCESS: {type(snapshot_data)}")
                if isinstance(snapshot_data, dict):
                    bids = snapshot_data.get('bids', [])
                    asks = snapshot_data.get('asks', [])
                    logger.error(f"Raw bids: {len(bids)}, asks: {len(asks)}")
            except Exception as e:
                logger.error(f"Request snapshot FAILED: {e}")
                
            # Test last traded prices
            logger.error("Testing last traded prices...")
            try:
                last_prices = await order_book_data_source.get_last_traded_prices([self.test_trading_pair])
                logger.error(f"Last prices SUCCESS: {last_prices}")
            except Exception as e:
                logger.error(f"Last prices FAILED: {e}")
                
        except Exception as e:
            logger.error(f"Order book data source creation FAILED: {e}")
    
    async def test_order_book_tracker(self):
        """Test order book tracker initialization."""
        logger.error("=== TESTING ORDER BOOK TRACKER ===")
        
        try:
            config_map = ClientConfigMap()
            connector = BackpackExchange(
                client_config_map=ClientConfigAdapter(config_map),
                backpack_api_key=self.api_key,
                backpack_api_secret=self.api_secret,
                trading_pairs=[self.test_trading_pair],
                trading_required=False
            )
            
            logger.error("Testing order book tracker creation...")
            
            # Force connector initialization
            await connector._initialize_trading_pair_symbol_map()
            logger.error("Symbol map initialized")
            
            # Debug connector properties
            logger.error(f"Connector domain: {connector.domain}")
            logger.error(f"Connector trading_pairs: {connector.trading_pairs}")
            logger.error(f"Connector _trading_pairs: {connector._trading_pairs}")
            
            # Check all important attributes
            important_attrs = ['_order_book_tracker', '_orderbook_ds', '_web_assistants_factory', '_throttler']
            for attr in important_attrs:
                if hasattr(connector, attr):
                    val = getattr(connector, attr)
                    logger.error(f"{attr}: {type(val)} {val if attr != '_throttler' else 'exists'}")
                else:
                    logger.error(f"{attr}: MISSING")
            
            # Check if order book tracker exists
            if hasattr(connector, '_order_book_tracker'):
                logger.error(f"Order book tracker exists: {type(connector._order_book_tracker)}")
                
                # Check data source
                if hasattr(connector, '_orderbook_ds'):
                    logger.error(f"Order book data source exists: {type(connector._orderbook_ds)}")
                else:
                    logger.error("Order book data source missing")
                
                # Try to start the tracker
                logger.error("Starting order book tracker...")
                tracker = connector._order_book_tracker
                if tracker:
                    await tracker.start()
                    logger.error("Order book tracker started")
                    
                    # Wait a moment for data
                    await asyncio.sleep(3)
                    
                    # Check order books
                    order_books = connector.order_books
                    logger.error(f"Order books count: {len(order_books)}")
                    for pair, book in order_books.items():
                        logger.error(f"Order book for {pair}: {type(book)}")
                        
            else:
                logger.error("Order book tracker does not exist")
                
                # Debug what attributes the connector has
                attrs = [attr for attr in dir(connector) if 'order_book' in attr.lower()]
                logger.error(f"Order book related attributes: {attrs}")
                
        except Exception as e:
            logger.error(f"Order book tracker test FAILED: {e}")
    
    async def test_trading_pair_validation(self):
        """Test if trading pair exists and is valid."""
        logger.error("=== TESTING TRADING PAIR VALIDATION ===")
        
        try:
            # Check if BTC-USDC is in available trading pairs
            config_map = ClientConfigMap()
            connector = BackpackExchange(
                client_config_map=ClientConfigAdapter(config_map),
                backpack_api_key=self.api_key,
                backpack_api_secret=self.api_secret,
                trading_required=False
            )
            
            # Force trading pair initialization
            logger.error("Forcing trading pair symbol map initialization...")
            await connector._initialize_trading_pair_symbol_map()
            
            # Check available pairs
            available_pairs = list(connector._trading_pair_to_exchange_symbol_map.keys())
            logger.error(f"Available trading pairs: {len(available_pairs)}")
            
            btc_pairs = [pair for pair in available_pairs if 'BTC' in pair]
            logger.error(f"BTC pairs available: {btc_pairs}")
            
            if self.test_trading_pair in available_pairs:
                logger.error(f"✅ {self.test_trading_pair} is available")
                exchange_symbol = connector._exchange_symbol_associated_to_pair(self.test_trading_pair)
                logger.error(f"Exchange symbol: {exchange_symbol}")
            else:
                logger.error(f"❌ {self.test_trading_pair} is NOT available")
                
        except Exception as e:
            logger.error(f"Trading pair validation FAILED: {e}")
    
    async def run_complete_debug(self):
        """Run all order book debugging tests."""
        logger.error("=" * 60)
        logger.error("BACKPACK ORDER BOOK DEBUG")
        logger.error("=" * 60)
        
        await self.test_order_book_endpoints()
        await self.test_websocket_connection()
        await self.test_trading_pair_validation()
        await self.test_order_book_data_source()
        await self.test_order_book_tracker()
        
        logger.error("=" * 60)
        logger.error("ORDER BOOK DEBUG COMPLETE")
        logger.error("=" * 60)


async def main():
    """Run the complete order book debugging suite."""
    debugger = BackpackOrderBookDebugger()
    await debugger.run_complete_debug()


if __name__ == "__main__":
    asyncio.run(main())