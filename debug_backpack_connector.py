#!/usr/bin/env python3
"""Debug script for Backpack connector initialization and trading pairs."""

import asyncio
import logging
from typing import Any, Dict

from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange
from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack import backpack_web_utils as web_utils

# Set up logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class BackpackConnectorDebugger:
    """Complete debugging for Backpack connector issues."""
    
    def __init__(self):
        self.api_key = "5qYV+z4HiGfVO02Sy4p9lWp/C6JmyqqiN9EBrYbBVKo="
        self.api_secret = "xrytCzv4u66CbKBKloQ1TcRYr8aHImyvICzq+odttog="
        
    async def test_public_endpoints(self):
        """Test all public endpoints that should work without auth."""
        logger.error("=== TESTING PUBLIC ENDPOINTS ===")
        
        public_endpoints = [
            (CONSTANTS.PING_URL, "Ping endpoint"),
            (CONSTANTS.TIME_URL, "Time endpoint"),
            (CONSTANTS.EXCHANGE_INFO_URL, "Markets endpoint"),
            ("api/v1/market?symbol=BTC_USDC", "Single market endpoint"),
            (CONSTANTS.TICKER_URL + "?symbol=BTC_USDC", "Single ticker endpoint"),
            ("api/v1/tickers", "All tickers endpoint"),
            (CONSTANTS.DEPTH_URL + "?symbol=BTC_USDC", "Depth endpoint"),
            (CONSTANTS.KLINES_URL + "?symbol=BTC_USDC&interval=1d&startTime=1756100000&endTime=1756200000", "Klines endpoint"),
            ("api/v1/markPrices", "Mark prices endpoint"),
            ("api/v1/openInterest", "Open interest endpoint"),
            ("api/v1/fundingRates?symbol=BTC_USDC_PERP", "Funding rates endpoint"),
            (CONSTANTS.TRADES_URL + "?symbol=BTC_USDC", "Recent trades endpoint"),
            ("api/v1/status", "System status endpoint"),
        ]
        
        for endpoint, description in public_endpoints:
            try:
                logger.error(f"Testing {description}: {endpoint}")
                
                # Create minimal web utils for testing
                throttler = web_utils.create_throttler()
                api_factory = web_utils.build_api_factory(throttler=throttler)
                rest_assistant = await api_factory.get_rest_assistant()
                
                from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTMethod
                
                url = f"https://api.backpack.exchange/{endpoint}"
                request = RESTRequest(method=RESTMethod.GET, url=url)
                response = await rest_assistant.call(request)
                
                if response.status == 200:
                    data = await response.json()
                    logger.error(f"  SUCCESS: {type(data)} with {len(data) if isinstance(data, (list, dict)) else 'N/A'} items")
                    if endpoint == CONSTANTS.EXCHANGE_INFO_URL:
                        spot_count = len([x for x in data if isinstance(x, dict) and x.get('marketType') == 'SPOT'])
                        logger.error(f"  Markets: {len(data)} total, {spot_count} SPOT")
                else:
                    logger.error(f"  FAILED: HTTP {response.status}")
                    
            except Exception as e:
                logger.error(f"  ERROR: {e}")
    
    async def test_futures_endpoints(self):
        """Test futures-specific endpoints separately."""
        logger.error("=== TESTING FUTURES ENDPOINTS ===")
        
        futures_endpoints = [
            ("api/v1/ticker?symbol=BTC_USDC_PERP", "Futures ticker"),
            ("api/v1/depth?symbol=BTC_USDC_PERP", "Futures depth"),
            ("api/v1/markPrices?symbol=BTC_USDC_PERP", "Single mark price"),
            ("api/v1/openInterest?symbol=BTC_USDC_PERP", "Single open interest"),
            ("api/v1/fundingRates?symbol=BTC_USDC_PERP&limit=10", "Funding rates with limit"),
            ("api/v1/trades?symbol=BTC_USDC_PERP", "Futures trades"),
        ]
        
        for endpoint, description in futures_endpoints:
            try:
                logger.error(f"Testing {description}: {endpoint}")
                
                throttler = web_utils.create_throttler()
                api_factory = web_utils.build_api_factory(throttler=throttler)
                rest_assistant = await api_factory.get_rest_assistant()
                
                from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTMethod
                
                url = f"https://api.backpack.exchange/{endpoint}"
                request = RESTRequest(method=RESTMethod.GET, url=url)
                response = await rest_assistant.call(request)
                
                if response.status == 200:
                    data = await response.json()
                    logger.error(f"  SUCCESS: {type(data)} with {len(data) if isinstance(data, (list, dict)) else 'N/A'} items")
                else:
                    error_text = await response.text()
                    logger.error(f"  FAILED: HTTP {response.status} - {error_text}")
                    
            except Exception as e:
                logger.error(f"  ERROR: {e}")
    
    async def test_connector_initialization(self):
        """Test connector initialization step by step."""
        logger.error("=== TESTING CONNECTOR INITIALIZATION ===")
        
        try:
            # Create connector with proper config
            logger.error("Creating connector instance...")
            from hummingbot.client.config.client_config_map import ClientConfigMap
            config_map = ClientConfigMap()
            
            connector = BackpackExchange(
                client_config_map=ClientConfigAdapter(config_map),
                backpack_api_key=self.api_key,
                backpack_api_secret=self.api_secret,
                trading_pairs=None,
                trading_required=False
            )
            
            logger.error(f"Connector created: {connector.name}")
            logger.error(f"Trading pairs request path: {connector.trading_pairs_request_path}")
            logger.error(f"Trading rules request path: {connector.trading_rules_request_path}")
            
            # Test the specific method that's failing
            logger.error("Testing _make_trading_pairs_request...")
            exchange_info = await connector._make_trading_pairs_request()
            logger.error(f"Exchange info type: {type(exchange_info)}")
            logger.error(f"Exchange info keys: {list(exchange_info.keys()) if isinstance(exchange_info, dict) else 'Not dict'}")
            
            if isinstance(exchange_info, dict):
                if "error" in exchange_info:
                    logger.error(f"API ERROR: {exchange_info}")
                elif "data" in exchange_info:
                    logger.error(f"Data type: {type(exchange_info['data'])}, length: {len(exchange_info['data'])}")
            
            # Test full connector initialization
            logger.error("Testing full connector startup...")
            try:
                await connector.start()
                logger.error("Connector startup SUCCESS")
                
                # Now check trading pairs
                trading_pairs = list(connector.trading_pairs)
                logger.error(f"Available trading pairs: {len(trading_pairs)}")
                btc_pairs = [tp for tp in trading_pairs if 'BTC' in tp]
                logger.error(f"BTC pairs: {btc_pairs}")
                
            except Exception as e:
                logger.error(f"Connector startup FAILED: {e}")
                
        except Exception as e:
            logger.error(f"Connector creation FAILED: {e}")
    
    async def test_trading_pair_conversion(self):
        """Test trading pair format conversions."""
        logger.error("=== TESTING TRADING PAIR CONVERSION ===")
        
        from hummingbot.connector.exchange.backpack import backpack_utils as utils
        
        test_pairs = ["BTC_USDC", "BTC/USDC", "BTC-USDC", "btc_usdc"]
        
        for test_pair in test_pairs:
            try:
                normalized = utils.normalize_trading_pair(test_pair)
                logger.error(f"'{test_pair}' -> '{normalized}'")
            except Exception as e:
                logger.error(f"'{test_pair}' -> ERROR: {e}")
    
    async def run_complete_debug(self):
        """Run all debugging tests."""
        logger.error("=" * 60)
        logger.error("BACKPACK CONNECTOR COMPLETE DEBUG")
        logger.error("=" * 60)
        
        await self.test_public_endpoints()
        await self.test_futures_endpoints()
        await self.test_connector_initialization()
        await self.test_trading_pair_conversion()
        
        logger.error("=" * 60)
        logger.error("DEBUG COMPLETE")
        logger.error("=" * 60)


async def main():
    """Run the complete debugging suite."""
    debugger = BackpackConnectorDebugger()
    await debugger.run_complete_debug()


if __name__ == "__main__":
    asyncio.run(main())