#!/usr/bin/env python3
"""Debug script for Backpack authentication issues.

This script tests the Backpack authentication implementation by making a direct
API call to the balance endpoint and logging detailed information about the
authentication process.
"""

import asyncio
import json
import time
from decimal import Decimal

from hummingbot.connector.exchange.backpack import backpack_web_utils as web_utils
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
import logging


class BackpackAuthDebugger:
    """Debug utility for Backpack authentication issues."""

    def __init__(self, api_key: str, api_secret: str):
        """Initialize debugger with credentials."""
        self.api_key = api_key
        self.api_secret = api_secret
        self.logger = logging.getLogger(__name__)
        
        # Create time synchronizer like the exchange does
        self.time_synchronizer = TimeSynchronizer()

    async def debug_balance_request(self):
        """Debug the balance endpoint authentication."""
        try:
            # Create authenticator like the exchange does
            auth = BackpackAuth(
                api_key=self.api_key,
                api_secret=self.api_secret,
                time_provider=self.time_synchronizer
            )
            
            # Create web assistants factory
            throttler = web_utils.create_throttler()
            api_factory = web_utils.build_api_factory(
                throttler=throttler,
                time_synchronizer=self.time_synchronizer,
                auth=auth
            )
            
            # Get REST assistant
            rest_assistant = await api_factory.get_rest_assistant()
            
            # Create the exact request that's failing
            url = "https://api.backpack.exchange/api/v1/capital"
            request = RESTRequest(
                method=RESTMethod.GET,
                url=url,
                is_auth_required=True
            )
            
            self.logger.error(f"Testing Backpack balance endpoint: {url}")
            
            # Make the request
            response = await rest_assistant.call(request)
            
            if response.status == 200:
                data = await response.json()
                self.logger.error(f"SUCCESS: Balance request succeeded")
                self.logger.error(f"Response data: {json.dumps(data, indent=2)}")
                return True
            else:
                error_text = await response.text()
                self.logger.error(f"FAILED: HTTP {response.status}")
                self.logger.error(f"Error response: {error_text}")
                return False
                
        except Exception as e:
            self.logger.error(f"EXCEPTION during balance request: {e}")
            return False

    async def test_timestamp_validation(self):
        """Test different timestamp formats to see what Backpack accepts."""
        try:
            current_time = time.time()
            
            test_timestamps = [
                int(current_time),                    # 10 digits (seconds)
                int(current_time * 1000),             # 13 digits (milliseconds)
                int(current_time * 1000000),          # 16 digits (microseconds)
            ]
            
            self.logger.error("Testing different timestamp formats:")
            for i, ts in enumerate(test_timestamps):
                self.logger.error(f"Format {i+1}: {ts} ({len(str(ts))} digits)")
                
        except Exception as e:
            self.logger.error(f"Error in timestamp validation: {e}")

    async def test_additional_endpoints(self):
        """Test additional endpoints to find account balances."""
        endpoints = [
            "/api/v1/account",
            "/api/v1/capital/collateral",
        ]
        
        try:
            auth = BackpackAuth(
                api_key=self.api_key,
                api_secret=self.api_secret,
                time_provider=self.time_synchronizer
            )
            
            throttler = web_utils.create_throttler()
            api_factory = web_utils.build_api_factory(
                throttler=throttler,
                time_synchronizer=self.time_synchronizer,
                auth=auth
            )
            rest_assistant = await api_factory.get_rest_assistant()
            
            for endpoint in endpoints:
                try:
                    url = f"https://api.backpack.exchange{endpoint}"
                    request = RESTRequest(
                        method=RESTMethod.GET,
                        url=url,
                        is_auth_required=True
                    )
                    
                    response = await rest_assistant.call(request)
                    if response.status == 200:
                        data = await response.json()
                        self.logger.error(f"SUCCESS {endpoint}: {json.dumps(data, indent=2)}")
                    else:
                        error_text = await response.text()
                        self.logger.error(f"FAILED {endpoint}: HTTP {response.status} - {error_text}")
                        
                except Exception as e:
                    self.logger.error(f"ERROR {endpoint}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error testing additional endpoints: {e}")

    async def debug_authentication(self):
        """Run complete authentication debugging."""
        self.logger.error("=" * 50)
        self.logger.error("BACKPACK AUTHENTICATION DEBUG")
        self.logger.error("=" * 50)
        
        # Test timestamp formats
        await self.test_timestamp_validation()
        
        # Test balance request
        self.logger.error("\nTesting balance endpoint authentication...")
        success = await self.debug_balance_request()
        
        # Test additional endpoints to find the 10 USDC
        await self.test_additional_endpoints()
        
        self.logger.error("=" * 50)
        if success:
            self.logger.error("RESULT: Authentication successful!")
        else:
            self.logger.error("RESULT: Authentication failed - check logs above")
        self.logger.error("=" * 50)
        
        return success


async def main():
    """Main function to run authentication debugging."""
    # You need to provide your actual Backpack credentials here
    # These should be the same credentials causing the authentication error
    API_KEY = "5qYV+z4HiGfVO02Sy4p9lWp/C6JmyqqiN9EBrYbBVKo="
    API_SECRET = "xrytCzv4u66CbKBKloQ1TcRYr8aHImyvICzq+odttog="
    
    if API_KEY == "your_backpack_api_key_here":
        print("ERROR: Please update API_KEY and API_SECRET with your actual Backpack credentials")
        return
        
    debugger = BackpackAuthDebugger(API_KEY, API_SECRET)
    await debugger.debug_authentication()


if __name__ == "__main__":
    asyncio.run(main())