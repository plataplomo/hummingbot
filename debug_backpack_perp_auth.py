"""Debug script for Backpack Perpetual authentication issues.

This script tests the Backpack Perpetual authentication implementation by making a direct
API call to the balance endpoint and logging detailed information about the
authentication process.
"""

import asyncio
import json
import logging
import time
import traceback

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class BackpackPerpetualAuthDebugger:
    """Debug utility for Backpack Perpetual authentication issues."""

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
            # Synchronize time first - this is critical for Backpack auth
            self.logger.error("Synchronizing time with server...")
            await self.time_synchronizer.update_server_time_offset_with_time_provider(
                time_provider=web_utils.get_current_server_time(
                    throttler=web_utils.create_throttler(),
                    domain="backpack",
                ),
            )
            self.logger.error("Time offset: %s milliseconds", self.time_synchronizer.time_offset_ms)

            # Create authenticator like the exchange does
            auth = BackpackPerpetualAuth(
                api_key=self.api_key,
                api_secret=self.api_secret,
                time_provider=self.time_synchronizer,
            )

            # Create web assistants factory
            throttler = web_utils.create_throttler()
            api_factory = web_utils.build_api_factory(
                throttler=throttler,
                time_synchronizer=self.time_synchronizer,
                auth=auth,
            )

            # Get REST assistant
            rest_assistant = await api_factory.get_rest_assistant()

            # Create the exact request that's failing - perpetual uses api/v1 same as spot
            url = "https://api.backpack.exchange/api/v1/capital"
            request = RESTRequest(
                method=RESTMethod.GET,
                url=url,
                is_auth_required=True,
            )

            self.logger.error("Testing Backpack Perpetual balance endpoint: %s", url)

            # Make the request
            response = await rest_assistant.call(request)

            if response.status == 200:
                data = await response.json()
                self.logger.error("SUCCESS: Balance request succeeded")
                self.logger.error("Response data: %s", json.dumps(data, indent=2))
                return True
            else:
                error_text = await response.text()
                self.logger.error("FAILED: HTTP %s", response.status)
                self.logger.error("Error response: %s", error_text)

                # Try to parse error for more info
                try:
                    error_json = json.loads(error_text)
                    if "message" in error_json:
                        self.logger.error("Error message: %s", error_json["message"])
                    if "code" in error_json:
                        self.logger.error("Error code: %s", error_json["code"])
                except (json.JSONDecodeError, KeyError):
                    pass  # Error details not available in JSON format

                return False

        except Exception as e:
            self.logger.error("EXCEPTION during balance request: %s", e)
            self.logger.error("Traceback: %s", traceback.format_exc())
            return False

    async def test_timestamp_validation(self):
        """Test different timestamp formats to see what Backpack accepts."""
        try:
            current_time = time.time()

            # Show what the time synchronizer gives us
            synced_time = self.time_synchronizer.time()

            test_timestamps = [
                ("Current local time (sec)", int(current_time)),
                ("Current local time (ms)", int(current_time * 1000)),
                ("Synchronized time (sec)", int(synced_time)),
                ("Synchronized time (ms)", int(synced_time * 1000)),
            ]

            self.logger.error("Testing different timestamp formats:")
            for label, ts in test_timestamps:
                self.logger.error("%s: %s (%s digits)", label, ts, len(str(ts)))

        except Exception as e:
            self.logger.error("Error in timestamp validation: %s", e)

    async def test_additional_endpoints(self):
        """Test additional perpetual endpoints."""
        endpoints = [
            "/api/v1/account",  # Account info
            "/api/v1/positions",  # Open positions
            "/api/v1/orders",  # Open orders
        ]

        try:
            # Ensure time is synchronized
            await self.time_synchronizer.update_server_time_offset_with_time_provider(
                time_provider=web_utils.get_current_server_time(
                    throttler=web_utils.create_throttler(),
                    domain="backpack",
                ),
            )

            auth = BackpackPerpetualAuth(
                api_key=self.api_key,
                api_secret=self.api_secret,
                time_provider=self.time_synchronizer,
            )

            throttler = web_utils.create_throttler()
            api_factory = web_utils.build_api_factory(
                throttler=throttler,
                time_synchronizer=self.time_synchronizer,
                auth=auth,
            )
            rest_assistant = await api_factory.get_rest_assistant()

            for endpoint in endpoints:
                try:
                    url = f"https://api.backpack.exchange{endpoint}"
                    request = RESTRequest(
                        method=RESTMethod.GET,
                        url=url,
                        is_auth_required=True,
                    )

                    response = await rest_assistant.call(request)
                    if response.status == 200:
                        data = await response.json()
                        self.logger.error("SUCCESS %s: %s", endpoint, json.dumps(data, indent=2))
                    else:
                        error_text = await response.text()
                        self.logger.error("FAILED %s: HTTP %s - %s", endpoint, response.status, error_text)

                except Exception as e:
                    self.logger.error("ERROR %s: %s", endpoint, e)

        except Exception as e:
            self.logger.error("Error testing additional endpoints: %s", e)

    async def test_auth_signature_generation(self):
        """Test the signature generation process step by step."""
        try:
            auth = BackpackPerpetualAuth(
                api_key=self.api_key,
                api_secret=self.api_secret,
                time_provider=self.time_synchronizer,
            )

            # Test generating auth headers
            timestamp = str(int(self.time_synchronizer.time() * 1000))
            window = "5000"

            # Test signature for a simple endpoint
            method = "GET"
            path = "/api/v1/capital"

            # Build payload like the auth does
            instruction = auth._get_instruction_for_endpoint(method, path)
            payload = f"instruction={instruction}&timestamp={timestamp}&window={window}"

            self.logger.error("Instruction for %s %s: %s", method, path, instruction)
            self.logger.error("Signature payload: %s", payload)

            # Generate signature
            signature = auth._generate_signature(payload)
            self.logger.error("Generated signature: %s", signature)

            # Show full headers that would be sent
            headers = auth._generate_auth_headers(method, path)
            self.logger.error("Auth headers: %s", json.dumps(headers, indent=2))

        except Exception as e:
            self.logger.error("Error testing signature generation: %s", e)
            self.logger.error("Traceback: %s", traceback.format_exc())

    async def debug_authentication(self):
        """Run complete authentication debugging."""
        self.logger.error("=" * 50)
        self.logger.error("BACKPACK PERPETUAL AUTHENTICATION DEBUG")
        self.logger.error("=" * 50)

        # Test timestamp formats
        await self.test_timestamp_validation()

        # Test auth signature generation
        self.logger.error("\nTesting authentication signature generation...")
        await self.test_auth_signature_generation()

        # Test balance request
        self.logger.error("\nTesting balance endpoint authentication...")
        success = await self.debug_balance_request()

        # Test additional endpoints
        self.logger.error("\nTesting additional perpetual endpoints...")
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
    # Configure logging to show all debug output
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # You need to provide your actual Backpack credentials here
    # These should be the same credentials causing the authentication error
    API_KEY = "L4FwYS6tp1Lo81sV4qrnajBdzx7BdEETuE0y8r4SNFM="
    API_SECRET = "OncW9JJNqRPUjfy4qUwXwiN29+qdjgERcgYy+gl4KVw="  # noqa: S105

    if API_KEY == "your_backpack_api_key_here":
        logger = logging.getLogger(__name__)
        logger.error("ERROR: Please update API_KEY and API_SECRET with your actual Backpack credentials")
        return

    debugger = BackpackPerpetualAuthDebugger(API_KEY, API_SECRET)
    await debugger.debug_authentication()


if __name__ == "__main__":
    asyncio.run(main())
