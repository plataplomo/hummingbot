import asyncio
import json
from typing import Awaitable
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses

import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_web_utils as web_utils
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class BackpackPerpetualWebUtilsTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_public_rest_url(self):
        """Test public REST URL construction."""
        url = web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_URL)
        self.assertEqual(url, f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_BOOK_URL}")

        url = web_utils.public_rest_url(CONSTANTS.TICKER_URL)
        self.assertEqual(url, f"{CONSTANTS.REST_URL}{CONSTANTS.TICKER_URL}")

    def test_private_rest_url(self):
        """Test private REST URL construction."""
        url = web_utils.private_rest_url(CONSTANTS.POSITIONS_URL)
        self.assertEqual(url, f"{CONSTANTS.REST_URL}{CONSTANTS.POSITIONS_URL}")

        url = web_utils.private_rest_url(CONSTANTS.PLACE_ORDER_URL)
        self.assertEqual(url, f"{CONSTANTS.REST_URL}{CONSTANTS.PLACE_ORDER_URL}")

    def test_build_api_factory(self):
        """Test API factory creation."""
        throttler = AsyncThrottler(rate_limits=[])
        api_factory = web_utils.build_api_factory(throttler=throttler)

        self.assertIsInstance(api_factory, WebAssistantsFactory)
        self.assertIs(api_factory._throttler, throttler)

    def test_build_api_factory_with_auth(self):
        """Test API factory creation with authentication."""
        throttler = AsyncThrottler(rate_limits=[])
        auth = MagicMock()
        api_factory = web_utils.build_api_factory(throttler=throttler, auth=auth)

        self.assertIsInstance(api_factory, WebAssistantsFactory)
        self.assertIs(api_factory._throttler, throttler)
        self.assertIs(api_factory._auth, auth)

    def test_create_throttler(self):
        """Test throttler creation with rate limits."""
        throttler = web_utils.create_throttler()

        self.assertIsInstance(throttler, AsyncThrottler)
        # Check that rate limits are configured
        self.assertGreater(len(throttler._rate_limits), 0)

    @aioresponses()
    def test_api_request_success(self, mock_api):
        """Test successful API request."""
        url = web_utils.public_rest_url(CONSTANTS.TICKER_URL)
        regex_url = f"^{url}"

        expected_response = {
            "symbol": "BTC_PERP",
            "lastPrice": "50000.00",
            "markPrice": "50001.00",
            "fundingRate": "0.0001",
        }

        mock_api.get(regex_url, body=json.dumps(expected_response))

        response = self.async_run_with_timeout(
            web_utils.api_request(
                path=CONSTANTS.TICKER_URL,
                api_factory=WebAssistantsFactory(),
                params={"symbol": "BTC_PERP"},
                method=RESTMethod.GET,
            )
        )

        self.assertEqual(response, expected_response)

    @aioresponses()
    def test_api_request_with_data(self, mock_api):
        """Test API request with POST data."""
        url = web_utils.private_rest_url(CONSTANTS.PLACE_ORDER_URL)
        regex_url = f"^{url}"

        order_data = {
            "symbol": "BTC_PERP",
            "side": "Buy",
            "orderType": "Limit",
            "quantity": "0.01",
            "price": "50000"
        }

        expected_response = {
            "orderId": "1234567890",
            "symbol": "BTC_PERP",
            "status": "NEW"
        }

        mock_api.post(regex_url, body=json.dumps(expected_response))

        response = self.async_run_with_timeout(
            web_utils.api_request(
                path=CONSTANTS.PLACE_ORDER_URL,
                api_factory=WebAssistantsFactory(),
                data=order_data,
                method=RESTMethod.POST,
            )
        )

        self.assertEqual(response, expected_response)

    @aioresponses()
    def test_api_request_with_auth(self, mock_api):
        """Test API request with authentication."""
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_INFO_URL)
        regex_url = f"^{url}"

        expected_response = {
            "balances": [
                {"asset": "USDC", "free": "10000.00", "locked": "0.00"}
            ]
        }

        mock_api.get(regex_url, body=json.dumps(expected_response))

        # Mock auth
        auth = AsyncMock()
        auth.rest_authenticate = AsyncMock(return_value=RESTRequest(
            method=RESTMethod.GET,
            url=url,
            headers={"X-API-Key": "test_key"},
            is_auth_required=True
        ))

        api_factory = WebAssistantsFactory(auth=auth)

        response = self.async_run_with_timeout(
            web_utils.api_request(
                path=CONSTANTS.ACCOUNT_INFO_URL,
                api_factory=api_factory,
                method=RESTMethod.GET,
                is_auth_required=True,
            )
        )

        self.assertEqual(response, expected_response)

    def test_get_current_server_time(self):
        """Test getting current server time."""
        # Since this is a utility function that returns current time in milliseconds
        server_time = web_utils.get_current_server_time()

        # Should be a positive integer representing milliseconds
        self.assertIsInstance(server_time, int)
        self.assertGreater(server_time, 0)

        # Should be a reasonable timestamp (after year 2020)
        self.assertGreater(server_time, 1577836800000)  # Jan 1, 2020 in milliseconds
