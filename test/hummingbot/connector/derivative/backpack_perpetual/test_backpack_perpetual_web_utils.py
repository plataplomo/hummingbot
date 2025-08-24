import asyncio
import time
from typing import Awaitable
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_web_utils as web_utils
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class BackpackPerpetualWebUtilsTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()

    def setUp(self) -> None:
        super().setUp()
        # Create a new event loop for each test to avoid closed loop issues
        self.async_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.async_loop)

    def tearDown(self) -> None:
        # Close the loop properly
        try:
            self.async_loop.stop()
            self.async_loop.close()
        except Exception:
            pass
        super().tearDown()

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        return self.async_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))

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

        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        self.assertEqual(url, f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_URL}")

    def test_build_api_factory(self):
        """Test API factory creation."""
        # Create throttler with proper rate limits
        rate_limits = [
            RateLimit(limit_id="default", limit=100, time_interval=60),
        ]
        throttler = AsyncThrottler(rate_limits=rate_limits)
        api_factory = web_utils.build_api_factory(throttler=throttler)

        self.assertIsInstance(api_factory, WebAssistantsFactory)
        self.assertIs(api_factory._throttler, throttler)

    def test_build_api_factory_with_auth(self):
        """Test API factory creation with authentication."""
        # Create throttler with proper rate limits
        rate_limits = [
            RateLimit(limit_id="default", limit=100, time_interval=60),
        ]
        throttler = AsyncThrottler(rate_limits=rate_limits)
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

    @patch("hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_web_utils.WebAssistantsFactory")
    def test_api_request_success(self, mock_factory_class):
        """Test successful API request."""
        expected_response = {
            "symbol": "BTC_PERP",
            "lastPrice": "50000.00",
            "markPrice": "50001.00",
            "fundingRate": "0.0001",
        }
        
        # Create mock factory and assistant
        mock_factory = AsyncMock()
        mock_factory_class.return_value = mock_factory
        
        mock_assistant = AsyncMock()
        mock_factory.get_rest_assistant = AsyncMock(return_value=mock_assistant)
        
        # Make execute_request return a dict directly for testing
        mock_assistant.execute_request = AsyncMock(return_value=expected_response)
        
        # Create throttler with proper rate limits
        throttler = web_utils.create_throttler()
        api_factory = mock_factory_class(throttler=throttler)
        
        response = self.async_run_with_timeout(
            web_utils.api_request(
                path=CONSTANTS.TICKER_URL,
                api_factory=api_factory,
                params={"symbol": "BTC_PERP"},
                method=RESTMethod.GET,
            )
        )
        
        self.assertEqual(response, expected_response)

    @patch("hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_web_utils.WebAssistantsFactory")
    def test_api_request_with_data(self, mock_factory_class):
        """Test API request with POST data."""
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
        
        # Create mock factory and assistant
        mock_factory = AsyncMock()
        mock_factory_class.return_value = mock_factory
        
        mock_assistant = AsyncMock()
        mock_factory.get_rest_assistant = AsyncMock(return_value=mock_assistant)
        
        # Make execute_request return a dict directly for testing
        mock_assistant.execute_request = AsyncMock(return_value=expected_response)
        
        # Create throttler with proper rate limits
        throttler = web_utils.create_throttler()
        api_factory = mock_factory_class(throttler=throttler)
        
        response = self.async_run_with_timeout(
            web_utils.api_request(
                path=CONSTANTS.ORDER_URL,
                api_factory=api_factory,
                data=order_data,
                method=RESTMethod.POST,
            )
        )
        
        self.assertEqual(response, expected_response)

    @patch("hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_web_utils.WebAssistantsFactory")
    def test_api_request_with_auth(self, mock_factory_class):
        """Test API request with authentication."""
        expected_response = {
            "balances": [
                {"asset": "USDC", "free": "10000.00", "locked": "0.00"}
            ]
        }
        
        # Mock auth
        auth = AsyncMock()
        auth.rest_authenticate = AsyncMock(return_value=RESTRequest(
            method=RESTMethod.GET,
            url=web_utils.private_rest_url(CONSTANTS.ACCOUNT_URL),
            headers={"X-API-Key": "test_key"},
            is_auth_required=True
        ))
        
        # Create mock factory and assistant
        mock_factory = AsyncMock()
        mock_factory_class.return_value = mock_factory
        
        mock_assistant = AsyncMock()
        mock_factory.get_rest_assistant = AsyncMock(return_value=mock_assistant)
        
        # Make execute_request return a dict directly for testing
        mock_assistant.execute_request = AsyncMock(return_value=expected_response)
        
        # Create throttler with proper rate limits
        throttler = web_utils.create_throttler()
        api_factory = mock_factory_class(throttler=throttler, auth=auth)
        
        response = self.async_run_with_timeout(
            web_utils.api_request(
                path=CONSTANTS.ACCOUNT_URL,
                api_factory=api_factory,
                method=RESTMethod.GET,
                is_auth_required=True,
            )
        )
        
        self.assertEqual(response, expected_response)

    @patch("hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_web_utils.build_api_factory_without_time_synchronizer_pre_processor")
    def test_get_current_server_time(self, mock_build_factory):
        """Test getting current server time."""
        # Mock response with server time in milliseconds
        current_time_ms = int(time.time() * 1000)
        expected_response = {"serverTime": current_time_ms}
        
        # Create mock factory and assistant
        mock_factory = AsyncMock()
        mock_build_factory.return_value = mock_factory
        
        mock_assistant = AsyncMock()
        mock_factory.get_rest_assistant = AsyncMock(return_value=mock_assistant)
        
        # Make execute_request return a dict directly for testing
        mock_assistant.execute_request = AsyncMock(return_value=expected_response)
        
        server_time = self.async_run_with_timeout(
            web_utils.get_current_server_time()
        )
        
        # Should be a positive number representing timestamp in seconds
        self.assertIsInstance(server_time, (int, float))
        self.assertGreater(server_time, 0)
        
        # Should be close to current time
        self.assertAlmostEqual(server_time, current_time_ms / 1000.0, delta=1)
