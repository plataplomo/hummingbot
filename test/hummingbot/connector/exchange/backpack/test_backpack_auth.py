import asyncio
import base64
from copy import copy
from unittest import TestCase
from unittest.mock import MagicMock

from cryptography.hazmat.primitives.asymmetric import ed25519
from typing_extensions import Awaitable

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class BackpackAuthTests(TestCase):

    def setUp(self) -> None:
        self._api_key = "testApiKey"
        private_key_bytes = bytes(range(32))
        self._private_key_bytes = private_key_bytes
        self._secret = base64.b64encode(private_key_bytes).decode("utf-8")

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_rest_authenticate(self):
        now = 1234567890.000
        mock_time_provider = MagicMock()
        mock_time_provider.time.return_value = now

        params = {
            "symbol": "BTC_USDC",
            "orderId": "123",
        }
        full_params = copy(params)

        auth = BackpackAuth(api_key=self._api_key, api_secret=self._secret, time_provider=mock_time_provider)
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.backpack.exchange/api/v1/order",
            params=params,
            is_auth_required=True,
        )
        configured_request = self.async_run_with_timeout(auth.rest_authenticate(request))

        timestamp = str(int(now * 1e3))
        full_params.update({"timestamp": timestamp, "window": str(CONSTANTS.AUTH_WINDOW_MS)})
        instruction = "orderQuery"
        payload_parts = [f"instruction={instruction}"]
        for key, value in sorted(params.items()):
            payload_parts.append(f"{key}={value}")
        payload_parts.append(f"timestamp={timestamp}")
        payload_parts.append(f"window={CONSTANTS.AUTH_WINDOW_MS}")
        payload = "&".join(payload_parts)

        expected_signature = base64.b64encode(
            ed25519.Ed25519PrivateKey.from_private_bytes(self._private_key_bytes).sign(payload.encode("utf-8"))
        ).decode("utf-8")

        self.assertEqual({"X-API-Key", "X-Timestamp", "X-Signature", "X-Window"}, set(configured_request.headers))
        self.assertEqual(self._api_key, configured_request.headers["X-API-Key"])
        self.assertEqual(timestamp, configured_request.headers["X-Timestamp"])
        self.assertEqual(expected_signature, configured_request.headers["X-Signature"])
        self.assertEqual(str(CONSTANTS.AUTH_WINDOW_MS), configured_request.headers["X-Window"])
