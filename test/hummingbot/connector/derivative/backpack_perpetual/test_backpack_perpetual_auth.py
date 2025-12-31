import asyncio
import base64
import json
import unittest
from typing import Awaitable

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest


class BackpackPerpetualAuthUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.api_key = "TEST_API_KEY"
        cls.private_key = ed25519.Ed25519PrivateKey.generate()
        cls.private_key_b64 = base64.b64encode(cls.private_key.private_bytes_raw()).decode("utf-8")
        cls.secret_key = cls.private_key_b64

    def setUp(self) -> None:
        super().setUp()
        self.emulated_time = 1640001112.223
        self.test_params = {"price": "100", "quantity": "1", "symbol": "SOL_USDC_PERP"}
        self.auth = BackpackPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.secret_key,
            time_provider=self)

    def _get_expected_signature(self, payload: str):
        signature = self.private_key.sign(payload.encode("utf-8"))
        return base64.b64encode(signature).decode("utf-8")

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def time(self):
        # Implemented to emulate a TimeSynchronizer
        return self.emulated_time

    def test_generate_signature_from_payload(self):
        payload = self._get_test_payload()
        signature = self.auth._generate_signature(payload)

        self.assertEqual(signature, self._get_expected_signature(payload))

    def _get_test_payload(self) -> str:
        expected_timestamp = str(int(self.emulated_time * 1e3))
        return (
            f"instruction=orderExecute&price=100&quantity=1&symbol=SOL_USDC_PERP&timestamp={expected_timestamp}"
            f"&window={CONSTANTS.AUTH_WINDOW_MS}"
        )

    def test_rest_authenticate_parameters_provided(self):
        params = {"orderId": "1", "symbol": "SOL_USDC_PERP"}
        request: RESTRequest = RESTRequest(
            method=RESTMethod.GET,
            url=f"https://api.backpack.exchange/{CONSTANTS.ORDER_URL}",
            params=params,
            is_auth_required=True,
        )

        signed_request: RESTRequest = self.async_run_with_timeout(self.auth.rest_authenticate(request))

        expected_timestamp = str(int(self.emulated_time * 1e3))
        expected_payload = (
            f"instruction=orderQuery&orderId=1&symbol=SOL_USDC_PERP&timestamp={expected_timestamp}"
            f"&window={CONSTANTS.AUTH_WINDOW_MS}"
        )

        self.assertIn("X-API-Key", signed_request.headers)
        self.assertEqual(signed_request.headers["X-API-Key"], self.api_key)
        self.assertEqual(signed_request.headers["X-Timestamp"], expected_timestamp)
        self.assertEqual(signed_request.headers["X-Window"], str(CONSTANTS.AUTH_WINDOW_MS))
        self.assertEqual(signed_request.headers["X-Signature"], self._get_expected_signature(expected_payload))

    def test_rest_authenticate_data_provided(self):
        request: RESTRequest = RESTRequest(
            method=RESTMethod.POST,
            url=f"https://api.backpack.exchange/{CONSTANTS.ORDER_URL}",
            data=json.dumps(self.test_params),
            is_auth_required=True,
        )

        signed_request: RESTRequest = self.async_run_with_timeout(self.auth.rest_authenticate(request))

        expected_timestamp = str(int(self.emulated_time * 1e3))
        expected_payload = (
            f"instruction=orderExecute&price=100&quantity=1&symbol=SOL_USDC_PERP&timestamp={expected_timestamp}"
            f"&window={CONSTANTS.AUTH_WINDOW_MS}"
        )

        self.assertIn("X-API-Key", signed_request.headers)
        self.assertEqual(signed_request.headers["X-API-Key"], self.api_key)
        self.assertEqual(signed_request.headers["X-Timestamp"], expected_timestamp)
        self.assertEqual(signed_request.headers["X-Window"], str(CONSTANTS.AUTH_WINDOW_MS))
        self.assertEqual(signed_request.headers["X-Signature"], self._get_expected_signature(expected_payload))

    def test_ws_authenticate(self):
        request: WSJSONRequest = WSJSONRequest(
            payload={"TEST": "SOME_TEST_PAYLOAD"}, throttler_limit_id="TEST_LIMIT_ID", is_auth_required=True
        )

        signed_request: WSJSONRequest = self.async_run_with_timeout(self.auth.ws_authenticate(request))

        self.assertEqual(request, signed_request)
