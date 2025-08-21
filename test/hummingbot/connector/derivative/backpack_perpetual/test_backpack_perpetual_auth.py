import asyncio
import base64
import json
import unittest
from typing import Awaitable
from unittest.mock import MagicMock, patch

from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest


class BackpackPerpetualAuthUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.api_key = "TEST_API_KEY"
        # Create a fake Ed25519 private key bytes (32 bytes)
        fake_private_key = b'\x01' * 32
        cls.api_secret = base64.b64encode(fake_private_key).decode()

    def setUp(self) -> None:
        super().setUp()
        self.emulated_time = 1640001112223  # milliseconds
        self.time_provider = lambda: self.emulated_time

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth.ed25519')
    def test_auth_initialization(self, mock_ed25519):
        """Test authentication initialization."""
        # Mock the Ed25519 module
        mock_private_key = MagicMock()
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.api_secret,
            time_provider=self.time_provider
        )

        self.assertEqual(auth.api_key, self.api_key)
        self.assertEqual(auth.api_secret, self.api_secret)
        self.assertEqual(auth._time_provider(), self.emulated_time)

        # Verify private key was loaded
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.assert_called_once()

    def test_auth_initialization_invalid_secret(self):
        """Test authentication initialization with invalid secret."""
        with self.assertRaises(ValueError) as context:
            BackpackPerpetualAuth(
                api_key=self.api_key,
                api_secret="invalid_base64",  # Invalid base64
                time_provider=self.time_provider
            )
        self.assertIn("Invalid API secret format", str(context.exception))

    @patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth.ed25519')
    def test_build_signature_payload_get_request(self, mock_ed25519):
        """Test signature payload building for GET request."""
        # Mock the Ed25519 module
        mock_private_key = MagicMock()
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.api_secret,
            time_provider=self.time_provider
        )

        # Test GET request payload
        payload = auth._build_signature_payload(
            timestamp=str(self.emulated_time),
            method="GET",
            path="/api/v1/positions",
            params={"symbol": "BTC-PERP"}
        )

        expected = f"{self.emulated_time}GET/api/v1/positions?symbol=BTC-PERP"
        self.assertEqual(payload, expected)

    @patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth.ed25519')
    def test_build_signature_payload_post_request(self, mock_ed25519):
        """Test signature payload building for POST request."""
        # Mock the Ed25519 module
        mock_private_key = MagicMock()
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.api_secret,
            time_provider=self.time_provider
        )

        # Test POST request payload
        data = '{"symbol":"BTC-PERP","side":"Buy","quantity":"0.01"}'
        payload = auth._build_signature_payload(
            timestamp=str(self.emulated_time),
            method="POST",
            path="/api/v1/order",
            data=data
        )

        expected = f'{self.emulated_time}POST/api/v1/order{data}'
        self.assertEqual(payload, expected)

    @patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth.ed25519')
    def test_generate_signature(self, mock_ed25519):
        """Test signature generation."""
        # Mock the Ed25519 module and signature
        mock_private_key = MagicMock()
        mock_signature = b'mock_signature_bytes'
        mock_private_key.sign.return_value = mock_signature
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.api_secret,
            time_provider=self.time_provider
        )

        payload = "test_payload"
        signature = auth._generate_signature(payload)

        # Verify signature method was called with correct payload
        mock_private_key.sign.assert_called_once_with(payload.encode('utf-8'))

        # Verify signature is base64 encoded
        expected_signature = base64.b64encode(mock_signature).decode('utf-8')
        self.assertEqual(signature, expected_signature)

    @patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth.ed25519')
    def test_rest_authenticate_get_request(self, mock_ed25519):
        """Test REST authentication for GET request."""
        # Mock the Ed25519 module
        mock_private_key = MagicMock()
        mock_signature = b'mock_signature'
        mock_private_key.sign.return_value = mock_signature
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.api_secret,
            time_provider=self.time_provider
        )

        request = RESTRequest(
            method=RESTMethod.GET,
            url="/api/v1/positions",
            params={"symbol": "BTC-PERP"},
            is_auth_required=True,
        )

        signed_request = self.async_run_with_timeout(auth.rest_authenticate(request))

        self.assertIn("X-API-Key", signed_request.headers)
        self.assertEqual(signed_request.headers["X-API-Key"], self.api_key)
        self.assertIn("X-Timestamp", signed_request.headers)
        self.assertEqual(signed_request.headers["X-Timestamp"], str(self.emulated_time))
        self.assertIn("X-Signature", signed_request.headers)
        self.assertIn("X-Window", signed_request.headers)
        self.assertEqual(signed_request.headers["X-Window"], "5000")

    @patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth.ed25519')
    def test_rest_authenticate_post_request(self, mock_ed25519):
        """Test REST authentication for POST request."""
        # Mock the Ed25519 module
        mock_private_key = MagicMock()
        mock_signature = b'mock_signature'
        mock_private_key.sign.return_value = mock_signature
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.api_secret,
            time_provider=self.time_provider
        )

        data = json.dumps({"symbol": "BTC-PERP", "side": "Buy", "quantity": "0.01"})
        request = RESTRequest(
            method=RESTMethod.POST,
            url="/api/v1/order",
            data=data,
            is_auth_required=True,
        )

        signed_request = self.async_run_with_timeout(auth.rest_authenticate(request))

        self.assertIn("X-API-Key", signed_request.headers)
        self.assertEqual(signed_request.headers["X-API-Key"], self.api_key)
        self.assertIn("X-Timestamp", signed_request.headers)
        self.assertIn("X-Signature", signed_request.headers)
        self.assertIn("X-Window", signed_request.headers)

    @patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth.ed25519')
    def test_ws_authenticate(self, mock_ed25519):
        """Test WebSocket authentication."""
        # Mock the Ed25519 module
        mock_private_key = MagicMock()
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.api_secret,
            time_provider=self.time_provider
        )

        request = WSJSONRequest(
            payload={"TEST": "SOME_TEST_PAYLOAD"},
            throttler_limit_id="TEST_LIMIT_ID",
            is_auth_required=True
        )

        # For Backpack, WebSocket auth is done via message after connection
        signed_request = self.async_run_with_timeout(auth.ws_authenticate(request))

        # Should return the same request unchanged
        self.assertEqual(request, signed_request)

    @patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth.ed25519')
    def test_get_ws_auth_message(self, mock_ed25519):
        """Test WebSocket authentication message generation."""
        # Mock the Ed25519 module
        mock_private_key = MagicMock()
        mock_signature = b'mock_ws_signature'
        mock_private_key.sign.return_value = mock_signature
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.api_secret,
            time_provider=self.time_provider
        )

        ws_auth_message = auth.get_ws_auth_message()

        expected_message = {
            "method": "auth",
            "params": {
                "apiKey": self.api_key,
                "timestamp": str(self.emulated_time),
                "signature": base64.b64encode(mock_signature).decode('utf-8'),
                "window": "5000"
            }
        }

        self.assertEqual(ws_auth_message["method"], expected_message["method"])
        self.assertEqual(ws_auth_message["params"]["apiKey"], expected_message["params"]["apiKey"])
        self.assertEqual(ws_auth_message["params"]["timestamp"], expected_message["params"]["timestamp"])
        self.assertEqual(ws_auth_message["params"]["window"], expected_message["params"]["window"])

    def test_missing_ed25519_library(self):
        """Test behavior when Ed25519 library is not available."""
        with patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth.ed25519', None):
            with self.assertRaises(ImportError) as context:
                BackpackPerpetualAuth(
                    api_key=self.api_key,
                    api_secret=self.api_secret
                )

            self.assertIn("cryptography library is required", str(context.exception))
