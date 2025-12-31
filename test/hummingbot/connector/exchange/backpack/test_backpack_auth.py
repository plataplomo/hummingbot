"""
Unit tests for Backpack authentication module.
"""

import base64
import unittest
from unittest.mock import MagicMock, patch

from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth


class TestBackpackAuth(unittest.TestCase):
    """Test cases for Backpack authentication."""

    def setUp(self):
        """Set up test fixtures."""
        # Generate a test Ed25519 private key for testing
        # This is a mock key - not a real private key
        self.test_api_key = "test_api_key"

        # Create a fake Ed25519 private key bytes (32 bytes)
        # In real usage, this would be a valid Ed25519 private key
        fake_private_key = b'\x01' * 32
        self.test_api_secret = base64.b64encode(fake_private_key).decode()

        # Mock time provider for consistent testing
        self.mock_timestamp = 1640995200.0  # Fixed timestamp in seconds
        self.mock_timestamp_ms = int(self.mock_timestamp * 1e3)
        self.time_provider = MagicMock()
        self.time_provider.time.return_value = self.mock_timestamp

    @patch('hummingbot.connector.exchange.backpack.backpack_auth.ed25519')
    def test_auth_initialization(self, mock_ed25519):
        """Test authentication initialization."""
        # Mock the Ed25519 module
        mock_private_key = unittest.mock.MagicMock()
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackAuth(
            api_key=self.test_api_key,
            api_secret=self.test_api_secret,
            time_provider=self.time_provider
        )

        self.assertEqual(auth.api_key, self.test_api_key)
        self.assertEqual(auth.api_secret, self.test_api_secret)
        self.assertEqual(auth._get_timestamp(), self.mock_timestamp_ms)

        # Verify private key was loaded
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.assert_called_once()

    def test_auth_initialization_invalid_secret(self):
        """Test authentication initialization with invalid secret."""
        with self.assertRaises(ValueError):
            BackpackAuth(
                api_key=self.test_api_key,
                api_secret="invalid_base64",  # Invalid base64
                time_provider=self.time_provider
            )

    @patch('hummingbot.connector.exchange.backpack.backpack_auth.ed25519')
    def test_build_signature_payload(self, mock_ed25519):
        """Test signature payload building."""
        # Mock the Ed25519 module
        mock_private_key = unittest.mock.MagicMock()
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackAuth(
            api_key=self.test_api_key,
            api_secret=self.test_api_secret,
            time_provider=self.time_provider
        )

        # Test GET request payload
        payload = auth._build_signature_payload(
            timestamp="1640995200000",
            method="GET",
            path="/api/v1/balances",
            params={"symbol": "BTC_USDC"}
        )

        expected = "1640995200000GET/api/v1/balances?symbol=BTC_USDC"
        self.assertEqual(payload, expected)

        # Test POST request payload
        payload = auth._build_signature_payload(
            timestamp="1640995200000",
            method="POST",
            path="/api/v1/order",
            data='{"symbol":"BTC_USDC","side":"Buy"}'
        )

        expected = '1640995200000POST/api/v1/order{"symbol":"BTC_USDC","side":"Buy"}'
        self.assertEqual(payload, expected)

    @patch('hummingbot.connector.exchange.backpack.backpack_auth.ed25519')
    def test_generate_signature(self, mock_ed25519):
        """Test signature generation."""
        # Mock the Ed25519 module and signature
        mock_private_key = unittest.mock.MagicMock()
        mock_signature = b'mock_signature_bytes'
        mock_private_key.sign.return_value = mock_signature
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackAuth(
            api_key=self.test_api_key,
            api_secret=self.test_api_secret,
            time_provider=self.time_provider
        )

        payload = "test_payload"
        signature = auth._generate_signature(payload)

        # Verify signature method was called with correct payload
        mock_private_key.sign.assert_called_once_with(payload.encode('utf-8'))

        # Verify signature is base64 encoded
        expected_signature = base64.b64encode(mock_signature).decode('utf-8')
        self.assertEqual(signature, expected_signature)

    @patch('hummingbot.connector.exchange.backpack.backpack_auth.ed25519')
    def test_generate_auth_headers(self, mock_ed25519):
        """Test authentication headers generation."""
        # Mock the Ed25519 module
        mock_private_key = unittest.mock.MagicMock()
        mock_signature = b'mock_signature'
        mock_private_key.sign.return_value = mock_signature
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackAuth(
            api_key=self.test_api_key,
            api_secret=self.test_api_secret,
            time_provider=self.time_provider
        )

        headers = auth._generate_auth_headers(
            method="GET",
            path="/api/v1/balances"
        )

        expected_headers = {
            "X-API-Key": self.test_api_key,
            "X-Timestamp": str(self.mock_timestamp_ms),
            "X-Signature": base64.b64encode(mock_signature).decode('utf-8'),
            "X-Window": "5000",
        }

        self.assertEqual(headers, expected_headers)

    @patch('hummingbot.connector.exchange.backpack.backpack_auth.ed25519')
    def test_get_ws_auth_message(self, mock_ed25519):
        """Test WebSocket authentication message generation."""
        # Mock the Ed25519 module
        mock_private_key = unittest.mock.MagicMock()
        mock_signature = b'mock_ws_signature'
        mock_private_key.sign.return_value = mock_signature
        mock_ed25519.Ed25519PrivateKey.from_private_bytes.return_value = mock_private_key

        auth = BackpackAuth(
            api_key=self.test_api_key,
            api_secret=self.test_api_secret,
            time_provider=self.time_provider
        )

        ws_auth_message = auth.get_ws_auth_message()

        expected_message = {
            "method": "authenticate",
            "params": {
                "apiKey": self.test_api_key,
                "timestamp": str(self.mock_timestamp_ms),
                "signature": base64.b64encode(mock_signature).decode('utf-8'),
                "window": 5000
            }
        }

        self.assertEqual(ws_auth_message, expected_message)


if __name__ == "__main__":
    unittest.main()
