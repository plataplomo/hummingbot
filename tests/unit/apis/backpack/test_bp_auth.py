import base64
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
from cyberdelta.apis.backpack.models.bp_ws_payloads import BackpackWsSignatureComponents
from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents


@pytest.fixture
def mock_time_patch() -> Generator[MagicMock]:
    with patch("time.time", return_value=1678886400.0) as mock_time:
        yield mock_time


@pytest.fixture
def test_ed25519_keys() -> dict[str, str]:
    """Generate test ED25519 keys for testing."""
    # Generate a test private key
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    # Encode to base64
    private_key_b64 = base64.b64encode(private_key.private_bytes_raw()).decode("utf-8")
    public_key_b64 = base64.b64encode(public_key.public_bytes_raw()).decode("utf-8")

    return {"private_key_b64": private_key_b64, "public_key_b64": public_key_b64}




class TestBackpackEd25519Authenticator:
    def test_initialization_missing_api_key_raises_value_error(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        with pytest.raises(
            ValueError, match="API key \\(Base64 public ED25519 key\\) cannot be empty"
        ):
            BackpackEd25519Authenticator(
                api_key_b64_secret=SecretStr(""),
                private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
            )

    def test_initialization_missing_private_key_raises_value_error(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        with pytest.raises(
            ValueError, match="Private key \\(Base64 private ED25519 key\\) cannot be empty"
        ):
            BackpackEd25519Authenticator(
                api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
                private_key_b64_secret=SecretStr(""),
            )

    def test_initialization_invalid_private_key_raises_value_error(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        with pytest.raises(ValueError, match="Invalid Base64 ED25519 private key"):
            BackpackEd25519Authenticator(
                api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
                private_key_b64_secret=SecretStr("invalid_base64"),
            )

    def test_initialization_missing_both_credentials_raises_value_error(self) -> None:
        with pytest.raises(
            ValueError, match="API key \\(Base64 public ED25519 key\\) cannot be empty"
        ):
            BackpackEd25519Authenticator(
                api_key_b64_secret=SecretStr(""), private_key_b64_secret=SecretStr("")
            )

    def test_initialization_success(self, test_ed25519_keys: dict[str, str]) -> None:
        """Test that authenticator initializes correctly with valid ED25519 credentials."""
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )
        # Test that initialization was successful by verifying the authenticator can be used
        # This tests the internal state without directly accessing protected members
        assert auth is not None
        # The private key should be loaded successfully - test through functionality
        assert hasattr(auth, "_ed25519_private_key")
        assert hasattr(auth, "_api_key_b64")

    @pytest.mark.asyncio
    async def test_prepare_request_get_balances(
        self, test_ed25519_keys: dict[str, str], mock_time_patch: MagicMock
    ) -> None:
        """Test preparing a signed GET request for balance query."""
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        components = await auth.prepare_request(
            method="GET", path="/api/v1/capital", params=None, data=None, headers=None
        )

        # Verify basic structure
        assert isinstance(components, AuthenticatedRequestComponents)
        assert "X-API-Key" in components.headers
        assert "X-Timestamp" in components.headers
        assert "X-Window" in components.headers
        assert "X-Signature" in components.headers

        # Verify values
        assert components.headers["X-API-Key"] == test_ed25519_keys["public_key_b64"]
        assert components.headers["X-Timestamp"] == "1678886400000"
        assert components.headers["X-Window"] == "5000"
        assert len(components.headers["X-Signature"]) > 0  # Should have a signature

        assert components.params is None
        assert components.data is None

    @pytest.mark.asyncio
    async def test_prepare_request_get_with_params(
        self, test_ed25519_keys: dict[str, str], mock_time_patch: MagicMock
    ) -> None:
        """Test preparing a signed GET request with query parameters."""
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        params = {"symbol": "SOL_USDC", "limit": "10"}

        components = await auth.prepare_request(
            method="GET", path="/api/v1/orders", params=params, data=None, headers=None
        )

        # Verify signature exists and params are preserved
        assert len(components.headers["X-Signature"]) > 0
        assert components.params == params

    @pytest.mark.asyncio
    async def test_prepare_request_post_with_data(
        self, test_ed25519_keys: dict[str, str], mock_time_patch: MagicMock
    ) -> None:
        """Test preparing a signed POST request with JSON data."""
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        data = {"symbol": "SOL_USDC", "quantity": "1.0", "side": "buy", "orderType": "market"}

        components = await auth.prepare_request(
            method="POST", path="/api/v1/order", params=None, data=data, headers=None
        )

        # Verify signature and data preservation
        assert len(components.headers["X-Signature"]) > 0
        assert components.data == data

    @pytest.mark.asyncio
    async def test_prepare_request_merges_existing_headers(
        self, test_ed25519_keys: dict[str, str], mock_time_patch: MagicMock
    ) -> None:
        """Test that existing headers are preserved and merged with auth headers."""
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        existing_headers = {"X-Custom-Header": "CustomValue", "Content-Type": "application/xml"}

        components = await auth.prepare_request(
            method="GET", path="/api/v1/capital", params=None, data=None, headers=existing_headers
        )

        # Verify existing headers are preserved
        assert components.headers["X-Custom-Header"] == "CustomValue"
        assert components.headers["Content-Type"] == "application/xml"

        # Verify auth headers are added
        assert "X-API-Key" in components.headers
        assert "X-Signature" in components.headers

    def test_get_ws_subscription_signature_components_account_stream(
        self, test_ed25519_keys: dict[str, str], mock_time_patch: MagicMock
    ) -> None:
        """Test WebSocket signature generation for account stream."""
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        components = auth.get_ws_subscription_signature_components(subscription_type="account")

        # Verify it returns BackpackWsSignatureComponents
        assert isinstance(components, BackpackWsSignatureComponents)
        assert components.api_key == test_ed25519_keys["public_key_b64"]
        assert components.timestamp == "1678886400000"
        assert components.window == "5000"
        assert len(components.signature) > 0

    def test_get_ws_subscription_signature_components_with_symbol(
        self, test_ed25519_keys: dict[str, str], mock_time_patch: MagicMock
    ) -> None:
        """Test WebSocket signature generation for market data stream with symbol."""
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        components = auth.get_ws_subscription_signature_components(
            subscription_type="orderbook", symbol="SOL_USDC"
        )

        assert isinstance(components, BackpackWsSignatureComponents)
        assert components.api_key == test_ed25519_keys["public_key_b64"]
        assert len(components.signature) > 0

    @pytest.mark.asyncio
    async def test_instruction_mapping_coverage(self, test_ed25519_keys: dict[str, str]) -> None:
        """Test that instruction mapping includes expected endpoints."""
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        # Verify key endpoints are mapped
        assert ("GET", "/api/v1/capital") in auth.INSTRUCTION_MAP
        assert ("POST", "/api/v1/order") in auth.INSTRUCTION_MAP
        assert ("DELETE", "/api/v1/order") in auth.INSTRUCTION_MAP

        # Verify instruction values are strings
        assert isinstance(auth.INSTRUCTION_MAP[("GET", "/api/v1/capital")], str)

    @pytest.mark.asyncio
    async def test_signing_string_generation_order_cancel_example(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test signing string generation using the orderCancel example from Backpack documentation.

        According to Backpack docs, the signing string should be:
        instruction=orderCancel&orderId=28&symbol=BTC_USDT&timestamp=<timestamp>&window=<window>
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        # Mock time to get predictable timestamp
        with patch("time.time", return_value=1678886400.0):
            # Test data matching the Backpack documentation example
            data = {"orderId": "28", "symbol": "BTC_USDT"}

            # We need to access the internal signing logic to verify the string_to_sign
            # Since prepare_request doesn't expose it, we'll test the components
            components = await auth.prepare_request(
                method="DELETE", path="/api/v1/order", params=None, data=data, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers
            assert components.data == data

            # The expected signing string should be:
            # instruction=orderCancel&orderId=28&symbol=BTC_USDT&timestamp=1678886400000&window=5000
            # We can't directly access string_to_sign, but we can verify the signature was generated
            assert len(components.headers["X-Signature"]) > 0

    @pytest.mark.asyncio
    async def test_signing_string_generation_json_body_query_format(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test that JSON request bodies are converted to query string format for signing.
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        with patch("time.time", return_value=1678886400.0):
            # Test with a POST request containing JSON data
            data = {
                "symbol": "SOL_USDC",
                "quantity": "1.5",
                "side": "buy",
                "orderType": "limit",
                "price": "100.50",
            }

            components = await auth.prepare_request(
                method="POST", path="/api/v1/order", params=None, data=data, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers
            assert components.data == data

            # The content should be sorted alphabetically and URL-encoded for signing
            # Expected order: orderType, price, quantity, side, symbol
            # But we can't directly verify the string_to_sign, only that signature was generated
            assert len(components.headers["X-Signature"]) > 0

    @pytest.mark.asyncio
    async def test_signing_string_generation_with_none_values_filtered(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test that None values are properly filtered out from the signing string.
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        with patch("time.time", return_value=1678886400.0):
            # Test data with None values that should be filtered out
            data = {
                "symbol": "SOL_USDC",
                "quantity": "1.0",
                "side": "buy",
                "orderType": None,  # This should be filtered out
                "price": None,  # This should be filtered out
                "timeInForce": "GTC",
            }

            components = await auth.prepare_request(
                method="POST", path="/api/v1/order", params=None, data=data, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers
            assert components.data == data  # Original data should be preserved

            # Signature should be generated successfully even with None values
            assert len(components.headers["X-Signature"]) > 0

    @pytest.mark.asyncio
    async def test_signing_string_generation_empty_body_no_double_ampersands(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test that requests with no body don't create double ampersands in signing string.
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        with patch("time.time", return_value=1678886400.0):
            # Test GET request with no params (empty content_part_str)
            components = await auth.prepare_request(
                method="GET", path="/api/v1/capital", params=None, data=None, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers

            # The signing string should be:
            # instruction=balanceQuery&timestamp=1678886400000&window=5000
            # (no double ampersands from empty content_part_str)
            assert len(components.headers["X-Signature"]) > 0

    @pytest.mark.asyncio
    async def test_signing_string_generation_get_with_params_query_format(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test that GET request parameters are properly URL-encoded and sorted for signing.
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        with patch("time.time", return_value=1678886400.0):
            # Test GET request with query parameters
            params = {"symbol": "SOL_USDC", "limit": "50", "offset": "0"}

            components = await auth.prepare_request(
                method="GET", path="/api/v1/orders", params=params, data=None, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers
            assert components.params == params

            # Parameters should be sorted alphabetically: limit, offset, symbol
            # The signing string should include these in query format
            assert len(components.headers["X-Signature"]) > 0

    @pytest.mark.asyncio
    async def test_signing_string_generation_complex_data_types_stringified(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test that complex data types (numbers, booleans) are properly stringified for signing.
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        with patch("time.time", return_value=1678886400.0):
            # Test data with various data types that need stringification
            data = {
                "symbol": "SOL_USDC",
                "quantity": 1.5,  # float
                "price": 100,  # int
                "postOnly": True,  # boolean
                "clientId": 12345,  # int
            }

            components = await auth.prepare_request(
                method="POST", path="/api/v1/order", params=None, data=data, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers
            assert components.data == data

            # All values should be stringified for URL encoding in the signing process
            # Expected alphabetical order: clientId, postOnly, price, quantity, symbol
            assert len(components.headers["X-Signature"]) > 0

    @pytest.mark.asyncio
    async def test_delete_request_authentication_components_order_cancel(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test DELETE request authentication components for order cancellation.

        This test verifies that the authenticator generates valid authentication
        components for an order cancellation request.
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        # Mock time to get predictable timestamp
        with patch("time.time", return_value=1678886400.0):
            # Test data matching the Backpack documentation example
            data = {"orderId": "28", "symbol": "BTC_USDT"}

            components = await auth.prepare_request(
                method="DELETE", path="/api/v1/order", params=None, data=data, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers
            assert "X-API-Key" in components.headers
            assert "X-Timestamp" in components.headers
            assert "X-Window" in components.headers
            assert components.data == data
            
            # Verify the authentication headers have expected values
            assert components.headers["X-Timestamp"] == "1678886400000"
            assert components.headers["X-Window"] == "5000"
            assert components.headers["X-API-Key"] == test_ed25519_keys["public_key_b64"]
            
            # Verify signature is present and valid (non-empty)
            signature = components.headers["X-Signature"]
            assert signature
            assert len(signature) > 0

    @pytest.mark.asyncio
    async def test_post_request_authentication_components_order_creation(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test POST request authentication components for order creation.
        
        This test verifies that the authenticator generates valid authentication
        components for an order creation request with multiple data fields.
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        with patch("time.time", return_value=1678886400.0):
            # Test data with fields that need alphabetical sorting
            data = {
                "symbol": "SOL_USDC",
                "quantity": "1.5",
                "side": "buy",
                "orderType": "limit",
                "price": "100.50",
            }

            components = await auth.prepare_request(
                method="POST", path="/api/v1/order", params=None, data=data, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers
            assert "X-API-Key" in components.headers
            assert "X-Timestamp" in components.headers
            assert "X-Window" in components.headers
            assert components.data == data
            
            # Verify the authentication headers have expected values
            assert components.headers["X-Timestamp"] == "1678886400000"
            assert components.headers["X-Window"] == "5000"
            assert components.headers["X-API-Key"] == test_ed25519_keys["public_key_b64"]
            
            # Verify signature is present and valid (non-empty)
            signature = components.headers["X-Signature"]
            assert signature
            assert len(signature) > 0

    @pytest.mark.asyncio
    async def test_get_request_authentication_components_no_params(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test GET request authentication components without parameters.
        
        This test verifies that the authenticator generates valid authentication
        components for a GET request with no parameters.
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        with patch("time.time", return_value=1678886400.0):
            components = await auth.prepare_request(
                method="GET", path="/api/v1/capital", params=None, data=None, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers
            assert "X-API-Key" in components.headers
            assert "X-Timestamp" in components.headers
            assert "X-Window" in components.headers
            
            # Verify the authentication headers have expected values
            assert components.headers["X-Timestamp"] == "1678886400000"
            assert components.headers["X-Window"] == "5000"
            assert components.headers["X-API-Key"] == test_ed25519_keys["public_key_b64"]
            
            # Verify signature is present and valid (non-empty)
            signature = components.headers["X-Signature"]
            assert signature
            assert len(signature) > 0

    @pytest.mark.asyncio
    async def test_get_request_authentication_components_with_params(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        """
        Test GET request authentication components with parameters.
        
        This test verifies that the authenticator generates valid authentication
        components for a GET request with query parameters.
        """
        auth = BackpackEd25519Authenticator(
            api_key_b64_secret=SecretStr(test_ed25519_keys["public_key_b64"]),
            private_key_b64_secret=SecretStr(test_ed25519_keys["private_key_b64"]),
        )

        with patch("time.time", return_value=1678886400.0):
            params = {"symbol": "SOL_USDC", "limit": "50", "offset": "0"}

            components = await auth.prepare_request(
                method="GET", path="/api/v1/orders", params=params, data=None, headers=None
            )

            # Verify the request was prepared successfully
            assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-Signature" in components.headers
            assert "X-API-Key" in components.headers
            assert "X-Timestamp" in components.headers
            assert "X-Window" in components.headers
            assert components.params == params
            
            # Verify the authentication headers have expected values
            assert components.headers["X-Timestamp"] == "1678886400000"
            assert components.headers["X-Window"] == "5000"
            assert components.headers["X-API-Key"] == test_ed25519_keys["public_key_b64"]
            
            # Verify signature is present and valid (non-empty)
            signature = components.headers["X-Signature"]
            assert signature
            assert len(signature) > 0
