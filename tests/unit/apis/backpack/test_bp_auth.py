import base64
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

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
        with pytest.raises(ValueError, match="API key \\(Base64\\) cannot be empty"):
            BackpackEd25519Authenticator(
                api_key_b64="", private_key_b64=test_ed25519_keys["private_key_b64"]
            )

    def test_initialization_missing_private_key_raises_value_error(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        with pytest.raises(ValueError, match="Private key \\(Base64\\) cannot be empty"):
            BackpackEd25519Authenticator(
                api_key_b64=test_ed25519_keys["public_key_b64"], private_key_b64=""
            )

    def test_initialization_invalid_private_key_raises_value_error(
        self, test_ed25519_keys: dict[str, str]
    ) -> None:
        with pytest.raises(ValueError, match="Invalid ED25519 private key"):
            BackpackEd25519Authenticator(
                api_key_b64=test_ed25519_keys["public_key_b64"], private_key_b64="invalid_base64"
            )

    def test_initialization_missing_both_credentials_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="API key \\(Base64\\) cannot be empty"):
            BackpackEd25519Authenticator(api_key_b64="", private_key_b64="")

    def test_initialization_success(self, test_ed25519_keys: dict[str, str]) -> None:
        """Test that authenticator initializes correctly with valid ED25519 credentials."""
        auth = BackpackEd25519Authenticator(
            api_key_b64=test_ed25519_keys["public_key_b64"],
            private_key_b64=test_ed25519_keys["private_key_b64"],
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
            api_key_b64=test_ed25519_keys["public_key_b64"],
            private_key_b64=test_ed25519_keys["private_key_b64"],
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
            api_key_b64=test_ed25519_keys["public_key_b64"],
            private_key_b64=test_ed25519_keys["private_key_b64"],
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
            api_key_b64=test_ed25519_keys["public_key_b64"],
            private_key_b64=test_ed25519_keys["private_key_b64"],
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
            api_key_b64=test_ed25519_keys["public_key_b64"],
            private_key_b64=test_ed25519_keys["private_key_b64"],
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
            api_key_b64=test_ed25519_keys["public_key_b64"],
            private_key_b64=test_ed25519_keys["private_key_b64"],
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
            api_key_b64=test_ed25519_keys["public_key_b64"],
            private_key_b64=test_ed25519_keys["private_key_b64"],
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
            api_key_b64=test_ed25519_keys["public_key_b64"],
            private_key_b64=test_ed25519_keys["private_key_b64"],
        )

        # Verify key endpoints are mapped
        assert ("GET", "/api/v1/capital") in auth.INSTRUCTION_MAP
        assert ("POST", "/api/v1/order") in auth.INSTRUCTION_MAP
        assert ("DELETE", "/api/v1/order") in auth.INSTRUCTION_MAP

        # Verify instruction values are strings
        assert isinstance(auth.INSTRUCTION_MAP[("GET", "/api/v1/capital")], str)
