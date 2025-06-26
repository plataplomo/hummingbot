"""Unit tests for HyperliquidEip712Authenticator.

These tests complement the comprehensive tests in test_hl_auth_sign_l1_action.py
and focus on initialization, validation, and error handling.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest
from eth_account.signers.local import LocalAccount
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


# Sample valid credentials
VALID_PRIVATE_KEY_HEX = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
VALID_WALLET_ADDRESS = "0x1Be31A94361a391bBaFB2a4CCd704F57dc04d4bb"  # Corrected derived address
VALID_CHAIN_ID = 1337


@pytest.fixture
def mock_account() -> MagicMock:
    """Fixture for a mocked eth_account.Account object."""
    account = MagicMock(spec=LocalAccount)
    account.address = VALID_WALLET_ADDRESS
    return account


# --- Test Initialization ---


@patch("eth_account.Account.from_key")
def test_hl_auth_init_success_with_private_key(
    mock_from_key: MagicMock,
    mock_account: MagicMock,
) -> None:
    """Test successful initialization with valid credentials."""
    mock_from_key.return_value = mock_account
    mock_account.address = VALID_WALLET_ADDRESS

    auth = HyperliquidEip712Authenticator(
        wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
        chain_id=VALID_CHAIN_ID,
    )
    # Note: now expects stripped version without 0x prefix for actual validation
    mock_from_key.assert_called_once_with(VALID_PRIVATE_KEY_HEX[2:])
    assert auth.wallet_address.lower() == VALID_WALLET_ADDRESS.lower()
    assert auth.chain_id == VALID_CHAIN_ID


def test_hl_auth_init_success_with_account_object(mock_account: MagicMock) -> None:
    """Test successful initialization with a pre-existing account object."""
    mock_account.address = VALID_WALLET_ADDRESS
    auth = HyperliquidEip712Authenticator(
        account_object=mock_account,
        chain_id=VALID_CHAIN_ID,
    )
    assert auth.wallet_address.lower() == VALID_WALLET_ADDRESS.lower()
    assert auth.chain_id == VALID_CHAIN_ID


def test_hl_auth_init_no_key_or_account() -> None:
    """Test initialization with no private key or account object raises ValueError."""
    with pytest.raises(
        ValueError,
        match="Either wallet_private_key_secret or account_object must be provided.",
    ):
        HyperliquidEip712Authenticator(chain_id=VALID_CHAIN_ID)


def test_hl_auth_init_both_key_and_account(mock_account: MagicMock) -> None:
    """Test initialization with both private key and account object raises ValueError."""
    with pytest.raises(
        ValueError,
        match="Provide either wallet_private_key_secret or account_object, not both.",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
            account_object=mock_account,
            chain_id=VALID_CHAIN_ID,
        )


@patch("eth_account.Account.from_key", side_effect=ValueError("Simulated Key Error"))
def test_hl_auth_init_from_key_value_error(mock_from_key: MagicMock) -> None:
    """Test initialization raises ValueError if Account.from_key raises ValueError."""
    # Use a properly formatted hex key that will pass format validation but fail Account.from_key
    properly_formatted_but_bad_key = (
        "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    )
    with pytest.raises(
        ValueError,
        match=(
            r"Invalid private key: Hyperliquid private_key is not cryptographically "
            r"valid: Simulated Key Error"
        ),
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr(properly_formatted_but_bad_key),
            chain_id=VALID_CHAIN_ID,
        )


def test_hl_auth_init_invalid_hex_format() -> None:
    """Test initialization raises ValueError for invalid hex format."""
    with pytest.raises(
        ValueError,
        match=r"Invalid private key: Hyperliquid private_key must be a 64-character hex string",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr("invalid-hex-format"),
            chain_id=VALID_CHAIN_ID,
        )


def test_hl_auth_init_wrong_length_hex() -> None:
    """Test initialization raises ValueError for wrong length hex string."""
    with pytest.raises(
        ValueError,
        match=r"Invalid private key: Hyperliquid private_key must be a 64-character hex string",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr("0x1234"),  # Too short
            chain_id=VALID_CHAIN_ID,
        )


def test_hl_auth_init_invalid_passphrase_word_count() -> None:
    """Test initialization raises ValueError for invalid passphrase word count."""
    with pytest.raises(
        ValueError,
        match=r"Invalid passphrase: Hyperliquid passphrase must consist of 12 or 24 words",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
            passphrase_secret=SecretStr("just five words here"),  # Only 4 words
            chain_id=VALID_CHAIN_ID,
        )


@patch("mnemonic.Mnemonic.check", return_value=False)
def test_hl_auth_init_invalid_bip39_passphrase(mock_mnemonic_check: MagicMock) -> None:
    """Test initialization raises ValueError for invalid BIP-39 passphrase."""
    twelve_words = "word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12"
    with pytest.raises(
        ValueError,
        match=r"Invalid passphrase: Hyperliquid passphrase is not a valid BIP-39 mnemonic",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
            passphrase_secret=SecretStr(twelve_words),
            chain_id=VALID_CHAIN_ID,
        )


# --- Test prepare_request (focus on new authentication method) ---


@pytest.fixture
def authenticator_instance() -> HyperliquidEip712Authenticator:
    """Fixture for a HyperliquidEip712Authenticator instance with a valid private key."""
    return HyperliquidEip712Authenticator(
        wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
        chain_id=VALID_CHAIN_ID,
    )


@pytest.mark.asyncio
async def test_prepare_request_non_exchange_endpoint_raises_not_implemented(
    authenticator_instance: HyperliquidEip712Authenticator,
) -> None:
    """Test that non-/exchange endpoints raise NotImplementedError."""
    with pytest.raises(NotImplementedError, match="Only /exchange endpoint is supported"):
        await authenticator_instance.prepare_request(
            method="POST",
            path="/info",
            params=None,
            data={"type": "meta"},
            headers=None,
        )


@pytest.mark.asyncio
async def test_prepare_request_data_is_none(
    authenticator_instance: HyperliquidEip712Authenticator,
) -> None:
    """Test prepare_request raises ValueError if data (action_payload) is None."""
    with pytest.raises(ValueError, match="Must be a dictionary"):
        await authenticator_instance.prepare_request("POST", "/exchange", None, None, None)


@pytest.mark.asyncio
async def test_prepare_request_data_is_not_dict(
    authenticator_instance: HyperliquidEip712Authenticator,
) -> None:
    """Test prepare_request raises ValueError if data is not a dict."""
    with pytest.raises(ValueError, match="Must be a dictionary"):
        # Cast to bypass type checking for this error handling test
        await authenticator_instance.prepare_request(
            "POST",
            "/exchange",
            None,
            "not a dict",  # type: ignore[arg-type]
            None,
        )


@pytest.mark.asyncio
async def test_prepare_request_uses_new_authentication_scheme(
    authenticator_instance: HyperliquidEip712Authenticator,
) -> None:
    """Test that prepare_request uses the new sign_l1_action scheme (no X-HL-* headers)."""
    result = await authenticator_instance.prepare_request(
        method="POST",
        path="/exchange",
        params=None,
        data={"type": "order", "coin": "BTC"},
        headers=None,
    )

    # Should NOT have X-HL-* headers (legacy authentication)
    assert "X-HL-Timestamp" not in result.headers
    assert "X-HL-Nonce" not in result.headers
    assert "X-HL-Signature" not in result.headers

    # Should have Content-Type header
    assert result.headers["Content-Type"] == "application/json"

    # Should have action, nonce, and signature in body (new authentication)
    assert result.data is not None
    assert "action" in result.data
    assert "nonce" in result.data
    assert "signature" in result.data

    # Signature should have r, s, v components
    sig = result.data["signature"]
    assert "r" in sig
    assert "s" in sig
    assert "v" in sig


# Class-based tests for better organization
class TestHyperliquidEip712Authenticator:
    """Test class for HyperliquidEip712Authenticator with better organization."""

    @pytest.fixture
    def mock_logger(self) -> MagicMock:
        """Return mock logger for testing."""
        return MagicMock(spec=logging.Logger)

    @pytest.fixture
    def auth_with_mock_account(
        self,
        mock_account: MagicMock,
        mock_logger: MagicMock,
    ) -> HyperliquidEip712Authenticator:
        """Create authenticator instance using a mocked account object."""
        mock_account.address = VALID_WALLET_ADDRESS
        return HyperliquidEip712Authenticator(
            account_object=mock_account,
            chain_id=VALID_CHAIN_ID,
            logger_param=mock_logger,
        )

    def test_instantiation_with_account_object(
        self,
        mock_account: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Test instantiation with a pre-configured account object."""
        mock_account.address = VALID_WALLET_ADDRESS
        auth = HyperliquidEip712Authenticator(
            account_object=mock_account,
            chain_id=VALID_CHAIN_ID,
            logger_param=mock_logger,
        )
        assert auth.wallet_address == VALID_WALLET_ADDRESS.lower()
        assert auth.chain_id == VALID_CHAIN_ID
        mock_logger.info.assert_any_call(
            f"HyperliquidEip712Authenticator initialized for address: "
            f"{VALID_WALLET_ADDRESS.lower()} on chain_id: {VALID_CHAIN_ID}",
        )

    def test_instantiation_no_key_or_account_object(self, mock_logger: MagicMock) -> None:
        """Test ValueError if neither private key nor account object is provided."""
        with pytest.raises(ValueError, match="must be provided"):
            HyperliquidEip712Authenticator(chain_id=VALID_CHAIN_ID, logger_param=mock_logger)

    def test_instantiation_both_key_and_account_object(
        self,
        mock_account: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Test ValueError if both private key and account object are provided."""
        with pytest.raises(ValueError, match="not both"):
            HyperliquidEip712Authenticator(
                wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
                account_object=mock_account,
                chain_id=VALID_CHAIN_ID,
                logger_param=mock_logger,
            )

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_auth.Account.recover_message")
    async def test_prepare_request_with_valid_action_payload(
        self,
        mock_recover: MagicMock,
        auth_with_mock_account: HyperliquidEip712Authenticator,
        mock_account: MagicMock,
    ) -> None:
        """Test prepare_request with a valid dictionary payload for 'data'."""
        # Set up the mock to return a proper signature structure
        from unittest.mock import MagicMock

        signed_msg_mock = MagicMock()
        signed_msg_mock.r = 12345
        signed_msg_mock.s = 67890
        signed_msg_mock.v = 27
        mock_account.sign_message.return_value = signed_msg_mock

        # Mock the recovery to return the expected address
        mock_recover.return_value = mock_account.address

        auth = auth_with_mock_account
        action_payload = {"type": "order", "orders": [{"coin": "BTC", "is_buy": True, "sz": "0.1"}]}
        result = await auth.prepare_request("POST", "/exchange", None, action_payload, None)

        # Verify the new authentication scheme is used
        assert result.data is not None
        assert "action" in result.data
        assert "signature" in result.data
        mock_account.sign_message.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_auth.Account.recover_message")
    async def test_signing_failure_raises_api_error(
        self,
        mock_recover: MagicMock,
        auth_with_mock_account: HyperliquidEip712Authenticator,
        mock_account: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Test APIError if account.sign_message raises an exception."""
        auth = auth_with_mock_account
        mock_account.sign_message.side_effect = Exception("Crypto error")

        with pytest.raises(APIError) as excinfo:
            await auth.prepare_request("POST", "/exchange", None, {"action": "fail"}, None)

        assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert "Failed to sign EIP-712 Agent request" in str(excinfo.value.message)

        # The test fixture uses a mocked logger, so we should check that the mocked logger was
        # called
        # with the expected structured logging call. There should be 2 calls (one from lower level,
        # one from higher level)
        assert mock_logger.error.call_count == 2

        # Check the second (higher level) call which is what this test originally checked
        call_args = mock_logger.error.call_args_list[1]  # Get the second call

        # Check that it was called with the structured logging format
        assert call_args[0][0] == "eip712_signing_failed"  # Event name
        assert call_args[1]["action"] == "sign_eip712_message"
        assert "Crypto error" in call_args[1]["error_details"]
        assert call_args[1]["exc_info"] is True
