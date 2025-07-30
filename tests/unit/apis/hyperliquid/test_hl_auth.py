"""Unit tests for HyperliquidEip712Authenticator.

These tests complement the comprehensive tests in test_hl_auth_sign_l1_action.py
and focus on initialization, validation, and error handling.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest
from eth_account.signers.local import LocalAccount
from pydantic import AnyUrl, HttpUrl, SecretStr

from cyberdelta.apis.base.network_security_domain import ChainId, NetworkEnvironment
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions.authentication import InvalidPrivateKeyError
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.exceptions.base import RequiredParameterError
from cyberdelta.exceptions.field_validation import PassphraseFieldError


# Sample valid credentials
VALID_PRIVATE_KEY_HEX = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
VALID_WALLET_ADDRESS = "0x1Be31A94361a391bBaFB2a4CCd704F57dc04d4bb"  # Corrected derived address
VALID_CHAIN_ID = 1337


@pytest.fixture
def mock_account() -> MagicMock:
    """Fixture for a mocked eth_account.Account object.

    Returns:
        Mock LocalAccount with test wallet address.
    """
    account = MagicMock(spec=LocalAccount)
    account.address = VALID_WALLET_ADDRESS
    return account


@pytest.fixture
def test_network_environment() -> NetworkEnvironment:
    """Fixture for a test network environment.

    Returns:
        NetworkEnvironment configured for testnet.
    """
    return NetworkEnvironment(
        chain_id=ChainId.TESTNET,
        api_endpoint=HttpUrl("https://api.hyperliquid-testnet.xyz"),
        websocket_endpoint=AnyUrl("wss://api.hyperliquid-testnet.xyz/ws"),
    )


# --- Test Initialization ---


@patch("eth_account.Account.from_key")
def test_hl_auth_init_success_with_private_key(
    mock_from_key: MagicMock,
    mock_account: MagicMock,
    test_network_environment: NetworkEnvironment,
) -> None:
    """Test successful initialization with valid credentials."""
    mock_from_key.return_value = mock_account
    mock_account.address = VALID_WALLET_ADDRESS

    auth = HyperliquidEip712Authenticator(
        wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
        chain_id=VALID_CHAIN_ID,
        network_environment=test_network_environment,
    )
    # Note: now expects stripped version without 0x prefix for actual validation
    mock_from_key.assert_called_once_with(VALID_PRIVATE_KEY_HEX[2:])
    assert auth.wallet_address.lower() == VALID_WALLET_ADDRESS.lower()
    assert auth.chain_id == VALID_CHAIN_ID


def test_hl_auth_init_success_with_account_object(
    mock_account: MagicMock, test_network_environment: NetworkEnvironment
) -> None:
    """Test successful initialization with a pre-existing account object."""
    mock_account.address = VALID_WALLET_ADDRESS
    auth = HyperliquidEip712Authenticator(
        account_object=mock_account,
        chain_id=VALID_CHAIN_ID,
        network_environment=test_network_environment,
    )
    assert auth.wallet_address.lower() == VALID_WALLET_ADDRESS.lower()
    assert auth.chain_id == VALID_CHAIN_ID


def test_hl_auth_init_no_key_or_account(test_network_environment: NetworkEnvironment) -> None:
    """Test initialization with no private key or account object raises RequiredParameterError."""
    with pytest.raises(
        RequiredParameterError,
        match=r"'wallet_private_key_secret or account_object' parameter is required",
    ):
        HyperliquidEip712Authenticator(
            chain_id=VALID_CHAIN_ID, network_environment=test_network_environment
        )


def test_hl_auth_init_both_key_and_account(
    mock_account: MagicMock, test_network_environment: NetworkEnvironment
) -> None:
    """Test initialization with both private key and account object raises RequiredParameterError.

    Args:
        mock_account: Mocked account object.
        test_network_environment: Test network environment fixture.
    """
    with pytest.raises(
        RequiredParameterError,
        match=r"'wallet_private_key_secret or account_object' parameter is required",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
            account_object=mock_account,
            chain_id=VALID_CHAIN_ID,
            network_environment=test_network_environment,
        )


@patch("eth_account.Account.from_key", side_effect=ValueError("Simulated Key Error"))
def test_hl_auth_init_from_key_value_error(
    mock_from_key: MagicMock, test_network_environment: NetworkEnvironment
) -> None:
    """Test initialization raises InvalidPrivateKeyError if Account.from_key raises ValueError."""
    # Use a properly formatted hex key that will pass format validation but fail Account.from_key
    properly_formatted_but_bad_key = (
        "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    )
    with pytest.raises(
        InvalidPrivateKeyError,
        match=r"not cryptographically valid",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr(properly_formatted_but_bad_key),
            chain_id=VALID_CHAIN_ID,
            network_environment=test_network_environment,
        )


def test_hl_auth_init_invalid_hex_format(test_network_environment: NetworkEnvironment) -> None:
    """Test initialization raises InvalidPrivateKeyError for invalid hex format."""
    with pytest.raises(
        InvalidPrivateKeyError,
        match=r"must be a 64-character hex string",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr("invalid-hex-format"),
            chain_id=VALID_CHAIN_ID,
            network_environment=test_network_environment,
        )


def test_hl_auth_init_wrong_length_hex(test_network_environment: NetworkEnvironment) -> None:
    """Test initialization raises InvalidPrivateKeyError for wrong length hex string."""
    with pytest.raises(
        InvalidPrivateKeyError,
        match=r"must be a 64-character hex string",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr("0x1234"),  # Too short
            chain_id=VALID_CHAIN_ID,
            network_environment=test_network_environment,
        )


def test_hl_auth_init_invalid_passphrase_word_count(
    test_network_environment: NetworkEnvironment,
) -> None:
    """Test initialization raises PassphraseFieldError for invalid passphrase word count."""
    with pytest.raises(
        PassphraseFieldError,
        match=r"must consist of 12 or 24 words",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
            passphrase_secret=SecretStr("just five words here"),  # Only 4 words
            chain_id=VALID_CHAIN_ID,
            network_environment=test_network_environment,
        )


@patch("mnemonic.Mnemonic.check", return_value=False)
def test_hl_auth_init_invalid_bip39_passphrase(
    mock_mnemonic_check: MagicMock, test_network_environment: NetworkEnvironment
) -> None:
    """Test initialization raises PassphraseFieldError for invalid BIP-39 passphrase."""
    twelve_words = "word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12"
    with pytest.raises(
        PassphraseFieldError,
        match=r"not a valid BIP-39 mnemonic",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
            passphrase_secret=SecretStr(twelve_words),
            chain_id=VALID_CHAIN_ID,
            network_environment=test_network_environment,
        )


# --- Test prepare_request (focus on new authentication method) ---


@pytest.fixture
def authenticator_instance(
    test_network_environment: NetworkEnvironment,
) -> HyperliquidEip712Authenticator:
    """Fixture for a HyperliquidEip712Authenticator instance with a valid private key.

    Returns:
        HyperliquidEip712Authenticator configured with test credentials.
    """
    return HyperliquidEip712Authenticator(
        wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
        chain_id=VALID_CHAIN_ID,
        network_environment=test_network_environment,
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
    with pytest.raises((ValueError, TypeError), match="Must be a dictionary"):
        await authenticator_instance.prepare_request("POST", "/exchange", None, None, None)


@pytest.mark.asyncio
async def test_prepare_request_data_is_not_dict(
    authenticator_instance: HyperliquidEip712Authenticator,
) -> None:
    """Test prepare_request raises ValueError if data is not a dict."""
    with pytest.raises((ValueError, TypeError), match="Must be a dictionary"):
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
        test_network_environment: NetworkEnvironment,
    ) -> HyperliquidEip712Authenticator:
        """Create authenticator instance using a mocked account object.

        Returns:
            HyperliquidEip712Authenticator configured with mock account.
        """
        mock_account.address = VALID_WALLET_ADDRESS
        return HyperliquidEip712Authenticator(
            account_object=mock_account,
            chain_id=VALID_CHAIN_ID,
            logger_param=mock_logger,
            network_environment=test_network_environment,
        )

    def test_instantiation_with_account_object(
        self,
        mock_account: MagicMock,
        mock_logger: MagicMock,
        test_network_environment: NetworkEnvironment,
    ) -> None:
        """Test instantiation with a pre-configured account object."""
        mock_account.address = VALID_WALLET_ADDRESS
        auth = HyperliquidEip712Authenticator(
            account_object=mock_account,
            chain_id=VALID_CHAIN_ID,
            logger_param=mock_logger,
            network_environment=test_network_environment,
        )
        assert auth.wallet_address == VALID_WALLET_ADDRESS.lower()
        assert auth.chain_id == VALID_CHAIN_ID
        mock_logger.info.assert_any_call(
            "authenticator_initialized",
            wallet_address=VALID_WALLET_ADDRESS.lower(),
            chain_id=VALID_CHAIN_ID,
            message="HyperliquidEip712Authenticator initialized for address: %s on chain_id: %s",
            message_args=(VALID_WALLET_ADDRESS.lower(), VALID_CHAIN_ID),
        )

    def test_instantiation_no_key_or_account_object(
        self, mock_logger: MagicMock, test_network_environment: NetworkEnvironment
    ) -> None:
        """Test RequiredParameterError if neither private key nor account object is provided."""
        with pytest.raises(RequiredParameterError, match="parameter is required"):
            HyperliquidEip712Authenticator(
                chain_id=VALID_CHAIN_ID,
                logger_param=mock_logger,
                network_environment=test_network_environment,
            )

    def test_instantiation_both_key_and_account_object(
        self,
        mock_account: MagicMock,
        mock_logger: MagicMock,
        test_network_environment: NetworkEnvironment,
    ) -> None:
        """Test RequiredParameterError if both private key and account object are provided."""
        with pytest.raises(RequiredParameterError, match="only one allowed"):
            HyperliquidEip712Authenticator(
                wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY_HEX),
                account_object=mock_account,
                chain_id=VALID_CHAIN_ID,
                logger_param=mock_logger,
                network_environment=test_network_environment,
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
        assert "Failed to sign EIP-712 message" in str(excinfo.value.message)

        # The test fixture uses a mocked logger, so we should check that the mocked logger was
        # called with the expected structured logging call. The business logic uses .exception()
        assert mock_logger.exception.call_count >= 1

        # Check that at least one call was for signing failure
        signing_exception_calls = [
            call
            for call in mock_logger.exception.call_args_list
            if call[0][0] == "eip712_message_signing_failed"
        ]
        assert len(signing_exception_calls) == 1

        # Check the signing exception call
        call_args = signing_exception_calls[0]

        # Check that it was called with the structured logging format
        assert call_args[0][0] == "eip712_message_signing_failed"  # Event name
        assert call_args[1]["action"] == "encode_and_sign_message"
        assert "Crypto error" in call_args[1]["error_details"]
