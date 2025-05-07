"""
Unit tests for HyperliquidEip712Authenticator.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from eth_account.datastructures import SignedMessage
from hexbytes import HexBytes
from web3 import Web3

from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Sample valid credentials
VALID_PRIVATE_KEY_HEX = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
VALID_WALLET_ADDRESS = "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"  # Derived from key above
VALID_CHAIN_ID = 1337

# Sample invalid credentials
INVALID_PRIVATE_KEY_HEX = "0xinvalidkey"
MISMATCHED_WALLET_ADDRESS = "0xDeadBeefDeadBeefDeadBeefDeadBeefDeadBeef"


@pytest.fixture
def mock_account() -> MagicMock:
    """Fixture for a mocked eth_account.Account object."""
    account = MagicMock()
    account.address = "0xWalletAddress"
    # Mock the sign_message method to return a MagicMock(SignedMessage)
    signed_msg_mock = MagicMock(spec=SignedMessage)
    signed_msg_mock.signature = HexBytes("0x" + "s" * 130)  # Placeholder signature 65 bytes
    account.sign_message.return_value = signed_msg_mock
    return account


@pytest.fixture
def authenticator_instance(mock_account: MagicMock) -> HyperliquidEip712Authenticator:
    """Fixture for a HyperliquidEip712Authenticator instance with mocked Account."""
    with patch("eth_account.Account.from_key", return_value=mock_account) as mock_from_key:
        auth = HyperliquidEip712Authenticator(
            private_key_hex="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            wallet_address="0xWalletAddress",
            chain_id=1337,
        )
        mock_from_key.assert_called_once_with(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        return auth


# --- Test Initialization ---


def test_hl_auth_init_success(mock_account: MagicMock) -> None:
    """Test successful initialization with valid credentials."""
    auth = HyperliquidEip712Authenticator(
        private_key_hex=VALID_PRIVATE_KEY_HEX,
        wallet_address=VALID_WALLET_ADDRESS,
        chain_id=VALID_CHAIN_ID,
    )
    assert auth._wallet_address == VALID_WALLET_ADDRESS.lower()  # noqa: SLF001
    assert auth._chain_id == VALID_CHAIN_ID  # noqa: SLF001
    assert auth._account is not None  # noqa: SLF001
    assert auth._account.address == VALID_WALLET_ADDRESS  # noqa: SLF001


def test_hl_auth_init_success_no_0x(mock_account: MagicMock) -> None:
    """Test successful initialization with valid key without '0x' prefix."""
    auth = HyperliquidEip712Authenticator(
        private_key_hex=VALID_PRIVATE_KEY_HEX[2:],
        wallet_address=VALID_WALLET_ADDRESS,
        chain_id=VALID_CHAIN_ID,
    )
    assert auth._account is not None  # noqa: SLF001


def test_hl_auth_init_no_private_key(mock_account: MagicMock) -> None:
    """Test initialization with no private key raises ValueError."""
    with pytest.raises(ValueError, match="Private key cannot be empty"):
        HyperliquidEip712Authenticator(
            private_key_hex="",
            wallet_address=VALID_WALLET_ADDRESS,
            chain_id=VALID_CHAIN_ID,
        )


def test_hl_auth_init_invalid_private_key(mock_account: MagicMock) -> None:
    """Test initialization raises ValueError for an invalid private key."""
    mock_account.sign_message.side_effect = ValueError("Invalid private key")
    with pytest.raises(ValueError, match="Invalid private key format or value"):
        HyperliquidEip712Authenticator(
            private_key_hex=INVALID_PRIVATE_KEY_HEX,
            wallet_address=VALID_WALLET_ADDRESS,
            chain_id=VALID_CHAIN_ID,
        )


def test_hl_auth_init_address_mismatch(
    mock_account: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Test initialization logs error if derived address mismatches provided address."""
    # No exception is raised by default, just logged error
    HyperliquidEip712Authenticator(
        private_key_hex=VALID_PRIVATE_KEY_HEX,
        wallet_address=MISMATCHED_WALLET_ADDRESS,
        chain_id=VALID_CHAIN_ID,
    )
    assert "CRITICAL: Provided wallet address" in caplog.text
    assert MISMATCHED_WALLET_ADDRESS in caplog.text
    assert VALID_WALLET_ADDRESS.lower() in caplog.text


# --- Test prepare_request ---


@pytest.mark.asyncio
async def test_prepare_request_success(
    mock_account: MagicMock,
) -> None:
    """Test successful preparation and signing of a request."""
    auth = HyperliquidEip712Authenticator(
        private_key_hex=VALID_PRIVATE_KEY_HEX,
        wallet_address=VALID_WALLET_ADDRESS,
        chain_id=VALID_CHAIN_ID,
    )

    method = "POST"
    path = "/exchange"
    params = {"query": "value"}
    data = {"action": "place_order", "details": {"coin": "BTC", "size": "1"}}
    headers = {"X-Custom-Header": "custom"}

    # Use a fixed time for predictable timestamp/nonce
    fixed_time_sec = 1678886400.123
    with patch("time.time", return_value=fixed_time_sec):
        result: AuthenticatedRequestComponents = await auth.prepare_request(
            method, path, params, data, headers
        )

    # Verify mocks were called correctly
    assert "X-HL-Signature" in result["headers"]
    assert result["headers"]["X-HL-Signature"].startswith("0x")
    assert len(result["headers"]["X-HL-Signature"]) == 132  # 0x + 65 bytes hex
    assert result["headers"]["X-HL-Timestamp"] == str(int(fixed_time_sec * 1000))
    assert result["headers"]["X-HL-Nonce"] == str(int(fixed_time_sec * 1000))  # Nonce is timestamp
    assert result["headers"]["X-Custom-Header"] == "custom"
    assert result["params"] == params  # Params should be unchanged
    assert result["data"] == data  # Data should be unchanged


@pytest.mark.asyncio
async def test_prepare_request_no_private_key(
    mock_account: MagicMock,
) -> None:
    """Test prepare_request raises APIError if authenticator has no private key (was init with empty string)."""
    # This test assumes that if private_key_hex was empty, _account would be None
    # and prepare_request would fail early. The __init__ now raises ValueError directly.
    # Thus, this specific test path for prepare_request is less relevant if __init__ already failed.
    # However, if an authenticator was somehow created with _account=None (e.g. bypassed init checks):
    with patch("eth_account.Account.from_key", return_value=mock_account):
        auth = HyperliquidEip712Authenticator(
            private_key_hex="0xPrivateKey",  # Needs a valid-looking key for init to pass this stage
            wallet_address=VALID_WALLET_ADDRESS,
            chain_id=VALID_CHAIN_ID,
        )
    auth._account = None  # Manually set _account to None to simulate this scenario # noqa: SLF001

    with pytest.raises(APIError, match="not have a usable private key") as excinfo:
        await auth.prepare_request("POST", "/exchange", None, {"action": "test"}, None)
    assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value


@pytest.mark.asyncio
async def test_prepare_request_no_data(
    mock_account: MagicMock,
) -> None:
    """Test prepare_request raises APIError if data is None."""
    auth = HyperliquidEip712Authenticator(
        private_key_hex=VALID_PRIVATE_KEY_HEX,
        wallet_address=VALID_WALLET_ADDRESS,
        chain_id=VALID_CHAIN_ID,
    )
    with pytest.raises(APIError, match="Action payload .* required") as excinfo:
        await auth.prepare_request("POST", "/exchange", None, None, None)
    assert excinfo.value.code == APIErrorCode.INVALID_PARAMS.value


@pytest.mark.asyncio
async def test_prepare_request_signing_error(
    mock_account: MagicMock,
) -> None:
    """Test prepare_request raises APIError if signing fails."""
    auth = HyperliquidEip712Authenticator(
        private_key_hex=VALID_PRIVATE_KEY_HEX,
        wallet_address=VALID_WALLET_ADDRESS,
        chain_id=VALID_CHAIN_ID,
    )
    mock_account.sign_message.side_effect = Exception("Signing failed")

    with pytest.raises(APIError, match="Failed to sign EIP-712 message") as excinfo:
        await auth.prepare_request("POST", "/exchange", None, {"action": "test"}, None)
    assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
    assert isinstance(excinfo.value.original_exception, Exception)
    assert str(excinfo.value.original_exception) == "Signing failed"


class TestHyperliquidEip712Authenticator:
    VALID_PRIVATE_KEY = "0x" + "a" * 64
    VALID_WALLET_ADDRESS = "0xDeaDBEEFdeaDbEeFDeAdBeEfdeAdCarToOndeaDbEEF"
    MOCKED_ACCOUNT_WALLET_ADDRESS = "0xWalletAddress"  # This is what mock_account.address returns

    def test_instantiation_success(self, mock_account: MagicMock) -> None:
        """Test successful instantiation of the authenticator."""
        with patch("eth_account.Account.from_key", return_value=mock_account) as mock_from_key:
            auth = HyperliquidEip712Authenticator(
                private_key_hex=self.VALID_PRIVATE_KEY,
                wallet_address=self.MOCKED_ACCOUNT_WALLET_ADDRESS,  # Use derived address for this test
                chain_id=1337,
            )
            mock_from_key.assert_called_once_with(self.VALID_PRIVATE_KEY)
            assert auth._account == mock_account  # noqa: SLF001
            assert auth._wallet_address == self.MOCKED_ACCOUNT_WALLET_ADDRESS  # noqa: SLF001
            assert auth._chain_id == 1337  # noqa: SLF001
            assert auth._domain_data["chainId"] == 1337  # noqa: SLF001

    def test_instantiation_private_key_no_prefix(self, mock_account: MagicMock) -> None:
        """Test instantiation with private key missing '0x' prefix."""
        pk_no_prefix = "b" * 64
        with patch("eth_account.Account.from_key", return_value=mock_account) as mock_from_key:
            HyperliquidEip712Authenticator(
                private_key_hex=pk_no_prefix,
                wallet_address=self.MOCKED_ACCOUNT_WALLET_ADDRESS,
                chain_id=1337,
            )
            mock_from_key.assert_called_once_with(f"0x{pk_no_prefix}")

    def test_instantiation_address_mismatch(self, mock_account: MagicMock) -> None:
        """Test instantiation raises ValueError if wallet address does not match derived address."""
        mock_account.address = "0xDifferentWalletAddress"
        with patch("eth_account.Account.from_key", return_value=mock_account):
            with pytest.raises(ValueError) as excinfo:
                HyperliquidEip712Authenticator(
                    private_key_hex=self.VALID_PRIVATE_KEY,
                    wallet_address=self.VALID_WALLET_ADDRESS,  # Original, non-matching address
                    chain_id=1337,
                )
            assert "Provided private key does not match wallet address" in str(excinfo.value)

    def test_instantiation_empty_private_key(self) -> None:
        """Test instantiation raises ValueError for empty private key."""
        with pytest.raises(ValueError, match="Private key cannot be empty."):
            HyperliquidEip712Authenticator(
                private_key_hex="", wallet_address=self.VALID_WALLET_ADDRESS, chain_id=1337
            )

    def test_instantiation_empty_wallet_address(self) -> None:
        """Test instantiation raises ValueError for empty wallet address."""
        with pytest.raises(ValueError, match="Wallet address cannot be empty."):
            HyperliquidEip712Authenticator(
                private_key_hex=self.VALID_PRIVATE_KEY, wallet_address="", chain_id=1337
            )

    def test_instantiation_invalid_private_key_format(self) -> None:
        """Test instantiation raises ValueError for invalid private key format (from_key might raise)."""
        with patch("eth_account.Account.from_key", side_effect=ValueError("bad key")):
            with pytest.raises(ValueError, match="Invalid private key: bad key"):
                HyperliquidEip712Authenticator(
                    private_key_hex="0xInvalidKey",
                    wallet_address=self.VALID_WALLET_ADDRESS,
                    chain_id=1337,
                )

    @pytest.mark.asyncio
    async def test_prepare_request_success(
        self, authenticator_instance: HyperliquidEip712Authenticator, mock_account: MagicMock
    ) -> None:
        """Test successful preparation of a signed request."""
        auth = authenticator_instance
        action_data = {"type": "order", "action": "buy"}
        original_headers = {"X-Custom-Header": "custom_value"}

        mocked_timestamp_ms = 1678886400000
        with (
            patch("time.time", return_value=mocked_timestamp_ms / 1000),
            patch("web3.Web3.keccak") as mock_keccak,
            patch("eth_account.messages.encode_typed_data") as mock_encode_typed_data,
        ):
            expected_connection_id_str = json.dumps(
                action_data, sort_keys=True, separators=(",", ":")
            )
            mock_keccak.return_value = Web3.keccak(text=expected_connection_id_str)

            mock_signable_message = MagicMock()
            mock_encode_typed_data.return_value = mock_signable_message

            components = await auth.prepare_request(
                method="POST",
                path="/exchange",
                params=None,
                data=action_data,
                headers=original_headers.copy(),
            )

            # The isinstance check for TypedDict is problematic with type checkers and not standard.
            # assert isinstance(components, AuthenticatedRequestComponents)
            assert "X-HL-Timestamp" in components["headers"]
            assert "X-HL-Nonce" in components["headers"]
            assert "X-HL-Signature" in components["headers"]
            assert components["headers"]["X-Custom-Header"] == "custom_value"
            assert components["params"] is None
            assert components["data"] == action_data

            mock_keccak.assert_called_once_with(text=expected_connection_id_str)

            expected_agent_message = {
                "source": "a",
                "connectionId": mock_keccak.return_value,
            }
            expected_structured_data = {
                "domain": auth._domain_data,  # noqa: SLF001
                "message": expected_agent_message,
                "primaryType": "Agent",
                "types": auth._agent_typed_data_message_types,  # noqa: SLF001
            }
            mock_encode_typed_data.assert_called_once_with(full_message=expected_structured_data)
            mock_account.sign_message.assert_called_once_with(mock_signable_message)

            assert components["headers"]["X-HL-Timestamp"] == str(mocked_timestamp_ms)
            assert components["headers"]["X-HL-Nonce"] == str(mocked_timestamp_ms)
            assert (
                components["headers"]["X-HL-Signature"]
                == mock_account.sign_message.return_value.signature.hex()
            )
            assert components["headers"]["X-Custom-Header"] == "custom_value"
            assert components["params"] is None
            assert components["data"] == action_data

    @pytest.mark.asyncio
    async def test_prepare_request_data_is_none(
        self, authenticator_instance: HyperliquidEip712Authenticator
    ) -> None:
        """Test prepare_request raises APIError if data is None."""
        auth = authenticator_instance
        with pytest.raises(APIError) as excinfo:
            await auth.prepare_request(
                method="POST", path="/exchange", params=None, data=None, headers={}
            )
        assert excinfo.value.code == APIErrorCode.INVALID_PARAMS.value
        assert "Data payload (action) is required" in excinfo.value.message

    @pytest.mark.asyncio
    async def test_nonce_increment(
        self, authenticator_instance: HyperliquidEip712Authenticator
    ) -> None:
        """Test that nonce (timestamp) is strictly increasing."""
        auth = authenticator_instance
        action_data = {"type": "test"}
        initial_time_sec = 1678886400.000

        with (
            patch("time.time") as mock_time,
            patch("web3.Web3.keccak"),
            patch("eth_account.messages.encode_typed_data") as mock_encode_typed_data,
        ):
            mock_encode_typed_data.return_value = MagicMock()

            mock_time.return_value = initial_time_sec
            components1 = await auth.prepare_request("POST", "/exchange", None, action_data, {})
            ts1 = int(components1["headers"]["X-HL-Timestamp"])
            assert ts1 == int(initial_time_sec * 1000)

            mock_time.return_value = initial_time_sec + 0.0001
            components2 = await auth.prepare_request("POST", "/exchange", None, action_data, {})
            ts2 = int(components2["headers"]["X-HL-Timestamp"])
            assert ts2 == ts1 + 1

            mock_time.return_value = initial_time_sec + 0.001
            components3 = await auth.prepare_request("POST", "/exchange", None, action_data, {})
            ts3 = int(components3["headers"]["X-HL-Timestamp"])
            assert ts3 == int((initial_time_sec + 0.001) * 1000)
            assert ts3 > ts2

            mock_time.return_value = initial_time_sec + 1.0
            components4 = await auth.prepare_request("POST", "/exchange", None, action_data, {})
            ts4 = int(components4["headers"]["X-HL-Timestamp"])
            assert ts4 == int((initial_time_sec + 1.0) * 1000)
            assert ts4 > ts3

    @pytest.mark.asyncio
    async def test_signing_failure(
        self, authenticator_instance: HyperliquidEip712Authenticator, mock_account: MagicMock
    ) -> None:
        """Test that APIError is raised if signing process fails."""
        auth = authenticator_instance
        mock_account.sign_message.side_effect = Exception("Signing exploded")
        action_data = {"type": "test"}

        with (
            patch("time.time", return_value=1678886400.0),
            patch("web3.Web3.keccak"),
            patch("eth_account.messages.encode_typed_data", return_value=MagicMock()),
        ):
            with pytest.raises(APIError) as excinfo:
                await auth.prepare_request("POST", "/exchange", None, action_data, {})
            assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
            assert "Failed to sign EIP-712 Agent request" in excinfo.value.message
            assert isinstance(excinfo.value.original_exception, Exception)
            assert str(excinfo.value.original_exception) == "Signing exploded"

    @pytest.mark.asyncio
    async def test_prepare_request_no_account_after_init(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test prepare_request behavior if _account is None (e.g., init failed silently)."""
        # This scenario is less likely now since __init__ raises directly on key errors.
        # However, testing the guard in prepare_request is still valid.
        auth = HyperliquidEip712Authenticator(
            private_key_hex=self.VALID_PRIVATE_KEY,  # Valid key to pass init
            wallet_address=self.MOCKED_ACCOUNT_WALLET_ADDRESS,  # Matching address for init
            chain_id=1337,
        )
        auth._account = None  # Manually force _account to None post-initialization # noqa: SLF001

        with pytest.raises(APIError, match="Authenticator does not have a usable private key"):
            await auth.prepare_request("POST", "/exchange", None, {"key": "value"}, None)
        assert "Account object is None, cannot sign." in caplog.text

    @pytest.mark.asyncio
    async def test_prepare_request_with_non_dict_data(
        self, authenticator_instance: HyperliquidEip712Authenticator
    ) -> None:
        """Test prepare_request raises APIError if data is not a dictionary."""
        with pytest.raises(APIError, match="Action payload must be a dictionary") as excinfo:
            await authenticator_instance.prepare_request(
                "POST",
                "/exchange",
                None,
                "not_a_dict",
                None,  # type: ignore[arg-type]
            )
        assert excinfo.value.code == APIErrorCode.INVALID_PARAMS.value
