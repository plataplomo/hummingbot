"""
Unit tests for HyperliquidEip712Authenticator.
"""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from eth_account.datastructures import SignedMessage
from hexbytes import HexBytes

from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Sample valid credentials
VALID_PRIVATE_KEY_HEX = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
VALID_WALLET_ADDRESS = "0x1Be31A94361a391bBaFB2a4CCd704F57dc04d4bb"  # Corrected derived address
VALID_CHAIN_ID = 1337

# Sample invalid credentials
INVALID_PRIVATE_KEY_HEX = "0xinvalidkey"
# MISMATCHED_WALLET_ADDRESS = "0xDeadBeefDeadBeefDeadBeefDeadBeefDeadBeef" # Removed as wallet_address is not a direct input with private key


@pytest.fixture
def mock_account() -> MagicMock:
    """Fixture for a mocked eth_account.Account object."""
    account = MagicMock()
    account.address = "0xWalletAddress"
    # Mock the sign_message method to return a MagicMock(SignedMessage)
    signed_msg_mock = MagicMock(spec=SignedMessage)
    signed_msg_mock.signature = HexBytes("0x" + "a" * 130)  # Placeholder signature 65 bytes
    account.sign_message.return_value = signed_msg_mock
    return account


@pytest.fixture
def authenticator_instance() -> HyperliquidEip712Authenticator:
    """Fixture for a HyperliquidEip712Authenticator instance with a valid private key.
    This uses a real key to create a real LocalAccount internally.
    For tests needing to mock account creation or behavior, patch Account.from_key or the account instance directly.
    """
    return HyperliquidEip712Authenticator(
        wallet_private_key=VALID_PRIVATE_KEY_HEX,
        chain_id=VALID_CHAIN_ID,
    )


# --- Test Initialization ---


@patch("eth_account.Account.from_key")
def test_hl_auth_init_success_with_private_key(
    mock_from_key: MagicMock, mock_account: MagicMock
) -> None:
    """Test successful initialization with valid credentials."""
    mock_from_key.return_value = mock_account
    # Ensure the mocked account has the address that would be derived from VALID_PRIVATE_KEY_HEX
    mock_account.address = VALID_WALLET_ADDRESS

    auth = HyperliquidEip712Authenticator(
        wallet_private_key=VALID_PRIVATE_KEY_HEX,
        chain_id=VALID_CHAIN_ID,
    )
    mock_from_key.assert_called_once_with(VALID_PRIVATE_KEY_HEX)
    assert auth.wallet_address.lower() == VALID_WALLET_ADDRESS.lower()
    assert auth.chain_id == VALID_CHAIN_ID
    assert auth._account is mock_account  # noqa: SLF001
    assert auth._account.address.lower() == VALID_WALLET_ADDRESS.lower()  # noqa: SLF001


def test_hl_auth_init_success_with_account_object(mock_account: MagicMock) -> None:
    """Test successful initialization with a pre-existing account object."""
    mock_account.address = VALID_WALLET_ADDRESS
    auth = HyperliquidEip712Authenticator(
        account_object=mock_account,
        chain_id=VALID_CHAIN_ID,
    )
    assert auth.wallet_address.lower() == VALID_WALLET_ADDRESS.lower()
    assert auth.chain_id == VALID_CHAIN_ID
    assert auth._account is mock_account  # noqa: SLF001


@patch("eth_account.Account.from_key")
def test_hl_auth_init_success_no_0x_private_key(
    mock_from_key: MagicMock, mock_account: MagicMock
) -> None:
    """Test successful initialization with valid key without '0x' prefix."""
    mock_from_key.return_value = mock_account
    mock_account.address = (
        VALID_WALLET_ADDRESS  # Assume this would be the address for key_no_prefix
    )

    key_no_prefix = VALID_PRIVATE_KEY_HEX[2:]
    auth = HyperliquidEip712Authenticator(
        wallet_private_key=key_no_prefix,
        chain_id=VALID_CHAIN_ID,
    )
    mock_from_key.assert_called_once_with(key_no_prefix)
    assert auth._account is mock_account  # noqa: SLF001
    assert auth.wallet_address.lower() == VALID_WALLET_ADDRESS.lower()


def test_hl_auth_init_no_key_or_account() -> None:
    """Test initialization with no private key or account object raises ValueError."""
    with pytest.raises(
        ValueError, match="Either wallet_private_key or account_object must be provided."
    ):
        HyperliquidEip712Authenticator(chain_id=VALID_CHAIN_ID)


def test_hl_auth_init_both_key_and_account(mock_account: MagicMock) -> None:
    """Test initialization with both private key and account object raises ValueError."""
    with pytest.raises(
        ValueError, match="Provide either wallet_private_key or account_object, not both."
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key=VALID_PRIVATE_KEY_HEX,
            account_object=mock_account,
            chain_id=VALID_CHAIN_ID,
        )


@patch("eth_account.Account.from_key", side_effect=ValueError("Simulated Key Error"))
def test_hl_auth_init_from_key_value_error(mock_from_key: MagicMock) -> None:
    """Test initialization raises ValueError if Account.from_key raises ValueError."""
    with pytest.raises(
        ValueError,
        match=r"Invalid private key: Simulated Key Error",
    ):
        HyperliquidEip712Authenticator(
            wallet_private_key="somekey_that_will_cause_error_in_mock",
            chain_id=VALID_CHAIN_ID,
        )


# --- Test prepare_request ---


@patch("eth_account.Account.from_key")
@pytest.mark.asyncio
async def test_prepare_request_success(mock_from_key: MagicMock, mock_account: MagicMock) -> None:
    """Test successful preparation and signing of a request."""
    mock_from_key.return_value = mock_account
    mock_account.address = VALID_WALLET_ADDRESS  # Ensure mock account has the right address

    # Setup the mock_account.sign_message return value
    signed_msg_mock = MagicMock(spec=SignedMessage)
    mock_signature_hex = "0x" + "a" * 130  # 65 bytes hex
    signed_msg_mock.signature = HexBytes(mock_signature_hex)
    mock_account.sign_message.return_value = signed_msg_mock

    auth = HyperliquidEip712Authenticator(
        wallet_private_key=VALID_PRIVATE_KEY_HEX,  # This will use the mocked from_key
        chain_id=VALID_CHAIN_ID,
    )

    method = "POST"
    path = "/exchange"
    params = {"query": "value"}
    data = {"action": "place_order", "details": {"coin": "BTC", "size": "1"}}
    headers = {"X-Custom-Header": "custom"}

    fixed_time_sec = 1678886400.123
    with patch("time.time", return_value=fixed_time_sec):
        result: AuthenticatedRequestComponents = await auth.prepare_request(
            method, path, params, data, headers
        )

    assert "X-HL-Signature" in result["headers"]
    assert len(result["headers"]["X-HL-Signature"]) == 130  # 65 bytes hex
    assert result["headers"]["X-HL-Timestamp"] == str(int(fixed_time_sec * 1000))
    assert result["headers"]["X-HL-Nonce"] == str(int(fixed_time_sec * 1000))
    assert result["headers"]["X-Custom-Header"] == "custom"
    assert result["params"] == params
    assert result["data"] == data

    # Verify that sign_message was called with the correct structure
    mock_account.sign_message.assert_called_once()
    call_args = mock_account.sign_message.call_args[0][0]  # Get the signable_message (TypedDict)

    # Basic checks on EIP-712 structure passed to sign_message
    assert call_args["domain"]["name"] == "Hyperliquid"
    assert call_args["domain"]["version"] == "1"
    assert call_args["domain"]["chainId"] == VALID_CHAIN_ID
    assert call_args["primaryType"] == "Agent"
    assert "Agent" in call_args["types"]
    assert "EIP712Domain" in call_args["types"]
    assert call_args["message"]["source"] == "a"  # As per HL spec/convention
    assert "connectionId" in call_args["message"]

    # Verify connectionId generation
    # This requires importing Web3 locally or making _generate_connection_id public (not ideal for unit test)
    # For now, we trust the internal generation if the overall signature process works.
    # Or, we can replicate the logic here for a more thorough check if needed.
    from web3 import Web3  # Local import for test

    expected_connection_id_bytes = Web3.keccak(
        text=json.dumps(data, sort_keys=True, separators=(",", ":"))
    )
    assert call_args["message"]["connectionId"] == expected_connection_id_bytes


@pytest.mark.asyncio
async def test_prepare_request_account_becomes_none_after_init(
    authenticator_instance: HyperliquidEip712Authenticator, mock_logger: MagicMock
) -> None:
    """
    Test prepare_request raises APIError if _account is None (e.g., due to a bug or unexpected state).
    This simulates a scenario where the account object becomes None after initialization.
    """
    # Intentionally set _account to None to simulate an internal error state
    # This is for testing a defensive check within prepare_request or its callees
    authenticator_instance._account = None  # type: ignore[assignment] # noqa: SLF001

    method = "POST"
    path = "/exchange"
    data = {"action": "test"}

    with pytest.raises(APIError) as _:  # Use _ for unused excinfo
        await authenticator_instance.prepare_request(method, path, None, data, None)
    # Use AUTHENTICATION_FAILED.value for comparison
    assert _.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
    # Ensure mock_logger.error itself is a mock and assert_called_with is called on it
    mock_logger.error.assert_called_with(
        "HyperliquidEip712Authenticator: Account object is None, cannot sign message.",
        exc_info=True,
    )


@pytest.mark.asyncio
async def test_prepare_request_data_is_none(
    authenticator_instance: HyperliquidEip712Authenticator,
) -> None:
    """Test prepare_request raises ValueError if data (action_payload) is None."""
    method = "POST"
    path = "/exchange"
    with pytest.raises(ValueError, match="Invalid 'data' for Hyperliquid EIP-712 Agent signature"):
        await authenticator_instance.prepare_request(method, path, None, None, None)


@patch("eth_account.Account.from_key")
@pytest.mark.asyncio
async def test_prepare_request_signing_error(
    mock_from_key: MagicMock, mock_account: MagicMock
) -> None:
    """Test prepare_request raises APIError if message signing fails."""
    mock_from_key.return_value = mock_account
    # Simulate a signing error
    mock_account.sign_message.side_effect = Exception("Simulated signing error")

    auth = HyperliquidEip712Authenticator(
        wallet_private_key=VALID_PRIVATE_KEY_HEX, chain_id=VALID_CHAIN_ID
    )

    method = "POST"
    path = "/exchange"
    data = {"action": "test"}

    with pytest.raises(APIError) as _:  # Use _ for unused excinfo
        await auth.prepare_request(method, path, None, data, None)
    # Use AUTHENTICATION_FAILED.value for comparison
    assert _.value.code == APIErrorCode.AUTHENTICATION_FAILED.value


@pytest.mark.asyncio
async def test_prepare_request_timestamp_nonce_generation(
    authenticator_instance: HyperliquidEip712Authenticator,
) -> None:
    """Test that timestamp and nonce are generated and included in headers."""
    method = "POST"
    path = "/exchange"
    data = {"action": "test_nonce"}
    fixed_time_sec = 1678886400.123  # Example: March 15, 2023 12:00:00.123 PM UTC

    with patch("time.time", return_value=fixed_time_sec):
        result = await authenticator_instance.prepare_request(method, path, None, data, None)

    expected_timestamp_ms = str(int(fixed_time_sec * 1000))
    assert result["headers"]["X-HL-Timestamp"] == expected_timestamp_ms
    # For the first call, nonce might be equal to timestamp if no previous calls.
    # This test primarily checks presence and format.
    assert "X-HL-Nonce" in result["headers"]
    assert result["headers"]["X-HL-Nonce"] == expected_timestamp_ms  # Or greater if stateful


@pytest.mark.asyncio
async def test_nonce_strictly_increasing(
    authenticator_instance: HyperliquidEip712Authenticator,
) -> None:
    """Test that nonce values are strictly increasing across multiple calls."""
    method = "POST"
    path = "/exchange"
    data_1 = {"action": "first_call"}
    data_2 = {"action": "second_call_same_ms"}
    data_3 = {"action": "third_call_later_ms"}

    fixed_time_sec_1 = 1678886400.123
    fixed_time_sec_2 = 1678886400.123  # Same millisecond
    fixed_time_sec_3 = 1678886400.124  # Next millisecond

    results: list[AuthenticatedRequestComponents] = []

    with patch("time.time", return_value=fixed_time_sec_1):
        results.append(
            await authenticator_instance.prepare_request(method, path, None, data_1, None)
        )

    with patch("time.time", return_value=fixed_time_sec_2):
        results.append(
            await authenticator_instance.prepare_request(method, path, None, data_2, None)
        )

    with patch("time.time", return_value=fixed_time_sec_3):
        results.append(
            await authenticator_instance.prepare_request(method, path, None, data_3, None)
        )

    nonce1 = int(results[0]["headers"]["X-HL-Nonce"])
    nonce2 = int(results[1]["headers"]["X-HL-Nonce"])
    nonce3 = int(results[2]["headers"]["X-HL-Nonce"])

    assert nonce2 > nonce1, "Nonce should increase even if time is the same millisecond"
    assert nonce3 > nonce2, "Nonce should increase with time"

    expected_ts1_ms = int(fixed_time_sec_1 * 1000)
    expected_ts3_ms = int(fixed_time_sec_3 * 1000)

    assert results[0]["headers"]["X-HL-Timestamp"] == str(expected_ts1_ms)
    assert results[1]["headers"]["X-HL-Timestamp"] == str(
        expected_ts1_ms
    )  # Time was patched to same
    assert results[2]["headers"]["X-HL-Timestamp"] == str(expected_ts3_ms)


# Test class for more structured tests
# (This duplicates some tests from above but might be preferred for organization)
class TestHyperliquidEip712Authenticator:
    # Use module-level constants for consistency
    VALID_PRIVATE_KEY = VALID_PRIVATE_KEY_HEX
    # This is the address corresponding to VALID_PRIVATE_KEY_HEX
    EXPECTED_WALLET_ADDRESS_CLASS_SCOPE = VALID_WALLET_ADDRESS
    MOCKED_ACCOUNT_WALLET_ADDRESS = (
        "0xMockedAccountObjectAddress000000000"  # A distinct address for clarity
    )
    CHAIN_ID = VALID_CHAIN_ID  # Use module-level constant

    @pytest.fixture
    def mock_logger(self) -> MagicMock:
        # Ensure this fixture returns MagicMock explicitly for type hinting clarity in tests
        return MagicMock(spec=logging.Logger)

    @pytest.fixture
    def auth_with_mock_account(
        self, mock_account: MagicMock, mock_logger: MagicMock
    ) -> HyperliquidEip712Authenticator:
        """Authenticator instance using a mocked account object."""
        # Configure the mock_account before passing it
        mock_account.address = self.MOCKED_ACCOUNT_WALLET_ADDRESS
        signed_msg_mock = MagicMock(spec=SignedMessage)
        signed_msg_mock.signature = HexBytes("0x" + "b" * 130)  # Different signature for clarity
        mock_account.sign_message.return_value = signed_msg_mock
        return HyperliquidEip712Authenticator(
            account_object=mock_account,
            chain_id=self.CHAIN_ID,
            logger=mock_logger,
        )

    @pytest.fixture
    @patch("eth_account.Account.from_key")
    def auth_with_mock_from_key(
        self, mock_from_key: MagicMock, mock_account: MagicMock, mock_logger: MagicMock
    ) -> HyperliquidEip712Authenticator:
        """Authenticator instance where Account.from_key is mocked."""
        mock_from_key.return_value = mock_account
        mock_account.address = (
            self.EXPECTED_WALLET_ADDRESS_CLASS_SCOPE
        )  # Ensure derived address matches
        return HyperliquidEip712Authenticator(
            wallet_private_key=self.VALID_PRIVATE_KEY,
            chain_id=self.CHAIN_ID,
            logger=mock_logger,
        )

    def test_instantiation_with_account_object(
        self, mock_account: MagicMock, mock_logger: MagicMock
    ) -> None:
        """Test instantiation with a pre-configured account object."""
        mock_account.address = self.MOCKED_ACCOUNT_WALLET_ADDRESS
        auth = HyperliquidEip712Authenticator(
            account_object=mock_account,
            chain_id=self.CHAIN_ID,
            logger=mock_logger,
        )
        assert auth.wallet_address == self.MOCKED_ACCOUNT_WALLET_ADDRESS
        assert auth.chain_id == self.CHAIN_ID
        assert auth._account is mock_account  # noqa: SLF001
        mock_logger.info.assert_any_call(
            f"HyperliquidEip712Authenticator initialized for address: {self.MOCKED_ACCOUNT_WALLET_ADDRESS} "
            f"on chain_id: {self.CHAIN_ID}"
        )

    def test_instantiation_with_private_key(
        self, auth_with_mock_from_key: HyperliquidEip712Authenticator, mock_account: MagicMock
    ) -> None:
        """Test instantiation with a private key, mocking Account.from_key."""
        auth = auth_with_mock_from_key
        assert auth.wallet_address.lower() == self.EXPECTED_WALLET_ADDRESS_CLASS_SCOPE.lower()
        assert auth.chain_id == self.CHAIN_ID
        assert auth._account is mock_account  # noqa: SLF001

    def test_instantiation_private_key_no_prefix(
        self, mock_account: MagicMock, mock_logger: MagicMock
    ) -> None:
        """Test instantiation with private key string without '0x' prefix."""
        key_no_prefix = self.VALID_PRIVATE_KEY[2:]
        mock_account.address = self.EXPECTED_WALLET_ADDRESS_CLASS_SCOPE

        with patch("eth_account.Account.from_key", return_value=mock_account) as mk_from_key:
            auth = HyperliquidEip712Authenticator(
                wallet_private_key=key_no_prefix,
                chain_id=self.CHAIN_ID,
                logger=mock_logger,
            )
        mk_from_key.assert_called_once_with(key_no_prefix)
        assert auth.wallet_address.lower() == self.EXPECTED_WALLET_ADDRESS_CLASS_SCOPE.lower()
        assert auth._account is mock_account  # noqa: SLF001

    def test_instantiation_no_key_or_account_object(self, mock_logger: MagicMock) -> None:
        """Test ValueError if neither private key nor account object is provided."""
        with pytest.raises(ValueError, match="must be provided"):
            HyperliquidEip712Authenticator(chain_id=self.CHAIN_ID, logger=mock_logger)

    def test_instantiation_both_key_and_account_object(
        self, mock_account: MagicMock, mock_logger: MagicMock
    ) -> None:
        """Test ValueError if both private key and account object are provided."""
        with pytest.raises(ValueError, match="not both"):
            HyperliquidEip712Authenticator(
                wallet_private_key=self.VALID_PRIVATE_KEY,
                account_object=mock_account,
                chain_id=self.CHAIN_ID,
                logger=mock_logger,
            )

    @patch("eth_account.Account.from_key", side_effect=ValueError("Bad Key From Test"))
    def test_instantiation_invalid_private_key_value_error(
        self, mock_from_key: MagicMock, mock_logger: MagicMock
    ) -> None:
        """Test ValueError from Account.from_key is propagated and logged."""
        with pytest.raises(ValueError, match="Invalid private key: Bad Key From Test"):
            HyperliquidEip712Authenticator(
                wallet_private_key="invalid-key",
                chain_id=self.CHAIN_ID,
                logger=mock_logger,
            )
        mock_logger.error.assert_called_once_with(
            "HyperliquidEip712Authenticator: Invalid private key: Bad Key From Test",
            exc_info=True,
        )

    @pytest.mark.asyncio
    async def test_prepare_request_success(
        self, auth_with_mock_account: HyperliquidEip712Authenticator, mock_account: MagicMock
    ) -> None:
        """Test successful request preparation and signing with a mocked account."""
        auth = auth_with_mock_account
        method = "POST"
        path = "/exchange"
        params = {"query": "param"}
        data = {"action": "order", "coin": "ETH"}
        headers = {"User-Agent": "Test"}

        fixed_time_sec = 1700000000.500
        with patch("time.time", return_value=fixed_time_sec):
            result = await auth.prepare_request(method, path, params, data, headers)

        assert "X-HL-Signature" in result["headers"]
        # mock_account.sign_message.return_value.signature is HexBytes("0x" + "b" * 130)
        assert (
            result["headers"]["X-HL-Signature"]
            == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        )
        expected_ts_ms = str(int(fixed_time_sec * 1000))
        assert result["headers"]["X-HL-Timestamp"] == expected_ts_ms
        assert result["headers"]["X-HL-Nonce"] == expected_ts_ms  # First call
        assert result["headers"]["User-Agent"] == "Test"
        assert result["params"] == params
        assert result["data"] == data

        mock_account.sign_message.assert_called_once()
        typed_data_signed = mock_account.sign_message.call_args[0][0]

        assert typed_data_signed["domain"]["name"] == "Hyperliquid"
        assert typed_data_signed["domain"]["chainId"] == self.CHAIN_ID
        assert typed_data_signed["primaryType"] == "Agent"
        assert typed_data_signed["message"]["source"] == "a"

        from web3 import Web3  # Local import for test

        expected_connection_id = Web3.keccak(
            text=json.dumps(data, sort_keys=True, separators=(",", ":"))
        )
        assert typed_data_signed["message"]["connectionId"] == expected_connection_id

    @pytest.mark.asyncio
    async def test_prepare_request_data_is_none(
        self, auth_with_mock_account: HyperliquidEip712Authenticator
    ) -> None:
        """Test ValueError if data is None when preparing request."""
        with pytest.raises(
            ValueError, match="Invalid 'data' for Hyperliquid EIP-712 Agent signature"
        ):
            await auth_with_mock_account.prepare_request("POST", "/exchange", None, None, None)

    @pytest.mark.asyncio
    async def test_nonce_increment(
        self, auth_with_mock_account: HyperliquidEip712Authenticator, mock_account: MagicMock
    ) -> None:
        """Test that nonce increments correctly even with same millisecond timestamps."""
        auth = auth_with_mock_account
        path = "/exchange"
        data1 = {"action": "call1"}
        data2 = {"action": "call2"}

        time_val = 1700000001.000  # Exact second
        with patch("time.time", return_value=time_val):
            res1 = await auth.prepare_request("POST", path, None, data1, None)
        # mock_account.sign_message should be called once for res1
        # Need to reset for the next call if we are asserting call_count per call
        mock_account.sign_message.reset_mock()

        with patch("time.time", return_value=time_val):  # Same time
            res2 = await auth.prepare_request("POST", path, None, data2, None)

        nonce1 = int(res1["headers"]["X-HL-Nonce"])
        nonce2 = int(res2["headers"]["X-HL-Nonce"])

        assert nonce2 == nonce1 + 1, "Nonce should increment by 1 for same timestamp"
        ts_ms = str(int(time_val * 1000))
        assert res1["headers"]["X-HL-Timestamp"] == ts_ms
        assert res2["headers"]["X-HL-Timestamp"] == ts_ms

    @pytest.mark.asyncio
    async def test_signing_failure(
        self, auth_with_mock_account: HyperliquidEip712Authenticator, mock_account: MagicMock
    ) -> None:
        """Test APIError if account.sign_message raises an exception."""
        auth = auth_with_mock_account
        mock_account.sign_message.side_effect = Exception("Crypto error")

        with pytest.raises(APIError) as excinfo:  # Keep excinfo if used, else use _
            await auth.prepare_request("POST", "/exchange", None, {"action": "fail"}, None)

        assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert "Failed to sign EIP-712 message" in str(excinfo.value.message)
        # Ensure auth.logger.error is a mock and assert_called_with is called on it
        auth.logger.error.assert_called_with(
            "HyperliquidEip712Authenticator: Failed to sign EIP-712 message: Crypto error",
            exc_info=True,
        )

    @pytest.mark.asyncio
    async def test_prepare_request_account_becomes_none_mid_flight(
        self, auth_with_mock_account: HyperliquidEip712Authenticator, mock_logger: MagicMock
    ) -> None:
        """Test APIError if self._account is None during prepare_request."""
        auth = auth_with_mock_account
        auth._account = None  # type: ignore[assignment] # noqa: SLF001

        with pytest.raises(APIError) as excinfo:  # Keep excinfo if used, else use _
            await auth.prepare_request(
                "POST", "/exchange", None, {"action": "test_no_account"}, None
            )

        assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert "Account object is None, cannot sign message." in str(excinfo.value.message)
        # Ensure mock_logger.error is a mock and assert_called_with is called on it
        # (auth.logger was set to mock_logger in the fixture for auth_with_mock_account)
        mock_logger.error.assert_called_with(
            "HyperliquidEip712Authenticator: Account object is None, cannot sign message.",
            exc_info=True,
        )

    @pytest.mark.asyncio
    async def test_prepare_request_with_non_dict_data(
        self, auth_with_mock_account: HyperliquidEip712Authenticator
    ) -> None:
        """Test ValueError if data is not a dict (e.g. list or string)."""
        auth = auth_with_mock_account

        # Test with list
        with pytest.raises(
            ValueError, match="Invalid 'data' for Hyperliquid EIP-712 Agent signature"
        ):
            await auth.prepare_request(
                "POST",
                "/exchange",
                None,
                ["not", "a", "dict"],  # type: ignore[arg-type]
                None,
            )
        auth.logger.error.assert_called_with(
            "HyperliquidEip712Authenticator: Invalid 'data' for Hyperliquid EIP-712 "
            "Agent signature: Must be a dictionary and not None. "
            "Received: <class 'list'>",
        )
        auth.logger.reset_mock()  # type: ignore[attr-defined]

        # Test with string
        with pytest.raises(
            ValueError, match="Invalid 'data' for Hyperliquid EIP-712 Agent signature"
        ):
            await auth.prepare_request(
                "POST",
                "/exchange",
                None,
                "not a dict",  # type: ignore[arg-type]
                None,
            )
        auth.logger.error.assert_called_with(
            "HyperliquidEip712Authenticator: Invalid 'data' for Hyperliquid EIP-712 "
            "Agent signature: Must be a dictionary and not None. "
            "Received: <class 'str'>",
        )

    @pytest.mark.asyncio
    async def test_prepare_request_action_with_valid_dict_payload(
        self, auth_with_mock_account: HyperliquidEip712Authenticator, mock_account: MagicMock
    ) -> None:
        """Test prepare_request with a valid dictionary payload for 'data'."""
        auth = auth_with_mock_account
        action_payload = {"type": "order", "orders": [{"coin": "BTC", "is_buy": True, "sz": "0.1"}]}
        result = await auth.prepare_request("POST", "/exchange", None, action_payload, None)
        assert "X-HL-Signature" in result["headers"]
        mock_account.sign_message.assert_called_once()  # Ensure signing occurred


@patch("eth_account.Account.from_key")
def test_internal_generate_connection_id_deterministic(
    mock_from_key: MagicMock, mock_account: MagicMock
) -> None:
    """Test that _generate_connection_id is deterministic for the same payload."""
    mock_from_key.return_value = mock_account
    mock_account.address = VALID_WALLET_ADDRESS
    auth = HyperliquidEip712Authenticator(
        wallet_private_key=VALID_PRIVATE_KEY_HEX, chain_id=VALID_CHAIN_ID
    )

    payload1 = {"coin": "BTC", "size": "1.0", "is_buy": True, "limit_px": "50000.0"}
    payload2 = {"coin": "BTC", "size": "1.0", "is_buy": True, "limit_px": "50000.0"}

    conn_id1 = auth._generate_connection_id(payload1)  # noqa: SLF001
    conn_id2 = auth._generate_connection_id(payload2)  # noqa: SLF001
    assert conn_id1 == conn_id2

    payload_shuffled = {"limit_px": "50000.0", "is_buy": True, "size": "1.0", "coin": "BTC"}
    conn_id_shuffled = auth._generate_connection_id(payload_shuffled)  # noqa: SLF001
    assert conn_id1 == conn_id_shuffled


@patch("eth_account.Account.from_key")
def test_internal_generate_connection_id_content_change(
    mock_from_key: MagicMock, mock_account: MagicMock
) -> None:
    """Test that _generate_connection_id changes if payload content changes."""
    mock_from_key.return_value = mock_account
    mock_account.address = VALID_WALLET_ADDRESS
    auth = HyperliquidEip712Authenticator(
        wallet_private_key=VALID_PRIVATE_KEY_HEX, chain_id=VALID_CHAIN_ID
    )

    payload1 = {"coin": "BTC", "size": "1.0"}
    payload2 = {"coin": "ETH", "size": "1.0"}  # Different coin
    payload3 = {"coin": "BTC", "size": "2.0"}  # Different size

    conn_id1 = auth._generate_connection_id(payload1)  # noqa: SLF001
    conn_id2 = auth._generate_connection_id(payload2)  # noqa: SLF001
    conn_id3 = auth._generate_connection_id(payload3)  # noqa: SLF001

    assert conn_id1 != conn_id2
    assert conn_id1 != conn_id3
    assert conn_id2 != conn_id3
