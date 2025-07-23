"""Simple unit tests for HyperliquidEip712Authenticator focusing on public API behavior."""

import string

import pytest
from pydantic import SecretStr

from cyberdelta.apis.base.network_security_domain import NetworkEnvironmentFactory
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.enums.environment import EnvironmentType


class TestHyperliquidEip712AuthenticatorPublic:
    """Test authenticator public API only."""

    def test_init_with_valid_private_key(self) -> None:
        """Test initialization with valid private key."""
        private_key = SecretStr(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )
        network_env = NetworkEnvironmentFactory.testnet(
            api_endpoint="https://api.hyperliquid-testnet.xyz",
            websocket_endpoint="wss://api.hyperliquid-testnet.xyz/ws",
        )

        auth = HyperliquidEip712Authenticator(
            wallet_private_key_secret=private_key, chain_id=421614, network_environment=network_env
        )

        assert auth is not None
        assert auth.wallet_address is not None
        assert auth.wallet_address.startswith("0x")
        assert len(auth.wallet_address) == 42
        assert auth.chain_id == 421614

    def test_wallet_address_property(self) -> None:
        """Test wallet_address property returns valid address."""
        private_key = SecretStr(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )
        network_env = NetworkEnvironmentFactory.testnet(
            api_endpoint="https://api.hyperliquid-testnet.xyz",
            websocket_endpoint="wss://api.hyperliquid-testnet.xyz/ws",
        )

        auth = HyperliquidEip712Authenticator(
            wallet_private_key_secret=private_key, chain_id=421614, network_environment=network_env
        )

        address = auth.wallet_address
        assert isinstance(address, str)
        assert address.startswith("0x")
        assert len(address) == 42
        # Check all characters after 0x are valid hex
        assert all(c in string.hexdigits for c in address[2:])

    def test_chain_id_property(self) -> None:
        """Test chain_id property returns correct value."""
        private_key = SecretStr(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )
        network_env = NetworkEnvironmentFactory.mainnet(
            api_endpoint="https://api.hyperliquid.xyz",
            websocket_endpoint="wss://api.hyperliquid.xyz/ws",
            chain_id=1337,
        )

        auth = HyperliquidEip712Authenticator(
            wallet_private_key_secret=private_key, chain_id=1337, network_environment=network_env
        )

        assert auth.chain_id == 1337

    @pytest.mark.parametrize("chain_id", [1337, 421614])
    def test_init_with_different_chain_ids(self, chain_id: int) -> None:
        """Test initialization with different chain IDs."""
        private_key = SecretStr(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )
        if chain_id == 1337:
            network_env = NetworkEnvironmentFactory.mainnet(
                api_endpoint="https://api.hyperliquid.xyz",
                websocket_endpoint="wss://api.hyperliquid.xyz/ws",
                chain_id=chain_id,
            )
        else:
            network_env = NetworkEnvironmentFactory.testnet(
                api_endpoint="https://api.hyperliquid-testnet.xyz",
                websocket_endpoint="wss://api.hyperliquid-testnet.xyz/ws",
                chain_id=chain_id,
            )

        auth = HyperliquidEip712Authenticator(
            wallet_private_key_secret=private_key,
            chain_id=chain_id,
            network_environment=network_env,
        )

        assert auth.chain_id == chain_id

    @pytest.mark.parametrize("env_type", [EnvironmentType.TESTNET, EnvironmentType.MAINNET])
    def test_init_with_different_networks(self, env_type: EnvironmentType) -> None:
        """Test initialization with different network environments."""
        private_key = SecretStr(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )

        if env_type == EnvironmentType.MAINNET:
            chain_id = 1337
            network_env = NetworkEnvironmentFactory.mainnet(
                api_endpoint="https://api.hyperliquid.xyz",
                websocket_endpoint="wss://api.hyperliquid.xyz/ws",
                chain_id=chain_id,
            )
        else:
            chain_id = 421614
            network_env = NetworkEnvironmentFactory.testnet(
                api_endpoint="https://api.hyperliquid-testnet.xyz",
                websocket_endpoint="wss://api.hyperliquid-testnet.xyz/ws",
                chain_id=chain_id,
            )

        auth = HyperliquidEip712Authenticator(
            wallet_private_key_secret=private_key,
            chain_id=chain_id,
            network_environment=network_env,
        )

        assert auth is not None
        assert auth.wallet_address is not None

    def test_wallet_address_consistency(self) -> None:
        """Test that wallet address is consistent across calls."""
        private_key = SecretStr(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )
        network_env = NetworkEnvironmentFactory.testnet(
            api_endpoint="https://api.hyperliquid-testnet.xyz",
            websocket_endpoint="wss://api.hyperliquid-testnet.xyz/ws",
        )

        auth = HyperliquidEip712Authenticator(
            wallet_private_key_secret=private_key, chain_id=421614, network_environment=network_env
        )

        address1 = auth.wallet_address
        address2 = auth.wallet_address

        assert address1 == address2

    def test_different_private_keys_different_addresses(self) -> None:
        """Test that different private keys generate different addresses."""
        private_key1 = SecretStr(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )
        private_key2 = SecretStr(
            "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
        )
        network_env = NetworkEnvironmentFactory.testnet(
            api_endpoint="https://api.hyperliquid-testnet.xyz",
            websocket_endpoint="wss://api.hyperliquid-testnet.xyz/ws",
        )

        auth1 = HyperliquidEip712Authenticator(
            wallet_private_key_secret=private_key1, chain_id=421614, network_environment=network_env
        )

        auth2 = HyperliquidEip712Authenticator(
            wallet_private_key_secret=private_key2, chain_id=421614, network_environment=network_env
        )

        assert auth1.wallet_address != auth2.wallet_address
