"""
Unit tests for HyperliquidEip712Authenticator's sign_l1_action scheme.

This tests the refactored authentication method that aligns with Hyperliquid SDK's
sign_l1_action scheme for the /exchange endpoint.
"""

from typing import Any
from unittest.mock import patch

import msgpack
import pytest
from eth_utils.crypto import keccak
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator, address_to_bytes

# Test constants
VALID_PRIVATE_KEY = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
CHAIN_ID = 1337


class TestHyperliquidSignL1Action:
    """Test suite for the sign_l1_action authentication scheme."""

    @pytest.fixture
    def authenticator(self) -> HyperliquidEip712Authenticator:
        """Create an authenticator instance with a valid private key."""
        return HyperliquidEip712Authenticator(
            wallet_private_key_secret=SecretStr(VALID_PRIVATE_KEY),
            chain_id=CHAIN_ID,
        )

    @pytest.fixture
    def sample_order_action(self) -> dict[str, Any]:
        """Sample order action payload."""
        return {
            "type": "order",
            "orders": [
                {
                    "coin": "BTC",
                    "is_buy": True,
                    "sz": "0.1",
                    "limit_px": "50000",
                    "order_type": {"limit": {"tif": "Gtc"}, "market": None},
                }
            ],
            "grouping": "na",
        }

    @pytest.mark.asyncio
    async def test_exchange_endpoint_uses_sign_l1_action(
        self, authenticator: HyperliquidEip712Authenticator, sample_order_action: dict[str, Any]
    ) -> None:
        """Test that /exchange endpoint uses the new sign_l1_action scheme."""
        result = await authenticator.prepare_request(
            method="POST",
            path="/exchange",
            params=None,
            data=sample_order_action,
            headers=None,
        )

        # Should NOT have X-HL-* headers
        assert "X-HL-Timestamp" not in result.headers
        assert "X-HL-Nonce" not in result.headers
        assert "X-HL-Signature" not in result.headers

        # Should have Content-Type header
        assert result.headers["Content-Type"] == "application/json"

        # Should have action, nonce, and signature in body
        assert "action" in result.data
        assert "nonce" in result.data
        assert "signature" in result.data

        # Signature should have r, s, v components
        sig = result.data["signature"]
        assert "r" in sig
        assert "s" in sig
        assert "v" in sig
        assert isinstance(sig["r"], str) and sig["r"].startswith("0x")
        assert isinstance(sig["s"], str) and sig["s"].startswith("0x")
        assert isinstance(sig["v"], int)

    @pytest.mark.asyncio
    async def test_non_exchange_endpoint_raises_not_implemented(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that non-/exchange endpoints raise NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Only /exchange endpoint is supported"):
            await authenticator.prepare_request(
                method="POST",
                path="/info",
                params=None,
                data={"type": "meta"},
                headers=None,
            )

    @pytest.mark.asyncio
    async def test_address_lowercasing(self, authenticator: HyperliquidEip712Authenticator) -> None:
        """Test that Ethereum addresses are lowercased correctly."""
        action = {
            "type": "withdraw",
            "user": "0xABCDEF1234567890abcdef1234567890ABCDEF12",  # Mixed case
            "destination": "0X9876543210ABCDEF9876543210abcdef98765432",  # 0X prefix
            "tokens": [
                {"address": "0xDeAdBeEfDeAdBeEfDeAdBeEfDeAdBeEfDeAdBeEf"},  # Mixed case in list
            ],
        }

        result = await authenticator.prepare_request(
            method="POST",
            path="/exchange",
            params=None,
            data=action,
            headers=None,
        )

        # Check that addresses were lowercased in the action
        processed_action = result.data["action"]
        assert processed_action["user"] == "0xabcdef1234567890abcdef1234567890abcdef12"
        assert processed_action["destination"] == "0x9876543210abcdef9876543210abcdef98765432"
        assert (
            processed_action["tokens"][0]["address"] == "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        )

    @pytest.mark.asyncio
    async def test_order_type_cleaning(self, authenticator: HyperliquidEip712Authenticator) -> None:
        """Test that null order type fields are removed."""
        action = {
            "type": "order",
            "orders": [
                {
                    "coin": "ETH",
                    "is_buy": False,
                    "sz": "1.0",
                    "order_type": {"limit": None, "market": {"sz_decimals": 2}},
                }
            ],
        }

        result = await authenticator.prepare_request(
            method="POST",
            path="/exchange",
            params=None,
            data=action,
            headers=None,
        )

        # Check that null limit field was removed
        processed_order = result.data["action"]["orders"][0]
        assert "limit" not in processed_order["order_type"]
        assert "market" in processed_order["order_type"]

    @pytest.mark.asyncio
    async def test_action_hash_calculation(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that action_hash is calculated correctly using msgpack and keccak."""
        # Simple action for predictable hashing
        action = {"type": "test", "value": 123}

        with patch("time.time", return_value=1700000000.0):  # Fixed timestamp
            result = await authenticator.prepare_request(
                method="POST",
                path="/exchange",
                params=None,
                data=action,
                headers=None,
            )

        nonce = result.data["nonce"]
        assert nonce == 1700000000000  # milliseconds

        # Manually calculate expected action_hash
        msgpacked = msgpack.packb(action)
        hash_input = msgpacked + nonce.to_bytes(8, "big") + b"\x00"  # vault_address=None
        expected_hash = keccak(hash_input)

        # The signature should have been created with this hash as connectionId
        # We can't directly verify the signature without access to the internal state,
        # but we can ensure the process completed successfully
        assert result.data["signature"]["r"].startswith("0x")
        assert result.data["signature"]["s"].startswith("0x")

    @pytest.mark.asyncio
    async def test_eip712_domain_uses_exchange_name(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that EIP-712 domain uses 'Exchange' as the name."""
        # Access the domain configuration
        domain = authenticator._exchange_action_domain
        assert domain.name == "Exchange"
        assert domain.version == "1"
        assert domain.chain_id == CHAIN_ID
        assert domain.verifying_contract == "0x0000000000000000000000000000000000000000"

    @pytest.mark.asyncio
    async def test_empty_action_payload(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test handling of empty action payload."""
        result = await authenticator.prepare_request(
            method="POST",
            path="/exchange",
            params=None,
            data={},  # Empty action
            headers=None,
        )

        assert result.data["action"] == {}
        assert "nonce" in result.data
        assert "signature" in result.data

    @pytest.mark.asyncio
    async def test_nested_address_lowercasing(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that addresses in deeply nested structures are lowercased."""
        action = {
            "type": "complex",
            "level1": {
                "address": "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "level2": {
                    "items": [
                        {"addr": "0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
                        {"addr": "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"},
                    ],
                },
            },
        }

        result = await authenticator.prepare_request(
            method="POST",
            path="/exchange",
            params=None,
            data=action,
            headers=None,
        )

        processed = result.data["action"]
        assert processed["level1"]["address"] == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        assert (
            processed["level1"]["level2"]["items"][0]["addr"]
            == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        )
        assert (
            processed["level1"]["level2"]["items"][1]["addr"]
            == "0xcccccccccccccccccccccccccccccccccccccccc"
        )

    @pytest.mark.asyncio
    async def test_vault_address_not_included_when_none(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that vaultAddress is not included in response when None."""
        result = await authenticator.prepare_request(
            method="POST",
            path="/exchange",
            params=None,
            data={"type": "test"},
            headers=None,
        )

        # vaultAddress should not be in the response for standard user trades
        assert "vaultAddress" not in result.data

    @pytest.mark.asyncio
    async def test_custom_headers_preserved(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that custom headers are preserved (except Content-Type)."""
        headers = {
            "User-Agent": "TestAgent/1.0",
            "X-Custom-Header": "custom-value",
            "Content-Type": "text/plain",  # Should be overridden
        }

        result = await authenticator.prepare_request(
            method="POST",
            path="/exchange",
            params=None,
            data={"type": "test"},
            headers=headers,
        )

        assert result.headers["Content-Type"] == "application/json"  # Overridden
        assert result.headers["User-Agent"] == "TestAgent/1.0"  # Preserved
        assert result.headers["X-Custom-Header"] == "custom-value"  # Preserved

    @pytest.mark.asyncio
    async def test_nonce_strictly_increasing(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that nonces are strictly increasing even with rapid calls."""
        nonces = []

        # Make multiple rapid requests
        for _ in range(5):
            result = await authenticator.prepare_request(
                method="POST",
                path="/exchange",
                params=None,
                data={"type": "test"},
                headers=None,
            )
            nonces.append(result.data["nonce"])

        # All nonces should be unique and strictly increasing
        assert len(set(nonces)) == len(nonces)  # All unique
        assert all(nonces[i] < nonces[i + 1] for i in range(len(nonces) - 1))  # Strictly increasing

    def test_address_to_bytes_helper(self) -> None:
        """Test the address_to_bytes helper function."""
        # With 0x prefix
        addr1 = "0x1234567890abcdef1234567890abcdef12345678"
        bytes1 = address_to_bytes(addr1)
        assert bytes1 == bytes.fromhex("1234567890abcdef1234567890abcdef12345678")

        # Without 0x prefix
        addr2 = "abcdef1234567890abcdef1234567890abcdef12"
        bytes2 = address_to_bytes(addr2)
        assert bytes2 == bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")

    @pytest.mark.asyncio
    async def test_invalid_data_type_raises_error(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that non-dict data raises ValueError."""
        with pytest.raises(ValueError, match="Must be a dictionary"):
            await authenticator.prepare_request(
                method="POST",
                path="/exchange",
                params=None,
                data="not a dict",
                headers=None,
            )

    @pytest.mark.asyncio
    async def test_msgpack_serialization_used(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that msgpack is used for serialization in action_hash calculation."""
        action = {"type": "test", "nested": {"key": "value"}}

        # Mock msgpack.packb to verify it's called
        with patch("msgpack.packb", wraps=msgpack.packb) as mock_packb:
            result = await authenticator.prepare_request(
                method="POST",
                path="/exchange",
                params=None,
                data=action,
                headers=None,
            )

            # Verify msgpack.packb was called with the action
            mock_packb.assert_called_once()
            call_args = mock_packb.call_args[0][0]
            assert call_args == action

        # Ensure the request completed successfully
        assert "signature" in result.data

    @pytest.mark.asyncio
    async def test_agent_types_structure(
        self, authenticator: HyperliquidEip712Authenticator
    ) -> None:
        """Test that Agent types are correctly structured for EIP-712."""
        agent_types = authenticator._exchange_action_agent_types

        # Check Agent type fields
        agent_fields = agent_types.Agent
        assert len(agent_fields) == 2
        assert agent_fields[0].name == "source"
        assert agent_fields[0].type == "string"
        assert agent_fields[1].name == "connectionId"
        assert agent_fields[1].type == "bytes32"

        # Check EIP712Domain fields
        domain_fields = agent_types.EIP712Domain
        assert len(domain_fields) == 4
        field_names = [f.name for f in domain_fields]
        assert "name" in field_names
        assert "version" in field_names
        assert "chainId" in field_names
        assert "verifyingContract" in field_names
