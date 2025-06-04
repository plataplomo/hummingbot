"""Unit tests for Hyperliquid EIP-712 models.
"""

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_eip712_models import (
    EIP712TypeField,
    HyperliquidAgentDomainData,
    HyperliquidAgentTypes,
)


class TestEIP712TypeField:
    """Test suite for EIP712TypeField model."""

    def test_valid_type_field(self) -> None:
        """Test creating a valid EIP712TypeField."""
        field = EIP712TypeField(name="source", type="string")
        assert field.name == "source"
        assert field.type == "string"

    def test_frozen_model(self) -> None:
        """Test that the model is frozen and cannot be modified."""
        field = EIP712TypeField(name="test", type="uint256")
        with pytest.raises(ValidationError):
            field.name = "changed"

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are not allowed."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            EIP712TypeField(name="test", type="string", extra="not_allowed")  # type: ignore[call-arg] # Testing that extra fields raise error


class TestHyperliquidAgentDomainData:
    """Test suite for HyperliquidAgentDomainData model."""

    def test_valid_domain_data(self) -> None:
        """Test creating valid domain data."""
        domain = HyperliquidAgentDomainData(
            name="Exchange",
            version="1",
            chainId=1337,
            verifyingContract="0x0000000000000000000000000000000000000000",
        )
        assert domain.name == "Exchange"
        assert domain.version == "1"
        assert domain.chain_id == 1337
        assert domain.verifying_contract == "0x0000000000000000000000000000000000000000"

    def test_alias_fields(self) -> None:
        """Test that alias fields work correctly."""
        # Can create with aliases
        domain = HyperliquidAgentDomainData(
            name="Test",
            version="2",
            chainId=42,
            verifyingContract="0x1234567890123456789012345678901234567890",
        )
        assert domain.chain_id == 42
        assert domain.verifying_contract == "0x1234567890123456789012345678901234567890"

        # Can also create with actual field names
        domain2 = HyperliquidAgentDomainData(
            name="Test",
            version="2",
            chainId=42,
            verifyingContract="0x1234567890123456789012345678901234567890",
        )
        assert domain2.chain_id == 42
        assert domain2.verifying_contract == "0x1234567890123456789012345678901234567890"

    def test_model_dump_by_alias(self) -> None:
        """Test that model_dump with by_alias returns aliased field names."""
        domain = HyperliquidAgentDomainData(
            name="Exchange",
            version="1",
            chainId=1337,
            verifyingContract="0x0000000000000000000000000000000000000000",
        )
        dumped = domain.model_dump(by_alias=True)
        assert dumped["chainId"] == 1337
        assert dumped["verifyingContract"] == "0x0000000000000000000000000000000000000000"
        assert "chain_id" not in dumped
        assert "verifying_contract" not in dumped

    def test_frozen_and_extra_forbidden(self) -> None:
        """Test that the model is frozen and extra fields are forbidden."""
        domain = HyperliquidAgentDomainData(
            name="Exchange",
            version="1",
            chainId=1337,
            verifyingContract="0x0000000000000000000000000000000000000000",
        )

        # Test frozen
        with pytest.raises(ValidationError):
            domain.name = "Changed"

        # Test extra forbidden
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            HyperliquidAgentDomainData(
                name="Exchange",
                version="1",
                chainId=1337,
                verifyingContract="0x0000000000000000000000000000000000000000",
                extra="field",  # type: ignore[call-arg] # Testing that extra fields raise error
            )


class TestHyperliquidAgentTypes:
    """Test suite for HyperliquidAgentTypes model."""

    def test_valid_agent_types(self) -> None:
        """Test creating valid agent types."""
        types = HyperliquidAgentTypes(
            EIP712Domain=[
                EIP712TypeField(name="name", type="string"),
                EIP712TypeField(name="version", type="string"),
                EIP712TypeField(name="chainId", type="uint256"),
                EIP712TypeField(name="verifyingContract", type="address"),
            ],
            Agent=[
                EIP712TypeField(name="source", type="string"),
                EIP712TypeField(name="connectionId", type="bytes32"),
            ],
        )

        assert len(types.EIP712Domain) == 4
        assert len(types.Agent) == 2
        assert types.Agent[0].name == "source"
        assert types.Agent[1].type == "bytes32"

    def test_model_dump_structure(self) -> None:
        """Test that model_dump produces the correct structure for EIP-712."""
        types = HyperliquidAgentTypes(
            EIP712Domain=[
                EIP712TypeField(name="name", type="string"),
                EIP712TypeField(name="version", type="string"),
                EIP712TypeField(name="chainId", type="uint256"),
                EIP712TypeField(name="verifyingContract", type="address"),
            ],
            Agent=[
                EIP712TypeField(name="source", type="string"),
                EIP712TypeField(name="connectionId", type="bytes32"),
            ],
        )

        dumped = types.model_dump(by_alias=True)

        # Check structure matches EIP-712 requirements
        assert "EIP712Domain" in dumped
        assert "Agent" in dumped
        assert isinstance(dumped["EIP712Domain"], list)
        assert isinstance(dumped["Agent"], list)

        # Check domain fields
        domain_fields: list[dict[str, str]] = dumped["EIP712Domain"]  # pyright: ignore[reportUnknownVariableType]
        assert len(domain_fields) == 4
        assert domain_fields[0]["name"] == "name"
        assert domain_fields[0]["type"] == "string"

        # Check agent fields
        agent_fields: list[dict[str, str]] = dumped["Agent"]  # pyright: ignore[reportUnknownVariableType]
        assert len(agent_fields) == 2
        assert agent_fields[0]["name"] == "source"
        assert agent_fields[0]["type"] == "string"
        assert agent_fields[1]["name"] == "connectionId"
        assert agent_fields[1]["type"] == "bytes32"

    def test_frozen_and_extra_forbidden(self) -> None:
        """Test that the model is frozen and extra fields are forbidden."""
        types = HyperliquidAgentTypes(
            EIP712Domain=[
                EIP712TypeField(name="name", type="string"),
            ],
            Agent=[
                EIP712TypeField(name="source", type="string"),
            ],
        )

        # Test frozen
        with pytest.raises(ValidationError):
            types.Agent = []

        # Test extra forbidden
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            HyperliquidAgentTypes(
                EIP712Domain=[],
                Agent=[],
                ExtraType=[],  # type: ignore[call-arg] # Testing that extra fields raise error
            )
