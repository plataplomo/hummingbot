"""
CyberDeltaEngine: Hyperliquid EIP-712 Pydantic Models
----------------------------------------------------

This module defines Pydantic models for EIP-712 type definitions and domain structures
used by the Hyperliquid authenticator. These models provide schema validation and type
safety for the complex nested structures required for EIP-712 Agent signatures.
"""

from pydantic import BaseModel, ConfigDict, Field


class EIP712TypeField(BaseModel):
    """
    Model for individual EIP-712 type field definitions.

    Represents a single field in an EIP-712 type definition, such as
    {"name": "source", "type": "string"} or {"name": "chainId", "type": "uint256"}.
    """

    name: str
    type: str  # e.g., "string", "uint256", "bytes32", "Agent"

    model_config = ConfigDict(extra="forbid", frozen=True)


class EIP712DomainData(BaseModel):
    """
    Model for EIP-712 domain data structure.

    This represents the actual domain values used in EIP-712 signatures,
    not the type definition structure.
    """

    name: str
    version: str
    chain_id: int = Field(alias="chainId")
    verifying_contract: str = Field(alias="verifyingContract")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class EIP712Types(BaseModel):
    """
    Model for EIP-712 type definitions.

    This represents the complete "types" structure required for EIP-712 signatures,
    containing both the EIP712Domain and Agent type definitions.
    """

    EIP712Domain: list[EIP712TypeField]
    Agent: list[EIP712TypeField]

    model_config = ConfigDict(extra="forbid", frozen=True)
