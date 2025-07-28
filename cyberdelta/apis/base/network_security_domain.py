"""Network environment and security domain objects with Pydantic validation.

This module provides comprehensive domain objects to replace critical boolean traps
in network environment and security configuration, preventing funds loss and
security vulnerabilities.
"""

from __future__ import annotations

import warnings
from enum import Enum

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cyberdelta.apis.exceptions.configuration_validation import (
    NetworkEnvironmentError,
    ThreatModelError,
)
from cyberdelta.enums.environment import EnvironmentType


class ChainId(Enum):
    """Blockchain chain ID for network environment.

    Replaces the boolean `is_mainnet_environment` parameter with explicit chain IDs.
    """

    MAINNET = 1337
    """Real funds, real trades - production environment."""

    TESTNET = 421614
    """Test funds, safe experimentation - development environment."""

    @property
    def is_production(self) -> bool:
        """Check if this is a production environment."""
        return self == ChainId.MAINNET

    @property
    def domain_name(self) -> str:
        """Get the domain name for EIP-712 signing."""
        return "Exchange" if self.is_production else "Exchange_Test"


class NetworkEnvironment(BaseModel):
    """Network environment configuration with validation.

    Replaces the dangerous boolean `is_mainnet_environment` with type-safe
    environment configuration that prevents funds loss through misconfiguration.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    chain_id: ChainId = Field(description="Blockchain chain ID for network environment")

    api_endpoint: HttpUrl = Field(description="API endpoint URL for the network")

    websocket_endpoint: AnyUrl = Field(description="WebSocket endpoint URL for the network")

    @field_validator("api_endpoint", "websocket_endpoint")
    @classmethod
    def validate_endpoints_match_chain(
        cls, v: HttpUrl | AnyUrl, info: ValidationInfo
    ) -> HttpUrl | AnyUrl:
        """Ensure endpoints match the chain environment.

        This validation prevents catastrophic mistakes like using mainnet
        endpoints with testnet configuration or vice versa.

        Args:
            v: The endpoint URL to validate
            info: Validation context containing chain_id

        Returns:
            The validated endpoint URL if it matches the chain environment

        Raises:
            NetworkEnvironmentError: If endpoint doesn't match chain environment
        """
        if info.data and "chain_id" in info.data:
            chain_id = info.data["chain_id"]
            endpoint_str = str(v).lower()

            # Validate mainnet endpoints
            if chain_id == ChainId.MAINNET:
                if "testnet" in endpoint_str or "test" in endpoint_str:
                    raise NetworkEnvironmentError(
                        chain_id=str(chain_id.value),
                        endpoint_type="API/WebSocket",
                        endpoint_url=str(v),
                        expected_environment="mainnet",
                    )

            # Validate testnet endpoints
            elif (
                chain_id == ChainId.TESTNET
                and "testnet" not in endpoint_str
                and "test" not in endpoint_str
            ):
                raise NetworkEnvironmentError(
                    chain_id=str(chain_id.value),
                    endpoint_type="API/WebSocket",
                    endpoint_url=str(v),
                    expected_environment="testnet",
                )

        return v

    @model_validator(mode="after")
    def validate_environment_consistency(self) -> NetworkEnvironment:
        """Validate overall environment consistency.

        Returns:
            The validated NetworkEnvironment instance

        Raises:
            NetworkEnvironmentError: If API and WebSocket endpoints are from different environments
        """
        # Additional cross-field validation
        api_str = str(self.api_endpoint).lower()
        ws_str = str(self.websocket_endpoint).lower()

        # Both endpoints should be from the same environment
        api_is_test = "testnet" in api_str or "test" in api_str
        ws_is_test = "testnet" in ws_str or "test" in ws_str

        if api_is_test != ws_is_test:
            raise NetworkEnvironmentError(
                chain_id=str(self.chain_id.value),
                endpoint_type="mixed",
                endpoint_url=f"API: {self.api_endpoint}, WebSocket: {self.websocket_endpoint}",
                expected_environment="consistent environment",
            )

        return self

    def is_mainnet(self) -> bool:
        """Check if this is a mainnet environment.

        Returns:
            True if this is a mainnet environment, False otherwise
        """
        return self.chain_id == ChainId.MAINNET


class NetworkEnvironmentFactory:
    """Factory for creating network environments from configuration.

    This factory creates validated network environments using configuration
    data instead of hardcoded values, making it exchange-agnostic.
    """

    @staticmethod
    def from_config(
        environment_type: EnvironmentType,
        chain_id: int,
        api_endpoint: str,
        websocket_endpoint: str,
    ) -> NetworkEnvironment:
        """Create network environment from configuration parameters.

        Args:
            environment_type: Production or testnet environment
            chain_id: Blockchain chain ID from config
            api_endpoint: API endpoint URL from config
            websocket_endpoint: WebSocket endpoint URL from config

        Returns:
            Configured NetworkEnvironment instance

        Raises:
            NetworkEnvironmentError: If chain_id doesn't match environment_type expectation
        """
        # Map environment type to ChainId enum for validation
        chain_id_enum = ChainId.MAINNET if environment_type.is_production else ChainId.TESTNET

        # Validate that config chain_id matches environment expectation
        if chain_id != chain_id_enum.value:
            raise NetworkEnvironmentError(
                chain_id=str(chain_id),
                endpoint_type="chain_id",
                endpoint_url=f"Expected {chain_id_enum.value} for {environment_type.value}",
                expected_environment=environment_type.value,
            )

        return NetworkEnvironment(
            chain_id=chain_id_enum,
            api_endpoint=HttpUrl(api_endpoint),
            websocket_endpoint=AnyUrl(websocket_endpoint),
        )

    @staticmethod
    def mainnet(
        api_endpoint: str, websocket_endpoint: str, chain_id: int = 1337
    ) -> NetworkEnvironment:
        """Create mainnet environment from config - real funds at risk.

        WARNING: This environment uses real funds and real trades.

        Args:
            api_endpoint: API endpoint URL for mainnet
            websocket_endpoint: WebSocket endpoint URL for mainnet
            chain_id: Blockchain chain ID (default: 1337 for mainnet)

        Returns:
            Configured NetworkEnvironment instance for mainnet
        """
        return NetworkEnvironmentFactory.from_config(
            environment_type=EnvironmentType.MAINNET,
            chain_id=chain_id,
            api_endpoint=api_endpoint,
            websocket_endpoint=websocket_endpoint,
        )

    @staticmethod
    def testnet(
        api_endpoint: str, websocket_endpoint: str, chain_id: int = 421614
    ) -> NetworkEnvironment:
        """Create testnet environment from config - safe for development.

        Args:
            api_endpoint: API endpoint URL for testnet
            websocket_endpoint: WebSocket endpoint URL for testnet
            chain_id: Blockchain chain ID (default: 421614 for testnet)

        Returns:
            Configured NetworkEnvironment instance for testnet
        """
        return NetworkEnvironmentFactory.from_config(
            environment_type=EnvironmentType.TESTNET,
            chain_id=chain_id,
            api_endpoint=api_endpoint,
            websocket_endpoint=websocket_endpoint,
        )


class ThreatModel(Enum):
    """Threat model for security configuration.

    Replaces boolean security flags with explicit threat models.
    """

    DEVELOPMENT = "development"
    """Minimal checks, fast iteration - development only."""

    STANDARD = "standard"
    """Production baseline security - default for production."""

    PARANOID = "paranoid"
    """Maximum security, performance cost - high-value operations."""

    AUDITED = "audited"
    """Compliance-grade validation - regulatory requirements."""


class InputValidationLevel(Enum):
    """Input validation level for security policies.

    Replaces boolean validation flags with explicit validation levels.
    """

    MINIMAL = "minimal"
    """Type checking only - fastest processing."""

    STANDARD = "standard"
    """Size and structure validation - balanced security."""

    STRICT = "strict"
    """Content filtering and limits - comprehensive validation."""

    COMPREHENSIVE = "comprehensive"
    """All validation plus threat detection - maximum security."""


class DosProtectionLevel(Enum):
    """DoS protection level for security policies.

    Replaces boolean DoS protection flags with explicit protection levels.
    """

    DISABLED = "disabled"
    """No DoS protection - development only."""

    BASIC = "basic"
    """Size limits only - minimal protection."""

    ENABLED = "enabled"
    """Size, depth, and rate limits - standard protection."""

    AGGRESSIVE = "aggressive"
    """All protections plus anomaly detection - maximum protection."""


class SecurityPolicy(BaseModel):
    """Security policy configuration with validation.

    Replaces multiple boolean security flags with structured, validated
    security policies that prevent configuration errors.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    threat_model: ThreatModel = Field(
        default=ThreatModel.STANDARD, description="Threat model for security configuration"
    )

    input_validation: InputValidationLevel = Field(
        default=InputValidationLevel.STRICT,
        description="Input validation level for data processing",
    )

    dos_protection: DosProtectionLevel = Field(
        default=DosProtectionLevel.ENABLED, description="DoS protection level for system security"
    )

    @model_validator(mode="after")
    def validate_security_consistency(self) -> SecurityPolicy:
        """Ensure security settings provide adequate protection.

        Returns:
            The validated SecurityPolicy instance

        Raises:
            ThreatModelError: If security settings are inconsistent with threat model
        """
        # Paranoid threat model requires comprehensive validation
        if (
            self.threat_model == ThreatModel.PARANOID
            and self.input_validation != InputValidationLevel.COMPREHENSIVE
        ):
            raise ThreatModelError(
                threat_model=self.threat_model.value,
                requirement="COMPREHENSIVE input validation",
                current_value=self.input_validation.value,
            )

        # Audited environments need strong DoS protection
        if (
            self.threat_model == ThreatModel.AUDITED
            and self.dos_protection == DosProtectionLevel.DISABLED
        ):
            raise ThreatModelError(
                threat_model=self.threat_model.value,
                requirement="DoS protection cannot be disabled",
                current_value=self.dos_protection.value,
            )

        # Development can have relaxed security but warn about it
        if self.threat_model == ThreatModel.DEVELOPMENT:
            warnings.warn(
                "DEVELOPMENT threat model should not be used in production",
                UserWarning,
                stacklevel=2,
            )

        # Standard threat model with minimal validation is risky
        if (
            self.threat_model == ThreatModel.STANDARD
            and self.input_validation == InputValidationLevel.MINIMAL
        ):
            warnings.warn(
                "STANDARD threat model with MINIMAL validation may be insufficient",
                UserWarning,
                stacklevel=2,
            )

        return self


class AuthenticationEnvironment(BaseModel):
    """Authentication environment configuration.

    Combines network environment with security policies for complete
    authentication context.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    network: NetworkEnvironment = Field(description="Network environment for authentication")

    security_policy: SecurityPolicy = Field(
        default_factory=SecurityPolicy, description="Security policy for authentication"
    )

    @model_validator(mode="after")
    def validate_authentication_consistency(self) -> AuthenticationEnvironment:
        """Validate authentication environment consistency.

        Returns:
            The validated AuthenticationEnvironment instance

        Raises:
            NetworkEnvironmentError: If mainnet uses development security or other inconsistencies
        """
        # Mainnet should have strong security
        if (
            self.network.chain_id == ChainId.MAINNET
            and self.security_policy.threat_model == ThreatModel.DEVELOPMENT
        ):
            raise NetworkEnvironmentError(
                chain_id=str(self.network.chain_id.value),
                endpoint_type="security policy",
                endpoint_url=self.security_policy.threat_model.value,
                expected_environment="production-grade security",
            )

        # Testnet can have relaxed security but warn
        if (
            self.network.chain_id == ChainId.TESTNET
            and self.security_policy.threat_model == ThreatModel.PARANOID
        ):
            warnings.warn(
                "PARANOID security on testnet may be unnecessarily restrictive",
                UserWarning,
                stacklevel=2,
            )

        return self
