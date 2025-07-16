"""Configuration validation exceptions for domain objects.

These exceptions handle errors during domain object validation,
including infrastructure, trading execution, and security configuration.
"""

from cyberdelta.apis.common import APIError, APIErrorCode


class CachingPolicyError(APIError):
    """Raised when caching policy configuration is invalid."""

    def __init__(
        self,
        policy: str,
        duration: float,
        constraint: str,
        *,
        parameter_name: str | None = None,
    ) -> None:
        """Initialize caching policy error.

        Args:
            policy: The caching policy that failed validation
            duration: The invalid duration value
            constraint: Description of the constraint violation
            parameter_name: Optional parameter name for context
        """
        message = f"Caching policy '{policy}' invalid: {constraint}"
        if parameter_name:
            message = f"Parameter '{parameter_name}': {message}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "policy": policy,
                "duration": duration,
                "constraint": constraint,
                "parameter_name": parameter_name,
            },
        )


class PerformanceProfileError(APIError):
    """Raised when performance profile configuration is inconsistent."""

    def __init__(
        self,
        profile: str,
        conflicting_setting: str,
        setting_value: str,
        *,
        reason: str | None = None,
    ) -> None:
        """Initialize performance profile error.

        Args:
            profile: The performance profile
            conflicting_setting: The setting that conflicts
            setting_value: The conflicting value
            reason: Optional detailed reason
        """
        base_message = f"Performance profile '{profile}' incompatible with"
        message = f"{base_message} {conflicting_setting}='{setting_value}'"
        if reason:
            message = f"{message}: {reason}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "performance_profile": profile,
                "conflicting_setting": conflicting_setting,
                "setting_value": setting_value,
                "reason": reason,
            },
        )


class ThreatModelError(APIError):
    """Raised when threat model security configuration is invalid."""

    def __init__(
        self,
        threat_model: str,
        requirement: str,
        current_value: str | None = None,
    ) -> None:
        """Initialize threat model error.

        Args:
            threat_model: The threat model that failed validation
            requirement: Description of the requirement violation
            current_value: The current invalid value
        """
        message = f"Threat model '{threat_model}' requires {requirement}"
        if current_value:
            message = f"{message} (current: {current_value})"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "threat_model": threat_model,
                "requirement": requirement,
                "current_value": current_value,
            },
        )


class TradingExecutionError(APIError):
    """Raised when trading execution configuration is unsafe."""

    def __init__(
        self,
        liquidity_requirement: str,
        margin_policy: str,
        *,
        risk_description: str | None = None,
    ) -> None:
        """Initialize trading execution error.

        Args:
            liquidity_requirement: The liquidity requirement
            margin_policy: The margin policy
            risk_description: Optional description of the risk
        """
        message = f"Unsafe trading configuration: {liquidity_requirement} with {margin_policy}"
        if risk_description:
            message = f"{message} - {risk_description}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "liquidity_requirement": liquidity_requirement,
                "margin_policy": margin_policy,
                "risk_description": risk_description,
            },
        )


class LeverageRiskError(APIError):
    """Raised when leverage configuration creates excessive risk."""

    def __init__(
        self,
        leverage_limit: int,
        automation_policy: str,
        *,
        risk_threshold: int | None = None,
    ) -> None:
        """Initialize leverage risk error.

        Args:
            leverage_limit: The leverage limit
            automation_policy: The automation policy
            risk_threshold: Optional risk threshold that was exceeded
        """
        message = (
            f"High leverage ({leverage_limit}x) with {automation_policy} creates excessive risk"
        )
        if risk_threshold:
            message = f"{message} (threshold: {risk_threshold}x)"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "leverage_limit": leverage_limit,
                "automation_policy": automation_policy,
                "risk_threshold": risk_threshold,
            },
        )


class NetworkEnvironmentError(APIError):
    """Raised when network environment configuration is inconsistent."""

    def __init__(
        self,
        chain_id: str,
        endpoint_type: str,
        endpoint_url: str,
        *,
        expected_environment: str | None = None,
    ) -> None:
        """Initialize network environment error.

        Args:
            chain_id: The chain ID
            endpoint_type: Type of endpoint (API, WebSocket, etc.)
            endpoint_url: The invalid endpoint URL
            expected_environment: Expected environment type
        """
        message = f"Chain '{chain_id}' cannot use {endpoint_type} endpoint: {endpoint_url}"
        if expected_environment:
            message = f"{message} (expected: {expected_environment})"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "chain_id": chain_id,
                "endpoint_type": endpoint_type,
                "endpoint_url": endpoint_url,
                "expected_environment": expected_environment,
            },
        )


class ValidationRangeError(APIError):
    """Raised when validation range constraints are invalid."""

    def __init__(
        self,
        field_name: str,
        min_value: float | str,
        max_value: float | str,
        *,
        constraint_type: str = "range",
    ) -> None:
        """Initialize validation range error.

        Args:
            field_name: Name of the field with invalid range
            min_value: The minimum value
            max_value: The maximum value
            constraint_type: Type of constraint (range, length, etc.)
        """
        message = (
            f"Invalid {constraint_type} for '{field_name}': min ({min_value}) > max ({max_value})"
        )

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "field_name": field_name,
                "min_value": str(min_value),
                "max_value": str(max_value),
                "constraint_type": constraint_type,
            },
        )
