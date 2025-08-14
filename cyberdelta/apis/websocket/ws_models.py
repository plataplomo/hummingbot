"""Base Pydantic Models for WebSocket Messages.

This module defines base models for WebSocket communication that can be
extended by exchange-specific implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.apis.websocket.ws_exceptions import (
    AuthenticationErrorMismatchError,
    SuccessErrorMismatchError,
)
from cyberdelta.apis.websocket.websocket_states import FieldPresenceState


class BaseWebSocketMessage(BaseModel, ABC):
    """Base model for all WebSocket messages.

    Provides common fields and configuration for WebSocket messages
    across all exchanges.
    """

    model_config = ConfigDict(
        frozen=True,
        populate_by_name=True,
        extra="forbid",
        str_strip_whitespace=True,
    )

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp when the message was created/received",
    )

    @classmethod
    def from_raw(cls, data: dict[str, Any]) -> BaseWebSocketMessage:
        """Factory method for creating from raw data.

        Args:
            data: Raw dictionary data to validate.

        Returns:
            Validated instance of the message.

        """
        return cls.model_validate(data)

    def to_wire_format(self) -> dict[str, Any]:
        """Convert to wire format for sending.

        Default implementation returns JSON-serializable dict.
        Override for custom serialization.

        Returns:
            Dictionary ready for JSON serialization.

        """
        return self.model_dump(mode="json", exclude={"timestamp"})


class BaseSubscriptionRequest(BaseWebSocketMessage):
    """Base model for subscription requests."""

    method: Literal["subscribe", "unsubscribe"] = Field(description="Subscription method")
    id: str | None = Field(
        default=None,
        description="Optional request ID for correlation",
    )

    @abstractmethod
    def to_wire_format(self) -> dict[str, Any]:
        """Convert to exchange-specific wire format.

        Must be implemented by exchange-specific classes.

        Returns:
            Exchange-specific subscription payload.

        """
        ...


class BaseSubscriptionResponse(BaseWebSocketMessage):
    """Base model for subscription responses."""

    id: str | None = Field(
        default=None,
        description="Request ID for correlation",
    )
    success: bool = Field(
        description="Whether the subscription was successful",
    )
    error: str | None = Field(
        default=None,
        description="Error message if subscription failed",
    )
    subscribed_topics: list[str] = Field(
        default_factory=list,
        description="List of successfully subscribed topics",
    )

    @field_validator("error")
    @classmethod
    def validate_error_consistency(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Ensure error is None when success is True.

        Args:
            v: Error message value to validate
            info: Validation context with other field values

        Returns:
            Validated error message or None

        Raises:
            SuccessErrorMismatchError: If error/success fields are inconsistent
        """
        if info.data.get("success") and v is not None:
            raise SuccessErrorMismatchError(
                success=True,
                has_error=True,
            )
        if not info.data.get("success") and v is None:
            raise SuccessErrorMismatchError(
                success=False,
                has_error=False,
            )
        return v


class BaseErrorResponse(BaseWebSocketMessage):
    """Base model for error responses."""

    error_code: str = Field(
        description="Error code for categorization",
    )
    error_message: str = Field(
        description="Human-readable error message",
    )
    error_details: dict[str, Any] | None = Field(
        default=None,
        description="Additional error context",
    )
    request_id: str | None = Field(
        default=None,
        description="ID of the request that caused the error",
    )


class BaseHeartbeat(BaseWebSocketMessage):
    """Base model for heartbeat/ping-pong messages."""

    type: Literal["ping", "pong"] = Field(
        description="Heartbeat message type",
    )
    sequence: int | None = Field(
        default=None,
        description="Optional sequence number for ordering",
    )
    payload: str | None = Field(
        default=None,
        description="Optional payload data",
    )


class BaseConnectionStatus(BaseWebSocketMessage):
    """Base model for connection status messages."""

    status: Literal["connected", "disconnected", "reconnecting", "error"] = Field(
        description="Connection status",
    )
    message: str | None = Field(
        default=None,
        description="Optional status message",
    )
    reconnect_in: float | None = Field(
        default=None,
        description="Seconds until reconnection attempt",
        ge=0,
    )


class BaseRateLimitNotification(BaseWebSocketMessage):
    """Base model for rate limit notifications."""

    limit_type: Literal["message", "connection", "subscription"] = Field(
        description="Type of rate limit",
    )
    current_rate: float = Field(
        description="Current message rate",
        ge=0,
    )
    limit_rate: float = Field(
        description="Maximum allowed rate",
        ge=0,
    )
    reset_in: float | None = Field(
        default=None,
        description="Seconds until rate limit resets",
        ge=0,
    )


class BaseAuthenticationRequest(BaseWebSocketMessage):
    """Base model for authentication requests."""

    method: Literal["authenticate"] = Field(
        default="authenticate",
        description="Authentication method",
    )

    @abstractmethod
    def to_wire_format(self) -> dict[str, Any]:
        """Convert to exchange-specific authentication format.

        Must be implemented by exchange-specific classes.

        Returns:
            Exchange-specific authentication payload.

        """
        ...


class BaseAuthenticationResponse(BaseWebSocketMessage):
    """Base model for authentication responses."""

    authenticated: bool = Field(
        description="Whether authentication was successful",
    )
    user_id: str | None = Field(
        default=None,
        description="User identifier if authenticated",
    )
    permissions: list[str] = Field(
        default_factory=list,
        description="List of granted permissions",
    )
    expires_at: datetime | None = Field(
        default=None,
        description="When the authentication expires",
    )
    error: str | None = Field(
        default=None,
        description="Error message if authentication failed",
    )

    @field_validator("error")
    @classmethod
    def validate_auth_consistency(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Ensure error is consistent with authentication status.

        Args:
            v: Error message value to validate
            info: Validation context with other field values

        Returns:
            Validated error message or None

        Raises:
            AuthenticationErrorMismatchError: If error is present when authenticated is True
        """
        if info.data.get("authenticated") and v is not None:
            raise AuthenticationErrorMismatchError(
                authenticated=True,
                auth_error=v,
            )
        return v
