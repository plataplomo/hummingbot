"""WebSocket Connection State Tracker.

This module provides proper tracking of WebSocket connection state,
including authentication status, subscriptions, and connection health.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.apis.websocket.security.channel_classifier import ChannelClassifier
from cyberdelta.enums import ExchangeName


@dataclass
class ChannelSubscription:
    """Represents a WebSocket channel subscription."""

    channel: str
    subscribed_at: datetime
    is_authenticated: bool
    requires_auth: bool
    last_message_at: datetime | None = None
    message_count: int = 0

    @classmethod
    def create(cls, channel: str, is_authenticated: bool = False) -> ChannelSubscription:
        """Create a new channel subscription.

        Args:
            channel: Channel/topic name
            is_authenticated: Whether subscription used authentication

        Returns:
            New ChannelSubscription instance
        """
        return cls(
            channel=channel,
            subscribed_at=datetime.now(UTC),
            is_authenticated=is_authenticated,
            requires_auth=ChannelClassifier.requires_authentication(channel),
            last_message_at=None,
            message_count=0,
        )

    def record_message(self) -> None:
        """Record that a message was received on this channel."""
        self.last_message_at = datetime.now(UTC)
        self.message_count += 1


@dataclass
class WebSocketConnectionState:
    """Tracks the state of a WebSocket connection.

    This class maintains comprehensive state about a WebSocket connection,
    including authentication status, active subscriptions, and connection health.
    """

    connection_id: str
    exchange: ExchangeName
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # Authentication state - tracks which channels are authenticated
    # NOT whether the connection itself is authenticated
    authenticated_channels: set[str] = Field(default_factory=set)

    # Subscription tracking
    subscriptions: dict[str, ChannelSubscription] = Field(default_factory=dict)

    # Connection health
    is_connected: bool = False
    connected_at: datetime | None = None
    disconnected_at: datetime | None = None
    reconnect_count: int = 0
    last_heartbeat: datetime | None = None

    # Message statistics
    total_messages_received: int = 0
    total_messages_sent: int = 0
    last_message_received: datetime | None = None

    # Error tracking
    error_count: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None

    def mark_channel_authenticated(self, channel: str) -> None:
        """Mark a specific channel as authenticated.

        Args:
            channel: Channel that was authenticated
        """
        self.authenticated_channels.add(channel)

    def mark_connected(self) -> None:
        """Mark the connection as connected."""
        self.is_connected = True
        self.connected_at = datetime.now(UTC)
        self.disconnected_at = None

    def mark_disconnected(self) -> None:
        """Mark the connection as disconnected."""
        self.is_connected = False
        self.disconnected_at = datetime.now(UTC)

    def mark_reconnected(self) -> None:
        """Mark the connection as reconnected."""
        self.reconnect_count += 1
        self.mark_connected()

    def add_subscription(self, channel: str, is_authenticated: bool = False) -> ChannelSubscription:
        """Add a channel subscription.

        Args:
            channel: Channel/topic name
            is_authenticated: Whether subscription used authentication

        Returns:
            The created or existing subscription
        """
        if channel not in self.subscriptions:
            self.subscriptions[channel] = ChannelSubscription.create(channel, is_authenticated)
            if is_authenticated:
                self.authenticated_channels.add(channel)
        return self.subscriptions[channel]

    def remove_subscription(self, channel: str) -> bool:
        """Remove a channel subscription.

        Args:
            channel: Channel/topic name

        Returns:
            True if subscription was removed, False if not found
        """
        if channel in self.subscriptions:
            del self.subscriptions[channel]
            return True
        return False

    def is_channel_authenticated(self, channel: str) -> bool:
        """Check if a specific channel is authenticated.

        Args:
            channel: Channel/topic name

        Returns:
            True if channel is authenticated
        """
        # Check if this specific channel is authenticated
        return channel in self.authenticated_channels

    def record_message_received(self, channel: str | None = None) -> None:
        """Record that a message was received.

        Args:
            channel: Optional channel the message was received on
        """
        self.total_messages_received += 1
        self.last_message_received = datetime.now(UTC)

        if channel and channel in self.subscriptions:
            self.subscriptions[channel].record_message()

    def record_message_sent(self) -> None:
        """Record that a message was sent."""
        self.total_messages_sent += 1

    def record_heartbeat(self) -> None:
        """Record a heartbeat."""
        self.last_heartbeat = datetime.now(UTC)

    def record_error(self, error: str) -> None:
        """Record an error.

        Args:
            error: Error message or description
        """
        self.error_count += 1
        self.last_error = error
        self.last_error_at = datetime.now(UTC)

    def get_subscription_count(self) -> int:
        """Get the number of active subscriptions.

        Returns:
            Number of active subscriptions
        """
        return len(self.subscriptions)

    def get_authenticated_subscription_count(self) -> int:
        """Get the number of authenticated subscriptions.

        Returns:
            Number of authenticated subscriptions
        """
        return sum(1 for sub in self.subscriptions.values() if sub.is_authenticated)

    def get_connection_duration(self) -> float | None:
        """Get connection duration in seconds.

        Returns:
            Duration in seconds if connected, None otherwise
        """
        if not self.connected_at:
            return None

        end_time = self.disconnected_at or datetime.now(UTC)
        return (end_time - self.connected_at).total_seconds()

    def is_healthy(self, max_heartbeat_age_seconds: int = 60) -> bool:
        """Check if connection is healthy.

        Args:
            max_heartbeat_age_seconds: Maximum age of heartbeat in seconds

        Returns:
            True if connection is healthy
        """
        if not self.is_connected:
            return False

        if self.last_heartbeat:
            heartbeat_age = (datetime.now(UTC) - self.last_heartbeat).total_seconds()
            if heartbeat_age > max_heartbeat_age_seconds:
                return False

        return True


@dataclass
class ConnectionStateManager:
    """Manages WebSocket connection states.

    This should be instantiated per router, not globally.
    """

    connections: dict[str, WebSocketConnectionState] = Field(default_factory=dict)

    def create_connection(
        self, connection_id: str, exchange: ExchangeName
    ) -> WebSocketConnectionState:
        """Create a new connection state.

        Args:
            connection_id: Unique connection identifier
            exchange: Exchange this connection is for

        Returns:
            New connection state
        """
        state = WebSocketConnectionState(connection_id=connection_id, exchange=exchange)
        self.connections[connection_id] = state
        return state

    def get_connection(self, connection_id: str) -> WebSocketConnectionState | None:
        """Get connection state by ID.

        Args:
            connection_id: Connection identifier

        Returns:
            Connection state or None if not found
        """
        return self.connections.get(connection_id)

    def remove_connection(self, connection_id: str) -> bool:
        """Remove a connection state.

        Args:
            connection_id: Connection identifier

        Returns:
            True if removed, False if not found
        """
        if connection_id in self.connections:
            del self.connections[connection_id]
            return True
        return False

    def get_all_connections(self) -> list[WebSocketConnectionState]:
        """Get all connection states.

        Returns:
            List of all connection states
        """
        return list(self.connections.values())

    def get_healthy_connections(self) -> list[WebSocketConnectionState]:
        """Get all healthy connections.

        Returns:
            List of healthy connections
        """
        return [conn for conn in self.connections.values() if conn.is_healthy()]

    def get_authenticated_channels_count(self) -> int:
        """Get the total number of authenticated channels across all connections.

        Returns:
            Count of authenticated channels
        """
        total = 0
        for conn in self.connections.values():
            total += len(conn.authenticated_channels)
        return total
