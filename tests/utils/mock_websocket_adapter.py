"""Mock WebSocket connection adapter for testing.

This module provides mock implementations for testing the error recovery system
without requiring actual WebSocket connections.
"""

from __future__ import annotations

import asyncio
import secrets
from typing import Any

from cyberdelta.config.structlog_config import get_logger


class MockWebSocketConnectionAdapter:
    """Mock adapter for testing error recovery functionality."""

    def __init__(
        self,
        connection_id: str,
        fail_after: float = 10,
        fail_probability: float = 0.0,
        connection_delay: float = 0.1,
        message_delay: float = 0.01,
    ) -> None:
        """Initialize mock adapter.

        Args:
            connection_id: Connection identifier
            fail_after: Number of operations before simulating failure
            fail_probability: Probability of random failures (0.0 - 1.0)
            connection_delay: Simulated connection establishment delay
            message_delay: Simulated message sending delay
        """
        self.connection_id = connection_id
        self.logger = get_logger(f"MockAdapter.{connection_id}")
        self._connected = False
        self._operation_count = 0
        self._fail_after = fail_after
        self._fail_probability = fail_probability
        self._connection_delay = connection_delay
        self._message_delay = message_delay
        self._total_connections = 0
        self._total_messages = 0

    async def connect(self) -> bool:
        """Simulate connection establishment.
        
        Returns:
            True if connection successful, False otherwise.
        """
        self._operation_count += 1
        self._total_connections += 1

        # Simulate connection failure
        if self._operation_count > self._fail_after:
            self.logger.warning(
                "simulated_connection_failure",
                connection_id=self.connection_id,
                operation_count=self._operation_count,
                fail_after=self._fail_after,
            )
            return False

        # Simulate random failures
        if secrets.SystemRandom().random() < self._fail_probability:
            self.logger.warning(
                "random_connection_failure",
                connection_id=self.connection_id,
                probability=self._fail_probability,
            )
            return False

        # Simulate connection delay
        await asyncio.sleep(self._connection_delay)
        self._connected = True
        self.logger.info(
            "mock_connection_established",
            connection_id=self.connection_id,
            total_connections=self._total_connections,
        )
        return True

    async def disconnect(self) -> None:
        """Simulate disconnection."""
        self._connected = False
        self.logger.info("mock_connection_closed", connection_id=self.connection_id)

    async def is_healthy(self) -> bool:
        """Simulate health check.
        
        Returns:
            True if connection is healthy, False otherwise.
        """
        # Random failures based on probability
        if secrets.SystemRandom().random() < self._fail_probability:
            self.logger.debug(
                "health_check_random_failure",
                connection_id=self.connection_id,
                probability=self._fail_probability,
            )
            return False

        return self._connected

    async def send_message(self, message: dict[str, Any]) -> bool:
        """Simulate message sending.
        
        Returns:
            True if message sent successfully, False otherwise.
        """
        if not self._connected:
            self.logger.warning(
                "message_send_not_connected",
                connection_id=self.connection_id,
            )
            return False

        self._operation_count += 1
        self._total_messages += 1

        # Simulate message failure after threshold
        if self._operation_count > self._fail_after:
            self.logger.warning(
                "simulated_message_failure",
                connection_id=self.connection_id,
                operation_count=self._operation_count,
                fail_after=self._fail_after,
            )
            return False

        # Simulate random message failures
        if secrets.SystemRandom().random() < self._fail_probability:
            self.logger.warning(
                "random_message_failure",
                connection_id=self.connection_id,
                probability=self._fail_probability,
            )
            return False

        # Simulate network delay
        await asyncio.sleep(self._message_delay)

        self.logger.debug(
            "mock_message_sent",
            connection_id=self.connection_id,
            message_size=len(str(message)),
            total_messages=self._total_messages,
        )
        return True

    def reset_failure_count(self) -> None:
        """Reset operation count to simulate recovery."""
        self._operation_count = 0
        self.logger.info(
            "failure_count_reset",
            connection_id=self.connection_id,
        )

    def set_fail_after(self, fail_after: int) -> None:
        """Update failure threshold for dynamic testing."""
        self._fail_after = fail_after
        self.logger.info(
            "fail_after_updated",
            connection_id=self.connection_id,
            new_threshold=fail_after,
        )

    def set_fail_probability(self, probability: float) -> None:
        """Update failure probability for dynamic testing."""
        self._fail_probability = probability
        self.logger.info(
            "fail_probability_updated",
            connection_id=self.connection_id,
            new_probability=probability,
        )

    def get_stats(self) -> dict[str, Any]:
        """Get mock adapter statistics.
        
        Returns:
            Dictionary containing adapter statistics and state.
        """
        return {
            "connection_id": self.connection_id,
            "connected": self._connected,
            "operation_count": self._operation_count,
            "fail_after": self._fail_after,
            "fail_probability": self._fail_probability,
            "total_connections": self._total_connections,
            "total_messages": self._total_messages,
        }


class ReliableMockAdapter(MockWebSocketConnectionAdapter):
    """Mock adapter that never fails - for testing success scenarios."""

    def __init__(self, connection_id: str) -> None:
        """Initialize reliable mock adapter.

        Args:
            connection_id: Unique identifier for the connection
        """
        super().__init__(
            connection_id=connection_id,
            fail_after=999999,  # Never fail (very high number)
            fail_probability=0.0,
            connection_delay=0.01,  # Fast connections
            message_delay=0.001,  # Fast messages
        )


class UnreliableMockAdapter(MockWebSocketConnectionAdapter):
    """Mock adapter with high failure rate - for testing recovery scenarios."""

    def __init__(self, connection_id: str) -> None:
        """Initialize unreliable mock adapter.

        Args:
            connection_id: Unique identifier for the connection
        """
        super().__init__(
            connection_id=connection_id,
            fail_after=5,  # Fail after 5 operations
            fail_probability=0.2,  # 20% random failure rate
            connection_delay=0.1,
            message_delay=0.01,
        )


class FlakeyMockAdapter(MockWebSocketConnectionAdapter):
    """Mock adapter with intermittent failures - for testing resilience."""

    def __init__(self, connection_id: str) -> None:
        """Initialize flakey mock adapter.

        Args:
            connection_id: Unique identifier for the connection
        """
        super().__init__(
            connection_id=connection_id,
            fail_after=999999,  # No threshold failures (very high number)
            fail_probability=0.1,  # 10% random failure rate
            connection_delay=0.05,
            message_delay=0.005,
        )

    async def is_healthy(self) -> bool:
        """Simulate intermittent health check failures.
        
        Returns:
            True if connection is healthy, False if health check fails.
        """
        # Higher failure rate for health checks to simulate flaky network
        if secrets.SystemRandom().random() < (self._fail_probability * 2):
            return False
        return self._connected
