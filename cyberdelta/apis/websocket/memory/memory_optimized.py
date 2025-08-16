"""Simple memory pool implementation.

Provides basic memory pooling functionality without overengineering.
Maintains the interface expected by router but with minimal complexity.
Thread-safe for production use.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any


class MemoryOptimizedMessageContext:
    """Simple memory-optimized context for high-frequency scenarios."""

    def __init__(
        self,
        envelope_type: str,
        routing_key: str,
        message_id: str,
        connection_id: str,
        symbol: str | None = None,
    ) -> None:
        """Initialize memory-optimized context.

        Args:
            envelope_type: Type of the envelope
            routing_key: Message routing key
            message_id: Message identifier
            connection_id: Connection identifier
            symbol: Optional symbol/coin
        """
        self.envelope_type = envelope_type
        self.routing_key = routing_key
        self.message_id = message_id
        self.connection_id = connection_id
        self.symbol = symbol


class MemoryPool:
    """Simple memory pool for context reuse with thread safety."""

    def __init__(self, pool_size: int = 1000) -> None:
        """Initialize memory pool.

        Args:
            pool_size: Maximum size of the pool
        """
        self.pool_size = pool_size
        self._context_pool: deque[MemoryOptimizedMessageContext] = deque(maxlen=pool_size)
        self._created_count = 0
        self._reused_count = 0
        self._lock = threading.RLock()  # Reentrant lock for thread safety

    def get_context(
        self,
        envelope_type: str,
        routing_key: str,
        message_id: str,
        connection_id: str,
        symbol: str | None = None,
    ) -> MemoryOptimizedMessageContext:
        """Get context from pool or create new one.

        Args:
            envelope_type: Type of envelope
            routing_key: Message routing key
            message_id: Message identifier
            connection_id: Connection identifier
            symbol: Optional symbol

        Returns:
            Memory-optimized context instance
        """
        # Thread-safe pool operations
        with self._lock:
            # Try to reuse from pool
            if self._context_pool:
                context = self._context_pool.popleft()
                # Reset the context with new values
                context.envelope_type = envelope_type
                context.routing_key = routing_key
                context.message_id = message_id
                context.connection_id = connection_id
                context.symbol = symbol
                self._reused_count += 1
                return context

            # Create new if pool is empty
            self._created_count += 1

        # Create context outside lock to minimize lock time
        return MemoryOptimizedMessageContext(
            envelope_type=envelope_type,
            routing_key=routing_key,
            message_id=message_id,
            connection_id=connection_id,
            symbol=symbol,
        )

    def return_context(self, context: MemoryOptimizedMessageContext) -> None:
        """Return context to pool for reuse.

        Args:
            context: Context to return to pool
        """
        with self._lock:
            if len(self._context_pool) < self.pool_size:
                self._context_pool.append(context)

    def clear_pools(self) -> None:
        """Clear all pools."""
        with self._lock:
            self._context_pool.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get memory pool statistics.

        Returns:
            Dictionary with pool statistics
        """
        with self._lock:
            return {
                "pool_size": self.pool_size,
                "contexts_in_pool": len(self._context_pool),
                "total_created": self._created_count,
                "total_reused": self._reused_count,
                "reuse_rate": (
                    self._reused_count / (self._created_count + self._reused_count)
                    if (self._created_count + self._reused_count) > 0
                    else 0.0
                ),
            }
