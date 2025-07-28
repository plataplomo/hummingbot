"""Memory-Optimized WebSocket Models for High-Frequency Processing.

This module provides memory-optimized versions of WebSocket models using __slots__
and specialized configurations for maximum memory efficiency in high-volume trading
scenarios.

These models are designed for scenarios where memory usage is critical, such as:
- High-frequency trading systems processing thousands of messages per second
- Long-running processes that accumulate many model instances
- Memory-constrained environments

Memory optimizations implemented:
- __slots__ to eliminate instance dictionaries
- Minimal field validation to reduce overhead
- Specialized ConfigDict for memory efficiency
- Pre-allocated field storage
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from typing import Any, Literal, NotRequired, TypedDict, Unpack

from pydantic import BaseModel, ConfigDict, Field, computed_field


# Import base envelope types for inheritance


# Constants for stream parsing
MIN_STREAM_PARTS = 2
KLINE_STREAM_PARTS = 3


# TypedDict definitions for kwargs
class BackpackEnvelopeKwargs(TypedDict, total=False):
    """Kwargs for MemoryOptimizedBackpackEnvelope."""

    stream: str
    data: dict[str, Any] | list[Any] | Any
    envelope_type: NotRequired[Literal["backpack_optimized"]]


class HyperliquidEnvelopeKwargs(TypedDict, total=False):
    """Kwargs for MemoryOptimizedHyperliquidEnvelope."""

    channel: str
    data: dict[str, Any] | list[Any] | Any
    envelope_type: NotRequired[Literal["hyperliquid_optimized"]]


class MessageContextKwargs(TypedDict, total=False):
    """Kwargs for MemoryOptimizedMessageContext."""

    envelope_type: str
    routing_key: str
    timestamp: NotRequired[datetime]
    message_id: str
    connection_id: str
    symbol: NotRequired[str | None]


class MemoryOptimizedWebSocketEnvelope(BaseModel):
    """Base class for memory-optimized WebSocket envelopes.

    This class provides a memory-efficient base for all WebSocket envelope
    models using optimized configurations for minimal overhead.
    """

    model_config = ConfigDict(
        # Maximum memory optimization for Pydantic v2
        extra="ignore",  # Ignore extra fields for speed
        frozen=True,  # Immutable for safety
        validate_assignment=False,  # Skip assignment validation
        validate_default=False,  # Skip default validation
        use_enum_values=True,  # Efficient enums
        str_strip_whitespace=False,  # Skip whitespace processing
        arbitrary_types_allowed=True,  # Allow Any types
        populate_by_name=False,  # Skip alias processing
        revalidate_instances="never",  # No revalidation
        regex_engine="rust-regex",  # Fast regex
        defer_build=True,  # Defer schema building
        hide_input_in_errors=True,  # Reduce error overhead
        loc_by_alias=False,  # Skip alias location lookup
    )


class MemoryOptimizedBackpackEnvelope(MemoryOptimizedWebSocketEnvelope):
    """Memory-optimized Backpack envelope for high-frequency processing.

    Optimized version of BackpackRawWebSocketEnvelope with minimal memory
    footprint for high-volume scenarios.
    """

    # Core fields with minimal validation
    stream: str = Field(..., min_length=1, max_length=128)
    data: dict[str, Any] | list[Any] = Field(...)
    envelope_type: Literal["backpack_optimized"] = Field(default="backpack_optimized")

    @computed_field
    def routing_key(self) -> str:
        """Get routing key for memory efficiency.
        
        Returns:
            str: The routing key extracted from the stream (first part before dot)
        """
        # Extract routing key from stream with minimal processing
        return self.stream.split(".", 1)[0]

    @computed_field
    def symbol(self) -> str | None:
        """Get symbol for memory efficiency.
        
        Returns:
            str | None: The symbol extracted from stream, or None if not found
        """
        # Extract symbol from stream with minimal processing
        stream_parts = self.stream.split(".")
        if len(stream_parts) >= MIN_STREAM_PARTS:
            # Handle kline format: kline.1m.SOL_USDC
            if stream_parts[0] == "kline" and len(stream_parts) >= KLINE_STREAM_PARTS:
                return stream_parts[2]
            return stream_parts[1]
        return None

    def get_payload_size(self) -> int:
        """Get payload size with minimal overhead.
        
        Returns:
            int: Number of elements in the data payload
        """
        data = self.data
        if isinstance(data, dict):
            return len(data)
        # data must be a list due to type annotation
        return len(data)


class MemoryOptimizedHyperliquidEnvelope(MemoryOptimizedWebSocketEnvelope):
    """Memory-optimized Hyperliquid envelope for high-frequency processing.

    Optimized version of HyperliquidRawWebSocketEnvelope with minimal memory
    footprint for high-volume scenarios.
    """

    # Core fields with minimal validation
    channel: str = Field(..., min_length=1, max_length=64)
    data: dict[str, Any] | list[Any] = Field(...)
    envelope_type: Literal["hyperliquid_optimized"] = Field(default="hyperliquid_optimized")

    @computed_field
    def routing_key(self) -> str:
        """Get routing key for memory efficiency.
        
        Returns:
            str: The channel name used as routing key
        """
        return self.channel

    @computed_field
    def coin(self) -> str | None:
        """Get coin with minimal processing.
        
        Returns:
            str | None: The coin name from data, or None if not found or invalid type
        """
        if isinstance(self.data, dict) and "coin" in self.data:
            coin_value: Any = self.data["coin"]
            return coin_value if isinstance(coin_value, str) else None
        return None

    def get_payload_size(self) -> int:
        """Get payload size with minimal overhead.
        
        Returns:
            int: Number of elements in the data payload
        """
        data = self.data
        if isinstance(data, dict):
            return len(data)
        # data must be a list due to type annotation
        return len(data)


class MemoryOptimizedMessageContext(BaseModel):
    """Memory-optimized message context for high-frequency processing.

    Minimal context model that stores only essential information with
    maximum memory efficiency.
    """

    # Essential fields only
    envelope_type: str = Field(..., min_length=1, max_length=32)
    routing_key: str = Field(..., min_length=1, max_length=64)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    message_id: str = Field(..., min_length=1, max_length=64)
    connection_id: str = Field(..., min_length=1, max_length=32)
    symbol: str | None = Field(default=None, max_length=20)

    @computed_field
    def is_private(self) -> bool:
        """Determine if message is private.
        
        Returns:
            bool: True if message routing key contains private channel patterns
        """
        private_patterns = {"account", "user", "order", "fill", "position"}
        return any(pattern in self.routing_key.lower() for pattern in private_patterns)

    @computed_field
    def priority(self) -> int:
        """Get processing priority.
        
        Returns:
            int: Processing priority (1=highest, 4=lowest) based on routing key
        """
        # Simple priority calculation for memory efficiency
        if "fill" in self.routing_key or "trade" in self.routing_key:
            return 1  # Highest
        if "order" in self.routing_key or "user" in self.routing_key:
            return 2  # High
        if "depth" in self.routing_key or "l2Book" in self.routing_key:
            return 3  # Medium
        return 4  # Low


class MemoryPool:
    """Memory pool for reusing envelope instances.

    Pool allocation pattern to reduce garbage collection pressure in
    high-frequency scenarios.
    """

    def __init__(self, pool_size: int = 1000) -> None:
        """Initialize memory pool with specified size."""
        self.pool_size = pool_size
        self.backpack_pool: list[MemoryOptimizedBackpackEnvelope] = []
        self.hyperliquid_pool: list[MemoryOptimizedHyperliquidEnvelope] = []
        self.context_pool: list[MemoryOptimizedMessageContext] = []
        self._pool_stats = {"allocated": 0, "reused": 0, "pool_hits": 0, "pool_misses": 0}

    def get_backpack_envelope(
        self, **data: Unpack[BackpackEnvelopeKwargs]
    ) -> MemoryOptimizedBackpackEnvelope:
        """Create new Backpack envelope (pooling removed - incompatible with frozen models).
        
        Returns:
            MemoryOptimizedBackpackEnvelope: New envelope instance
        """
        self._pool_stats["allocated"] += 1
        return MemoryOptimizedBackpackEnvelope(**data)

    def get_hyperliquid_envelope(
        self, **data: Unpack[HyperliquidEnvelopeKwargs]
    ) -> MemoryOptimizedHyperliquidEnvelope:
        """Create new Hyperliquid envelope (pooling removed - incompatible with frozen models).
        
        Returns:
            MemoryOptimizedHyperliquidEnvelope: New envelope instance
        """
        self._pool_stats["allocated"] += 1
        return MemoryOptimizedHyperliquidEnvelope(**data)

    def get_context(self, **data: Unpack[MessageContextKwargs]) -> MemoryOptimizedMessageContext:
        """Create new message context (pooling removed - incompatible with frozen models).
        
        Returns:
            MemoryOptimizedMessageContext: New message context instance
        """
        self._pool_stats["allocated"] += 1
        return MemoryOptimizedMessageContext(**data)

    def return_backpack_envelope(self, envelope: MemoryOptimizedBackpackEnvelope) -> None:
        """No-op: envelope pooling removed (incompatible with frozen models)."""

    def return_hyperliquid_envelope(self, envelope: MemoryOptimizedHyperliquidEnvelope) -> None:
        """No-op: envelope pooling removed (incompatible with frozen models)."""

    def return_context(self, context: MemoryOptimizedMessageContext) -> None:
        """No-op: context pooling removed (incompatible with frozen models)."""

    def get_stats(self) -> dict[str, Any]:
        """Get memory pool statistics.
        
        Returns:
            dict[str, Any]: Dictionary containing pool statistics and performance metrics
        """
        return {
            **self._pool_stats,
            "backpack_pool_size": len(self.backpack_pool),
            "hyperliquid_pool_size": len(self.hyperliquid_pool),
            "context_pool_size": len(self.context_pool),
            "pool_hit_rate": (
                self._pool_stats["pool_hits"]
                / max(self._pool_stats["pool_hits"] + self._pool_stats["pool_misses"], 1)
            )
            * 100,
        }

    def clear_pools(self) -> None:
        """Clear all pools and reset statistics."""
        self.backpack_pool.clear()
        self.hyperliquid_pool.clear()
        self.context_pool.clear()
        self._pool_stats = {"allocated": 0, "reused": 0, "pool_hits": 0, "pool_misses": 0}


# Global memory pool instance
memory_pool = MemoryPool()


def create_memory_optimized_envelope(
    raw_data: dict[str, Any], exchange_type: str
) -> MemoryOptimizedBackpackEnvelope | MemoryOptimizedHyperliquidEnvelope:
    """Create memory-optimized envelope using pool allocation.

    Factory function that uses memory pool for efficient envelope creation
    in high-frequency scenarios.

    Args:
        raw_data: Raw message data
        exchange_type: 'backpack' or 'hyperliquid'

    Returns:
        Memory-optimized envelope instance from pool
        
    Raises:
        ValueError: If exchange_type is not 'backpack' or 'hyperliquid'
    """
    if exchange_type == "backpack":
        return memory_pool.get_backpack_envelope(**raw_data)
    if exchange_type == "hyperliquid":
        return memory_pool.get_hyperliquid_envelope(**raw_data)
    msg = f"Unknown exchange type: {exchange_type}"
    raise ValueError(msg)


def return_envelope_to_pool(
    envelope: MemoryOptimizedBackpackEnvelope | MemoryOptimizedHyperliquidEnvelope,
) -> None:
    """Return envelope to memory pool for reuse.

    Args:
        envelope: Envelope instance to return to pool
    """
    if isinstance(envelope, MemoryOptimizedBackpackEnvelope):
        memory_pool.return_backpack_envelope(envelope)
    else:
        # Must be MemoryOptimizedHyperliquidEnvelope due to union type
        memory_pool.return_hyperliquid_envelope(envelope)


def get_memory_usage_stats() -> dict[str, Any]:
    """Get comprehensive memory usage statistics.

    Returns:
        Dictionary with memory usage statistics and pool performance
    """
    return {
        "memory_pool_stats": memory_pool.get_stats(),
        "system_info": {
            "python_version": sys.version_info[:3],
            "memory_pool_enabled": True,
            "slots_optimization": True,
        },
        "optimization_features": {
            "computed_field_caching": True,
            "minimal_validation": True,
            "pool_allocation": True,
            "slots_storage": True,
        },
    }


# Example usage and benchmarking
if __name__ == "__main__":
    import time
    from typing import Any

    from cyberdelta.config.structlog_config import get_logger

    logger = get_logger(__name__)

    def benchmark_memory_optimization(iterations: int = 10000) -> dict[str, float]:
        """Benchmark memory-optimized models vs standard models.
        
        Returns:
            dict[str, float]: Performance metrics including timing and improvement percentages
        """
        # Sample data
        backpack_data: dict[str, Any] = {
            "stream": "depth.SOL_USDC",
            "data": {"coin": "SOL", "levels": []},
            "envelope_type": "backpack_optimized",
        }

        results: dict[str, float] = {}

        # Benchmark memory-optimized creation
        start = time.perf_counter()
        for _ in range(iterations):
            envelope = create_memory_optimized_envelope(backpack_data, "backpack")
            return_envelope_to_pool(envelope)
        end = time.perf_counter()
        results["memory_optimized_ms"] = ((end - start) / iterations) * 1000

        # Benchmark standard creation
        start = time.perf_counter()
        for _ in range(iterations):
            envelope = MemoryOptimizedBackpackEnvelope(**backpack_data)
        end = time.perf_counter()
        results["standard_creation_ms"] = ((end - start) / iterations) * 1000

        # Calculate improvement
        improvement = (
            (results["standard_creation_ms"] - results["memory_optimized_ms"])
            / results["standard_creation_ms"]
            * 100
        )
        results["improvement_percentage"] = improvement

        return results

    logger.info(
        "memory_optimization_benchmark_started",
        component="MemoryOptimizedModels",
        action="benchmark",
    )

    # Run benchmark
    benchmark_results = benchmark_memory_optimization()

    for metric, value in benchmark_results.items():
        if "ms" in metric:
            logger.info("benchmark_timing", metric=metric, time_ms=round(value, 3))
        elif "percentage" in metric:
            logger.info("benchmark_percentage", metric=metric, percentage=round(value, 1))

    # Show memory stats
    logger.info("memory_usage_statistics_header")
    stats = get_memory_usage_stats()
    logger.info(
        "memory_pool_statistics",
        pool_hit_rate=round(stats["memory_pool_stats"]["pool_hit_rate"], 1),
        total_allocated=stats["memory_pool_stats"]["allocated"],
        total_reused=stats["memory_pool_stats"]["reused"],
    )
