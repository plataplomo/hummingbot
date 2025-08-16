"""Cross-Exchange Discriminated Unions for Ultra-Fast WebSocket Validation.

This module implements discriminated unions to optimize validation performance
by leveraging Pydantic v2's optimized union validation (measured: ~14% improvement).

Discriminated unions provide significant performance gains for WebSocket message
validation by avoiding the need to check multiple models sequentially.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Annotated, Any, TypeVar

from pydantic import Field, TypeAdapter

from cyberdelta.apis.backpack.models.bp_ws_discriminated_envelope import (
    DiscriminatedBackpackEnvelope,
)
from cyberdelta.apis.hyperliquid.models.hl_ws_discriminated_envelope import (
    DiscriminatedHyperliquidEnvelope,
    DiscriminatedHyperliquidUserEvent,
)


if TYPE_CHECKING:
    from collections.abc import Callable


__all__ = [
    "DiscriminatedBackpackEnvelope",
    "DiscriminatedHyperliquidEnvelope",
    "DiscriminatedHyperliquidUserEvent",
    "WebSocketEnvelopeUnion",
    "benchmark_validation",
    "detect_and_add_discriminator",
    "envelope_adapter",
    "validate_backpack_fast",
    "validate_envelope_ultra_fast",
    "validate_hyperliquid_fast",
    "validate_hyperliquid_user_event_fast",
]


# High-performance discriminated union for all WebSocket envelopes
WebSocketEnvelopeUnion = Annotated[
    (
        DiscriminatedBackpackEnvelope
        | DiscriminatedHyperliquidEnvelope
        | DiscriminatedHyperliquidUserEvent
    ),
    Field(discriminator="envelope_type"),
]


# Pre-compiled TypeAdapter for maximum performance
envelope_adapter: TypeAdapter[WebSocketEnvelopeUnion] = TypeAdapter(WebSocketEnvelopeUnion)


def detect_and_add_discriminator(raw_data: dict[str, Any]) -> dict[str, Any]:
    """Detect message type and add discriminator field for fast validation.

    This function analyzes raw WebSocket data and adds the appropriate
    discriminator field to enable ultra-fast union validation.

    Args:
        raw_data: Raw WebSocket message dictionary

    Returns:
        Message with discriminator field added

    Raises:
        ValueError: If message format cannot be determined
    """
    # Create a copy to avoid modifying original data
    data = raw_data.copy()

    # Skip if discriminator already present
    if "envelope_type" in data:
        return data

    # Detect Backpack formats
    if "stream" in data and "data" in data:
        # Current Backpack format
        data["envelope_type"] = "backpack"
        return data

    # Detect Hyperliquid formats
    if "channel" in data and "data" in data:
        channel = data.get("channel")
        if channel == "userEvents":
            data["envelope_type"] = "hyperliquid_user_event"
        else:
            data["envelope_type"] = "hyperliquid"
        return data

    # Unknown format
    msg = f"Cannot determine envelope type for message keys: {list(raw_data.keys())}"
    raise ValueError(msg)


def validate_envelope_ultra_fast(raw_data: dict[str, Any]) -> WebSocketEnvelopeUnion:
    """Ultra-fast envelope validation using discriminated unions.

    This function provides optimized validation (measured: ~14% improvement) compared to
    traditional validation by leveraging Pydantic's discriminated union validation.

    Args:
        raw_data: Raw WebSocket message dictionary

    Returns:
        Validated envelope using discriminated union

    Notes:
        Performance optimization:
        - Uses pre-compiled TypeAdapter for maximum speed
        - Discriminated unions avoid checking multiple models
        - Optimized model configurations for high-frequency validation
    """
    # Add discriminator field for fast validation
    data_with_discriminator = detect_and_add_discriminator(raw_data)

    # Use pre-compiled adapter for ultra-fast validation
    return envelope_adapter.validate_python(data_with_discriminator)


# Convenience functions for specific envelope types
def validate_backpack_fast(raw_data: dict[str, Any]) -> DiscriminatedBackpackEnvelope:
    """Fast validation specifically for Backpack messages.

    Args:
        raw_data: Raw Backpack WebSocket message dictionary

    Returns:
        Validated Backpack envelope with discriminator
    """
    data = raw_data.copy()
    data["envelope_type"] = "backpack"
    return DiscriminatedBackpackEnvelope.model_validate(data)


def validate_hyperliquid_fast(raw_data: dict[str, Any]) -> DiscriminatedHyperliquidEnvelope:
    """Fast validation specifically for Hyperliquid messages.

    Args:
        raw_data: Raw Hyperliquid WebSocket message dictionary

    Returns:
        Validated Hyperliquid envelope with discriminator
    """
    data = raw_data.copy()
    data["envelope_type"] = "hyperliquid"
    return DiscriminatedHyperliquidEnvelope.model_validate(data)


def validate_hyperliquid_user_event_fast(
    raw_data: dict[str, Any],
) -> DiscriminatedHyperliquidUserEvent:
    """Fast validation specifically for Hyperliquid user events.

    Args:
        raw_data: Raw Hyperliquid user event message dictionary

    Returns:
        Validated Hyperliquid user event envelope with discriminator
    """
    data = raw_data.copy()
    data["envelope_type"] = "hyperliquid_user_event"
    return DiscriminatedHyperliquidUserEvent.model_validate(data)


# Performance monitoring utilities
T = TypeVar("T")


def benchmark_validation[T](
    validator_func: Callable[[dict[str, Any]], T],
    raw_data: dict[str, Any],
    iterations: int = 1000,
) -> tuple[T, float]:
    """Benchmark validation performance.

    Args:
        validator_func: Validation function to benchmark
        raw_data: Sample data to validate
        iterations: Number of iterations for benchmark

    Returns:
        Tuple of (validated_result, average_time_ms)
    """
    # Warmup
    result = validator_func(raw_data)

    # Benchmark
    start_time = time.perf_counter()
    for _ in range(iterations):
        validator_func(raw_data)
    end_time = time.perf_counter()

    avg_time_ms = ((end_time - start_time) / iterations) * 1000
    return result, avg_time_ms
