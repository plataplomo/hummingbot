"""Pre-compiled TypeAdapters for Ultra-Fast JSON Validation.

This module provides pre-compiled TypeAdapters for direct JSON validation,
eliminating the overhead of model instantiation and providing maximum
validation performance.

TypeAdapters provide significant performance improvements for high-frequency
validation by bypassing the normal model creation overhead.
"""

from __future__ import annotations

import time

# Protocol for models that can be serialized
from typing import Any, Protocol, TypeVar

from pydantic import TypeAdapter

from cyberdelta.apis.connectivity.json_security import secure_json_loads

# Import financial models for adapter creation
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsFillEvent,
)

# Import discriminated union types (now in same integration layer)
from cyberdelta.apis.integration.ws_discriminated_unions import (
    DiscriminatedBackpackEnvelope,
    DiscriminatedHyperliquidEnvelope,
    DiscriminatedHyperliquidUserEvent,
    WebSocketEnvelopeUnion,
)


class SerializableModel(Protocol):
    """Protocol for models that can be JSON serialized."""

    def model_dump_json(self) -> str | bytes:
        """Serialize model to JSON string or bytes."""
        ...


# Pre-compiled TypeAdapters for maximum performance
class WebSocketTypeAdapters:
    """Collection of pre-compiled TypeAdapters for WebSocket validation.

    Pre-compiled adapters eliminate the overhead of repeated model compilation
    and provide maximum validation performance.
    """

    # Primary envelope adapter (discriminated union)
    envelope_adapter: TypeAdapter[WebSocketEnvelopeUnion] = TypeAdapter(WebSocketEnvelopeUnion)

    # Individual envelope adapters for specific use cases
    backpack_adapter = TypeAdapter(DiscriminatedBackpackEnvelope)
    hyperliquid_adapter = TypeAdapter(DiscriminatedHyperliquidEnvelope)
    hyperliquid_user_event_adapter = TypeAdapter(DiscriminatedHyperliquidUserEvent)

    # Financial data adapters for high-frequency validation
    fill_event_adapter = TypeAdapter(HyperliquidRawWsFillEvent)

    # Union adapters for different scenarios
    backpack_union_adapter: TypeAdapter[DiscriminatedBackpackEnvelope] = TypeAdapter(
        DiscriminatedBackpackEnvelope,
    )

    hyperliquid_union_adapter: TypeAdapter[
        DiscriminatedHyperliquidEnvelope | DiscriminatedHyperliquidUserEvent
    ] = TypeAdapter(DiscriminatedHyperliquidEnvelope | DiscriminatedHyperliquidUserEvent)

    @classmethod
    def validate_json_ultra_fast(cls, json_data: str | bytes) -> WebSocketEnvelopeUnion:
        """Ultra-fast JSON validation using pre-compiled adapters.

        Direct JSON validation without intermediate dict conversion for
        maximum performance.

        Args:
            json_data: Raw JSON string or bytes

        Returns:
            Validated envelope model
        """
        return cls.envelope_adapter.validate_json(json_data)

    @classmethod
    def validate_python_ultra_fast(cls, python_data: dict[str, Any]) -> WebSocketEnvelopeUnion:
        """Ultra-fast Python dict validation using pre-compiled adapters.

        Args:
            python_data: Python dictionary data

        Returns:
            Validated envelope model
        """
        return cls.envelope_adapter.validate_python(python_data)

    @classmethod
    def validate_backpack_json(cls, json_data: str | bytes) -> DiscriminatedBackpackEnvelope:
        """Fast Backpack-specific JSON validation.

        Args:
            json_data: Raw JSON string or bytes for Backpack message

        Returns:
            Validated Backpack envelope
        """
        return cls.backpack_adapter.validate_json(json_data)

    @classmethod
    def validate_hyperliquid_json(cls, json_data: str | bytes) -> DiscriminatedHyperliquidEnvelope:
        """Fast Hyperliquid-specific JSON validation.

        Args:
            json_data: Raw JSON string or bytes for Hyperliquid message

        Returns:
            Validated Hyperliquid envelope
        """
        return cls.hyperliquid_adapter.validate_json(json_data)

    @classmethod
    def validate_fill_event_json(cls, json_data: str | bytes) -> HyperliquidRawWsFillEvent:
        """Ultra-fast fill event validation for high-frequency trading.

        Args:
            json_data: Raw JSON string or bytes for fill event

        Returns:
            Validated fill event model
        """
        return cls.fill_event_adapter.validate_json(json_data)

    @classmethod
    def dump_json_ultra_fast(cls, model: SerializableModel) -> str:
        """Ultra-fast JSON serialization using TypeAdapters.

        Args:
            model: Pydantic model instance to serialize

        Returns:
            JSON string representation

        Raises:
            TypeError: If no serialization adapter is available for the model type.
        """
        # Use the appropriate adapter based on model type
        if isinstance(
            model,
            (
                DiscriminatedBackpackEnvelope,
                DiscriminatedHyperliquidEnvelope,
                DiscriminatedHyperliquidUserEvent,
            ),
        ):
            return cls.envelope_adapter.dump_json(model).decode("utf-8")
        if isinstance(model, HyperliquidRawWsFillEvent):
            return cls.fill_event_adapter.dump_json(model).decode("utf-8")
        # No fallback - require explicit adapter support
        raise TypeError(type(model).__name__)


class StreamingValidationAdapter:
    """Specialized adapter for streaming validation scenarios.

    Optimized for continuous validation of streaming WebSocket messages
    with minimal memory allocation.
    """

    def __init__(self) -> None:
        """Initialize streaming adapter with pre-compiled validators."""
        self.adapters = WebSocketTypeAdapters()
        self._validation_cache: dict[str, Any] = {}

    def validate_streaming_message(
        self,
        message: str | bytes | dict[str, Any],
    ) -> WebSocketEnvelopeUnion:
        """Validate streaming message with optimized path selection.

        Args:
            message: Message in any supported format

        Returns:
            Validated envelope model
        """
        if isinstance(message, (str, bytes)):
            # Direct JSON validation - fastest path
            return self.adapters.validate_json_ultra_fast(message)
        # Python dict validation - second fastest path
        return self.adapters.validate_python_ultra_fast(message)

    def validate_batch(
        self,
        messages: list[str | bytes | dict[str, Any]],
    ) -> list[WebSocketEnvelopeUnion]:
        """Validate batch of messages with optimized processing.

        Args:
            messages: List of messages to validate

        Returns:
            List of validated envelope models

        Raises:
            ValueError: If batch validation fails on any message
        """
        results: list[WebSocketEnvelopeUnion] = []
        for message in messages:
            try:
                validated = self.validate_streaming_message(message)
                results.append(validated)
            except Exception as e:
                # In production, you might want to handle errors differently
                # For now, we re-raise to maintain error visibility
                msg = f"Batch validation failed on message: {e}"
                raise ValueError(msg) from e

        return results


# Performance monitoring and benchmarking utilities
T = TypeVar("T")


class ValidationBenchmark:
    """Benchmark utilities for validation performance measurement.

    Tools to measure and compare validation performance across different
    validation strategies.
    """

    @staticmethod
    def benchmark_json_validation(json_data: str, iterations: int = 1000) -> dict[str, float]:
        """Benchmark different JSON validation approaches.

        Args:
            json_data: Sample JSON string to validate
            iterations: Number of iterations for benchmark

        Returns:
            Dictionary with timing results for different approaches
        """
        adapters = WebSocketTypeAdapters()
        results: dict[str, float] = {}

        # Benchmark TypeAdapter direct JSON validation
        start = time.perf_counter()
        for _ in range(iterations):
            adapters.validate_json_ultra_fast(json_data)
        end = time.perf_counter()
        results["type_adapter_json"] = ((end - start) / iterations) * 1000

        # Benchmark traditional validation (JSON -> dict -> model)
        start = time.perf_counter()
        for _ in range(iterations):
            json_obj = secure_json_loads(json_data)
            if isinstance(json_obj, dict):
                adapters.validate_python_ultra_fast(json_obj)
        end = time.perf_counter()
        results["traditional_validation"] = ((end - start) / iterations) * 1000

        # Calculate performance improvement
        improvement = (
            (results["traditional_validation"] - results["type_adapter_json"])
            / results["traditional_validation"]
            * 100
        )
        results["improvement_percentage"] = improvement

        return results

    @staticmethod
    def benchmark_batch_validation(
        messages: list[str | bytes | dict[str, Any]],
        batch_sizes: list[int] | None = None,
    ) -> dict[int, float]:
        """Benchmark batch validation performance.

        Args:
            messages: List of sample messages
            batch_sizes: Different batch sizes to test

        Returns:
            Dictionary mapping batch size to average validation time per message
        """
        if batch_sizes is None:
            batch_sizes = [1, 10, 100, 1000]
        adapter = StreamingValidationAdapter()
        results: dict[int, float] = {}

        for batch_size in batch_sizes:
            if batch_size > len(messages):
                continue

            batch = messages[:batch_size]

            # Warmup
            adapter.validate_batch(batch)

            # Benchmark
            start = time.perf_counter()
            adapter.validate_batch(batch)
            end = time.perf_counter()

            avg_time_per_message = ((end - start) / batch_size) * 1000
            results[batch_size] = avg_time_per_message

        return results


# Global instances for convenient access
adapters = WebSocketTypeAdapters()
streaming_adapter = StreamingValidationAdapter()
benchmark = ValidationBenchmark()
