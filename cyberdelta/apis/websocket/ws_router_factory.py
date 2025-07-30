"""Factory for creating WebSocket routers with optimized configurations.

This module provides factory functions for creating WebSocket routers
with different performance and memory optimization profiles.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.websocket.ws_context import ExchangeType
from cyberdelta.apis.websocket.ws_error_recovery import ErrorRecoveryConfig
from cyberdelta.apis.websocket.ws_memory_config import (
    MemoryOptimizationConfig,
    PerformanceMode,
    PerformanceModePresets,
    get_recommended_mode_for_scenario,
)
from cyberdelta.apis.websocket.ws_metrics import WebSocketMetricsCollector
from cyberdelta.apis.websocket.ws_validators import WebSocketPayloadValidators
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_error_handler import BaseErrorHandler


logger = get_logger(__name__)


class RouterConfiguration:
    """Router configuration builder with performance optimization presets."""

    def __init__(self) -> None:
        """Initialize router configuration builder."""
        self.exchange_name: str | None = None
        self.exchange_type: ExchangeType | None = None
        self.error_handler: BaseErrorHandler | None = None
        self.envelope_validator: Callable[[dict[str, Any]], Any] | None = None
        self.payload_validator: WebSocketPayloadValidators | None = None
        self.metrics_collector: WebSocketMetricsCollector | None = None
        self.recovery_config: ErrorRecoveryConfig | None = None
        self.memory_config: MemoryOptimizationConfig | None = None
        self.performance_mode: PerformanceMode = PerformanceMode.STANDARD

    def with_exchange(self, name: str, exchange_type: ExchangeType) -> RouterConfiguration:
        """Configure exchange information.

        Args:
            name: Exchange name
            exchange_type: Type of exchange (backpack/hyperliquid)

        Returns:
            Updated configuration builder
        """
        self.exchange_name = name
        self.exchange_type = exchange_type
        return self

    def with_error_handler(self, error_handler: BaseErrorHandler) -> RouterConfiguration:
        """Configure error handler.

        Args:
            error_handler: Error handler instance

        Returns:
            Updated configuration builder
        """
        self.error_handler = error_handler
        return self

    def with_envelope_validator(
        self,
        validator: Callable[[dict[str, Any]], Any],
    ) -> RouterConfiguration:
        """Configure envelope validator.

        Args:
            validator: Envelope validation function

        Returns:
            Updated configuration builder
        """
        self.envelope_validator = validator
        return self

    def with_performance_mode(self, mode: PerformanceMode) -> RouterConfiguration:
        """Configure performance mode.

        Args:
            mode: Performance mode to use

        Returns:
            Updated configuration builder
        """
        self.performance_mode = mode
        self.memory_config = PerformanceModePresets.get_config(mode)
        return self

    def with_custom_memory_config(self, config: MemoryOptimizationConfig) -> RouterConfiguration:
        """Configure custom memory optimization settings.

        Args:
            config: Custom memory optimization configuration

        Returns:
            Updated configuration builder
        """
        self.memory_config = config
        return self

    def with_recovery_config(self, config: ErrorRecoveryConfig) -> RouterConfiguration:
        """Configure error recovery settings.

        Args:
            config: Error recovery configuration

        Returns:
            Updated configuration builder
        """
        self.recovery_config = config
        return self

    def get_router_kwargs(self) -> dict[str, Any]:
        """Get keyword arguments for router initialization.

        Returns:
            Dictionary of router initialization arguments

        Raises:
            ValueError: If required configuration is missing
        """
        if not self.exchange_name:
            msg = "Exchange name is required"
            raise ValueError(msg)

        if not self.exchange_type:
            msg = "Exchange type is required"
            raise ValueError(msg)

        if not self.error_handler:
            msg = "Error handler is required"
            raise ValueError(msg)

        # Get memory configuration or use standard preset
        memory_config = self.memory_config or PerformanceModePresets.standard()

        # Determine if memory optimization should be enabled
        enable_memory_optimization = (
            memory_config.enable_pooling
            or memory_config.enable_slots_optimization
            or self.performance_mode != PerformanceMode.STANDARD
        )

        kwargs = {
            "exchange_name": self.exchange_name,
            "exchange_type": self.exchange_type,
            "error_handler": self.error_handler,
            "envelope_validator": self.envelope_validator,
            "payload_validator": self.payload_validator,
            "metrics_collector": self.metrics_collector,
            "recovery_config": self.recovery_config,
            "enable_error_recovery": True,  # Always enabled for production reliability
            "enable_memory_optimization": enable_memory_optimization,
            "memory_pool_size": memory_config.pool_size,
        }

        # Filter out None values
        return {k: v for k, v in kwargs.items() if v is not None}


def create_standard_router(
    exchange_name: str,
    exchange_type: ExchangeType,
    error_handler: BaseErrorHandler,
    envelope_validator: Callable[[dict[str, Any]], Any] | None = None,
) -> RouterConfiguration:
    """Create a standard router configuration for regular trading scenarios.

    Args:
        exchange_name: Name of the exchange
        exchange_type: Type of exchange
        error_handler: Error handler instance
        envelope_validator: Optional envelope validator

    Returns:
        Router configuration for standard trading
    """
    config = (
        RouterConfiguration()
        .with_exchange(exchange_name, exchange_type)
        .with_error_handler(error_handler)
        .with_performance_mode(PerformanceMode.STANDARD)
    )

    if envelope_validator:
        config = config.with_envelope_validator(envelope_validator)

    logger.info(
        "standard_router_configuration_created",
        exchange=exchange_name,
        mode=PerformanceMode.STANDARD.value,
    )

    return config


def create_high_frequency_router(
    exchange_name: str,
    exchange_type: ExchangeType,
    error_handler: BaseErrorHandler,
    envelope_validator: Callable[[dict[str, Any]], Any] | None = None,
    message_rate_per_second: int | None = None,
) -> RouterConfiguration:
    """Create a high-frequency trading router configuration.

    Args:
        exchange_name: Name of the exchange
        exchange_type: Type of exchange
        error_handler: Error handler instance
        envelope_validator: Optional envelope validator
        message_rate_per_second: Expected message rate for pool sizing

    Returns:
        Router configuration optimized for high-frequency trading
    """
    # Adjust pool size based on message rate
    memory_config = PerformanceModePresets.high_frequency()
    if message_rate_per_second:
        # Scale pool size based on message rate (roughly 2x the expected rate)
        memory_config.pool_size = max(2000, message_rate_per_second * 2)

    config = (
        RouterConfiguration()
        .with_exchange(exchange_name, exchange_type)
        .with_error_handler(error_handler)
        .with_performance_mode(PerformanceMode.HIGH_FREQUENCY)
        .with_custom_memory_config(memory_config)
    )

    if envelope_validator:
        config = config.with_envelope_validator(envelope_validator)

    logger.info(
        "high_frequency_router_configuration_created",
        exchange=exchange_name,
        mode=PerformanceMode.HIGH_FREQUENCY.value,
        pool_size=memory_config.pool_size,
        message_rate=message_rate_per_second,
    )

    return config


def create_ultra_low_latency_router(
    exchange_name: str,
    exchange_type: ExchangeType,
    error_handler: BaseErrorHandler,
    envelope_validator: Callable[[dict[str, Any]], Any] | None = None,
) -> RouterConfiguration:
    """Create an ultra-low latency router configuration for market making.

    Args:
        exchange_name: Name of the exchange
        exchange_type: Type of exchange
        error_handler: Error handler instance
        envelope_validator: Optional envelope validator

    Returns:
        Router configuration optimized for ultra-low latency
    """
    config = (
        RouterConfiguration()
        .with_exchange(exchange_name, exchange_type)
        .with_error_handler(error_handler)
        .with_performance_mode(PerformanceMode.ULTRA_LOW_LATENCY)
    )

    if envelope_validator:
        config = config.with_envelope_validator(envelope_validator)

    logger.info(
        "ultra_low_latency_router_configuration_created",
        exchange=exchange_name,
        mode=PerformanceMode.ULTRA_LOW_LATENCY.value,
    )

    return config


def create_memory_optimized_router(
    exchange_name: str,
    exchange_type: ExchangeType,
    error_handler: BaseErrorHandler,
    envelope_validator: Callable[[dict[str, Any]], Any] | None = None,
    memory_limit_mb: float | None = None,
) -> RouterConfiguration:
    """Create a memory-optimized router configuration.

    Args:
        exchange_name: Name of the exchange
        exchange_type: Type of exchange
        error_handler: Error handler instance
        envelope_validator: Optional envelope validator
        memory_limit_mb: Memory limit in megabytes

    Returns:
        Router configuration optimized for low memory usage
    """
    # Adjust pool size based on memory limit
    memory_config = PerformanceModePresets.memory_optimized()
    if memory_limit_mb:
        # Very conservative pool sizing for memory-constrained environments
        estimated_pool_size = max(100, int(memory_limit_mb / 0.5))  # ~0.5MB per 1000 objects
        memory_config.pool_size = min(memory_config.pool_size, estimated_pool_size)

    config = (
        RouterConfiguration()
        .with_exchange(exchange_name, exchange_type)
        .with_error_handler(error_handler)
        .with_performance_mode(PerformanceMode.MEMORY_OPTIMIZED)
        .with_custom_memory_config(memory_config)
    )

    if envelope_validator:
        config = config.with_envelope_validator(envelope_validator)

    logger.info(
        "memory_optimized_router_configuration_created",
        exchange=exchange_name,
        mode=PerformanceMode.MEMORY_OPTIMIZED.value,
        pool_size=memory_config.pool_size,
        memory_limit_mb=memory_limit_mb,
    )

    return config


def auto_configure_router(
    exchange_name: str,
    exchange_type: ExchangeType,
    error_handler: BaseErrorHandler,
    envelope_validator: Callable[[dict[str, Any]], Any] | None = None,
    message_rate_per_second: int | None = None,
    memory_limit_mb: float | None = None,
    latency_requirement_ms: float | None = None,
) -> RouterConfiguration:
    """Automatically configure router based on requirements.

    Args:
        exchange_name: Name of the exchange
        exchange_type: Type of exchange
        error_handler: Error handler instance
        envelope_validator: Optional envelope validator
        message_rate_per_second: Expected message processing rate
        memory_limit_mb: Memory limit in megabytes
        latency_requirement_ms: Maximum acceptable latency

    Returns:
        Router configuration optimized for the specified requirements
    """
    # Get recommended performance mode
    recommended_mode = get_recommended_mode_for_scenario(
        message_rate_per_second=message_rate_per_second or 100,
        memory_limit_mb=memory_limit_mb,
        latency_requirement_ms=latency_requirement_ms,
    )

    logger.info(
        "auto_router_configuration_started",
        exchange=exchange_name,
        recommended_mode=recommended_mode.value,
        message_rate=message_rate_per_second,
        memory_limit_mb=memory_limit_mb,
        latency_requirement_ms=latency_requirement_ms,
    )

    # Create configuration based on recommended mode
    if recommended_mode == PerformanceMode.HIGH_FREQUENCY:
        return create_high_frequency_router(
            exchange_name,
            exchange_type,
            error_handler,
            envelope_validator,
            message_rate_per_second,
        )
    if recommended_mode == PerformanceMode.ULTRA_LOW_LATENCY:
        return create_ultra_low_latency_router(
            exchange_name,
            exchange_type,
            error_handler,
            envelope_validator,
        )
    if recommended_mode == PerformanceMode.MEMORY_OPTIMIZED:
        return create_memory_optimized_router(
            exchange_name,
            exchange_type,
            error_handler,
            envelope_validator,
            memory_limit_mb,
        )
    return create_standard_router(exchange_name, exchange_type, error_handler, envelope_validator)


# Example usage
if __name__ == "__main__":
    # This would be used in actual router implementations
    logger.info("router_factory_examples_started")

    # Example configurations
    examples = [
        {
            "name": "Standard Development Router",
            "factory_func": "create_standard_router",
            "description": "Regular trading scenarios, development testing",
        },
        {
            "name": "High-Frequency Trading Router",
            "factory_func": "create_high_frequency_router",
            "description": "Algorithmic trading, 1000+ messages/sec",
        },
        {
            "name": "Ultra-Low Latency Router",
            "factory_func": "create_ultra_low_latency_router",
            "description": "Market making, sub-millisecond requirements",
        },
        {
            "name": "Memory-Optimized Router",
            "factory_func": "create_memory_optimized_router",
            "description": "Memory-constrained environments, embedded systems",
        },
        {
            "name": "Auto-Configured Router",
            "factory_func": "auto_configure_router",
            "description": "Automatically optimized based on requirements",
        },
    ]

    for example in examples:
        logger.info(
            "router_factory_example",
            name=example["name"],
            factory_function=example["factory_func"],
            description=example["description"],
        )
