"""Factory for creating WebSocket error handlers with proper configuration.

This factory provides a centralized way to create error handlers with
the appropriate configuration for different exchanges and environments.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from cyberdelta.apis.websocket.ws_error_metrics import WebSocketErrorMetrics
from cyberdelta.apis.websocket.ws_stream_error_handler import (
    RecoveryHandlerProtocol,
    WebSocketStreamErrorHandler,
)
from cyberdelta.apis.websocket.ws_stream_recovery import (
    ConnectionManagerProtocol,
    StateManagerProtocol,
    StreamRecoverySystem,
    SubscriptionManagerProtocol,
)
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorAlertingConfig,
    WebSocketErrorConfig,
    WebSocketErrorLoggingConfig,
    WebSocketErrorMetricsConfig,
    WebSocketErrorRecoveryConfig,
)
from cyberdelta.enums import ExchangeName


if TYPE_CHECKING:
    from logging import Logger


# ============================================================================
# Factory Implementation
# ============================================================================


class WebSocketErrorHandlerFactory:
    """Factory for creating WebSocket error handlers.

    This factory creates properly configured error handlers for different
    exchanges and environments, ensuring consistent configuration and
    dependency injection.
    """

    @staticmethod
    def create_handler(
        exchange: ExchangeName,
        config: WebSocketErrorConfig,
        logger: Logger | None = None,
        metrics_collector: WebSocketErrorMetrics | None = None,
        connection_manager: ConnectionManagerProtocol | None = None,
        subscription_manager: SubscriptionManagerProtocol | None = None,
        state_manager: StateManagerProtocol | None = None,
    ) -> WebSocketStreamErrorHandler:
        """Create a WebSocket error handler for the specified exchange.

        Args:
            exchange: Exchange enum value
            config: Error handling configuration
            logger: Optional logger instance
            metrics_collector: Optional metrics collector
            connection_manager: Optional connection manager for recovery
            subscription_manager: Optional subscription manager for recovery
            state_manager: Optional state manager for recovery

        Returns:
            WebSocketStreamErrorHandler: Configured error handler

        Raises:
            ValueError: If exchange is not supported
        """
        # Validate supported exchanges
        supported_exchanges = {ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK}
        if exchange not in supported_exchanges:
            raise ValueError(
                f"Unsupported exchange '{exchange.value}'. "
                f"Supported exchanges: {', '.join(sorted(e.value for e in supported_exchanges))}"
            )

        # Create exchange-specific logger
        if logger is None:
            logger = logging.getLogger(f"websocket.{exchange.value}.error_handler")

        # Create recovery handler if managers are provided
        recovery_handler: RecoveryHandlerProtocol | None = None
        if any([connection_manager, subscription_manager, state_manager]):
            recovery_handler = StreamRecoverySystem(
                config=config.recovery,
                connection_manager=connection_manager,
                subscription_manager=subscription_manager,
                state_manager=state_manager,
                logger=logging.getLogger(f"websocket.{exchange.value}.recovery"),
            )

        # Create and configure the error handler
        handler = WebSocketStreamErrorHandler(
            config=config,
            logger=logger,
            metrics_collector=metrics_collector,
            recovery_handler=recovery_handler,
        )

        # Log handler creation
        logger.info(
            "Created WebSocket error handler for exchange '%s' with recovery='%s', metrics='%s'",
            exchange.value,
            "enabled" if recovery_handler else "disabled",
            "enabled" if metrics_collector else "disabled",
        )

        return handler

    @staticmethod
    def create_default_config(
        exchange: ExchangeName,
        environment: str = "production",
    ) -> WebSocketErrorConfig:
        """Create default configuration for an exchange.

        Args:
            exchange: Exchange name
            environment: Environment (production, staging, development)

        Returns:
            WebSocketErrorConfig: Default configuration for the exchange

        Raises:
            ValueError: If exchange or environment is not supported
        """
        environment = environment.lower().strip()

        # Validate inputs
        supported_exchanges = {ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK}
        if exchange not in supported_exchanges:
            raise ValueError(
                f"Unsupported exchange '{exchange.value}'. "
                f"Supported: {', '.join(sorted(e.value for e in supported_exchanges))}"
            )

        supported_envs = {"production", "staging", "development", "test"}
        if environment not in supported_envs:
            raise ValueError(
                f"Unsupported environment '{environment}'. "
                f"Supported: {', '.join(sorted(supported_envs))}"
            )

        # Create exchange-specific recovery configuration
        if exchange == ExchangeName.HYPERLIQUID:
            recovery_config = WebSocketErrorRecoveryConfig(
                # Hyperliquid-specific settings
                max_recovery_attempts=5,  # Hyperliquid can be more volatile
                initial_backoff_ms=2000,  # Longer initial backoff
                max_backoff_ms=60000,  # 1 minute max
                backoff_multiplier=2.0,
                jitter_enabled=True,
                jitter_factor=0.15,  # 15% jitter
                # Reconnection settings
                max_reconnect_attempts=10,
                reconnect_delay_ms=5000,
                switch_endpoint_after_failures=3,
                # Circuit breaker
                circuit_breaker_enabled=True,
                circuit_breaker_threshold=8,  # Higher threshold for volatile exchange
                circuit_breaker_timeout_ms=45000,  # 45 seconds
                circuit_breaker_half_open_requests=2,
                # Sequence handling
                max_acceptable_sequence_gap=200,  # Higher tolerance
                sequence_gap_recovery_enabled=True,
                sequence_gap_recovery_method="request_missing",
            )

        elif exchange == ExchangeName.BACKPACK:
            recovery_config = WebSocketErrorRecoveryConfig(
                # Backpack-specific settings
                max_recovery_attempts=3,  # More conservative
                initial_backoff_ms=1000,  # Standard backoff
                max_backoff_ms=30000,  # 30 seconds max
                backoff_multiplier=1.8,  # Slightly less aggressive
                jitter_enabled=True,
                jitter_factor=0.10,  # 10% jitter
                # Reconnection settings
                max_reconnect_attempts=5,
                reconnect_delay_ms=3000,
                switch_endpoint_after_failures=2,
                # Circuit breaker
                circuit_breaker_enabled=True,
                circuit_breaker_threshold=5,  # Lower threshold
                circuit_breaker_timeout_ms=30000,  # 30 seconds
                circuit_breaker_half_open_requests=1,
                # Sequence handling
                max_acceptable_sequence_gap=50,  # Lower tolerance
                sequence_gap_recovery_enabled=True,
                sequence_gap_recovery_method="full_resync",
            )

        else:
            # This should never happen due to validation above
            raise ValueError(f"Unsupported exchange: {exchange}")

        # Adjust settings based on environment
        if environment == "development":
            # More aggressive settings for development
            recovery_config.max_recovery_attempts = min(
                recovery_config.max_recovery_attempts * 2, 10
            )
            recovery_config.circuit_breaker_threshold *= 2
            recovery_config.circuit_breaker_timeout_ms = min(
                recovery_config.circuit_breaker_timeout_ms * 2, 120000
            )

        elif environment == "test":
            # Conservative settings for testing
            recovery_config.max_recovery_attempts = 1
            recovery_config.circuit_breaker_enabled = False
            recovery_config.sequence_gap_recovery_enabled = False

        # Create environment-specific configurations
        metrics_config = WebSocketErrorMetricsConfig(
            enable_metrics_collection=environment != "test",
            metrics_buffer_size=1000 if environment == "production" else 100,
            metrics_flush_interval_ms=60000,  # 1 minute
            track_error_rates=True,
            error_rate_window_ms=300000,  # 5 minutes
            error_rate_buckets=10,
            track_recovery_times=True,
            track_connection_durations=environment == "production",
            track_message_latencies=environment == "production",
            track_error_chains=True,
            max_error_chain_depth=5,
            track_stack_traces=environment == "development",
        )

        alerting_config = WebSocketErrorAlertingConfig(
            enable_alerting=environment == "production",
            critical_error_threshold=1,
            error_rate_alert_threshold=0.1,  # 10%
            connection_failure_alert_threshold=5,
            alert_cooldown_ms=300000,  # 5 minutes
            alert_aggregation_window_ms=60000,  # 1 minute
            log_alerts=True,
            console_alerts=environment in {"development", "test"},
            webhook_alerts=environment == "production",
            webhook_url=None,  # Would be set from environment variables
        )

        logging_config = WebSocketErrorLoggingConfig(
            log_all_errors=environment == "development",
            min_severity_to_log="INFO" if environment == "production" else "DEBUG",
            log_error_context=True,
            log_recovery_attempts=True,
            structured_logging=environment == "production",
            include_timestamps=True,
            include_correlation_ids=True,
            filter_sensitive_data=True,
            max_log_message_length=1000,
        )

        # Create the complete configuration
        return WebSocketErrorConfig(
            recovery=recovery_config,
            metrics=metrics_config,
            alerting=alerting_config,
            logging=logging_config,
        )

    @staticmethod
    def create_minimal_handler(
        exchange: ExchangeName,
        logger: Logger | None = None,
    ) -> WebSocketStreamErrorHandler:
        """Create a minimal error handler for testing or simple use cases.

        Args:
            exchange: Exchange enum value
            logger: Optional logger instance

        Returns:
            WebSocketStreamErrorHandler: Minimal error handler
        """
        # Create minimal configuration
        minimal_config = WebSocketErrorHandlerFactory.create_default_config(
            exchange=exchange,
            environment="test",
        )

        # Create minimal handler without recovery or metrics
        return WebSocketErrorHandlerFactory.create_handler(
            exchange=exchange,
            config=minimal_config,
            logger=logger,
            metrics_collector=None,
            connection_manager=None,
            subscription_manager=None,
            state_manager=None,
        )

    @staticmethod
    def validate_configuration(config: WebSocketErrorConfig) -> list[str]:
        """Validate error handler configuration.

        Args:
            config: Configuration to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors: list[str] = []

        # Validate recovery config
        if config.recovery.max_recovery_attempts < 0:
            errors.append("max_recovery_attempts must be non-negative")

        if config.recovery.initial_backoff_ms < 100:
            errors.append("initial_backoff_ms must be at least 100ms")

        if config.recovery.max_backoff_ms < config.recovery.initial_backoff_ms:
            errors.append("max_backoff_ms must be >= initial_backoff_ms")

        if config.recovery.backoff_multiplier < 1.0:
            errors.append("backoff_multiplier must be >= 1.0")

        if not 0.0 <= config.recovery.jitter_factor <= 0.5:
            errors.append("jitter_factor must be between 0.0 and 0.5")

        if config.recovery.circuit_breaker_threshold < 1:
            errors.append("circuit_breaker_threshold must be >= 1")

        if config.recovery.circuit_breaker_timeout_ms < 5000:
            errors.append("circuit_breaker_timeout_ms must be at least 5 seconds")

        # Validate metrics config
        if config.metrics.metrics_buffer_size < 10:
            errors.append("metrics_buffer_size must be at least 10")

        if config.metrics.metrics_flush_interval_ms < 1000:
            errors.append("metrics_flush_interval_ms must be at least 1 second")

        if config.metrics.error_rate_buckets < 1:
            errors.append("error_rate_buckets must be at least 1")

        # Validate alerting config
        if config.alerting.critical_error_threshold < 1:
            errors.append("critical_error_threshold must be at least 1")

        if not 0.0 < config.alerting.error_rate_alert_threshold <= 1.0:
            errors.append("error_rate_alert_threshold must be between 0.0 and 1.0")

        if config.alerting.alert_cooldown_ms < 30000:
            errors.append("alert_cooldown_ms must be at least 30 seconds")

        # Validate logging config
        valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if config.logging.min_severity_to_log.upper() not in valid_log_levels:
            errors.append(f"min_severity_to_log must be one of: {', '.join(valid_log_levels)}")

        if config.logging.max_log_message_length < 100:
            errors.append("max_log_message_length must be at least 100")

        return errors
