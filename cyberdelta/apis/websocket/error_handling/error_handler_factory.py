"""Factory for creating WebSocket error handlers with proper configuration.

This factory provides a centralized way to create error handlers with
the appropriate configuration for different exchanges and environments.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from cyberdelta.apis.websocket.error_handling.recovery import (
    ConnectionManagerProtocol,
    StateManagerProtocol,
    SubscriptionManagerProtocol,
)
from cyberdelta.apis.websocket.error_handling.recovery.recovery_executor import RecoveryExecutor
from cyberdelta.apis.websocket.error_handling.recovery.recovery_policy import RecoveryPolicyManager
from cyberdelta.apis.websocket.error_handling.stream_error_handler import (
    WebSocketStreamErrorHandler,
)
from cyberdelta.apis.websocket.exceptions import WebSocketConfigurationError
from cyberdelta.apis.websocket.metrics.error_metrics import WebSocketErrorMetrics
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


class AppConfigProtocol(Protocol):
    """Protocol for application configuration objects that provide WebSocket error config."""

    @property
    def websocket_error_config(self) -> WebSocketErrorConfig:
        """Get WebSocket error configuration with get_exchange_config method."""
        ...


# Constants for validation and configuration
MAX_RETRY_COUNT = 100
MIN_BACKOFF_MULTIPLIER = 0.5
MAX_TIMEOUT_MS = 5000
MIN_CIRCUIT_BREAKER_COUNT = 10
MAX_CIRCUIT_BREAKER_TIMEOUT = 1000
MAX_DEV_CIRCUIT_BREAKER_TIMEOUT = 30000
MAX_PRODUCTION_RETRY_COUNT = 100

# Validation constants
MIN_INITIAL_BACKOFF_MS = 100
MIN_JITTER_FACTOR = 0.0
MAX_JITTER_FACTOR = 0.5
MIN_CIRCUIT_BREAKER_TIMEOUT_MS = 5000
MIN_METRICS_BUFFER_SIZE = 10
MIN_METRICS_FLUSH_INTERVAL_MS = 1000
MIN_ALERT_COOLDOWN_MS = 30000  # 30 seconds
MIN_LOG_MESSAGE_LENGTH = 100


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
            WebSocketConfigurationError: If exchange is not supported
        """
        # Validate supported exchanges
        supported_exchanges = {ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK}
        if exchange not in supported_exchanges:
            supported_list = [e.value for e in supported_exchanges]
            error_msg = (
                f"Unsupported exchange '{exchange.value}'. "
                f"Supported exchanges are: {', '.join(supported_list)}. "
                f"Please provide a valid exchange from the supported list."
            )
            raise WebSocketConfigurationError(
                message=error_msg,
                error_id=str(uuid4()),
                correlation_id=str(uuid4()),
            )

        # Create exchange-specific logger
        if logger is None:
            logger = logging.getLogger(f"websocket.{exchange.value}.error_handler")

        # Create unified recovery system
        if any([connection_manager, subscription_manager, state_manager]):
            # Create unified recovery system with managers
            recovery_policy = RecoveryPolicyManager(config)
            recovery_executor = RecoveryExecutor(
                policy=recovery_policy,
                connection_manager=connection_manager,
                subscription_manager=subscription_manager,
                state_manager=state_manager,
            )
        else:
            # Create default unified system
            recovery_policy = RecoveryPolicyManager(config)
            recovery_executor = RecoveryExecutor(policy=recovery_policy)

        # Create and configure the error handler
        # Note: Pass None for logger since WebSocketStreamErrorHandler implements TypedLogger
        # and can create its own Python logger internally
        handler = WebSocketStreamErrorHandler(
            config=config,
            logger=None,
            metrics_collector=metrics_collector,
            recovery_policy=recovery_policy,
            recovery_executor=recovery_executor,
        )

        # Log handler creation
        logger.info(
            "Created WebSocket error handler for exchange '%s' with recovery='%s', metrics='%s'",
            exchange.value,
            "enabled" if recovery_policy and recovery_executor else "disabled",
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
            WebSocketConfigurationError: If exchange or environment is not supported
        """
        environment = environment.lower().strip()

        # Validate inputs
        supported_exchanges = {ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK}
        if exchange not in supported_exchanges:
            supported_list = [e.value for e in supported_exchanges]
            error_msg = (
                f"Unsupported exchange '{exchange.value}'. "
                f"Supported exchanges are: {', '.join(supported_list)}. "
                f"Please provide a valid exchange from the supported list."
            )
            raise WebSocketConfigurationError(
                message=error_msg,
                error_id=str(uuid4()),
                correlation_id=str(uuid4()),
            )

        supported_envs = {"production", "staging", "development", "test"}
        if environment not in supported_envs:
            supported_list = sorted(supported_envs)
            error_msg = (
                f"Unsupported environment '{environment}'. "
                f"Supported environments are: {', '.join(supported_list)}. "
                f"Please provide a valid environment from the supported list."
            )
            raise WebSocketConfigurationError(
                message=error_msg,
                error_id=str(uuid4()),
                correlation_id=str(uuid4()),
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

        # Note: No else clause needed - validation above ensures only supported exchanges reach here

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
    def create_handler_from_app_config(
        exchange: ExchangeName,
        app_config_getter: AppConfigProtocol | Callable[[ExchangeName], WebSocketErrorConfig],
        environment: str = "production",
        connection_manager: ConnectionManagerProtocol | None = None,
        subscription_manager: SubscriptionManagerProtocol | None = None,
        state_manager: StateManagerProtocol | None = None,
        metrics_collector: WebSocketErrorMetrics | None = None,
    ) -> WebSocketStreamErrorHandler:
        """Create handler using application configuration.

        This method provides a higher-level interface that extracts
        WebSocket-specific configuration from the application config.

        Args:
            exchange: Exchange name
            app_config_getter: AppSettings instance or callable that provides websocket config
            environment: Environment name for configuration
            connection_manager: Optional connection manager
            subscription_manager: Optional subscription manager
            state_manager: Optional state manager
            metrics_collector: Optional metrics collector

        Returns:
            WebSocketStreamErrorHandler: Configured error handler

        Note:
            Uses WebSocketErrorHandlerFactory.create_default_config() as fallback
        """
        # Extract WebSocket configuration from app config
        websocket_config: WebSocketErrorConfig

        if callable(app_config_getter):
            # Callable that returns websocket config
            websocket_config = app_config_getter(exchange)
        elif hasattr(app_config_getter, "websocket_error_config"):
            # App config object with websocket_error_config attribute
            config_obj = app_config_getter.websocket_error_config
            websocket_config = config_obj.get_exchange_config(exchange)
        else:
            # Fall back to default configuration
            websocket_config = WebSocketErrorHandlerFactory.create_default_config(
                exchange=exchange,
                environment=environment,
            )

        return WebSocketErrorHandlerFactory.create_handler(
            exchange=exchange,
            config=websocket_config,
            logger=None,  # Let handler create its own logger
            metrics_collector=metrics_collector,
            connection_manager=connection_manager,
            subscription_manager=subscription_manager,
            state_manager=state_manager,
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

        errors.extend(WebSocketErrorHandlerFactory._validate_recovery_config(config.recovery))
        errors.extend(WebSocketErrorHandlerFactory._validate_metrics_config(config.metrics))
        errors.extend(WebSocketErrorHandlerFactory._validate_alerting_config(config.alerting))
        errors.extend(WebSocketErrorHandlerFactory._validate_logging_config(config.logging))

        return errors

    @staticmethod
    def _validate_recovery_config(config: WebSocketErrorRecoveryConfig) -> list[str]:
        """Validate recovery configuration.

        Args:
            config: Recovery configuration to validate

        Returns:
            List of validation error messages.
        """
        errors: list[str] = []

        if config.max_recovery_attempts < 0:
            errors.append("max_recovery_attempts must be non-negative")

        if config.initial_backoff_ms < MIN_INITIAL_BACKOFF_MS:
            errors.append(f"initial_backoff_ms must be at least {MIN_INITIAL_BACKOFF_MS}ms")

        if config.max_backoff_ms < config.initial_backoff_ms:
            errors.append("max_backoff_ms must be >= initial_backoff_ms")

        if config.backoff_multiplier < 1.0:
            errors.append("backoff_multiplier must be >= 1.0")

        if not MIN_JITTER_FACTOR <= config.jitter_factor <= MAX_JITTER_FACTOR:
            errors.append(
                f"jitter_factor must be between {MIN_JITTER_FACTOR} and {MAX_JITTER_FACTOR}"
            )

        if config.circuit_breaker_threshold < 1:
            errors.append("circuit_breaker_threshold must be >= 1")

        if config.circuit_breaker_timeout_ms < MIN_CIRCUIT_BREAKER_TIMEOUT_MS:
            timeout_seconds = MIN_CIRCUIT_BREAKER_TIMEOUT_MS // 1000
            errors.append(f"circuit_breaker_timeout_ms must be at least {timeout_seconds} seconds")

        return errors

    @staticmethod
    def _validate_metrics_config(config: WebSocketErrorMetricsConfig) -> list[str]:
        """Validate metrics configuration.

        Args:
            config: Metrics configuration to validate

        Returns:
            List of validation error messages.
        """
        errors: list[str] = []

        if config.metrics_buffer_size < MIN_METRICS_BUFFER_SIZE:
            errors.append(f"metrics_buffer_size must be at least {MIN_METRICS_BUFFER_SIZE}")

        if config.metrics_flush_interval_ms < MIN_METRICS_FLUSH_INTERVAL_MS:
            flush_seconds = MIN_METRICS_FLUSH_INTERVAL_MS // 1000
            errors.append(f"metrics_flush_interval_ms must be at least {flush_seconds} second")

        if config.error_rate_buckets < 1:
            errors.append("error_rate_buckets must be at least 1")

        if config.latency_histogram_buckets < 1:
            errors.append("latency_histogram_buckets must be at least 1")

        return errors

    @staticmethod
    def _validate_alerting_config(config: WebSocketErrorAlertingConfig) -> list[str]:
        """Validate alerting configuration.

        Args:
            config: Alerting configuration to validate

        Returns:
            List of validation error messages.
        """
        errors: list[str] = []

        if config.critical_error_threshold < 1:
            errors.append("critical_error_threshold must be at least 1")

        if not 0.0 < config.error_rate_alert_threshold <= 1.0:
            errors.append("error_rate_alert_threshold must be between 0.0 and 1.0")

        if config.alert_cooldown_ms < MIN_ALERT_COOLDOWN_MS:
            cooldown_seconds = MIN_ALERT_COOLDOWN_MS // 1000
            errors.append(f"alert_cooldown_ms must be at least {cooldown_seconds} seconds")

        return errors

    @staticmethod
    def _validate_logging_config(config: WebSocketErrorLoggingConfig) -> list[str]:
        """Validate logging configuration.

        Args:
            config: Logging configuration to validate

        Returns:
            List of validation error messages.
        """
        errors: list[str] = []

        valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if config.min_severity_to_log.upper() not in valid_log_levels:
            errors.append(f"min_severity_to_log must be one of: {', '.join(valid_log_levels)}")

        if config.max_log_message_length < MIN_LOG_MESSAGE_LENGTH:
            errors.append(f"max_log_message_length must be at least {MIN_LOG_MESSAGE_LENGTH}")

        return errors
