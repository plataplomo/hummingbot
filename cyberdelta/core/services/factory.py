"""Service factory for ExecutionHandler refactoring.

This module provides dependency injection container and service factory
for easy instantiation and configuration of all extracted services.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger
from cyberdelta.core.services.compensation import CompensationService
from cyberdelta.core.services.error_handling import ExecutionErrorHandler
from cyberdelta.core.services.interfaces import (
    CompensationConfig,
    OrderServiceConfig,
    StateManagerConfig,
    ValidationConfig,
)
from cyberdelta.core.services.order_management import OrderManagementService
from cyberdelta.core.services.state_management import ThreadSafeExecutionStateManager
from cyberdelta.core.services.validation import ExecutionInputValidator


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.config.models.config_models import AppSettings
    from cyberdelta.core.services.interfaces import IAlertService
    from cyberdelta.core.symbol_mapper import SymbolMapper
    from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem


class ServiceFactory:
    """Factory for creating and configuring execution services."""

    def __init__(
        self,
        api_clients: dict[str, ExchangeAPI],
        symbol_mapper: SymbolMapper,
        app_settings: AppSettings,
        circuit_breaker: CircuitBreakerSystem | None = None,
        alert_service: IAlertService | None = None,
        logger: TraceLevelLogger | None = None,
    ) -> None:
        """Initialize service factory.

        Args:
            api_clients: Dictionary of exchange API clients
            symbol_mapper: Symbol mapping service
            app_settings: Application settings
            circuit_breaker: Optional circuit breaker system
            alert_service: Optional alert service
            logger: Optional logger instance
        """
        self.api_clients = api_clients
        self.symbol_mapper = symbol_mapper
        self.app_settings = app_settings
        self.circuit_breaker = circuit_breaker
        self.alert_service = alert_service
        self.logger = logger or get_logger(__name__)

        # Cache for created services
        self._services: dict[str, Any] = {}

    def create_error_handler(self) -> ExecutionErrorHandler:
        """Create error handling service.

        Returns:
            Configured ExecutionErrorHandler instance
        """
        if "error_handler" not in self._services:
            self._services["error_handler"] = ExecutionErrorHandler(
                circuit_breaker=self.circuit_breaker, logger=self.logger
            )
        service = self._services["error_handler"]
        if not isinstance(service, ExecutionErrorHandler):
            msg = f"Expected ExecutionErrorHandler, got {type(service)}"
            raise TypeError(msg)
        return service

    def create_order_service(self) -> OrderManagementService:
        """Create order management service.

        Returns:
            Configured OrderManagementService instance
        """
        if "order_service" not in self._services:
            # Create configuration from app settings
            config = OrderServiceConfig()
            if hasattr(self.app_settings, "execution"):
                execution_settings = self.app_settings.execution
                if hasattr(execution_settings, "max_retries"):
                    config.max_retries = execution_settings.max_retries
                if hasattr(execution_settings, "retry_delay_base_sec"):
                    config.retry_delay_base_seconds = float(execution_settings.retry_delay_base_sec)
                # max_retry_delay_seconds and order_timeout_seconds are not in ExecutionSettings
                # They use the defaults from OrderServiceConfig

            self._services["order_service"] = OrderManagementService(
                api_clients=self.api_clients,
                error_handler=self.create_error_handler(),
                circuit_breaker=self.circuit_breaker,
                config=config,
                logger=self.logger,
            )
        service = self._services["order_service"]
        if not isinstance(service, OrderManagementService):
            msg = f"Expected OrderManagementService, got {type(service)}"
            raise TypeError(msg)
        return service

    def create_state_manager(self) -> ThreadSafeExecutionStateManager:
        """Create execution state manager.

        Returns:
            Configured ThreadSafeExecutionStateManager instance
        """
        if "state_manager" not in self._services:
            # Create configuration from app settings
            config = StateManagerConfig()
            # Note: ExecutionSettings doesn't have state manager specific configs
            # Using defaults from StateManagerConfig

            self._services["state_manager"] = ThreadSafeExecutionStateManager(
                config=config, logger=self.logger
            )
        service = self._services["state_manager"]
        if not isinstance(service, ThreadSafeExecutionStateManager):
            msg = f"Expected ThreadSafeExecutionStateManager, got {type(service)}"
            raise TypeError(msg)
        return service

    def create_input_validator(self) -> ExecutionInputValidator:
        """Create input validation service.

        Returns:
            Configured ExecutionInputValidator instance
        """
        if "input_validator" not in self._services:
            # Create configuration from app settings
            config = ValidationConfig()
            # Note: ExecutionSettings doesn't have validation-specific configs
            # These fields are not present in ExecutionSettings model
            # Using defaults from ValidationConfig

            self._services["input_validator"] = ExecutionInputValidator(
                api_clients=self.api_clients,
                symbol_mapper=self.symbol_mapper,
                config=config,
                logger=self.logger,
            )
        service = self._services["input_validator"]
        if not isinstance(service, ExecutionInputValidator):
            msg = f"Expected ExecutionInputValidator, got {type(service)}"
            raise TypeError(msg)
        return service

    def create_compensation_service(self) -> CompensationService:
        """Create compensation service.

        Returns:
            Configured CompensationService instance
        """
        if "compensation_service" not in self._services:
            # Create configuration from app settings
            config = CompensationConfig()
            if hasattr(self.app_settings, "execution"):
                execution_settings = self.app_settings.execution
                if hasattr(execution_settings, "compensation"):
                    comp_settings = execution_settings.compensation
                    # Map ExecutionCompensationSettings to CompensationConfig
                    if hasattr(comp_settings, "use_limit_orders"):
                        config.use_limit_orders = comp_settings.use_limit_orders
                    if hasattr(comp_settings, "limit_price_offset_pct"):
                        config.limit_price_offset_pct = comp_settings.limit_price_offset_pct

                # Set monitor_timeout_seconds based on execution settings
                # Use a reasonable multiple of retry delay * max retries as timeout
                if hasattr(execution_settings, "max_retries") and hasattr(
                    execution_settings, "retry_delay_base_sec"
                ):
                    # Calculate timeout as: (max_retries * retry_delay * 2) + buffer
                    # This ensures we have enough time for all retries plus some buffer
                    base_timeout = float(
                        execution_settings.max_retries * execution_settings.retry_delay_base_sec * 2
                    )
                    buffer_time = 60.0  # 1 minute buffer
                    # Cap at 10 minutes
                    config.monitor_timeout_seconds = min(base_timeout + buffer_time, 600.0)
                else:
                    # Keep default of 300 seconds if we can't calculate
                    config.monitor_timeout_seconds = 300.0

            self._services["compensation_service"] = CompensationService(
                order_service=self.create_order_service(),
                error_handler=self.create_error_handler(),
                alert_service=self.alert_service,
                config=config,
                logger=self.logger,
            )
        service = self._services["compensation_service"]
        if not isinstance(service, CompensationService):
            msg = f"Expected CompensationService, got {type(service)}"
            raise TypeError(msg)
        return service

    def create_all_services(self) -> ServiceContainer:
        """Create all services and return them in a container.

        Returns:
            ServiceContainer with all configured services
        """
        return ServiceContainer(
            error_handler=self.create_error_handler(),
            order_service=self.create_order_service(),
            state_manager=self.create_state_manager(),
            input_validator=self.create_input_validator(),
            compensation_service=self.create_compensation_service(),
        )

    async def start_services(self) -> None:
        """Start all async services."""
        services_to_start: list[Any] = [
            self.create_state_manager(),
            self.create_compensation_service(),
        ]

        for service in services_to_start:
            try:
                await service.start()
                self.logger.info("Service started", service_type=type(service).__name__)
            except Exception as e:
                self.logger.exception(
                    "Failed to start service", service_type=type(service).__name__, error=str(e)
                )
                raise

    async def stop_services(self) -> None:
        """Stop all async services."""
        services_to_stop: list[Any] = []

        # Get services that need stopping (in reverse order)
        if "compensation_service" in self._services:
            services_to_stop.append(self._services["compensation_service"])
        if "state_manager" in self._services:
            services_to_stop.append(self._services["state_manager"])

        for service in services_to_stop:
            try:
                await service.stop()
                self.logger.info("Service stopped", service_type=type(service).__name__)
            except Exception as e:
                self.logger.exception(
                    "Failed to stop service", service_type=type(service).__name__, error=str(e)
                )

    def get_service(self, service_name: str) -> object:
        """Get a service by name.

        Args:
            service_name: Name of the service to retrieve

        Returns:
            Service instance if found

        Raises:
            KeyError: If service not found
        """
        return self._services[service_name]

    def clear_cache(self) -> None:
        """Clear the service cache (for testing)."""
        self._services.clear()


class ServiceContainer:
    """Container holding all configured services for dependency injection."""

    def __init__(
        self,
        error_handler: ExecutionErrorHandler,
        order_service: OrderManagementService,
        state_manager: ThreadSafeExecutionStateManager,
        input_validator: ExecutionInputValidator,
        compensation_service: CompensationService,
    ) -> None:
        """Initialize service container.

        Args:
            error_handler: Error handling service
            order_service: Order management service
            state_manager: State management service
            input_validator: Input validation service
            compensation_service: Compensation service
        """
        self.error_handler = error_handler
        self.order_service = order_service
        self.state_manager = state_manager
        self.input_validator = input_validator
        self.compensation_service = compensation_service

    async def start_all(self) -> None:
        """Start all async services in the container."""
        await self.state_manager.start()
        await self.compensation_service.start()

    async def stop_all(self) -> None:
        """Stop all async services in the container."""
        await self.compensation_service.stop()
        await self.state_manager.stop()

    def validate_services(self) -> bool:
        """Validate that all services are properly configured.

        Returns:
            True if all services are valid
        """
        required_services: list[Any] = [
            self.error_handler,
            self.order_service,
            self.state_manager,
            self.input_validator,
            self.compensation_service,
        ]

        return all(service is not None for service in required_services)
