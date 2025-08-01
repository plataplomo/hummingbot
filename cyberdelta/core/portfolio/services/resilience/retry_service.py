"""Retry service for handling operation retries with exponential backoff."""

from __future__ import annotations

import asyncio
import secrets
from typing import Any, Awaitable, Callable, TypeVar

from pydantic import Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import ResilienceConfigurationError
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

logger = get_logger(__name__)

T = TypeVar("T", bound=object)


@dataclass
class RetryConfig:
    """Configuration for retry mechanisms."""

    max_attempts: int = Field(default=3, gt=0, le=10, description="Maximum retry attempts")
    initial_delay: float = Field(
        default=1.0, gt=0, le=60, description="Initial retry delay in seconds"
    )
    max_delay: float = Field(
        default=60.0, gt=0, le=300, description="Maximum retry delay in seconds"
    )
    backoff_multiplier: float = Field(
        default=2.0, gt=1, le=10, description="Exponential backoff multiplier"
    )
    jitter: bool = Field(default=True, description="Add jitter to retry delays")
    retriable_exceptions: tuple[type[Exception], ...] = Field(
        default_factory=lambda: (Exception,), description="Exceptions to retry"
    )

    @field_validator("max_delay", mode="before")
    @classmethod
    def validate_delays(cls, v: float, info: ValidationInfo) -> float:
        if "initial_delay" in info.data and v <= info.data["initial_delay"]:
            raise ResilienceConfigurationError(
                config_type="retry_delay",
                invalid_relationship="max_delay must be greater than initial_delay",
            )
        return v


class RetryMechanism:
    """Retry mechanism with exponential backoff and jitter."""

    def __init__(self, config: RetryConfig) -> None:
        self.config = config

    async def execute(
        self, func: Callable[..., Awaitable[T]], *args: object, **kwargs: object
    ) -> T:
        """Execute function with retry logic."""
        last_exception = None
        delay = self.config.initial_delay

        for attempt in range(self.config.max_attempts):
            try:
                result = func(*args, **kwargs)
                return await result

            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                last_exception = e

                # Check if exception is retriable
                if not any(
                    isinstance(e, exc_type) for exc_type in self.config.retriable_exceptions
                ):
                    logger.warning(
                        "non_retriable_exception",
                        exception_type=type(e).__name__,
                        attempt=attempt + 1,
                        max_attempts=self.config.max_attempts,
                    )
                    raise

                # Don't retry on last attempt
                if attempt == self.config.max_attempts - 1:
                    break

                # Calculate delay with jitter
                actual_delay = delay
                if self.config.jitter:
                    actual_delay *= 0.5 + secrets.SystemRandom().random() * 0.5

                logger.info(
                    "retry_attempt",
                    attempt=attempt + 1,
                    max_attempts=self.config.max_attempts,
                    delay=actual_delay,
                    exception_type=type(e).__name__,
                )

                await asyncio.sleep(actual_delay)

                # Exponential backoff
                delay = min(delay * self.config.backoff_multiplier, self.config.max_delay)

        # All attempts failed
        logger.error(
            "retry_exhausted",
            max_attempts=self.config.max_attempts,
            final_exception=str(last_exception),
        )
        if last_exception:
            raise last_exception
        raise RuntimeError("All retry attempts failed")


class RetryService(BasePortfolioService):
    """Service for managing retry mechanisms across portfolio operations."""

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__("retry_service", config)
        self._raw_config = config or {}
        self.logger = get_logger(__name__)
        
        # Retry configurations per service
        self.service_configs: dict[str, RetryConfig] = {}
        self.default_config = self._init_default_config()
        self.retry_mechanisms: dict[str, RetryMechanism] = {}

    async def _initialize_service(self) -> None:
        """Initialize retry service."""
        self.logger.info("Initializing retry service")

    async def _shutdown_service(self) -> None:
        """Shutdown retry service."""
        self.logger.info("Shutting down retry service")

    async def _start_internal(self) -> None:
        """Start internal retry operations."""
        pass

    async def _stop_internal(self) -> None:
        """Stop internal retry operations."""
        pass

    def _init_default_config(self) -> RetryConfig:
        """Initialize default retry configuration."""
        max_attempts = self._raw_config.get("default_max_attempts", 3)
        initial_delay = self._raw_config.get("default_initial_delay", 1.0)
        max_delay = self._raw_config.get("default_max_delay", 60.0)
        backoff_multiplier = self._raw_config.get("default_backoff_multiplier", 2.0)
        jitter = self._raw_config.get("default_jitter", True)

        return RetryConfig(
            max_attempts=max_attempts,
            initial_delay=float(initial_delay),
            max_delay=float(max_delay),
            backoff_multiplier=float(backoff_multiplier),
            jitter=jitter,
        )

    def register_retry_config(self, service_name: str, config: RetryConfig) -> None:
        """Register retry configuration for a service."""
        self.service_configs[service_name] = config
        self.retry_mechanisms[service_name] = RetryMechanism(config)
        
        self.logger.info(
            "retry_config_registered",
            service_name=service_name,
            max_attempts=config.max_attempts,
            initial_delay=config.initial_delay,
        )

    def get_retry_mechanism(self, service_name: str) -> RetryMechanism:
        """Get retry mechanism for a service."""
        if service_name not in self.retry_mechanisms:
            config = self.service_configs.get(service_name, self.default_config)
            self.retry_mechanisms[service_name] = RetryMechanism(config)
        
        return self.retry_mechanisms[service_name]

    async def execute_with_retry(
        self,
        service_name: str,
        func: Callable[..., Awaitable[T]],
        *args: object,
        custom_config: RetryConfig | None = None,
        **kwargs: object,
    ) -> T:
        """Execute function with retry logic."""
        if custom_config:
            retry_mechanism = RetryMechanism(custom_config)
        else:
            retry_mechanism = self.get_retry_mechanism(service_name)

        return await retry_mechanism.execute(func, *args, **kwargs)

    def get_retry_stats(self) -> dict[str, Any]:
        """Get retry statistics and configurations."""
        return {
            "registered_services": list(self.service_configs.keys()),
            "default_config": {
                "max_attempts": self.default_config.max_attempts,
                "initial_delay": self.default_config.initial_delay,
                "max_delay": self.default_config.max_delay,
                "backoff_multiplier": self.default_config.backoff_multiplier,
                "jitter": self.default_config.jitter,
            },
            "service_configs": {
                name: {
                    "max_attempts": config.max_attempts,
                    "initial_delay": config.initial_delay,
                    "max_delay": config.max_delay,
                    "backoff_multiplier": config.backoff_multiplier,
                    "jitter": config.jitter,
                }
                for name, config in self.service_configs.items()
            },
        }

    def update_default_config(self, config: RetryConfig) -> None:
        """Update default retry configuration."""
        self.default_config = config
        self.logger.info("default_retry_config_updated", max_attempts=config.max_attempts)