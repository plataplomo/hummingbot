"""Validated WebSocket Manager with Enhanced Security.

This module provides an enhanced WebSocket manager with pre-validation,
size limits, and improved security measures to prevent DoS attacks.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

import aiohttp
import orjson
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode
from cyberdelta.apis.connectivity.ws_manager import MessageHandler, WebSocketManager
from cyberdelta.config.structlog_config import get_logger


# Constants for data preview
DATA_PREVIEW_LENGTH = 100

# Constants for validation
MIN_MESSAGE_SIZE_KB = 1024  # 1KB minimum
MAX_MESSAGE_SIZE_MB = 100 * 1024 * 1024  # 100MB maximum
MAX_NESTING_DEPTH_LIMIT = 100
MAX_ARRAY_LENGTH_LIMIT = 1_000_000


class MessageSizeError(ValueError):
    """Raised when message size constraints are violated."""

    def __init__(self) -> None:
        """Initialize with default message."""
        super().__init__("Message size constraint violated")


class MessageSizeTooSmallError(MessageSizeError):
    """Raised when message size is too small."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__()
        self.args = ("max_message_size must be at least 1KB",)


class MessageSizeTooBigError(MessageSizeError):
    """Raised when message size is too large."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__()
        self.args = ("max_message_size cannot exceed 100MB",)


class NestingDepthError(ValueError):
    """Raised when nesting depth constraints are violated."""

    def __init__(self) -> None:
        """Initialize with default message."""
        super().__init__("Nesting depth constraint violated")


class NestingDepthTooSmallError(NestingDepthError):
    """Raised when nesting depth is too small."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__()
        self.args = ("max_nesting_depth must be at least 1",)


class NestingDepthTooBigError(NestingDepthError):
    """Raised when nesting depth is too large."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__()
        self.args = ("max_nesting_depth cannot exceed 100",)


class ArrayLengthError(ValueError):
    """Raised when array length constraints are violated."""

    def __init__(self) -> None:
        """Initialize with default message."""
        super().__init__("Array length constraint violated")


class ArrayLengthTooSmallError(ArrayLengthError):
    """Raised when array length is too small."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__()
        self.args = ("max_array_length must be at least 1",)


class ArrayLengthTooBigError(ArrayLengthError):
    """Raised when array length is too large."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__()
        self.args = ("max_array_length cannot exceed 1,000,000",)


if TYPE_CHECKING:
    from cyberdelta.apis.connectivity.connectivity_models import WebSocketManagerConfig
    from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime

# Type alias for task factory
TaskFactory = Callable[..., asyncio.Task[Any]]


class WebSocketMessageConfig(BaseModel):
    """Configuration for WebSocket message handling with security limits."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_message_size: int = Field(
        default=10 * 1024 * 1024,  # 10MB
        description="Maximum allowed message size in bytes",
    )
    max_nesting_depth: int = Field(
        default=10,
        description="Maximum allowed JSON nesting depth",
    )
    max_array_length: int = Field(
        default=10000,
        description="Maximum allowed array length in JSON",
    )
    parse_timeout: float = Field(
        default=1.0,
        description="Maximum time allowed for JSON parsing in seconds",
    )
    enable_compression: bool = Field(
        default=False,
        description="Whether to enable WebSocket compression",
    )

    @field_validator("max_message_size")
    @classmethod
    def validate_max_message_size(cls, v: int) -> int:
        """Validate message size is within reasonable bounds.
        
        Returns:
            int: The validated message size.
            
        Raises:
            MessageSizeTooSmallError: If message size is below minimum.
            MessageSizeTooBigError: If message size exceeds maximum.
        """
        if v < MIN_MESSAGE_SIZE_KB:
            raise MessageSizeTooSmallError
        if v > MAX_MESSAGE_SIZE_MB:
            raise MessageSizeTooBigError
        return v

    @field_validator("max_nesting_depth")
    @classmethod
    def validate_max_nesting_depth(cls, v: int) -> int:
        """Validate nesting depth is reasonable.
        
        Returns:
            int: The validated nesting depth.
            
        Raises:
            NestingDepthTooSmallError: If nesting depth is below 1.
            NestingDepthTooBigError: If nesting depth exceeds maximum.
        """
        if v < 1:
            raise NestingDepthTooSmallError
        if v > MAX_NESTING_DEPTH_LIMIT:
            raise NestingDepthTooBigError
        return v

    @field_validator("max_array_length")
    @classmethod
    def validate_max_array_length(cls, v: int) -> int:
        """Validate array length limit.
        
        Returns:
            int: The validated array length limit.
            
        Raises:
            ArrayLengthTooSmallError: If array length is below 1.
            ArrayLengthTooBigError: If array length exceeds maximum.
        """
        if v < 1:
            raise ArrayLengthTooSmallError
        if v > MAX_ARRAY_LENGTH_LIMIT:
            raise ArrayLengthTooBigError
        return v


class WebSocketPreValidator:
    """Pre-validates WebSocket messages before processing."""

    def __init__(self, config: WebSocketMessageConfig) -> None:
        """Initialize the pre-validator.

        Args:
            config: Configuration for validation limits.

        """
        self.config = config
        self.logger = get_logger(__name__)

    def validate(
        self, data: dict[str, Any] | list[Any] | str | float | bool | None
    ) -> dict[str, Any] | list[Any]:
        """Validate the structure of parsed JSON data.

        Args:
            data: The parsed JSON data to validate.

        Returns:
            The validated data.

        Raises:
            APIError: If validation fails.

        """
        # Check basic type
        if not isinstance(data, (dict, list)):
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=(
                    f"Invalid WebSocket message type: {type(data).__name__}. Expected dict or list."
                ),
                http_status=None,
            )

        # Check nesting depth
        depth = self._calculate_depth(data)
        if depth > self.config.max_nesting_depth:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=(
                    f"Message nesting depth {depth} exceeds limit {self.config.max_nesting_depth}"
                ),
                http_status=None,
            )

        # Check array lengths
        self._check_array_lengths(data)

        return data

    def _calculate_depth(
        self, obj: dict[str, Any] | list[Any] | str | float | bool | None, current_depth: int = 0
    ) -> int:
        """Calculate the maximum nesting depth of an object.

        Args:
            obj: The object to check.
            current_depth: Current recursion depth.

        Returns:
            Maximum depth found.

        """
        if current_depth > self.config.max_nesting_depth:
            return current_depth

        if isinstance(obj, dict):
            if not obj:
                return current_depth
            return max(self._calculate_depth(v, current_depth + 1) for v in obj.values())
        if isinstance(obj, list):
            if not obj:
                return current_depth
            return max(self._calculate_depth(item, current_depth + 1) for item in obj)
        return current_depth

    def _check_array_lengths(
        self, obj: dict[str, Any] | list[Any] | str | float | bool | None
    ) -> None:
        """Check that all arrays are within length limits.

        Args:
            obj: The object to check.

        Raises:
            APIError: If any array exceeds the length limit.

        """
        if isinstance(obj, list):
            if len(obj) > self.config.max_array_length:
                raise APIError(
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    message=f"Array length {len(obj)} exceeds limit {self.config.max_array_length}",
                    http_status=None,
                )
            for item in obj:
                self._check_array_lengths(item)
        elif isinstance(obj, dict):
            for value in obj.values():
                self._check_array_lengths(value)


class ValidatedWebSocketManager(WebSocketManager):
    """Enhanced WebSocket manager with pre-validation and security measures."""

    def __init__(
        self,
        exchange_name: str,
        message_handler: MessageHandler,
        config: WebSocketManagerConfig,
        message_config: WebSocketMessageConfig | None = None,
        on_connected_callback: Callable[[], Coroutine[Any, Any, None]] | None = None,
        session: aiohttp.ClientSession | None = None,
        task_factory: TaskFactory | None = None,
        outgoing_message_limiter: TokenBucketRateLimiterRuntime | None = None,
    ) -> None:
        """Initialize the ValidatedWebSocketManager.

        Args:
            exchange_name: Name of the exchange for logging.
            message_handler: Handler for validated messages.
            config: WebSocket connection configuration.
            message_config: Message validation configuration.
            on_connected_callback: Optional callback for connection events.
            session: Optional aiohttp session.
            task_factory: Optional task factory for creating tasks.
            outgoing_message_limiter: Optional rate limiter for outgoing messages.

        """
        super().__init__(
            exchange_name,
            message_handler,
            config,
            on_connected_callback,
            session,
            task_factory,
            outgoing_message_limiter,
        )
        self.msg_config = message_config or WebSocketMessageConfig()
        self.pre_validator = WebSocketPreValidator(self.msg_config)
        self._validation_stats = {
            "received": 0,
            "validated": 0,
            "rejected": 0,
            "oversized": 0,
            "parse_errors": 0,
        }

    async def _handle_text_message(self, msg: aiohttp.WSMessage) -> None:
        """Handle text WebSocket messages with validation.

        Args:
            msg: The WebSocket message to handle.

        """
        self._validation_stats["received"] += 1
        start_time = time.perf_counter()

        try:
            # Size validation
            if len(msg.data) > self.msg_config.max_message_size:
                self._validation_stats["oversized"] += 1
                self._logger.warning(
                    "oversized_websocket_message",
                    size=len(msg.data),
                    limit=self.msg_config.max_message_size,
                    exchange=self._exchange_name,
                )
                return

            # Parse with orjson for better performance and security
            try:
                # Set a timeout for parsing using asyncio
                parse_task = asyncio.create_task(asyncio.to_thread(orjson.loads, msg.data))
                data = await asyncio.wait_for(parse_task, timeout=self.msg_config.parse_timeout)
            except TimeoutError:
                self._validation_stats["parse_errors"] += 1
                self._logger.exception(
                    "websocket_parse_timeout",
                    timeout=self.msg_config.parse_timeout,
                    exchange=self._exchange_name,
                )
                return
            except (orjson.JSONDecodeError, ValueError) as e:
                self._validation_stats["parse_errors"] += 1
                self._logger.exception(
                    "invalid_websocket_json",
                    error=str(e),
                    exchange=self._exchange_name,
                    data_preview=(
                        msg.data[:DATA_PREVIEW_LENGTH]
                        if len(msg.data) <= DATA_PREVIEW_LENGTH
                        else msg.data[:DATA_PREVIEW_LENGTH] + "..."
                    ),
                )
                return

            # Pre-validation
            try:
                validated_data = self.pre_validator.validate(data)
                self._validation_stats["validated"] += 1
            except (ValidationError, APIError) as e:
                self._validation_stats["rejected"] += 1
                self._logger.exception(
                    "websocket_pre_validation_failed",
                    error=str(e),
                    exchange=self._exchange_name,
                    data_type=type(data).__name__,
                )
                return

            # Pass to handler - validate it's a dict for consistent interface
            if isinstance(validated_data, dict):
                await self._message_handler(validated_data)
            else:
                # Log warning for non-dict messages but don't alter interface
                self._logger.warning(
                    "non_dict_websocket_message",
                    action="process_websocket_message",
                    data_type=type(validated_data).__name__,
                    message="Received non-dict WebSocket message, skipping handler",
                )
                return

            # Log performance metrics periodically
            elapsed = time.perf_counter() - start_time
            if self._validation_stats["received"] % 1000 == 0:
                self._logger.info(
                    "websocket_message_stats",
                    stats=self._validation_stats,
                    last_message_time_ms=elapsed * 1000,
                    exchange=self._exchange_name,
                )

        except Exception as e:
            # Catch-all for unexpected errors
            self._logger.exception(
                "unexpected_websocket_error",
                error=str(e),
                exchange=self._exchange_name,
            )

    def _handle_binary_message(self, msg: aiohttp.WSMessage) -> None:
        """Handle binary WebSocket messages.

        Args:
            msg: The binary WebSocket message.

        """
        # For now, log and ignore binary messages
        self._logger.warning(
            "binary_websocket_message_received",
            size=len(msg.data),
            exchange=self._exchange_name,
        )

    async def get_stats(self) -> dict[str, Any]:
        """Get message processing statistics.

        Returns:
            Dictionary of statistics.

        """
        return {
            "exchange": self._exchange_name,
            "is_connected": self._is_connected,
            "message_stats": self._validation_stats.copy(),
            "config": {
                "max_message_size": self.msg_config.max_message_size,
                "max_nesting_depth": self.msg_config.max_nesting_depth,
                "max_array_length": self.msg_config.max_array_length,
            },
        }
