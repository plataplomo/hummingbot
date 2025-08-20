"""Rate limiting models for WebSocket operations.

This module contains Pydantic models for rate limiting configuration
and results in the WebSocket system.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from cyberdelta.apis.enums.websocket import RateLimitAlgorithm, RateLimitType
from cyberdelta.apis.exceptions.websocket import BurstSizeTooLargeError


class RateLimitConfig(BaseModel):
    """Configuration for a rate limit rule."""

    limit_type: RateLimitType
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    requests_per_second: float = Field(gt=0, description="Maximum requests per second")
    burst_size: int = Field(gt=0, description="Maximum burst size")
    window_size_seconds: int = Field(default=60, gt=0, description="Window size for sliding window")
    message_types: list[str] = Field(
        default_factory=list,
        description="Message types this limit applies to",
    )
    enabled: bool = True

    @field_validator("burst_size")
    @classmethod
    def burst_size_must_be_reasonable(cls, v: int, info: ValidationInfo) -> int:
        """Validate burst size is reasonable compared to rate.

        Returns:
            The validated burst size value.

        Raises:
            BurstSizeTooLargeError: If burst size exceeds 10 times the requests per second.
        """
        data = info.data
        if "requests_per_second" in data:
            max_burst = int(data["requests_per_second"] * 10)  # Max 10 seconds worth
            if v > max_burst:
                rate = data["requests_per_second"]
                raise BurstSizeTooLargeError(v, rate)
        return v


class RateLimitResult(BaseModel):
    """Result of a rate limit check."""

    allowed: bool
    limit_type: RateLimitType
    remaining: int = Field(ge=0, description="Remaining requests in current window")
    reset_time: datetime = Field(description="When the limit resets")
    retry_after_seconds: float | None = Field(
        default=None,
        description="Seconds to wait before retry",
    )
    current_rate: float = Field(ge=0, description="Current request rate")
