"""WebSocket rate limiting enums.

This module contains enums for rate limiting configuration and behavior.
"""

from enum import StrEnum


class RateLimitType(StrEnum):
    """Types of rate limits."""

    GLOBAL = "global"
    PER_CONNECTION = "per_connection"
    PER_MESSAGE_TYPE = "per_message_type"
    PER_USER = "per_user"


class RateLimitAlgorithm(StrEnum):
    """Rate limiting algorithms."""

    TOKEN_BUCKET = "token_bucket"  # noqa: S105
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


__all__ = [
    "RateLimitAlgorithm",
    "RateLimitType",
]
