"""Rate limiting behavior enums for the API layer.

This module contains enums that define behavior when rate limits are exceeded.
"""

from enum import Enum


class RateLimitBehavior(Enum):
    """Behavior when rate limit is exceeded.

    Replaces the boolean `raise_on_limit` parameter in rate limiting.
    """

    RAISE_ERROR = "raise_error"
    """Raise an exception when rate limit is exceeded (was raise_on_limit=True)."""

    RETURN_RESULT = "return_result"
    """Return rate limit result without raising (was raise_on_limit=False)."""

    LOG_AND_CONTINUE = "log_and_continue"
    """Log the rate limit event and continue."""

    QUEUE_REQUEST = "queue_request"
    """Queue the request for later processing."""

    @property
    def should_raise(self) -> bool:
        """Check if an exception should be raised."""
        return self == RateLimitBehavior.RAISE_ERROR


__all__ = [
    "RateLimitBehavior",
]
