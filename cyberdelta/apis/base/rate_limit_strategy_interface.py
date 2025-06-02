"""
cyberdelta.apis.base.rate_limit_strategy_interface
-----------------------------------------------
Interface definition for rate limiting strategies used by ExchangeAPI implementations.

This interface allows each exchange to implement its own specific rate limiting logic
while providing a common contract for the base ExchangeAPI class.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class RateLimitStrategy(ABC):
    """
    Abstract base class for rate limiting strategies.

    Each exchange can implement its own rate limiting logic by subclassing this interface.
    The strategy pattern allows for different rate limiting approaches (simple token bucket,
    IP weight-based, multiple limiters, etc.) while keeping the base ExchangeAPI generic.
    """

    @abstractmethod
    async def prepare_and_acquire(self, request_context: dict[str, Any]) -> dict[str, Any] | None:
        """
        Prepares for and acquires necessary rate limit tokens/permissions.
        Can optionally modify and return the request data payload if needed
        (e.g., to inject a rate-limit specific nonce, though not used by HL/BP REST).
        Should raise APIError(code=RATE_LIMITED) if acquisition times out or fails.

        Args:
            request_context: Dict containing details like 'method', 'endpoint',
                             'action_payload', 'exchange_name', 'request_weight',
                             'endpoint_group'.

        Returns:
            Optionally, a modified action_payload dict, or None if no modifications.

        Raises:
            APIError: If rate limiting fails or times out.
        """
        pass
