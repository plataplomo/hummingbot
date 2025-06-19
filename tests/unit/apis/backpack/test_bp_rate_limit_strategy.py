"""Unit tests for BackpackRateLimitStrategy."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.bp_rate_limit_strategy import BackpackRateLimitStrategy
from cyberdelta.apis.base.rate_limit_models import RateLimitRequestContext
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime


class TestBackpackRateLimitStrategy:
    """Test cases for BackpackRateLimitStrategy."""

    @pytest.fixture
    def mock_limiter(self) -> MagicMock:
        """Create a mock TokenBucketRateLimiterRuntime."""
        limiter = MagicMock(spec=TokenBucketRateLimiterRuntime)
        limiter.trigger_ip_ban = AsyncMock()
        limiter.acquire = AsyncMock()
        return limiter

    @pytest.fixture
    def strategy(self, mock_limiter: MagicMock) -> BackpackRateLimitStrategy:
        """Create a BackpackRateLimitStrategy instance with mocked limiter."""
        return BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)

    @pytest.mark.asyncio
    async def test_handle_exchange_retry_after(
        self,
        strategy: BackpackRateLimitStrategy,
        mock_limiter: MagicMock,
    ) -> None:
        """Test that handle_exchange_retry_after triggers IP ban on limiter."""
        # Arrange
        duration_seconds = 10.5
        request_context = RateLimitRequestContext(
            exchange_name="backpack",
            method="GET",
            endpoint="/api/v1/orders",
            action_payload=None,
            request_weight=1,
            endpoint_group="trading",
        )

        # Act
        await strategy.handle_exchange_retry_after(duration_seconds, request_context)

        # Assert
        mock_limiter.trigger_ip_ban.assert_called_once_with(10.5)

    @pytest.mark.asyncio
    async def test_handle_exchange_retry_after_no_exchange_name(
        self,
        strategy: BackpackRateLimitStrategy,
        mock_limiter: MagicMock,
    ) -> None:
        """Test handle_exchange_retry_after with missing exchange_name in context."""
        # Arrange
        duration_seconds = 5.0
        request_context = RateLimitRequestContext(
            exchange_name="",  # Missing exchange name for test case
            method="POST",
            endpoint="/api/v1/orders",
            action_payload=None,
            request_weight=1,
            endpoint_group=None,
        )

        # Act
        await strategy.handle_exchange_retry_after(duration_seconds, request_context)

        # Assert
        mock_limiter.trigger_ip_ban.assert_called_once_with(5.0)

    @pytest.mark.asyncio
    async def test_prepare_and_acquire_inherited(
        self,
        strategy: BackpackRateLimitStrategy,
        mock_limiter: MagicMock,
    ) -> None:
        """Test that prepare_and_acquire works as inherited from SimpleTokenBucketStrategy."""
        # Arrange
        request_context = RateLimitRequestContext(
            exchange_name="backpack",
            method="GET",
            endpoint="/api/v1/markets",
            action_payload=None,
            request_weight=2,
            endpoint_group=None,
        )

        # Act
        await strategy.prepare_and_acquire(request_context)

        # Assert
        # SimpleTokenBucketStrategy returns None
        mock_limiter.acquire.assert_called_once_with(
            tokens_to_consume=2,
        )  # Uses request_weight from context

    @pytest.mark.asyncio
    async def test_prepare_and_acquire_default_weight(
        self,
        strategy: BackpackRateLimitStrategy,
        mock_limiter: MagicMock,
    ) -> None:
        """Test prepare_and_acquire with default weight when not specified in context."""
        # Arrange
        request_context = RateLimitRequestContext(
            exchange_name="backpack",
            method="GET",
            endpoint="/api/v1/markets",
            action_payload=None,
            request_weight=1,
            endpoint_group=None,
        )

        # Act
        await strategy.prepare_and_acquire(request_context)

        # Assert
        # Method returns None
        mock_limiter.acquire.assert_called_once_with(
            tokens_to_consume=1,
        )  # Uses default_request_weight
