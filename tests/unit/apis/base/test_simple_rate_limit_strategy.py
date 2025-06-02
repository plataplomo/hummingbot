"""
Unit tests for SimpleTokenBucketStrategy.
Tests the basic rate limiting strategy used by exchanges like Backpack.
"""

from unittest.mock import AsyncMock

import pytest

from cyberdelta.apis.base.simple_rate_limit_strategy import SimpleTokenBucketStrategy
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime


class TestSimpleTokenBucketStrategy:
    """Test suite for SimpleTokenBucketStrategy."""

    @pytest.fixture
    def mock_limiter(self) -> AsyncMock:
        """Create a mock TokenBucketRateLimiterRuntime."""
        limiter = AsyncMock(spec=TokenBucketRateLimiterRuntime)
        limiter.acquire.return_value = 0.0
        return limiter

    @pytest.fixture
    def strategy(self, mock_limiter: AsyncMock) -> SimpleTokenBucketStrategy:
        """Create a SimpleTokenBucketStrategy with mock limiter."""
        return SimpleTokenBucketStrategy(limiter=mock_limiter, default_request_weight=1)

    async def test_prepare_and_acquire_default_weight(
        self, strategy: SimpleTokenBucketStrategy, mock_limiter: AsyncMock
    ) -> None:
        """Test acquiring with default request weight."""
        request_context = {
            "exchange_name": "backpack",
            "method": "GET",
            "endpoint": "/api/v1/ticker",
            "action_payload": None
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        mock_limiter.acquire.assert_called_once_with(tokens_to_consume=1)

    async def test_prepare_and_acquire_custom_weight(
        self, strategy: SimpleTokenBucketStrategy, mock_limiter: AsyncMock
    ) -> None:
        """Test acquiring with custom request weight from context."""
        request_context = {
            "exchange_name": "backpack",
            "method": "POST",
            "endpoint": "/api/v1/order",
            "action_payload": {"symbol": "BTC-USD"},
            "request_weight": 5
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        mock_limiter.acquire.assert_called_once_with(tokens_to_consume=5)

    async def test_prepare_and_acquire_zero_weight(
        self, strategy: SimpleTokenBucketStrategy, mock_limiter: AsyncMock
    ) -> None:
        """Test acquiring with zero weight (should not call limiter)."""
        request_context = {
            "exchange_name": "backpack",
            "method": "GET",
            "endpoint": "/api/v1/status",
            "request_weight": 0
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        mock_limiter.acquire.assert_not_called()

    async def test_prepare_and_acquire_negative_weight(
        self, strategy: SimpleTokenBucketStrategy, mock_limiter: AsyncMock
    ) -> None:
        """Test acquiring with negative weight (should not call limiter)."""
        request_context = {
            "exchange_name": "backpack",
            "method": "GET",
            "endpoint": "/api/v1/test",
            "request_weight": -1
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        mock_limiter.acquire.assert_not_called()

    async def test_prepare_and_acquire_missing_weight(
        self, strategy: SimpleTokenBucketStrategy, mock_limiter: AsyncMock
    ) -> None:
        """Test acquiring when request_weight is missing from context."""
        request_context = {
            "exchange_name": "backpack",
            "method": "GET",
            "endpoint": "/api/v1/ticker"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        mock_limiter.acquire.assert_called_once_with(tokens_to_consume=1)

    def test_custom_default_weight(self, mock_limiter: AsyncMock) -> None:
        """Test strategy with custom default weight."""
        strategy = SimpleTokenBucketStrategy(limiter=mock_limiter, default_request_weight=3)
        assert strategy.default_request_weight == 3

    async def test_real_limiter_integration(self) -> None:
        """Test with a real TokenBucketRateLimiterRuntime."""
        real_limiter = TokenBucketRateLimiterRuntime(rate=10.0, bucket_size=10)
        strategy = SimpleTokenBucketStrategy(limiter=real_limiter, default_request_weight=2)
        
        request_context = {"request_weight": 3}
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        assert real_limiter.tokens == 7.0  # 10 - 3 = 7

    async def test_limiter_wait_time_propagation(self, mock_limiter: AsyncMock) -> None:
        """Test that wait time from limiter is handled properly."""
        mock_limiter.acquire.return_value = 1.5  # Simulate wait time
        strategy = SimpleTokenBucketStrategy(limiter=mock_limiter)
        
        request_context = {"request_weight": 2}
        
        # The strategy doesn't return wait time, but should still call limiter
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        mock_limiter.acquire.assert_called_once_with(tokens_to_consume=2)

    async def test_exception_from_limiter_propagates(self, mock_limiter: AsyncMock) -> None:
        """Test that exceptions from limiter are propagated."""
        mock_limiter.acquire.side_effect = Exception("Rate limiter error")
        strategy = SimpleTokenBucketStrategy(limiter=mock_limiter)
        
        request_context = {"request_weight": 1}
        
        with pytest.raises(Exception, match="Rate limiter error"):
            await strategy.prepare_and_acquire(request_context)

    async def test_strategy_does_not_modify_payload(
        self, strategy: SimpleTokenBucketStrategy, mock_limiter: AsyncMock
    ) -> None:
        """Test that strategy returns None (does not modify payload)."""
        request_context = {
            "action_payload": {"symbol": "BTC-USD", "side": "buy"},
            "request_weight": 1
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        # Should not modify the payload
        assert result is None
        mock_limiter.acquire.assert_called_once_with(tokens_to_consume=1)


class TestSimpleTokenBucketStrategyInitialization:
    """Test initialization scenarios for SimpleTokenBucketStrategy."""

    def test_initialization_with_defaults(self) -> None:
        """Test initialization with default parameters."""
        limiter = TokenBucketRateLimiterRuntime(rate=1.0, bucket_size=1)
        strategy = SimpleTokenBucketStrategy(limiter=limiter)
        
        assert strategy.limiter is limiter
        assert strategy.default_request_weight == 1

    def test_initialization_with_custom_weight(self) -> None:
        """Test initialization with custom default weight."""
        limiter = TokenBucketRateLimiterRuntime(rate=1.0, bucket_size=1)
        strategy = SimpleTokenBucketStrategy(limiter=limiter, default_request_weight=5)
        
        assert strategy.limiter is limiter
        assert strategy.default_request_weight == 5

    def test_initialization_with_none_limiter_raises_error(self) -> None:
        """Test that initialization with None limiter raises appropriate error."""
        with pytest.raises((TypeError, AttributeError)):
            SimpleTokenBucketStrategy(limiter=None)  # type: ignore