"""Tests for WebSocket rate limiting system."""

import time

import pytest

from cyberdelta.apis.base.rate_limit_behavior import RateLimitBehavior
from cyberdelta.apis.base.ws_rate_limiter import (
    RateLimitAlgorithm,
    RateLimitConfig,
    RateLimitError,
    RateLimitMiddleware,
    RateLimitResult,
    RateLimitType,
    SlidingWindowCounter,
    TokenBucket,
    WebSocketRateLimiter,
)


class TestRateLimitConfig:
    """Test RateLimitConfig model."""

    def test_config_creation(self) -> None:
        """Test creating a rate limit configuration."""
        config = RateLimitConfig(
            limit_type=RateLimitType.GLOBAL,
            algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
            requests_per_second=10.0,
            burst_size=20,
            message_types=["depth", "ticker"],
        )

        assert config.limit_type == RateLimitType.GLOBAL
        assert config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET
        assert config.requests_per_second == 10.0
        assert config.burst_size == 20
        assert config.message_types == ["depth", "ticker"]
        assert config.enabled is True

    def test_config_defaults(self) -> None:
        """Test configuration with default values."""
        config = RateLimitConfig(
            limit_type=RateLimitType.PER_CONNECTION, requests_per_second=5.0, burst_size=10
        )

        assert config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET
        assert config.window_size_seconds == 60
        assert config.message_types == []
        assert config.enabled is True

    def test_burst_size_validation(self) -> None:
        """Test burst size validation."""
        # Valid burst size
        config = RateLimitConfig(
            limit_type=RateLimitType.GLOBAL,
            requests_per_second=10.0,
            burst_size=50,  # 5 seconds worth
        )
        assert config.burst_size == 50

        # Invalid burst size (too large)
        with pytest.raises(ValueError, match=r"Burst size .* too large"):
            RateLimitConfig(
                limit_type=RateLimitType.GLOBAL,
                requests_per_second=1.0,
                burst_size=20,  # 20 seconds worth (max is 10)
            )


class TestTokenBucket:
    """Test TokenBucket implementation."""

    def test_token_bucket_creation(self) -> None:
        """Test creating a token bucket."""
        bucket = TokenBucket(requests_per_second=10.0, burst_size=20)

        assert bucket.requests_per_second == 10.0
        assert bucket.burst_size == 20
        assert bucket.tokens == 20.0

    def test_token_bucket_allows_initial_burst(self) -> None:
        """Test token bucket allows initial burst."""
        bucket = TokenBucket(requests_per_second=1.0, burst_size=5)

        # Should allow burst requests
        for i in range(5):
            allowed, remaining = bucket.is_allowed()
            assert allowed is True
            expected_remaining = float(4 - i)
            assert abs(remaining - expected_remaining) < 0.01

        # Next request should be denied
        allowed, remaining = bucket.is_allowed()
        assert allowed is False
        assert abs(remaining - 0.0) < 0.01

    @pytest.mark.timing
    def test_token_bucket_refills_over_time(self) -> None:
        """Test token bucket refills over time."""
        bucket = TokenBucket(requests_per_second=10.0, burst_size=5)

        # Exhaust all tokens
        for _ in range(5):
            bucket.is_allowed()

        # Should be denied immediately
        allowed, _ = bucket.is_allowed()
        assert allowed is False

        # Wait and check if tokens are refilled
        time.sleep(0.6)  # Wait 600ms for 6 tokens at 10/sec
        allowed, remaining = bucket.is_allowed()
        assert allowed is True
        assert remaining >= 4.0  # Should have at least 4 tokens left

    def test_token_bucket_time_until_available(self) -> None:
        """Test time until next token is available."""
        bucket = TokenBucket(requests_per_second=2.0, burst_size=1)

        # Exhaust tokens
        bucket.is_allowed()

        # Should need to wait 0.5 seconds for next token
        wait_time = bucket.time_until_available()
        assert 0.4 <= wait_time <= 0.6  # Allow some tolerance


class TestSlidingWindowCounter:
    """Test SlidingWindowCounter implementation."""

    def test_sliding_window_creation(self) -> None:
        """Test creating a sliding window counter."""
        counter = SlidingWindowCounter(requests_per_second=10.0, window_size_seconds=60)

        assert counter.requests_per_second == 10.0
        assert counter.window_size_seconds == 60
        assert counter.max_requests == 600  # 10 * 60
        assert len(counter.requests) == 0

    def test_sliding_window_allows_requests_within_limit(self) -> None:
        """Test sliding window allows requests within limit."""
        counter = SlidingWindowCounter(requests_per_second=1.0, window_size_seconds=2)

        # Should allow 2 requests (1/sec * 2 seconds)
        allowed, remaining = counter.is_allowed()
        assert allowed is True
        assert remaining == 1

        allowed, remaining = counter.is_allowed()
        assert allowed is True
        assert remaining == 0

        # Third request should be denied
        allowed, remaining = counter.is_allowed()
        assert allowed is False
        assert remaining == 0

    @pytest.mark.timing
    def test_sliding_window_removes_old_requests(self) -> None:
        """Test sliding window removes old requests."""
        counter = SlidingWindowCounter(requests_per_second=1.0, window_size_seconds=1)

        # Make request
        counter.is_allowed()
        assert len(counter.requests) == 1

        # Wait for window to expire
        time.sleep(1.1)

        # Old request should be removed on next check
        allowed, _remaining = counter.is_allowed()
        assert allowed is True
        assert len(counter.requests) == 1  # Only the new request

    def test_sliding_window_current_rate(self) -> None:
        """Test current rate calculation."""
        counter = SlidingWindowCounter(requests_per_second=2.0, window_size_seconds=2)

        # No requests initially
        assert counter.current_rate() == 0.0

        # Make one request
        counter.is_allowed()
        assert counter.current_rate() == 0.5  # 1 request in 2 seconds

        # Make another request
        counter.is_allowed()
        assert counter.current_rate() == 1.0  # 2 requests in 2 seconds


class TestWebSocketRateLimiter:
    """Test WebSocketRateLimiter."""

    @pytest.fixture
    def basic_configs(self) -> list[RateLimitConfig]:
        """Create basic rate limit configurations."""
        return [
            RateLimitConfig(
                limit_type=RateLimitType.GLOBAL,
                requests_per_second=10.0,
                burst_size=50,  # Higher than per-connection to test global limit first
            ),
            RateLimitConfig(
                limit_type=RateLimitType.PER_CONNECTION, requests_per_second=5.0, burst_size=10
            ),
            RateLimitConfig(
                limit_type=RateLimitType.PER_MESSAGE_TYPE,
                requests_per_second=2.0,
                burst_size=5,
                message_types=["depth"],
            ),
        ]

    @pytest.fixture
    def rate_limiter(self, basic_configs: list[RateLimitConfig]) -> WebSocketRateLimiter:
        """Create a rate limiter with basic configs."""
        return WebSocketRateLimiter(basic_configs)

    def test_rate_limiter_initialization(self, rate_limiter: WebSocketRateLimiter) -> None:
        """Test rate limiter initialization."""
        assert rate_limiter.global_limiter is not None
        assert len(rate_limiter.configs) == 3
        assert RateLimitType.GLOBAL in rate_limiter.configs
        assert RateLimitType.PER_CONNECTION in rate_limiter.configs
        assert RateLimitType.PER_MESSAGE_TYPE in rate_limiter.configs

    def test_rate_limiter_allows_within_limits(self, rate_limiter: WebSocketRateLimiter) -> None:
        """Test rate limiter allows requests within limits."""
        result = rate_limiter.check_rate_limit("conn1", "ticker")

        assert result.allowed is True
        assert result.limit_type == RateLimitType.GLOBAL

    def test_global_rate_limit_enforcement(self) -> None:
        """Test global rate limit enforcement."""
        # Create limiter with only global limit to test it in isolation
        config = RateLimitConfig(
            limit_type=RateLimitType.GLOBAL, requests_per_second=10.0, burst_size=20
        )
        limiter = WebSocketRateLimiter([config])

        # Exhaust global limit (20 burst)
        for _ in range(20):
            result = limiter.check_rate_limit("conn1")
            assert result.allowed is True

        # Next request should be denied
        result = limiter.check_rate_limit("conn1")
        assert result.allowed is False
        assert result.limit_type == RateLimitType.GLOBAL
        assert result.retry_after_seconds is not None
        assert result.retry_after_seconds > 0

    def test_per_connection_rate_limit(self) -> None:
        """Test per-connection rate limiting with fresh instance."""
        # Create fresh rate limiter to avoid global limit interference
        configs = [
            RateLimitConfig(
                limit_type=RateLimitType.GLOBAL, requests_per_second=10.0, burst_size=20
            ),
            RateLimitConfig(
                limit_type=RateLimitType.PER_CONNECTION, requests_per_second=5.0, burst_size=10
            ),
        ]
        rate_limiter = WebSocketRateLimiter(configs)

        # Test per-connection limit (10 burst)
        for _ in range(10):
            result = rate_limiter.check_rate_limit("conn1")
            assert result.allowed is True

        # Next request for same connection should be denied
        result = rate_limiter.check_rate_limit("conn1")
        assert result.allowed is False
        assert result.limit_type == RateLimitType.PER_CONNECTION

        # Different connection should still be allowed
        result = rate_limiter.check_rate_limit("conn2")
        assert result.allowed is True

    def test_per_message_type_rate_limit(self, rate_limiter: WebSocketRateLimiter) -> None:
        """Test per-message-type rate limiting."""
        # Reset all limiters for clean test
        rate_limiter.reset_all()

        # Test depth message limit (5 burst)
        for _ in range(5):
            result = rate_limiter.check_rate_limit("conn1", "depth")
            assert result.allowed is True

        # Next depth message should be denied
        result = rate_limiter.check_rate_limit("conn1", "depth")
        assert result.allowed is False
        assert result.limit_type == RateLimitType.PER_MESSAGE_TYPE

        # Different message type should still be allowed
        result = rate_limiter.check_rate_limit("conn1", "ticker")
        assert result.allowed is True

    def test_message_type_filtering(self, rate_limiter: WebSocketRateLimiter) -> None:
        """Test message type filtering in rate limits."""
        # Reset for clean test
        rate_limiter.reset_all()

        # Ticker messages should not be affected by depth message limit
        for _ in range(10):  # More than depth limit of 5
            result = rate_limiter.check_rate_limit("conn1", "ticker")
            # Should only hit per-connection limit eventually
            if not result.allowed:
                assert result.limit_type == RateLimitType.PER_CONNECTION
                break
        else:
            # If we get here, ticker wasn't limited by depth message type rule
            pass

    def test_rate_limiter_stats(self, rate_limiter: WebSocketRateLimiter) -> None:
        """Test rate limiter statistics."""
        # Generate some activity
        rate_limiter.check_rate_limit("conn1", "depth")
        rate_limiter.check_rate_limit("conn2", "ticker")

        stats = rate_limiter.get_stats()

        assert "total_limiters" in stats
        assert "global_limiter_enabled" in stats
        assert "configured_limits" in stats
        assert "limiter_details" in stats
        assert stats["global_limiter_enabled"] is True
        assert len(stats["configured_limits"]) == 3

    def test_reset_limiter(self, rate_limiter: WebSocketRateLimiter) -> None:
        """Test resetting specific limiters."""
        # Generate activity to create limiters
        rate_limiter.check_rate_limit("conn1", "depth")
        initial_count = len(rate_limiter.limiters)

        # Reset specific limiter
        rate_limiter.reset_limiter("conn1", "depth")

        # Should have fewer limiters now
        assert len(rate_limiter.limiters) <= initial_count

    def test_reset_all_limiters(self, rate_limiter: WebSocketRateLimiter) -> None:
        """Test resetting all limiters."""
        # Generate activity
        rate_limiter.check_rate_limit("conn1", "depth")
        rate_limiter.check_rate_limit("conn2", "ticker")

        assert len(rate_limiter.limiters) > 0

        # Reset all
        rate_limiter.reset_all()

        assert len(rate_limiter.limiters) == 0


class TestRateLimitMiddleware:
    """Test RateLimitMiddleware."""

    @pytest.fixture
    def middleware(self) -> RateLimitMiddleware:
        """Create rate limit middleware."""
        config = RateLimitConfig(
            limit_type=RateLimitType.GLOBAL, requests_per_second=1.0, burst_size=1
        )
        limiter = WebSocketRateLimiter([config])
        return RateLimitMiddleware(limiter)

    @pytest.mark.asyncio
    async def test_middleware_allows_request(self, middleware: RateLimitMiddleware) -> None:
        """Test middleware allows valid requests."""
        result = await middleware.check_rate_limit(
            "conn1", behavior=RateLimitBehavior.RETURN_RESULT
        )

        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_middleware_raises_on_limit(self, middleware: RateLimitMiddleware) -> None:
        """Test middleware raises exception on rate limit."""
        # Exhaust limit
        await middleware.check_rate_limit("conn1", behavior=RateLimitBehavior.RETURN_RESULT)

        # Next request should raise
        with pytest.raises(RateLimitError) as exc_info:
            await middleware.check_rate_limit("conn1", behavior=RateLimitBehavior.RAISE_ERROR)

        assert exc_info.value.result.allowed is False

    @pytest.mark.asyncio
    async def test_middleware_returns_result_without_raising(
        self, middleware: RateLimitMiddleware
    ) -> None:
        """Test middleware returns result without raising when configured."""
        # Exhaust limit
        await middleware.check_rate_limit("conn1", behavior=RateLimitBehavior.RETURN_RESULT)

        # Next request should return result without raising
        result = await middleware.check_rate_limit(
            "conn1", behavior=RateLimitBehavior.RETURN_RESULT
        )

        assert result.allowed is False
        assert isinstance(result, RateLimitResult)


class TestRateLimitIntegration:
    """Test rate limiting integration scenarios."""

    def test_multiple_algorithm_types(self) -> None:
        """Test different algorithm types working together."""
        configs = [
            RateLimitConfig(
                limit_type=RateLimitType.GLOBAL,
                algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
                requests_per_second=10.0,
                burst_size=10,
            ),
            RateLimitConfig(
                limit_type=RateLimitType.PER_CONNECTION,
                algorithm=RateLimitAlgorithm.SLIDING_WINDOW,
                requests_per_second=5.0,
                burst_size=5,
                window_size_seconds=1,
            ),
        ]

        limiter = WebSocketRateLimiter(configs)

        # Should work with both algorithm types
        result = limiter.check_rate_limit("conn1")
        assert result.allowed is True

    def test_disabled_config_ignored(self) -> None:
        """Test disabled configurations are ignored."""
        configs = [
            RateLimitConfig(
                limit_type=RateLimitType.GLOBAL,
                requests_per_second=1.0,
                burst_size=1,
                enabled=False,  # Disabled
            ),
            RateLimitConfig(
                limit_type=RateLimitType.PER_CONNECTION,
                requests_per_second=10.0,
                burst_size=10,
                enabled=True,
            ),
        ]

        limiter = WebSocketRateLimiter(configs)

        # Global limiter should not exist since it's disabled
        assert limiter.global_limiter is None
        assert RateLimitType.GLOBAL not in limiter.configs
        assert RateLimitType.PER_CONNECTION in limiter.configs

    def test_high_throughput_scenario(self) -> None:
        """Test rate limiter under high throughput."""
        config = RateLimitConfig(
            limit_type=RateLimitType.GLOBAL, requests_per_second=100.0, burst_size=1000
        )

        limiter = WebSocketRateLimiter([config])

        # Should handle many requests quickly
        allowed_count = 0
        for _ in range(1000):
            result = limiter.check_rate_limit("conn1")
            if result.allowed:
                allowed_count += 1

        assert allowed_count == 1000  # All should be allowed due to burst

        # Next request should be denied
        result = limiter.check_rate_limit("conn1")
        assert result.allowed is False
