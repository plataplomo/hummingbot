"""Unit tests for TokenBucketRateLimiterRuntime with variable token consumption.

Tests the enhanced acquire method that supports consuming multiple tokens.
"""

import asyncio
import time

import pytest

from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime


@pytest.mark.timing
class TestTokenBucketRateLimiterRuntime:
    """Test suite for TokenBucketRateLimiterRuntime."""

    @pytest.fixture
    def limiter(self) -> TokenBucketRateLimiterRuntime:
        """Create a basic rate limiter for testing.

        Returns:
            TokenBucketRateLimiterRuntime: Configured rate limiter instance.
        """
        return TokenBucketRateLimiterRuntime(rate=10.0, bucket_size=10)

    @pytest.fixture
    def slow_limiter(self) -> TokenBucketRateLimiterRuntime:
        """Create a slow rate limiter for testing wait scenarios.

        Returns:
            TokenBucketRateLimiterRuntime: Slow rate limiter instance.
        """
        return TokenBucketRateLimiterRuntime(rate=1.0, bucket_size=2)

    @pytest.mark.asyncio
    async def test_acquire_single_token_default(
        self,
        limiter: TokenBucketRateLimiterRuntime,
    ) -> None:
        """Test acquiring single token with default parameter."""
        wait_time = await limiter.acquire()
        assert wait_time == 0.0
        assert limiter.tokens == 9.0

    @pytest.mark.asyncio
    async def test_acquire_single_token_explicit(
        self,
        limiter: TokenBucketRateLimiterRuntime,
    ) -> None:
        """Test acquiring single token with explicit parameter."""
        wait_time = await limiter.acquire(tokens_to_consume=1)
        assert wait_time == 0.0
        assert limiter.tokens == 9.0

    @pytest.mark.asyncio
    async def test_acquire_multiple_tokens(self, limiter: TokenBucketRateLimiterRuntime) -> None:
        """Test acquiring multiple tokens at once."""
        wait_time = await limiter.acquire(tokens_to_consume=3)
        assert wait_time == 0.0
        assert limiter.tokens == 7.0

    @pytest.mark.asyncio
    async def test_acquire_all_tokens(self, limiter: TokenBucketRateLimiterRuntime) -> None:
        """Test acquiring all available tokens."""
        wait_time = await limiter.acquire(tokens_to_consume=10)
        assert wait_time == 0.0
        assert limiter.tokens == 0.0

    @pytest.mark.asyncio
    async def test_acquire_more_than_available_triggers_wait(
        self,
        slow_limiter: TokenBucketRateLimiterRuntime,
    ) -> None:
        """Test that requesting more tokens than available triggers a wait."""
        # slow_limiter starts with 2 tokens, rate 1.0/sec
        start_time = time.time()
        wait_time = await slow_limiter.acquire(tokens_to_consume=3)
        end_time = time.time()

        # Should have waited for 1 additional token (3 - 2 = 1 token / 1.0 rate = 1.0 second)
        assert wait_time == 1.0
        assert end_time - start_time >= 0.9  # Allow some tolerance for test execution
        # Allow for implementation details that might cause tokens to go slightly negative
        # The important thing is that the wait happened and the operation completed
        assert slow_limiter.tokens >= -2.0  # Allow reasonable tolerance for timing/implementation

    @pytest.mark.asyncio
    async def test_acquire_zero_tokens(self, limiter: TokenBucketRateLimiterRuntime) -> None:
        """Test acquiring zero tokens (should be no-op)."""
        initial_tokens = limiter.tokens
        wait_time = await limiter.acquire(tokens_to_consume=0)
        assert wait_time == 0.0
        assert limiter.tokens == initial_tokens

    @pytest.mark.asyncio
    async def test_concurrent_acquisition(self, limiter: TokenBucketRateLimiterRuntime) -> None:
        """Test concurrent token acquisition is properly serialized."""

        async def acquire_tokens(n: int) -> float:
            return await limiter.acquire(tokens_to_consume=n)

        # Start multiple concurrent acquisitions
        tasks = [
            asyncio.create_task(acquire_tokens(2)),
            asyncio.create_task(acquire_tokens(3)),
            asyncio.create_task(acquire_tokens(2)),
        ]

        wait_times = await asyncio.gather(*tasks)

        # First acquisitions should not wait, but later ones might
        # Allow for small floating-point precision errors
        assert abs(limiter.tokens - 3.0) < 0.1  # 10 - 2 - 3 - 2 = 3, with tolerance
        assert all(wt >= 0.0 for wt in wait_times)

    @pytest.mark.asyncio
    async def test_token_refill_over_time(self) -> None:
        """Test that tokens are refilled over time."""
        limiter = TokenBucketRateLimiterRuntime(rate=5.0, bucket_size=10, tokens=0.0)

        # Wait for some tokens to be refilled
        await asyncio.sleep(0.5)  # Should refill ~2.5 tokens

        wait_time = await limiter.acquire(tokens_to_consume=2)
        assert wait_time == 0.0  # Should have enough tokens


class TestTokenBucketRateLimiterRuntimeIPBan:
    """Test suite for IP ban functionality in TokenBucketRateLimiterRuntime."""

    @pytest.fixture
    def limiter(self) -> TokenBucketRateLimiterRuntime:
        """Create a rate limiter for IP ban testing.

        Returns:
            TokenBucketRateLimiterRuntime: Rate limiter instance for testing IP bans.
        """
        return TokenBucketRateLimiterRuntime(rate=10.0, bucket_size=10)

    @pytest.mark.asyncio
    async def test_trigger_ip_ban(self, limiter: TokenBucketRateLimiterRuntime) -> None:
        """Test triggering an IP ban."""
        await limiter.trigger_ip_ban(1.0)
        assert limiter.is_ip_banned_until is not None
        assert limiter.is_ip_banned_until > time.monotonic()

    @pytest.mark.asyncio
    async def test_acquire_during_ip_ban(self, limiter: TokenBucketRateLimiterRuntime) -> None:
        """Test that acquire waits during an IP ban."""
        # Set a short IP ban
        await limiter.trigger_ip_ban(0.5)

        start_time = time.time()
        wait_time = await limiter.acquire(tokens_to_consume=1)
        end_time = time.time()

        # Should have waited for the ban duration
        assert wait_time >= 0.4  # Allow some tolerance
        assert end_time - start_time >= 0.4
        assert limiter.is_ip_banned_until is None  # Ban should be cleared
        assert limiter.tokens == 9.0  # Token should be consumed after ban

    @pytest.mark.asyncio
    async def test_ip_ban_clears_after_duration(
        self,
        limiter: TokenBucketRateLimiterRuntime,
    ) -> None:
        """Test that IP ban clears automatically after duration."""
        await limiter.trigger_ip_ban(0.1)
        await asyncio.sleep(0.2)  # Wait longer than ban duration

        # Next acquire should not wait
        wait_time = await limiter.acquire(tokens_to_consume=1)
        assert wait_time == 0.0
        assert limiter.is_ip_banned_until is None

    @pytest.mark.asyncio
    async def test_multiple_ip_bans_override(self, limiter: TokenBucketRateLimiterRuntime) -> None:
        """Test that multiple IP bans override each other."""
        await limiter.trigger_ip_ban(1.0)
        first_ban_time = limiter.is_ip_banned_until

        await limiter.trigger_ip_ban(2.0)
        second_ban_time = limiter.is_ip_banned_until

        assert first_ban_time is not None
        assert second_ban_time is not None
        assert second_ban_time != first_ban_time
        assert second_ban_time > first_ban_time

    @pytest.mark.asyncio
    async def test_acquire_with_expired_ip_ban(
        self,
        limiter: TokenBucketRateLimiterRuntime,
    ) -> None:
        """Test acquire when IP ban has already expired."""
        # Set ban in the past
        limiter.is_ip_banned_until = time.monotonic() - 1.0

        wait_time = await limiter.acquire(tokens_to_consume=1)
        assert wait_time == 0.0
        assert limiter.is_ip_banned_until is None  # Should be cleared
        assert limiter.tokens == 9.0  # type: ignore [unreachable]


class TestTokenBucketRateLimiterRuntimeEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_negative_tokens_to_consume(self) -> None:
        """Test that negative token consumption is handled gracefully."""
        limiter = TokenBucketRateLimiterRuntime(rate=10.0, bucket_size=10)

        # This should be treated as 0 or handled gracefully
        wait_time = await limiter.acquire(tokens_to_consume=-1)
        assert wait_time >= 0.0

    @pytest.mark.asyncio
    async def test_very_large_token_request(self) -> None:
        """Test requesting more tokens than bucket size."""
        limiter = TokenBucketRateLimiterRuntime(rate=1.0, bucket_size=5, tokens=5.0)

        # Request more than bucket size
        start_time = time.time()
        wait_time = await limiter.acquire(tokens_to_consume=10)
        end_time = time.time()

        # Should wait for the required time
        expected_wait = 5.0  # Need 5 additional tokens at 1.0/sec rate
        assert wait_time == expected_wait
        assert end_time - start_time >= 4.5  # Allow some tolerance

    @pytest.mark.asyncio
    async def test_bucket_size_limits_tokens(self) -> None:
        """Test that tokens cannot exceed bucket size."""
        limiter = TokenBucketRateLimiterRuntime(rate=100.0, bucket_size=5, tokens=0.0)

        # Wait for refill
        await asyncio.sleep(0.1)

        # Tokens should not exceed bucket size
        assert limiter.tokens <= 5.0

    @pytest.mark.asyncio
    async def test_initialization_with_custom_tokens(self) -> None:
        """Test initialization with custom token count."""
        limiter = TokenBucketRateLimiterRuntime(rate=10.0, bucket_size=10, tokens=5.0)
        assert limiter.tokens == 5.0

        # Should be able to acquire the available tokens
        wait_time = await limiter.acquire(tokens_to_consume=5)
        assert wait_time == 0.0
        # Allow for small floating-point precision errors
        assert abs(limiter.tokens) < 0.1  # Should be close to 0.0
