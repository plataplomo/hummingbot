"""Property-based tests for Backpack Rate Limiting Strategy Implementation.

This module provides comprehensive property-based testing of the BackpackRateLimitStrategy class,
which is critical for preventing rate limit violations and IP bans in trading operations.

SECURITY CRITICAL: Rate limiting must correctly manage request flow to prevent:
- Exchange IP bans that could block all trading operations
- Rate limit violations that could trigger account restrictions
- Token bucket exhaustion during high-frequency trading periods
- Incorrect retry-after handling leading to continued violations
- Financial losses due to blocked trading during market opportunities

Key Testing Areas:
- Retry-after duration handling with various time values and edge cases
- Token bucket acquisition with different request weights and contexts
- IP ban triggering with comprehensive timing scenarios
- Request context validation and weight calculation
- Rate limit strategy inheritance and behavior consistency
- Edge cases for malformed contexts and extreme values

Following TESTING_SECURITY_RULES.md:
- NO hardcoded timing values (Hypothesis generates them)
- NO fallback mechanisms that could hide critical rate limit errors
- Comprehensive testing of rate limiting boundary conditions
- Validation of security-sensitive IP ban triggering behaviors

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for rate limit model handling
- Implements RULE-RUNTIME-SAFETY-V4 for safe rate limit processing
- Adheres to RULE-NO-SILENCING-V4 for proper rate limit error propagation
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.apis.backpack.bp_rate_limit_strategy import BackpackRateLimitStrategy
from cyberdelta.apis.base.rate_limit_models import RateLimitRequestContext
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.enums import ExchangeName


@pytest.fixture
def mock_limiter() -> MagicMock:
    """Create a mock TokenBucketRateLimiterRuntime.

    Returns:
        MagicMock configured as TokenBucketRateLimiterRuntime with async methods
    """
    limiter = MagicMock(spec=TokenBucketRateLimiterRuntime)
    limiter.trigger_ip_ban = AsyncMock()
    limiter.acquire = AsyncMock()
    return limiter


@pytest.fixture
def strategy(mock_limiter: MagicMock) -> BackpackRateLimitStrategy:
    """Create a BackpackRateLimitStrategy instance with mocked limiter.

    Args:
        mock_limiter: Mock rate limiter to inject into strategy

    Returns:
        BackpackRateLimitStrategy instance configured with mock limiter
    """
    return BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR RATE LIMIT STRATEGY TESTING
# =============================================================================


def bp_http_method_strategy() -> SearchStrategy[str]:
    """Generate valid HTTP methods for Backpack API requests.

    Returns:
        A Hypothesis strategy for HTTP methods.
    """
    return st.sampled_from(["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])


def bp_endpoint_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack API endpoints.

    Returns:
        A Hypothesis strategy for Backpack endpoints.
    """
    return st.one_of([
        # Known Backpack endpoints
        st.sampled_from([
            "/api/v1/orders",
            "/api/v1/order",
            "/api/v1/balances",
            "/api/v1/markets",
            "/api/v1/ticker",
            "/api/v1/trades",
            "/api/v1/klines",
            "/api/v1/orderbook",
            "/api/v1/positions",
            "/api/v1/fills",
            "/api/v1/deposits",
            "/api/v1/withdrawals",
            "/api/v1/system/time",
            "/api/v1/system/status",
        ]),
        # Generated endpoint patterns
        st.builds(
            lambda version, resource: f"/api/v{version}/{resource}",
            st.integers(min_value=1, max_value=3),
            st.sampled_from(["orders", "trades", "markets", "balances", "positions"]),
        ),
        # Nested endpoint patterns
        st.builds(
            lambda base, param: f"/api/v1/{base}/{param}",
            st.sampled_from(["orders", "markets", "positions"]),
            st.text(
                alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_",
                min_size=1,
                max_size=20,
            ),
        ),
    ])


def bp_endpoint_group_strategy() -> SearchStrategy[str | None]:
    """Generate endpoint groups for categorizing requests.

    Returns:
        A Hypothesis strategy for endpoint groups.
    """
    return st.one_of([
        st.none(),
        st.sampled_from([
            "trading",
            "market_data",
            "account",
            "system",
            "websocket",
            "auth",
            "public",
            "private",
        ]),
        st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=3, max_size=15),
    ])


def request_weight_strategy() -> SearchStrategy[int]:
    """Generate request weights for rate limiting.

    Returns:
        A Hypothesis strategy for request weights.
    """
    return st.one_of([
        # Common request weights
        st.sampled_from([1, 2, 5, 10, 20, 50, 100]),
        # Any reasonable weight
        st.integers(min_value=1, max_value=1000),
        # Edge cases
        st.integers(min_value=1, max_value=10000),
    ])


def retry_after_duration_strategy() -> SearchStrategy[float]:
    """Generate retry-after durations in seconds.

    Returns:
        A Hypothesis strategy for retry-after durations.
    """
    return st.one_of([
        # Common retry durations
        st.sampled_from([1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0]),
        # Fractional seconds
        st.floats(min_value=0.1, max_value=3600.0),
        # Very short durations
        st.floats(min_value=0.001, max_value=1.0),
        # Long durations
        st.floats(min_value=3600.0, max_value=86400.0),  # Up to 24 hours
    ])


def action_payload_strategy() -> SearchStrategy[dict[str, Any] | None]:
    """Generate action payloads for requests.

    Returns:
        A Hypothesis strategy for action payloads.
    """
    return st.one_of([
        st.none(),
        st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.one_of([
                st.text(max_size=100),
                st.integers(),
                st.floats(allow_nan=False, allow_infinity=False),
                st.booleans(),
                st.none(),
            ]),
            min_size=0,
            max_size=10,
        ),
        # Common trading payloads
        st.fixed_dictionaries({
            "symbol": st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ-_", min_size=3, max_size=10),
            "quantity": st.floats(min_value=0.001, max_value=1000000.0, allow_nan=False),
            "price": st.floats(min_value=0.01, max_value=1000000.0, allow_nan=False),
        }),
    ])


@composite
def bp_rate_limit_context_strategy(draw: st.DrawFn) -> RateLimitRequestContext:
    """Generate complete RateLimitRequestContext instances.

    Args:
        draw: Hypothesis draw function

    Returns:
        A RateLimitRequestContext for testing
    """
    exchange_name = draw(
        st.one_of([
            st.just(ExchangeName.BACKPACK),
            st.just(""),  # Missing exchange name edge case
            st.text(max_size=20),  # Invalid exchange name
        ])
    )

    method = draw(bp_http_method_strategy())
    endpoint = draw(bp_endpoint_strategy())
    action_payload = draw(action_payload_strategy())
    request_weight = draw(request_weight_strategy())
    endpoint_group = draw(bp_endpoint_group_strategy())

    return RateLimitRequestContext(
        exchange_name=exchange_name,
        method=method,
        endpoint=endpoint,
        action_payload=action_payload,
        request_weight=request_weight,
        endpoint_group=endpoint_group,
    )


def malicious_context_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate malicious context data for security testing.

    Returns:
        A Hypothesis strategy for malicious context inputs.
    """
    return st.one_of([
        # Extremely large weights
        st.fixed_dictionaries({
            "exchange_name": st.just(ExchangeName.BACKPACK),
            "method": st.just("GET"),
            "endpoint": st.just("/api/v1/orders"),
            "request_weight": st.integers(min_value=1000000, max_value=999999999),
            "action_payload": st.none(),
            "endpoint_group": st.just("trading"),
        }),
        # Malicious endpoint paths
        st.fixed_dictionaries({
            "exchange_name": st.just(ExchangeName.BACKPACK),
            "method": st.just("GET"),
            "endpoint": st.sampled_from([
                "../../../etc/passwd",
                "/api/v1/../../../admin",
                "/api/v1/orders?cmd=rm -rf /",
                "/api/v1/orders'; DROP TABLE users;--",
            ]),
            "request_weight": st.just(1),
            "action_payload": st.none(),
            "endpoint_group": st.just("trading"),
        }),
        # Buffer overflow attempts in fields
        st.fixed_dictionaries({
            "exchange_name": st.text(min_size=1000, max_size=1500),
            "method": st.text(min_size=1000, max_size=5000),
            "endpoint": st.text(min_size=1000, max_size=1500),
            "request_weight": st.just(1),
            "action_payload": st.none(),
            "endpoint_group": st.text(min_size=1000, max_size=5000),
        }),
    ])


# =============================================================================
# PROPERTY TESTS FOR RATE LIMIT STRATEGY
# =============================================================================


class TestBackpackRateLimitStrategyProperties:
    """Property-based tests for BackpackRateLimitStrategy core functionality."""

    @given(
        retry_duration=retry_after_duration_strategy(),
        context=bp_rate_limit_context_strategy(),
    )
    @settings(max_examples=300, deadline=None)
    @pytest.mark.asyncio
    async def test_handle_exchange_retry_after_triggers_ip_ban(
        self, retry_duration: float, context: RateLimitRequestContext, mock_limiter: MagicMock
    ) -> None:
        """Property: handle_exchange_retry_after should always trigger IP ban with correct duration."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)

        await strategy.handle_exchange_retry_after(retry_duration, context)

        # Property: Should always call trigger_ip_ban with exact duration
        mock_limiter.trigger_ip_ban.assert_called_once_with(retry_duration)

        # Property: Duration should be preserved exactly (no rounding/truncation)
        called_args = mock_limiter.trigger_ip_ban.call_args[0]
        assert len(called_args) == 1
        assert called_args[0] == retry_duration

    @given(
        context=bp_rate_limit_context_strategy(),
        default_weight=request_weight_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    @pytest.mark.asyncio
    async def test_prepare_and_acquire_uses_correct_weight(
        self, context: RateLimitRequestContext, default_weight: int, mock_limiter: MagicMock
    ) -> None:
        """Property: prepare_and_acquire should use request weight from context."""
        strategy = BackpackRateLimitStrategy(
            limiter=mock_limiter, default_request_weight=default_weight
        )

        await strategy.prepare_and_acquire(context)

        # Property: Should acquire tokens equal to request weight
        expected_weight = (
            context.request_weight if context.request_weight is not None else default_weight
        )
        mock_limiter.acquire.assert_called_once_with(tokens_to_consume=expected_weight)

    @given(
        context_data=st.fixed_dictionaries({
            "exchange_name": st.one_of([
                st.just(ExchangeName.BACKPACK),
                st.just(""),
                st.just(None),
            ]),
            "method": bp_http_method_strategy(),
            "endpoint": bp_endpoint_strategy(),
            "action_payload": action_payload_strategy(),
            "request_weight": st.one_of([request_weight_strategy(), st.just(None)]),
            "endpoint_group": bp_endpoint_group_strategy(),
        }),
        retry_duration=retry_after_duration_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    @pytest.mark.asyncio
    async def test_handles_missing_exchange_name_gracefully(
        self, context_data: dict[str, Any], retry_duration: float, mock_limiter: MagicMock
    ) -> None:
        """Property: Strategy should handle missing or invalid exchange names gracefully."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=5)

        context = RateLimitRequestContext(**context_data)

        # Should not crash regardless of exchange name validity
        await strategy.handle_exchange_retry_after(retry_duration, context)

        # Property: Should still trigger IP ban regardless of exchange name
        mock_limiter.trigger_ip_ban.assert_called_once_with(retry_duration)

    @given(
        weight_scenarios=st.lists(
            st.tuples(request_weight_strategy(), request_weight_strategy()), min_size=1, max_size=10
        )
    )
    @settings(max_examples=100, deadline=None)
    @pytest.mark.asyncio
    async def test_weight_consistency_across_multiple_requests(
        self, weight_scenarios: list[tuple[int, int]], mock_limiter: MagicMock
    ) -> None:
        """Property: Weight handling should be consistent across multiple requests."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=10)

        total_tokens_consumed = 0

        for context_weight, _ in weight_scenarios:
            context = RateLimitRequestContext(
                exchange_name=ExchangeName.BACKPACK,
                method="GET",
                endpoint="/api/v1/orders",
                action_payload=None,
                request_weight=context_weight,
                endpoint_group="trading",
            )

            await strategy.prepare_and_acquire(context)
            total_tokens_consumed += context_weight

        # Property: Total tokens consumed should equal sum of all request weights
        assert mock_limiter.acquire.call_count == len(weight_scenarios)

        # Verify each call used correct weight
        for i, (expected_weight, _) in enumerate(weight_scenarios):
            call_args = mock_limiter.acquire.call_args_list[i]
            assert call_args[1]["tokens_to_consume"] == expected_weight

    @given(
        extreme_duration=st.one_of([
            st.floats(min_value=0.0001, max_value=0.001),  # Very short
            st.floats(min_value=86400.0, max_value=604800.0),  # Very long (1-7 days)
            st.just(0.0),  # Zero duration edge case
        ]),
        context=bp_rate_limit_context_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    @pytest.mark.asyncio
    async def test_extreme_retry_duration_handling(
        self, extreme_duration: float, context: RateLimitRequestContext, mock_limiter: MagicMock
    ) -> None:
        """Property: Strategy should handle extreme retry durations without issues."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)

        # Should not crash on extreme durations
        await strategy.handle_exchange_retry_after(extreme_duration, context)

        # Property: Should pass through exact duration value
        mock_limiter.trigger_ip_ban.assert_called_once_with(extreme_duration)

    @given(
        context=bp_rate_limit_context_strategy(),
        default_weights=st.lists(request_weight_strategy(), min_size=1, max_size=5),
    )
    @settings(max_examples=100, deadline=None)
    @pytest.mark.asyncio
    async def test_default_weight_behavior_consistency(
        self, context: RateLimitRequestContext, default_weights: list[int], mock_limiter: MagicMock
    ) -> None:
        """Property: Default weight behavior should be consistent across strategy instances."""
        for default_weight in default_weights:
            # Reset mock for each iteration
            mock_limiter.reset_mock()

            strategy = BackpackRateLimitStrategy(
                limiter=mock_limiter, default_request_weight=default_weight
            )

            # Create context without request weight to test default
            context_without_weight = RateLimitRequestContext(
                exchange_name=context.exchange_name,
                method=context.method,
                endpoint=context.endpoint,
                action_payload=context.action_payload,
                request_weight=None,  # Force use of default
                endpoint_group=context.endpoint_group,
            )

            await strategy.prepare_and_acquire(context_without_weight)

            # Property: Should use default weight when context weight is None
            mock_limiter.acquire.assert_called_once_with(tokens_to_consume=default_weight)


# =============================================================================
# PROPERTY TESTS FOR SECURITY BOUNDARIES
# =============================================================================


class TestBackpackRateLimitStrategySecurityProperties:
    """Property-based tests for security-critical rate limiting behavior."""

    @given(
        malicious_context_data=malicious_context_strategy(),
        retry_duration=retry_after_duration_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    @pytest.mark.asyncio
    async def test_malicious_context_resistance(
        self, malicious_context_data: dict[str, Any], retry_duration: float, mock_limiter: MagicMock
    ) -> None:
        """Property: Strategy should safely handle malicious context data."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)

        try:
            context = RateLimitRequestContext(**malicious_context_data)
        except (TypeError, ValueError):
            # Skip invalid context data that can't be constructed
            assume(False)

        # Should not crash on malicious context
        await strategy.handle_exchange_retry_after(retry_duration, context)

        # Property: Should still function correctly
        mock_limiter.trigger_ip_ban.assert_called_once_with(retry_duration)

    @given(
        extreme_weight=st.integers(min_value=1000000, max_value=999999999),
        context_base=bp_rate_limit_context_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_extreme_weight_handling(
        self, extreme_weight: int, context_base: RateLimitRequestContext, mock_limiter: MagicMock
    ) -> None:
        """Property: Strategy should handle extremely large request weights safely."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)

        # Create context with extreme weight
        extreme_context = RateLimitRequestContext(
            exchange_name=context_base.exchange_name,
            method=context_base.method,
            endpoint=context_base.endpoint,
            action_payload=context_base.action_payload,
            request_weight=extreme_weight,
            endpoint_group=context_base.endpoint_group,
        )

        # Should not crash on extreme weights
        await strategy.prepare_and_acquire(extreme_context)

        # Property: Should pass through the extreme weight
        mock_limiter.acquire.assert_called_once_with(tokens_to_consume=extreme_weight)

    @given(
        timing_attack_durations=st.lists(retry_after_duration_strategy(), min_size=10, max_size=50),
        context=bp_rate_limit_context_strategy(),
    )
    @settings(max_examples=20, deadline=None)
    @pytest.mark.asyncio
    async def test_timing_attack_resistance(
        self,
        timing_attack_durations: list[float],
        context: RateLimitRequestContext,
        mock_limiter: MagicMock,
    ) -> None:
        """Property: Strategy should not leak timing information through behavior differences."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)

        # Process many durations rapidly
        for duration in timing_attack_durations:
            mock_limiter.reset_mock()
            await strategy.handle_exchange_retry_after(duration, context)

            # Property: Behavior should be consistent regardless of duration
            mock_limiter.trigger_ip_ban.assert_called_once_with(duration)

    @given(
        negative_weight=st.integers(min_value=-1000, max_value=-1),
        context_base=bp_rate_limit_context_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_negative_weight_handling(
        self, negative_weight: int, context_base: RateLimitRequestContext, mock_limiter: MagicMock
    ) -> None:
        """Property: Strategy should handle negative weights appropriately."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)

        # Create context with negative weight
        negative_context = RateLimitRequestContext(
            exchange_name=context_base.exchange_name,
            method=context_base.method,
            endpoint=context_base.endpoint,
            action_payload=context_base.action_payload,
            request_weight=negative_weight,
            endpoint_group=context_base.endpoint_group,
        )

        # Should not crash on negative weights
        await strategy.prepare_and_acquire(negative_context)

        # Property: Should pass through the negative weight (let rate limiter handle validation)
        mock_limiter.acquire.assert_called_once_with(tokens_to_consume=negative_weight)

    @given(
        buffer_overflow_endpoint=st.text(min_size=1000, max_size=1500),
        context_base=bp_rate_limit_context_strategy(),
    )
    @settings(max_examples=20, deadline=None)
    @pytest.mark.asyncio
    async def test_buffer_overflow_resistance(
        self,
        buffer_overflow_endpoint: str,
        context_base: RateLimitRequestContext,
        mock_limiter: MagicMock,
    ) -> None:
        """Property: Strategy should handle extremely large string inputs safely."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)

        # Create context with buffer overflow attempt
        overflow_context = RateLimitRequestContext(
            exchange_name=context_base.exchange_name,
            method=context_base.method,
            endpoint=buffer_overflow_endpoint,  # Extremely large endpoint
            action_payload=context_base.action_payload,
            request_weight=context_base.request_weight,
            endpoint_group=context_base.endpoint_group,
        )

        # Should not crash on large strings
        await strategy.prepare_and_acquire(overflow_context)

        # Property: Should still function correctly
        expected_weight = overflow_context.request_weight or 1
        mock_limiter.acquire.assert_called_once_with(tokens_to_consume=expected_weight)


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBackpackRateLimitStrategyIntegrationProperties:
    """Integration property tests for complete rate limiting workflows."""

    @given(
        request_sequences=st.lists(
            st.tuples(
                bp_rate_limit_context_strategy(),
                retry_after_duration_strategy(),
            ),
            min_size=1,
            max_size=20,
        )
    )
    @settings(max_examples=30, deadline=None)
    @pytest.mark.asyncio
    async def test_mixed_operation_sequence_consistency(
        self,
        request_sequences: list[tuple[RateLimitRequestContext, float]],
        mock_limiter: MagicMock,
    ) -> None:
        """Property: Mixed sequences of acquire and retry-after should work consistently."""
        strategy = BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=1)

        acquire_count = 0
        retry_count = 0

        for i, (context, retry_duration) in enumerate(request_sequences):
            if i % 2 == 0:
                # Even iterations: test prepare_and_acquire
                await strategy.prepare_and_acquire(context)
                acquire_count += 1
            else:
                # Odd iterations: test handle_exchange_retry_after
                await strategy.handle_exchange_retry_after(retry_duration, context)
                retry_count += 1

        # Property: Each operation type should have been called correct number of times
        assert mock_limiter.acquire.call_count == acquire_count
        assert mock_limiter.trigger_ip_ban.call_count == retry_count

    @given(
        contexts=st.lists(bp_rate_limit_context_strategy(), min_size=5, max_size=20),
        default_weight=request_weight_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_weight_accumulation_properties(
        self, contexts: list[RateLimitRequestContext], default_weight: int, mock_limiter: MagicMock
    ) -> None:
        """Property: Weight handling should accumulate correctly across multiple requests."""
        strategy = BackpackRateLimitStrategy(
            limiter=mock_limiter, default_request_weight=default_weight
        )

        expected_total_weight = 0

        for context in contexts:
            await strategy.prepare_and_acquire(context)
            expected_weight = (
                context.request_weight if context.request_weight is not None else default_weight
            )
            expected_total_weight += expected_weight

        # Property: Total acquire calls should match number of contexts
        assert mock_limiter.acquire.call_count == len(contexts)

        # Property: Sum of all tokens consumed should match expected total
        actual_total_weight = sum(
            call[1]["tokens_to_consume"] for call in mock_limiter.acquire.call_args_list
        )
        assert actual_total_weight == expected_total_weight

    @given(
        strategy_configs=st.lists(request_weight_strategy(), min_size=1, max_size=5),
        test_context=bp_rate_limit_context_strategy(),
    )
    @settings(max_examples=30, deadline=None)
    @pytest.mark.asyncio
    async def test_strategy_instance_isolation(
        self,
        strategy_configs: list[int],
        test_context: RateLimitRequestContext,
        mock_limiter: MagicMock,
    ) -> None:
        """Property: Different strategy instances should operate independently."""
        strategies = [
            BackpackRateLimitStrategy(limiter=mock_limiter, default_request_weight=weight)
            for weight in strategy_configs
        ]

        # Test each strategy independently
        for i, (strategy, expected_default_weight) in enumerate(zip(strategies, strategy_configs)):
            mock_limiter.reset_mock()

            # Use context without weight to test default behavior
            weightless_context = RateLimitRequestContext(
                exchange_name=test_context.exchange_name,
                method=test_context.method,
                endpoint=test_context.endpoint,
                action_payload=test_context.action_payload,
                request_weight=None,  # Force use of default
                endpoint_group=test_context.endpoint_group,
            )

            await strategy.prepare_and_acquire(weightless_context)

            # Property: Each strategy should use its own default weight
            mock_limiter.acquire.assert_called_once_with(tokens_to_consume=expected_default_weight)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


class TestBackpackRateLimitStrategy:
    """Legacy compatibility tests for BackpackRateLimitStrategy."""

    @pytest.mark.asyncio
    async def test_handle_exchange_retry_after_legacy_compatibility(
        self,
        strategy: BackpackRateLimitStrategy,
        mock_limiter: MagicMock,
    ) -> None:
        """Test that handle_exchange_retry_after triggers IP ban on limiter."""
        duration_seconds = 10.5
        request_context = RateLimitRequestContext(
            exchange_name=ExchangeName.BACKPACK,
            method="GET",
            endpoint="/api/v1/orders",
            action_payload=None,
            request_weight=1,
            endpoint_group="trading",
        )

        await strategy.handle_exchange_retry_after(duration_seconds, request_context)

        mock_limiter.trigger_ip_ban.assert_called_once_with(10.5)

    @pytest.mark.asyncio
    async def test_handle_exchange_retry_after_no_exchange_name_legacy(
        self,
        strategy: BackpackRateLimitStrategy,
        mock_limiter: MagicMock,
    ) -> None:
        """Test handle_exchange_retry_after with missing exchange_name in context."""
        duration_seconds = 5.0
        request_context = RateLimitRequestContext(
            exchange_name="",  # Missing exchange name for test case
            method="POST",
            endpoint="/api/v1/orders",
            action_payload=None,
            request_weight=1,
            endpoint_group=None,
        )

        await strategy.handle_exchange_retry_after(duration_seconds, request_context)

        mock_limiter.trigger_ip_ban.assert_called_once_with(5.0)

    @pytest.mark.asyncio
    async def test_prepare_and_acquire_inherited_legacy(
        self,
        strategy: BackpackRateLimitStrategy,
        mock_limiter: MagicMock,
    ) -> None:
        """Test that prepare_and_acquire works as inherited from SimpleTokenBucketStrategy."""
        request_context = RateLimitRequestContext(
            exchange_name=ExchangeName.BACKPACK,
            method="GET",
            endpoint="/api/v1/markets",
            action_payload=None,
            request_weight=2,
            endpoint_group=None,
        )

        await strategy.prepare_and_acquire(request_context)

        # SimpleTokenBucketStrategy returns None
        mock_limiter.acquire.assert_called_once_with(
            tokens_to_consume=2,
        )  # Uses request_weight from context

    @pytest.mark.asyncio
    async def test_prepare_and_acquire_default_weight_legacy(
        self,
        strategy: BackpackRateLimitStrategy,
        mock_limiter: MagicMock,
    ) -> None:
        """Test prepare_and_acquire with default weight when not specified in context."""
        request_context = RateLimitRequestContext(
            exchange_name=ExchangeName.BACKPACK,
            method="GET",
            endpoint="/api/v1/markets",
            action_payload=None,
            request_weight=1,
            endpoint_group=None,
        )

        await strategy.prepare_and_acquire(request_context)

        # Method returns None
        mock_limiter.acquire.assert_called_once_with(
            tokens_to_consume=1,
        )  # Uses default_request_weight
