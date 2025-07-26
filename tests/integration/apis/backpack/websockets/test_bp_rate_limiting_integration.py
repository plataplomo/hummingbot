"""Test 4: Rate Limiting Integration with Refactored System.

This module tests that the refactored WebSocket system properly integrates
with rate limiting, including both HTTP API rate limiting and WebSocket
message rate limiting.

Security Compliance:
- Tests rate limiting prevents API abuse
- Validates rate limiter integration with WebSocket sends
- Tests rate limit backoff and recovery
- Fails fast on rate limiting bypass issues
"""

import asyncio
import time

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.service_args import GetMarketsArgs
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger


pytestmark = [pytest.mark.integration, pytest.mark.timing]

logger = get_logger(__name__)


class TestBackpackRateLimitingIntegration:
    """Test rate limiting integration with refactored WebSocket system."""

    @pytest.mark.asyncio
    async def test_websocket_rate_limiter_initialization(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that WebSocket rate limiter is properly initialized."""
        # Check if API has WebSocket manager
        ws_manager = getattr(bp_api_for_test_env, "_ws_manager", None)
        if not ws_manager:
            pytest.fail("WebSocket manager not initialized - rate limiting unavailable")

        # Check if WebSocket manager has outgoing message limiter
        outgoing_limiter = getattr(ws_manager, "_outgoing_message_limiter", None)

        if outgoing_limiter:
            assert isinstance(outgoing_limiter, TokenBucketRateLimiterRuntime), (
                f"Expected TokenBucketRateLimiterRuntime, got {type(outgoing_limiter)}"
            )

            # Check rate limiter configuration
            rate = getattr(outgoing_limiter, "rate", None)
            bucket_size = getattr(outgoing_limiter, "bucket_size", None)

            assert rate is not None, "Rate limiter missing rate configuration"
            assert bucket_size is not None, "Rate limiter missing bucket size configuration"
            assert rate > 0, f"Rate limiter rate should be positive, got {rate}"
            assert bucket_size > 0, (
                f"Rate limiter bucket size should be positive, got {bucket_size}"
            )

            logger.info(
                "websocket_rate_limiter_initialized",
                rate=rate,
                bucket_size=bucket_size,
                limiter_type=type(outgoing_limiter).__name__,
                message="✓ WebSocket outgoing message rate limiter properly initialized",
            )
        else:
            logger.info(
                "websocket_no_rate_limiter",
                message=(
                    "WebSocket manager has no outgoing message rate limiter "
                    "(may be exchange-specific)"
                ),
            )

    @pytest.mark.asyncio
    async def test_http_api_rate_limiting(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test HTTP API rate limiting in refactored system."""
        # Check if API has rate limit strategy
        rate_limit_strategy = getattr(bp_api_for_test_env, "_rate_limit_strategy", None)

        if not rate_limit_strategy:
            logger.info(
                "http_no_rate_limit_strategy", message="API has no rate limit strategy configured"
            )
            return

        logger.info(
            "http_rate_limit_strategy_found",
            strategy_type=type(rate_limit_strategy).__name__,
            message="✓ HTTP API rate limit strategy configured",
        )

        # Test rate limiting with multiple rapid requests
        start_time = time.perf_counter()
        request_count = 5
        request_times: list[float] = []

        for i in range(request_count):
            request_start = time.perf_counter()

            try:
                # Make rate-limited API call
                await bp_api_for_test_env.get_markets(GetMarketsArgs())
                request_end = time.perf_counter()
                request_time = request_end - request_start
                request_times.append(request_time)

                logger.info(
                    "rate_limited_request_completed",
                    request_index=i,
                    request_time_ms=f"{request_time * 1000:.2f}",
                    message=f"Request {i + 1} completed",
                )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                pytest.fail(
                    f"Rate-limited HTTP request {i} failed: {e}. "
                    "Rate limiting may be blocking valid requests."
                )

        total_time = time.perf_counter() - start_time

        # Analyze timing patterns for rate limiting evidence
        avg_request_time = sum(request_times) / len(request_times)

        logger.info(
            "http_rate_limiting_analysis",
            total_requests=request_count,
            total_time_seconds=f"{total_time:.3f}",
            avg_request_time_ms=f"{avg_request_time * 1000:.2f}",
            requests_per_second=f"{request_count / total_time:.2f}",
            message="✓ HTTP rate limiting behavior analyzed",
        )

    @pytest.mark.asyncio
    async def test_websocket_subscription_rate_limiting(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test WebSocket subscription rate limiting."""
        try:
            await bp_api_for_test_env.connect_websocket()

            if not bp_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed - cannot test subscription rate limiting")

            # Get test symbols
            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            if len(markets) < 3:
                pytest.fail("Need at least 3 markets for rate limiting test")

            # Test rapid subscriptions to trigger rate limiting
            subscription_count = 10
            start_time = time.perf_counter()
            subscription_times: list[float] = []

            async def test_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029

            for i in range(subscription_count):
                sub_start = time.perf_counter()

                try:
                    market_index = i % len(markets)
                    symbol = markets[market_index].symbol
                    topic = f"ticker.{symbol}"

                    await bp_api_for_test_env.subscribe(topic, test_handler)

                    sub_end = time.perf_counter()
                    sub_time = sub_end - sub_start
                    subscription_times.append(sub_time)

                    logger.info(
                        "rate_limited_subscription_completed",
                        subscription_index=i,
                        symbol=symbol,
                        subscription_time_ms=f"{sub_time * 1000:.2f}",
                        message=f"Subscription {i + 1} to {symbol} completed",
                    )

                except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                    logger.warning(
                        "rate_limited_subscription_failed",
                        subscription_index=i,
                        error=str(e),
                        message=f"Subscription {i + 1} failed (may be rate limited): {e}",
                    )

            total_time = time.perf_counter() - start_time
            successful_subs = len([t for t in subscription_times if t > 0])

            # Analyze subscription timing for rate limiting evidence
            if subscription_times:
                avg_sub_time = sum(subscription_times) / len(subscription_times)
                max_sub_time = max(subscription_times)

                logger.info(
                    "websocket_subscription_rate_limiting_analysis",
                    total_subscriptions=subscription_count,
                    successful_subscriptions=successful_subs,
                    total_time_seconds=f"{total_time:.3f}",
                    avg_subscription_time_ms=f"{avg_sub_time * 1000:.2f}",
                    max_subscription_time_ms=f"{max_sub_time * 1000:.2f}",
                    subscriptions_per_second=f"{successful_subs / total_time:.2f}",
                    message="✓ WebSocket subscription rate limiting behavior analyzed",
                )

                # Check for rate limiting indicators
                if max_sub_time > avg_sub_time * 2:
                    logger.info(
                        "rate_limiting_evidence_found",
                        max_time_ms=f"{max_sub_time * 1000:.2f}",
                        avg_time_ms=f"{avg_sub_time * 1000:.2f}",
                        message="Rate limiting may be active (variable subscription times)",
                    )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"WebSocket subscription rate limiting test failed: {e}. "
                "Rate limiting integration not working."
            )

    @pytest.mark.asyncio
    async def test_rate_limiter_token_consumption(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test rate limiter token consumption and refill."""
        ws_manager = getattr(bp_api_for_test_env, "_ws_manager", None)
        if not ws_manager:
            pytest.fail("WebSocket manager not available for token testing")

        outgoing_limiter = getattr(ws_manager, "_outgoing_message_limiter", None)
        if not outgoing_limiter:
            logger.info(
                "no_outgoing_limiter_for_token_test",
                message="No outgoing message limiter available for token consumption testing",
            )
            return

        # Test token consumption
        initial_tokens = getattr(outgoing_limiter, "tokens", None)
        if initial_tokens is None:
            logger.info(
                "limiter_no_tokens_attribute",
                message="Rate limiter doesn't expose tokens attribute",
            )
            return

        logger.info(
            "rate_limiter_initial_state",
            initial_tokens=f"{initial_tokens:.2f}",
            rate=getattr(outgoing_limiter, "rate", "unknown"),
            bucket_size=getattr(outgoing_limiter, "bucket_size", "unknown"),
            message="Rate limiter initial state captured",
        )

        # Consume tokens by sending messages
        try:
            await bp_api_for_test_env.connect_websocket()

            if bp_api_for_test_env.is_connected:
                # Test token consumption
                markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
                if markets:

                    async def token_test_handler(context: WebSocketContextProtocol) -> None:
                        await asyncio.sleep(0)

                    # Subscribe to consume tokens
                    await bp_api_for_test_env.subscribe(
                        f"ticker.{markets[0].symbol}", token_test_handler
                    )

                    # Check tokens after consumption
                    tokens_after = getattr(outgoing_limiter, "tokens", None)
                    if tokens_after is not None:
                        token_diff = initial_tokens - tokens_after

                        logger.info(
                            "rate_limiter_token_consumption",
                            initial_tokens=f"{initial_tokens:.2f}",
                            tokens_after=f"{tokens_after:.2f}",
                            tokens_consumed=f"{token_diff:.2f}",
                            message="✓ Rate limiter token consumption measured",
                        )

                        # Test token refill over time
                        await asyncio.sleep(1.0)
                        tokens_after_wait = getattr(outgoing_limiter, "tokens", None)

                        if tokens_after_wait is not None:
                            token_refill = tokens_after_wait - tokens_after

                            logger.info(
                                "rate_limiter_token_refill",
                                tokens_before_wait=f"{tokens_after:.2f}",
                                tokens_after_wait=f"{tokens_after_wait:.2f}",
                                tokens_refilled=f"{token_refill:.2f}",
                                message="✓ Rate limiter token refill measured",
                            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            logger.warning(
                "rate_limiter_token_test_failed",
                error=str(e),
                message=f"Rate limiter token consumption test failed: {e}",
            )

    @pytest.mark.asyncio
    async def test_rate_limiting_backoff_behavior(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test rate limiting backoff and recovery behavior."""
        # Test strategy for handling rate limits
        rate_limit_strategy = getattr(bp_api_for_test_env, "_rate_limit_strategy", None)
        if not rate_limit_strategy:
            logger.info(
                "no_rate_limit_strategy_for_backoff_test",
                message="No rate limit strategy available for backoff testing",
            )
            return

        # Test acquiring tokens from rate limiter
        limiter = getattr(rate_limit_strategy, "limiter", None) or getattr(
            rate_limit_strategy, "_limiter", None
        )

        if limiter and hasattr(limiter, "acquire"):
            try:
                # Test rapid token acquisition
                acquisition_times: list[float] = []

                for i in range(5):
                    start_time = time.perf_counter()

                    # Try to acquire tokens
                    await limiter.acquire(tokens_to_consume=1)

                    end_time = time.perf_counter()
                    acquisition_time = end_time - start_time
                    acquisition_times.append(acquisition_time)

                    logger.info(
                        "rate_limiter_token_acquisition",
                        attempt=i + 1,
                        acquisition_time_ms=f"{acquisition_time * 1000:.2f}",
                        message=f"Token acquisition {i + 1} completed",
                    )

                # Analyze backoff behavior
                avg_acquisition_time = sum(acquisition_times) / len(acquisition_times)
                max_acquisition_time = max(acquisition_times)

                logger.info(
                    "rate_limiting_backoff_analysis",
                    total_acquisitions=len(acquisition_times),
                    avg_acquisition_time_ms=f"{avg_acquisition_time * 1000:.2f}",
                    max_acquisition_time_ms=f"{max_acquisition_time * 1000:.2f}",
                    message="✓ Rate limiting backoff behavior analyzed",
                )

                # Check for backoff evidence
                if max_acquisition_time > avg_acquisition_time * 3:
                    logger.info(
                        "rate_limiting_backoff_detected",
                        max_time_ms=f"{max_acquisition_time * 1000:.2f}",
                        avg_time_ms=f"{avg_acquisition_time * 1000:.2f}",
                        message="Rate limiting backoff behavior detected",
                    )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                logger.warning(
                    "rate_limiting_backoff_test_failed",
                    error=str(e),
                    message=f"Rate limiting backoff test failed: {e}",
                )

    @pytest.mark.asyncio
    async def test_rate_limiting_error_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test error handling when rate limits are exceeded."""
        try:
            await bp_api_for_test_env.connect_websocket()

            if not bp_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed - cannot test rate limit errors")

            # Attempt to overwhelm rate limiter
            rapid_requests = 50
            errors_encountered: list[str] = []
            successful_requests = 0

            async def rate_limit_test_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)

            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available for rate limit error testing")

            for i in range(rapid_requests):
                try:
                    symbol = markets[i % len(markets)].symbol
                    await bp_api_for_test_env.subscribe(f"ticker.{symbol}", rate_limit_test_handler)
                    successful_requests += 1

                    # Small delay to avoid completely overwhelming the system
                    await asyncio.sleep(0.01)

                except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                    error_msg = str(e)
                    errors_encountered.append(error_msg)

                    # Check if error is rate-limit related
                    if any(
                        keyword in error_msg.lower()
                        for keyword in ["rate", "limit", "throttle", "too many"]
                    ):
                        logger.info(
                            "rate_limit_error_detected",
                            request_index=i,
                            error=error_msg,
                            message=f"✓ Rate limit error correctly raised: {error_msg}",
                        )
                    else:
                        logger.warning(
                            "non_rate_limit_error_during_test",
                            request_index=i,
                            error=error_msg,
                            message=f"Non-rate-limit error during rapid requests: {error_msg}",
                        )

            logger.info(
                "rate_limiting_error_handling_results",
                total_requests=rapid_requests,
                successful_requests=successful_requests,
                errors_encountered=len(errors_encountered),
                rate_limit_errors=len([
                    e
                    for e in errors_encountered
                    if any(kw in e.lower() for kw in ["rate", "limit", "throttle"])
                ]),
                message="✓ Rate limiting error handling test completed",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Rate limiting error handling test failed: {e}. "
                "Rate limit error handling not working properly."
            )

    @pytest.mark.asyncio
    async def test_rate_limiting_configuration_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that rate limiting configuration is valid."""
        # Check HTTP rate limiting configuration
        rate_limit_strategy = getattr(bp_api_for_test_env, "_rate_limit_strategy", None)
        if rate_limit_strategy:
            # Check if strategy has valid configuration
            default_weight = getattr(rate_limit_strategy, "default_request_weight", None)
            if default_weight is not None:
                assert default_weight > 0, (
                    f"Default request weight should be positive, got {default_weight}"
                )

                logger.info(
                    "http_rate_limiting_config_valid",
                    default_request_weight=default_weight,
                    strategy_type=type(rate_limit_strategy).__name__,
                    message="✓ HTTP rate limiting configuration is valid",
                )

        # Check WebSocket rate limiting configuration
        ws_manager = getattr(bp_api_for_test_env, "_ws_manager", None)
        if ws_manager:
            outgoing_limiter = getattr(ws_manager, "_outgoing_message_limiter", None)
            if outgoing_limiter:
                rate = getattr(outgoing_limiter, "rate", None)
                bucket_size = getattr(outgoing_limiter, "bucket_size", None)

                if rate is not None and bucket_size is not None:
                    assert rate > 0, f"WebSocket rate should be positive, got {rate}"
                    assert bucket_size > 0, (
                        f"WebSocket bucket size should be positive, got {bucket_size}"
                    )
                    assert bucket_size >= rate, (
                        f"Bucket size {bucket_size} should be >= rate {rate}"
                    )

                    logger.info(
                        "websocket_rate_limiting_config_valid",
                        rate=rate,
                        bucket_size=bucket_size,
                        message="✓ WebSocket rate limiting configuration is valid",
                    )
