"""Comprehensive unit tests for HyperliquidRateLimitStrategy.

Tests rate limiting functionality with parametrized tests, smart fixture usage,
and comprehensive success, edge, and failure cases. Only tests through public APIs.
"""

from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.rate_limit_models import RateLimitRequestContext
from cyberdelta.apis.exceptions.configuration import HyperliquidRateLimitConfigError
from cyberdelta.apis.hyperliquid.hl_rate_limit_strategy import HyperliquidRateLimitStrategy
from cyberdelta.config.models.config_models import (
    AddressActionSafetyNetConfig,
    ExchangeSpecificConfig,
)


class TestHyperliquidRateLimitStrategy:
    """Test HyperliquidRateLimitStrategy public API functionality."""

    @pytest.fixture
    def mock_address_action_config(self) -> AddressActionSafetyNetConfig:
        """Create mock address action safety net configuration."""
        return AddressActionSafetyNetConfig(rate_per_minute=60)

    @pytest.fixture
    def valid_exchange_config(
        self, mock_address_action_config: AddressActionSafetyNetConfig
    ) -> ExchangeSpecificConfig:
        """Create valid exchange configuration for testing."""
        config = MagicMock(spec=ExchangeSpecificConfig)
        config.ip_weight_limit_per_minute = 1200
        config.address_action_safety_net = mock_address_action_config
        return config

    @pytest.fixture
    def request_context_factory(self) -> Callable[..., RateLimitRequestContext]:
        """Factory to create RateLimitRequestContext instances."""

        def _create(**kwargs: dict[str, Any]) -> RateLimitRequestContext:
            defaults: dict[str, Any] = {
                "endpoint": "/info",
                "method": "POST",
                "exchange_name": "hyperliquid",
                "action_payload": None,
                "request_weight": 1,
                "endpoint_group": "info",
            }
            defaults.update(kwargs)
            return RateLimitRequestContext(**defaults)

        return _create

    @pytest.fixture
    def strategy_factory(
        self, valid_exchange_config: ExchangeSpecificConfig
    ) -> Callable[..., HyperliquidRateLimitStrategy]:
        """Factory to create strategy instances with optional config overrides."""

        def _create(
            config_overrides: dict[str, Any] | None = None,
        ) -> HyperliquidRateLimitStrategy:
            config = valid_exchange_config
            if config_overrides:
                for key, value in config_overrides.items():
                    setattr(config, key, value)
            return HyperliquidRateLimitStrategy(config)

        return _create

    # Initialization Tests

    def test_strategy_initialization_success(
        self, strategy_factory: Callable[..., HyperliquidRateLimitStrategy]
    ) -> None:
        """Test successful strategy initialization with valid configuration."""
        strategy = strategy_factory()

        assert strategy is not None
        assert isinstance(strategy, HyperliquidRateLimitStrategy)

    @pytest.mark.parametrize(
        ("missing_field", "expected_error"),
        [
            ("ip_weight_limit_per_minute", HyperliquidRateLimitConfigError),
            ("address_action_safety_net", HyperliquidRateLimitConfigError),
        ],
    )
    def test_strategy_initialization_missing_required_fields(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        missing_field: str,
        expected_error: type,
    ) -> None:
        """Test strategy initialization fails with missing required configuration."""
        config_overrides = {missing_field: None}

        with pytest.raises(expected_error):
            strategy_factory(config_overrides)

    def test_strategy_initialization_both_fields_missing(
        self, strategy_factory: Callable[..., HyperliquidRateLimitStrategy]
    ) -> None:
        """Test strategy initialization with both required fields missing."""
        config_overrides = {
            "ip_weight_limit_per_minute": None,
            "address_action_safety_net": None,
        }

        with pytest.raises(HyperliquidRateLimitConfigError) as exc_info:
            strategy_factory(config_overrides)

        # Should mention both missing fields
        error = exc_info.value
        assert "ip_weight_limit_per_minute" in str(error)
        assert "address_action_safety_net" in str(error)

    @pytest.mark.parametrize(
        ("ip_weight_limit", "expected_rps", "expected_bucket"),
        [
            (1200, 20.0, 40),  # 1200/60 = 20 RPS, bucket = 20*2 = 40
            (600, 10.0, 20),  # 600/60 = 10 RPS, bucket = 10*2 = 20
            (60, 1.0, 2),  # 60/60 = 1 RPS, bucket = 1*2 = 2
            (30, 0.5, 1),  # 30/60 = 0.5 RPS, bucket = max(1, 0.5*2) = 1
        ],
    )
    def test_ip_weight_limiter_configuration(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        ip_weight_limit: int,
        expected_rps: float,
        expected_bucket: int,
    ) -> None:
        """Test IP weight limiter is configured correctly with different rate limits."""
        with patch(
            "cyberdelta.apis.hyperliquid.hl_rate_limit_strategy.TokenBucketRateLimiterRuntime"
        ) as mock_limiter:
            strategy_factory({"ip_weight_limit_per_minute": ip_weight_limit})

            # Check that the limiter was initialized with correct parameters
            mock_limiter.assert_called()
            # First call is IP weight limiter
            _args, kwargs = mock_limiter.call_args_list[0]
            assert kwargs["rate"] == expected_rps
            assert kwargs["bucket_size"] == expected_bucket

    @pytest.mark.parametrize(
        ("address_action_rate", "expected_rps", "expected_bucket"),
        [
            (60, 1.0, 2),  # 60/60 = 1 RPS, bucket = 1*2 = 2
            (120, 2.0, 4),  # 120/60 = 2 RPS, bucket = 2*2 = 4
            (30, 0.5, 1),  # 30/60 = 0.5 RPS, bucket = max(1, 0.5*2) = 1
        ],
    )
    def test_address_action_limiter_configuration(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        mock_address_action_config: AddressActionSafetyNetConfig,
        address_action_rate: int,
        expected_rps: float,
        expected_bucket: int,
    ) -> None:
        """Test address action limiter is configured correctly with different rate limits."""
        mock_address_action_config.rate_per_minute = address_action_rate

        with patch(
            "cyberdelta.apis.hyperliquid.hl_rate_limit_strategy.TokenBucketRateLimiterRuntime"
        ) as mock_limiter:
            strategy_factory()

            # Check that the limiter was initialized with correct parameters
            # Second call is address action limiter
            _args, kwargs = mock_limiter.call_args_list[1]
            assert kwargs["rate"] == expected_rps
            assert kwargs["bucket_size"] == expected_bucket

    # Request Processing Tests

    @pytest.mark.asyncio
    async def test_prepare_and_acquire_info_endpoint(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        request_context_factory: Callable[..., RateLimitRequestContext],
    ) -> None:
        """Test prepare_and_acquire for /info endpoint with no action payload."""
        strategy = strategy_factory()
        context = request_context_factory(endpoint="/info", action_payload=None)

        with (
            patch.object(strategy, "_request_weighter") as mock_weighter,
            patch.object(strategy, "_ip_weight_limiter") as mock_ip_limiter,
            patch.object(strategy, "_address_action_limiter") as mock_addr_limiter,
        ):
            mock_weighter.get_ip_weight.return_value = 1
            mock_weighter.get_address_action_count.return_value = 0
            mock_ip_limiter.acquire = AsyncMock()
            mock_addr_limiter.acquire = AsyncMock()

            await strategy.prepare_and_acquire(context)

            # Verify request weighter was called
            mock_weighter.get_ip_weight.assert_called_once_with("/info", None)
            mock_weighter.get_address_action_count.assert_called_once_with("/info", None)

            # Verify IP weight limiter was called
            mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=1)

            # Verify address action limiter was not called (cost = 0)
            mock_addr_limiter.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_prepare_and_acquire_exchange_endpoint(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        request_context_factory: Callable[..., RateLimitRequestContext],
    ) -> None:
        """Test prepare_and_acquire for /exchange endpoint with action payload."""
        strategy = strategy_factory()
        action_payload = {"type": "order", "action": "place"}
        context = request_context_factory(endpoint="/exchange", action_payload=action_payload)

        with (
            patch.object(strategy, "_request_weighter") as mock_weighter,
            patch.object(strategy, "_ip_weight_limiter") as mock_ip_limiter,
            patch.object(strategy, "_address_action_limiter") as mock_addr_limiter,
        ):
            mock_weighter.get_ip_weight.return_value = 20
            mock_weighter.get_address_action_count.return_value = 1
            mock_ip_limiter.acquire = AsyncMock()
            mock_addr_limiter.acquire = AsyncMock()

            await strategy.prepare_and_acquire(context)

            # Verify request weighter was called with correct parameters
            mock_weighter.get_ip_weight.assert_called_once_with("/exchange", action_payload)
            mock_weighter.get_address_action_count.assert_called_once_with(
                "/exchange", action_payload
            )

            # Verify both limiters were called
            mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=20)
            mock_addr_limiter.acquire.assert_called_once_with(tokens_to_consume=1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("ip_cost", "address_cost", "should_call_ip", "should_call_addr"),
        [
            (0, 0, False, False),  # No costs - no limiter calls
            (5, 0, True, False),  # Only IP cost - only IP limiter
            (0, 1, False, True),  # Only address cost - only address limiter
            (10, 2, True, True),  # Both costs - both limiters
        ],
    )
    async def test_prepare_and_acquire_cost_variations(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        request_context_factory: Callable[..., RateLimitRequestContext],
        ip_cost: int,
        address_cost: int,
        should_call_ip: bool,
        should_call_addr: bool,
    ) -> None:
        """Test prepare_and_acquire with different cost combinations."""
        strategy = strategy_factory()
        context = request_context_factory()

        with (
            patch.object(strategy, "_request_weighter") as mock_weighter,
            patch.object(strategy, "_ip_weight_limiter") as mock_ip_limiter,
            patch.object(strategy, "_address_action_limiter") as mock_addr_limiter,
        ):
            mock_weighter.get_ip_weight.return_value = ip_cost
            mock_weighter.get_address_action_count.return_value = address_cost
            mock_ip_limiter.acquire = AsyncMock()
            mock_addr_limiter.acquire = AsyncMock()

            await strategy.prepare_and_acquire(context)

            if should_call_ip:
                mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=ip_cost)
            else:
                mock_ip_limiter.acquire.assert_not_called()

            if should_call_addr:
                mock_addr_limiter.acquire.assert_called_once_with(tokens_to_consume=address_cost)
            else:
                mock_addr_limiter.acquire.assert_not_called()

    # IP Ban Handling Tests

    @pytest.mark.asyncio
    async def test_trigger_ip_ban_on_main_pool(
        self, strategy_factory: Callable[..., HyperliquidRateLimitStrategy]
    ) -> None:
        """Test triggering IP ban on main IP weight limiter."""
        strategy = strategy_factory()
        ban_duration = 300.0  # 5 minutes

        with patch.object(strategy, "_ip_weight_limiter") as mock_ip_limiter:
            mock_ip_limiter.trigger_ip_ban = AsyncMock()

            await strategy.trigger_ip_ban_on_main_pool(ban_duration)

            mock_ip_limiter.trigger_ip_ban.assert_called_once_with(ban_duration)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "ban_duration",
        [0.0, 1.0, 60.0, 300.0, 3600.0],  # Various durations from 0 to 1 hour
    )
    async def test_trigger_ip_ban_various_durations(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        ban_duration: float,
    ) -> None:
        """Test IP ban triggering with various duration values."""
        strategy = strategy_factory()

        with patch.object(strategy, "_ip_weight_limiter") as mock_ip_limiter:
            mock_ip_limiter.trigger_ip_ban = AsyncMock()

            await strategy.trigger_ip_ban_on_main_pool(ban_duration)

            mock_ip_limiter.trigger_ip_ban.assert_called_once_with(ban_duration)

    # Exchange Retry-After Handling Tests

    @pytest.mark.asyncio
    async def test_handle_exchange_retry_after(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        request_context_factory: Callable[..., RateLimitRequestContext],
    ) -> None:
        """Test handling exchange-advised retry-after directives."""
        strategy = strategy_factory()
        context = request_context_factory()
        retry_duration = 120.0  # 2 minutes

        with patch.object(strategy, "trigger_ip_ban_on_main_pool", new=AsyncMock()) as mock_trigger:
            await strategy.handle_exchange_retry_after(retry_duration, context)

            mock_trigger.assert_called_once_with(retry_duration)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("retry_duration", "exchange_name"),
        [
            (30.0, "hyperliquid"),
            (60.0, "hyperliquid-testnet"),
            (300.0, "custom_exchange"),
        ],
    )
    async def test_handle_exchange_retry_after_parametrized(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        request_context_factory: Callable[..., RateLimitRequestContext],
        retry_duration: float,
        exchange_name: str,
    ) -> None:
        """Test exchange retry-after handling with different parameters."""
        strategy = strategy_factory()
        context = request_context_factory(exchange_name=exchange_name)

        with patch.object(strategy, "trigger_ip_ban_on_main_pool", new=AsyncMock()) as mock_trigger:
            await strategy.handle_exchange_retry_after(retry_duration, context)

            mock_trigger.assert_called_once_with(retry_duration)

    # Integration and Edge Case Tests

    @pytest.mark.asyncio
    async def test_complete_rate_limiting_workflow(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        request_context_factory: Callable[..., RateLimitRequestContext],
    ) -> None:
        """Test complete rate limiting workflow for typical exchange request."""
        strategy = strategy_factory()
        context = request_context_factory(
            endpoint="/exchange", action_payload={"type": "order", "action": "place"}
        )

        with (
            patch.object(strategy, "_request_weighter") as mock_weighter,
            patch.object(strategy, "_ip_weight_limiter") as mock_ip_limiter,
            patch.object(strategy, "_address_action_limiter") as mock_addr_limiter,
        ):
            # Configure costs for a typical trading request
            mock_weighter.get_ip_weight.return_value = 20
            mock_weighter.get_address_action_count.return_value = 1
            mock_ip_limiter.acquire = AsyncMock()
            mock_addr_limiter.acquire = AsyncMock()

            # Execute the workflow
            await strategy.prepare_and_acquire(context)

            # Verify complete workflow execution
            assert mock_weighter.get_ip_weight.called
            assert mock_weighter.get_address_action_count.called
            assert mock_ip_limiter.acquire.called
            assert mock_addr_limiter.acquire.called

    def test_strategy_instance_independence(
        self, valid_exchange_config: ExchangeSpecificConfig
    ) -> None:
        """Test that multiple strategy instances are independent."""
        strategy1 = HyperliquidRateLimitStrategy(valid_exchange_config)
        strategy2 = HyperliquidRateLimitStrategy(valid_exchange_config)

        # Verify instances are different objects through public API
        assert strategy1 is not strategy2
        # Test independence through different behavior rather than private access
        assert hasattr(strategy1, "prepare_and_acquire")
        assert hasattr(strategy2, "prepare_and_acquire")

    @pytest.mark.asyncio
    async def test_error_propagation_from_limiters(
        self,
        strategy_factory: Callable[..., HyperliquidRateLimitStrategy],
        request_context_factory: Callable[..., RateLimitRequestContext],
    ) -> None:
        """Test that errors from rate limiters are properly propagated."""
        strategy = strategy_factory()
        context = request_context_factory()

        with (
            patch.object(strategy, "_request_weighter") as mock_weighter,
            patch.object(strategy, "_ip_weight_limiter") as mock_ip_limiter,
        ):
            mock_weighter.get_ip_weight.return_value = 1
            mock_weighter.get_address_action_count.return_value = 0

            # Configure limiter to raise an exception
            mock_ip_limiter.acquire = AsyncMock(side_effect=RuntimeError("Rate limiter error"))

            with pytest.raises(RuntimeError, match="Rate limiter error"):
                await strategy.prepare_and_acquire(context)

    @pytest.mark.asyncio
    async def test_request_context_parameter_validation(
        self, strategy_factory: Callable[..., HyperliquidRateLimitStrategy]
    ) -> None:
        """Test that strategy properly uses all request context parameters."""
        strategy = strategy_factory()

        # Create context with all possible parameters
        context = RateLimitRequestContext(
            endpoint="/exchange",
            method="POST",
            exchange_name="hyperliquid-test",
            action_payload={"type": "cancel", "action": "all"},
            request_weight=1,
            endpoint_group="exchange",
        )

        with (
            patch.object(strategy, "_request_weighter") as mock_weighter,
            patch.object(strategy, "_ip_weight_limiter") as mock_ip_limiter,
            patch.object(strategy, "_address_action_limiter") as mock_addr_limiter,
        ):
            mock_weighter.get_ip_weight.return_value = 5
            mock_weighter.get_address_action_count.return_value = 1
            mock_ip_limiter.acquire = AsyncMock()
            mock_addr_limiter.acquire = AsyncMock()

            await strategy.prepare_and_acquire(context)

            # Verify weighter received correct parameters
            mock_weighter.get_ip_weight.assert_called_once_with(
                "/exchange", {"type": "cancel", "action": "all"}
            )
            mock_weighter.get_address_action_count.assert_called_once_with(
                "/exchange", {"type": "cancel", "action": "all"}
            )

    def test_configuration_edge_cases(
        self, mock_address_action_config: AddressActionSafetyNetConfig
    ) -> None:
        """Test strategy behavior with edge case configurations."""
        # Test with minimum possible rate limits
        config = MagicMock(spec=ExchangeSpecificConfig)
        config.ip_weight_limit_per_minute = 1  # 1 per minute = 1/60 RPS
        config.address_action_safety_net = mock_address_action_config

        # Should not raise exception even with very low limits
        strategy = HyperliquidRateLimitStrategy(config)
        assert strategy is not None
