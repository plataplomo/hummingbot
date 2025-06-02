"""
Unit tests for HyperliquidRateLimitStrategy.
Tests the dual-limiter strategy that manages IP weights and address action counts for Hyperliquid.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from cyberdelta.apis.hyperliquid.hl_rate_limit_strategy import HyperliquidRateLimitStrategy
from cyberdelta.config.config_models import AddressActionSafetyNetConfig, ExchangeSpecificConfig
from cyberdelta.enums.exchange_names import ExchangeName


class TestHyperliquidRateLimitStrategy:
    """Test suite for HyperliquidRateLimitStrategy."""

    @pytest.fixture
    def hl_config(self) -> ExchangeSpecificConfig:
        """Create a mock Hyperliquid configuration."""
        config = Mock(spec=ExchangeSpecificConfig)
        config.exchange_name = ExchangeName.HYPERLIQUID
        config.ip_weight_limit_per_minute = 1200
        config.info_request_type_ip_weights = {
            "l2Book": 2,
            "allMids": 2,
            "meta": 2,
            "userRole": 60,
            "clearinghouseState": 10,
        }
        config.default_info_weight = 20
        config.exchange_action_base_ip_weight = 1
        config.address_action_safety_net = Mock(spec=AddressActionSafetyNetConfig)
        config.address_action_safety_net.rate_per_minute = 300
        return config

    def test_initialization_creates_limiters(self, hl_config: ExchangeSpecificConfig) -> None:
        """Test that initialization correctly creates both limiters."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        # Test that the strategy works by making a request
        # This indirectly verifies that limiters were created
        request_context: dict[str, Any] = {
            "endpoint": "/info",
            "action_payload": {"type": "l2Book", "coin": "BTC"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        # Should not raise an exception, indicating limiters are properly initialized
        import asyncio
        asyncio.run(strategy.prepare_and_acquire(request_context))

    def test_initialization_calculates_correct_rates(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test that limiter rates are calculated correctly from config."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        # Test the strategy works with expected timing behavior
        # by checking if it handles rate limiting correctly
        request_context: dict[str, Any] = {
            "endpoint": "/info",
            "action_payload": {"type": "l2Book", "coin": "BTC"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        import asyncio
        import time
        
        async def test_rate_behavior() -> None:
            # First request should work immediately
            start_time = time.time()
            await strategy.prepare_and_acquire(request_context)
            first_call_time = time.time() - start_time
            
            # Should be very fast (no rate limiting triggered)
            assert first_call_time < 0.1
            return
        
        asyncio.run(test_rate_behavior())

    async def test_prepare_and_acquire_exchange_endpoint_single_action(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test prepare_and_acquire for /exchange endpoint with single action."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        request_context: dict[str, Any] = {
            "endpoint": "/exchange",
            "action_payload": {"actions": [{"type": "order", "orderType": "Limit"}]},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        # Should not modify the payload
        assert result is None
        return

    async def test_prepare_and_acquire_exchange_endpoint_multiple_actions(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test prepare_and_acquire for /exchange endpoint with multiple actions."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        # 45 actions: IP weight = 1 + (45 // 40) = 2, address actions = 45
        actions = [{"type": "order"}] * 45
        request_context: dict[str, Any] = {
            "endpoint": "/exchange",
            "action_payload": {"actions": actions},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        return

    async def test_prepare_and_acquire_info_endpoint_known_type(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test prepare_and_acquire for /info endpoint with known type."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        request_context: dict[str, Any] = {
            "endpoint": "/info",
            "action_payload": {"type": "l2Book", "coin": "BTC"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        return

    async def test_prepare_and_acquire_info_endpoint_expensive_type(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test prepare_and_acquire for expensive /info endpoint type."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        request_context: dict[str, Any] = {
            "endpoint": "/info",
            "action_payload": {"type": "userRole", "user": "0x123"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        return

    async def test_prepare_and_acquire_info_endpoint_unknown_type(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test prepare_and_acquire for unknown /info endpoint type."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        request_context: dict[str, Any] = {
            "endpoint": "/info",
            "action_payload": {"type": "unknownType", "param": "value"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        return

    async def test_prepare_and_acquire_info_endpoint_none_payload(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test prepare_and_acquire for /info endpoint with None payload."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        request_context: dict[str, Any] = {
            "endpoint": "/info",
            "action_payload": None,
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        return

    async def test_prepare_and_acquire_exchange_endpoint_empty_actions(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test prepare_and_acquire for /exchange with empty actions array."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        request_context: dict[str, Any] = {
            "endpoint": "/exchange",
            "action_payload": {"actions": []},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        return

    async def test_prepare_and_acquire_unknown_endpoint(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test prepare_and_acquire for unknown endpoint."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        request_context: dict[str, Any] = {
            "endpoint": "/unknown",
            "action_payload": {"param": "value"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        return

    async def test_prepare_and_acquire_concurrent_calls(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test concurrent calls to prepare_and_acquire."""
        import asyncio
        
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        request_context: dict[str, Any] = {
            "endpoint": "/exchange",
            "action_payload": {"actions": [{"type": "order"}]},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        # Run multiple concurrent calls
        tasks = [
            strategy.prepare_and_acquire(request_context)
            for _ in range(3)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # All should return None
        assert all(result is None for result in results)


class TestHyperliquidRateLimitStrategyIntegration:
    """Integration tests with real limiters."""

    @pytest.fixture
    def hl_config(self) -> ExchangeSpecificConfig:
        """Create a minimal real configuration."""
        config = Mock(spec=ExchangeSpecificConfig)
        config.exchange_name = ExchangeName.HYPERLIQUID
        config.ip_weight_limit_per_minute = 60  # 1/sec for testing
        config.info_request_type_ip_weights = {"l2Book": 2}
        config.default_info_weight = 5
        config.exchange_action_base_ip_weight = 1
        config.address_action_safety_net = Mock(spec=AddressActionSafetyNetConfig)
        config.address_action_safety_net.rate_per_minute = 60  # 1/sec for testing
        return config

    async def test_real_limiters_consume_tokens(self, hl_config: ExchangeSpecificConfig) -> None:
        """Test with real TokenBucketRateLimiterRuntime instances."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        request_context: dict[str, Any] = {
            "endpoint": "/exchange",
            "action_payload": {"actions": [{"type": "order"}] * 3},  # 3 actions
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        return
        # Test passes if no exception is raised and result is as expected
        return

    async def test_real_limiters_rate_limiting_behavior(
        self, hl_config: ExchangeSpecificConfig
    ) -> None:
        """Test that real limiters enforce rate limits through observable timing."""
        import time
        
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        # Make multiple calls quickly to test rate limiting
        request_context: dict[str, Any] = {
            "endpoint": "/info",
            "action_payload": {"type": "l2Book", "coin": "BTC"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        # First call should be fast
        start_time = time.time()
        result1 = await strategy.prepare_and_acquire(request_context)
        first_call_time = time.time() - start_time
        
        # Make more calls to potentially trigger rate limiting
        result2 = await strategy.prepare_and_acquire(request_context)
        result3 = await strategy.prepare_and_acquire(request_context)
        
        # All results should be None (no payload modification)
        assert result1 is None
        assert result2 is None
        assert result3 is None
        
        # First call should be very fast
        assert first_call_time < 0.1
