"""
Unit tests for HyperliquidRateLimitStrategy.
Tests the dual-limiter strategy that manages IP weights and address action counts for Hyperliquid.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from cyberdelta.apis.hyperliquid.hl_rate_limit_strategy import HyperliquidRateLimitStrategy
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
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

    @pytest.fixture
    def mock_ip_limiter(self) -> AsyncMock:
        """Create a mock IP weight limiter."""
        limiter = AsyncMock(spec=TokenBucketRateLimiterRuntime)
        limiter.acquire.return_value = 0.0
        return limiter

    @pytest.fixture
    def mock_address_limiter(self) -> AsyncMock:
        """Create a mock address action limiter."""
        limiter = AsyncMock(spec=TokenBucketRateLimiterRuntime)
        limiter.acquire.return_value = 0.0
        return limiter

    @pytest.fixture
    def strategy_with_mocks(
        self, hl_config: ExchangeSpecificConfig, mock_ip_limiter: AsyncMock, mock_address_limiter: AsyncMock
    ) -> HyperliquidRateLimitStrategy:
        """Create a HyperliquidRateLimitStrategy with mocked limiters."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        # Replace the real limiters with mocks
        strategy._ip_weight_limiter = mock_ip_limiter
        strategy._address_action_limiter = mock_address_limiter
        return strategy

    def test_initialization_creates_limiters(self, hl_config: ExchangeSpecificConfig) -> None:
        """Test that initialization correctly creates both limiters."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        assert strategy._ip_weight_limiter is not None
        assert strategy._address_action_limiter is not None
        assert strategy._request_weighter is not None

    def test_initialization_calculates_correct_rates(self, hl_config: ExchangeSpecificConfig) -> None:
        """Test that limiter rates are calculated correctly from config."""
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        # IP weight limiter: 1200/min = 20/sec
        expected_ip_rate = 1200.0 / 60.0
        assert strategy._ip_weight_limiter.rate == expected_ip_rate
        
        # Address action limiter: 300/min = 5/sec
        expected_address_rate = 300.0 / 60.0
        assert strategy._address_action_limiter.rate == expected_address_rate

    async def test_prepare_and_acquire_exchange_endpoint_single_action(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy, 
        mock_ip_limiter: AsyncMock, 
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test prepare_and_acquire for /exchange endpoint with single action."""
        request_context = {
            "endpoint": "/exchange",
            "action_payload": {"actions": [{"type": "order", "orderType": "Limit"}]},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # Should acquire 1 IP weight token for single action
        mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=1)
        # Should acquire 1 address action token for single action
        mock_address_limiter.acquire.assert_called_once_with(tokens_to_consume=1)

    async def test_prepare_and_acquire_exchange_endpoint_multiple_actions(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test prepare_and_acquire for /exchange endpoint with multiple actions."""
        # 45 actions: IP weight = 1 + (45 // 40) = 2, address actions = 45
        actions = [{"type": "order"}] * 45
        request_context = {
            "endpoint": "/exchange",
            "action_payload": {"actions": actions},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # IP weight: base (1) + batch factor (45 // 40 = 1) = 2
        mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=2)
        # Address actions: 45
        mock_address_limiter.acquire.assert_called_once_with(tokens_to_consume=45)

    async def test_prepare_and_acquire_info_endpoint_known_type(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test prepare_and_acquire for /info endpoint with known type."""
        request_context = {
            "endpoint": "/info",
            "action_payload": {"type": "l2Book", "coin": "BTC"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # Should acquire IP weight based on config (l2Book = 2)
        mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=2)
        # /info endpoint should not consume address actions
        mock_address_limiter.acquire.assert_not_called()

    async def test_prepare_and_acquire_info_endpoint_expensive_type(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test prepare_and_acquire for expensive /info endpoint type."""
        request_context = {
            "endpoint": "/info",
            "action_payload": {"type": "userRole", "user": "0x123"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # userRole is expensive (60 IP weight)
        mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=60)
        mock_address_limiter.acquire.assert_not_called()

    async def test_prepare_and_acquire_info_endpoint_unknown_type(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test prepare_and_acquire for unknown /info endpoint type."""
        request_context = {
            "endpoint": "/info",
            "action_payload": {"type": "unknownType", "param": "value"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # Should use default weight (20)
        mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=20)
        mock_address_limiter.acquire.assert_not_called()

    async def test_prepare_and_acquire_info_endpoint_none_payload(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test prepare_and_acquire for /info endpoint with None payload."""
        request_context = {
            "endpoint": "/info",
            "action_payload": None,
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # Should use default weight when payload is None
        mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=20)
        mock_address_limiter.acquire.assert_not_called()

    async def test_prepare_and_acquire_exchange_endpoint_empty_actions(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test prepare_and_acquire for /exchange with empty actions array."""
        request_context = {
            "endpoint": "/exchange",
            "action_payload": {"actions": []},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # Empty actions should default to 1 for both IP weight and address actions
        mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=1)
        mock_address_limiter.acquire.assert_called_once_with(tokens_to_consume=1)

    async def test_prepare_and_acquire_unknown_endpoint(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test prepare_and_acquire for unknown endpoint."""
        request_context = {
            "endpoint": "/unknown",
            "action_payload": {"param": "value"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # Should use default IP weight, no address actions
        mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=20)
        mock_address_limiter.acquire.assert_not_called()

    async def test_prepare_and_acquire_zero_ip_weight_skips_limiter(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test that zero IP weight skips the IP limiter."""
        # Mock the weighter to return 0 IP weight
        strategy_with_mocks._request_weighter.get_ip_weight = Mock(return_value=0)
        strategy_with_mocks._request_weighter.get_address_action_count = Mock(return_value=1)
        
        request_context = {
            "endpoint": "/exchange",
            "action_payload": {"actions": [{"type": "order"}]},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # Should not call IP limiter when cost is 0
        mock_ip_limiter.acquire.assert_not_called()
        # Should still call address limiter
        mock_address_limiter.acquire.assert_called_once_with(tokens_to_consume=1)

    async def test_prepare_and_acquire_zero_address_actions_skips_limiter(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test that zero address actions skips the address limiter."""
        # Mock the weighter to return 0 address actions
        strategy_with_mocks._request_weighter.get_ip_weight = Mock(return_value=2)
        strategy_with_mocks._request_weighter.get_address_action_count = Mock(return_value=0)
        
        request_context = {
            "endpoint": "/info",
            "action_payload": {"type": "l2Book", "coin": "BTC"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy_with_mocks.prepare_and_acquire(request_context)
        
        assert result is None
        # Should call IP limiter
        mock_ip_limiter.acquire.assert_called_once_with(tokens_to_consume=2)
        # Should not call address limiter when cost is 0
        mock_address_limiter.acquire.assert_not_called()

    async def test_prepare_and_acquire_concurrent_calls(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test concurrent calls to prepare_and_acquire."""
        import asyncio
        
        request_context = {
            "endpoint": "/exchange",
            "action_payload": {"actions": [{"type": "order"}]},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        # Run multiple concurrent calls
        tasks = [
            strategy_with_mocks.prepare_and_acquire(request_context)
            for _ in range(3)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # All should return None
        assert all(result is None for result in results)
        # Should have called both limiters 3 times each
        assert mock_ip_limiter.acquire.call_count == 3
        assert mock_address_limiter.acquire.call_count == 3

    async def test_exception_from_ip_limiter_propagates(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test that exceptions from IP limiter are propagated."""
        mock_ip_limiter.acquire.side_effect = Exception("IP limiter error")
        
        request_context = {
            "endpoint": "/exchange",
            "action_payload": {"actions": [{"type": "order"}]},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        with pytest.raises(Exception, match="IP limiter error"):
            await strategy_with_mocks.prepare_and_acquire(request_context)

    async def test_exception_from_address_limiter_propagates(
        self, strategy_with_mocks: HyperliquidRateLimitStrategy,
        mock_ip_limiter: AsyncMock,
        mock_address_limiter: AsyncMock
    ) -> None:
        """Test that exceptions from address limiter are propagated."""
        mock_address_limiter.acquire.side_effect = Exception("Address limiter error")
        
        request_context = {
            "endpoint": "/exchange",
            "action_payload": {"actions": [{"type": "order"}]},
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        with pytest.raises(Exception, match="Address limiter error"):
            await strategy_with_mocks.prepare_and_acquire(request_context)


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
        
        # Check initial state
        initial_ip_tokens = strategy._ip_weight_limiter.tokens
        initial_address_tokens = strategy._address_action_limiter.tokens
        
        request_context = {
            "endpoint": "/exchange",
            "action_payload": {"actions": [{"type": "order"}] * 3},  # 3 actions
            "method": "POST",
            "exchange_name": "hyperliquid"
        }
        
        result = await strategy.prepare_and_acquire(request_context)
        
        assert result is None
        # Should have consumed 1 IP weight token (base weight for 3 actions)
        assert strategy._ip_weight_limiter.tokens == initial_ip_tokens - 1
        # Should have consumed 3 address action tokens
        assert strategy._address_action_limiter.tokens == initial_address_tokens - 3

    async def test_real_limiters_rate_limiting_behavior(self, hl_config: ExchangeSpecificConfig) -> None:
        """Test that real limiters enforce rate limits."""
        import time
        
        strategy = HyperliquidRateLimitStrategy(hl_config)
        
        # Consume all available tokens
        await strategy._ip_weight_limiter.acquire(tokens_to_consume=int(strategy._ip_weight_limiter.tokens))
        
        request_context = {
            "endpoint": "/info",
            "action_payload": {"type": "l2Book", "coin": "BTC"},
            "method": "GET",
            "exchange_name": "hyperliquid"
        }
        
        # Next call should wait for token refill
        start_time = time.time()
        result = await strategy.prepare_and_acquire(request_context)
        end_time = time.time()
        
        assert result is None
        # Should have waited for at least some time (allowing for test tolerance)
        assert end_time - start_time >= 1.5  # l2Book costs 2 tokens, rate is 1/sec