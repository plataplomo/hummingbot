"""Unit tests for HyperliquidRequestWeighter.

Tests the IP weight and address action calculation logic for Hyperliquid requests.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from cyberdelta.apis.exceptions.configuration import HyperliquidRateLimitConfigError
from cyberdelta.apis.hyperliquid.hl_request_weighter import HyperliquidRequestWeighter
from cyberdelta.config.models.exchange_config import (
    AddressActionSafetyNetConfig,
    ExchangeSpecificConfig,
)
from cyberdelta.enums.exchange_names import ExchangeName


class TestHyperliquidRequestWeighter:
    """Test suite for HyperliquidRequestWeighter."""

    @pytest.fixture
    def hl_config(self) -> ExchangeSpecificConfig:
        """Create a mock Hyperliquid configuration.

        Returns:
            Mock ExchangeSpecificConfig for Hyperliquid testing.
        """
        config = Mock(spec=ExchangeSpecificConfig)
        config.exchange_name = ExchangeName.HYPERLIQUID
        config.info_request_type_ip_weights = {
            "l2Book": 2,
            "allMids": 2,
            "meta": 2,
            "userRole": 60,
            "clearinghouseState": 10,
            "openOrders": 1,
        }
        config.default_info_weight = 20
        config.exchange_action_base_ip_weight = 1
        config.address_action_safety_net = Mock(spec=AddressActionSafetyNetConfig)
        config.address_action_safety_net.rate_per_minute = 300
        return config

    @pytest.fixture
    def weighter(self, hl_config: ExchangeSpecificConfig) -> HyperliquidRequestWeighter:
        """Create a HyperliquidRequestWeighter with mock config.

        Returns:
            HyperliquidRequestWeighter instance for testing.
        """
        return HyperliquidRequestWeighter(hl_config)

    def test_initialization_with_valid_config(self, hl_config: ExchangeSpecificConfig) -> None:
        """Test successful initialization with valid config."""
        weighter = HyperliquidRequestWeighter(hl_config)
        assert weighter.hl_exchange_config is hl_config

    def test_initialization_missing_required_fields_raises_error(self) -> None:
        """Test initialization fails with missing required fields."""
        config = Mock(spec=ExchangeSpecificConfig)
        config.info_request_type_ip_weights = None  # Missing required field
        config.default_info_weight = 20
        config.exchange_action_base_ip_weight = 1

        with pytest.raises(
            HyperliquidRateLimitConfigError, match="HyperliquidRateLimitStrategy requires"
        ):
            HyperliquidRequestWeighter(config)


class TestHyperliquidRequestWeighterIPWeight:
    """Test IP weight calculation logic."""

    @pytest.fixture
    def hl_config(self) -> ExchangeSpecificConfig:
        """Create a mock Hyperliquid configuration.

        Returns:
            Mock ExchangeSpecificConfig for Hyperliquid testing.
        """
        config = Mock(spec=ExchangeSpecificConfig)
        config.info_request_type_ip_weights = {
            "l2Book": 2,
            "allMids": 2,
            "meta": 2,
            "userRole": 60,
            "clearinghouseState": 10,
        }
        config.default_info_weight = 20
        config.exchange_action_base_ip_weight = 1
        config.address_action_safety_net = Mock()
        return config

    @pytest.fixture
    def weighter(self, hl_config: ExchangeSpecificConfig) -> HyperliquidRequestWeighter:
        """Create a HyperliquidRequestWeighter.

        Returns:
            HyperliquidRequestWeighter instance for testing.
        """
        return HyperliquidRequestWeighter(hl_config)

    def test_exchange_endpoint_single_action(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation for /exchange endpoint with single action."""
        payload = {"actions": [{"type": "order", "orderType": "Limit"}]}

        ip_weight = weighter.get_ip_weight("/exchange", payload)

        # base_weight (1) + (batch_length (1) // 40) = 1 + 0 = 1
        assert ip_weight == 1

    def test_exchange_endpoint_multiple_actions(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation for /exchange endpoint with multiple actions."""
        payload = {"actions": [{"type": "order"}] * 45}  # 45 actions

        ip_weight = weighter.get_ip_weight("/exchange", payload)

        # base_weight (1) + (batch_length (45) // 40) = 1 + 1 = 2
        assert ip_weight == 2

    def test_exchange_endpoint_batch_formula_large(
        self,
        weighter: HyperliquidRequestWeighter,
    ) -> None:
        """Test IP weight calculation for large batch."""
        payload = {"actions": [{"type": "order"}] * 120}  # 120 actions

        ip_weight = weighter.get_ip_weight("/exchange", payload)

        # base_weight (1) + (batch_length (120) // 40) = 1 + 3 = 4
        assert ip_weight == 4

    def test_exchange_endpoint_empty_actions(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation for empty actions array."""
        payload: dict[str, Any] = {"actions": []}

        ip_weight = weighter.get_ip_weight("/exchange", payload)

        # Empty actions should default to 1 action
        assert ip_weight == 1

    def test_exchange_endpoint_no_actions_key(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation when actions key is missing."""
        payload = {"other_field": "value"}

        ip_weight = weighter.get_ip_weight("/exchange", payload)

        # Missing actions should default to 1 action
        assert ip_weight == 1

    def test_exchange_endpoint_none_payload(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation with None payload."""
        ip_weight = weighter.get_ip_weight("/exchange", None)

        # None payload should default to 1 action
        assert ip_weight == 1

    def test_exchange_endpoint_non_list_actions(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation when actions is not a list."""
        payload = {"actions": "not_a_list"}

        ip_weight = weighter.get_ip_weight("/exchange", payload)

        # Non-list actions should default to 1 action
        assert ip_weight == 1

    def test_info_endpoint_known_type(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation for /info endpoint with known type."""
        payload = {"type": "l2Book", "coin": "BTC"}

        ip_weight = weighter.get_ip_weight("/info", payload)

        assert ip_weight == 2  # From config

    def test_info_endpoint_expensive_type(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation for expensive info type."""
        payload = {"type": "userRole", "user": "0x123"}

        ip_weight = weighter.get_ip_weight("/info", payload)

        assert ip_weight == 60  # From config

    def test_info_endpoint_unknown_type(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation for unknown info type."""
        payload = {"type": "unknownType", "param": "value"}

        ip_weight = weighter.get_ip_weight("/info", payload)

        assert ip_weight == 20  # Default weight

    def test_info_endpoint_missing_type(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation when type is missing."""
        payload = {"coin": "BTC"}

        ip_weight = weighter.get_ip_weight("/info", payload)

        assert ip_weight == 20  # Default weight

    def test_info_endpoint_none_payload(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation for /info with None payload."""
        ip_weight = weighter.get_ip_weight("/info", None)

        assert ip_weight == 20  # Default weight

    def test_unknown_endpoint(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation for unknown endpoint."""
        ip_weight = weighter.get_ip_weight("/unknown", {"param": "value"})

        assert ip_weight == 20  # Default weight

    def test_empty_endpoint(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test IP weight calculation for empty endpoint."""
        ip_weight = weighter.get_ip_weight("", {"param": "value"})

        assert ip_weight == 20  # Default weight


class TestHyperliquidRequestWeighterAddressActionCount:
    """Test address action count calculation logic."""

    @pytest.fixture
    def hl_config(self) -> ExchangeSpecificConfig:
        """Create a mock Hyperliquid configuration.

        Returns:
            Mock ExchangeSpecificConfig for Hyperliquid testing.
        """
        config = Mock(spec=ExchangeSpecificConfig)
        config.info_request_type_ip_weights = {"l2Book": 2}
        config.default_info_weight = 20
        config.exchange_action_base_ip_weight = 1
        config.address_action_safety_net = Mock()
        return config

    @pytest.fixture
    def weighter(self, hl_config: ExchangeSpecificConfig) -> HyperliquidRequestWeighter:
        """Create a HyperliquidRequestWeighter.

        Returns:
            HyperliquidRequestWeighter instance for testing.
        """
        return HyperliquidRequestWeighter(hl_config)

    def test_exchange_endpoint_single_action(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test address action count for single action."""
        payload = {"actions": [{"type": "order"}]}

        action_count = weighter.get_address_action_count("/exchange", payload)

        assert action_count == 1

    def test_exchange_endpoint_multiple_actions(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test address action count for multiple actions."""
        payload = {"actions": [{"type": "order"}] * 5}

        action_count = weighter.get_address_action_count("/exchange", payload)

        assert action_count == 5

    def test_exchange_endpoint_empty_actions(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test address action count for empty actions."""
        payload: dict[str, Any] = {"actions": []}

        action_count = weighter.get_address_action_count("/exchange", payload)

        assert action_count == 1  # Default to 1

    def test_exchange_endpoint_no_actions_key(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test address action count when actions key is missing."""
        payload = {"other_field": "value"}

        action_count = weighter.get_address_action_count("/exchange", payload)

        assert action_count == 1  # Default to 1

    def test_exchange_endpoint_none_payload(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test address action count with None payload."""
        action_count = weighter.get_address_action_count("/exchange", None)

        assert action_count == 1  # Default to 1

    def test_info_endpoint_returns_zero(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test that /info endpoint returns zero address actions."""
        payload = {"type": "l2Book", "coin": "BTC"}

        action_count = weighter.get_address_action_count("/info", payload)

        assert action_count == 0

    def test_unknown_endpoint_returns_zero(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test that unknown endpoints return zero address actions."""
        payload = {"param": "value"}

        action_count = weighter.get_address_action_count("/unknown", payload)

        assert action_count == 0

    def test_empty_endpoint_returns_zero(self, weighter: HyperliquidRequestWeighter) -> None:
        """Test that empty endpoint returns zero address actions."""
        action_count = weighter.get_address_action_count("", {"param": "value"})

        assert action_count == 0


class TestHyperliquidRequestWeighterEdgeCases:
    """Test edge cases and error scenarios."""

    @pytest.fixture
    def minimal_config(self) -> ExchangeSpecificConfig:
        """Create minimal valid config.

        Returns:
            Minimal ExchangeSpecificConfig for testing.
        """
        config = Mock(spec=ExchangeSpecificConfig)
        config.info_request_type_ip_weights = {}
        config.default_info_weight = 1
        config.exchange_action_base_ip_weight = 1
        config.ip_weight_limit_per_minute = 1200
        config.address_action_safety_net = Mock(spec=AddressActionSafetyNetConfig)
        config.address_action_safety_net.rate_per_minute = 300
        return config

    def test_empty_ip_weights_dict(self, minimal_config: ExchangeSpecificConfig) -> None:
        """Test with empty IP weights dictionary."""
        weighter = HyperliquidRequestWeighter(minimal_config)

        ip_weight = weighter.get_ip_weight("/info", {"type": "anyType"})

        assert ip_weight == 1  # Falls back to default

    def test_none_default_weights_uses_fallback(self) -> None:
        """Test behavior when default weights are None."""
        config = Mock(spec=ExchangeSpecificConfig)
        config.info_request_type_ip_weights = {}
        config.default_info_weight = None
        config.exchange_action_base_ip_weight = None
        config.ip_weight_limit_per_minute = 1200
        config.address_action_safety_net = Mock(spec=AddressActionSafetyNetConfig)
        config.address_action_safety_net.rate_per_minute = 300

        # This should raise HyperliquidRateLimitConfigError due to validation
        with pytest.raises(
            HyperliquidRateLimitConfigError,
            match="HyperliquidRateLimitStrategy requires",
        ):
            HyperliquidRequestWeighter(config)

    def test_very_large_action_batch(self, minimal_config: ExchangeSpecificConfig) -> None:
        """Test with very large action batch."""
        weighter = HyperliquidRequestWeighter(minimal_config)

        # 1000 actions
        payload: dict[str, Any] = {"actions": [{}] * 1000}

        ip_weight = weighter.get_ip_weight("/exchange", payload)
        action_count = weighter.get_address_action_count("/exchange", payload)

        # IP weight: 1 + (1000 // 40) = 1 + 25 = 26
        assert ip_weight == 26
        assert action_count == 1000
