"""Comprehensive unit tests for HyperliquidRequestWeighter.

Tests request weight calculation functionality with parametrized tests, smart fixture usage,
and comprehensive success, edge, and failure cases. Only tests through public APIs.
"""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

from cyberdelta.apis.exceptions.configuration import HyperliquidRateLimitConfigError
from cyberdelta.apis.hyperliquid.hl_request_weighter import HyperliquidRequestWeighter
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig


class TestHyperliquidRequestWeighter:
    """Test HyperliquidRequestWeighter public API functionality."""

    @pytest.fixture
    def mock_info_weights(self) -> dict[str, int]:
        """Create mock info request type weights.

        Returns:
            dict[str, int]: Mapping of request types to their IP weights.
        """
        return {
            "metaAndAssetCtxs": 10,
            "allMids": 1,
            "userState": 5,
            "orderStatus": 2,
            "candleSnapshot": 20,
        }

    @pytest.fixture
    def valid_exchange_config(self, mock_info_weights: dict[str, int]) -> ExchangeSpecificConfig:
        """Create valid exchange configuration for testing.

        Returns:
            ExchangeSpecificConfig: Mock exchange configuration with test weights.
        """
        config = MagicMock(spec=ExchangeSpecificConfig)
        config.info_request_type_ip_weights = mock_info_weights
        config.default_info_weight = 20
        config.exchange_action_base_ip_weight = 40
        return config

    @pytest.fixture
    def weighter_factory(
        self, valid_exchange_config: ExchangeSpecificConfig
    ) -> Callable[..., HyperliquidRequestWeighter]:
        """Factory to create weighter instances with optional config overrides.

        Returns:
            Callable[..., HyperliquidRequestWeighter]: Factory function that creates
                weighter instances.
        """

        def _create(
            config_overrides: dict[str, Any] | None = None,
        ) -> HyperliquidRequestWeighter:
            config = valid_exchange_config
            if config_overrides:
                for key, value in config_overrides.items():
                    setattr(config, key, value)
            return HyperliquidRequestWeighter(config)

        return _create

    # Initialization Tests

    def test_weighter_initialization_success(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test successful weighter initialization with valid configuration."""
        weighter = weighter_factory()

        assert weighter is not None
        assert isinstance(weighter, HyperliquidRequestWeighter)

    @pytest.mark.parametrize(
        ("missing_field", "expected_error"),
        [
            ("info_request_type_ip_weights", HyperliquidRateLimitConfigError),
            ("default_info_weight", HyperliquidRateLimitConfigError),
            ("exchange_action_base_ip_weight", HyperliquidRateLimitConfigError),
        ],
    )
    def test_weighter_initialization_missing_required_fields(
        self,
        weighter_factory: Callable[..., HyperliquidRequestWeighter],
        missing_field: str,
        expected_error: type,
    ) -> None:
        """Test weighter initialization fails with missing required configuration."""
        config_overrides = {missing_field: None}

        with pytest.raises(expected_error):
            weighter_factory(config_overrides)

    def test_weighter_initialization_multiple_missing_fields(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test weighter initialization with multiple missing fields."""
        config_overrides = {
            "info_request_type_ip_weights": None,
            "default_info_weight": None,
        }

        with pytest.raises(HyperliquidRateLimitConfigError) as exc_info:
            weighter_factory(config_overrides)

        # Should mention both missing fields
        error = exc_info.value
        assert "info_request_type_ip_weights" in str(error)
        assert "default_info_weight" in str(error)

    # IP Weight Calculation Tests - /info endpoint

    @pytest.mark.parametrize(
        ("request_type", "expected_weight"),
        [
            ("metaAndAssetCtxs", 10),
            ("allMids", 1),
            ("userState", 5),
            ("orderStatus", 2),
            ("candleSnapshot", 20),
        ],
    )
    def test_get_ip_weight_info_endpoint_known_types(
        self,
        weighter_factory: Callable[..., HyperliquidRequestWeighter],
        request_type: str,
        expected_weight: int,
    ) -> None:
        """Test IP weight calculation for /info endpoint with known request types."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"type": request_type}

        result = weighter.get_ip_weight("/info", action_payload)

        assert result == expected_weight

    def test_get_ip_weight_info_endpoint_unknown_type(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for /info endpoint with unknown request type."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"type": "unknownType"}

        result = weighter.get_ip_weight("/info", action_payload)

        # Should use default weight
        assert result == 20

    def test_get_ip_weight_info_endpoint_no_type(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for /info endpoint with no type in payload."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"other_field": "value"}

        result = weighter.get_ip_weight("/info", action_payload)

        # Should use default weight
        assert result == 20

    def test_get_ip_weight_info_endpoint_none_payload(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for /info endpoint with None payload."""
        weighter = weighter_factory()

        result = weighter.get_ip_weight("/info", None)

        # Should use default weight
        assert result == 20

    def test_get_ip_weight_info_endpoint_empty_payload(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for /info endpoint with empty payload."""
        weighter = weighter_factory()

        result = weighter.get_ip_weight("/info", {})

        # Should use default weight
        assert result == 20

    # IP Weight Calculation Tests - /exchange endpoint

    def test_get_ip_weight_exchange_endpoint_single_action(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for /exchange endpoint with single action."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"action": {"type": "order"}, "nonce": 123}

        result = weighter.get_ip_weight("/exchange", action_payload)

        # base_weight (40) + (1 // 40) = 40 + 0 = 40
        assert result == 40

    @pytest.mark.parametrize(
        ("batch_size", "expected_weight"),
        [
            (1, 40),  # 40 + (1 // 40) = 40 + 0 = 40
            (5, 40),  # 40 + (5 // 40) = 40 + 0 = 40
            (39, 40),  # 40 + (39 // 40) = 40 + 0 = 40
            (40, 41),  # 40 + (40 // 40) = 40 + 1 = 41
            (41, 41),  # 40 + (41 // 40) = 40 + 1 = 41
            (80, 42),  # 40 + (80 // 40) = 40 + 2 = 42
            (160, 44),  # 40 + (160 // 40) = 40 + 4 = 44
        ],
    )
    def test_get_ip_weight_exchange_endpoint_batch_actions(
        self,
        weighter_factory: Callable[..., HyperliquidRequestWeighter],
        batch_size: int,
        expected_weight: int,
    ) -> None:
        """Test IP weight calculation for /exchange endpoint with batch actions."""
        weighter = weighter_factory()
        actions = [{"type": "order", "order": {"symbol": f"BTC{i}"}} for i in range(batch_size)]
        action_payload: dict[str, Any] = {"actions": actions}

        result = weighter.get_ip_weight("/exchange", action_payload)

        assert result == expected_weight

    def test_get_ip_weight_exchange_endpoint_empty_actions_list(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for /exchange endpoint with empty actions list."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"actions": []}

        result = weighter.get_ip_weight("/exchange", action_payload)

        # Empty list should default to batch_length = 1
        assert result == 40

    def test_get_ip_weight_exchange_endpoint_non_list_actions(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for /exchange endpoint with non-list actions."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"actions": "not_a_list"}

        result = weighter.get_ip_weight("/exchange", action_payload)

        # Non-list actions should default to batch_length = 1
        assert result == 40

    def test_get_ip_weight_exchange_endpoint_no_actions(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for /exchange endpoint with no actions field."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"other_field": "value"}

        result = weighter.get_ip_weight("/exchange", action_payload)

        # No actions field should default to batch_length = 1
        assert result == 40

    def test_get_ip_weight_exchange_endpoint_none_payload(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for /exchange endpoint with None payload."""
        weighter = weighter_factory()

        result = weighter.get_ip_weight("/exchange", None)

        # None payload should default to batch_length = 1
        assert result == 40

    # IP Weight Calculation Tests - Custom base weight

    def test_get_ip_weight_exchange_custom_base_weight(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation with custom exchange base weight."""
        weighter = weighter_factory({"exchange_action_base_ip_weight": 60})
        action_payload: dict[str, Any] = {"actions": [{"type": "order"} for _ in range(50)]}

        result = weighter.get_ip_weight("/exchange", action_payload)

        # base_weight (60) + (50 // 40) = 60 + 1 = 61
        assert result == 61

    def test_get_ip_weight_exchange_fallback_base_weight(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation when base weight is None (fallback to 1)."""
        # This will fail initialization due to config validation
        with pytest.raises(HyperliquidRateLimitConfigError):
            weighter_factory({"exchange_action_base_ip_weight": None})

    # IP Weight Calculation Tests - Unknown endpoints

    @pytest.mark.parametrize(
        "unknown_endpoint",
        ["/unknown", "/websocket", "/health", "/metrics", ""],
    )
    def test_get_ip_weight_unknown_endpoint(
        self,
        weighter_factory: Callable[..., HyperliquidRequestWeighter],
        unknown_endpoint: str,
    ) -> None:
        """Test IP weight calculation for unknown endpoints."""
        weighter = weighter_factory()

        result = weighter.get_ip_weight(unknown_endpoint, {"test": "payload"})

        # Should use default weight
        assert result == 20

    def test_get_ip_weight_unknown_endpoint_custom_default(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation for unknown endpoints with custom default."""
        weighter = weighter_factory({"default_info_weight": 50})

        result = weighter.get_ip_weight("/unknown", None)

        # Should use custom default
        assert result == 50

    def test_get_ip_weight_unknown_endpoint_fallback_default(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test IP weight calculation when default weight is None (fallback to 20)."""
        # This will fail initialization due to config validation
        with pytest.raises(HyperliquidRateLimitConfigError):
            weighter_factory({"default_info_weight": None})

    # Address Action Count Tests

    @pytest.mark.parametrize(
        ("batch_size", "expected_count"),
        [
            (1, 1),
            (5, 5),
            (10, 10),
            (50, 50),
            (100, 100),
        ],
    )
    def test_get_address_action_count_exchange_endpoint(
        self,
        weighter_factory: Callable[..., HyperliquidRequestWeighter],
        batch_size: int,
        expected_count: int,
    ) -> None:
        """Test address action count for /exchange endpoint with various batch sizes."""
        weighter = weighter_factory()
        actions = [{"type": "order"} for _ in range(batch_size)]
        action_payload: dict[str, Any] = {"actions": actions}

        result = weighter.get_address_action_count("/exchange", action_payload)

        assert result == expected_count

    def test_get_address_action_count_exchange_single_action(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test address action count for /exchange endpoint with single action."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"action": {"type": "order"}}

        result = weighter.get_address_action_count("/exchange", action_payload)

        # Single action should count as 1
        assert result == 1

    def test_get_address_action_count_exchange_empty_actions(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test address action count for /exchange endpoint with empty actions."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"actions": []}

        result = weighter.get_address_action_count("/exchange", action_payload)

        # Empty actions should default to 1
        assert result == 1

    def test_get_address_action_count_exchange_non_list_actions(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test address action count for /exchange endpoint with non-list actions."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"actions": "not_a_list"}

        result = weighter.get_address_action_count("/exchange", action_payload)

        # Non-list actions should default to 1
        assert result == 1

    def test_get_address_action_count_exchange_none_payload(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test address action count for /exchange endpoint with None payload."""
        weighter = weighter_factory()

        result = weighter.get_address_action_count("/exchange", None)

        # None payload should default to 1
        assert result == 1

    @pytest.mark.parametrize(
        "non_exchange_endpoint",
        ["/info", "/websocket", "/health", "/unknown", ""],
    )
    def test_get_address_action_count_non_exchange_endpoints(
        self,
        weighter_factory: Callable[..., HyperliquidRequestWeighter],
        non_exchange_endpoint: str,
    ) -> None:
        """Test address action count for non-/exchange endpoints."""
        weighter = weighter_factory()
        action_payload: dict[str, Any] = {"actions": [{"type": "order"} for _ in range(10)]}

        result = weighter.get_address_action_count(non_exchange_endpoint, action_payload)

        # Non-exchange endpoints should always return 0
        assert result == 0

    # Edge Cases and Integration Tests

    def test_comprehensive_weight_calculation_workflow(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test complete workflow for weight calculation."""
        weighter = weighter_factory()

        # Test /info endpoint
        info_payload = {"type": "metaAndAssetCtxs"}
        ip_weight_info = weighter.get_ip_weight("/info", info_payload)
        address_count_info = weighter.get_address_action_count("/info", info_payload)

        assert ip_weight_info == 10
        assert address_count_info == 0

        # Test /exchange endpoint
        exchange_payload = {"actions": [{"type": "order"} for _ in range(5)]}
        ip_weight_exchange = weighter.get_ip_weight("/exchange", exchange_payload)
        address_count_exchange = weighter.get_address_action_count("/exchange", exchange_payload)

        assert ip_weight_exchange == 40  # 40 + (5 // 40) = 40 + 0 = 40
        assert address_count_exchange == 5

    def test_weighter_instance_independence(
        self, valid_exchange_config: ExchangeSpecificConfig
    ) -> None:
        """Test that multiple weighter instances are independent."""
        weighter1 = HyperliquidRequestWeighter(valid_exchange_config)
        weighter2 = HyperliquidRequestWeighter(valid_exchange_config)

        # Verify instances are different objects
        assert weighter1 is not weighter2
        # Same config object
        assert weighter1.hl_exchange_config is weighter2.hl_exchange_config

    def test_config_parameter_usage(
        self, weighter_factory: Callable[..., HyperliquidRequestWeighter]
    ) -> None:
        """Test that all configuration parameters are properly used."""
        custom_weights = {"customType": 999}
        weighter = weighter_factory({
            "info_request_type_ip_weights": custom_weights,
            "default_info_weight": 123,
            "exchange_action_base_ip_weight": 456,
        })

        # Test custom info weight
        info_result = weighter.get_ip_weight("/info", {"type": "customType"})
        assert info_result == 999

        # Test custom default weight
        default_result = weighter.get_ip_weight("/info", {"type": "unknownType"})
        assert default_result == 123

        # Test custom exchange base weight
        exchange_result = weighter.get_ip_weight("/exchange", {"actions": []})
        # custom base + 0
        assert exchange_result == 456

    @pytest.mark.parametrize(
        ("endpoint", "payload", "expect_ip_weight", "expect_address_count"),
        [
            ("/info", {"type": "allMids"}, True, False),
            ("/exchange", {"actions": [{"type": "order"}]}, True, True),
            ("/unknown", None, True, False),
        ],
    )
    def test_method_combinations(
        self,
        weighter_factory: Callable[..., HyperliquidRequestWeighter],
        endpoint: str,
        payload: dict[str, Any] | None,
        expect_ip_weight: bool,
        expect_address_count: bool,
    ) -> None:
        """Test that both methods work correctly for different endpoint types."""
        weighter = weighter_factory()

        ip_weight = weighter.get_ip_weight(endpoint, payload)
        address_count = weighter.get_address_action_count(endpoint, payload)

        if expect_ip_weight:
            assert ip_weight > 0

        if expect_address_count:
            assert address_count > 0
        else:
            assert address_count == 0
