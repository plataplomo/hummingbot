"""Test suite for Backpack component registry functionality.

Tests the registry pattern implementation including component registration,
retrieval, replacement, and factory integration.
"""

from unittest.mock import Mock, patch

import pytest
from pydantic import AnyUrl, HttpUrl, SecretStr

from cyberdelta.apis.backpack.bp_api_components_factory import BackpackAPIComponentsFactory
from cyberdelta.apis.backpack.mappers import BackpackBalanceMapper
from cyberdelta.apis.backpack.utils.component_registry import (
    BackpackComponentRegistry,
    BackpackMapperRegistry,
    BackpackRequestBuilderRegistry,
    BackpackResponseHandlerRegistry,
    IMapper,
    replace_mapper,
)
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.enums.exchange_names import ExchangeName


class TestBackpackMapperRegistry:
    """Test the mapper registry functionality."""

    def test_register_and_retrieve_mapper(self) -> None:
        """Test basic mapper registration and retrieval."""
        registry = BackpackMapperRegistry()
        mock_mapper = Mock(spec=IMapper)

        # Register mapper
        registry.register("test.mapper", mock_mapper)

        # Retrieve mapper
        retrieved = registry.get("test.mapper")
        assert retrieved is mock_mapper

    def test_register_duplicate_raises_error(self) -> None:
        """Test that registering duplicate name raises ValueError."""
        registry = BackpackMapperRegistry()
        mock_mapper = Mock(spec=IMapper)

        # Register once
        registry.register("test.mapper", mock_mapper)

        # Try to register again
        with pytest.raises(ValueError) as excinfo:
            registry.register("test.mapper", mock_mapper)
        assert "already registered" in str(excinfo.value)

    def test_get_nonexistent_raises_error(self) -> None:
        """Test that getting non-existent mapper raises KeyError."""
        registry = BackpackMapperRegistry()

        with pytest.raises(KeyError) as excinfo:
            registry.get("nonexistent.mapper")
        assert "not found" in str(excinfo.value)

    def test_is_registered(self) -> None:
        """Test checking if mapper is registered."""
        registry = BackpackMapperRegistry()
        mock_mapper = Mock(spec=IMapper)

        # Before registration
        assert not registry.is_registered("test.mapper")

        # After registration
        registry.register("test.mapper", mock_mapper)
        assert registry.is_registered("test.mapper")

    def test_list_registered(self) -> None:
        """Test listing all registered mappers."""
        registry = BackpackMapperRegistry()

        # Empty registry
        assert registry.list_registered() == []

        # Add some mappers
        registry.register("mapper1", Mock(spec=IMapper))
        registry.register("mapper2", Mock(spec=IMapper))

        registered = registry.list_registered()
        assert len(registered) == 2
        assert "mapper1" in registered
        assert "mapper2" in registered

    def test_unregister(self) -> None:
        """Test unregistering a mapper."""
        registry = BackpackMapperRegistry()
        mock_mapper = Mock(spec=IMapper)

        # Register and then unregister
        registry.register("test.mapper", mock_mapper)
        unregistered = registry.unregister("test.mapper")

        assert unregistered is mock_mapper
        assert not registry.is_registered("test.mapper")

    def test_register_default_mappers(self) -> None:
        """Test registering all default mappers."""
        registry = BackpackMapperRegistry()
        registry.register_default_mappers()

        # Check some key mappers are registered
        assert registry.is_registered("account.balance")
        assert registry.is_registered("account.position")
        assert registry.is_registered("trading.order")
        assert registry.is_registered("market.ticker")

        # Check total count (5 account + 1 trading + 6 market data + 1 utility = 13)
        registered = registry.list_registered()
        assert len(registered) == 13


class TestBackpackComponentRegistry:
    """Test the main component registry."""

    def test_initialization_with_defaults(self) -> None:
        """Test registry initialization with default components."""
        registry = BackpackComponentRegistry(auto_register=True)

        # Check sub-registries exist
        assert isinstance(registry.mappers, BackpackMapperRegistry)
        assert isinstance(registry.request_builders, BackpackRequestBuilderRegistry)
        assert isinstance(registry.response_handlers, BackpackResponseHandlerRegistry)

        # Check defaults are registered
        assert registry.mappers.is_registered("account.balance")
        assert registry.request_builders.is_registered("account")
        assert registry.response_handlers.is_registered("trading")

    def test_initialization_without_defaults(self) -> None:
        """Test registry initialization without auto-registration."""
        registry = BackpackComponentRegistry(auto_register=False)

        # Sub-registries should exist but be empty
        assert len(registry.mappers.list_registered()) == 0
        assert len(registry.request_builders.list_registered()) == 0
        assert len(registry.response_handlers.list_registered()) == 0

    def test_validate_required_components(self) -> None:
        """Test validation of required components."""
        # Registry with defaults should pass validation
        registry = BackpackComponentRegistry(auto_register=True)
        assert registry.validate_required_components()

        # Empty registry should fail validation
        empty_registry = BackpackComponentRegistry(auto_register=False)
        assert not empty_registry.validate_required_components()

    def test_get_component_stats(self) -> None:
        """Test getting component statistics."""
        registry = BackpackComponentRegistry(auto_register=True)
        stats = registry.get_component_stats()

        assert "mappers" in stats
        assert "request_builders" in stats
        assert "response_handlers" in stats
        assert "total_components" in stats

        # Check counts
        assert stats["mappers"]["count"] == 13
        assert stats["request_builders"]["count"] >= 3
        assert stats["response_handlers"]["count"] >= 3
        assert stats["total_components"] >= 19


class TestReplaceMapper:
    """Test the replace_mapper utility function."""

    def test_replace_existing_mapper(self) -> None:
        """Test replacing an existing mapper."""
        registry = BackpackComponentRegistry(auto_register=True)
        custom_mapper = Mock(spec=BackpackBalanceMapper)

        # Replace existing mapper
        replace_mapper(registry, "account.balance", custom_mapper)

        # Verify replacement
        retrieved = registry.mappers.get("account.balance")
        assert retrieved is custom_mapper

    def test_replace_nonexistent_mapper(self) -> None:
        """Test replacing a non-existent mapper (should just register)."""
        registry = BackpackComponentRegistry(auto_register=False)
        custom_mapper = Mock(spec=IMapper)

        # Replace non-existent mapper
        replace_mapper(registry, "custom.mapper", custom_mapper)

        # Verify registration
        assert registry.mappers.is_registered("custom.mapper")
        assert registry.mappers.get("custom.mapper") is custom_mapper


class TestFactoryRegistryIntegration:
    """Test integration between factory and registry."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.exchange_config = ExchangeSpecificConfig(
            api_base_url_mainnet=HttpUrl("https://api.backpack.exchange"),
            ws_url_mainnet=AnyUrl("wss://ws.backpack.exchange"),
            symbols={"BTC_USDC": "BTC_USDC", "ETH_USDC": "ETH_USDC"},
            exchange_name=ExchangeName.BACKPACK,
            rate_limit_per_minute=1200,
        )

        self.exchange_secrets = ApiKeyAuthSecrets(
            auth_type="api_key",
            api_key=SecretStr("test_public_key"),
            api_secret=SecretStr("test_private_key"),
        )

    def test_factory_uses_default_registry(self) -> None:
        """Test factory creates and uses default registry."""
        factory = BackpackAPIComponentsFactory(
            exchange_config=self.exchange_config,
            exchange_secrets=self.exchange_secrets,
        )

        # Factory should have a registry
        registry = factory.get_component_registry()
        assert isinstance(registry, BackpackComponentRegistry)

        # Registry should have defaults
        assert registry.validate_required_components()

    def test_factory_uses_custom_registry(self) -> None:
        """Test factory uses provided custom registry."""
        # Create custom registry with a custom mapper
        custom_registry = BackpackComponentRegistry(auto_register=True)
        custom_mapper = Mock(spec=BackpackBalanceMapper)
        replace_mapper(custom_registry, "account.balance", custom_mapper)

        # Create factory with custom registry
        factory = BackpackAPIComponentsFactory(
            exchange_config=self.exchange_config,
            exchange_secrets=self.exchange_secrets,
            component_registry=custom_registry,
        )

        # Factory should use the custom registry
        assert factory.get_component_registry() is custom_registry

        # Shared components should use custom mapper
        balance_mapper = factory.get_shared_component("balance_mapper")
        assert balance_mapper is custom_mapper

    @patch("cyberdelta.apis.backpack.bp_api_components_factory.BackpackBalanceMapper")
    def test_factory_fallback_when_not_in_registry(self, mock_balance_class: Mock) -> None:
        """Test factory falls back to direct instantiation when component not in registry."""
        # Create empty registry
        empty_registry = BackpackComponentRegistry(auto_register=False)

        # Create factory with empty registry
        factory = BackpackAPIComponentsFactory(
            exchange_config=self.exchange_config,
            exchange_secrets=self.exchange_secrets,
            component_registry=empty_registry,
        )

        # Should fall back to creating new instance
        balance_mapper = factory.get_shared_component("balance_mapper")
        assert balance_mapper is not None
        mock_balance_class.assert_called_once()

    def test_service_creation_with_registry_mappers(self) -> None:
        """Test service creation uses mappers from registry."""
        # Create registry with custom mapper
        custom_registry = BackpackComponentRegistry(auto_register=True)
        custom_balance_mapper = Mock(spec=BackpackBalanceMapper)
        replace_mapper(custom_registry, "account.balance", custom_balance_mapper)

        # Create factory
        factory = BackpackAPIComponentsFactory(
            exchange_config=self.exchange_config,
            exchange_secrets=self.exchange_secrets,
            component_registry=custom_registry,
        )

        # Create account service
        http_client = Mock()
        authenticator = Mock()

        account_service = factory.create_account_service(
            http_client_requester=http_client,
            authenticator=authenticator,
            exchange_name="backpack",
        )

        # Service should exist (actual mapper injection verified in service tests)
        assert account_service is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
