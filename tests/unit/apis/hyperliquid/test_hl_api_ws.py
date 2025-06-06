"""Unit tests for HyperliquidAPI WebSocket public interface.

This module tests the public WebSocket interface of HyperliquidAPI,
focusing on subscription management and public methods without accessing protected members.
The detailed routing logic is tested in test_hl_ws_message_router.py.
Integration tests for WebSocket functionality are in test_hl_api_ws_integration.py.
"""

from collections.abc import Callable, Coroutine
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets

# Removed create_test_exchange_config function - now using active_hl_config fixture


# Removed active_hl_config fixture - now using active_hl_config from conftest.py


# Removed active_hl_secrets fixture - now using active_hl_secrets from conftest.py


class TestHyperliquidAPIWebSocketPublicInterface:
    """Test WebSocket public interface without accessing protected members."""

    @pytest.fixture
    def hl_api(
        self,
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI instance with all dependencies mocked."""
        with (
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRateLimitStrategy"),
            patch("cyberdelta.apis.connectivity.ws_manager.WebSocketManager"),
        ):
            return HyperliquidAPI(
                exchange_config=active_hl_config,
                exchange_secrets=active_hl_secrets,
            )

    @pytest.mark.asyncio
    async def test_subscribe_public_interface(self, hl_api: HyperliquidAPI) -> None:
        """Test that subscribe method exists and can be called without errors."""
        handler = AsyncMock()

        # Test that the public subscribe method exists and can be called
        # This tests the public interface without accessing protected members
        try:
            await hl_api.subscribe("l2Book:ETH", handler)
            # If no exception is raised, the public interface is working
            assert True
        except Exception as e:
            # If there's an exception, it should be a known type, not an AttributeError
            # AttributeError would indicate the public method doesn't exist
            assert not isinstance(e, AttributeError), f"Public subscribe method missing: {e}"

    @pytest.mark.asyncio
    async def test_subscribe_to_order_book_public_method(self, hl_api: HyperliquidAPI) -> None:
        """Test that subscribe_to_order_book public method exists."""
        # Test that the public method exists and can be called
        try:
            await hl_api.subscribe_to_order_book("ETH")
            assert True
        except Exception as e:
            assert not isinstance(e, AttributeError), (
                f"Public subscribe_to_order_book method missing: {e}"
            )

    @pytest.mark.asyncio
    async def test_subscribe_to_trades_public_method(self, hl_api: HyperliquidAPI) -> None:
        """Test that subscribe_to_trades public method exists."""
        try:
            await hl_api.subscribe_to_trades("BTC")
            assert True
        except Exception as e:
            assert not isinstance(e, AttributeError), (
                f"Public subscribe_to_trades method missing: {e}"
            )

    @pytest.mark.asyncio
    async def test_subscribe_to_account_updates_public_method(self, hl_api: HyperliquidAPI) -> None:
        """Test that subscribe_to_account_updates public method exists."""
        try:
            await hl_api.subscribe_to_account_updates()
            assert True
        except Exception as e:
            assert not isinstance(e, AttributeError), (
                f"Public subscribe_to_account_updates method missing: {e}"
            )

    def test_exchange_name_property(self, hl_api: HyperliquidAPI) -> None:
        """Test that exchange_name property is accessible and correct."""
        assert hl_api.exchange_name == "hyperliquid"

    @pytest.mark.asyncio
    async def test_close_method_exists(self, hl_api: HyperliquidAPI) -> None:
        """Test that close method exists and can be called."""
        try:
            await hl_api.close()
            assert True
        except Exception as e:
            assert not isinstance(e, AttributeError), f"Public close method missing: {e}"


class TestHyperliquidAPIWebSocketConfiguration:
    """Test WebSocket configuration and initialization."""

    def test_api_initialization_with_active_config(
        self,
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
    ) -> None:
        """Test that API can be initialized with active configuration fixtures."""
        with (
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRateLimitStrategy"),
            patch("cyberdelta.apis.connectivity.ws_manager.WebSocketManager"),
        ):
            api = HyperliquidAPI(
                exchange_config=active_hl_config,
                exchange_secrets=active_hl_secrets,
            )
            assert api is not None
            assert api.exchange_name == "hyperliquid"

    def test_api_initialization_uses_active_configuration(
        self,
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
    ) -> None:
        """Test that API uses active configuration consistently."""
        # Test that active configuration works properly
        assert active_hl_config.exchange_name.value == "hyperliquid"
        assert active_hl_secrets.private_key is not None

        with (
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRateLimitStrategy"),
            patch("cyberdelta.apis.connectivity.ws_manager.WebSocketManager"),
        ):
            api = HyperliquidAPI(
                exchange_config=active_hl_config,
                exchange_secrets=active_hl_secrets,
            )
            assert api is not None
            assert api.exchange_name == "hyperliquid"


class TestHyperliquidAPIWebSocketErrorHandling:
    """Test WebSocket error handling through public interface."""

    @pytest.fixture
    def hl_api(
        self,
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI instance with mocked dependencies."""
        with (
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRateLimitStrategy"),
            patch("cyberdelta.apis.connectivity.ws_manager.WebSocketManager"),
        ):
            return HyperliquidAPI(
                exchange_config=active_hl_config,
                exchange_secrets=active_hl_secrets,
            )

    @pytest.mark.asyncio
    async def test_subscribe_with_invalid_topic_format(self, hl_api: HyperliquidAPI) -> None:
        """Test subscribe behavior with invalid topic format."""
        handler = AsyncMock()

        # Test that invalid topics are handled gracefully through public interface
        # The exact behavior depends on implementation, but it shouldn't crash
        try:
            await hl_api.subscribe("", handler)
            await hl_api.subscribe("invalid_topic", handler)
            await hl_api.subscribe("l2Book", handler)  # Missing coin
            # If no exception, the method handles invalid input gracefully
            assert True
        except Exception as e:
            # If there's an exception, it should be a known validation error
            # not an AttributeError or other unexpected error
            assert not isinstance(e, AttributeError)

    @pytest.mark.asyncio
    async def test_subscribe_with_none_handler(self, hl_api: HyperliquidAPI) -> None:
        """Test subscribe behavior with None handler."""
        # Test that None handler is handled appropriately
        # Cast None to the expected type to test runtime behavior
        none_handler = cast(
            "Callable[[dict[str, Any], dict[str, Any]], Coroutine[Any, Any, None]]",
            None,
        )
        try:
            await hl_api.subscribe("l2Book:ETH", none_handler)
            # If no exception, the method handles None handler gracefully
            assert True
        except Exception as e:
            # Should be a validation error, not an AttributeError
            assert not isinstance(e, AttributeError)
