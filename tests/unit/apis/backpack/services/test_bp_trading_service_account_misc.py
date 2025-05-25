"""
Unit tests for BackpackTradingService account and miscellaneous functionality.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.services.conftest_trading"]


class TestBackpackTradingServiceAccountMisc:
    """Tests for the BackpackTradingService account and miscellaneous functionality."""

    @pytest.mark.asyncio
    async def test_constructor_with_custom_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_rate_limiter_service: AsyncMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test constructor with custom mapper injection."""
        service = BackpackTradingService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="test_exchange",
            rate_limiter_service=mock_rate_limiter_service,
            mapper=mock_order_mapper,
        )

        # Verify the injected mapper was set correctly
        assert service._trading_mapper == mock_order_mapper

    @pytest.mark.asyncio
    async def test_constructor_with_default_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test constructor creates default mapper when none provided."""
        service = BackpackTradingService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="test_exchange",
            rate_limiter_service=mock_rate_limiter_service,
            mapper=None,
        )

        # Verify a default mapper was created
        assert service._trading_mapper is not None
        assert isinstance(service._trading_mapper, BackpackTradingDataMapper)

    @pytest.mark.asyncio
    async def test_service_properties(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test that the service has the expected properties."""
        # Test that the service has the required attributes
        assert hasattr(bp_trading_service, "_http_client_requester")
        assert hasattr(bp_trading_service, "_request_builder")
        assert hasattr(bp_trading_service, "_response_handler")
        assert hasattr(bp_trading_service, "_authenticator")
        assert hasattr(bp_trading_service, "_exchange_name")
        assert hasattr(bp_trading_service, "_rate_limiter_service")
        assert hasattr(bp_trading_service, "_trading_mapper")

        # Verify exchange name is set correctly
        assert bp_trading_service._exchange_name == "backpack_test_trading"

    @pytest.mark.asyncio
    async def test_service_initialization(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test that the service is properly initialized with dependencies."""
        # Verify all dependencies are properly injected
        assert bp_trading_service._http_client_requester == mock_http_client_requester
        assert bp_trading_service._request_builder == mock_request_builder
        assert bp_trading_service._response_handler == mock_response_handler
        assert bp_trading_service._authenticator == mock_authenticator
        assert bp_trading_service._rate_limiter_service == mock_rate_limiter_service

        # Verify the trading mapper was created/injected
        assert bp_trading_service._trading_mapper is not None

    @pytest.mark.asyncio
    async def test_trading_service_inherits_base_attributes(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test that the trading service properly inherits from base service."""
        # Verify it has the base service attributes
        assert hasattr(bp_trading_service, "_http_client_requester")
        assert hasattr(bp_trading_service, "_request_builder")
        assert hasattr(bp_trading_service, "_response_handler")
        assert hasattr(bp_trading_service, "_exchange_name")
        assert hasattr(bp_trading_service, "_rate_limiter_service")

        # Verify trading-specific attributes
        assert hasattr(bp_trading_service, "_authenticator")
        assert hasattr(bp_trading_service, "_trading_mapper")

    @pytest.mark.asyncio
    async def test_trading_service_type_annotations(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test that the service type annotations are correct."""
        # This test ensures the service is properly typed
        assert isinstance(bp_trading_service, BackpackTradingService)

        # Verify the mapper type
        assert isinstance(bp_trading_service._trading_mapper, BackpackTradingDataMapper)

    @pytest.mark.asyncio
    async def test_service_configuration(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test that the service is configured correctly for trading operations."""
        # Verify exchange name is set properly for trading
        exchange_name = bp_trading_service._exchange_name
        assert exchange_name == "backpack_test_trading"
        assert "trading" in exchange_name

        # Verify the service has authenticator (required for trading operations)
        assert bp_trading_service._authenticator is not None

        # Verify the service has trading mapper (specific to trading operations)
        assert bp_trading_service._trading_mapper is not None
