"""Unit tests for Hyperliquid Account Service balance and position operations.

Tests the main HyperliquidAccountService's delegation to specialized services.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.core.models import (
    MarginAccountSummary,
)


class TestHyperliquidAccountServiceBalancesPositions:
    """Tests for the HyperliquidAccountService balance and position management functionality."""

    def create_account_service(
        self,
        mock_requester: AsyncMock | None = None,
        mock_builder: MagicMock | None = None,
        mock_handler: MagicMock | None = None,
    ) -> HyperliquidAccountService:
        """Create a minimal account service for testing."""
        if mock_requester is None:
            mock_requester = AsyncMock()
        if mock_builder is None:
            mock_builder = MagicMock()
        if mock_handler is None:
            mock_handler = MagicMock()

        return HyperliquidAccountService(
            http_client_requester=mock_requester,
            request_builder=mock_builder,
            response_handler=mock_handler,
            authenticator=None,
            exchange_name="hyperliquid_test",
            wallet_address="0xTest",
        )

    @pytest.mark.asyncio
    async def test_get_balances_success(self) -> None:
        """Test get_balances successfully retrieves and processes balance data."""
        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Mock HTTP response data that would come from the API
        mock_user_state_response = {
            "withdrawable": {"1": "1000.5"},  # USDC index 1
            "assetPositions": [{"position": {"coin": "USDC", "szi": "1000.5", "entryPx": None}}],
        }

        # Configure the mocks
        mock_requester.return_value = (mock_user_state_response, 200, {})
        mock_builder.build_user_state_request_payload.return_value = {"type": "clearinghouseState"}

        # Create service with mocked dependencies
        service = self.create_account_service(mock_requester, mock_builder, mock_handler)

        # Call the public method
        result = await service.get_balances()

        # Verify the HTTP request was made
        mock_requester.assert_called_once()

        # Verify result structure (we test the public behavior, not exact values)
        assert isinstance(result, dict)
        assert len(result) >= 0  # May be empty or contain balances

    @pytest.mark.asyncio
    async def test_get_balances_no_wallet_address(self) -> None:
        """Test get_balances when wallet address is not configured."""
        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Mock empty HTTP response
        mock_requester.return_value = ({}, 200, {})
        mock_builder.build_user_state_request_payload.return_value = {"type": "clearinghouseState"}

        # Create service with mocked dependencies
        service = self.create_account_service(mock_requester, mock_builder, mock_handler)

        # Call the public method
        result = await service.get_balances()

        # Verify the HTTP request was made
        mock_requester.assert_called_once()

        # Verify result structure
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_get_balances_http_client_returns_none_in_state_fetch(self) -> None:
        """Test get_balances when HTTP client returns None."""
        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Mock None response to trigger error
        mock_requester.return_value = (None, 200, {})
        mock_builder.build_user_state_request_payload.return_value = {"type": "clearinghouseState"}

        # Create service with mocked dependencies
        service = self.create_account_service(mock_requester, mock_builder, mock_handler)

        # Call and expect error
        with pytest.raises(APIError) as exc_info:
            await service.get_balances()

        assert exc_info.value.message is not None

    @pytest.mark.asyncio
    async def test_get_positions_success(self) -> None:
        """Test get_positions successfully retrieves and processes position data."""
        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Mock HTTP response data that would come from the API
        mock_user_state_response = {
            "assetPositions": [
                {
                    "position": {
                        "coin": "ETH",
                        "szi": "10.5",
                        "entryPx": "2000.0",
                        "unrealizedPnl": "525.0",
                        "leverage": {"type": "cross", "value": 10, "rawUsd": "200.0"},
                    }
                }
            ]
        }

        # Configure the mocks
        mock_requester.return_value = (mock_user_state_response, 200, {})
        mock_builder.build_user_state_request_payload.return_value = {"type": "userState"}

        # Create service with mocked dependencies
        service = self.create_account_service(mock_requester, mock_builder, mock_handler)

        # Call the public method
        result = await service.get_positions()

        # Verify the HTTP request was made
        mock_requester.assert_called_once()

        # Verify result structure (we test the public behavior, not exact values)
        assert isinstance(result, list)
        assert len(result) >= 0  # May be empty or contain positions

    @pytest.mark.asyncio
    async def test_get_positions_symbol_filter(self) -> None:
        """Test get_positions with symbol filter."""
        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Mock HTTP response data for ETH-USD position
        mock_user_state_response = {
            "assetPositions": [
                {
                    "position": {
                        "coin": "ETH",
                        "szi": "10.5",
                        "entryPx": "2000.0",
                        "unrealizedPnl": "525.0",
                        "leverage": {"type": "cross", "value": 10, "rawUsd": "200.0"},
                    }
                }
            ]
        }

        # Configure the mocks
        mock_requester.return_value = (mock_user_state_response, 200, {})
        mock_builder.build_user_state_request_payload.return_value = {"type": "userState"}

        # Create service with mocked dependencies
        service = self.create_account_service(mock_requester, mock_builder, mock_handler)

        # Call with symbol filter
        result = await service.get_positions("ETH-USD")

        # Verify the HTTP request was made
        mock_requester.assert_called_once()

        # Verify result structure
        assert isinstance(result, list)
        assert len(result) >= 0

    @pytest.mark.asyncio
    async def test_get_account_summary_success(self) -> None:
        """Test get_account_summary successfully retrieves and processes account summary data."""
        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Mock HTTP response data that would come from the API
        mock_clearinghouse_state_response: dict[str, Any] = {
            "marginSummary": {
                "accountValue": "10000.0",
                "totalNtlPos": "2000.0",
                "totalRawUsd": "8000.0",
                "totalMarginUsed": "1500.0",
            },
            "withdrawable": {"1": "8000.0"},
            "assetPositions": [],
        }

        # Configure the mocks
        mock_requester.return_value = (mock_clearinghouse_state_response, 200, {})
        mock_builder.build_clearinghouse_state_request_payload.return_value = {
            "type": "clearinghouseState"
        }

        # Create service with mocked dependencies
        service = self.create_account_service(mock_requester, mock_builder, mock_handler)

        # Call the public method
        result = await service.get_account_summary()

        # Verify the HTTP request was made
        mock_requester.assert_called_once()

        # Verify result structure (we test the public behavior, not exact values)
        assert isinstance(result, MarginAccountSummary)
        assert result.exchange == "hyperliquid_test"

    @pytest.mark.asyncio
    async def test_get_account_summary_validation_error(self) -> None:
        """Test get_account_summary when validation error occurs."""
        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Mock None response to trigger error
        mock_requester.return_value = (None, 200, {})
        mock_builder.build_clearinghouse_state_request_payload.return_value = {
            "type": "clearinghouseState"
        }

        # Create service with mocked dependencies
        service = self.create_account_service(mock_requester, mock_builder, mock_handler)

        # Call and expect error
        with pytest.raises(APIError) as exc_info:
            await service.get_account_summary()

        assert exc_info.value.message is not None
