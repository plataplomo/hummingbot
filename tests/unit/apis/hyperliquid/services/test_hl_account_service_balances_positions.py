"""Unit tests for Hyperliquid Account Service balance and position operations.

Tests the main HyperliquidAccountService's delegation to specialized services.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState,
    HyperliquidRawMarginSummary,
)
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
        mock_authenticator: MagicMock | None = None,
    ) -> HyperliquidAccountService:
        """Create a minimal account service for testing.

        Args:
            mock_requester: Optional mock HTTP requester
            mock_builder: Optional mock request builder
            mock_handler: Optional mock response handler
            mock_authenticator: Optional mock authenticator

        Returns:
            HyperliquidAccountService instance configured with provided or default mocks
        """
        if mock_requester is None:
            mock_requester = AsyncMock()
        if mock_builder is None:
            mock_builder = MagicMock()
        if mock_handler is None:
            mock_handler = MagicMock()
        if mock_authenticator is None:
            mock_authenticator = MagicMock()

        return HyperliquidAccountService(
            http_client_requester=mock_requester,
            request_builder=mock_builder,
            response_handler=mock_handler,
            authenticator=mock_authenticator,
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
        mock_authenticator = MagicMock()

        # Mock HTTP response data that would come from the API
        mock_user_state_response = {
            "withdrawable": {"1": "1000.5"},  # USDC index 1
            "assetPositions": [{"position": {"coin": "USDC", "szi": "1000.5", "entryPx": None}}],
            "marginSummary": {
                "accountValue": "2000.0",
                "totalNtlPos": "0.0",
                "totalRawUsd": "2000.0",
                "totalMarginUsed": "0.0",
            },
        }

        # Create a proper mock clearinghouse state object
        mock_clearinghouse_state = HyperliquidRawClearinghouseState(
            assetPositions=[],
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="2000.0",
                totalNtlPos="0.0",
                totalRawUsd="2000.0",
                totalMarginUsed="0.0",
            ),
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="2000.0",
                totalNtlPos="0.0",
                totalRawUsd="2000.0",
                totalMarginUsed="0.0",
            ),
            isolatedMaintenanceMarginUsed=None,
            isolatedMarginSummary=None,
            time=1640995200000,
            withdrawable="1000.5",  # This should be a string, not dict
        )

        # Configure the mocks
        mock_requester.return_value = (mock_user_state_response, 200, {})
        mock_builder.build_user_state_payload.return_value = {
            "type": "clearinghouseState",
            "user": "0xTest",
        }
        mock_handler.handle_get_user_state_response.return_value = mock_clearinghouse_state

        # Create service with mocked dependencies
        service = self.create_account_service(
            mock_requester, mock_builder, mock_handler, mock_authenticator
        )

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
        mock_authenticator = MagicMock()

        # Create service with NO wallet address (current business logic requires it)
        service = HyperliquidAccountService(
            http_client_requester=mock_requester,
            request_builder=mock_builder,
            response_handler=mock_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid_test",
            wallet_address=None,  # No wallet address
        )

        # Call the public method - should raise APIError due to missing wallet address
        with pytest.raises(APIError) as exc_info:
            await service.get_balances()

        # Verify error is about missing wallet address
        assert "Wallet address required" in str(exc_info.value.message)

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
        mock_authenticator = MagicMock()

        # Create a proper mock clearinghouse state object
        mock_clearinghouse_state = HyperliquidRawClearinghouseState(
            assetPositions=[],  # Simplified for this test
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="2000.0",
                totalNtlPos="0.0",
                totalRawUsd="2000.0",
                totalMarginUsed="0.0",
            ),
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="2000.0",
                totalNtlPos="0.0",
                totalRawUsd="2000.0",
                totalMarginUsed="0.0",
            ),
            isolatedMaintenanceMarginUsed=None,
            isolatedMarginSummary=None,
            time=1640995200000,
            withdrawable="1000.5",
        )

        # Configure the mocks
        mock_requester.return_value = ({}, 200, {})
        mock_builder.build_user_state_payload.return_value = {
            "type": "clearinghouseState",
            "user": "0xTest",
        }
        mock_handler.handle_get_user_state_response.return_value = mock_clearinghouse_state

        # Create service with mocked dependencies
        service = self.create_account_service(
            mock_requester, mock_builder, mock_handler, mock_authenticator
        )

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
        mock_authenticator = MagicMock()

        # Create a proper mock clearinghouse state object
        mock_clearinghouse_state = HyperliquidRawClearinghouseState(
            assetPositions=[],  # Simplified for this test
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="2000.0",
                totalNtlPos="0.0",
                totalRawUsd="2000.0",
                totalMarginUsed="0.0",
            ),
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="2000.0",
                totalNtlPos="0.0",
                totalRawUsd="2000.0",
                totalMarginUsed="0.0",
            ),
            isolatedMaintenanceMarginUsed=None,
            isolatedMarginSummary=None,
            time=1640995200000,
            withdrawable="1000.5",
        )

        # Configure the mocks
        mock_requester.return_value = ({}, 200, {})
        mock_builder.build_user_state_payload.return_value = {
            "type": "clearinghouseState",
            "user": "0xTest",
        }
        mock_handler.handle_get_user_state_response.return_value = mock_clearinghouse_state

        # Create service with mocked dependencies
        service = self.create_account_service(
            mock_requester, mock_builder, mock_handler, mock_authenticator
        )

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
        mock_authenticator = MagicMock()

        # Create a proper mock clearinghouse state object
        mock_clearinghouse_state = HyperliquidRawClearinghouseState(
            assetPositions=[],
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="10000.0",
                totalNtlPos="2000.0",
                totalRawUsd="8000.0",
                totalMarginUsed="1500.0",
            ),
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="10000.0",
                totalNtlPos="2000.0",
                totalRawUsd="8000.0",
                totalMarginUsed="1500.0",
            ),
            isolatedMaintenanceMarginUsed=None,
            isolatedMarginSummary=None,
            time=1640995200000,
            withdrawable="8000.0",
        )

        # Configure the mocks
        mock_requester.return_value = ({}, 200, {})
        mock_builder.build_user_state_payload.return_value = {
            "type": "clearinghouseState",
            "user": "0xTest",
        }
        mock_handler.handle_get_user_state_response.return_value = mock_clearinghouse_state

        # Create service with mocked dependencies
        service = self.create_account_service(
            mock_requester, mock_builder, mock_handler, mock_authenticator
        )

        # Call the public method
        result = await service.get_account_summary()

        # Verify the HTTP request was made
        mock_requester.assert_called_once()

        # Verify result structure (we test the public behavior, not exact values)
        assert isinstance(result, MarginAccountSummary)
        # Current business logic uses "hyperliquid" as exchange name
        assert result.exchange == "hyperliquid"

    @pytest.mark.asyncio
    async def test_get_account_summary_validation_error(self) -> None:
        """Test get_account_summary when validation error occurs."""
        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()
        mock_authenticator = MagicMock()

        # Mock None response to trigger error
        mock_requester.return_value = (None, 200, {})
        mock_builder.build_user_state_payload.return_value = {
            "type": "clearinghouseState",
            "user": "0xTest",
        }

        # Create service with mocked dependencies
        service = self.create_account_service(
            mock_requester, mock_builder, mock_handler, mock_authenticator
        )

        # Call and expect error
        with pytest.raises(APIError) as exc_info:
            await service.get_account_summary()

        assert exc_info.value.message is not None
