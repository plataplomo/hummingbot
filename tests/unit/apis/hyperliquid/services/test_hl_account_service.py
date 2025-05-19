"""
Unit tests for the HyperliquidAccountService.
"""
from decimal import Decimal
from typing import Any, Callable, Mapping, Awaitable
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_mapper import (
    HyperliquidMapper,
    HyperliquidOrderMapper,
    HyperliquidUserFillMapper, # For trade history tests if needed
)
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState, HyperliquidRawAssetPosition, HyperliquidRawMarginSummary, HyperliquidRawLeverage, HyperliquidRawPositionInfo
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.financial import SpotBalance, DerivativePosition, MarginAccountSummary

# Type alias for the HTTP client requester callable
HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]

@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    return AsyncMock(spec=HttpClientRequesterSig)

@pytest.fixture
def mock_request_builder() -> MagicMock:
    return MagicMock(spec=HyperliquidRequestBuilder)

@pytest.fixture
def mock_response_handler() -> MagicMock:
    return MagicMock(spec=HyperliquidResponseHandler)

@pytest.fixture
def mock_authenticator() -> MagicMock:
    return MagicMock(spec=IAuthenticator)

@pytest.fixture
def mock_hl_mapper() -> MagicMock: # For general user state to balance/summary
    return MagicMock(spec=HyperliquidMapper)

@pytest.fixture
def mock_hl_order_mapper() -> MagicMock: # For order/fill related mappings
    return MagicMock(spec=HyperliquidOrderMapper)

@pytest.fixture
def mock_hl_user_fill_mapper() -> MagicMock:
    return MagicMock(spec=HyperliquidUserFillMapper)

@pytest.fixture
def hyperliquid_account_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_authenticator: MagicMock,
    mock_hl_mapper: MagicMock,
    mock_hl_order_mapper: MagicMock,
) -> HyperliquidAccountService:
    service = HyperliquidAccountService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name="hyperliquid_test_account",
        info_url="https://info.hyperliquid.xyz",
        wallet_address="0xTestWalletAddress",
    )
    # Replace internally created mappers with mocks
    service._mapper = mock_hl_mapper # type: ignore[protected-access]
    service._order_mapper = mock_hl_order_mapper # type: ignore[protected-access]
    return service

class TestHyperliquidAccountService:
    """Tests for the HyperliquidAccountService class."""

    @pytest.mark.asyncio
    async def test_get_balances_success(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_hl_mapper: MagicMock, # Use the HyperliquidMapper mock
    ) -> None:
        """Test get_balances successfully retrieves and processes balance data."""
        wallet_address = "0xTestWalletAddress"
        mock_endpoint_path = "/info"
        
        # 1. Mock RequestBuilder call for user_state_payload
        mock_user_state_payload_model = MagicMock()
        mock_user_state_payload_dict = {"type": "clearinghouseState", "user": wallet_address}
        mock_user_state_payload_model.model_dump.return_value = mock_user_state_payload_dict
        mock_request_builder.build_user_state_payload.return_value = mock_user_state_payload_model

        # 2. Mock HttpClient Requester call for user state
        # Hyperliquid /info for user state returns a list with one dict element usually
        mock_raw_user_state_response_list: ParsedJsonResponse = [
            {
                "assetPositions": [
                    {"type": "erc20", "asset": "USDC", "position": {"type": "cross", "amount": "1000500000"}} # 1000.5 USDC (6 decimals)
                ],
                "marginSummary": {"accountValue": "1000500000"}, 
                # ... other fields in clearinghouse state
            }
        ]
        mock_status_code = 200
        mock_headers: dict[str, str] = {}
        mock_http_client_requester.return_value = (
            mock_raw_user_state_response_list,
            mock_status_code,
            mock_headers,
        )

        # 3. Mock ResponseHandler call for user state
        # Response handler expects the inner dict, service extracts raw_data[0]
        mock_raw_leverage = HyperliquidRawLeverage(type="cross", value=10)
        mock_raw_position_info = HyperliquidRawPositionInfo(
            coin="USDC",
            entryPx="0",
            leverage=mock_raw_leverage,
            liquidationPx="0",
            marginUsed="0",
            maxLeverage=20,
            positionValue="1000.5",
            returnOnEquity="0",
            szi="1000.5",
            unrealizedPnl="0"
        )
        mock_raw_asset_position_usdc = HyperliquidRawAssetPosition(
            asset="USDC", 
            position=mock_raw_position_info
        )

        mock_raw_margin_summary = HyperliquidRawMarginSummary(
            accountValue="1000.5", # Use alias
            totalMarginUsed="0", # Use alias
            totalNtlPos="1000.5", # Use alias
            totalRawUsd="1000.5" # Use alias
        )

        mock_raw_clearinghouse_state_model = HyperliquidRawClearinghouseState(
            assetPositions=[mock_raw_asset_position_usdc], # Use alias
            marginSummary=mock_raw_margin_summary, # Use alias
            crossMaintenanceMarginUsed="0", # Use alias
            crossMarginSummary=mock_raw_margin_summary, # Use alias
            isolatedMaintenanceMarginUsed="0", # Use alias
            isolatedMarginSummary=mock_raw_margin_summary, # Use alias
            withdrawable="1000.5"
        )
        mock_response_handler.handle_info_user_state_response.return_value = mock_raw_clearinghouse_state_model

        # 4. Mock Mapper call (HyperliquidMapper)
        expected_internal_balances: dict[str, SpotBalance] = {
            "USDC": SpotBalance(asset_symbol="USDC", total_balance=Decimal("1000.5"), available_balance=Decimal("1000.5"))
        }
        mock_hl_mapper.map_raw_clearinghouse_state_to_spot_balances.return_value = expected_internal_balances

        # Call the service method
        result_balances = await hyperliquid_account_service.get_balances()

        # Assertions
        mock_request_builder.build_user_state_payload.assert_called_once_with(wallet_address)
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path=mock_endpoint_path,
            data=mock_user_state_payload_dict,
            is_info_endpoint=True,
            is_signed=False,
        )
        mock_response_handler.handle_info_user_state_response.assert_called_once_with(
            raw_response_content=mock_raw_user_state_response_list[0], # Service passes the dict
            user_address=wallet_address
        )
        mock_hl_mapper.map_raw_clearinghouse_state_to_spot_balances.assert_called_once_with(mock_raw_clearinghouse_state_model)
        assert result_balances == expected_internal_balances

    @pytest.mark.asyncio
    async def test_get_balances_no_wallet_address(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
    ) -> None:
        """Test get_balances raises APIError if wallet_address is not set in service."""
        hyperliquid_account_service._wallet_address = None # type: ignore[protected-access]
        with pytest.raises(APIError) as excinfo:
            await hyperliquid_account_service.get_balances()
        assert excinfo.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Wallet address is required" in excinfo.value.message

    # TODO: Add more tests for other methods:
    # - get_positions (success, errors, symbol filter)
    # - get_account_summary (success, errors)
    # - get_order_history (success, errors, symbol filter)
    # - get_trade_history (success, errors, symbol filter)
    # - Test error conditions from requester, response_handler, and mappers for each method.
    # - Test case where _get_raw_clearinghouse_state returns APIError. 