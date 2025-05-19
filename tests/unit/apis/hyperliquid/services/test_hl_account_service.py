"""
Unit tests for the HyperliquidAccountService.
"""

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_mapper import (
    HyperliquidMapper,
    HyperliquidOrderMapper,
    HyperliquidUserFillMapper,  # For trade history tests if needed
)
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import SpotBalance

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
def mock_hl_mapper() -> MagicMock:  # For general user state to balance/summary
    return MagicMock(spec=HyperliquidMapper)


@pytest.fixture
def mock_hl_order_mapper() -> MagicMock:  # For order/fill related mappings
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
    mock_hl_user_fill_mapper: MagicMock,
) -> HyperliquidAccountService:
    service = HyperliquidAccountService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name="hyperliquid_test_account",
        info_url="https://info.hyperliquid.xyz",
        wallet_address="0xTestWalletAddress",
        mapper=mock_hl_mapper,
        order_mapper=mock_hl_order_mapper,
        user_fill_mapper=mock_hl_user_fill_mapper,
    )
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
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_balances successfully retrieves and processes balance data."""
        mock_endpoint_path = "/info"

        # 1. Mock RequestBuilder for build_user_state_payload
        #    (called by _get_raw_clearinghouse_state)
        mock_user_state_payload_model = MagicMock()
        mock_user_state_payload_dict = {"type": "clearinghouseState", "user": "0xTestWalletAddress"}
        mock_user_state_payload_model.model_dump.return_value = mock_user_state_payload_dict
        mock_request_builder.build_user_state_payload.return_value = mock_user_state_payload_model

        # 2. Mock HttpClient Requester (called by _get_raw_clearinghouse_state)
        mock_raw_user_state_response_list: list[dict[str, Any]] = [
            {
                "assetPositions": [
                    {
                        "type": "erc20",
                        "asset": "USDC",
                        "position": {"type": "cross", "amount": "1000500000"},
                    }
                ],
                "marginSummary": {"accountValue": "1000500000"},
            }
        ]
        mock_status_code = 200
        mock_headers: dict[str, str] = {}
        mock_http_client_requester.return_value = (
            mock_raw_user_state_response_list,
            mock_status_code,
            mock_headers,
        )

        # 3. Mock ResponseHandler for handle_info_user_state_response
        #    (called by _get_raw_clearinghouse_state)
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
            unrealizedPnl="0",
        )
        mock_raw_asset_position_usdc = HyperliquidRawAssetPosition(
            asset="USDC", position=mock_raw_position_info
        )
        mock_raw_margin_summary = HyperliquidRawMarginSummary(
            accountValue="1000.5",
            totalMarginUsed="0",
            totalNtlPos="1000.5",
            totalRawUsd="1000.5",
        )
        mock_processed_raw_clearinghouse_state_model = HyperliquidRawClearinghouseState(
            assetPositions=[mock_raw_asset_position_usdc],
            marginSummary=mock_raw_margin_summary,
            crossMaintenanceMarginUsed="0",
            crossMarginSummary=mock_raw_margin_summary,
            isolatedMaintenanceMarginUsed="0",
            isolatedMarginSummary=mock_raw_margin_summary,
            withdrawable="1000.5",
        )
        mock_response_handler.handle_info_user_state_response.return_value = (
            mock_processed_raw_clearinghouse_state_model
        )

        # 4. Mock Mapper (HyperliquidMapper) for map_raw_clearinghouse_state_to_spot_balances
        mock_ts = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        expected_internal_balances: dict[str, SpotBalance] = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=mock_ts,
                total_quantity=Decimal("1000.5"),
                available_quantity=Decimal("1000.5"),
            )
        }
        mock_hl_mapper.map_raw_clearinghouse_state_to_spot_balances.return_value = (
            expected_internal_balances
        )

        # Call the service method - this will now execute the actual _get_raw_clearinghouse_state
        result_balances = await hyperliquid_account_service.get_balances()

        # Assertions
        mock_request_builder.build_user_state_payload.assert_called_once_with("0xTestWalletAddress")
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path=mock_endpoint_path,
            data=mock_user_state_payload_dict,
            is_info_endpoint=True,
            is_signed=False,
        )
        mock_response_handler.handle_info_user_state_response.assert_called_once_with(
            raw_response_content=mock_raw_user_state_response_list[0],
            user_address="0xTestWalletAddress",
        )
        mock_hl_mapper.map_raw_clearinghouse_state_to_spot_balances.assert_called_once_with(
            mock_processed_raw_clearinghouse_state_model
        )
        assert result_balances == expected_internal_balances

    @pytest.mark.asyncio
    async def test_get_balances_no_wallet_address(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_hl_mapper: MagicMock,
        mock_hl_order_mapper: MagicMock,
        mock_hl_user_fill_mapper: MagicMock,
    ) -> None:
        """Test get_balances raises APIError if wallet_address is not set in service."""
        # Instantiate service directly with wallet_address=None
        service_no_wallet = HyperliquidAccountService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid_test_no_wallet",
            info_url="https://info.hyperliquid.xyz",
            wallet_address=None,  # Key change here
            mapper=mock_hl_mapper,
            order_mapper=mock_hl_order_mapper,
            user_fill_mapper=mock_hl_user_fill_mapper,
        )

        with pytest.raises(APIError) as excinfo:
            await service_no_wallet.get_balances()
        assert excinfo.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Wallet address is required" in excinfo.value.message

    @pytest.mark.asyncio
    async def test_get_positions_success(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_positions returns all positions correctly."""
        mock_user_state_payload_model = MagicMock()
        mock_user_state_payload_dict = {"type": "clearinghouseState", "user": "0xTestWalletAddress"}
        mock_user_state_payload_model.model_dump.return_value = mock_user_state_payload_dict
        mock_request_builder.build_user_state_payload.return_value = mock_user_state_payload_model

        mock_raw_user_state_response_list: list[dict[str, Any]] = [
            {"mock_state_data": "some_value"}
        ]
        mock_http_client_requester.return_value = (mock_raw_user_state_response_list, 200, {})

        mock_processed_raw_clearinghouse_state_model = MagicMock(
            spec=HyperliquidRawClearinghouseState
        )
        mock_response_handler.handle_info_user_state_response.return_value = (
            mock_processed_raw_clearinghouse_state_model
        )

        mock_btc_pos = MagicMock(name="BTC_Position")
        mock_eth_pos = MagicMock(name="ETH_Position")
        mock_hl_mapper.map_raw_clearinghouse_state_to_derivative_positions.return_value = {
            "BTC": mock_btc_pos,
            "ETH": mock_eth_pos,
        }

        result = await hyperliquid_account_service.get_positions()

        mock_request_builder.build_user_state_payload.assert_called_once_with("0xTestWalletAddress")
        mock_http_client_requester.assert_called_once()
        mock_response_handler.handle_info_user_state_response.assert_called_once_with(
            raw_response_content=mock_raw_user_state_response_list[0],
            user_address="0xTestWalletAddress",
        )

        mock_hl_mapper.map_raw_clearinghouse_state_to_derivative_positions.assert_called_once_with(
            mock_processed_raw_clearinghouse_state_model
        )
        assert isinstance(result, list)
        assert len(result) == 2
        assert mock_btc_pos in result
        assert mock_eth_pos in result

    @pytest.mark.asyncio
    async def test_get_positions_symbol_filter(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_positions filters by symbol."""
        mock_user_state_payload_model = MagicMock()
        mock_user_state_payload_dict = {"type": "clearinghouseState", "user": "0xTestWalletAddress"}
        mock_user_state_payload_model.model_dump.return_value = mock_user_state_payload_dict
        mock_request_builder.build_user_state_payload.return_value = mock_user_state_payload_model

        mock_raw_user_state_response_list: list[dict[str, Any]] = [{"mock_state_data": "val"}]
        mock_http_client_requester.return_value = (mock_raw_user_state_response_list, 200, {})

        mock_processed_raw_clearinghouse_state_model = MagicMock(
            spec=HyperliquidRawClearinghouseState
        )
        mock_response_handler.handle_info_user_state_response.return_value = (
            mock_processed_raw_clearinghouse_state_model
        )

        btc_position = MagicMock(name="BTC_Pos_Filter")
        eth_position = MagicMock(name="ETH_Pos_Filter")
        mock_hl_mapper.map_raw_clearinghouse_state_to_derivative_positions.return_value = {
            "BTC": btc_position,
            "ETH": eth_position,
        }

        result_btc = await hyperliquid_account_service.get_positions(symbol="BTC")
        assert result_btc == [btc_position]

        result_doge = await hyperliquid_account_service.get_positions(symbol="DOGE")
        assert result_doge == []

        assert mock_request_builder.build_user_state_payload.call_count == 2
        mock_request_builder.build_user_state_payload.assert_any_call("0xTestWalletAddress")
        assert mock_http_client_requester.call_count == 2
        assert mock_response_handler.handle_info_user_state_response.call_count == 2
        mock_response_handler.handle_info_user_state_response.assert_any_call(
            raw_response_content=mock_raw_user_state_response_list[0],
            user_address="0xTestWalletAddress",
        )
        assert mock_hl_mapper.map_raw_clearinghouse_state_to_derivative_positions.call_count == 2

    @pytest.mark.asyncio
    async def test_get_positions_error_from_state(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_positions propagates APIError when underlying HTTP request fails."""
        # Ensure the request builder is called and returns a valid payload model
        mock_user_state_payload_model = MagicMock()
        mock_user_state_payload_model.model_dump.return_value = {
            "type": "clearinghouseState",
            "user": "0xTestWalletAddress",
        }
        mock_request_builder.build_user_state_payload.return_value = mock_user_state_payload_model

        # Simulate an APIError from the http_client_requester
        expected_error = APIError("HTTP fail", APIErrorCode.NETWORK_ISSUE.value)
        mock_http_client_requester.side_effect = expected_error

        with pytest.raises(APIError) as excinfo:
            await hyperliquid_account_service.get_positions()

        assert excinfo.value == expected_error
        mock_request_builder.build_user_state_payload.assert_called_once_with("0xTestWalletAddress")
        mock_http_client_requester.assert_called_once()  # Verifies it was called before erroring
        mock_hl_mapper.map_raw_clearinghouse_state_to_derivative_positions.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_account_summary_success(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_account_summary returns summary."""
        mock_user_state_payload_model = MagicMock()
        mock_user_state_payload_dict = {"type": "clearinghouseState", "user": "0xTestWalletAddress"}
        mock_user_state_payload_model.model_dump.return_value = mock_user_state_payload_dict
        mock_request_builder.build_user_state_payload.return_value = mock_user_state_payload_model

        mock_raw_user_state_response_list: list[dict[str, Any]] = [{"mock_summary_data": "value"}]
        mock_http_client_requester.return_value = (mock_raw_user_state_response_list, 200, {})

        mock_processed_raw_clearinghouse_state_model = MagicMock(
            spec=HyperliquidRawClearinghouseState
        )
        mock_response_handler.handle_info_user_state_response.return_value = (
            mock_processed_raw_clearinghouse_state_model
        )

        mock_summary_object = MagicMock(name="AccountSummaryObject")
        mock_hl_mapper.map_raw_clearinghouse_state_to_margin_summary.return_value = (
            mock_summary_object
        )

        result = await hyperliquid_account_service.get_account_summary()

        mock_request_builder.build_user_state_payload.assert_called_once_with("0xTestWalletAddress")
        mock_http_client_requester.assert_called_once()
        mock_response_handler.handle_info_user_state_response.assert_called_once_with(
            raw_response_content=mock_raw_user_state_response_list[0],
            user_address="0xTestWalletAddress",
        )

        assert result == mock_summary_object
        mock_hl_mapper.map_raw_clearinghouse_state_to_margin_summary.assert_called_once_with(
            mock_processed_raw_clearinghouse_state_model
        )

    @pytest.mark.asyncio
    async def test_get_account_summary_validation_error(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_account_summary handles mapper validation error post state retrieval."""
        # 1. Setup for successful state retrieval by _get_raw_clearinghouse_state
        mock_user_state_payload_model = MagicMock()
        mock_user_state_payload_dict = {"type": "clearinghouseState", "user": "0xTestWalletAddress"}
        mock_user_state_payload_model.model_dump.return_value = mock_user_state_payload_dict
        mock_request_builder.build_user_state_payload.return_value = mock_user_state_payload_model

        # Mock raw response from HTTP client for _get_raw_clearinghouse_state
        mock_raw_user_state_response_list: list[dict[str, Any]] = [
            {"valid_state_data": "some_value"}
        ]
        mock_http_client_requester.return_value = (mock_raw_user_state_response_list, 200, {})

        # Mock processed response from ResponseHandler for _get_raw_clearinghouse_state
        # This is the 'mock_state' that was previously returned by the patched method
        mock_processed_state = MagicMock(spec=HyperliquidRawClearinghouseState)
        mock_response_handler.handle_info_user_state_response.return_value = mock_processed_state

        # 2. Setup mapper to cause a validation error
        mock_hl_mapper.map_raw_clearinghouse_state_to_margin_summary.side_effect = ValueError(
            "bad map"
        )

        with pytest.raises(APIError) as excinfo:
            await hyperliquid_account_service.get_account_summary()

        assert "Processing HL account summary data failed" in str(excinfo.value)
        assert isinstance(
            excinfo.value.__cause__, ValueError
        )  # Check that original ValueError is preserved
        assert str(excinfo.value.__cause__) == "bad map"

        mock_request_builder.build_user_state_payload.assert_called_once_with("0xTestWalletAddress")
        mock_http_client_requester.assert_called_once()
        mock_response_handler.handle_info_user_state_response.assert_called_once_with(
            raw_response_content=mock_raw_user_state_response_list[0],
            user_address="0xTestWalletAddress",
        )
        mock_hl_mapper.map_raw_clearinghouse_state_to_margin_summary.assert_called_once_with(
            mock_processed_state
        )

    @pytest.mark.asyncio
    async def test_get_account_summary_error_from_state(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_account_summary propagates APIError when underlying HTTP request fails."""
        mock_user_state_payload_model = MagicMock()
        mock_user_state_payload_model.model_dump.return_value = {
            "type": "clearinghouseState",
            "user": "0xTestWalletAddress",
        }
        mock_request_builder.build_user_state_payload.return_value = mock_user_state_payload_model

        expected_error = APIError("HTTP fail for summary", APIErrorCode.NETWORK_ISSUE.value)
        mock_http_client_requester.side_effect = expected_error

        with pytest.raises(APIError) as excinfo:
            await hyperliquid_account_service.get_account_summary()

        assert excinfo.value == expected_error
        mock_request_builder.build_user_state_payload.assert_called_once_with("0xTestWalletAddress")
        mock_http_client_requester.assert_called_once()
        mock_hl_mapper.map_raw_clearinghouse_state_to_margin_summary.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_order_history_success(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_order_mapper: MagicMock,
    ) -> None:
        """Test get_order_history returns mapped orders."""
        # wallet_address is already set by the hyperliquid_account_service fixture
        # hyperliquid_account_service._wallet_address = "0xTestWallet" # REMOVE THIS

        # Patch builder
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_order_history_payload.return_value = mock_payload_model
        # Patch http_client_requester
        mock_http_client_requester.return_value = ([{"order": 1}], 200, {})
        # Patch response handler
        mock_raw_order = MagicMock()
        mock_response_handler.handle_query_order_history_response.return_value = [
            MagicMock(order=mock_raw_order)
        ]
        # Patch order mapper
        mapped_order = MagicMock(symbol="BTC")
        mock_hl_order_mapper.transform_raw_historical_order_to_internal.return_value = mapped_order
        result = await hyperliquid_account_service.get_order_history(
            symbol="BTC", start_time=datetime(2024, 1, 1), end_time=datetime(2024, 1, 2)
        )
        assert result == [mapped_order]

    @pytest.mark.asyncio
    async def test_get_order_history_symbol_filter(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_order_mapper: MagicMock,
    ) -> None:
        """Test get_order_history filters by symbol."""
        # Wallet address is already set by the hyperliquid_account_service fixture
        # hyperliquid_account_service._wallet_address = "0xTestWallet" # This line was removed
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_order_history_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = ([{"order": 1}], 200, {})
        mock_raw_order1 = MagicMock()
        mock_raw_order2 = MagicMock()
        mock_response_handler.handle_query_order_history_response.return_value = [
            MagicMock(order=mock_raw_order1),
            MagicMock(order=mock_raw_order2),
        ]
        mapped_order1 = MagicMock(symbol="BTC")
        mapped_order2 = MagicMock(symbol="ETH")

        def map_side_effect(raw: MagicMock, trigger: MagicMock | None = None) -> MagicMock:
            return mapped_order1 if raw is mock_raw_order1 else mapped_order2

        mock_hl_order_mapper.transform_raw_historical_order_to_internal.side_effect = (
            map_side_effect
        )
        result = await hyperliquid_account_service.get_order_history(
            symbol="BTC",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
        )
        assert result == [mapped_order1]
        result_all = await hyperliquid_account_service.get_order_history(
            symbol=None,
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
        )
        assert set(result_all) == {mapped_order1, mapped_order2}

    @pytest.mark.asyncio
    async def test_get_order_history_error_conditions(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_hl_mapper: MagicMock,
        mock_hl_order_mapper: MagicMock,
        mock_hl_user_fill_mapper: MagicMock,
    ) -> None:
        """Test get_order_history error handling for missing wallet, missing times, and APIError
        from requester."""
        # No wallet address case: Instantiate service with wallet_address=None
        service_no_wallet = HyperliquidAccountService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid_test_no_wallet_order_hist",
            info_url="https://info.hyperliquid.xyz",
            wallet_address=None,  # Key change here
            mapper=mock_hl_mapper,
            order_mapper=mock_hl_order_mapper,
            user_fill_mapper=mock_hl_user_fill_mapper,
        )
        with pytest.raises(APIError) as excinfo_no_wallet:
            await service_no_wallet.get_order_history(
                symbol=None,
                start_time=datetime(2024, 1, 1),
                end_time=datetime(2024, 1, 2),
            )
        assert excinfo_no_wallet.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Wallet address is required" in excinfo_no_wallet.value.message

        # Missing times (uses the standard hyperliquid_account_service fixture
        # which has a wallet address)
        # hyperliquid_account_service._wallet_address = "0xTestWallet" # No longer needed
        with pytest.raises(APIError) as excinfo_no_times:
            await hyperliquid_account_service.get_order_history(
                symbol=None, start_time=None, end_time=None
            )
        assert excinfo_no_times.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "start_time and end_time are required" in excinfo_no_times.value.message

        # APIError from requester (uses the standard hyperliquid_account_service
        # fixture which has a wallet address)
        # No need to manipulate _wallet_address here, fixture provides it.
        mock_request_builder.build_order_history_payload.return_value = MagicMock(
            model_dump=lambda: {"foo": "bar"}
        )
        mock_http_client_requester.side_effect = APIError("fail", 1)
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_order_history(
                symbol=None,
                start_time=datetime(2024, 1, 1),
                end_time=datetime(2024, 1, 2),
            )

    @pytest.mark.asyncio
    async def test_get_trade_history_success(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_user_fill_mapper: MagicMock,
    ) -> None:
        """Test get_trade_history returns mapped trades."""
        # Wallet address is already set by the hyperliquid_account_service fixture
        # hyperliquid_account_service._wallet_address = "0xTestWallet" # This line was removed
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_user_fills_request_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = ([{"fill": 1}], 200, {})
        mock_raw_fill = MagicMock()
        mock_response_handler.handle_info_user_fills_response.return_value = MagicMock(
            root=[mock_raw_fill]
        )
        mapped_trade = MagicMock(symbol="BTC")
        with patch.object(
            HyperliquidUserFillMapper, "map", return_value=mapped_trade
        ) as mocked_map_method:
            result = await hyperliquid_account_service.get_trade_history(symbol="BTC")
            assert result == [mapped_trade]
            mocked_map_method.assert_called_once_with(mock_raw_fill)

    @pytest.mark.asyncio
    async def test_get_trade_history_symbol_filter(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_user_fill_mapper: MagicMock,
    ) -> None:
        """Test get_trade_history filters by symbol."""
        # Wallet address is already set by the hyperliquid_account_service fixture
        # hyperliquid_account_service._wallet_address = "0xTestWallet" # This line was removed
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_user_fills_request_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = ([{"fill": 1}], 200, {})
        mock_raw_fill1 = MagicMock(name="raw_fill1")
        mock_raw_fill2 = MagicMock(name="raw_fill2")
        mock_response_handler.handle_info_user_fills_response.return_value = MagicMock(
            root=[mock_raw_fill1, mock_raw_fill2]
        )
        mapped_trade1 = MagicMock(symbol="BTC")
        mapped_trade2 = MagicMock(symbol="ETH")

        def map_side_effect_func(raw_fill_arg: MagicMock) -> MagicMock:
            if raw_fill_arg is mock_raw_fill1:
                return mapped_trade1
            if raw_fill_arg is mock_raw_fill2:
                return mapped_trade2
            raise AssertionError(f"Unexpected raw_fill_arg: {raw_fill_arg}")

        with patch.object(
            HyperliquidUserFillMapper, "map", side_effect=map_side_effect_func
        ) as mocked_map_method:
            result = await hyperliquid_account_service.get_trade_history(symbol="BTC")
            assert result == [mapped_trade1]
            mocked_map_method.assert_any_call(mock_raw_fill1)

            mocked_map_method.reset_mock()
            result_all = await hyperliquid_account_service.get_trade_history(symbol=None)
            assert set(result_all) == {mapped_trade1, mapped_trade2}
            mocked_map_method.assert_any_call(mock_raw_fill1)
            mocked_map_method.assert_any_call(mock_raw_fill2)

    @pytest.mark.asyncio
    async def test_get_trade_history_error_conditions(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_hl_mapper: MagicMock,
        mock_hl_order_mapper: MagicMock,
        mock_hl_user_fill_mapper: MagicMock,
    ) -> None:
        """Test get_trade_history error handling for missing wallet and APIError from requester."""
        # No wallet address case: Instantiate service with wallet_address=None
        service_no_wallet_trade_hist = HyperliquidAccountService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid_test_no_wallet_trade_hist",
            info_url="https://info.hyperliquid.xyz",
            wallet_address=None,  # Key: Instantiate with None
            mapper=mock_hl_mapper,
            order_mapper=mock_hl_order_mapper,
            user_fill_mapper=mock_hl_user_fill_mapper,
        )
        with pytest.raises(APIError) as excinfo_no_wallet:
            await service_no_wallet_trade_hist.get_trade_history(symbol=None)
        assert excinfo_no_wallet.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Wallet address is required" in excinfo_no_wallet.value.message

        # APIError from requester (uses the standard hyperliquid_account_service fixture which has a wallet address)
        # No need to manipulate _wallet_address here, fixture provides it.
        mock_request_builder.build_user_fills_request_payload.return_value = MagicMock(
            model_dump=lambda: {"foo": "bar"}
        )
        mock_http_client_requester.side_effect = APIError("fail", 1)
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_trade_history(symbol=None)  # Uses the fixture

    @pytest.mark.asyncio
    async def test_get_balances_api_error_from_state(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_balances handles APIError when underlying HTTP request fails."""
        mock_user_state_payload_model = MagicMock()
        mock_user_state_payload_model.model_dump.return_value = {
            "type": "clearinghouseState",
            "user": "0xTestWalletAddress",
        }
        mock_request_builder.build_user_state_payload.return_value = mock_user_state_payload_model

        expected_api_error = APIError(
            "Failed to get raw state via HTTP", APIErrorCode.SERVER_ERROR.value
        )
        mock_http_client_requester.side_effect = expected_api_error

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_account_service.get_balances()

        assert exc_info.value == expected_api_error
        mock_request_builder.build_user_state_payload.assert_called_once_with("0xTestWalletAddress")
        mock_http_client_requester.assert_called_once()
        mock_hl_mapper.map_raw_clearinghouse_state_to_spot_balances.assert_not_called()
