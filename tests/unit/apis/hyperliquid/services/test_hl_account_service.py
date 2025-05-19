"""
Unit tests for the HyperliquidAccountService.
"""

from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

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
from cyberdelta.core.models.financial import SpotBalance

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
    service._mapper = mock_hl_mapper  # type: ignore[protected-access]
    service._order_mapper = mock_hl_order_mapper  # type: ignore[protected-access]
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
        mock_hl_mapper: MagicMock,  # Use the HyperliquidMapper mock
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
                    {
                        "type": "erc20",
                        "asset": "USDC",
                        "position": {"type": "cross", "amount": "1000500000"},
                    }  # 1000.5 USDC (6 decimals)
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
            unrealizedPnl="0",
        )
        mock_raw_asset_position_usdc = HyperliquidRawAssetPosition(
            asset="USDC", position=mock_raw_position_info
        )

        mock_raw_margin_summary = HyperliquidRawMarginSummary(
            accountValue="1000.5",  # Use alias
            totalMarginUsed="0",  # Use alias
            totalNtlPos="1000.5",  # Use alias
            totalRawUsd="1000.5",  # Use alias
        )

        mock_raw_clearinghouse_state_model = HyperliquidRawClearinghouseState(
            assetPositions=[mock_raw_asset_position_usdc],  # Use alias
            marginSummary=mock_raw_margin_summary,  # Use alias
            crossMaintenanceMarginUsed="0",  # Use alias
            crossMarginSummary=mock_raw_margin_summary,  # Use alias
            isolatedMaintenanceMarginUsed="0",  # Use alias
            isolatedMarginSummary=mock_raw_margin_summary,  # Use alias
            withdrawable="1000.5",
        )
        mock_response_handler.handle_info_user_state_response.return_value = (
            mock_raw_clearinghouse_state_model
        )

        # 4. Mock Mapper call (HyperliquidMapper)
        expected_internal_balances: dict[str, SpotBalance] = {
            "USDC": SpotBalance(
                asset_symbol="USDC",
                total_balance=Decimal("1000.5"),
                available_balance=Decimal("1000.5"),
            )
        }
        mock_hl_mapper.map_raw_clearinghouse_state_to_spot_balances.return_value = (
            expected_internal_balances
        )

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
            raw_response_content=mock_raw_user_state_response_list[0],  # Service passes the dict
            user_address=wallet_address,
        )
        mock_hl_mapper.map_raw_clearinghouse_state_to_spot_balances.assert_called_once_with(
            mock_raw_clearinghouse_state_model
        )
        assert result_balances == expected_internal_balances

    @pytest.mark.asyncio
    async def test_get_balances_no_wallet_address(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
    ) -> None:
        """Test get_balances raises APIError if wallet_address is not set in service."""
        hyperliquid_account_service._wallet_address = None  # type: ignore[protected-access]
        with pytest.raises(APIError) as excinfo:
            await hyperliquid_account_service.get_balances()
        assert excinfo.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Wallet address is required" in excinfo.value.message

    @pytest.mark.asyncio
    async def test_get_positions_success(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_hl_mapper: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_positions returns all positions correctly."""
        # Mock _get_raw_clearinghouse_state
        mock_state = MagicMock()
        mock_hl_mapper.map_raw_clearinghouse_state_to_derivative_positions.return_value = {
            "BTC": MagicMock(),
            "ETH": MagicMock(),
        }
        hyperliquid_account_service._get_raw_clearinghouse_state = AsyncMock(
            return_value=mock_state
        )  # type: ignore
        result = await hyperliquid_account_service.get_positions()
        mock_hl_mapper.map_raw_clearinghouse_state_to_derivative_positions.assert_called_once_with(
            mock_state
        )
        assert isinstance(result, list)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_positions_symbol_filter(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_positions filters by symbol."""
        mock_state = MagicMock()
        btc_position = MagicMock()
        mock_hl_mapper.map_raw_clearinghouse_state_to_derivative_positions.return_value = {
            "BTC": btc_position,
            "ETH": MagicMock(),
        }
        hyperliquid_account_service._get_raw_clearinghouse_state = AsyncMock(
            return_value=mock_state
        )  # type: ignore
        result = await hyperliquid_account_service.get_positions(symbol="BTC")
        assert result == [btc_position]
        result_none = await hyperliquid_account_service.get_positions(symbol="DOGE")
        assert result_none == []

    @pytest.mark.asyncio
    async def test_get_positions_error_from_state(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
    ) -> None:
        """Test get_positions propagates APIError from _get_raw_clearinghouse_state."""
        hyperliquid_account_service._get_raw_clearinghouse_state = AsyncMock(
            side_effect=APIError("fail", 1)
        )  # type: ignore
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_positions()

    @pytest.mark.asyncio
    async def test_get_account_summary_success(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_account_summary returns summary."""
        mock_state = MagicMock()
        mock_summary = MagicMock()
        hyperliquid_account_service._get_raw_clearinghouse_state = AsyncMock(
            return_value=mock_state
        )  # type: ignore
        mock_hl_mapper.map_raw_clearinghouse_state_to_margin_summary.return_value = mock_summary
        result = await hyperliquid_account_service.get_account_summary()
        assert result == mock_summary

    @pytest.mark.asyncio
    async def test_get_account_summary_validation_error(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_account_summary handles validation error from mapper."""
        mock_state = MagicMock()
        hyperliquid_account_service._get_raw_clearinghouse_state = AsyncMock(
            return_value=mock_state
        )  # type: ignore
        mock_hl_mapper.map_raw_clearinghouse_state_to_margin_summary.side_effect = ValueError("bad")
        with pytest.raises(APIError) as excinfo:
            await hyperliquid_account_service.get_account_summary()
        assert "Processing HL account summary data failed" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_get_account_summary_error_from_state(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
    ) -> None:
        """Test get_account_summary propagates APIError from _get_raw_clearinghouse_state."""
        hyperliquid_account_service._get_raw_clearinghouse_state = AsyncMock(
            side_effect=APIError("fail", 1)
        )  # type: ignore
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_account_summary()

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
        # Patch wallet address
        hyperliquid_account_service._wallet_address = "0xTestWallet"
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
        # Accessing protected member for test setup is acceptable in test context
        hyperliquid_account_service._wallet_address = "0xTestWallet"  # type: ignore[attr-defined]  # noqa: SLF001
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
        mock_hl_order_mapper.transform_raw_historical_order_to_internal.side_effect = map_side_effect
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
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_history error handling for missing wallet, missing times, and APIError from \
        requester."""
        # No wallet address
        hyperliquid_account_service._wallet_address = None  # type: ignore[attr-defined]  # noqa: SLF001
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_order_history(
                symbol=None,
                start_time=datetime(2024, 1, 1),
                end_time=datetime(2024, 1, 2),
            )
        # Missing times
        hyperliquid_account_service._wallet_address = "0xTestWallet"  # type: ignore[attr-defined]  # noqa: SLF001
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_order_history(
                symbol=None, start_time=None, end_time=None
            )
        # APIError from requester
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
        hyperliquid_account_service._wallet_address = "0xTestWallet"
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_user_fills_request_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = ([{"fill": 1}], 200, {})
        mock_raw_fill = MagicMock()
        mock_response_handler.handle_info_user_fills_response.return_value = MagicMock(
            root=[mock_raw_fill]
        )
        mapped_trade = MagicMock(symbol="BTC")
        mock_hl_user_fill_mapper.map.return_value = mapped_trade
        # Patch the static method
        import cyberdelta.apis.hyperliquid.hl_mapper as hl_mapper_mod

        orig_map = hl_mapper_mod.HyperliquidUserFillMapper.map
        hl_mapper_mod.HyperliquidUserFillMapper.map = mock_hl_user_fill_mapper.map
        try:
            result = await hyperliquid_account_service.get_trade_history(symbol="BTC")
            assert result == [mapped_trade]
        finally:
            hl_mapper_mod.HyperliquidUserFillMapper.map = orig_map

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
        # Accessing protected member for test setup is acceptable in test context
        hyperliquid_account_service._wallet_address = "0xTestWallet"  # type: ignore[attr-defined]  # noqa: SLF001
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_user_fills_request_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = ([{"fill": 1}], 200, {})
        mock_raw_fill1 = MagicMock()
        mock_raw_fill2 = MagicMock()
        mock_response_handler.handle_info_user_fills_response.return_value = MagicMock(
            root=[mock_raw_fill1, mock_raw_fill2]
        )
        mapped_trade1 = MagicMock(symbol="BTC")
        mapped_trade2 = MagicMock(symbol="ETH")
        import cyberdelta.apis.hyperliquid.hl_mapper as hl_mapper_mod
        orig_map = hl_mapper_mod.HyperliquidUserFillMapper.map
        def map_side_effect(raw: MagicMock, trigger: MagicMock | None = None) -> MagicMock:
            return mapped_trade1 if raw is mock_raw_fill1 else mapped_trade2
        mock_hl_user_fill_mapper.map.side_effect = map_side_effect
        hl_mapper_mod.HyperliquidUserFillMapper.map = mock_hl_user_fill_mapper.map
        try:
            result = await hyperliquid_account_service.get_trade_history(symbol="BTC")
            assert result == [mapped_trade1]
            result_all = await hyperliquid_account_service.get_trade_history(symbol=None)
            assert set(result_all) == {mapped_trade1, mapped_trade2}
        finally:
            hl_mapper_mod.HyperliquidUserFillMapper.map = orig_map

    @pytest.mark.asyncio
    async def test_get_trade_history_error_conditions(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_trade_history error handling for missing wallet and APIError from requester."""
        # Accessing protected member for test setup is acceptable in test context
        hyperliquid_account_service._wallet_address = None  # type: ignore[attr-defined]  # noqa: SLF001
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_trade_history(symbol=None)
        hyperliquid_account_service._wallet_address = "0xTestWallet"  # type: ignore[attr-defined]  # noqa: SLF001
        mock_request_builder.build_user_fills_request_payload.return_value = MagicMock(
            model_dump=lambda: {"foo": "bar"}
        )
        mock_http_client_requester.side_effect = APIError("fail", 1)
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_trade_history(symbol=None)

    @pytest.mark.asyncio
    async def test_get_raw_clearinghouse_state_api_error(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
    ) -> None:
        """Test _get_raw_clearinghouse_state propagates APIError."""
        # Accessing protected member for test setup is acceptable in test context
        hyperliquid_account_service._wallet_address = None  # type: ignore[attr-defined]  # noqa: SLF001
        with pytest.raises(APIError):
            await hyperliquid_account_service._get_raw_clearinghouse_state()
