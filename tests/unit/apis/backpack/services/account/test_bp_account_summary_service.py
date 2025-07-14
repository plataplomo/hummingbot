"""Unit tests for Backpack Account Summary Service.

Tests cover all methods of the BackpackAccountSummaryService including:
- Account summary retrieval (basic and enhanced)
- Account settings updates
- Collateral data integration
- Subaccount support
- Error handling and fallback scenarios
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_collateral import (
    BackpackRawCollateralResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.services.account.bp_account_summary_service import (
    BackpackAccountSummaryService,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions import EmptyResponseError
from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs
from cyberdelta.core.models import AccountSettings
from cyberdelta.core.models.margin_account import BackpackMarginDetails, MarginAccountSummary


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Create a mock HTTP client requester."""
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Create a mock request builder."""
    return MagicMock(spec=BackpackAccountRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Create a mock response handler."""
    return MagicMock(spec=BackpackAccountResponseHandler)


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Create a mock data mapper."""
    return MagicMock(spec=BackpackBalanceMapper)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Create a mock authenticator."""
    return MagicMock()


@pytest.fixture
def account_summary_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_mapper: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackAccountSummaryService:
    """Create an account summary service instance with mocks."""
    return BackpackAccountSummaryService(
        http_client_requester=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        authenticator=mock_authenticator,
        exchange_name="backpack",
    )


@pytest.fixture
def mock_raw_account_summary() -> BackpackRawAccountSummary:
    """Create a mock raw account summary."""
    return BackpackRawAccountSummary.model_validate({
        "autoBorrowSettlements": True,
        "autoLend": True,
        "autoRealizePnl": True,
        "autoRepayBorrows": True,
        "borrowLimit": "100000.00",
        "futuresMakerFee": "0.0002",
        "futuresTakerFee": "0.0004",
        "leverageLimit": "10.00",
        "limitOrders": 100,
        "liquidating": False,
        "positionLimit": "1000000.00",
        "spotMakerFee": "0.0010",
        "spotTakerFee": "0.0015",
        "triggerOrders": 50,
    })


@pytest.fixture
def mock_raw_balance() -> BackpackRawBalance:
    """Create a mock raw balance."""
    return BackpackRawBalance(
        available="1000.00",
        locked="100.00",
        staked="0.00",
    )


@pytest.fixture
def mock_raw_position() -> BackpackRawPosition:
    """Create a mock raw position."""
    return BackpackRawPosition.model_validate({
        "symbol": "BTC-PERP",
        "netQuantity": "1.5",
        "entryPrice": "50000.00",
        "netExposureNotional": "75000.00",
        "pnlUnrealized": "1000.00",
        "pnlRealized": "500.00",
        "estLiquidationPrice": "45000.00",
        "markPrice": "50666.67",
        "breakEvenPrice": "50000.00",
        "imf": "0.05",
        "imfFunction": {"base": "0.01", "factor": "0.04"},
        "mmf": "0.025",
        "mmfFunction": {"base": "0.005", "factor": "0.02"},
        "netCost": "75000.00",
        "netExposureQuantity": "1.5",
        "cumulativeFundingPayment": "0.00",
        "userId": 123,
        "positionId": "pos_123",
        "subaccountId": 0,
        "cumulativeInterest": "0.00",
    })


@pytest.fixture
def mock_collateral_response() -> BackpackRawCollateralResponse:
    """Create a mock collateral response."""
    return BackpackRawCollateralResponse.model_validate({
        "netEquity": "77600.00",
        "netEquityAvailable": "77390.00",
        "netEquityLocked": "210.00",
        "assetsValue": "77600.00",
        "liabilitiesValue": "0.00",
        "imf": "0.10",
        "mmf": "0.05",
        "marginFraction": "0.97",
        "borrowLiability": "0.00",
        "pnlUnrealized": "1000.00",
        "unsettledEquity": "0.00",
        "netExposureFutures": "75000.00",
        "collateral": [
            {
                "symbol": "USDC",
                "assetMarkPrice": "1.00",
                "totalQuantity": "2100.00",
                "balanceNotional": "2100.00",
                "collateralWeight": "1.00",
                "collateralValue": "2100.00",
                "openOrderQuantity": "100.00",
                "lendQuantity": "0.00",
                "availableQuantity": "2000.00",
            },
        ],
    })


@pytest.fixture
def mock_margin_account_summary() -> MarginAccountSummary:
    """Create a mock margin account summary."""
    return MarginAccountSummary(
        exchange="backpack",
        timestamp=datetime.now(UTC),
        total_equity=Decimal("77600.00"),
        available_equity=Decimal("69890.00"),
        total_initial_margin_required=Decimal("210.00"),
        total_maintenance_margin_required=Decimal("0.00"),
        total_position_notional=Decimal("7710.00"),
        total_unrealized_pnl=Decimal("1000.00"),
        bp_details=BackpackMarginDetails(
            assets_value=Decimal("77600.00"),
            liabilities_value=Decimal("0.00"),
            locked_equity=Decimal("7710.00"),
            leverage_limit=Decimal("10.00"),
            margin_fraction=Decimal("0.97"),
        ),
    )


@pytest.fixture
def mock_account_settings() -> AccountSettings:
    """Create mock account settings."""
    return AccountSettings(
        exchange="backpack",
        auto_borrow_settlements=True,
        auto_lend=False,
        auto_realize_pnl=True,
        auto_repay_borrows=False,
        leverage_limit=Decimal("5.00"),
        timestamp=datetime.now(UTC),
    )


class TestBackpackAccountSummaryService:
    """Test suite for BackpackAccountSummaryService."""

    @pytest.mark.asyncio
    async def test_get_account_summary_enhanced_success(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_account_summary: BackpackRawAccountSummary,
        mock_collateral_response: BackpackRawCollateralResponse,
        mock_raw_position: BackpackRawPosition,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test successful enhanced account summary retrieval."""
        # Arrange
        # Mock the three parallel API calls
        mock_http_client.side_effect = [
            # Collateral response
            (mock_collateral_response.model_dump(), 200, {}),
            # Account settings response
            (mock_raw_account_summary.model_dump(), 200, {}),
            # Positions response
            ([mock_raw_position.model_dump()], 200, {}),
        ]

        mock_response_handler.handle_get_collateral_response.return_value = mock_collateral_response
        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )
        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]

        mock_mapper.transform_enhanced_account_data_to_margin_summary.return_value = (
            mock_margin_account_summary
        )

        # Act
        result = await account_summary_service.get_account_summary()

        # Assert
        assert result == mock_margin_account_summary
        assert mock_http_client.call_count == 3
        mock_mapper.transform_enhanced_account_data_to_margin_summary.assert_called_once_with(
            raw_collateral=mock_collateral_response,
            raw_settings=mock_raw_account_summary,
            raw_positions=[mock_raw_position],
        )

    @pytest.mark.asyncio
    async def test_get_account_summary_fallback_to_basic(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_account_summary: BackpackRawAccountSummary,
        mock_raw_balance: BackpackRawBalance,
        mock_raw_position: BackpackRawPosition,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test account summary falls back to basic when collateral unavailable."""
        # Arrange
        # First attempt (enhanced) - collateral returns 404
        mock_http_client.side_effect = [
            # Collateral 404
            APIError(
                message="Not found",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
                http_status=404,
            ),
            # Then basic mode calls
            # Account settings
            (mock_raw_account_summary.model_dump(), 200, {}),
            # Balances
            ({"USDC": mock_raw_balance.model_dump()}, 200, {}),
            # Positions
            ([mock_raw_position.model_dump()], 200, {}),
        ]

        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )
        mock_response_handler.handle_get_balances_response.return_value = {"USDC": mock_raw_balance}
        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]

        mock_mapper.transform_raw_account_summary_to_internal.return_value = (
            mock_margin_account_summary
        )

        # Act
        result = await account_summary_service.get_account_summary()

        # Assert
        assert result == mock_margin_account_summary
        assert mock_http_client.call_count == 4  # 1 failed collateral + 3 basic calls
        mock_mapper.transform_raw_account_summary_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_account_summary_with_subaccount(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_collateral_response: BackpackRawCollateralResponse,
        mock_raw_account_summary: BackpackRawAccountSummary,
        mock_raw_position: BackpackRawPosition,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test account summary with subaccount ID."""
        # Arrange
        subaccount_id = 123

        mock_http_client.side_effect = [
            (mock_collateral_response.model_dump(), 200, {}),
            (mock_raw_account_summary.model_dump(), 200, {}),
            ([mock_raw_position.model_dump()], 200, {}),
        ]

        mock_response_handler.handle_get_collateral_response.return_value = mock_collateral_response
        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )
        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]

        mock_mapper.transform_enhanced_account_data_to_margin_summary.return_value = (
            mock_margin_account_summary
        )

        # Act
        result = await account_summary_service.get_account_summary(subaccount_id=subaccount_id)

        # Assert
        assert result == mock_margin_account_summary
        # Verify subaccount ID was passed to collateral endpoint
        mock_response_handler.handle_get_collateral_response.assert_called_once()
        call_args = mock_response_handler.handle_get_collateral_response.call_args
        assert call_args.kwargs["subaccount_id"] == subaccount_id

    @pytest.mark.asyncio
    async def test_get_account_summary_invalid_subaccount_id(
        self,
        account_summary_service: BackpackAccountSummaryService,
    ) -> None:
        """Test account summary with invalid subaccount ID."""
        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await account_summary_service.get_account_summary(subaccount_id=70000)  # > uint16 max

        assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Must be uint16" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_update_account_settings_success(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_mapper: MagicMock,
        mock_account_settings: AccountSettings,
    ) -> None:
        """Test successful account settings update."""
        # Arrange
        args = UpdateAccountSettingsArgs(
            auto_borrow_settlements=True,
            auto_lend=False,
            auto_realize_pnl=True,
            auto_repay_borrows=False,
            leverage_limit=Decimal("5.00"),
        )

        mock_payload = {"leverage_limit": "5.00"}
        mock_request_builder.build_update_account_settings_payload.return_value = mock_payload

        mock_http_client.return_value = (None, 200, {})

        mock_mapper.transform_account_settings_update_to_internal.return_value = (
            mock_account_settings
        )

        # Act
        result = await account_summary_service.update_account_settings(args)

        # Assert
        assert result == mock_account_settings
        mock_request_builder.build_update_account_settings_payload.assert_called_once_with(
            auto_borrow_settlements=args.auto_borrow_settlements,
            auto_lend=args.auto_lend,
            auto_realize_pnl=args.auto_realize_pnl,
            auto_repay_borrows=args.auto_repay_borrows,
            leverage_limit=args.leverage_limit,
        )
        mock_http_client.assert_called_once()
        assert mock_http_client.call_args.kwargs["method"] == "PATCH"
        assert mock_http_client.call_args.kwargs["endpoint"] == "/api/v1/account"
        assert mock_http_client.call_args.kwargs["data"] == mock_payload

    @pytest.mark.asyncio
    async def test_update_account_settings_http_error(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test account settings update with HTTP error."""
        # Arrange
        args = UpdateAccountSettingsArgs(leverage_limit=Decimal("3.00"))

        mock_request_builder.build_update_account_settings_payload.return_value = {}
        mock_http_client.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await account_summary_service.update_account_settings(args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_enhanced_account_info_transformation_error(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_collateral_response: BackpackRawCollateralResponse,
        mock_raw_account_summary: BackpackRawAccountSummary,
        mock_raw_position: BackpackRawPosition,
    ) -> None:
        """Test enhanced account info with transformation error."""
        # Arrange
        mock_http_client.side_effect = [
            (mock_collateral_response.model_dump(), 200, {}),
            (mock_raw_account_summary.model_dump(), 200, {}),
            ([mock_raw_position.model_dump()], 200, {}),
        ]

        mock_response_handler.handle_get_collateral_response.return_value = mock_collateral_response
        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )
        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]

        mock_mapper.transform_enhanced_account_data_to_margin_summary.side_effect = (
            TransformationError("Invalid data format")
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await account_summary_service.get_account_summary()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to process/transform enhanced collateral data" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_basic_account_info_empty_positions(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_account_summary: BackpackRawAccountSummary,
        mock_raw_balance: BackpackRawBalance,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test basic account info with empty positions."""
        # Arrange
        # Force fallback to basic mode
        mock_http_client.side_effect = [
            # Collateral fails
            APIError(message="Not found", code=APIErrorCode.ORDER_NOT_FOUND.value, http_status=404),
            # Basic mode calls
            (mock_raw_account_summary.model_dump(), 200, {}),
            ({"USDC": mock_raw_balance.model_dump()}, 200, {}),
            ([], 200, {}),  # Empty positions
        ]

        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )
        mock_response_handler.handle_get_balances_response.return_value = {"USDC": mock_raw_balance}
        mock_response_handler.handle_get_positions_response.return_value = []

        mock_mapper.transform_raw_account_summary_to_internal.return_value = (
            mock_margin_account_summary
        )

        # Act
        result = await account_summary_service.get_account_summary()

        # Assert
        assert result == mock_margin_account_summary
        mock_mapper.transform_raw_account_summary_to_internal.assert_called_once_with(
            raw_settings=mock_raw_account_summary,
            spot_balances_raw={"USDC": mock_raw_balance},
            derivative_positions_raw=[],
        )

    @pytest.mark.asyncio
    async def test_get_raw_collateral_empty_response(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
    ) -> None:
        """Test collateral fetch with empty response."""
        # Arrange
        mock_http_client.return_value = (None, 200, {})

        # Act & Assert
        with pytest.raises(EmptyResponseError) as exc_info:
            await account_summary_service._get_raw_collateral_response()

        assert "collateral request" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_raw_positions_with_dict_response(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_raw_position: BackpackRawPosition,
    ) -> None:
        """Test positions fetch with dict response (instead of list)."""
        # Arrange
        dict_response = {"BTC-PERP": mock_raw_position.model_dump()}
        mock_http_client.return_value = (dict_response, 200, {})

        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]

        # Act
        result = await account_summary_service._get_raw_positions_list()

        # Assert
        assert len(result) == 1
        assert result[0] == mock_raw_position
        mock_response_handler.handle_get_positions_response.assert_called_once_with(
            dict_response,
            None,
            200,
        )

    @pytest.mark.asyncio
    async def test_get_raw_positions_404_returns_empty(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
    ) -> None:
        """Test positions fetch with 404 returns empty list."""
        # Arrange
        mock_http_client.side_effect = APIError(
            message="Not found",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=404,
        )

        # Act
        result = await account_summary_service._get_raw_positions_list()

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_account_summary_without_authenticator(
        self,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test account summary when authenticator is None."""
        # Arrange
        service = BackpackAccountSummaryService(
            http_client_requester=mock_http_client,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            mapper=mock_mapper,
            authenticator=None,  # No authenticator
            exchange_name="backpack",
        )

        # Force fallback to basic mode
        mock_http_client.side_effect = [
            APIError(message="Not found", code=APIErrorCode.ORDER_NOT_FOUND.value, http_status=404),
        ]

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await service.get_account_summary()

        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert "Authentication required" in exc_info.value.message
