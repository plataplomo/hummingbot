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

from cyberdelta.apis.backpack.mappers.account.bp_account_summary_mapper import (
    BackpackAccountSummaryMapper,
)
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummaryResponse
from cyberdelta.apis.backpack.models.bp_raw_collateral import (
    BackpackRawCollateralResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionResponse
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.services.account.bp_account_summary_service import (
    BackpackAccountSummaryService,
)
from cyberdelta.apis.base.trading_execution_domain import (
    AccountSettings as DomainAccountSettings,
    AccountSettingsPolicy,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions import EmptyResponseError
from cyberdelta.apis.models.service_args.account import UpdateAccountSettingsArgs
from cyberdelta.models import AccountSettings
from cyberdelta.models.margin_account import BackpackMarginDetails, MarginAccountSummary
from tests.common_symbols import BTC_BP


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Create a mock HTTP client requester.

    Returns:
        AsyncMock: A mock instance of the HTTP client.
    """
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Create a mock request builder.

    Returns:
        MagicMock: A mock instance of BackpackAccountRequestBuilder.
    """
    return MagicMock(spec=BackpackAccountRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Create a mock response handler.

    Returns:
        MagicMock: A mock instance of BackpackAccountResponseHandler.
    """
    return MagicMock(spec=BackpackAccountResponseHandler)


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Create a mock data mapper.

    Returns:
        MagicMock: A mock instance of BackpackAccountSummaryMapper.
    """
    return MagicMock(spec=BackpackAccountSummaryMapper)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Create a mock authenticator.

    Returns:
        MagicMock: A mock instance of authenticator.
    """
    return MagicMock()


@pytest.fixture
def mock_account_state_service() -> AsyncMock:
    """Create a mock account state service.

    Returns:
        AsyncMock: A mock instance of account state service.
    """
    return AsyncMock()


@pytest.fixture
def account_summary_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_mapper: MagicMock,
    mock_authenticator: MagicMock,
    mock_account_state_service: AsyncMock,
) -> BackpackAccountSummaryService:
    """Create an account summary service instance with mocks.

    Returns:
        BackpackAccountSummaryService: Service instance configured with mock dependencies.
    """
    return BackpackAccountSummaryService(
        http_client_requester=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        authenticator=mock_authenticator,
        exchange_name="backpack",
        account_state_service=mock_account_state_service,
    )


@pytest.fixture
def mock_raw_account_summary() -> BackpackRawAccountSummaryResponse:
    """Create a mock raw account summary.

    Returns:
        BackpackRawAccountSummaryResponse: A mock account summary with test data.
    """
    return BackpackRawAccountSummaryResponse.model_validate({
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
def mock_raw_balance() -> BackpackRawBalanceResponse:
    """Create a mock raw balance.

    Returns:
        BackpackRawBalanceResponse: A mock balance response with test data.
    """
    return BackpackRawBalanceResponse(
        available="1000.00",
        locked="100.00",
        staked="0.00",
    )


@pytest.fixture
def mock_raw_position() -> BackpackRawPositionResponse:
    """Create a mock raw position.

    Returns:
        BackpackRawPositionResponse: A mock position for BTC-PERP.
    """
    return BackpackRawPositionResponse.model_validate({
        "symbol": BTC_BP.value,
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
    """Create a mock collateral response.

    Returns:
        BackpackRawCollateralResponse: A mock collateral response with USDC data.
    """
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
    """Create a mock margin account summary.

    Returns:
        MarginAccountSummary: A mock margin account summary with test values.
    """
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
    """Create mock account settings.

    Returns:
        AccountSettings: Mock account settings with test configuration.
    """
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
        mock_account_state_service: AsyncMock,
        mock_raw_account_summary: BackpackRawAccountSummaryResponse,
        mock_collateral_response: BackpackRawCollateralResponse,
        mock_raw_position: BackpackRawPositionResponse,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test successful enhanced account summary retrieval."""
        # Arrange
        # Mock the HTTP calls for account settings and positions
        mock_http_client.side_effect = [
            # Account settings response
            (mock_raw_account_summary.model_dump(), 200, {}),
            # Positions response
            ([mock_raw_position.model_dump()], 200, {}),
        ]

        # Mock the account state service to return collateral data
        mock_account_state_service.get_account_state.return_value = mock_collateral_response

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
        assert mock_http_client.call_count == 2
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
        mock_account_state_service: AsyncMock,
        mock_raw_account_summary: BackpackRawAccountSummaryResponse,
        mock_raw_balance: BackpackRawBalanceResponse,
        mock_raw_position: BackpackRawPositionResponse,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test account summary falls back to basic when collateral unavailable."""
        # Arrange
        # Configure account state service to fail (triggering fallback)
        mock_account_state_service.get_account_state.side_effect = APIError(
            message="Not found",
            code=APIErrorCode.ORDER_NOT_FOUND.value,
            http_status=404,
        )

        # HTTP calls for basic mode after collateral fails
        mock_http_client.side_effect = [
            (mock_raw_account_summary.model_dump(), 200, {}),
            ([mock_raw_position.model_dump()], 200, {}),
            ({"USDC": mock_raw_balance.model_dump()}, 200, {}),
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
        assert mock_http_client.call_count == 3  # 3 basic calls after account state service fails
        mock_mapper.transform_raw_account_summary_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_account_summary_with_subaccount(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_account_state_service: AsyncMock,
        mock_collateral_response: BackpackRawCollateralResponse,
        mock_raw_account_summary: BackpackRawAccountSummaryResponse,
        mock_raw_position: BackpackRawPositionResponse,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test account summary with subaccount ID."""
        # Arrange
        subaccount_id = 123

        # Mock the HTTP calls for account settings and positions
        mock_http_client.side_effect = [
            # Account settings response
            (mock_raw_account_summary.model_dump(), 200, {}),
            # Positions response
            ([mock_raw_position.model_dump()], 200, {}),
        ]

        # Mock the account state service to return collateral data
        mock_account_state_service.get_account_state.return_value = mock_collateral_response

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
        # Verify subaccount ID was passed to account state service
        mock_account_state_service.get_account_state.assert_called_once_with(subaccount_id)

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

        # The business logic calls with leverage (int) and account_settings (domain object)
        expected_account_settings = DomainAccountSettings(
            leverage_limit=5,  # int conversion of Decimal("5.00")
            automation_policy=AccountSettingsPolicy.MANUAL_CONTROL,
        )
        mock_request_builder.build_update_account_settings_payload.assert_called_once_with(
            leverage=5,
            account_settings=expected_account_settings,
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
        mock_account_state_service: AsyncMock,
        mock_collateral_response: BackpackRawCollateralResponse,
        mock_raw_account_summary: BackpackRawAccountSummaryResponse,
        mock_raw_position: BackpackRawPositionResponse,
        mock_raw_balance: BackpackRawBalanceResponse,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test enhanced account info with transformation error falls back to basic mode."""
        # Arrange
        # Mock the HTTP calls for account settings, positions, and balances (for fallback)
        mock_http_client.side_effect = [
            # Account settings response
            (mock_raw_account_summary.model_dump(), 200, {}),
            # Positions response
            ([mock_raw_position.model_dump()], 200, {}),
            # Balances response (fallback)
            ({"USDC": mock_raw_balance.model_dump()}, 200, {}),
        ]

        # Mock account state service to return collateral data
        mock_account_state_service.get_account_state.return_value = mock_collateral_response

        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )
        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]
        mock_response_handler.handle_get_balances_response.return_value = {"USDC": mock_raw_balance}

        # Set up the mapper to throw TransformationError for enhanced data
        mock_mapper.transform_enhanced_account_data_to_margin_summary.side_effect = (
            TransformationError("Invalid data format")
        )
        mock_mapper.transform_raw_account_summary_to_internal.return_value = (
            mock_margin_account_summary
        )

        # Act
        result = await account_summary_service.get_account_summary()

        # Assert - should successfully fallback to basic mode
        assert result == mock_margin_account_summary
        # Enhanced transformation should be attempted first
        mock_mapper.transform_enhanced_account_data_to_margin_summary.assert_called_once()
        # Then fallback to basic transformation
        mock_mapper.transform_raw_account_summary_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_basic_account_info_empty_positions(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_account_state_service: AsyncMock,
        mock_raw_account_summary: BackpackRawAccountSummaryResponse,
        mock_raw_balance: BackpackRawBalanceResponse,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test basic account info with empty positions."""
        # Arrange
        # Force fallback to basic mode by making account state service fail
        mock_account_state_service.get_account_state.side_effect = APIError(
            message="Not found", code=APIErrorCode.ORDER_NOT_FOUND.value, http_status=404
        )

        # Basic mode HTTP calls
        mock_http_client.side_effect = [
            (mock_raw_account_summary.model_dump(), 200, {}),
            ([], 200, {}),  # Empty positions
            ({"USDC": mock_raw_balance.model_dump()}, 200, {}),
        ]

        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )
        mock_response_handler.handle_get_positions_response.return_value = []
        mock_response_handler.handle_get_balances_response.return_value = {"USDC": mock_raw_balance}

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
    async def test_get_account_summary_collateral_empty_response(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_account_state_service: AsyncMock,
        mock_raw_account_summary: BackpackRawAccountSummaryResponse,
        mock_raw_balance: BackpackRawBalanceResponse,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test account summary when collateral fetch returns empty response."""
        # Arrange
        # Mock account state service to raise EmptyResponseError for collateral request
        mock_account_state_service.get_account_state.side_effect = EmptyResponseError(
            response_type="data",
            operation="collateral request",
            http_status=200,
            exchange="backpack",
        )

        # Set up HTTP calls for basic mode fallback
        mock_http_client.side_effect = [
            (mock_raw_account_summary.model_dump(), 200, {}),
            ([], 200, {}),  # Empty positions
            ({"USDC": mock_raw_balance.model_dump()}, 200, {}),  # Balances
        ]

        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )
        mock_response_handler.handle_get_positions_response.return_value = []
        mock_response_handler.handle_get_balances_response.return_value = {"USDC": mock_raw_balance}

        mock_mapper.transform_raw_account_summary_to_internal.return_value = (
            mock_margin_account_summary
        )

        # Act
        result = await account_summary_service.get_account_summary()

        # Assert - should successfully fallback to basic mode when collateral is empty
        assert result == mock_margin_account_summary
        mock_mapper.transform_raw_account_summary_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_account_summary_positions_dict_response(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_account_state_service: AsyncMock,
        mock_raw_position: BackpackRawPositionResponse,
        mock_collateral_response: BackpackRawCollateralResponse,
        mock_raw_account_summary: BackpackRawAccountSummaryResponse,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test account summary when positions API returns dict response (instead of list)."""
        # Arrange
        dict_response = {BTC_BP.value: mock_raw_position.model_dump()}

        # Mock the HTTP calls for account settings and positions
        mock_http_client.side_effect = [
            # Account settings response
            (mock_raw_account_summary.model_dump(), 200, {}),
            # Positions response (dict instead of list)
            (dict_response, 200, {}),
        ]

        # Mock account state service to return collateral data
        mock_account_state_service.get_account_state.return_value = mock_collateral_response

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
        # Verify positions handler was called with dict response
        mock_response_handler.handle_get_positions_response.assert_called_once_with(
            dict_response,
            None,
            200,
        )

    @pytest.mark.asyncio
    async def test_get_account_summary_positions_404_handled(
        self,
        account_summary_service: BackpackAccountSummaryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_account_state_service: AsyncMock,
        mock_collateral_response: BackpackRawCollateralResponse,
        mock_raw_account_summary: BackpackRawAccountSummaryResponse,
        mock_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test account summary when positions fetch returns 404 (should handle gracefully)."""
        # Arrange
        # Mock the HTTP calls in the order they're made by the service
        mock_http_client.side_effect = [
            # Account settings response
            (mock_raw_account_summary.model_dump(), 200, {}),
            # Positions response - 404
            APIError(
                message="Not found",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=404,
            ),
        ]

        # Mock account state service for collateral data
        mock_account_state_service.get_account_state.return_value = mock_collateral_response

        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )

        # Configure mapper to create summary with empty positions
        mock_mapper.transform_enhanced_account_data_to_margin_summary.return_value = (
            mock_margin_account_summary
        )

        # Act
        result = await account_summary_service.get_account_summary()

        # Assert
        assert result == mock_margin_account_summary
        # Verify mapper was called with empty positions list (due to 404)
        mock_mapper.transform_enhanced_account_data_to_margin_summary.assert_called_once_with(
            raw_collateral=mock_collateral_response,
            raw_settings=mock_raw_account_summary,
            raw_positions=[],  # Empty positions due to 404
        )

    @pytest.mark.asyncio
    async def test_get_account_summary_without_authenticator(
        self,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_account_summary: BackpackRawAccountSummaryResponse,
    ) -> None:
        """Test account summary when authenticator is None."""
        # Arrange
        # Create a mock account state service that will fail
        mock_account_state_service = AsyncMock()
        mock_account_state_service.get_account_state.side_effect = APIError(
            message="Authentication required",
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
            http_status=401,
        )

        service = BackpackAccountSummaryService(
            http_client_requester=mock_http_client,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            mapper=mock_mapper,
            authenticator=None,  # No authenticator
            exchange_name="backpack",
            account_state_service=mock_account_state_service,
        )

        # Mock successful account settings and empty positions calls
        mock_http_client.side_effect = [
            # Account settings response
            (mock_raw_account_summary.model_dump(), 200, {}),
            # Positions response - succeeds with empty list
            ([], 200, {}),
        ]

        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_raw_account_summary
        )
        mock_response_handler.handle_get_positions_response.return_value = []

        # Act & Assert - Should fail when trying to get balances in fallback mode
        with pytest.raises(APIError) as exc_info:
            await service.get_account_summary()

        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert "Authentication required" in exc_info.value.message
