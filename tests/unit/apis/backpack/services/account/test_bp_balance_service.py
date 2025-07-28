"""Unit tests for Backpack Balance Service.

Tests cover all methods of the BackpackBalanceService including:
- Balance retrieval and processing
- Collateral information integration
- Auto-lending scenario handling
- Error handling and validation
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_collateral import (
    BackpackRawCollateralResponse,
)
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.services.account.bp_balance_service import (
    BackpackBalanceService,
)
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.core.models import SpotBalance
from cyberdelta.core.models.spot_balance import BackpackSpotBalanceDetails


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
        MagicMock: A mock instance of BackpackBalanceMapper.
    """
    return MagicMock(spec=BackpackBalanceMapper)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Create a mock authenticator.
    
    Returns:
        MagicMock: A mock instance of authenticator.
    """
    return MagicMock()


@pytest.fixture
def balance_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_mapper: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackBalanceService:
    """Create a balance service instance with mocks.
    
    Returns:
        BackpackBalanceService: Service instance configured with mock dependencies.
    """
    return BackpackBalanceService(
        http_client_requester=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        authenticator=mock_authenticator,
        exchange_name="backpack",
    )


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
def mock_spot_balance() -> SpotBalance:
    """Create a mock spot balance.
    
    Returns:
        SpotBalance: A mock spot balance for USDC.
    """
    return SpotBalance(
        asset="USDC",
        total_quantity=Decimal("1100.00"),
        available_quantity=Decimal("1000.00"),
        exchange="backpack",
        timestamp=datetime.now(UTC),
        bp_details=BackpackSpotBalanceDetails(
            open_order_quantity=Decimal("100.00"),
            lend_quantity=Decimal("0.00"),
            collateral_weight=Decimal("1.0"),
        ),
    )


@pytest.fixture
def mock_collateral_response() -> BackpackRawCollateralResponse:
    """Create a mock collateral response.
    
    Returns:
        BackpackRawCollateralResponse: A mock collateral response with USDC data.
    """
    return BackpackRawCollateralResponse.model_validate({
        "netEquity": "10000.00",
        "netEquityAvailable": "9000.00",
        "netEquityLocked": "1000.00",
        "assetsValue": "10000.00",
        "liabilitiesValue": "0.00",
        "imf": "0.10",
        "mmf": "0.05",
        "marginFraction": None,
        "borrowLiability": "0.00",
        "pnlUnrealized": "0.00",
        "unsettledEquity": "0.00",
        "netExposureFutures": "0.00",
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


class TestBackpackBalanceService:
    """Test suite for BackpackBalanceService."""

    @pytest.mark.asyncio
    async def test_get_balances_success(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_balance: BackpackRawBalanceResponse,
        mock_spot_balance: SpotBalance,
    ) -> None:
        """Test successful balance retrieval."""
        # Arrange
        # Balances endpoint returns a dict where keys are assets
        raw_balances_response = {"USDC": mock_raw_balance.model_dump()}
        # Collateral response for enhancement
        raw_collateral_response: dict[str, list[Any]] = {"collateral": []}

        # Service makes 2 calls: balances and collateral
        mock_http_client.side_effect = [
            (raw_balances_response, 200, {}),
            (raw_collateral_response, 200, {}),
        ]

        # Response handler returns dict of asset -> BackpackRawBalanceResponse
        validated_balances = {"USDC": mock_raw_balance}
        mock_response_handler.handle_get_balances_response.return_value = validated_balances

        mock_mapper.transform_raw_balance_to_internal.return_value = mock_spot_balance

        # Act
        result = await balance_service.get_balances()

        # Assert
        assert "USDC" in result
        assert result["USDC"] == mock_spot_balance
        assert mock_http_client.call_count == 2  # Balances + collateral
        mock_response_handler.handle_get_balances_response.assert_called_once()
        mock_mapper.transform_raw_balance_to_internal.assert_called()

    @pytest.mark.asyncio
    async def test_get_balances_with_collateral_enhancement(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_balance: BackpackRawBalanceResponse,
        mock_spot_balance: SpotBalance,
        mock_collateral_response: BackpackRawCollateralResponse,
    ) -> None:
        """Test balance retrieval with collateral enhancement."""
        # Arrange
        # First call for balances - dict format
        raw_balances_response = {"USDC": mock_raw_balance.model_dump()}
        # Second call for collateral
        raw_collateral_response = mock_collateral_response.model_dump()

        mock_http_client.side_effect = [
            (raw_balances_response, 200, {}),
            (raw_collateral_response, 200, {}),
        ]

        validated_balances = {"USDC": mock_raw_balance}
        mock_response_handler.handle_get_balances_response.return_value = validated_balances
        mock_response_handler.handle_get_collateral_response.return_value = mock_collateral_response

        # Enhanced balance with collateral info
        enhanced_balance = SpotBalance(
            asset="USDC",
            total_quantity=Decimal("2100.00"),  # From collateral
            available_quantity=Decimal("2000.00"),  # From collateral
            exchange="backpack",
            timestamp=datetime.now(UTC),
            bp_details=BackpackSpotBalanceDetails(
                open_order_quantity=Decimal("100.00"),
                lend_quantity=Decimal("0.00"),
                collateral_weight=Decimal("1.00"),
            ),
        )

        mock_mapper.transform_raw_balance_to_internal.side_effect = [
            mock_spot_balance,
            enhanced_balance,
        ]

        # Act
        result = await balance_service.get_balances()

        # Assert
        assert "USDC" in result
        assert result["USDC"].total_quantity == Decimal("2100.00")
        assert mock_http_client.call_count == 2
        mock_response_handler.handle_get_collateral_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_balances_empty_response(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test balance retrieval with empty response."""
        # Arrange - empty dict response
        # Service makes 2 calls: balances and collateral
        mock_http_client.side_effect = [
            ({}, 200, {}),  # Empty balances
            ({"collateral": []}, 200, {}),  # Empty collateral
        ]
        mock_response_handler.handle_get_balances_response.return_value = {}

        # Act
        result = await balance_service.get_balances()

        # Assert
        assert result == {}

    @pytest.mark.asyncio
    async def test_get_balances_http_error(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
    ) -> None:
        """Test balance retrieval with HTTP error."""
        # Arrange
        # The http client should raise APIError for network issues
        error = APIError(
            message="Network error",
            code=APIErrorCode.NETWORK_ISSUE.value,
        )
        mock_http_client.side_effect = error

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await balance_service.get_balances()

        # Error should propagate through
        assert exc_info.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert "Network error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_balances_validation_error(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test balance retrieval with response validation error."""
        # Arrange - dict format with invalid data
        mock_http_client.return_value = ({"INVALID": {"invalid": "data"}}, 200, {})

        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                "validation_error",
                [{"type": "missing", "loc": ("symbol",), "input": {}}],
            )
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await balance_service.get_balances()

        # Business logic converts ValidationError to UNKNOWN code
        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve balance data" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_balances_transformation_error(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_balance: BackpackRawBalanceResponse,
    ) -> None:
        """Test balance retrieval with transformation error."""
        # Arrange - dict format
        raw_balances_response = {"USDC": mock_raw_balance.model_dump()}
        mock_http_client.return_value = (raw_balances_response, 200, {})

        validated_balances = {"USDC": mock_raw_balance}
        mock_response_handler.handle_get_balances_response.return_value = validated_balances

        mock_mapper.transform_raw_balance_to_internal.side_effect = APIError(
            message="Transformation failed",
            code=APIErrorCode.TRANSFORMATION_FAILED.value,
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await balance_service.get_balances()

        assert exc_info.value.code == APIErrorCode.TRANSFORMATION_FAILED.value

    @pytest.mark.asyncio
    async def test_get_balances_collateral_validation_error_continues(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_balance: BackpackRawBalanceResponse,
        mock_spot_balance: SpotBalance,
    ) -> None:
        """Test that balance retrieval continues when collateral data validation fails."""
        # Arrange - dict format
        raw_balances_response = {"USDC": mock_raw_balance.model_dump()}

        # Mock the balance call to succeed
        mock_http_client.return_value = (raw_balances_response, 200, {})

        validated_balances = {"USDC": mock_raw_balance}
        mock_response_handler.handle_get_balances_response.return_value = validated_balances

        mock_mapper.transform_raw_balance_to_internal.return_value = mock_spot_balance

        # Mock the HTTP collateral request to fail with validation error to trigger error handling
        # We'll mock the collateral endpoint call to fail, which will be caught by error handling
        original_return_value = mock_http_client.return_value

        def http_side_effect(
            *args: str, **kwargs: dict[str, Any]
        ) -> tuple[dict[str, Any], int, dict[str, Any]]:
            # Check if this is a collateral request
            if len(args) > 1 and "/collateral" in str(args[1]):
                raise ValidationError.from_exception_data(
                    "validation_error", [{"type": "missing", "loc": ("collateral",), "input": {}}]
                )
            return original_return_value  # type: ignore[no-any-return]

        mock_http_client.side_effect = http_side_effect

        # Act
        result = await balance_service.get_balances()

        # Assert - Should still return balances without collateral enhancement
        assert "USDC" in result
        assert result["USDC"] == mock_spot_balance
        # Two calls: balances call succeeds, collateral call fails but is handled gracefully
        assert mock_http_client.call_count == 2

    @pytest.mark.asyncio
    async def test_handle_auto_lending_scenario(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_collateral_response: BackpackRawCollateralResponse,
    ) -> None:
        """Test auto-lending scenario where all balances are zero."""
        # Arrange
        # Create a zero balance to trigger auto-lending detection
        zero_balance = BackpackRawBalanceResponse(
            available="0.00",
            locked="0.00",
            staked="0.00",
        )

        # Balances API returns USDC with zero balance
        raw_balances_response = {"USDC": zero_balance.model_dump()}
        mock_http_client.return_value = (raw_balances_response, 200, {})

        validated_balances = {"USDC": zero_balance}
        mock_response_handler.handle_get_balances_response.return_value = validated_balances

        # First transformation returns zero balance
        zero_spot_balance = SpotBalance(
            asset="USDC",
            total_quantity=Decimal("0.00"),
            available_quantity=Decimal("0.00"),
            exchange="backpack",
            timestamp=datetime.now(UTC),
            bp_details=BackpackSpotBalanceDetails(
                open_order_quantity=Decimal("0.00"),
                lend_quantity=Decimal("0.00"),
                collateral_weight=Decimal("1.0"),
            ),
        )
        mock_mapper.transform_raw_balance_to_internal.return_value = zero_spot_balance

        # Create balance from collateral (from shared state)
        auto_lend_balance = SpotBalance(
            asset="USDC",
            total_quantity=Decimal("2100.00"),
            available_quantity=Decimal("2000.00"),
            exchange="backpack",
            timestamp=datetime.now(UTC),
            bp_details=BackpackSpotBalanceDetails(
                open_order_quantity=Decimal("100.00"),
                lend_quantity=Decimal("0.00"),
                collateral_weight=Decimal("1.00"),
            ),
        )
        mock_mapper.create_balance_from_collateral.return_value = auto_lend_balance

        # Mock the HTTP client to return both balances and collateral data
        def http_side_effect(
            *args: str, **kwargs: dict[str, Any]
        ) -> tuple[dict[str, Any], int, dict[str, Any]]:
            # Check if this is a collateral request
            if len(args) > 1 and "/collateral" in str(args[1]):
                return (mock_collateral_response.model_dump(), 200, {})
            # Otherwise return balance data
            return (raw_balances_response, 200, {})

        mock_http_client.side_effect = http_side_effect

        # Mock response handler to handle collateral response
        mock_response_handler.handle_get_collateral_response.return_value = mock_collateral_response

        # Act
        result = await balance_service.get_balances()

        # Assert
        assert "USDC" in result
        assert result["USDC"].total_quantity == Decimal("2100.00")
        mock_mapper.create_balance_from_collateral.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_balances_includes_zero_balances(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test that zero balances are included in the result."""
        # Arrange
        zero_balance = BackpackRawBalanceResponse(
            available="0.00",
            locked="0.00",
            staked="0.00",
        )

        non_zero_balance = BackpackRawBalanceResponse(
            available="100.00",
            locked="0.00",
            staked="0.00",
        )

        # Dict format - BTC has zero balance, USDC has non-zero
        raw_balances_response = {
            "BTC": zero_balance.model_dump(),
            "USDC": non_zero_balance.model_dump(),
        }
        # Collateral response for enhancement
        raw_collateral_response: dict[str, list[Any]] = {"collateral": []}

        mock_http_client.side_effect = [
            (raw_balances_response, 200, {}),
            (raw_collateral_response, 200, {}),
        ]

        validated_balances = {"BTC": zero_balance, "USDC": non_zero_balance}
        mock_response_handler.handle_get_balances_response.return_value = validated_balances

        # Transform both balances (service will filter out zero later)
        btc_balance = SpotBalance(
            asset="BTC",
            total_quantity=Decimal("0.00"),
            available_quantity=Decimal("0.00"),
            exchange="backpack",
            timestamp=datetime.now(UTC),
            bp_details=BackpackSpotBalanceDetails(
                open_order_quantity=Decimal("0.00"),
                lend_quantity=Decimal("0.00"),
                collateral_weight=Decimal("1.0"),
            ),
        )

        usdc_balance = SpotBalance(
            asset="USDC",
            total_quantity=Decimal("100.00"),
            available_quantity=Decimal("100.00"),
            exchange="backpack",
            timestamp=datetime.now(UTC),
            bp_details=BackpackSpotBalanceDetails(
                open_order_quantity=Decimal("0.00"),
                lend_quantity=Decimal("0.00"),
                collateral_weight=Decimal("1.0"),
            ),
        )

        # Mock mapper to return appropriate balance for each asset
        def side_effect(asset: str, balance: dict[str, Any]) -> SpotBalance:
            if asset == "BTC":
                return btc_balance
            return usdc_balance

        mock_mapper.transform_raw_balance_to_internal.side_effect = side_effect

        # Act
        result = await balance_service.get_balances()

        # Assert
        assert "USDC" in result
        assert "BTC" in result  # Zero balance is included, not filtered
        assert result["BTC"].total_quantity == Decimal("0.00")
        assert result["USDC"].total_quantity == Decimal("100.00")
        # Mapper should be called twice (for both assets)
        assert mock_mapper.transform_raw_balance_to_internal.call_count == 2

    @pytest.mark.asyncio
    async def test_get_balances_none_response(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
    ) -> None:
        """Test balance retrieval with None response."""
        # Arrange
        mock_http_client.return_value = (None, 200, {})

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await balance_service.get_balances()

        # ensure_dict_response converts None to APIError with INVALID_RESPONSE code
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for balances" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_balances_with_authenticator_none(
        self,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_balance: BackpackRawBalanceResponse,
        mock_spot_balance: SpotBalance,
    ) -> None:
        """Test balance retrieval when authenticator is None."""
        # Arrange
        balance_service = BackpackBalanceService(
            http_client_requester=mock_http_client,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            mapper=mock_mapper,
            authenticator=None,  # No authenticator
            exchange_name="backpack",
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await balance_service.get_balances()

        # Business logic checks for authenticator before making API calls
        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert "Authentication required" in str(exc_info.value)
