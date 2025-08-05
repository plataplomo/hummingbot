"""Unit tests for Backpack Position Service.

Tests cover all methods of the BackpackPositionService including:
- Position retrieval and processing
- Collateral data integration
- Position validation and filtering
- Error handling
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers.account.bp_position_mapper import BackpackPositionMapper
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
from cyberdelta.apis.backpack.services.account.bp_position_service import (
    BackpackPositionService,
)
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.models import DerivativePosition
from cyberdelta.models.derivative_position import (
    BackpackPositionDetails as BackpackDerivativePositionDetails,
)
from tests.common_symbols import BTC_BP, ETH_BP


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Create a mock HTTP client requester.

    Returns:
        AsyncMock: Mock HTTP client for testing.
    """
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Create a mock request builder.

    Returns:
        MagicMock: Mock BackpackAccountRequestBuilder instance for testing.
    """
    return MagicMock(spec=BackpackAccountRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Create a mock response handler.

    Returns:
        MagicMock: Mock BackpackAccountResponseHandler instance for testing.
    """
    return MagicMock(spec=BackpackAccountResponseHandler)


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Create a mock data mapper.

    Returns:
        MagicMock: Mock BackpackPositionMapper instance for testing.
    """
    return MagicMock(spec=BackpackPositionMapper)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Create a mock authenticator.

    Returns:
        MagicMock: Mock authenticator instance for testing.
    """
    return MagicMock()


@pytest.fixture
def position_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_mapper: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackPositionService:
    """Create a position service instance with mocks.

    Returns:
        BackpackPositionService: Configured service instance with mocked dependencies.
    """
    return BackpackPositionService(
        http_client_requester=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        authenticator=mock_authenticator,
        exchange_name="backpack",
    )


@pytest.fixture
def mock_raw_position() -> BackpackRawPositionResponse:
    """Create a mock raw position.

    Returns:
        BackpackRawPositionResponse: Mock position response with test data.
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
def mock_derivative_position() -> DerivativePosition:
    """Create a mock derivative position.

    Returns:
        DerivativePosition: Mock DerivativePosition instance with test data.
    """
    return DerivativePosition(
        symbol=BTC_BP,
        side=OrderSide.BUY,
        size=Decimal("1.5"),
        entry_price=Decimal("50000.00"),
        mark_price=Decimal("50666.67"),
        unrealized_pnl=Decimal("1000.00"),
        realized_pnl=Decimal("500.00"),
        liquidation_price=Decimal("45000.00"),
        exchange=ExchangeName.BACKPACK,
        timestamp=datetime.now(UTC),
        bp_details=BackpackDerivativePositionDetails(),
    )


@pytest.fixture
def mock_collateral_response() -> BackpackRawCollateralResponse:
    """Create a mock collateral response with derivative positions.

    Returns:
        BackpackRawCollateralResponse: Mock collateral response with test data.
    """
    return BackpackRawCollateralResponse.model_validate({
        "netEquity": "100000.00",
        "netEquityAvailable": "92500.00",
        "netEquityLocked": "7500.00",
        "assetsValue": "100000.00",
        "liabilitiesValue": "0.00",
        "imf": "0.10",
        "mmf": "0.05",
        "marginFraction": "0.075",
        "borrowLiability": "0.00",
        "pnlUnrealized": "1000.00",
        "unsettledEquity": "0.00",
        "netExposureFutures": "75000.00",
        "collateral": [
            {
                "symbol": BTC_BP.value,
                "assetMarkPrice": "50666.67",
                "totalQuantity": "1.5",
                "balanceNotional": "75000.00",
                "collateralWeight": "0.90",
                "collateralValue": "67500.00",
                "openOrderQuantity": "0.00",
                "lendQuantity": "0.00",
                "availableQuantity": "1.5",
            },
        ],
    })


class TestBackpackPositionService:
    """Test suite for BackpackPositionService."""

    @pytest.mark.asyncio
    async def test_get_positions_success(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_position: BackpackRawPositionResponse,
        mock_derivative_position: DerivativePosition,
    ) -> None:
        """Test successful position retrieval."""
        # Arrange
        raw_positions_response = [mock_raw_position.model_dump()]
        mock_http_client.return_value = (raw_positions_response, 200, {})

        validated_positions = [mock_raw_position]
        mock_response_handler.handle_get_positions_response.return_value = validated_positions

        mock_mapper.transform_raw_position_to_internal.return_value = mock_derivative_position

        # Act
        result = await position_service.get_positions()

        # Assert
        assert len(result) == 1
        assert result[0] == mock_derivative_position
        mock_http_client.assert_called_once()
        mock_response_handler.handle_get_positions_response.assert_called_once()
        mock_mapper.transform_raw_position_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_positions_empty_response(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test position retrieval with empty response."""
        # Arrange
        # Empty positions response
        mock_http_client.return_value = ([], 200, {})

        mock_response_handler.handle_get_positions_response.return_value = []

        # Act
        result = await position_service.get_positions()

        # Assert
        assert len(result) == 0
        assert result == []
        mock_http_client.assert_called_once()
        mock_response_handler.handle_get_positions_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_positions_by_symbol(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_derivative_position: DerivativePosition,
    ) -> None:
        """Test position retrieval filtered by symbol."""
        # Arrange
        btc_position = mock_derivative_position

        # Create actual BackpackRawPositionResponse objects with correct symbols
        btc_raw_position = BackpackRawPositionResponse.model_validate({
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

        eth_raw_position = BackpackRawPositionResponse.model_validate({
            "symbol": ETH_BP.value,
            "netQuantity": "-10.0",
            "entryPrice": "3500.00",
            "netExposureNotional": "35000.00",
            "pnlUnrealized": "500.00",
            "pnlRealized": "0.00",
            "estLiquidationPrice": "3800.00",
            "markPrice": "3450.00",
            "breakEvenPrice": "3500.00",
            "imf": "0.05",
            "imfFunction": {"base": "0.01", "factor": "0.04"},
            "mmf": "0.025",
            "mmfFunction": {"base": "0.005", "factor": "0.02"},
            "netCost": "35000.00",
            "netExposureQuantity": "-10.0",
            "cumulativeFundingPayment": "0.00",
            "userId": 123,
            "positionId": "pos_456",
            "subaccountId": 0,
            "cumulativeInterest": "0.00",
        })

        raw_positions = [
            btc_raw_position.model_dump(),
            eth_raw_position.model_dump(),
        ]
        mock_http_client.return_value = (raw_positions, 200, {})

        # Response handler returns BackpackRawPositionResponse objects with correct symbols
        mock_response_handler.handle_get_positions_response.return_value = [
            btc_raw_position,
            eth_raw_position,
        ]

        # Mapper transforms only the BTC position since it's the only one that passes the filter
        mock_mapper.transform_raw_position_to_internal.return_value = btc_position

        # Act
        result = await position_service.get_positions(symbol=BTC_BP)

        # Assert
        assert len(result) == 1
        assert result[0].symbol == BTC_BP
        # Only the BTC position should be transformed since the filter runs before transformation
        mock_mapper.transform_raw_position_to_internal.assert_called_once_with(btc_raw_position)

    @pytest.mark.asyncio
    async def test_get_positions_http_error(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
    ) -> None:
        """Test position retrieval with HTTP error."""
        # Arrange
        # The http client should raise APIError for network issues
        error = APIError(
            message="Network error",
            code=APIErrorCode.NETWORK_ISSUE.value,
        )
        mock_http_client.side_effect = error

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await position_service.get_positions()

        # Error should propagate through
        assert exc_info.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert "Network error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_positions_validation_error(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test position retrieval with response validation error."""
        # Arrange
        mock_http_client.return_value = ([{"invalid": "data"}], 200, {})

        mock_response_handler.handle_get_positions_response.side_effect = (
            ValidationError.from_exception_data(
                "validation_error",
                [{"type": "missing", "loc": ("symbol",), "input": {}}],
            )
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await position_service.get_positions()

        # Business logic converts ValidationError to UNKNOWN code
        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve position data" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_positions_transformation_error(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_position: BackpackRawPositionResponse,
    ) -> None:
        """Test position retrieval with transformation error."""
        # Arrange
        raw_positions_response = [mock_raw_position.model_dump()]
        mock_http_client.return_value = (raw_positions_response, 200, {})

        validated_positions = [mock_raw_position]
        mock_response_handler.handle_get_positions_response.return_value = validated_positions

        mock_mapper.transform_raw_position_to_internal.side_effect = APIError(
            message="Transformation failed",
            code=APIErrorCode.TRANSFORMATION_FAILED.value,
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await position_service.get_positions()

        assert exc_info.value.code == APIErrorCode.TRANSFORMATION_FAILED.value

    @pytest.mark.asyncio
    async def test_get_positions_collateral_error_returns_direct_positions(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_position: BackpackRawPositionResponse,
        mock_derivative_position: DerivativePosition,
    ) -> None:
        """Test that direct positions are returned even if collateral fetch fails."""
        # Arrange
        raw_positions_response = [mock_raw_position.model_dump()]

        # First call succeeds, collateral call fails
        mock_http_client.side_effect = [
            (raw_positions_response, 200, {}),
            Exception("Collateral fetch failed"),
        ]

        validated_positions = [mock_raw_position]
        mock_response_handler.handle_get_positions_response.return_value = validated_positions

        mock_mapper.transform_raw_position_to_internal.return_value = mock_derivative_position

        # Act
        result = await position_service.get_positions()

        # Assert - Should still return positions from direct API
        assert len(result) == 1
        assert result[0] == mock_derivative_position

    @pytest.mark.asyncio
    async def test_get_positions_direct_api_only(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_position: BackpackRawPositionResponse,
    ) -> None:
        """Test that positions are retrieved from direct API only."""
        # Arrange
        btc_position = DerivativePosition(
            symbol=BTC_BP,
            side=OrderSide.BUY,
            size=Decimal("1.5"),
            entry_price=Decimal("50000.00"),
            mark_price=Decimal("50666.67"),
            unrealized_pnl=Decimal("1000.00"),
            realized_pnl=Decimal("500.00"),
            liquidation_price=Decimal("45000.00"),
            exchange=ExchangeName.BACKPACK,
            timestamp=datetime.now(UTC),
            bp_details=BackpackDerivativePositionDetails(),
        )

        # Position service only uses direct API, not collateral
        raw_positions_response = [mock_raw_position.model_dump()]
        mock_http_client.return_value = (raw_positions_response, 200, {})

        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]
        mock_mapper.transform_raw_position_to_internal.return_value = btc_position

        # Act
        result = await position_service.get_positions()

        # Assert
        assert len(result) == 1
        assert result[0].symbol == BTC_BP
        assert result[0] == btc_position

        # Service should only call direct API, not collateral
        mock_http_client.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_positions_deduplicates_by_symbol(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_position: BackpackRawPositionResponse,
        mock_derivative_position: DerivativePosition,
    ) -> None:
        """Test that duplicate positions are deduplicated by symbol."""
        # Arrange
        # Same position from both sources
        raw_positions_response = [mock_raw_position.model_dump()]

        mock_http_client.side_effect = [
            (raw_positions_response, 200, {}),
            ({"collaterals": []}, 200, {}),  # Empty collateral
        ]

        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]
        mock_response_handler.handle_get_collateral_response.return_value = (
            BackpackRawCollateralResponse.model_validate({
                "netEquity": "100000.00",
                "netEquityAvailable": "100000.00",
                "netEquityLocked": "0.00",
                "assetsValue": "100000.00",
                "liabilitiesValue": "0.00",
                "imf": "0.10",
                "mmf": "0.05",
                "marginFraction": None,
                "borrowLiability": "0.00",
                "pnlUnrealized": "0.00",
                "unsettledEquity": "0.00",
                "netExposureFutures": "0.00",
                "collateral": [],
            })
        )

        mock_mapper.transform_raw_position_to_internal.return_value = mock_derivative_position

        # Act
        result = await position_service.get_positions()

        # Assert - Should only have one position
        assert len(result) == 1
        assert result[0].symbol == BTC_BP
