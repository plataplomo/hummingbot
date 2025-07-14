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

from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
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
from cyberdelta.apis.backpack.services.account.bp_position_service import (
    BackpackPositionService,
)
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.core.models import DerivativePosition, OrderSide
from cyberdelta.core.models.derivative_position import (
    BackpackPositionDetails as BackpackDerivativePositionDetails,
)


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
def position_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_mapper: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackPositionService:
    """Create a position service instance with mocks."""
    return BackpackPositionService(
        http_client_requester=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        authenticator=mock_authenticator,
        exchange_name="backpack",
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
def mock_derivative_position() -> DerivativePosition:
    """Create a mock derivative position."""
    return DerivativePosition(
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        size=Decimal("1.5"),
        entry_price=Decimal("50000.00"),
        mark_price=Decimal("50666.67"),
        unrealized_pnl=Decimal("1000.00"),
        realized_pnl=Decimal("500.00"),
        liquidation_price=Decimal("45000.00"),
        exchange="backpack",
        timestamp=datetime.now(UTC),
        bp_details=BackpackDerivativePositionDetails(),
    )


@pytest.fixture
def mock_collateral_response() -> BackpackRawCollateralResponse:
    """Create a mock collateral response with derivative positions."""
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
                "symbol": "BTC-PERP",
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
        mock_raw_position: BackpackRawPosition,
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
    async def test_get_positions_from_collateral(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_collateral_response: BackpackRawCollateralResponse,
        mock_derivative_position: DerivativePosition,
    ) -> None:
        """Test position retrieval from collateral API."""
        # Arrange
        # Empty positions response, then collateral response
        mock_http_client.side_effect = [
            ([], 200, {}),  # Empty positions
            (mock_collateral_response.model_dump(), 200, {}),  # Collateral with positions
        ]

        mock_response_handler.handle_get_positions_response.return_value = []
        mock_response_handler.handle_get_collateral_response.return_value = mock_collateral_response

        mock_mapper.transform_raw_position_to_internal.return_value = mock_derivative_position

        # Act
        result = await position_service.get_positions()

        # Assert
        assert len(result) == 1
        assert result[0] == mock_derivative_position
        assert mock_http_client.call_count == 2
        mock_response_handler.handle_get_collateral_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_positions_empty_response(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test position retrieval with empty response."""
        # Arrange
        mock_http_client.side_effect = [
            ([], 200, {}),  # Empty positions
            ({"collaterals": []}, 200, {}),  # Empty collateral
        ]

        mock_response_handler.handle_get_positions_response.return_value = []
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

        # Act
        result = await position_service.get_positions()

        # Assert
        assert result == []

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
        eth_position = DerivativePosition(
            symbol="ETH-PERP",
            side=OrderSide.SELL,
            size=Decimal("-10.0"),
            entry_price=Decimal("3500.00"),
            mark_price=Decimal("3450.00"),
            unrealized_pnl=Decimal("500.00"),
            realized_pnl=Decimal("0.00"),
            liquidation_price=Decimal("3800.00"),
            exchange="backpack",
            timestamp=datetime.now(UTC),
            bp_details=BackpackDerivativePositionDetails(),
        )

        raw_positions = [
            {"symbol": "BTC-PERP", "size": "1.5"},
            {"symbol": "ETH-PERP", "size": "10.0"},
        ]
        mock_http_client.return_value = (raw_positions, 200, {})

        mock_response_handler.handle_get_positions_response.return_value = [
            MagicMock(),
            MagicMock(),
        ]

        mock_mapper.transform_raw_position_to_internal.side_effect = [btc_position, eth_position]

        # Act
        result = await position_service.get_positions(symbol="BTC-PERP")

        # Assert
        assert len(result) == 1
        assert result[0].symbol == "BTC-PERP"

    @pytest.mark.asyncio
    async def test_get_positions_http_error(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
    ) -> None:
        """Test position retrieval with HTTP error."""
        # Arrange
        mock_http_client.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await position_service.get_positions()

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

        assert exc_info.value.code == APIErrorCode.RESPONSE_VALIDATION_FAILED.value

    @pytest.mark.asyncio
    async def test_get_positions_transformation_error(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_position: BackpackRawPosition,
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
        mock_raw_position: BackpackRawPosition,
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
    async def test_get_positions_combines_direct_and_collateral(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_position: BackpackRawPosition,
        mock_collateral_response: BackpackRawCollateralResponse,
    ) -> None:
        """Test that positions from both direct API and collateral are combined."""
        # Arrange
        btc_position = DerivativePosition(
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            size=Decimal("1.5"),
            entry_price=Decimal("50000.00"),
            mark_price=Decimal("50666.67"),
            unrealized_pnl=Decimal("1000.00"),
            realized_pnl=Decimal("500.00"),
            liquidation_price=Decimal("45000.00"),
            exchange="backpack",
            timestamp=datetime.now(UTC),
            bp_details=BackpackDerivativePositionDetails(),
        )

        eth_position = DerivativePosition(
            symbol="ETH-PERP",
            side=OrderSide.SELL,
            size=Decimal("-10.0"),
            entry_price=Decimal("3500.00"),
            mark_price=Decimal("3450.00"),
            unrealized_pnl=Decimal("500.00"),
            realized_pnl=Decimal("0.00"),
            liquidation_price=Decimal("3800.00"),
            exchange="backpack",
            timestamp=datetime.now(UTC),
            bp_details=BackpackDerivativePositionDetails(),
        )

        # BTC from direct API
        raw_positions_response = [mock_raw_position.model_dump()]

        # ETH from collateral API
        collateral_with_eth = BackpackRawCollateralResponse.model_validate({
            "netEquity": "100000.00",
            "netEquityAvailable": "92500.00",
            "netEquityLocked": "7500.00",
            "assetsValue": "100000.00",
            "liabilitiesValue": "0.00",
            "imf": "0.10",
            "mmf": "0.05",
            "marginFraction": "0.075",
            "borrowLiability": "0.00",
            "pnlUnrealized": "500.00",
            "unsettledEquity": "0.00",
            "netExposureFutures": "34500.00",
            "collateral": [
                {
                    "symbol": "ETH-PERP",
                    "assetMarkPrice": "3450.00",
                    "totalQuantity": "-10.0",
                    "balanceNotional": "34500.00",
                    "collateralWeight": "0.90",
                    "collateralValue": "31050.00",
                    "openOrderQuantity": "0.00",
                    "lendQuantity": "0.00",
                    "availableQuantity": "-10.0",
                },
            ],
        })

        mock_http_client.side_effect = [
            (raw_positions_response, 200, {}),
            (collateral_with_eth.model_dump(), 200, {}),
        ]

        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]
        mock_response_handler.handle_get_collateral_response.return_value = collateral_with_eth

        mock_mapper.transform_raw_position_to_internal.side_effect = [
            btc_position,  # From direct API
            eth_position,  # From collateral
        ]

        # Act
        result = await position_service.get_positions()

        # Assert
        assert len(result) == 2
        symbols = {pos.symbol for pos in result}
        assert "BTC-PERP" in symbols
        assert "ETH-PERP" in symbols

    @pytest.mark.asyncio
    async def test_get_positions_deduplicates_by_symbol(
        self,
        position_service: BackpackPositionService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_position: BackpackRawPosition,
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
        assert result[0].symbol == "BTC-PERP"
