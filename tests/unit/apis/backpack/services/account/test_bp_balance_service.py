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
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
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
from cyberdelta.apis.exceptions import EmptyResponseError
from cyberdelta.core.models import SpotBalance
from cyberdelta.core.models.spot_balance import BackpackSpotBalanceDetails


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
def balance_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_mapper: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackBalanceService:
    """Create a balance service instance with mocks."""
    return BackpackBalanceService(
        http_client_requester=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        authenticator=mock_authenticator,
        exchange_name="backpack",
    )


@pytest.fixture
def mock_raw_balance() -> BackpackRawBalance:
    """Create a mock raw balance."""
    return BackpackRawBalance(
        available="1000.00",
        locked="100.00",
        staked="0.00",
    )


@pytest.fixture
def mock_spot_balance() -> SpotBalance:
    """Create a mock spot balance."""
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
    """Create a mock collateral response."""
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
        mock_raw_balance: BackpackRawBalance,
        mock_spot_balance: SpotBalance,
    ) -> None:
        """Test successful balance retrieval."""
        # Arrange
        raw_balances_response = [mock_raw_balance.model_dump()]
        mock_http_client.return_value = (raw_balances_response, 200, {})

        validated_balances = [mock_raw_balance]
        mock_response_handler.handle_get_balances_response.return_value = validated_balances

        mock_mapper.transform_raw_balance_to_internal.return_value = mock_spot_balance

        # Act
        result = await balance_service.get_balances()

        # Assert
        assert "USDC" in result
        assert result["USDC"] == mock_spot_balance
        mock_http_client.assert_called_once()
        mock_response_handler.handle_get_balances_response.assert_called_once()
        mock_mapper.transform_raw_balance_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_balances_with_collateral_enhancement(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_balance: BackpackRawBalance,
        mock_spot_balance: SpotBalance,
        mock_collateral_response: BackpackRawCollateralResponse,
    ) -> None:
        """Test balance retrieval with collateral enhancement."""
        # Arrange
        # First call for balances
        raw_balances_response = [mock_raw_balance.model_dump()]
        # Second call for collateral
        raw_collateral_response = mock_collateral_response.model_dump()

        mock_http_client.side_effect = [
            (raw_balances_response, 200, {}),
            (raw_collateral_response, 200, {}),
        ]

        validated_balances = [mock_raw_balance]
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
        # Arrange
        mock_http_client.return_value = ([], 200, {})
        mock_response_handler.handle_get_balances_response.return_value = []

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
        mock_http_client.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await balance_service.get_balances()

        assert "Network error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_balances_validation_error(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test balance retrieval with response validation error."""
        # Arrange
        mock_http_client.return_value = ([{"invalid": "data"}], 200, {})

        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                "validation_error",
                [{"type": "missing", "loc": ("symbol",), "input": {}}],
            )
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await balance_service.get_balances()

        assert exc_info.value.code == APIErrorCode.RESPONSE_VALIDATION_FAILED.value

    @pytest.mark.asyncio
    async def test_get_balances_transformation_error(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_balance: BackpackRawBalance,
    ) -> None:
        """Test balance retrieval with transformation error."""
        # Arrange
        raw_balances_response = [mock_raw_balance.model_dump()]
        mock_http_client.return_value = (raw_balances_response, 200, {})

        validated_balances = [mock_raw_balance]
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
    async def test_get_balances_collateral_error_continues(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_balance: BackpackRawBalance,
        mock_spot_balance: SpotBalance,
    ) -> None:
        """Test that balance retrieval continues even if collateral fetch fails."""
        # Arrange
        raw_balances_response = [mock_raw_balance.model_dump()]

        # First call succeeds, second call fails
        mock_http_client.side_effect = [
            (raw_balances_response, 200, {}),
            Exception("Collateral fetch failed"),
        ]

        validated_balances = [mock_raw_balance]
        mock_response_handler.handle_get_balances_response.return_value = validated_balances

        mock_mapper.transform_raw_balance_to_internal.return_value = mock_spot_balance

        # Act
        result = await balance_service.get_balances()

        # Assert - Should still return balances without collateral
        assert "USDC" in result
        assert result["USDC"] == mock_spot_balance
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
        """Test auto-lending scenario where balances API returns empty but collateral has data."""
        # Arrange
        # Empty balances response
        mock_http_client.side_effect = [
            ([], 200, {}),  # Empty balances
            (mock_collateral_response.model_dump(), 200, {}),  # Collateral data
        ]

        mock_response_handler.handle_get_balances_response.return_value = []
        mock_response_handler.handle_get_collateral_response.return_value = mock_collateral_response

        # Create balance from collateral
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

        mock_mapper.transform_raw_balance_to_internal.return_value = auto_lend_balance

        # Act
        result = await balance_service.get_balances()

        # Assert
        assert "USDC" in result
        assert result["USDC"].total_quantity == Decimal("2100.00")
        assert mock_http_client.call_count == 2

    @pytest.mark.asyncio
    async def test_get_balances_filters_zero_balances(
        self,
        balance_service: BackpackBalanceService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test that zero balances are filtered out."""
        # Arrange
        zero_balance = BackpackRawBalance(
            available="0.00",
            locked="0.00",
            staked="0.00",
        )

        non_zero_balance = BackpackRawBalance(
            available="100.00",
            locked="0.00",
            staked="0.00",
        )

        raw_balances_response = [
            zero_balance.model_dump(),
            non_zero_balance.model_dump(),
        ]
        mock_http_client.return_value = (raw_balances_response, 200, {})

        validated_balances = [zero_balance, non_zero_balance]
        mock_response_handler.handle_get_balances_response.return_value = validated_balances

        # Only transform non-zero balance
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

        mock_mapper.transform_raw_balance_to_internal.return_value = usdc_balance

        # Act
        result = await balance_service.get_balances()

        # Assert
        assert "USDC" in result
        assert "BTC" not in result
        # Mapper should only be called for non-zero balance
        mock_mapper.transform_raw_balance_to_internal.assert_called_once()

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
        with pytest.raises(EmptyResponseError):
            await balance_service.get_balances()

    @pytest.mark.asyncio
    async def test_get_balances_with_authenticator_none(
        self,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_balance: BackpackRawBalance,
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

        raw_balances_response = [mock_raw_balance.model_dump()]
        mock_http_client.return_value = (raw_balances_response, 200, {})

        validated_balances = [mock_raw_balance]
        mock_response_handler.handle_get_balances_response.return_value = validated_balances

        mock_mapper.transform_raw_balance_to_internal.return_value = mock_spot_balance

        # Act
        result = await balance_service.get_balances()

        # Assert
        assert "USDC" in result
        assert result["USDC"] == mock_spot_balance
