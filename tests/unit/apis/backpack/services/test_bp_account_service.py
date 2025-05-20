"""
Unit tests for the BackpackAccountService.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.backpack.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler, RawJsonResponse
from cyberdelta.apis.backpack.models.bp_raw_account import (
    BackpackRawBalance,
)
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.enums import (
    InternalTransferStatus,
    InternalWithdrawalStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.margin_account import BackpackMarginDetails, MarginAccountSummary
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.core.models.operations import (
    BackpackTransferDetails,
    BackpackWithdrawalDetails,
    Transfer,
    Withdrawal,
)
from cyberdelta.core.models.spot_balance import SpotBalance

# Type alias for the HTTP client requester callable
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Provides a mock HTTP client requester."""
    return AsyncMock(spec=HttpClientRequesterSig)


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Provides a mock BackpackRequestBuilder."""
    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Provides a mock BackpackResponseHandler."""
    return MagicMock(spec=BackpackResponseHandler)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Provides a mock IAuthenticator."""
    return MagicMock(spec=IAuthenticator)


@pytest.fixture
def mock_rate_limiter_service() -> AsyncMock:
    """Provides a mock RateLimiterService."""
    return AsyncMock(spec=RateLimiterService)


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Provides a mock BackpackOrderMapper."""
    return MagicMock(spec=BackpackOrderMapper)


@pytest.fixture
def bp_account_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_authenticator: MagicMock,
    mock_rate_limiter_service: AsyncMock,
    mock_mapper: MagicMock,
) -> BackpackAccountService:
    """Provides an instance of BackpackAccountService with mocked dependencies."""
    service = BackpackAccountService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name="backpack_test_account",
    )
    return service


class TestBackpackAccountService:
    """Tests for the BackpackAccountService class."""

    # The tests for the former 'transfer_raw' method have been refactored
    # to test the public 'transfer' method.

    @pytest.mark.asyncio
    async def test_transfer_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test transfer successfully initiates a transfer and returns an internal Transfer
        model."""
        asset = "USDC"
        amount = Decimal("100.0")
        from_account = "SPOT"
        to_account = "FUTURES"
        client_transfer_id = "testTransfer123"

        mock_payload = {
            "symbol": asset,
            "quantity": str(amount),
            "fromAccount": from_account,
            "toAccount": to_account,
            "clientId": client_transfer_id,
        }
        mock_raw_response_content: RawJsonResponse = {
            "status": "success",
            "id": "transfer789",
            "message": "Transfer completed",
        }

        expected_internal_transfer = Transfer(
            id="transfer789",
            exchange="backpack_test_account",
            asset=asset,
            quantity=amount,
            status=InternalTransferStatus.COMPLETED,
            timestamp=MagicMock(spec=datetime),
            response_message="Transfer completed",
            bp_details=BackpackTransferDetails(
                client_id=client_transfer_id,
                from_account_type=from_account,
                to_account_type=to_account,
            ),
            hl_details=None,
        )

        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response_content, 200, MagicMock())
        mock_response_handler.handle_transfer_response.return_value = mock_raw_response_content
        mock_mapper.transform_raw_transfer_to_internal.return_value = expected_internal_transfer

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.transfer(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
                client_transfer_id=client_transfer_id,
            )

        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol=asset,
            amount_str=str(amount),
            from_account=from_account,
            to_account=to_account,
            client_transfer_id=client_transfer_id,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/wapi/v1/capital/transfer/internal",
            data=mock_payload,
            is_signed=True,
            endpoint_group="private_write",
            request_weight=1,
        )
        mock_response_handler.handle_transfer_response.assert_called_once_with(
            mock_raw_response_content
        )
        mock_mapper.transform_raw_transfer_to_internal.assert_called_once_with(
            raw_response=mock_raw_response_content,
            asset=asset,
            quantity=amount,
            from_account_type_raw=from_account,
            to_account_type_raw=to_account,
            client_transfer_id=client_transfer_id,
        )
        assert result == expected_internal_transfer
        assert isinstance(result.timestamp, datetime)

    @pytest.mark.asyncio
    async def test_transfer_api_error_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test public transfer handles APIError raised by the http_client_requester."""
        asset = "USDC"
        amount = Decimal("50")
        from_account = "SPOT"
        to_account = "DERIVATIVES"
        api_error_instance = APIError("Requester failed", code=APIErrorCode.SERVER_ERROR.value)

        mock_payload = {"some": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.side_effect = api_error_instance

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.transfer(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )

        assert exc_info.value is api_error_instance
        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol=asset,
            amount_str=str(amount),
            from_account=from_account,
            to_account=to_account,
            client_transfer_id=None,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/wapi/v1/capital/transfer/internal",
            data=mock_payload,
            is_signed=True,
            endpoint_group="private_write",
            request_weight=1,
        )

    @pytest.mark.asyncio
    async def test_transfer_response_none_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test public transfer handles None response from http_client_requester by
        raising APIError."""
        asset = "BTC"
        amount = Decimal("0.1")
        from_account = "MAIN"
        to_account = "TRADING"
        status_code_from_requester = 200  # Example status code

        mock_payload = {"another": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        # Simulate requester returning None for content
        mock_http_client_requester.return_value = (None, status_code_from_requester, MagicMock())

        expected_error_message = (
            f"No response data received for transfer request. Status: {status_code_from_requester}"
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.transfer(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert exc_info.value.message == expected_error_message
        assert exc_info.value.http_status == status_code_from_requester

        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol=asset,
            amount_str=str(amount),
            from_account=from_account,
            to_account=to_account,
            client_transfer_id=None,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/wapi/v1/capital/transfer/internal",
            data=mock_payload,
            is_signed=True,
            endpoint_group="private_write",
            request_weight=1,
        )

    @pytest.mark.asyncio
    async def test_transfer_unexpected_exception_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test public transfer handles unexpected exceptions from http_client_requester
        by wrapping in APIError."""
        asset = "ETH"
        amount = Decimal("1.0")
        from_account = "SUB_01"
        to_account = "SPOT"
        unexpected_error = ValueError("Something went very wrong in requester")

        mock_payload = {"unexpected": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.side_effect = unexpected_error

        expected_error_message = f"Unexpected error processing transfer request: {unexpected_error}"

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.transfer(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert exc_info.value.message == expected_error_message
        assert exc_info.value.original_exception is unexpected_error

        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol=asset,
            amount_str=str(amount),
            from_account=from_account,
            to_account=to_account,
            client_transfer_id=None,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/wapi/v1/capital/transfer/internal",
            data=mock_payload,
            is_signed=True,
            endpoint_group="private_write",
            request_weight=1,
        )

    @pytest.mark.asyncio
    async def test_get_balances_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_balances_raw successfully fetches and processes balance data.
        Refactored to test through the public get_balances method.
        """
        mock_raw_response_data = {
            "USDC": {"available": "1000.0", "locked": "0", "debt": "0", "total": "1000.0"}
        }
        mock_validated_raw_balances: dict[str, BackpackRawBalance] = {
            "USDC": BackpackRawBalance(asset="USDC", available="1000.0", total="1000.0")
        }
        # Expected result from the public get_balances method
        mock_internal_spot_balance = SpotBalance(
            exchange="backpack_test_account",
            asset="USDC",
            timestamp=MagicMock(spec=datetime),  # Timestamp will be generated internally
            total_quantity=Decimal("1000.0"),
            available_quantity=Decimal("1000.0"),
        )
        expected_balances_result: dict[str, SpotBalance] = {"USDC": mock_internal_spot_balance}

        mock_http_client_requester.return_value = (mock_raw_response_data, 200, MagicMock())
        mock_response_handler.handle_get_balances_response.return_value = (
            mock_validated_raw_balances
        )
        mock_mapper.transform_raw_balance_to_internal.return_value = mock_internal_spot_balance

        # Configure the mock rate limiter (though not directly asserted here, it's part of the flow)
        mock_limiter_instance = AsyncMock()
        mock_limiter_instance.acquire = AsyncMock()  # Ensure acquire is an Awaitable
        mock_rate_limiter_service.get_limiter.return_value = mock_limiter_instance

        # Call the public method
        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_balances()

        # Assertions for the public method's interaction
        mock_request_builder.build_get_balances_params.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/capital",
            params=None,
            is_signed=True,
            # Add endpoint_group and request_weight if they are always passed
            # by the service
            # For now, assuming they might be conditional or defaulted, so not
            # asserting them rigidly yet.
            # Re-checking BackpackAccountService._get_raw_balances_dict call:
            # it passes is_signed=True. Let's assume endpoint_group and
            # weight might be handled by http_client.
        )
        mock_response_handler.handle_get_balances_response.assert_called_once_with(
            mock_raw_response_data
        )
        mock_mapper.transform_raw_balance_to_internal.assert_called_once_with(
            "USDC", mock_validated_raw_balances["USDC"]
        )
        assert result == expected_balances_result
        # Verify timestamp was set
        assert isinstance(result["USDC"].timestamp, datetime)

    @pytest.mark.asyncio
    async def test_get_positions_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_positions_raw successfully fetches and processes position data.
        Refactored to test through the public get_positions method.
        """
        symbol_arg = "SOL-PERP"
        mock_raw_positions_data_item = {
            "symbol": "SOL-PERP",
            "netQuantity": "10.0",
            "entryPrice": "100.0",
            "markPrice": "110.0",
            "imf": "0.1",
            "mmf": "0.05",
            "pnlUnrealized": "100.0",
            "pnlRealized": "0.0",
            "netCost": "1000.0",
            "netExposureNotional": "1100.0",
            "netExposureQuantity": "10.0",
            "positionId": "pos123",
            "userId": 1,
            "breakEvenPrice": "105.0",
            "estLiquidationPrice": "90.0",
            "imfFunction": {"base": "0.005", "factor": "0.000001"},
            "mmfFunction": {"base": "0.002", "factor": "0.0000005"},
            "cumulativeFundingPayment": "-5.0",
            "cumulativeInterest": "-0.1",
        }
        mock_raw_positions_data_list = [mock_raw_positions_data_item]
        mock_validated_raw_positions = [
            BackpackRawPosition.model_validate(mock_raw_positions_data_item)
        ]

        # Expected result from public get_positions
        mock_internal_derivative_position = DerivativePosition(
            exchange="backpack_test_account",
            symbol=symbol_arg,
            timestamp=MagicMock(spec=datetime),
            side=OrderSide.BUY
            if Decimal(mock_validated_raw_positions[0].net_quantity) > 0
            else OrderSide.SELL,
            size=Decimal(mock_validated_raw_positions[0].net_quantity),
            entry_price=Decimal(mock_validated_raw_positions[0].entry_price),
            mark_price=Decimal(mock_validated_raw_positions[0].mark_price),
            unrealized_pnl=Decimal(mock_validated_raw_positions[0].pnl_unrealized),
        )
        expected_positions_result: list[DerivativePosition] = [mock_internal_derivative_position]

        # --- Test without symbol ---
        mock_http_client_requester.return_value = (mock_raw_positions_data_list, 200, MagicMock())
        mock_response_handler.handle_get_positions_response.return_value = (
            mock_validated_raw_positions
        )
        mock_mapper.transform_raw_positions_to_internal.return_value = (
            expected_positions_result  # For list
        )

        # Configure rate limiter
        mock_limiter_no_symbol = AsyncMock()
        mock_limiter_no_symbol.acquire = AsyncMock()
        mock_rate_limiter_service.get_limiter.return_value = mock_limiter_no_symbol

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result_no_symbol = await bp_account_service.get_positions(symbol=None)

        mock_request_builder.build_get_positions_params.assert_called_with(symbol=None)
        mock_http_client_requester.assert_called_with(
            method="GET",
            endpoint="/api/v1/positions",  # Endpoint for no symbol
            params=None,
            is_signed=True,
        )
        mock_response_handler.handle_get_positions_response.assert_called_with(
            mock_raw_positions_data_list, None
        )
        mock_mapper.transform_raw_positions_to_internal.assert_called_with(
            mock_validated_raw_positions
        )
        assert result_no_symbol == expected_positions_result
        assert isinstance(result_no_symbol[0].timestamp, datetime)

        # --- Test with symbol ---
        mock_http_client_requester.reset_mock()
        mock_response_handler.reset_mock()
        mock_request_builder.build_get_positions_params.reset_mock()
        mock_mapper.transform_raw_positions_to_internal.reset_mock()
        mock_rate_limiter_service.get_limiter.reset_mock()  # Reset for new call pattern

        # Reconfigure mocks for the call with symbol
        # Backpack positions endpoint can return a single dict if symbol is specified, or a list
        # The _get_raw_positions_list was adapted to always return a list by wrapping single dict.
        # The public get_positions also returns a list.
        # So, the http client might return a dict here for a single symbol
        mock_http_client_requester.return_value = (
            mock_raw_positions_data_item,
            200,
            MagicMock(),
        )  # Returns dict
        # response_handler.handle_get_positions_response expects a list if symbol is
        # None, or dict if symbol is specified.
        # Let's assume it correctly handles the dict and returns a list of one
        # validated item
        mock_response_handler.handle_get_positions_response.return_value = (
            mock_validated_raw_positions  # Still returns list
        )
        mock_mapper.transform_raw_positions_to_internal.return_value = (
            expected_positions_result  # Still returns list
        )

        mock_limiter_with_symbol = AsyncMock()
        mock_limiter_with_symbol.acquire = AsyncMock()
        mock_rate_limiter_service.get_limiter.return_value = mock_limiter_with_symbol

        expected_params_with_symbol = {"symbol": symbol_arg}
        mock_request_builder.build_get_positions_params.return_value = expected_params_with_symbol
        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result_with_symbol = await bp_account_service.get_positions(symbol=symbol_arg)

        mock_request_builder.build_get_positions_params.assert_called_with(symbol=symbol_arg)
        mock_http_client_requester.assert_called_with(
            method="GET",
            endpoint=f"/api/v1/positions/{symbol_arg}",  # Endpoint for specific symbol
            params=expected_params_with_symbol,  # Or None if builder handles it differently
            is_signed=True,
        )
        mock_response_handler.handle_get_positions_response.assert_called_with(
            mock_raw_positions_data_item, symbol_arg
        )
        mock_mapper.transform_raw_positions_to_internal.assert_called_with(
            mock_validated_raw_positions
        )
        assert result_with_symbol == expected_positions_result
        assert isinstance(result_with_symbol[0].timestamp, datetime)

    @pytest.mark.asyncio
    async def test_get_account_info_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_account_info_raw successfully fetches and processes account info.
        Refactored to test through the public get_account_info method.
        """
        mock_raw_response_data = {
            "autoBorrowSettlements": True,
            "autoLend": False,
            # ... (other fields from BackpackRawAccountSummary)
            "leverageLimit": "20.00000000",
        }
        mock_validated_raw_account_summary = BackpackRawAccountSummary.model_validate(
            mock_raw_response_data
        )
        # Expected result from public get_account_info
        mock_bp_details = BackpackMarginDetails(
            imf_raw=str(
                mock_validated_raw_account_summary.leverage_limit
            ),  # Convert Decimal to str
            # Populate other BackpackMarginDetails fields as None or with
            # appropriate mock values if needed by the test
        )
        mock_internal_margin_summary = MarginAccountSummary(
            exchange="backpack_test_account",
            timestamp=MagicMock(spec=datetime),
            total_equity=Decimal("0"),  # Placeholder, actual value depends on mapper logic
            available_equity=Decimal("0"),  # Placeholder
            bp_details=mock_bp_details,
            # Ensure all required fields of MarginAccountSummary are present
        )
        expected_account_info_result: MarginAccountSummary = mock_internal_margin_summary

        mock_http_client_requester.return_value = (mock_raw_response_data, 200, MagicMock())
        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_validated_raw_account_summary
        )
        mock_mapper.transform_raw_account_summary_to_internal.return_value = (
            mock_internal_margin_summary
        )

        # Configure rate limiter
        mock_limiter_instance = AsyncMock()
        mock_limiter_instance.acquire = AsyncMock()
        mock_rate_limiter_service.get_limiter.return_value = mock_limiter_instance

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_account_info()

        mock_request_builder.build_get_account_info_params.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/account",
            params=None,  # Assuming no params for account info
            is_signed=True,
        )
        mock_response_handler.handle_get_account_info_response.assert_called_once_with(
            mock_raw_response_data
        )
        mock_mapper.transform_raw_account_summary_to_internal.assert_called_once_with(
            mock_validated_raw_account_summary
        )
        assert result == expected_account_info_result
        assert isinstance(result.timestamp, datetime)

    @pytest.mark.asyncio
    async def test_get_balances_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_balances successfully retrieves and processes balance data."""
        mock_endpoint_path_for_get_balances = "/api/v1/capital"
        mock_params_from_builder_for_get_balances = None

        mock_raw_response_dict: RawJsonResponse = {
            "USDC": {"available": "1000.5", "locked": "10.0"},
            "SOL": {"available": "50.2", "locked": "0.5"},
        }
        mock_status_code = 200
        mock_headers: dict[str, str] = {}

        mock_request_builder.build_get_balances_params.return_value = (
            mock_params_from_builder_for_get_balances
        )

        mock_http_client_requester.return_value = (
            mock_raw_response_dict,
            mock_status_code,
            mock_headers,
        )

        mock_raw_balances_payload: dict[str, BackpackRawBalance] = {
            "USDC": BackpackRawBalance(asset="USDC", available="1000.5", total="1010.5"),
            "SOL": BackpackRawBalance(asset="SOL", available="50.2", total="50.7"),
        }
        mock_response_handler.handle_get_balances_response.return_value = mock_raw_balances_payload

        expected_internal_balances: dict[str, SpotBalance] = {
            "USDC": SpotBalance(
                exchange="backpack_test_account",
                asset="USDC",
                timestamp=MagicMock(spec=datetime),
                total_quantity=Decimal("1010.5"),
                available_quantity=Decimal("1000.5"),
            ),
            "SOL": SpotBalance(
                exchange="backpack_test_account",
                asset="SOL",
                timestamp=MagicMock(spec=datetime),
                total_quantity=Decimal("50.7"),
                available_quantity=Decimal("50.2"),
            ),
        }

        usdc_spot_balance = SpotBalance(
            exchange="backpack_test_account",
            asset="USDC",
            timestamp=MagicMock(spec=datetime),
            total_quantity=Decimal("1010.5"),
            available_quantity=Decimal("1000.5"),
        )
        sol_spot_balance = SpotBalance(
            exchange="backpack_test_account",
            asset="SOL",
            timestamp=MagicMock(spec=datetime),
            total_quantity=Decimal("50.7"),
            available_quantity=Decimal("50.2"),
        )

        def mapper_side_effect(
            asset_symbol: str, raw_balance_model: BackpackRawBalance
        ) -> SpotBalance:
            if asset_symbol == "USDC" and raw_balance_model == mock_raw_balances_payload["USDC"]:
                return usdc_spot_balance
            if asset_symbol == "SOL" and raw_balance_model == mock_raw_balances_payload["SOL"]:
                return sol_spot_balance
            pytest.fail(
                f"mock_mapper.transform_raw_balance_to_internal called with unexpected args: "
                f"{asset_symbol}, {raw_balance_model}"
            )
            raise AssertionError(
                "Fell through mapper_side_effect logic, should be impossible due to pytest.fail"
            )

        mock_mapper.transform_raw_balance_to_internal.side_effect = mapper_side_effect

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result_balances = await bp_account_service.get_balances()

        mock_request_builder.build_get_balances_params.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint=mock_endpoint_path_for_get_balances,
            params=mock_params_from_builder_for_get_balances,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        mock_response_handler.handle_get_balances_response.assert_called_once_with(
            mock_raw_response_dict
        )

        assert mock_mapper.transform_raw_balance_to_internal.call_count == 2
        mock_mapper.transform_raw_balance_to_internal.assert_any_call(
            "USDC", mock_raw_balances_payload["USDC"]
        )
        mock_mapper.transform_raw_balance_to_internal.assert_any_call(
            "SOL", mock_raw_balances_payload["SOL"]
        )

        assert result_balances == expected_internal_balances

    @pytest.mark.asyncio
    async def test_get_balances_api_error_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_balances handles APIError from http_client_requester."""
        mock_params_from_builder = None

        mock_request_builder.build_get_balances_params.return_value = mock_params_from_builder
        mock_http_client_requester.side_effect = APIError(
            message="Network error", code=APIErrorCode.SERVER_ERROR.value
        )

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_balances()

        assert excinfo.value.code == APIErrorCode.SERVER_ERROR.value
        mock_request_builder.build_get_balances_params.assert_called_once()
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/capital",
            params=mock_params_from_builder,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

    @pytest.mark.asyncio
    async def test_get_balances_validation_error_from_response_handler(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances handles ValidationError (via APIError) from response_handler."""
        mock_params_from_builder = None
        mock_raw_response_dict: RawJsonResponse = {"invalid": "data"}
        mock_status_code = 200
        mock_headers: dict[str, str] = {}

        mock_request_builder.build_get_balances_params.return_value = mock_params_from_builder
        mock_http_client_requester.return_value = (
            mock_raw_response_dict,
            mock_status_code,
            mock_headers,
        )

        handler_api_error = APIError(
            "Pydantic validation failed for balances", APIErrorCode.INVALID_RESPONSE.value
        )
        mock_response_handler.handle_get_balances_response.side_effect = handler_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_balances()

        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert excinfo.value.message == "Pydantic validation failed for balances"
        mock_response_handler.handle_get_balances_response.assert_called_once_with(
            mock_raw_response_dict
        )

    @pytest.mark.asyncio
    async def test_get_balances_error_from_mapper(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_balances handles an error (e.g., ValueError) from mapper,
        wrapped in APIError."""
        mock_params_from_builder = None
        mock_raw_response_dict: RawJsonResponse = {
            "USDC": {"available": "1000.5", "locked": "10.0"}
        }
        mock_status_code = 200
        mock_headers: dict[str, str] = {}

        mock_request_builder.build_get_balances_params.return_value = mock_params_from_builder
        mock_http_client_requester.return_value = (
            mock_raw_response_dict,
            mock_status_code,
            mock_headers,
        )

        mock_raw_balances_payload = {
            "USDC": BackpackRawBalance(asset="USDC", available="1000.5", total="1000.5")
        }
        mock_response_handler.handle_get_balances_response.return_value = mock_raw_balances_payload

        mapper_internal_error = ValueError("Bad raw balance value for mapping")
        mock_mapper.transform_raw_balance_to_internal.side_effect = mapper_internal_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_balances()

        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Mapping balance data failed for asset USDC" in excinfo.value.message
        assert excinfo.value.original_exception is mapper_internal_error
        mock_mapper.transform_raw_balance_to_internal.assert_called_once_with(
            "USDC", mock_raw_balances_payload["USDC"]
        )

    @pytest.mark.asyncio
    async def test_transfer_api_error_from_response_handler(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test transfer handles APIError from response_handler."""
        asset = "USDC"
        amount = Decimal("100.0")
        from_account = "SPOT"
        to_account = "DERIVATIVES"

        mock_built_payload = {"some": "payload"}
        mock_raw_response_content: RawJsonResponse = {"error": "bad format"}  # Malformed
        mock_status_code = 200
        mock_headers: Mapping[str, str] = {}

        mock_request_builder.build_internal_transfer_payload.return_value = mock_built_payload
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers,
        )
        handler_api_error = APIError(
            "Invalid transfer response: 'success' field missing",
            APIErrorCode.INVALID_RESPONSE.value,
        )
        mock_response_handler.handle_transfer_response.side_effect = handler_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.transfer(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )
        assert excinfo.value is handler_api_error
        mock_response_handler.handle_transfer_response.assert_called_once_with(
            mock_raw_response_content
        )

    @pytest.mark.asyncio
    async def test_transfer_api_error_from_mapper(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test transfer handles APIError from mapper."""
        asset = "USDC"
        amount = Decimal("100.0")
        from_account = "SPOT"
        to_account = "DERIVATIVES"
        client_transfer_id = "clientTransfer001"

        mock_built_payload = {"some": "payload"}
        mock_raw_response_content: RawJsonResponse = {
            "success": False,
            "message": "Transfer failed by exchange",
        }
        mock_status_code = 200
        mock_headers: Mapping[str, str] = {}

        mock_request_builder.build_internal_transfer_payload.return_value = mock_built_payload
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers,
        )
        mock_response_handler.handle_transfer_response.return_value = mock_raw_response_content

        # Simulate mapper raising an APIError (e.g., if it wraps a ValueError)
        mapper_api_error = APIError(
            "Failed to map raw transfer data", APIErrorCode.INVALID_RESPONSE.value
        )
        mock_mapper.transform_raw_transfer_to_internal.side_effect = mapper_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.transfer(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
                client_transfer_id=client_transfer_id,
            )
        assert excinfo.value is mapper_api_error
        mock_mapper.transform_raw_transfer_to_internal.assert_called_once_with(
            raw_response=mock_raw_response_content,
            asset=asset,
            quantity=amount,
            from_account_type_raw=from_account,
            to_account_type_raw=to_account,
            client_transfer_id=client_transfer_id,
        )

    @pytest.mark.asyncio
    async def test_withdraw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test successful withdrawal."""
        asset = "USDC"
        amount = Decimal("500.0")
        address = "SOLANA_ADDRESS_HERE_XYZ"
        network = "SOLANA"
        client_withdrawal_id = "clientWithdraw002"
        two_factor_token = "123456"

        mock_built_payload = {
            "coin": asset,
            "address": address,
            "network": network,
            "quantity": str(amount),
            "clientId": client_withdrawal_id,
            "twoFactorToken": two_factor_token,
        }
        # From cyberdelta/apis/backpack/models/bp_raw_withdrawal.py
        mock_raw_response_data: RawJsonResponse = {
            "id": "bpWithdrawId789",
            "blockchain": network,
            "quantity": str(amount),
            "fee": "0.1",
            "symbol": asset,
            "status": "processing",  # Example status
            "toAddress": address,
            "createdAt": "2023-01-01T12:00:00.000Z",
            "isInternal": False,
        }
        mock_status_code = 200
        mock_headers: Mapping[str, str] = {}

        # Validated raw model from response_handler
        mock_validated_raw_withdrawal = BackpackRawWithdrawalResponse.model_validate(
            mock_raw_response_data
        )

        expected_internal_withdrawal = Withdrawal(
            id="bpWithdrawId789",
            exchange="backpack_test_account",
            status=InternalWithdrawalStatus.PROCESSING,  # Mapped from "processing"
            asset=asset,
            quantity=amount,
            address=address,
            timestamp=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),  # From createdAt
            fee=Decimal("0.1"),
            tx_hash=None,  # Not in mock_raw_response_data
            response_message=None,  # Not directly in raw model, mapper might add based on status
            bp_details=BackpackWithdrawalDetails(
                blockchain=network,
                is_internal=False,
                client_id=client_withdrawal_id,  # This comes from input params, not raw response
            ),
            hl_details=None,
        )

        mock_request_builder.build_withdraw_payload.return_value = mock_built_payload
        mock_http_client_requester.return_value = (
            mock_raw_response_data,
            mock_status_code,
            mock_headers,
        )
        mock_response_handler.handle_withdraw_response.return_value = mock_validated_raw_withdrawal
        mock_mapper.transform_raw_withdrawal_response_to_internal.return_value = (
            expected_internal_withdrawal
        )

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.withdraw(
                asset=asset,
                amount=amount,
                address=address,
                network=network,
                client_withdrawal_id=client_withdrawal_id,
                two_factor_token=two_factor_token,
            )

        mock_request_builder.build_withdraw_payload.assert_called_once_with(
            asset_symbol=asset,
            quantity_str=str(amount),
            address=address,
            network=network,
            tag=None,  # Default in service method
            client_withdrawal_id=client_withdrawal_id,
            two_factor_token=two_factor_token,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/wapi/v1/capital/withdrawals",
            data=mock_built_payload,
            is_signed=True,
            endpoint_group="private_write",
            request_weight=1,
        )
        mock_response_handler.handle_withdraw_response.assert_called_once_with(
            mock_raw_response_data
        )
        mock_mapper.transform_raw_withdrawal_response_to_internal.assert_called_once_with(
            raw_response=mock_validated_raw_withdrawal,
            asset=asset,  # Pass original params to mapper for context if needed
            quantity=amount,
            address=address,
            network=network,
            client_withdrawal_id=client_withdrawal_id,
            tag=None,
        )
        assert result == expected_internal_withdrawal

    @pytest.mark.asyncio
    async def test_withdraw_api_error_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test withdraw handles APIError from http_client_requester."""
        mock_built_payload = {"some": "payload"}
        mock_request_builder.build_withdraw_payload.return_value = mock_built_payload
        requester_api_error = APIError("Withdrawal failed", APIErrorCode.SERVER_ERROR.value)
        mock_http_client_requester.side_effect = requester_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.withdraw(
                asset="USDC", amount=Decimal("10"), address="ADDR", network="NET"
            )
        assert excinfo.value is requester_api_error
        mock_request_builder.build_withdraw_payload.assert_called_once()
        mock_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_withdraw_api_error_from_response_handler(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test withdraw handles APIError from response_handler."""
        mock_built_payload = {"some": "payload"}
        mock_raw_response_content: RawJsonResponse = {"error": "invalid withdrawal data"}
        mock_status_code = 400

        mock_request_builder.build_withdraw_payload.return_value = mock_built_payload
        mock_http_client_requester.return_value = (mock_raw_response_content, mock_status_code, {})
        handler_api_error = APIError("Invalid raw withdrawal", APIErrorCode.INVALID_RESPONSE.value)
        mock_response_handler.handle_withdraw_response.side_effect = handler_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.withdraw(
                asset="USDC", amount=Decimal("10"), address="ADDR", network="NET"
            )
        assert excinfo.value is handler_api_error
        mock_response_handler.handle_withdraw_response.assert_called_once_with(
            mock_raw_response_content
        )

    @pytest.mark.asyncio
    async def test_withdraw_api_error_from_mapper(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test withdraw handles APIError from mapper."""
        asset = "USDC"
        amount = Decimal("500.0")
        address = "SOLANA_ADDRESS_HERE_XYZ"
        network = "SOLANA"
        client_withdrawal_id = "clientWithdraw002"

        mock_built_payload = {"some": "payload"}
        mock_raw_response_data: RawJsonResponse = {"id": "bpWithdrawId789", "status": "failed"}
        mock_status_code = 200
        mock_validated_raw_withdrawal = BackpackRawWithdrawalResponse.model_validate(
            mock_raw_response_data
        )

        mock_request_builder.build_withdraw_payload.return_value = mock_built_payload
        mock_http_client_requester.return_value = (mock_raw_response_data, mock_status_code, {})
        mock_response_handler.handle_withdraw_response.return_value = mock_validated_raw_withdrawal

        mapper_api_error = APIError("Withdrawal mapping failed", APIErrorCode.UNKNOWN.value)
        mock_mapper.transform_raw_withdrawal_response_to_internal.side_effect = mapper_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.withdraw(
                asset=asset,
                amount=amount,
                address=address,
                network=network,
                client_withdrawal_id=client_withdrawal_id,
            )
        assert excinfo.value is mapper_api_error
        mock_mapper.transform_raw_withdrawal_response_to_internal.assert_called_once_with(
            raw_response=mock_validated_raw_withdrawal,
            asset=asset,
            quantity=amount,
            address=address,
            network=network,
            client_withdrawal_id=client_withdrawal_id,
            tag=None,
        )

    @pytest.mark.asyncio
    async def test_get_order_history_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test successful fetching of order history."""
        symbol = "SOL_USDC"
        limit = 5
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC)

        mock_built_params = {
            "symbol": symbol,
            "limit": limit,
            "startTime": int(start_time.timestamp() * 1000),
        }
        mock_raw_order_data = {
            "id": "orderHist123",
            "symbol": symbol,
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "10",
            "price": "100",
            "status": "FILLED",
            "createdAt": int(start_time.timestamp() * 1000),
            "timeInForce": "GTC",
        }
        mock_raw_response_list: list[dict[str, Any]] = [mock_raw_order_data]
        mock_status_code = 200
        mock_headers: Mapping[str, str] = {}

        mock_validated_raw_orders = [BackpackRawOrder.model_validate(mock_raw_order_data)]

        expected_internal_order = Order(
            exchange_order_id="orderHist123",
            exchange="backpack_test_account",
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.FILLED,
            quantity_requested=Decimal("10"),
            price=Decimal("100"),
            time_in_force=TimeInForce.GTC,
            created_at=start_time,
            updated_at=start_time,
            client_order_id="mock_client_order_id",
            quantity_filled=Decimal("10"),
            average_fill_price=Decimal("100"),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        expected_internal_orders_list = [expected_internal_order]

        mock_request_builder.build_get_order_history_params.return_value = mock_built_params
        mock_http_client_requester.return_value = (
            mock_raw_response_list,
            mock_status_code,
            mock_headers,
        )
        mock_response_handler.handle_get_order_history_response.return_value = (
            mock_validated_raw_orders
        )
        mock_mapper.transform_raw_orders_to_internal.return_value = expected_internal_orders_list

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_order_history(
                symbol=symbol, limit=limit, start_time=start_time, end_time=end_time
            )

        mock_request_builder.build_get_order_history_params.assert_called_once_with(
            symbol=symbol,
            startTime=start_time,
            endTime=end_time,
            limit=limit,
            orderId=None,
            clientId=None,
        )
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/history/orders",
            params=mock_built_params,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        mock_response_handler.handle_get_order_history_response.assert_called_once_with(
            mock_raw_response_list, symbol
        )
        mock_mapper.transform_raw_orders_to_internal.assert_called_once_with(
            mock_validated_raw_orders
        )
        assert result == expected_internal_orders_list

    @pytest.mark.asyncio
    async def test_get_order_history_api_error_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_order_history handles APIError from http_client_requester."""
        mock_built_params = {"symbol": "SOL_USDC"}
        mock_request_builder.build_get_order_history_params.return_value = mock_built_params
        requester_api_error = APIError(
            "Order history fetch failed", APIErrorCode.NETWORK_ISSUE.value
        )
        mock_http_client_requester.side_effect = requester_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_order_history(symbol="SOL_USDC")
        assert excinfo.value is requester_api_error
        mock_request_builder.build_get_order_history_params.assert_called_once()
        mock_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_order_history_api_error_from_response_handler(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_history handles APIError from response_handler."""
        symbol = "SOL_USDC"
        mock_built_params = {"symbol": symbol}
        mock_raw_response_list: list[dict[str, Any]] = [{"invalid": "order"}]
        mock_status_code = 200

        mock_request_builder.build_get_order_history_params.return_value = mock_built_params
        mock_http_client_requester.return_value = (mock_raw_response_list, mock_status_code, {})
        handler_api_error = APIError(
            "Invalid raw order history", APIErrorCode.INVALID_RESPONSE.value
        )
        mock_response_handler.handle_get_order_history_response.side_effect = handler_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_order_history(symbol=symbol)
        assert excinfo.value is handler_api_error
        mock_response_handler.handle_get_order_history_response.assert_called_once_with(
            mock_raw_response_list, symbol
        )

    @pytest.mark.asyncio
    async def test_get_order_history_api_error_from_mapper(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_order_history handles APIError from mapper."""
        symbol = "SOL_USDC"
        mock_built_params = {"symbol": symbol}
        mock_raw_order_data = {
            "id": "orderHist123",
            "symbol": symbol,
            "status": "INVALID_STATUS",
            "timeInForce": "GTC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "1",
            "price": "1",
            "createdAt": 123,
        }
        mock_raw_response_list: list[dict[str, Any]] = [mock_raw_order_data]
        mock_status_code = 200
        mock_validated_raw_orders = [BackpackRawOrder.model_validate(mock_raw_order_data)]

        mock_request_builder.build_get_order_history_params.return_value = mock_built_params
        mock_http_client_requester.return_value = (mock_raw_response_list, mock_status_code, {})
        mock_response_handler.handle_get_order_history_response.return_value = (
            mock_validated_raw_orders
        )

        mapper_api_error = APIError("Order history mapping failed", APIErrorCode.UNKNOWN.value)
        mock_mapper.transform_raw_orders_to_internal.side_effect = mapper_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_order_history(symbol=symbol)
        assert excinfo.value is mapper_api_error
        mock_mapper.transform_raw_orders_to_internal.assert_called_once_with(
            mock_validated_raw_orders
        )

    @pytest.mark.asyncio
    async def test_get_trade_history_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test successful fetching of trade history."""
        symbol = "SOL_USDC"
        limit = 10
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC)

        mock_built_params = {
            "symbol": symbol,
            "limit": limit,
            "from": int(start_time.timestamp() * 1000),
        }
        # BackpackRawTrade fields: id, symbol, price, qty, time, orderId
        mock_raw_trade_data = {
            "id": "tradeHist789",
            "symbol": symbol,
            "price": "101.5",
            "qty": "2.5",
            "time": int(start_time.timestamp() * 1000) + 1000,
            "orderId": "orderAssociated123",
        }
        mock_raw_response_list: list[dict[str, Any]] = [mock_raw_trade_data]
        mock_status_code = 200
        mock_headers: Mapping[str, str] = {}

        mock_validated_raw_trades = [BackpackRawTrade.model_validate(mock_raw_trade_data)]

        expected_internal_trade = Trade(
            id="tradeHist789",
            exchange="backpack_test_account",
            symbol=symbol,
            side=OrderSide.BUY,  # Placeholder: mapper would determine this
            order_id="orderAssociated123",
            price=Decimal("101.5"),
            quantity=Decimal("2.5"),
            executed_at=datetime.fromtimestamp(
                (int(start_time.timestamp() * 1000) + 1000) / 1000, tz=UTC
            ),
            # Defaulting other fields for this test
            fee=Decimal("0"),
        )
        expected_internal_trades_list = [expected_internal_trade]

        mock_request_builder.build_get_trade_history_params.return_value = mock_built_params
        mock_http_client_requester.return_value = (
            mock_raw_response_list,
            mock_status_code,
            mock_headers,
        )
        mock_response_handler.handle_get_trade_history_response.return_value = (
            mock_validated_raw_trades
        )
        mock_mapper.transform_raw_trades_to_internal.return_value = expected_internal_trades_list

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_trade_history(symbol=symbol, limit=limit)

        mock_request_builder.build_get_trade_history_params.assert_called_once_with(
            symbol=symbol,
            from_time=start_time,
            to_time=end_time,
            limit=limit,
            orderId=None,
            fromId=None,
        )
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/history/fills",
            params=mock_built_params,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        mock_response_handler.handle_get_trade_history_response.assert_called_once_with(
            mock_raw_response_list, symbol
        )
        mock_mapper.transform_raw_trades_to_internal.assert_called_once_with(
            mock_validated_raw_trades
        )
        assert result == expected_internal_trades_list

    @pytest.mark.asyncio
    async def test_get_trade_history_api_error_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_trade_history handles APIError from http_client_requester."""
        mock_built_params = {"symbol": "SOL_USDC"}
        mock_request_builder.build_get_trade_history_params.return_value = mock_built_params
        requester_api_error = APIError(
            "Trade history fetch failed", APIErrorCode.NETWORK_ISSUE.value
        )
        mock_http_client_requester.side_effect = requester_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_trade_history(symbol="SOL_USDC")
        assert excinfo.value is requester_api_error
        mock_request_builder.build_get_trade_history_params.assert_called_once()
        mock_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_trade_history_api_error_from_response_handler(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_trade_history handles APIError from response_handler."""
        symbol = "SOL_USDC"
        mock_built_params = {"symbol": symbol}
        mock_raw_response_list: list[dict[str, Any]] = [{"invalid": "trade"}]
        mock_status_code = 200

        mock_request_builder.build_get_trade_history_params.return_value = mock_built_params
        mock_http_client_requester.return_value = (mock_raw_response_list, mock_status_code, {})
        handler_api_error = APIError(
            "Invalid raw trade history", APIErrorCode.INVALID_RESPONSE.value
        )
        mock_response_handler.handle_get_trade_history_response.side_effect = handler_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_trade_history(symbol=symbol)
        assert excinfo.value is handler_api_error
        mock_response_handler.handle_get_trade_history_response.assert_called_once_with(
            mock_raw_response_list, symbol
        )

    @pytest.mark.asyncio
    async def test_get_trade_history_api_error_from_mapper(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_trade_history handles APIError from mapper."""
        symbol = "SOL_USDC"
        mock_built_params = {"symbol": symbol}
        mock_raw_trade_data = {
            "id": "tradeHist789",
            "symbol": symbol,
            "price": "-100",
        }  # Invalid price
        mock_raw_response_list: list[dict[str, Any]] = [mock_raw_trade_data]
        mock_status_code = 200
        # Pydantic validation for BackpackRawTrade will fail first if essential
        # fields like qty, time, orderId are missing
        # For this test, assume BackpackRawTrade passes, but internal Trade model
        # fails due to mapper.
        # To ensure BackpackRawTrade passes, add required fields:
        mock_raw_trade_data_complete = {
            "id": "tradeHist789",
            "symbol": symbol,
            "price": "101.5",
            "qty": "2.5",
            "time": int(datetime.now(UTC).timestamp() * 1000),
            "orderId": "orderAssociated123",
        }
        mock_validated_raw_trades = [BackpackRawTrade.model_validate(mock_raw_trade_data_complete)]

        mock_request_builder.build_get_trade_history_params.return_value = mock_built_params
        mock_http_client_requester.return_value = (mock_raw_response_list, mock_status_code, {})
        # Response handler will try to validate mock_raw_response_list (which
        # contains mock_raw_trade_data with invalid price)
        # So, to test mapper error, we need response_handler to return a valid
        # list of BackpackRawTrade
        mock_response_handler.handle_get_trade_history_response.return_value = (
            mock_validated_raw_trades
        )

        mapper_api_error = APIError("Trade history mapping failed", APIErrorCode.UNKNOWN.value)
        mock_mapper.transform_raw_trades_to_internal.side_effect = mapper_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_trade_history(symbol=symbol)
        assert excinfo.value is mapper_api_error
        mock_mapper.transform_raw_trades_to_internal.assert_called_once_with(
            mock_validated_raw_trades
        )
