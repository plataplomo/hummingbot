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
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.enums import (
    InternalTransferStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.margin_account import BackpackMarginDetails, MarginAccountSummary
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.operations import (
    BackpackTransferDetails,
    Transfer,
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
    return AsyncMock()


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
) -> BackpackAccountService:
    """Provides an instance of BackpackAccountService with mocked dependencies."""
    service = BackpackAccountService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name="backpack_test_account",
        rate_limiter_service=mock_rate_limiter_service,
    )
    return service


class TestBackpackAccountService:
    """Tests for the BackpackAccountService class."""

    @pytest.mark.asyncio
    async def test_transfer_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
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
            "id": "transfer789",
            "status": "COMPLETED",
            "message": "Transfer completed",
        }

        expected_internal_transfer = Transfer(
            id="transfer789",
            exchange="backpack_test_account",
            asset=asset,
            quantity=amount,
            status=InternalTransferStatus.COMPLETED,
            timestamp=datetime.now(UTC),
            response_message="Transfer completed",
            bp_details=BackpackTransferDetails(
                client_id=client_transfer_id,
                from_account_type=from_account,
                to_account_type=to_account,
            ),
            hl_details=None,
        )

        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response_content, 200, {})
        mock_response_handler.handle_transfer_response.return_value = mock_raw_response_content

        actual_transfer = await bp_account_service.transfer(
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
            endpoint="/api/v1/capital/transfer",
            data=mock_payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
            is_public_info_endpoint=False,
        )

        mock_response_handler.handle_transfer_response.assert_called_once_with(
            mock_raw_response_content
        )

        assert actual_transfer.id == expected_internal_transfer.id
        assert actual_transfer.exchange == expected_internal_transfer.exchange
        assert actual_transfer.asset == expected_internal_transfer.asset
        assert actual_transfer.quantity == expected_internal_transfer.quantity
        assert actual_transfer.status == expected_internal_transfer.status
        assert isinstance(actual_transfer.timestamp, datetime)
        assert actual_transfer.timestamp.tzinfo == UTC
        assert actual_transfer.response_message == expected_internal_transfer.response_message
        assert actual_transfer.bp_details == expected_internal_transfer.bp_details
        assert actual_transfer.hl_details == expected_internal_transfer.hl_details

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

        expected_payload_to_requester = {"some": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = (
            expected_payload_to_requester
        )
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
        mock_http_client_requester.assert_called_once()
        _call_pos_args, call_kwargs = mock_http_client_requester.call_args
        assert not _call_pos_args
        assert call_kwargs.get("method") == "POST"
        assert call_kwargs.get("endpoint") == "/api/v1/capital/transfer"
        assert call_kwargs.get("data") == expected_payload_to_requester
        assert call_kwargs.get("is_signed") is True
        expected_kwarg_keys = {"method", "endpoint", "data", "is_signed"}
        if "rate_limiter_service" in call_kwargs:
            expected_kwarg_keys.add("rate_limiter_service")
        if "endpoint_group" in call_kwargs:  # private endpoints
            expected_kwarg_keys.add("endpoint_group")
            expected_kwarg_keys.add("request_weight")
        if "is_public_info_endpoint" in call_kwargs:
            expected_kwarg_keys.add("is_public_info_endpoint")
        assert set(call_kwargs.keys()) == expected_kwarg_keys

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
        status_code_from_requester = 200

        expected_payload_to_requester = {"another": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = (
            expected_payload_to_requester
        )
        mock_http_client_requester.return_value = (None, status_code_from_requester, MagicMock())

        expected_error_message = (
            f"No data received for transfer, status: {status_code_from_requester}"
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
        mock_http_client_requester.assert_called_once()
        _call_pos_args, call_kwargs = mock_http_client_requester.call_args
        assert not _call_pos_args
        assert call_kwargs.get("method") == "POST"
        assert call_kwargs.get("endpoint") == "/api/v1/capital/transfer"
        assert call_kwargs.get("data") == expected_payload_to_requester
        assert call_kwargs.get("is_signed") is True
        expected_kwarg_keys = {"method", "endpoint", "data", "is_signed"}
        if "rate_limiter_service" in call_kwargs:
            expected_kwarg_keys.add("rate_limiter_service")
        if "endpoint_group" in call_kwargs:  # private endpoints
            expected_kwarg_keys.add("endpoint_group")
            expected_kwarg_keys.add("request_weight")
        if "is_public_info_endpoint" in call_kwargs:
            expected_kwarg_keys.add("is_public_info_endpoint")
        assert set(call_kwargs.keys()) == expected_kwarg_keys

    @pytest.mark.asyncio
    async def test_transfer_unexpected_exception_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_rate_limiter_service: AsyncMock,
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

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.transfer(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Processing transfer data failed: {unexpected_error}" in exc_info.value.message
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
            endpoint="/api/v1/capital/transfer",
            data=mock_payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
            is_public_info_endpoint=False,
        )

    @pytest.mark.asyncio
    async def test_get_balances_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test _get_raw_balances_dict successfully fetches and processes balance data,
        tested via public get_balances."""
        mock_raw_response_data_dict: RawJsonResponse = {
            "USDC": {"available": "1000.0", "locked": "0", "debt": "0", "total": "1000.0"}
        }
        mock_validated_raw_balances_dict: dict[str, BackpackRawBalance] = {
            "USDC": BackpackRawBalance(asset="USDC", available="1000.0", total="1000.0")
        }

        mock_internal_spot_balance = SpotBalance(
            exchange="backpack_test_account",
            asset="USDC",
            timestamp=datetime.now(UTC),
            total_quantity=Decimal("1000.0"),
            available_quantity=Decimal("1000.0"),
        )
        expected_final_balances_result: dict[str, SpotBalance] = {
            "USDC": mock_internal_spot_balance
        }

        mock_request_builder.build_get_balances_params.return_value = None
        mock_http_client_requester.return_value = (mock_raw_response_data_dict, 200, MagicMock())
        mock_response_handler.handle_get_balances_response.return_value = (
            mock_validated_raw_balances_dict
        )

        mock_mapper.transform_raw_balance_to_internal.return_value = mock_internal_spot_balance

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_balances()

        mock_request_builder.build_get_balances_params.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/capital",
            params=None,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
            is_public_info_endpoint=False,
        )
        mock_response_handler.handle_get_balances_response.assert_called_once_with(
            mock_raw_response_data_dict
        )
        mock_mapper.transform_raw_balance_to_internal.assert_called_once_with(
            "USDC", mock_validated_raw_balances_dict["USDC"]
        )

        assert result["USDC"].exchange == expected_final_balances_result["USDC"].exchange
        assert result["USDC"].asset == expected_final_balances_result["USDC"].asset
        assert isinstance(result["USDC"].timestamp, datetime)
        assert result["USDC"].timestamp.tzinfo == UTC
        assert (
            result["USDC"].total_quantity == expected_final_balances_result["USDC"].total_quantity
        )
        assert (
            result["USDC"].available_quantity
            == expected_final_balances_result["USDC"].available_quantity
        )

    @pytest.mark.asyncio
    async def test_get_positions_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test _get_raw_positions_list successfully fetches and processes position data,
        tested via public get_positions."""
        symbol_arg = "SOL-PERP"
        mock_raw_positions_data_item_dict: RawJsonResponse = {
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
        mock_validated_raw_positions = [
            BackpackRawPosition.model_validate(mock_raw_positions_data_item_dict)
        ]

        mock_internal_derivative_position = DerivativePosition(
            exchange="backpack_test_account",
            symbol=symbol_arg,
            timestamp=datetime.now(UTC),
            side=OrderSide.BUY,
            size=Decimal("10.0"),
            entry_price=Decimal("100.0"),
            mark_price=Decimal("110.0"),
            unrealized_pnl=Decimal("100.0"),
            realized_pnl=Decimal("0.0"),
            liquidation_price=Decimal("90.0"),
            bp_details=None,
            hl_details=None,
        )
        expected_positions_result: list[DerivativePosition] = [mock_internal_derivative_position]

        def build_get_positions_params_side_effect(
            symbol: str | None = None,
        ) -> dict[str, str] | None:
            if symbol is None:
                return None
            return {"symbol": symbol}

        mock_request_builder.build_get_positions_params.side_effect = (
            build_get_positions_params_side_effect
        )

        mock_http_client_requester.return_value = (
            mock_raw_positions_data_item_dict,
            200,
            MagicMock(),
        )
        mock_response_handler.handle_get_positions_response.return_value = (
            mock_validated_raw_positions
        )
        mock_mapper.transform_raw_position_to_internal.return_value = (
            mock_internal_derivative_position
        )

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result_no_symbol = await bp_account_service.get_positions(symbol=None)

        mock_request_builder.build_get_positions_params.assert_called_with(None)
        mock_http_client_requester.assert_called_with(
            method="GET",
            endpoint="/api/v1/positions",  # Endpoint for no symbol
            params=None,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
            is_public_info_endpoint=False,
        )
        mock_response_handler.handle_get_positions_response.assert_called_with(
            mock_raw_positions_data_item_dict, None
        )
        mock_mapper.transform_raw_position_to_internal.assert_called_with(
            mock_validated_raw_positions[0]
        )
        assert len(result_no_symbol) == len(expected_positions_result)
        for actual, expected in zip(result_no_symbol, expected_positions_result, strict=False):
            assert actual.exchange == expected.exchange
            assert actual.symbol == expected.symbol
            assert isinstance(actual.timestamp, datetime)
            assert actual.timestamp.tzinfo == UTC
            assert actual.side == expected.side
            assert actual.size == expected.size
            assert actual.entry_price == expected.entry_price
            assert actual.mark_price == expected.mark_price
            assert actual.unrealized_pnl == expected.unrealized_pnl

        # --- Test with symbol ---
        mock_http_client_requester.reset_mock()
        mock_response_handler.reset_mock()
        mock_request_builder.build_get_positions_params.reset_mock()
        mock_mapper.transform_raw_position_to_internal.reset_mock()

        # Reconfigure mocks for the call with symbol
        mock_response_handler.handle_get_positions_response.return_value = (
            mock_validated_raw_positions  # Still returns list
        )
        mock_mapper.transform_raw_position_to_internal.return_value = (
            mock_internal_derivative_position
        )

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result_with_symbol = await bp_account_service.get_positions(symbol=symbol_arg)

        mock_request_builder.build_get_positions_params.assert_called_with(symbol_arg)
        mock_http_client_requester.assert_called_with(
            method="GET",
            endpoint="/api/v1/positions",
            params={"symbol": symbol_arg},
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
            is_public_info_endpoint=False,
        )
        mock_response_handler.handle_get_positions_response.assert_called_with(
            mock_raw_positions_data_item_dict, symbol_arg
        )
        mock_mapper.transform_raw_position_to_internal.assert_called_with(
            mock_validated_raw_positions[0]
        )
        assert len(result_with_symbol) == len(expected_positions_result)
        for actual, expected in zip(result_with_symbol, expected_positions_result, strict=False):
            assert actual.exchange == expected.exchange
            assert actual.symbol == expected.symbol
            assert isinstance(actual.timestamp, datetime)
            assert actual.timestamp.tzinfo == UTC
            assert actual.side == expected.side
            assert actual.size == expected.size
            assert actual.entry_price == expected.entry_price
            assert actual.mark_price == expected.mark_price
            assert actual.unrealized_pnl == expected.unrealized_pnl

    @pytest.mark.asyncio
    async def test_get_account_info_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_account_info successfully fetches and processes account info
        by mocking its internal helper methods that perform raw data fetching.
        """
        mock_raw_account_data = {
            "autoBorrowSettlements": True,
            "autoLend": False,
            "autoRealizePnl": True,
            "autoRepayBorrows": False,
            "borrowLimit": "10000.00",
            "futuresMakerFee": "0.0002",
            "futuresTakerFee": "0.0005",
            "leverageLimit": "20.00000000",
            "limitOrders": 50,
            "liquidating": False,
            "positionLimit": "500000.00",
            "spotMakerFee": "0.001",
            "spotTakerFee": "0.001",
            "triggerOrders": 10,
        }
        mock_validated_raw_account_summary = BackpackRawAccountSummary.model_validate(
            mock_raw_account_data
        )
        mock_empty_raw_balances: dict[str, BackpackRawBalance] = {}
        mock_empty_raw_positions: list[BackpackRawPosition] = []

        mock_bp_details = BackpackMarginDetails(
            imf_raw=str(mock_validated_raw_account_summary.leverage_limit),
        )
        mock_internal_margin_summary = MarginAccountSummary(
            exchange="backpack_test_account",
            timestamp=datetime.now(UTC),
            total_equity=Decimal("0"),
            available_equity=Decimal("0"),
            bp_details=mock_bp_details,
            total_initial_margin_required=Decimal("0"),
            total_maintenance_margin_required=Decimal("0"),
            total_position_notional=Decimal("0"),
        )

        # Patch the internal helper methods of the service instance
        with (
            patch.object(
                bp_account_service,
                "_get_raw_account_summary_obj",
                new_callable=AsyncMock,
                return_value=mock_validated_raw_account_summary,
            ) as mock_get_summary_obj,
            patch.object(
                bp_account_service,
                "_get_raw_balances_dict",
                new_callable=AsyncMock,
                return_value=mock_empty_raw_balances,
            ) as mock_get_balances_dict,
            patch.object(
                bp_account_service,
                "_get_raw_positions_list",
                new_callable=AsyncMock,
                return_value=mock_empty_raw_positions,
            ) as mock_get_positions_list,
            patch.object(bp_account_service, "_mapper", mock_mapper),
        ):  # Patch the mapper as before
            mock_mapper.transform_raw_account_summary_to_internal.return_value = (
                mock_internal_margin_summary
            )
            result = await bp_account_service.get_account_info()

        mock_get_summary_obj.assert_called_once_with()
        mock_get_balances_dict.assert_called_once_with()
        mock_get_positions_list.assert_called_once_with()

        mock_mapper.transform_raw_account_summary_to_internal.assert_called_once_with(
            raw_settings=mock_validated_raw_account_summary,
            spot_balances_raw=mock_empty_raw_balances,
            derivative_positions_raw=mock_empty_raw_positions,
        )
        assert result.exchange == mock_internal_margin_summary.exchange
        assert isinstance(result.timestamp, datetime)
        assert result.timestamp.tzinfo == UTC
        assert result.total_equity == mock_internal_margin_summary.total_equity
        assert result.available_equity == mock_internal_margin_summary.available_equity
        assert result.bp_details == mock_internal_margin_summary.bp_details

    @pytest.mark.asyncio
    async def test_get_balances_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_rate_limiter_service: AsyncMock,
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
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("1010.5"),
                available_quantity=Decimal("1000.5"),
            ),
            "SOL": SpotBalance(
                exchange="backpack_test_account",
                asset="SOL",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("50.7"),
                available_quantity=Decimal("50.2"),
            ),
        }

        usdc_spot_balance = SpotBalance(
            exchange="backpack_test_account",
            asset="USDC",
            timestamp=datetime.now(UTC),
            total_quantity=Decimal("1010.5"),
            available_quantity=Decimal("1000.5"),
        )
        sol_spot_balance = SpotBalance(
            exchange="backpack_test_account",
            asset="SOL",
            timestamp=datetime.now(UTC),
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
            is_public_info_endpoint=False,
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

        assert len(result_balances) == len(expected_internal_balances)
        for asset_key in expected_internal_balances:
            assert asset_key in result_balances
            actual_bal = result_balances[asset_key]
            expected_bal = expected_internal_balances[asset_key]
            assert actual_bal.exchange == expected_bal.exchange
            assert actual_bal.asset == expected_bal.asset
            assert isinstance(actual_bal.timestamp, datetime)
            assert actual_bal.timestamp.tzinfo == UTC
            assert actual_bal.total_quantity == expected_bal.total_quantity
            assert actual_bal.available_quantity == expected_bal.available_quantity

    @pytest.mark.asyncio
    async def test_get_order_history_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_rate_limiter_service: AsyncMock,
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
            updated_at=start_time,  # Assuming updated_at is same as created_at for this mock
            client_order_id="mock_client_order_id",  # This should come from mapper or be None
            quantity_filled=Decimal("10"),
            average_fill_price=Decimal("100"),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            bp_details=None,  # Add missing field
            hl_details=None,  # Add missing field
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
        mock_mapper.transform_raw_order_to_internal.return_value = expected_internal_order

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_order_history(
                symbol=symbol, limit=limit, start_time=start_time, end_time=end_time
            )

        mock_request_builder.build_get_order_history_params.assert_called_once_with(
            symbol=symbol,
            start_time_ms=int(start_time.timestamp() * 1000),
            end_time_ms=int(end_time.timestamp() * 1000),
            limit=limit,
            order_id=None,
            client_order_id=None,
        )
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/history/orders",
            params=mock_built_params,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
            is_public_info_endpoint=False,
        )
        mock_response_handler.handle_get_order_history_response.assert_called_once_with(
            mock_raw_response_list, symbol
        )
        mock_mapper.transform_raw_order_to_internal.assert_called_once_with(
            mock_validated_raw_orders[0]
        )
        assert len(result) == len(expected_internal_orders_list)
        for actual, expected in zip(result, expected_internal_orders_list, strict=False):
            assert actual.exchange_order_id == expected.exchange_order_id
            assert actual.exchange == expected.exchange
            assert actual.symbol == expected.symbol
            assert actual.side == expected.side
            assert actual.order_type == expected.order_type
            assert actual.status == expected.status
            assert actual.quantity_requested == expected.quantity_requested
            assert actual.price == expected.price
            assert actual.time_in_force == expected.time_in_force
            assert actual.created_at == expected.created_at
            # Timestamps can be tricky, ensure they are datetimes and UTC for actual
            assert isinstance(actual.updated_at, datetime)
            assert actual.updated_at.tzinfo == UTC
            # For expected, it's already set to start_time (which is UTC)
            # We might need to mock datetime.now(UTC) in the mapper if it's used for updated_at
            # For now, if expected.updated_at is fixed, compare directly if appropriate
            assert actual.updated_at == expected.updated_at

            assert actual.client_order_id == expected.client_order_id
            assert actual.quantity_filled == expected.quantity_filled
            assert actual.average_fill_price == expected.average_fill_price
            assert actual.triggered_at == expected.triggered_at
            assert actual.strategy_name == expected.strategy_name
            assert actual.signal_id == expected.signal_id
            assert actual.bp_details == expected.bp_details
            assert actual.hl_details == expected.hl_details

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
            "status": "FILLED",
            "timeInForce": "GTC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "1",
            "price": "1",
            "createdAt": 1234567890000,
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
        mock_mapper.transform_raw_order_to_internal.side_effect = mapper_api_error

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            with pytest.raises(APIError) as excinfo:
                await bp_account_service.get_order_history(symbol=symbol)
            assert excinfo.value is mapper_api_error

    @pytest.mark.asyncio
    async def test_get_order_history_response_none_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_order_history when HTTP client returns None content."""
        symbol = "SOL_USDC"
        limit = 10

        mock_params = {"symbol": symbol, "limit": limit}
        mock_request_builder.build_get_order_history_params.return_value = mock_params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            with pytest.raises(APIError) as exc_info:
                await bp_account_service.get_order_history(symbol=symbol, limit=limit)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert "No data received for order history, status: 200" in exc_info.value.message

            mock_request_builder.build_get_order_history_params.assert_called_once_with(
                symbol=symbol,
                start_time_ms=None,
                end_time_ms=None,
                limit=limit,
                order_id=None,
                client_order_id=None,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/history/orders",
                params=mock_params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
                is_public_info_endpoint=False,
            )
            mock_response_handler.handle_get_order_history_response.assert_not_called()
            mock_mapper.transform_raw_order_to_internal.assert_not_called()
            if hasattr(mock_mapper, "transform_raw_orders_to_internal_list"):
                mock_mapper.transform_raw_orders_to_internal_list.assert_not_called()
