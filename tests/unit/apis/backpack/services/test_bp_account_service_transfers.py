"""Unit tests for BackpackAccountService transfer functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args_models import TransferArgs
from cyberdelta.core.enums import InternalTransferStatus
from cyberdelta.core.models.operations import BackpackTransferDetails, Transfer
from cyberdelta.utils.typing import ParsedJsonResponse


class TestBackpackAccountServiceTransfers:
    """Tests for the BackpackAccountService transfer functionality."""

    @pytest.mark.asyncio
    async def test_transfer_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test transfer successfully initiates a transfer and returns an internal Transfer.

        Returns internal Transfer model.
        """
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
        mock_raw_response_content: ParsedJsonResponse = {
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

        transfer_args = TransferArgs(
            asset=asset,
            amount=amount,
            from_account_type=from_account,
            to_account_type=to_account,
            client_transfer_id=client_transfer_id,
        )
        actual_transfer = await bp_account_service.transfer(transfer_args)

        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol="USDC",
            amount=Decimal("100.0"),
            from_wallet="SPOT",
            to_wallet="FUTURES",
        )
        # Verify HTTP request was made correctly - business logic uses request_config parameter
        mock_http_client_requester.assert_called_once()
        call_args = mock_http_client_requester.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/api/v1/capital/transfer"
        assert call_args[1]["data"] == mock_payload

        mock_response_handler.handle_transfer_response.assert_called_once_with(
            mock_raw_response_content,
            200,
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
        amount = Decimal(50)
        from_account = "SPOT"
        to_account = "FUTURES"
        api_error_instance = APIError("Requester failed", code=APIErrorCode.SERVER_ERROR.value)

        expected_payload_to_requester = {"some": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = (
            expected_payload_to_requester
        )
        mock_http_client_requester.side_effect = api_error_instance

        with pytest.raises(APIError) as exc_info:
            transfer_args = TransferArgs(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )
            await bp_account_service.transfer(transfer_args)

        assert exc_info.value is api_error_instance
        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol=asset,
            amount=amount,
            from_wallet=from_account,
            to_wallet=to_account,
        )
        mock_http_client_requester.assert_called_once()
        call_pos_args, call_kwargs = mock_http_client_requester.call_args
        assert not call_pos_args
        assert call_kwargs.get("method") == "POST"
        assert call_kwargs.get("endpoint") == "/api/v1/capital/transfer"
        assert call_kwargs.get("data") == expected_payload_to_requester
        # Business logic uses request_config parameter instead of is_signed
        assert "request_config" in call_kwargs
        # Parameters structure has been updated to use request_config

    @pytest.mark.asyncio
    async def test_transfer_response_none_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test public transfer handles None response from http_client_requester.

        Should raise APIError.
        """
        asset = "BTC"
        amount = Decimal("0.1")
        from_account = "SPOT"
        to_account = "FUTURES"
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
            transfer_args = TransferArgs(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )
            await bp_account_service.transfer(transfer_args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert exc_info.value.message == expected_error_message
        assert exc_info.value.http_status == status_code_from_requester

        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol=asset,
            amount=amount,
            from_wallet=from_account,
            to_wallet=to_account,
        )
        mock_http_client_requester.assert_called_once()
        call_pos_args, call_kwargs = mock_http_client_requester.call_args
        assert not call_pos_args
        assert call_kwargs.get("method") == "POST"
        assert call_kwargs.get("endpoint") == "/api/v1/capital/transfer"
        assert call_kwargs.get("data") == expected_payload_to_requester
        # Business logic uses request_config parameter instead of is_signed
        assert "request_config" in call_kwargs
        # Parameters structure has been updated to use request_config

    @pytest.mark.asyncio
    async def test_transfer_unexpected_exception_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test public transfer handles unexpected exceptions from http_client_requester.

        Should wrap in APIError.
        """
        asset = "ETH"
        amount = Decimal("1.0")
        from_account = "MARGIN"
        to_account = "SPOT"
        unexpected_error = ValueError("Something went very wrong in requester")

        mock_payload = {"unexpected": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.side_effect = unexpected_error

        with pytest.raises(APIError) as exc_info:
            transfer_args = TransferArgs(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )
            await bp_account_service.transfer(transfer_args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Service internal logic error" in exc_info.value.message
        assert exc_info.value.original_exception is unexpected_error

        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol=asset,
            amount=amount,
            from_wallet=from_account,
            to_wallet=to_account,
        )
        # Verify HTTP request was made correctly - business logic uses request_config parameter
        mock_http_client_requester.assert_called_once()
        call_args = mock_http_client_requester.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/api/v1/capital/transfer"
        assert call_args[1]["data"] == mock_payload

    @pytest.mark.asyncio
    async def test_transfer_response_handler_returns_non_dict(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test transfer when response handler returns non-dict type."""
        asset = "USDC"
        amount = Decimal("100.0")
        from_account = "SPOT"
        to_account = "FUTURES"

        mock_payload = {"asset": asset, "amount": str(amount)}
        mock_raw_response = {"id": "transfer_123", "status": "COMPLETED"}

        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        # Return a non-dict type (like a string)
        mock_response_handler.handle_transfer_response.return_value = "invalid_non_dict_response"

        with pytest.raises(APIError) as exc_info:
            transfer_args = TransferArgs(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )
            await bp_account_service.transfer(transfer_args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Transfer response handler returned unexpected type" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_transfer_with_all_parameters(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test transfer with all optional parameters."""
        asset = "BTC"
        amount = Decimal("0.5")
        from_account = "SPOT"
        to_account = "FUTURES"
        client_transfer_id = "transfer456"

        mock_payload = {
            "symbol": asset,
            "quantity": str(amount),
            "fromAccount": from_account,
            "toAccount": to_account,
            "clientId": client_transfer_id,
        }
        mock_raw_response = {
            "id": "transfer789",
            "status": "COMPLETED",
            "message": "Transfer completed",
        }
        Transfer(
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
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_transfer_response.return_value = mock_raw_response

        transfer_args = TransferArgs(
            asset=asset,
            amount=amount,
            from_account_type=from_account,
            to_account_type=to_account,
            client_transfer_id=client_transfer_id,
        )
        result = await bp_account_service.transfer(transfer_args)

        # Business logic returns transfer with exchange name from service initialization
        assert result.id == "transfer789"
        assert result.exchange == "backpack_test_account"
        assert result.asset == asset
        assert result.quantity == amount
        assert result.status == InternalTransferStatus.COMPLETED
