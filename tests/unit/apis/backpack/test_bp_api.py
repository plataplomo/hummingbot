from __future__ import annotations

import asyncio
import json
import unittest.mock
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import pytest_asyncio
from _pytest.logging import LogCaptureFixture

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_position import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
    BackpackRawPosition,
)
from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.connectivity.http_client import HttpRequestFailedError
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import MarginAccountSummary
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.margin_account import BackpackMarginDetails
from cyberdelta.core.models.market.order import Order

# from cyberdelta.shared_logger import logger # Comment out if still unresolved


@pytest.fixture
def mock_loop() -> MagicMock:
    return MagicMock(spec=asyncio.AbstractEventLoop)


@pytest.fixture
def default_bp_config() -> dict[str, Any]:
    return {
        "exchange_name": "backpack",
        "rest_endpoint": "https://api.backpack.test",
        "ws_endpoint": "wss://ws.backpack.test",
        "rate_limits": {"default_rate": 10, "default_bucket_size": 10},
    }


@pytest.fixture
def bp_secrets_valid() -> dict[str, str | None]:
    return {"BACKPACK_API_KEY": "test_key", "BACKPACK_API_SECRET": "test_secret"}


@pytest.fixture
def bp_secrets_invalid() -> dict[str, str | None]:
    return {"BACKPACK_API_KEY": None, "BACKPACK_API_SECRET": None}


@pytest.fixture
def mock_bp_authenticator_instance() -> MagicMock:
    mock_auth = MagicMock(spec=BackpackHmacAuthenticator)
    mock_auth.prepare_request = AsyncMock(
        return_value=AuthenticatedRequestComponents(
            headers={"X-Test-Signed": "true"}, params=None, data=None
        )
    )
    return mock_auth


@pytest.fixture
def mock_raw_account_fixture() -> dict[str, Any]:
    """Provides a valid raw data dictionary for BackpackRawAccount."""
    return {"id": "test-account-id-123", "email": "testuser@example.com", "status": "active"}


@pytest.fixture
def mock_raw_account_summary_fixture() -> BackpackRawAccountSummary:
    return BackpackRawAccountSummary(
        autoBorrowSettlements=True,
        autoLend=False,
        autoRealizePnl=True,
        autoRepayBorrows=True,
        borrowLimit=Decimal("100000.0"),
        futuresMakerFee=Decimal("0.0002"),
        futuresTakerFee=Decimal("0.0005"),
        leverageLimit=Decimal("20.0"),
        limitOrders=50,
        liquidating=False,
        positionLimit=Decimal("500000.0"),
        spotMakerFee=Decimal("0.0008"),
        spotTakerFee=Decimal("0.0010"),
        triggerOrders=10,
    )


@pytest.fixture
def mock_raw_balances_dict_fixture() -> dict[str, BackpackRawBalance]:
    return {
        "USDC": BackpackRawBalance(total="10000.0", available="8000.0", asset="USDC"),
        "SOL": BackpackRawBalance(total="50.0", available="40.0", asset="SOL"),
    }


@pytest.fixture
def mock_raw_positions_list_fixture() -> list[BackpackRawPosition]:
    return [
        BackpackRawPosition(
            symbol="SOL-PERP",
            netQuantity="2.5",
            entryPrice="130.00",
            markPrice="135.00",
            imf="0.1",
            imfFunction=BackpackRawImfFunction(base="0.005", factor="0.000001"),
            mmf="0.05",
            mmfFunction=BackpackRawMmfFunction(base="0.002", factor="0.0000005"),
            netCost="325.00",
            netExposureQuantity="2.5",
            netExposureNotional="337.50",
            pnlRealized="10.00",
            pnlUnrealized="12.50",
            cumulativeFundingPayment="-0.50",
            userId=123,
            positionId="pos1",
            estLiquidationPrice="100.00",
            breakEvenPrice="132.00",
            cumulativeInterest="0.0",
        )
    ]


@pytest.fixture
def expected_margin_account_summary_fixture(
    mock_raw_account_summary_fixture: BackpackRawAccountSummary,
) -> MarginAccountSummary:
    return MarginAccountSummary(
        exchange="backpack",
        timestamp=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
        total_equity=Decimal("10000.0"),
        available_equity=Decimal("8000.0"),
        total_initial_margin_required=None,
        total_maintenance_margin_required=None,
        total_position_notional=Decimal("325.0"),
        total_unrealized_pnl=Decimal("12.50"),
        bp_details=BackpackMarginDetails(
            assets_value=Decimal("10000.0"),
            borrow_liability=None,
            liabilities_value=None,
            locked_equity=None,
            margin_fraction=None,
            imf_raw=None,
            mmf_raw=None,
        ),
        hl_details=None,
    )


@pytest.fixture
def raw_dict_for_account_summary_response() -> dict[str, Any]:
    """Provides a raw dictionary similar to the JSON response for account summary."""
    return {
        "autoBorrowSettlements": True,
        "autoLend": False,
        "autoRealizePnl": True,
        "autoRepayBorrows": True,
        "borrowLimit": "100000.0",
        "futuresMakerFee": "0.0002",
        "futuresTakerFee": "0.0005",
        "leverageLimit": "20.0",
        "limitOrders": 50,
        "liquidating": False,
        "positionLimit": "500000.0",
        "spotMakerFee": "0.0008",
        "spotTakerFee": "0.0010",
        "triggerOrders": 10,
    }


class TestBackpackAPI_Authentication:
    def test_backpack_api_initialization_with_valid_secrets(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
        mock_loop: MagicMock,  # mock_loop might be unused if not passed to API
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        # We expect the authenticator to be an instance of BackpackHmacAuthenticator
        # if valid secrets are provided. Accessing api.authenticator is public.
        assert api.authenticator is not None
        assert isinstance(api.authenticator, BackpackHmacAuthenticator)
        # These internal checks can be removed if we trust the constructor sets them
        # based on the public authenticator. For now, keeping to see if linter still flags.
        assert api._api_key == "test_key"
        assert api._api_secret == "test_secret"

    def test_backpack_api_initialization_with_invalid_secrets(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_invalid: dict[str, str | None],
        mock_loop: MagicMock,  # mock_loop might be unused
        caplog: LogCaptureFixture,
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_invalid)
        assert api.authenticator is None  # Public accessor
        assert "Authenticator not initialized" in caplog.text

    @pytest.mark.asyncio
    async def test_authenticate_delegates_to_bp_authenticator_prepare_request(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
    ) -> None:
        """Test that _authenticate method correctly uses the BackpackHmacAuthenticator's
        prepare_request."""
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        assert api.authenticator is not None  # Should be initialized

        method = "GET"
        path = "/api/v1/capital"
        params = {"test_param": "value"}
        data = {"test_data": "value"}

        expected_components_from_auth = AuthenticatedRequestComponents(
            headers={"X-BP-ApiKey": "test_key", "X-BP-Signature": "signed_value"},
            params=params,
            data=data,
        )

        with patch.object(
            api.authenticator, "prepare_request", new_callable=AsyncMock
        ) as mock_prepare_request:
            mock_prepare_request.return_value = expected_components_from_auth

            # Call the _authenticate method directly (still testing this internal,
            # but with instance's authenticator)
            auth_result_dict = await api._authenticate(method, path, params, data)  # type: ignore[attr-defined]

            mock_prepare_request.assert_called_once_with(
                method=method,
                path=path,
                params=params,
                data=data,
                headers=api.default_headers.copy(),
            )
            assert auth_result_dict["headers"] == expected_components_from_auth["headers"]
            assert auth_result_dict["params"] == expected_components_from_auth["params"]
            assert auth_result_dict["data"] == expected_components_from_auth["data"]

    @pytest.mark.asyncio
    async def test_authenticate_raises_if_authenticator_not_initialized(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_invalid: dict[str, str | None],
        mock_loop: MagicMock,  # mock_loop might be unused
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_invalid)
        assert api.authenticator is None  # Ensure it's not initialized

        with pytest.raises(APIError) as exc_info:
            await api._authenticate("GET", "/test", None, None)
        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert "Backpack authenticator not initialized" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_signed_public_method_uses_authenticator(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
    ) -> None:
        """
        Test that a signed public method (e.g., place_order) correctly uses the
        authenticator's prepare_request method via the internal _authenticate call,
        and then calls _request with the authenticated components.
        """
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        assert api.authenticator is not None

        expected_builder_payload = {
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "1",
            "price": "100",
            "timeInForce": "GTC",
        }
        authenticated_components = AuthenticatedRequestComponents(
            headers={**api.default_headers, "X-BP-Signature": "mock_signature"},
            params=None,
            data=expected_builder_payload,
        )
        mock_order_response_content = {"id": "123", "status": "NEW", "symbol": "SOL_USDC"}

        with (
            patch.object(
                api.authenticator, "prepare_request", new_callable=AsyncMock
            ) as mock_prepare_request,
            patch.object(api, "_request", new_callable=AsyncMock) as mock_api_request,
            patch(
                "cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder.build_place_order_payload"
            ) as mock_build_payload,
            patch.object(api._bp_mapper, "transform_raw_order_to_internal") as mock_transform_order,
        ):  # type: ignore[attr-defined]
            mock_build_payload.return_value = expected_builder_payload
            mock_prepare_request.return_value = authenticated_components
            mock_api_request.return_value = mock_order_response_content

            mock_transformed_order = MagicMock(spec=Order)
            mock_transform_order.return_value = mock_transformed_order

            await api.place_order(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1"),
                price=Decimal("100"),
                time_in_force=TimeInForce.GTC,
            )

            mock_build_payload.assert_called_once_with(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1"),
                price=Decimal("100"),
                time_in_force=TimeInForce.GTC,
                client_order_id=None,
                post_only=False,
                trigger_price=None,
            )

            mock_prepare_request.assert_called_once()
            call_args_to_prepare = mock_prepare_request.call_args
            assert call_args_to_prepare is not None
            assert call_args_to_prepare.kwargs.get("data") == expected_builder_payload

            mock_api_request.assert_called_once_with(
                method="POST",
                endpoint="/api/v1/order",
                params=authenticated_components["params"],
                data=authenticated_components["data"],
                headers=authenticated_components["headers"],
                is_signed=True,  # This flag is for _request internal logic,
                # not passed to HttpClient
            )

            mock_transform_order.assert_called_once_with(
                unittest.mock.ANY  # Using ANY because the raw order model is
                # complex to reconstruct here
            )


class TestBackpackAPIMethodErrors:
    @pytest.mark.filterwarnings("ignore:unclosed <socket.socket.*>:ResourceWarning")
    @pytest.mark.filterwarnings("ignore:unclosed event loop.*:ResourceWarning")
    @pytest.mark.asyncio
    async def test_get_ticker_handles_mapped_invalid_symbol_error(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
    ) -> None:
        """Test get_ticker correctly maps an HTTP error from _request."""
        api = BackpackAPI(default_bp_config, bp_secrets_valid)

        http_status_from_exchange = 400
        # This body structure is for BackpackErrorMapper to parse
        error_body_from_exchange_dict = {
            "message": "Invalid symbol provided via test.",
            "code": "INVALID_SYMBOL",
        }
        error_body_json_str = json.dumps(error_body_from_exchange_dict)

        # Simulate that api._request (which calls HttpClient.request) raises HttpRequestFailedError
        http_failure = HttpRequestFailedError(
            message="HTTP 400 Error from _request",  # Message from HttpRequestFailedError
            http_status_code=http_status_from_exchange,
            response_body=error_body_json_str,  # Must be string for HttpRequestFailedError
        )

        # Patch api._request directly
        with (
            patch.object(api, "_request", side_effect=http_failure) as mock_api_request,
            patch(
                "cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder.build_get_ticker_params"
            ) as mock_build_params,
        ):
            expected_params_from_builder = {"symbol": "XYZ_USDC"}
            mock_build_params.return_value = expected_params_from_builder

            with pytest.raises(APIError) as exc_info:
                await api.get_ticker(symbol="XYZ_USDC")

            mock_build_params.assert_called_once_with(symbol="XYZ_USDC")

            # Verify api._request was called by get_ticker with correct params
            mock_api_request.assert_called_once_with(
                "GET",  # method
                "/api/v1/ticker",  # endpoint
                params=expected_params_from_builder,
                # is_signed is False by default for get_ticker in _request call
            )

            assert exc_info.value.code == APIErrorCode.INVALID_SYMBOL.value
            assert exc_info.value.http_status == http_status_from_exchange
            # APIError.message should now be based on the mapped error from BackpackErrorMapper
            assert "Invalid symbol provided via test." in exc_info.value.message
            # APIError.exchange_message should be the message from the raw error dict
            assert exc_info.value.exchange_message == error_body_from_exchange_dict.get("message")

    @pytest.mark.asyncio
    async def test_place_order_handles_mapped_insufficient_funds_error(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
    ) -> None:
        """Test place_order maps insufficient funds error from _request via BackpackErrorMapper."""
        api = BackpackAPI(default_bp_config, bp_secrets_valid)

        http_status_from_exchange = 400
        error_body_dict = {
            "message": "Account has insufficient balance for requested action.",
            "code": "INSUFFICIENT_FUNDS",
        }
        error_body_json_str = json.dumps(error_body_dict)

        http_failure = HttpRequestFailedError(
            message="HTTP 400 Error from _request",
            http_status_code=http_status_from_exchange,
            response_body=error_body_json_str,
        )

        # Patch api._request directly
        with (
            patch.object(api, "_request", side_effect=http_failure) as mock_api_request,
            patch(
                "cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder.build_place_order_payload"
            ) as mock_build_payload,
        ):
            expected_payload = {
                "symbol": "BTC_USDC",
                "side": "Buy",
                "orderType": "Limit",
                "quantity": "1.0",
                "price": "50000",
                "timeInForce": "GTC",
            }
            mock_build_payload.return_value = expected_payload

            with pytest.raises(APIError) as exc_info:
                await api.place_order(
                    symbol="BTC_USDC",
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("1.0"),
                    price=Decimal("50000"),
                    time_in_force=TimeInForce.GTC,
                )

            mock_build_payload.assert_called_once()
            # Verify api._request was called by place_order
            mock_api_request.assert_called_once_with(
                method="POST", endpoint="/api/v1/order", data=expected_payload, is_signed=True
            )

            assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
            assert exc_info.value.http_status == http_status_from_exchange
            assert "Account has insufficient balance" in exc_info.value.message
            assert exc_info.value.exchange_message == error_body_dict.get("message")

    # TODO: Add more error tests for other methods and error types


class TestBackpackAPIWebSocketRouting:
    """Tests WebSocket message routing logic within BackpackAPI."""

    @pytest_asyncio.fixture
    async def api_for_ws_tests(
        self, default_bp_config: dict[str, Any], bp_secrets_valid: dict[str, str | None]
    ) -> AsyncGenerator[BackpackAPI]:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        # Mock the WebSocketManager instance used by the API
        mock_ws_manager = AsyncMock()
        mock_ws_manager.send_json = AsyncMock()
        mock_ws_manager.close = AsyncMock()
        # If WebSocketManager has an is_connected property or similar, mock it if needed
        api._ws_manager = mock_ws_manager
        try:
            yield api
        finally:
            await api.close()  # Ensures http_client session is closed

    @pytest.mark.asyncio
    async def test_construct_subscription_payload(self, api_for_ws_tests: BackpackAPI) -> None:
        topic = "depth.SOL_USDC"
        payload = api_for_ws_tests._construct_subscription_payload(topic)
        assert payload == {
            "op": "subscribe",
            "channel": topic,
            "args": {},
        }

    @pytest.mark.asyncio
    async def test_route_ws_message_public_topic_handler_called(
        self, api_for_ws_tests: BackpackAPI
    ) -> None:
        mock_handler = AsyncMock()
        topic = "depth.SOL_USDC"

        # Use public subscribe method
        await api_for_ws_tests.subscribe(topic, mock_handler)
        # Verify subscribe called ws_manager.send_json (implicitly tests _construct_payload)
        assert api_for_ws_tests._ws_manager is not None
        api_for_ws_tests._ws_manager.send_json.assert_called_with(  # type: ignore[attr-defined]
            {
                "op": "subscribe",
                "channel": topic,
                "args": {},
            }
        )

        test_message_data = {"bids": [["100", "1"]], "asks": [["101", "2"]]}
        test_message = {"topic": topic, "data": test_message_data}

        await api_for_ws_tests._handle_websocket_message(test_message)
        mock_handler.assert_called_once_with(test_message_data, test_message)

    @pytest.mark.asyncio
    async def test_route_ws_message_private_topic_handler_called(
        self, api_for_ws_tests: BackpackAPI
    ) -> None:
        mock_handler = AsyncMock()
        topic_internal = "fills"  # This is treated as a topic by BackpackAPI

        await api_for_ws_tests.subscribe(topic_internal, mock_handler)
        assert api_for_ws_tests._ws_manager is not None
        api_for_ws_tests._ws_manager.send_json.assert_called_with(  # type: ignore[attr-defined]
            {
                "op": "subscribe",
                "channel": topic_internal,
                "args": {},
            }
        )

        test_fill_data = {"id": "fill123", "price": "150", "qty": "0.5"}
        # For private streams like 'fills', BackpackAPI's _route_ws_message uses 'type' as key
        test_message = {"type": topic_internal, "data": test_fill_data}

        await api_for_ws_tests._handle_websocket_message(test_message)
        # The handler for 'fills' expects the full message as data_payload and full message again
        mock_handler.assert_called_once_with(test_message, test_message)

    @pytest.mark.asyncio
    async def test_route_ws_message_no_handler_logs_debug(
        self,
        api_for_ws_tests: BackpackAPI,
        caplog: LogCaptureFixture,
    ) -> None:
        test_message = {"topic": "unhandled.topic", "data": {"key": "value"}}
        # Ensure no handler is registered for this topic by not calling subscribe
        # Or explicitly clear if necessary: api_for_ws_tests._ws_handlers.clear()
        api_for_ws_tests._ws_handlers.clear()

        with patch("cyberdelta.apis.backpack.bp_api.logger.debug") as mock_logger_debug:
            await api_for_ws_tests._handle_websocket_message(test_message)
            mock_logger_debug.assert_called_once_with(
                f"[{api_for_ws_tests.exchange_name}] No handler registered for topic: "
                f"unhandled.topic"
            )

    @pytest.mark.asyncio
    async def test_route_ws_message_no_topic_or_type_logs_debug(
        self,
        api_for_ws_tests: BackpackAPI,
        caplog: LogCaptureFixture,
    ) -> None:
        test_message = {
            "event": "random_event",  # Neither 'topic' nor 'type'
            "data": {"key": "value"},
        }
        with patch("cyberdelta.apis.backpack.bp_api.logger.debug") as mock_logger_debug:
            await api_for_ws_tests._handle_websocket_message(test_message)
            mock_logger_debug.assert_called_once_with(
                f"[{api_for_ws_tests.exchange_name}] Unroutable message (no clear string "
                f"topic/type): {test_message}"
            )

    @pytest.mark.asyncio
    async def test_route_ws_message_topic_no_data_logs_debug(
        self, api_for_ws_tests: BackpackAPI, caplog: LogCaptureFixture
    ) -> None:
        mock_handler = AsyncMock()
        topic = "public.depth.SOL_USDC"
        await api_for_ws_tests.subscribe(topic, mock_handler)  # Register a handler

        test_message_no_data = {"topic": topic}  # Message has topic but no 'data' field
        with patch("cyberdelta.apis.backpack.bp_api.logger.debug") as mock_logger_debug:
            await api_for_ws_tests._handle_websocket_message(test_message_no_data)
            mock_logger_debug.assert_called_once_with(
                f"[{api_for_ws_tests.exchange_name}] Received message with topic '{topic}'"
                f" but no data: {test_message_no_data}"
            )
            mock_handler.assert_not_called()  # Handler should not be called if data is missing


class TestBackpackAPIGetAccountSummary:
    @pytest.mark.asyncio
    async def test_get_account_summary_success(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
        mock_raw_account_summary_fixture: BackpackRawAccountSummary,
        mock_raw_balances_dict_fixture: dict[str, BackpackRawBalance],
        mock_raw_positions_list_fixture: list[BackpackRawPosition],
        expected_margin_account_summary_fixture: MarginAccountSummary,
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        fixed_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        current_expected_summary = expected_margin_account_summary_fixture.model_copy(
            update={"timestamp": fixed_now}, deep=True
        )
        mock_get_account_info = AsyncMock(return_value=mock_raw_account_summary_fixture)
        mock_handle_balances_response = MagicMock(return_value=mock_raw_balances_dict_fixture)
        mock_handle_positions_response = MagicMock(return_value=mock_raw_positions_list_fixture)

        with (
            patch.object(api, "get_account_info", mock_get_account_info) as _mock_get_info,
            patch(
                "cyberdelta.apis.backpack.bp_api.BackpackResponseHandler.handle_get_balances_response",
                mock_handle_balances_response,
            ) as mock_handler_balances,
            patch(
                "cyberdelta.apis.backpack.bp_api.BackpackResponseHandler.handle_get_positions_response",
                mock_handle_positions_response,
            ) as mock_handler_positions,
            patch(
                "cyberdelta.apis.backpack.bp_order_mapper.datetime", wraps=datetime
            ) as mock_mapper_dt,
            patch.object(api, "_request") as mock_api_request,
        ):
            mock_mapper_dt.utcnow.return_value = fixed_now

            async def mock_request_side_effect(
                method: str, endpoint: str, **kwargs: dict[str, Any]
            ) -> dict[str, Any] | list[Any]:
                if endpoint.endswith("/api/v1/capital"):
                    return {
                        k: v.model_dump(by_alias=True)
                        for k, v in mock_raw_balances_dict_fixture.items()
                    }
                elif endpoint.endswith("/api/v1/positions"):
                    return [
                        pos.model_dump(by_alias=True) for pos in mock_raw_positions_list_fixture
                    ]
                raise ValueError(
                    f"Unexpected endpoint in success test mock_request_side_effect: {endpoint}"
                )

            mock_api_request.side_effect = mock_request_side_effect
            api._bp_mapper = MagicMock()
            api._bp_mapper.transform_raw_account_summary_to_internal = MagicMock(
                return_value=current_expected_summary
            )
            result = await api.get_account_summary()
            assert result == current_expected_summary
            _mock_get_info.assert_called_once()
            expected_calls_to_request = [
                call("GET", "/api/v1/capital", params=None, is_signed=True),
                call("GET", "/api/v1/positions", params=None, is_signed=True),
            ]
            mock_api_request.assert_has_calls(expected_calls_to_request, any_order=True)
            mock_handler_balances.assert_called_once_with(
                {k: v.model_dump(by_alias=True) for k, v in mock_raw_balances_dict_fixture.items()}
            )
            mock_handler_positions.assert_called_once_with(
                [pos.model_dump(by_alias=True) for pos in mock_raw_positions_list_fixture],
                symbol=None,
            )
            api._bp_mapper.transform_raw_account_summary_to_internal.assert_called_once_with(
                raw_settings=mock_raw_account_summary_fixture,
                spot_balances_raw=mock_raw_balances_dict_fixture,
                derivative_positions_raw=mock_raw_positions_list_fixture,
            )
            mock_mapper_dt.utcnow.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_account_summary_handles_get_account_info_failure(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
        caplog: LogCaptureFixture,
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        mock_get_account_info = AsyncMock(return_value=None)
        with patch.object(api, "get_account_info", mock_get_account_info):
            result = await api.get_account_summary()

        assert result is None
        assert (
            f"[{api.exchange_name}] Failed to fetch raw account settings for summary."
            in caplog.text
        )
        mock_get_account_info.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_account_summary_balances_raw_failure(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
        caplog: LogCaptureFixture,
        raw_dict_for_account_summary_response: dict[str, Any],
        mock_raw_account_summary_fixture: BackpackRawAccountSummary,
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)

        with (
            patch.object(
                api, "get_account_info", return_value=mock_raw_account_summary_fixture
            ) as mock_get_info_method,
            patch.object(api, "_request") as mock_api_request,
        ):

            def request_side_effect(method: str, endpoint: str, **kwargs: dict[str, Any]) -> None:
                if endpoint.endswith("/api/v1/capital"):
                    raise HttpRequestFailedError(
                        "Simulated balances fetch error", http_status_code=500
                    )
                elif endpoint.endswith("/api/v1/positions"):
                    raise ValueError(
                        "Positions should not be fetched if balances failed contextually"
                        " in this test"
                    )
                raise ValueError(
                    f"Unexpected endpoint in balances_raw_failure test mock_request: {endpoint}"
                )

            mock_api_request.side_effect = request_side_effect

            with pytest.raises(APIError) as exc_info:
                await api.get_account_summary()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error during get_account_summary" in str(exc_info.value.message)
        assert (
            "Positions should not be fetched if balances failed contextually in this test"
            in str(exc_info.value.original_exception)
        )

        mock_get_info_method.assert_called_once()
        balance_call_found = False
        position_call_found = False
        for call_item in mock_api_request.call_args_list:
            if call_item.args[1].endswith("/api/v1/capital"):
                balance_call_found = True
            if call_item.args[1].endswith("/api/v1/positions"):
                position_call_found = True
        assert balance_call_found
        assert position_call_found

    @pytest.mark.asyncio
    async def test_get_account_summary_mapper_failure(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
        caplog: LogCaptureFixture,
        mock_raw_account_summary_fixture: BackpackRawAccountSummary,
        mock_raw_balances_dict_fixture: dict[str, BackpackRawBalance],
        mock_raw_positions_list_fixture: list[BackpackRawPosition],
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)

        with (
            patch.object(api, "get_account_info", return_value=mock_raw_account_summary_fixture),
            patch.object(api, "_request") as mock_api_request,
            patch.object(
                api._bp_mapper,
                "transform_raw_account_summary_to_internal",
                side_effect=ValueError("Mapper internal error"),
            ) as mock_transform_summary,
        ):

            def request_side_effect(
                method: str, endpoint: str, **kwargs: dict[str, Any]
            ) -> dict[str, Any] | list[Any]:
                if endpoint.endswith("/api/v1/capital"):
                    return {
                        k: v.model_dump(by_alias=True)
                        for k, v in mock_raw_balances_dict_fixture.items()
                    }
                elif endpoint.endswith("/api/v1/positions"):
                    return [
                        pos.model_dump(by_alias=True) for pos in mock_raw_positions_list_fixture
                    ]
                raise ValueError(f"Unexpected URL in mapper_failure mock_request: {endpoint}")

            mock_api_request.side_effect = request_side_effect

            with pytest.raises(APIError) as exc_info:
                await api.get_account_summary()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to map account summary" in str(exc_info.value.message)
        assert "Mapper internal error" in str(exc_info.value.original_exception)
        assert (
            "Error mapping raw account data to internal summary: Mapper internal error"
            in caplog.text
        )
        mock_transform_summary.assert_called_once()
