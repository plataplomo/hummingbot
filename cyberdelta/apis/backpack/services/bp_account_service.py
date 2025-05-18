"""
CyberDeltaEngine: Backpack Account Management Service (Raw Model Focused)
----------------------------------------------------------------------

This service encapsulates the logic for fetching and validating raw account-specific
data from the Backpack Exchange. This includes balances, positions,
order history, trade history, and account information.

It uses the passed-in HTTP requester, BackpackRequestBuilder, and BackpackResponseHandler
to interact with the API and validate responses into Raw Pydantic Models.

The service methods return these validated Raw Pydantic Models.
Transformation to internal domain models is handled by the calling API client.
"""

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import (
    BackpackResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawFill
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class BackpackAccountService:
    """
    Service class for Backpack account management operations, returning Raw Pydantic Models.
    """

    def __init__(
        self,
        http_client_requester: Callable[..., Any],  # Type hint for a callable like _request
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        authenticator: IAuthenticator | None,
        rate_limiter_service: RateLimiterService,  # Services will need this for the requester
        exchange_name: str,
    ) -> None:
        """
        Initialize the BackpackAccountService.

        Args:
            http_client_requester: A callable (e.g., api_client._request) for making API calls.
            request_builder: An instance of BackpackRequestBuilder.
            response_handler: An instance of BackpackResponseHandler.
            authenticator: An instance of IAuthenticator for signed requests.
            rate_limiter_service: The rate limiter service instance.
            exchange_name: The name of the exchange.
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._rate_limiter_service = rate_limiter_service
        self._exchange_name = exchange_name

    async def get_balances_raw(self) -> dict[str, BackpackRawBalance]:
        """Fetches raw account balances from Backpack.

        Returns:
            A dictionary mapping asset symbols to their BackpackRawBalance objects.

        Raises:
            APIError: If the request fails or the response is invalid.
        """
        response_data_raw: RawJsonResponse | None = None
        try:
            limiter = self._rate_limiter_service.get_limiter(
                "GET",
                "/api/v1/capital",  # Limiter key might still use full path
            )
            await limiter.acquire()
            # Call the requester (ExchangeAPI._request)
            # Get balances does not typically take query parameters for Backpack
            query_params_balances = self._request_builder.build_get_balances_params()

            response_data_raw, _, _ = await self._http_client_requester(
                method="GET",
                endpoint="/api/v1/capital",  # Use 'endpoint' positional arg
                params=query_params_balances,  # Explicitly pass params (likely None)
                endpoint_group="PRIVATE",
                is_signed=True,
            )

            if not isinstance(response_data_raw, dict):
                logger.error(
                    f"[{self._exchange_name}] Unexpected raw balances response format: "
                    f"{type(response_data_raw)}. Expected dict."
                )
                raise APIError(
                    message=f"Unexpected raw balances response: {type(response_data_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            # Validate raw balances using the response handler
            return self._response_handler.handle_get_balances_response(response_data_raw)

        except APIError:  # Re-raise APIErrors from http_client_requester or explicit raises
            raise
        except ValidationError as e_val:  # Catch Pydantic validation errors from handler
            logger.error(
                f"[{self._exchange_name}] Raw balances response validation failed: {e_val}. "
                f"Raw: {response_data_raw!r}"
            )
            raise APIError(
                message=f"Raw balances validation failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:  # Catch any other unexpected errors
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_balances_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing raw balances: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_positions_raw(self, symbol: str | None = None) -> list[BackpackRawPosition]:
        """Fetches raw current open positions from Backpack.

        Args:
            symbol: Optional. If provided, filters positions for this specific symbol.

        Returns:
            A list of BackpackRawPosition objects.

        Raises:
            APIError: If the request fails or the response is invalid.
        """
        response_data_raw: RawJsonResponse | None = None
        try:
            limiter = self._rate_limiter_service.get_limiter(
                "GET",
                "/api/v1/positions",  # Limiter key
            )
            await limiter.acquire()
            # Call the requester (ExchangeAPI._request)
            actual_endpoint_path_positions = "/api/v1/positions"
            query_params_positions = self._request_builder.build_get_positions_params(symbol=symbol)

            response_data_raw, _, _ = await self._http_client_requester(
                method="GET",
                endpoint=actual_endpoint_path_positions,  # Use 'endpoint' positional arg
                params=query_params_positions,  # Pass the built params
                endpoint_group="PRIVATE",
                is_signed=True,
            )

            if not isinstance(response_data_raw, list):
                logger.error(
                    f"[{self._exchange_name}] Unexpected raw positions response format: "
                    f"{type(response_data_raw)}. Expected list."
                )
                raise APIError(
                    message=f"Unexpected raw positions response: {type(response_data_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_get_positions_response(response_data_raw, symbol)

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Raw positions response validation failed: {e_val}. "
                f"Raw: {response_data_raw!r}"
            )
            raise APIError(
                message=f"Raw positions validation failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_positions_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing raw positions: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_order_history_raw(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = 100,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> list[BackpackRawOrder]:
        """Fetches raw historical orders from Backpack.

        Args:
            symbol: Optional symbol to filter orders by.
            start_time: Optional start time for filtering orders.
            end_time: Optional end time for filtering orders.
            limit: Optional limit on the number of orders to return.
            order_id: Optional specific order ID to fetch.
            client_order_id: Optional client order ID.

        Returns:
            A list of BackpackRawOrder objects.

        Raises:
            APIError: If the request fails or the response is invalid.
        """
        endpoint = "/wapi/v1/history/orders"
        params = self._request_builder.build_get_order_history_params(
            symbol=symbol,
            start_time_ms=int(start_time.timestamp() * 1000) if start_time else None,
            end_time_ms=int(end_time.timestamp() * 1000) if end_time else None,
            limit=limit,
            order_id=order_id,
            client_order_id=client_order_id,
        )
        response_data_raw: RawJsonResponse | None = None
        try:
            response_data_raw, _, _ = await self._http_client_requester(
                method="GET",
                endpoint_path=endpoint,
                params=params,
                authenticator=self._authenticator,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=True,
            )

            if not isinstance(response_data_raw, list):
                logger.error(
                    f"[{self._exchange_name}] Unexpected raw order history response format: "
                    f"{type(response_data_raw)}. Expected list."
                )
                raise APIError(
                    message=f"Unexpected raw order history response: {type(response_data_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_get_order_history_response(
                response_data_raw, symbol
            )

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Raw order history response validation failed: {e_val}. "
                f"Raw: {response_data_raw!r}"
            )
            raise APIError(
                message=f"Raw order history validation failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_order_history_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing raw order history: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_trade_history_raw(
        self, symbol: str | None = None, limit: int = 100
    ) -> list[BackpackRawFill]:
        """Fetches raw historical trades (fills) from Backpack.

        Args:
            symbol: Optional symbol to filter trades by.
            limit: Optional limit on the number of trades to return.

        Returns:
            A list of BackpackRawFill objects.

        Raises:
            APIError: If the request fails or the response is invalid.
        """
        endpoint = "/wapi/v1/history/fills"
        params = self._request_builder.build_get_trade_history_params(
            symbol=symbol,
            limit=limit,
            start_time_ms=None,  # These were confirmed to be needed, pass None if not used
            end_time_ms=None,
        )
        response_data_raw: RawJsonResponse | None = None
        try:
            response_data_raw, _, _ = await self._http_client_requester(
                method="GET",
                endpoint_path=endpoint,
                params=params,
                authenticator=self._authenticator,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=True,
            )

            if not isinstance(response_data_raw, list):
                logger.error(
                    f"[{self._exchange_name}] Unexpected raw trade history response format: "
                    f"{type(response_data_raw)}. Expected list."
                )
                raise APIError(
                    message=f"Unexpected raw trade history response: {type(response_data_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            # Use handle_get_trade_history_response which returns list[BackpackRawFill]
            return self._response_handler.handle_get_trade_history_response(
                response_data_raw, symbol
            )

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Raw trade history response validation failed: {e_val}. "
                f"Raw: {response_data_raw!r}"
            )
            raise APIError(
                message=f"Raw trade history validation failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_trade_history_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing raw trade history: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_account_info_raw(self) -> BackpackRawAccountSummary | None:
        """Fetches the raw account settings/summary from Backpack.

        This typically provides general account status and settings.

        Returns:
            BackpackRawAccountSummary if successful, None if not found or error that implies absence.
            Can raise APIError for other failures.
        """
        response_data_raw: RawJsonResponse | None = None
        try:
            limiter = self._rate_limiter_service.get_limiter(
                "GET", "/api/v1/account"
            )  # Limiter key
            await limiter.acquire()
            # Call the requester (ExchangeAPI._request) with endpoint_group and endpoint_specific
            response_data_raw, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint_group="PRIVATE",  # Assuming this is a private endpoint
                endpoint="/api/v1/account",  # Was endpoint_specific
                is_signed=True,
            )
            # Handle non-200 status if necessary, though _request might raise APIError for >=400

            if status_code == 404:  # Or other codes indicating 'not found'
                logger.info(
                    f"[{self._exchange_name}] Account info not found (status {status_code})."
                )
                return None

            # handle_get_account_info_response expects a dict.
            if not isinstance(response_data_raw, dict):
                logger.error(
                    f"[{self._exchange_name}] Unexpected raw account info response format: "
                    f"{type(response_data_raw)}. Expected dict."
                )
                raise APIError(
                    message=f"Unexpected raw account info: {type(response_data_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_get_account_info_response(response_data_raw)

        except APIError:  # Re-raise APIErrors from http_client_requester or explicit handler raises
            # If a 404 was already handled to return None, this won't be hit for that case.
            # Other APIErrors will propagate.
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Raw account info response validation failed: {e_val}. "
                f"Raw: {response_data_raw!r}"
            )
            raise APIError(
                message=f"Raw account info validation failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_account_info_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing raw account info: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def transfer_raw(
        self,
        asset: str,
        amount: Decimal,
        from_account_type: str,
        to_account_type: str,
        client_transfer_id: str | None = None,
    ) -> RawJsonResponse:
        """Initiates a transfer between account types on Backpack (RAW response).

        Args:
            asset: The asset to transfer (e.g., "USDC").
            amount: The amount to transfer.
            from_account_type: Source account type (e.g., "SPOT").
            to_account_type: Destination account type (e.g., "FUTURES").
            client_transfer_id: Optional client-provided ID.

        Returns:
            The raw JSON response from the server.

        Raises:
            APIError: If the request fails or the response is invalid.
        """
        endpoint = "/wapi/v1/capital/transfer/internal"  # Confirmed endpoint
        logger.info(
            f"[{self._exchange_name}] Initiating raw transfer: {amount} {asset} "
            f"from {from_account_type} to {to_account_type}"
        )

        payload = self._request_builder.build_internal_transfer_payload(
            asset_symbol=asset,
            amount_str=str(amount),
            from_account=from_account_type,
            to_account=to_account_type,
            client_transfer_id=client_transfer_id,
        )
        response_data_raw: RawJsonResponse | None = None
        try:
            response_data_raw, _, _ = await self._http_client_requester(
                method="POST",
                endpoint=endpoint,  # Was endpoint_path
                data=payload,
                is_signed=True,
            )

            if response_data_raw is None:
                logger.warning(
                    f"[{self._exchange_name}] Transfer response was None for {amount} {asset}."
                )
                raise APIError(
                    message="No response data received for transfer request.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            # For transfers, Backpack might return a simple confirmation or an ID.
            # The response_handler might have a specific handle_transfer_response or a generic one.
            # If a specific handler exists and validates to a Pydantic model, use it:
            # Example: return self._response_handler.handle_transfer_response(response_data_raw)
            # If not, and the response is just JSON, return it directly as RawJsonResponse.
            return response_data_raw  # Assuming it's JSON compatible as per RawJsonResponse

        except APIError:  # Re-raise APIErrors from http_client_requester
            raise
        except ValidationError as e_val:  # If a handler were used and caused validation error
            logger.error(
                f"[{self._exchange_name}] Transfer response validation failed: {e_val}. "
                f"Raw: {response_data_raw!r}"
            )
            raise APIError(
                message=f"Transfer response validation failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in transfer_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing transfer: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def withdraw_raw(
        self,
        asset: str,
        amount: Decimal,
        address: str,
        network: str | None = None,
        tag: str | None = None,
        client_withdrawal_id: str | None = None,
        two_factor_token: str | None = None,
        **kwargs: dict[str, Any],
    ) -> BackpackRawWithdrawalResponse:
        """Initiates a withdrawal from Backpack, returning the raw validated response.

        Note: Backpack's withdrawal API details might require confirmation.
        This implementation assumes a POST request and standard handling.

        Args:
            asset: The asset to withdraw.
            amount: The amount to withdraw.
            address: The destination address.
            network: Optional blockchain network.
            tag: Optional destination tag/memo.
            client_withdrawal_id: Optional client-provided ID.
            two_factor_token: Optional 2FA token, if required by Backpack.
            **kwargs: Additional parameters for the request builder.

        Returns:
            A BackpackRawWithdrawalResponse object.

        Raises:
            APIError: If the request fails or the response is invalid.
            NotImplementedError: If critical details (like 2FA) are unknown for Backpack.
        """
        endpoint = "/wapi/v1/capital/withdrawals"

        # Removed presumptive 2FA check.
        # If Backpack requires 2FA and it's not handled by their API implicitly
        # or passed via two_factor_token, their API should error out.
        # The builder.build_withdraw_payload handles the two_factor_token if provided.

        data = self._request_builder.build_withdraw_payload(
            asset=asset,
            amount=amount,
            address=address,
            network=network,
            tag=tag,
            client_withdrawal_id=client_withdrawal_id,
            two_factor_token=two_factor_token,
            **kwargs,
        )
        response_data_raw: RawJsonResponse | None = None
        try:
            response_data_raw, _, _ = await self._http_client_requester(
                method="POST",
                endpoint_path=endpoint,
                data=data,
                authenticator=self._authenticator,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=True,
            )

            if not isinstance(response_data_raw, dict):
                logger.error(
                    f"[{self._exchange_name}] Unexpected raw withdrawal response format: "
                    f"{type(response_data_raw)}. Expected dict."
                )
                raise APIError(
                    message=f"Unexpected raw withdrawal response: {type(response_data_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_withdraw_response(response_data_raw)

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Raw withdrawal response validation failed: {e_val}. "
                f"Raw: {response_data_raw!r}"
            )
            raise APIError(
                message=f"Raw withdrawal validation failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in withdraw_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing raw withdrawal: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_account_summary_components_raw(
        self,
    ) -> tuple[
        BackpackRawAccountSummary | None, dict[str, BackpackRawBalance], list[BackpackRawPosition]
    ]:
        """Fetches all raw components needed for an account summary from Backpack.

        This method orchestrates calls to get raw account info, balances, and positions.
        It's intended to be used by the API client, which will then map these raw
        components to an internal MarginAccountSummary.

        Returns:
            A tuple containing:
            - BackpackRawAccountSummary or None
            - Dictionary of asset symbols to BackpackRawBalance objects
            - List of BackpackRawPosition objects

        Raises:
            APIError: If any of the underlying data fetching calls fail critically.
                      Partial data might be returned if some calls succeed and others return None (e.g. for 404s).
        """
        # Note: This implementation assumes that if one part fails in a way that returns None (e.g. 404),
        # we still try to get the other parts. Critical APIErrors will propagate.

        account_info_raw: BackpackRawAccountSummary | None = None
        balances_raw: dict[str, BackpackRawBalance] = {}
        positions_raw: list[BackpackRawPosition] = []

        try:
            account_info_raw = await self.get_account_info_raw()
        except APIError as e_info:
            logger.warning(
                f"[{self._exchange_name}] Failed to get raw account info for summary: {e_info}"
            )
            # Decide if this error should halt everything or if we can proceed
            # For now, we'll log and continue, so other components can be fetched.
            # If get_account_info_raw returns None (e.g. 404), that's handled.

        try:
            balances_raw = await self.get_balances_raw()
        except APIError as e_bal:
            logger.error(f"[{self._exchange_name}] Failed to get raw balances for summary: {e_bal}")
            raise APIError(  # Re-raise as this is critical for a summary
                message=f"Failed to fetch raw balances for account summary: {e_bal}",
                code=e_bal.code or APIErrorCode.SERVICE_UNAVAILABLE.value,
                original_exception=e_bal,
            ) from e_bal

        try:
            # Assuming positions are for all symbols for a general summary context
            positions_raw = await self.get_positions_raw(symbol=None)
        except APIError as e_pos:
            logger.warning(
                f"[{self._exchange_name}] Failed to get raw positions for summary: {e_pos}"
            )
            # Similar to account_info, log and continue if positions are not critical for a partial summary
            # or re-raise if they are essential.
            # For now, log and proceed, returning empty list if it failed this way.

        return account_info_raw, balances_raw, positions_raw
