"""
CyberDeltaEngine: Hyperliquid Account Management Service (Raw Model Focused)
-------------------------------------------------------------------------

This service encapsulates the logic for fetching and validating raw account-specific
data from the Hyperliquid Exchange. This includes balances, positions,
order history, trade history, and account information.

It uses the passed-in HTTP requester, HyperliquidRequestBuilder, and HyperliquidResponseHandler
to interact with the API and validate responses into Raw Pydantic Models.

The service methods return these validated Raw Pydantic Models.
Transformation to internal domain models is handled by the calling API client.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)

# Raw Model Imports will be added as methods are implemented
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,  # For transfer/withdraw
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,  # For order history, order status
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOpenOrdersResponse
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFillsResponse
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.utils.logging_config import get_logger

if TYPE_CHECKING:
    from decimal import Decimal

    from cyberdelta.apis.base.authenticator_interface import IAuthenticator
    from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService


logger = get_logger(__name__)


class HyperliquidAccountService:
    """
    Service class for Hyperliquid account management operations, returning Raw Pydantic Models.
    """

    def __init__(
        self,
        exchange_http_client_requester: Callable[..., Any],
        info_http_client_requester: Callable[..., Any],
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        authenticator: IAuthenticator | None,
        rate_limiter_service: RateLimiterService,
        exchange_name: str,
        wallet_address: str | None,
    ) -> None:
        """
        Initialize the HyperliquidAccountService.
        """
        self._exchange_http_client_requester = exchange_http_client_requester
        self._info_http_client_requester = info_http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._rate_limiter_service = rate_limiter_service
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address

    async def get_account_summary_raw(self) -> HyperliquidRawClearinghouseState:
        """Fetches the raw clearinghouse state for the user (basis for account summary).

        This data is also used for balances and positions.

        Returns:
            HyperliquidRawClearinghouseState: The raw state of the clearinghouse for the user.

        Raises:
            APIError: If the wallet address is not set, or if the API request fails.
        """
        if not self._wallet_address:
            logger.error(
                f"[{self._exchange_name}] Wallet address not set. Cannot fetch user state."
            )
            raise APIError(
                message="Wallet address is required to fetch user state.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        endpoint_path_for_info_client = "/info"
        payload = self._request_builder.build_user_state_payload(self._wallet_address)

        response_data_raw: Any | None = None
        try:
            response_data_raw, _, _ = await self._info_http_client_requester(
                method="POST",
                endpoint_path=endpoint_path_for_info_client,
                data=payload.model_dump(),
                authenticator=None,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=False,
            )

            if response_data_raw is None:
                raise APIError(
                    message="No response data received for user state request.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_info_user_state_response(
                raw_response_content=response_data_raw, user_address=self._wallet_address
            )
        except APIError:  # Re-raise APIErrors from http_client_requester or handler
            raise
        except ValidationError as e_val:  # Catch Pydantic validation errors specifically
            logger.warning(
                f"[{self._exchange_name}] Validation error in get_account_summary_raw: {e_val}",
                exc_info=True,
            )
            raise APIError(
                message=f"Invalid data received for account summary: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:  # Catch any other unexpected errors
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_account_summary_raw: "
                f"{e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing raw account summary: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_balances_raw(self) -> HyperliquidRawClearinghouseState:
        # Corresponds to HyperliquidAPI.get_balances
        # Uses INFO_URL, non-signed (but needs wallet address for response handler)
        # This will use the same underlying data as get_account_summary_raw
        return await self.get_account_summary_raw()

    async def get_positions_raw(self) -> HyperliquidRawClearinghouseState:
        # Corresponds to HyperliquidAPI.get_positions
        # Uses INFO_URL, non-signed (but needs wallet address for response handler)
        # This will also use the same underlying data as get_account_summary_raw
        return await self.get_account_summary_raw()

    async def get_open_orders_raw(self) -> HyperliquidRawOpenOrdersResponse:
        """Fetches raw open orders for the user.

        Returns:
            HyperliquidRawOpenOrdersResponse: The raw open orders for the user.

        Raises:
            APIError: If the wallet address is not set, or if the API request fails.
        """
        if not self._wallet_address:
            logger.error(
                f"[{self._exchange_name}] Wallet address not set. Cannot fetch open orders."
            )
            raise APIError(
                message="Wallet address is required to fetch open orders.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        endpoint_path_for_info_client = "/info"
        payload_data = {"type": "openOrders", "user": self._wallet_address}

        response_data_raw: Any | None = None
        try:
            response_data_raw, _, _ = await self._info_http_client_requester(
                method="POST",
                endpoint_path=endpoint_path_for_info_client,
                data=payload_data,
                authenticator=None,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=False,
            )

            if response_data_raw is None:
                raise APIError(
                    message="No response data received for open orders request.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_info_open_orders_response(
                raw_response_content=response_data_raw, user_address=self._wallet_address
            )
        except APIError:
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_open_orders_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing raw open orders: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def transfer_raw(
        self,
        asset: str,  # Should be USDC for HL
        amount: Decimal,
        to_account: str,  # Destination L2 address
    ) -> HyperliquidRawExchangeResponse:
        """Executes an L2 USD(C) transfer and returns the raw exchange response.

        Args:
            asset: The asset to transfer (must be "USDC" for Hyperliquid L2 transfers).
            amount: The amount to transfer.
            to_account: The destination L2 wallet address.

        Returns:
            HyperliquidRawExchangeResponse: The raw response from the exchange.

        Raises:
            ValueError: If asset is not USDC.
            APIError: If the request fails or authenticator is not available.
        """
        if asset.upper() != "USDC":
            raise ValueError("Hyperliquid L2 transfers only support USDC.")
        if not self._authenticator:
            logger.error(
                f"[{self._exchange_name}] Authenticator not available for signed "
                f"request: transfer_raw"
            )
            raise APIError(
                message="Authenticator required for L2 transfer.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        endpoint_group_for_exchange_client = "exchange"

        payload_model = self._request_builder.build_l2_usd_transfer_payload(
            destination_address=to_account, amount=amount
        )
        request_data_dict = payload_model.model_dump()

        response_data_raw: Any | None = None
        try:
            response_data_raw, _, _ = await self._exchange_http_client_requester(
                method="POST",
                endpoint_group=endpoint_group_for_exchange_client,
                data=request_data_dict,
                authenticator=self._authenticator,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=True,
            )

            if response_data_raw is None:
                raise APIError(
                    message="No response data received for L2 transfer request.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_exchange_response(
                raw_response_content=response_data_raw,
                action_type=payload_model.type,  # context for handler (e.g., "usdTransfer")
            )
        except APIError:  # Re-raise APIErrors from http_client_requester or handler
            raise
        except Exception as e_unhandled:  # Catch any other unexpected errors
            logger.error(
                f"[{self._exchange_name}] Unexpected error in transfer_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing L2 transfer: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def withdraw_raw(
        self,
        asset: str,
        amount: Decimal,
        address: str,  # L1 destination address
    ) -> HyperliquidRawExchangeResponse:
        """Executes a withdrawal to L1 and returns the raw exchange response.

        Args:
            asset: The asset to withdraw (e.g., "ETH", "USDC").
            amount: The amount to withdraw.
            address: The destination L1 wallet address.

        Returns:
            HyperliquidRawExchangeResponse: The raw response from the exchange.

        Raises:
            ValueError: If address is not provided.
            APIError: If the request fails or authenticator is not available.
        """
        if not address:
            raise ValueError("Destination address (L1) is required for withdrawal.")
        if not self._authenticator:
            logger.error(
                f"[{self._exchange_name}] Authenticator not available for signed "
                f"request: withdraw_raw"
            )
            raise APIError(
                message="Authenticator required for L1 withdrawal.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        endpoint_group_for_exchange_client = "exchange"

        payload_model = self._request_builder.build_withdrawal_payload(
            asset=asset, amount=amount, destination_address=address
        )
        request_data_dict = payload_model.model_dump()

        response_data_raw: Any | None = None
        try:
            response_data_raw, _, _ = await self._exchange_http_client_requester(
                method="POST",
                endpoint_group=endpoint_group_for_exchange_client,
                data=request_data_dict,
                authenticator=self._authenticator,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=True,
            )

            if response_data_raw is None:
                raise APIError(
                    message="No response data received for withdrawal request.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_exchange_response(
                raw_response_content=response_data_raw,
                action_type=payload_model.type,  # context for handler (e.g., "withdraw"
                # or "withdrawEth")
            )
        except APIError:
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in withdraw_raw: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing L1 withdrawal: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_order_history_raw(
        self,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[HyperliquidRawHistoricalOrderResponse]:
        """Fetches raw order history for the user within a time range.

        Args:
            start_time_ms: Start time in milliseconds since epoch.
            end_time_ms: End time in milliseconds since epoch.

        Returns:
            list[HyperliquidRawHistoricalOrderResponse]: List of raw historical orders.

        Raises:
            APIError: If the wallet address is not set, or if the API request fails.
        """
        if not self._wallet_address:
            logger.error(
                f"[{self._exchange_name}] Wallet address not set. Cannot fetch order history."
            )
            raise APIError(
                message="Wallet address is required to fetch order history.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        endpoint_path_for_info_client = "/info"
        payload = self._request_builder.build_order_history_payload(
            wallet_address=self._wallet_address,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        payload_data = payload.model_dump()

        response_data_raw: Any | None = None
        try:
            response_data_raw, _, _ = await self._info_http_client_requester(
                method="POST",
                endpoint_path=endpoint_path_for_info_client,
                data=payload_data,
                authenticator=None,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=False,
            )

            if response_data_raw is None:
                raise APIError(
                    message="No response data received for order history request.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_query_order_history_response(
                raw_response_content=response_data_raw, user_address=self._wallet_address
            )
        except APIError:
            raise
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
        self,
    ) -> HyperliquidRawUserFillsResponse:
        """Fetches raw trade history (user fills) for the user.

        Hyperliquid's userFills endpoint returns all fills; filtering by limit
        should be done by the caller if needed.

        Returns:
            HyperliquidRawUserFillsResponse: The raw user fills.

        Raises:
            APIError: If the wallet address is not set, or if the API request fails.
        """
        if not self._wallet_address:
            logger.error(
                f"[{self._exchange_name}] Wallet address not set. Cannot fetch trade history."
            )
            raise APIError(
                message="Wallet address is required to fetch trade history.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        endpoint_path_for_info_client = "/info"
        payload_data = {"type": "userFills", "user": self._wallet_address}

        response_data_raw: Any | None = None
        try:
            response_data_raw, _, _ = await self._info_http_client_requester(
                method="POST",
                endpoint_path=endpoint_path_for_info_client,
                data=payload_data,
                authenticator=None,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=False,
            )

            if response_data_raw is None:
                raise APIError(
                    message="No response data received for trade history request.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            return self._response_handler.handle_info_user_fills_response(
                raw_response_content=response_data_raw, user_address=self._wallet_address
            )
        except APIError:
            raise
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

    async def get_order_status_raw(self, order_id: int) -> HyperliquidRawHistoricalOrderResponse:
        """Fetches the raw status of a specific order by its OID.

        Args:
            order_id: The order ID (OID) of the order to fetch.

        Returns:
            HyperliquidRawHistoricalOrderResponse: The raw historical order response.

        Raises:
            APIError: If the wallet address is not set, or if the API request fails,
                      or if the order is not found.
        """
        if not self._wallet_address:
            logger.error(
                f"[{self._exchange_name}] Wallet address not set. Cannot fetch order status."
            )
            raise APIError(
                message="Wallet address is required to fetch order status.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        endpoint_path_for_info_client = "/info"
        payload = self._request_builder.build_order_status_payload(
            wallet_address=self._wallet_address, order_id=order_id
        )
        payload_data = payload.model_dump()

        response_data_raw: Any | None = None
        try:
            response_data_raw, _, _ = await self._info_http_client_requester(
                method="POST",
                endpoint_path=endpoint_path_for_info_client,
                data=payload_data,
                authenticator=None,
                rate_limiter_service=self._rate_limiter_service,
                is_signed=False,
            )

            if response_data_raw is None:
                # Hyperliquid returns a 404 string if order not found, which _request
                # might parse as non-JSON
                # or return None if it can't parse. The handler expects a dict for success.
                logger.warning(
                    f"[{self._exchange_name}] No response data or non-JSON for order status "
                    f"oid {order_id}. It might not exist or was invalid."
                )
                raise APIError(
                    message=f"No valid response data received for order status (oid {order_id}).",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,  # More specific if possible
                )

            return self._response_handler.handle_info_order_status_response(
                raw_response_content=response_data_raw,
                user_address=self._wallet_address,  # Context for handler
                order_id=order_id,  # Context for handler
            )
        except APIError as e:
            # If ORDER_NOT_FOUND was already raised, re-raise it.
            # Otherwise, if a different APIError, re-raise that.
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.info(
                    f"[{self._exchange_name}] Order oid {order_id} not found, as "
                    f"reported by service call."
                )
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_order_status_raw "
                f"for oid {order_id}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing raw order status "
                f"(oid {order_id}): {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled
