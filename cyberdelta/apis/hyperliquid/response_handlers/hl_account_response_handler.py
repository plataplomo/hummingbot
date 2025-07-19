"""Hyperliquid Account Response Handler.

This module handles the validation and processing of account-related API responses,
extracted from the monolithic response handler to improve maintainability and testability.

Focused on:
- User state responses (clearinghouse state)
- Balance and position responses
- Account settings responses
- Vault details responses
- Transfer and withdrawal responses
"""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions import MissingRequiredParameterError
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
    HyperliquidRawHistoricalOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_usd_transfer_response import (
    HyperliquidRawUsdTransferResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState,
    HyperliquidRawClearinghouseState as HyperliquidRawUserStateResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_vault_details import (
    HyperliquidRawVaultDetailsResponse,
)
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import AccountResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.response_handlers.hl_response_handler_base import (
    HyperliquidResponseHandlerBase,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_trading_response_handler import (
    HyperliquidTradingResponseHandler,
)
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)


class HyperliquidAccountResponseHandler(
    HyperliquidResponseHandlerBase, AccountResponseHandlerProtocol
):
    """Handles validation of account-related JSON responses from Hyperliquid API.

    Uses Pydantic models to validate the structure and types of the raw data.
    Raises APIError if validation fails. Implements AccountResponseHandlerProtocol
    for type safety and consistency.
    """

    # Base protocol method implementation
    def handle_response(
        self, response: dict[str, object], status_code: int, headers: dict[str, str], context: str
    ) -> object:
        """Handle API response per base protocol.

        Args:
            response: Raw response data from API
            status_code: HTTP status code
            headers: Response headers
            context: Context information about the request

        Returns:
            Processed response object
        """
        # Route to appropriate handler based on context
        if context == "user_state":
            return self.handle_info_user_state_response(response, "unknown", status_code, headers)
        if context in {"open_orders", "frontend_open_orders"}:
            return self.handle_info_open_orders_response(response, status_code, headers)
        if context == "order_status":
            # Need to use trading response handler for order status
            handler = HyperliquidTradingResponseHandler()
            return handler.handle_info_order_status_response(response, status_code, headers)
        if context == "user_fills":
            return self.handle_info_user_fills_response(response, status_code, headers)
        if context == "historical_orders":
            return self.handle_historical_orders_response(response, "unknown", status_code, headers)
        raise APIError(
            message=f"Unknown account response context: {context}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    def handle_info_user_state_response(
        self,
        raw_response_content: ParsedJsonResponse,
        user_address: str,
        status_code: int,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawUserStateResponse:
        """Validate raw JSON response for user state (clearinghouse) info endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            user_address: User wallet address for context
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawUserStateResponse: Validated user state response

        Raises:
            APIError: If validation fails
        """
        context = f"info (user state for {user_address})"
        try:
            # Ensure we have a dict
            response_dict = ensure_dict_response(raw_response_content, context, status_code)
            # Validate with Pydantic
            return HyperliquidRawUserStateResponse(**response_dict)
        except ValidationError as e:
            raise self._handle_validation_error(
                e, context, raw_response_content, status_code, headers
            ) from e

    # TODO: Re-enable when spot asset models are available

    def handle_info_vault_details_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawVaultDetailsResponse:
        """Validate raw JSON response for vault details info endpoint.

        Args:
            raw_data: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawVaultDetailsResponse: Validated vault details

        Raises:
            APIError: If validation fails
        """
        context = "info_vault_details"
        if status_code is None:
            raise MissingRequiredParameterError("status_code", context)

        try:
            # Ensure we have a dict
            response_dict = ensure_dict_response(raw_data, context, status_code)
            # For vault details, if the response is None or empty, raise an API error
            if not response_dict:
                raise APIError(
                    code=APIErrorCode.RESPONSE_VALIDATION_FAILED.value,
                    message="Empty vault details response from API",
                    metadata={"raw_data": raw_data},
                )

            # Validate with Pydantic
            return HyperliquidRawVaultDetailsResponse(**response_dict)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_info_open_orders_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawOpenOrdersResponse:
        """Validate raw JSON response for open orders info endpoint.

        Args:
            raw_data: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawOpenOrdersResponse: Validated open orders response

        Raises:
            APIError: If validation fails
        """
        context = "info_open_orders"
        if status_code is None:
            raise MissingRequiredParameterError("status_code", context)

        try:
            # Response is expected to be a list
            response_list = ensure_list_response(raw_data, context, status_code)
            # Wrap in response model
            return HyperliquidRawOpenOrdersResponse(response_list)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_info_user_fills_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawUserFillsResponse:
        """Validate raw JSON response for user fills (trade history) info endpoint.

        Args:
            raw_data: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawUserFillsResponse: Validated user fills response

        Raises:
            APIError: If validation fails
        """
        context = "info_user_fills"
        if status_code is None:
            raise MissingRequiredParameterError("status_code", context)

        try:
            # Response is expected to be a list
            response_list = ensure_list_response(raw_data, context, status_code)
            # Wrap in response model
            return HyperliquidRawUserFillsResponse(response_list)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_historical_orders_response(
        self,
        raw_data: ParsedJsonResponse,
        wallet_address: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawHistoricalOrderResponse]:
        """Validate raw JSON response for historical orders endpoint.

        Args:
            raw_data: Raw JSON response from the API
            wallet_address: User's wallet address for context
            status_code: HTTP status code
            headers: Response headers

        Returns:
            list[HyperliquidRawHistoricalOrderResponse]: Validated list of historical orders

        Raises:
            APIError: If validation fails
        """
        context = f"historicalOrders (for {wallet_address})"
        try:
            # Use RootModel for validation - it handles list structure and filtering
            validated_response = HyperliquidRawHistoricalOrdersResponse.model_validate(raw_data)
            # Return the items from the RootModel
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e
        else:
            return validated_response.root

    # Protocol implementation methods
    @staticmethod
    def handle_get_user_state_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawClearinghouseState:
        """Handle user state response using protocol interface.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing user state data
        """
        context = "get_user_state"
        try:
            # Validate with Pydantic
            return HyperliquidRawClearinghouseState.model_validate(raw_response_content)
        except ValidationError as e:
            logger.exception(
                "user_state_validation_failed",
                context=context,
                status_code=status_code,
                validation_errors=e.errors(),
                message="Failed to validate user state response",
            )
            raise APIError(
                code=APIErrorCode.RESPONSE_VALIDATION_FAILED.value,
                message="Failed to validate user state response",
                metadata={"validation_errors": e.errors()},
            ) from e

    @staticmethod
    def handle_get_clearinghouse_state_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawClearinghouseState:
        """Handle clearinghouse state response using protocol interface.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing clearinghouse state data
        """
        # Clearinghouse state uses the same model as user state
        return HyperliquidAccountResponseHandler.handle_get_user_state_response(
            raw_response_content, status_code
        )

    @staticmethod
    def handle_get_open_orders_response(
        raw_response_content: ParsedJsonResponse, status_code: int
    ) -> HyperliquidRawOpenOrdersResponse:
        """Handle open orders response using protocol interface.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order data
        """
        context = "get_open_orders"
        try:
            # Hyperliquid API returns the orders list directly
            # Validate that we have a list response
            orders_list = ensure_list_response(raw_response_content, context, status_code)
            return HyperliquidRawOpenOrdersResponse(orders_list)
        except (ValidationError, ValueError) as e:
            logger.exception(
                "open_orders_validation_failed",
                context=context,
                status_code=status_code,
                error=str(e),
                message="Failed to validate open orders response",
            )
            raise APIError(
                code=APIErrorCode.RESPONSE_VALIDATION_FAILED.value,
                message="Failed to validate open orders response",
                metadata={"error": str(e)},
            ) from e

    @staticmethod
    def handle_get_user_fills_response(
        raw_response_content: ParsedJsonResponse, status_code: int
    ) -> HyperliquidRawUserFillsResponse:
        """Handle user fills response using protocol interface.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing fill data
        """
        context = "get_user_fills"
        try:
            # Hyperliquid API returns the fills list directly
            # Validate that we have a list response
            fills_list = ensure_list_response(raw_response_content, context, status_code)
            return HyperliquidRawUserFillsResponse(fills_list)
        except (ValidationError, ValueError) as e:
            logger.exception(
                "user_fills_validation_failed",
                context=context,
                status_code=status_code,
                error=str(e),
                message="Failed to validate user fills response",
            )
            raise APIError(
                code=APIErrorCode.RESPONSE_VALIDATION_FAILED.value,
                message="Failed to validate user fills response",
                metadata={"error": str(e)},
            ) from e

    @staticmethod
    def handle_transfer_response(
        raw_response_content: ParsedJsonResponse,
        status_code: int,
    ) -> HyperliquidRawUsdTransferResponse:
        """Handle internal transfer response following Backpack pattern.

        Validates basic response structure and logs warnings for invalid fields.
        Returns validated Pydantic model for mapper processing.

        Args:
            raw_response_content: Raw JSON response from Hyperliquid /exchange endpoint
            status_code: HTTP status code from the response

        Returns:
            HyperliquidRawUsdTransferResponse: Validated Pydantic model for mapper transformation

        Raises:
            APIError: If response structure is fundamentally invalid
        """
        context = "internal_transfer"

        logger.debug(
            "hyperliquid_transfer_response_handler_start",
            context=context,
            status_code=status_code,
            response_type=type(raw_response_content).__name__,
            message=f"[{context}] Starting validation of transfer response",
        )

        try:
            # Ensure we have a dictionary response
            response_dict = ensure_dict_response(raw_response_content, context, status_code)

            # Validate response using HyperliquidRawUsdTransferResponse model
            validated_response = HyperliquidRawUsdTransferResponse.model_validate(response_dict)

            logger.debug(
                "hyperliquid_transfer_response_handler_complete",
                context=context,
                status_code=status_code,
                response_status=validated_response.status,
                has_response_data=validated_response.response is not None,
                message=(
                    f"[{context}] Successfully validated transfer response using "
                    f"HyperliquidRawUsdTransferResponse"
                ),
            )

        except (ValidationError, ValueError) as e:
            logger.exception(
                "hyperliquid_transfer_response_validation_failed",
                context=context,
                status_code=status_code,
                error=str(e),
                message="Failed to validate transfer response structure",
            )
            raise APIError(
                code=APIErrorCode.RESPONSE_VALIDATION_FAILED.value,
                message="Failed to validate transfer response",
                metadata={"error": str(e), "context": context},
            ) from e
        else:
            return validated_response
