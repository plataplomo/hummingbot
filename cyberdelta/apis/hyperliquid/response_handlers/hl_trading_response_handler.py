"""Hyperliquid Trading Response Handler.

This module handles the validation and processing of trading-related API responses,
extracted from the monolithic response handler to improve maintainability and testability.

Focused on:
- Order placement responses
- Order cancellation responses
- Order status and query responses
- Open orders responses
- User fills (trade history) responses
- Exchange response validation
"""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions import MissingRequiredParameterError
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
    HyperliquidRawHistoricalOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
    HyperliquidRawOrderStatusResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import TradingResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.response_handlers.hl_response_handler_base import (
    HyperliquidResponseHandlerBase,
)
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)


class HyperliquidTradingResponseHandler(
    HyperliquidResponseHandlerBase,
    TradingResponseHandlerProtocol,
):
    """Handles validation of trading-related JSON responses from Hyperliquid API.

    Uses Pydantic models to validate the structure and types of the raw data.
    Raises APIError if validation fails. Implements TradingResponseHandlerProtocol
    for type safety and consistency.
    """

    # Base protocol method implementation
    def handle_response(
        self,
        response: dict[str, object],
        status_code: int,
        headers: dict[str, str],
        context: str,
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
        # All trading responses use the exchange response handler
        return self.handle_exchange_response(response, status_code, headers)

    def handle_exchange_response(
        self,
        raw_response_content: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawExchangeResponse:
        """Validate raw JSON response for exchange trading operations.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawExchangeResponse: Validated exchange response model

        Raises:
            MissingRequiredParameterError: If status_code is None

        Note:
            The _handle_validation_error method may raise APIError if validation fails.
        """
        context = "exchange_response"
        try:
            # Ensure we have a dict
            # DEFENSIVE CHECK: Handle None status code. Pyright=[reportArgumentType]
            if status_code is None:
                raise MissingRequiredParameterError("status_code", context)
            response_dict = ensure_dict_response(raw_response_content, context, status_code)
            # Validate with Pydantic
            return HyperliquidRawExchangeResponse.model_validate(response_dict)
        except ValidationError as e:
            raise self._handle_validation_error(
                e,
                context,
                raw_response_content,
                status_code,
                headers,
            ) from e

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
            MissingRequiredParameterError: If status_code is None

        Note:
            The _handle_validation_error method may raise APIError if validation fails.
        """
        context = "info_open_orders"
        try:
            # Response is expected to be a list
            # DEFENSIVE CHECK: Handle None status code. Pyright=[reportArgumentType]
            if status_code is None:
                raise MissingRequiredParameterError("status_code", context)
            response_list = ensure_list_response(raw_data, context, status_code)
            # Wrap in response model
            return HyperliquidRawOpenOrdersResponse(response_list)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_info_user_fills_response(
        self,
        raw_response_content: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawUserFillsResponse:
        """Validate raw JSON response for user fills (trade history) info endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawUserFillsResponse: Validated user fills response

        Raises:
            MissingRequiredParameterError: If status_code is None

        Note:
            The _handle_validation_error method may raise APIError if validation fails.
        """
        context = "info_user_fills"
        try:
            # Response is expected to be a list
            # DEFENSIVE CHECK: Handle None status code. Pyright=[reportArgumentType]
            if status_code is None:
                raise MissingRequiredParameterError("status_code", context)
            response_list = ensure_list_response(raw_response_content, context, status_code)
            # Wrap in response model
            return HyperliquidRawUserFillsResponse(response_list)
        except ValidationError as e:
            raise self._handle_validation_error(
                e,
                context,
                raw_response_content,
                status_code,
                headers,
            ) from e

    def handle_info_order_status_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawOrderStatusResponse:
        """Validate raw JSON response for order status info endpoint.

        Args:
            raw_data: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawOrderStatusResponse: Validated order status

        Raises:
            APIError: If order not found (unknownOid status)
            MissingRequiredParameterError: If status_code is None

        Note:
            The _handle_validation_error method may raise APIError if validation fails.
        """
        context = "info_order_status"
        try:
            # Ensure we have a dict
            # DEFENSIVE CHECK: Handle None status code. Pyright=[reportArgumentType]
            if status_code is None:
                raise MissingRequiredParameterError("status_code", context)
            response_dict = ensure_dict_response(raw_data, context, status_code)

            # Check for error statuses before Pydantic validation
            if "status" in response_dict and response_dict["status"] == "unknownOid":
                raise APIError(
                    message="Order not found",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                    metadata={"original_status": response_dict["status"]},
                )

            # Use the model that matches the actual API structure
            return HyperliquidRawOrderStatusResponse(**response_dict)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_historical_orders_response(
        self,
        raw_response_content: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawHistoricalOrdersResponse:
        """Validate raw JSON response for historical orders endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawHistoricalOrdersResponse: Validated historical orders

        Raises:
            MissingRequiredParameterError: If status_code is None

        Note:
            The _handle_validation_error method may raise APIError if validation fails.
        """
        context = "historical_orders"
        try:
            # Response is expected to be a list
            # DEFENSIVE CHECK: Handle None status code. Pyright=[reportArgumentType]
            if status_code is None:
                raise MissingRequiredParameterError("status_code", context)
            response_list = ensure_list_response(raw_response_content, context, status_code)
            # Convert each dict to HyperliquidRawHistoricalOrderResponse
            orders = [
                HyperliquidRawHistoricalOrderResponse(**order_dict) for order_dict in response_list
            ]
            # Wrap in response model
            return HyperliquidRawHistoricalOrdersResponse(orders)
        except ValidationError as e:
            raise self._handle_validation_error(
                e,
                context,
                raw_response_content,
                status_code,
                headers,
            ) from e

    # Protocol implementation methods
    @staticmethod
    def handle_place_order_response(
        raw_response_content: dict[str, object],
        status_code: int,
    ) -> HyperliquidRawExchangeResponse:
        """Handle order placement response using protocol interface.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order placement result

        Raises:
            APIError: If response validation fails
        """
        context = "place_order"
        try:
            # Validate with Pydantic
            return HyperliquidRawExchangeResponse.model_validate(raw_response_content)
        except ValidationError as e:
            logger.exception(
                "place_order_validation_failed",
                context=context,
                status_code=status_code,
                validation_errors=e.errors(),
                message="Failed to validate place order response",
            )
            raise APIError(
                code=APIErrorCode.RESPONSE_VALIDATION_FAILED.value,
                message="Failed to validate place order response",
                metadata={"validation_errors": e.errors()},
            ) from e

    @staticmethod
    def handle_cancel_order_response(
        raw_response_content: dict[str, object],
        status_code: int,
    ) -> HyperliquidRawExchangeResponse:
        """Handle order cancellation response using protocol interface.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order cancellation result

        Raises:
            APIError: If response validation fails
        """
        context = "cancel_order"
        try:
            # Validate with Pydantic
            return HyperliquidRawExchangeResponse.model_validate(raw_response_content)
        except ValidationError as e:
            logger.exception(
                "cancel_order_validation_failed",
                context=context,
                status_code=status_code,
                validation_errors=e.errors(),
                message="Failed to validate cancel order response",
            )
            raise APIError(
                code=APIErrorCode.RESPONSE_VALIDATION_FAILED.value,
                message="Failed to validate cancel order response",
                metadata={"validation_errors": e.errors()},
            ) from e

    @staticmethod
    def handle_cancel_all_orders_response(
        raw_response_content: dict[str, object],
        status_code: int,
    ) -> HyperliquidRawExchangeResponse:
        """Handle cancel all orders response using protocol interface.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing cancel all orders result

        Raises:
            APIError: If response validation fails
        """
        context = "cancel_all_orders"
        try:
            # Validate with Pydantic
            return HyperliquidRawExchangeResponse.model_validate(raw_response_content)
        except ValidationError as e:
            logger.exception(
                "cancel_all_orders_validation_failed",
                context=context,
                status_code=status_code,
                validation_errors=e.errors(),
                message="Failed to validate cancel all orders response",
            )
            raise APIError(
                code=APIErrorCode.RESPONSE_VALIDATION_FAILED.value,
                message="Failed to validate cancel all orders response",
                metadata={"validation_errors": e.errors()},
            ) from e

    @staticmethod
    def handle_modify_order_response(
        raw_response_content: dict[str, object],
        status_code: int,
    ) -> HyperliquidRawExchangeResponse:
        """Handle order modification response using protocol interface.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order modification result
        """
        # Hyperliquid doesn't have direct order modification - it's cancel + place
        # So this would return the same exchange response
        return HyperliquidTradingResponseHandler.handle_place_order_response(
            raw_response_content,
            status_code,
        )
