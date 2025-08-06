"""Hyperliquid Fill History Service.

This service handles all fill history operations for the Hyperliquid exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Fill history retrieval
- Fill filtering and processing
- Fill transformation and mapping
- Comprehensive error handling
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFillsResponse
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import TradingRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import AccountResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import TransactionMapperProtocol
from cyberdelta.apis.models.service_args.hyperliquid import HyperliquidGetUserFillsArgs
from cyberdelta.apis.models.service_args.trading import GetTradeHistoryArgs
from cyberdelta.apis.utils.response_validation import ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models import Fill
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidTradeHistoryService:
    """Focused service for Hyperliquid trade history operations.

    Handles validation, processing, and transformation of trade history requests
    with comprehensive error handling and filtering capabilities.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: TradingRequestBuilderProtocol,
        response_handler: AccountResponseHandlerProtocol,
        mapper: TransactionMapperProtocol,
        authenticator: IAuthenticator | None,
        exchange_name: str = "hyperliquid",
        wallet_address: str | None = None,
    ) -> None:
        """Initialize the trade history service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            mapper: Data mapper for transforming raw responses to internal models
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            wallet_address: Wallet address for trade history requests
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address

    async def get_fill_history(self, args: GetTradeHistoryArgs) -> list[Fill]:
        """Retrieve user fill history (fills).

        Args:
            args: Parameters for filtering fill history including symbol and limit.

        Returns:
            list[Fill]: List of fill executions

        Raises:
            APIError: If trade history retrieval fails or processing fails
            TransformationError: If response transformation to internal models fails
            ValidationError: If response validation or data validation fails
            ValueError: If service logic encounters invalid values during processing
            TypeError: If service logic encounters type errors during processing

        Note:
            Hyperliquid's userFills endpoint doesn't support server-side filtering.
            Symbol filtering and limit are applied client-side after fetching all fills.
        """
        # Initialize context for error handling
        raw_response_list: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "retrieving_trade_history",
                exchange=self._exchange_name,
                symbol=args.symbol,
                limit=args.limit,
                message="Retrieving fill history from API",
            )

            (
                raw_response_list,
                status_code,
                raw_response_content,
            ) = await self._fetch_trade_history_data()

            validated_response = self._validate_trade_history_response(
                raw_response_list,
                status_code,
            )
            validated_fills_response = self._process_trade_history_response(
                validated_response,
                status_code,
            )

            # Map to internal Fill models
            internal_fills = self._map_fills_to_internal_fills(validated_fills_response)

            # Apply client-side filtering
            filtered_fills = self._apply_fill_filters(internal_fills, args)

            logger.info(
                "fill_history_retrieved",
                exchange=self._exchange_name,
                symbol=args.symbol,
                fill_count=len(filtered_fills),
                total_fetched=len(internal_fills),
                message="Successfully retrieved fill history",
            )
        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_transformation_error(
                e_transform,
                "get_fill_history",
                status_code,
                raw_response_content,
            )
            raise
        except ValidationError as e_val:
            self._handle_validation_error(
                e_val,
                "get_fill_history",
                status_code,
                raw_response_content,
            )
            raise
        except (ValueError, TypeError) as e_service_logic:
            self._handle_service_logic_error(e_service_logic, "get_fill_history")
            raise
        except Exception as e_unexpected:
            self._handle_unexpected_error(
                e_unexpected,
                "get_fill_history",
                status_code,
                raw_response_content,
            )
            raise
        else:
            return filtered_fills

    async def _fetch_trade_history_data(
        self,
    ) -> tuple[ParsedJsonResponse | None, int, str | None]:
        """Fetch trade history data from the API.

        Returns:
            tuple: Raw data, status code, and response content

        Raises:
            APIError: If the request fails or response is invalid
        """
        if not self._authenticator:
            raise APIError(
                message="Authentication required for retrieving trade history",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        if not self._wallet_address:
            raise APIError(
                message="Wallet address required for trade history requests",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        endpoint = "/info"
        user_fills_args = HyperliquidGetUserFillsArgs(wallet_address=self._wallet_address)
        payload = self._request_builder.build_user_fills_request_payload(user_fills_args)

        logger.debug(
            "fetching_trade_history_data",
            exchange=self._exchange_name,
            wallet_address=self._wallet_address,
            message="Fetching trade history data from API",
        )

        raw_data, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint,
            data=payload,
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.UNSIGNED,  # User fills requests don't require signing
                endpoint_group="info",
                request_weight=1,
            ),
        )

        raw_response_content = str(raw_data) if raw_data is not None else None
        return raw_data, status_code, raw_response_content

    def _validate_trade_history_response(
        self,
        raw_response_list: ParsedJsonResponse | None,
        status_code: int,
    ) -> ParsedJsonResponse:
        """Validate the trade history response.

        Args:
            raw_response_list: Raw response data
            status_code: HTTP status code

        Returns:
            ParsedJsonResponse: Validated response data

        Note:
            This method delegates to ensure_list_response which may raise APIError.
        """
        return ensure_list_response(
            raw_response_list,
            "user fills",
            status_code,
        )

    def _process_trade_history_response(
        self,
        validated_response: ParsedJsonResponse,
        status_code: int,
    ) -> HyperliquidRawUserFillsResponse:
        """Process the trade history response.

        Args:
            validated_response: Validated response data
            status_code: HTTP status code

        Returns:
            HyperliquidRawUserFillsResponse: Processed user fills response

        Raises:
            APIError: If processing fails
        """
        # Type guard for response handler
        if not isinstance(validated_response, dict):
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"Expected dict response for user fills, got {type(validated_response)}",
                http_status=status_code,
            )

        return self._response_handler.handle_info_user_fills_response(
            validated_response,
            status_code,
        )

    def _map_fills_to_internal_fills(
        self,
        fills_response: HyperliquidRawUserFillsResponse,
    ) -> list[Fill]:
        """Map raw fills to internal Fill models.

        Args:
            fills_response: Raw user fills response

        Returns:
            list[Fill]: Mapped internal fills

        Note:
            The mapper may raise TransformationError if mapping fails.
        """
        return [
            self._mapper.transform_raw_user_fill_to_internal(fill) for fill in fills_response.root
        ]

    def _apply_fill_filters(self, fills: list[Fill], args: GetTradeHistoryArgs) -> list[Fill]:
        """Apply client-side filtering to fills.

        Args:
            fills: List of fills to filter
            args: Filter arguments

        Returns:
            list[Fill]: Filtered fills
        """
        filtered_fills = fills

        # Filter by symbol if specified
        if args.symbol:
            filtered_fills = [fill for fill in filtered_fills if fill.symbol == args.symbol]
            logger.debug(
                "fills_filtered_by_symbol",
                exchange=self._exchange_name,
                symbol=args.symbol,
                original_count=len(fills),
                filtered_count=len(filtered_fills),
                message="Filtered fills by symbol",
            )

        # Apply limit if specified
        if args.limit and args.limit > 0:
            filtered_fills = filtered_fills[: args.limit]
            logger.debug(
                "fills_limited",
                exchange=self._exchange_name,
                limit=args.limit,
                final_count=len(filtered_fills),
                message="Applied limit to fills",
            )

        return filtered_fills

    def _handle_transformation_error(
        self,
        e_transform: TransformationError,
        method_name: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle transformation errors.

        Logs the transformation error and converts it to an APIError with appropriate context.

        Args:
            e_transform: The transformation error that occurred
            method_name: Name of the method where the error occurred
            status_code: HTTP status code from the response
            raw_response_content: Raw response content for debugging

        Raises:
            APIError: Always raises APIError with transformation error details
        """
        logger.error(
            "transformation_error",
            action=method_name,
            exchange=self._exchange_name,
            error=str(e_transform),
            message="Failed to transform exchange data",
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=e_transform,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e_transform

    def _handle_validation_error(
        self,
        e_val: ValidationError,
        method_name: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle validation errors.

        Logs the validation error and converts it to an APIError with appropriate context.

        Args:
            e_val: The validation error that occurred
            method_name: Name of the method where the error occurred
            status_code: HTTP status code from the response
            raw_response_content: Raw response content for debugging

        Raises:
            APIError: Always raises APIError with validation error details
        """
        logger.error(
            "validation_error",
            action=method_name,
            exchange=self._exchange_name,
            error=str(e_val),
            message="Internal data validation failed",
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=e_val,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e_val

    def _handle_service_logic_error(
        self,
        e_service_logic: ValueError | TypeError,
        method_name: str,
    ) -> None:
        """Handle service logic errors.

        Determines if the error is an input validation error (re-raises) or internal
        service error (converts to APIError).

        Args:
            e_service_logic: The ValueError or TypeError that occurred
            method_name: Name of the method where the error occurred

        Raises:
            APIError: If it's an internal service error

        Note:
            Re-raises the original ValueError or TypeError if it appears to be an input
            validation error (when method_name appears in the error message).
        """
        error_msg = str(e_service_logic)
        if method_name in error_msg:
            # Input validation error - re-raise
            raise e_service_logic
        # Internal service error
        logger.error(
            "service_logic_error",
            action=method_name,
            exchange=self._exchange_name,
            error=str(e_service_logic),
            message="Service internal logic error",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=e_service_logic,
        ) from e_service_logic

    def _handle_unexpected_error(
        self,
        e_unexpected: Exception,
        method_name: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle unexpected errors.

        Logs unexpected errors and converts them to APIError for consistent error handling.

        Args:
            e_unexpected: The unexpected exception that occurred
            method_name: Name of the method where the error occurred
            status_code: HTTP status code from the response
            raw_response_content: Raw response content for debugging

        Raises:
            APIError: Always raises APIError with unexpected error details
        """
        logger.error(
            "unexpected_service_failure",
            action=method_name,
            exchange=self._exchange_name,
            error=str(e_unexpected),
            message="Unexpected service failure",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e_unexpected,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e_unexpected
