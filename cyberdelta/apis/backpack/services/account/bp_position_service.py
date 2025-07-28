"""Backpack Position Service.

This service handles all position-related operations for the Backpack exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Derivative position retrieval and management
- Position validation and filtering
- Position data transformation
- Collateral calculations for positions
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackPositionMapper
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionResponse
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.services.account.bp_account_state_service import (
    BackpackAccountStateService,
)
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions import EmptyResponseError
from cyberdelta.apis.utils import ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import DerivativePosition
from cyberdelta.exceptions.service_validation import EmptyStringParameterError
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackPositionService:
    """Focused service for Backpack position operations.

    Handles retrieval, validation, and transformation of derivative position data
    with comprehensive error handling and symbol filtering.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackAccountRequestBuilder,
        response_handler: BackpackAccountResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str = "backpack",
        # Optional dependency injection for shared state service (Hyperliquid pattern)
        account_state_service: BackpackAccountStateService | None = None,
        # Optional dependency injection for mapper
        mapper: BackpackPositionMapper | None = None,
    ) -> None:
        """Initialize the position service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            account_state_service: Optional shared account state service
                (creates default if not provided)
            mapper: Optional position mapper instance for dependency injection
                (creates default if not provided)
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name

        # Shared state service injection (following Hyperliquid clearinghouse pattern)
        self._account_state_service = account_state_service or BackpackAccountStateService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
        )

        # Provide sensible default if mapper not injected
        self._mapper = mapper or BackpackPositionMapper()

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Fetch derivative positions, optionally filtered by symbol.

        Args:
            symbol: Optional symbol to filter positions (e.g., "SOL-PERP")

        Returns:
            List of DerivativePosition objects

        Raises:
            APIError: If API request fails or data transformation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_positions"

        # Validate symbol parameter
        self._validate_position_symbol(symbol, current_method)

        try:
            # Get raw positions
            raw_positions = await self._get_raw_positions_list(symbol)

            # Transform to internal models
            internal_positions = await self._transform_raw_positions(raw_positions, symbol)

            logger.info(
                "positions_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                position_count=len(internal_positions),
                symbol_filter=symbol,
                message="Successfully retrieved derivative positions",
            )

        except APIError:
            raise
        except (ValidationError, TransformationError, ValueError, TypeError) as e:
            return self._handle_position_exception(e, current_method, symbol)
        else:
            return internal_positions

    async def _get_raw_positions_list(
        self, symbol: str | None = None
    ) -> list[BackpackRawPositionResponse]:
        """Fetch raw position data from the API.

        Args:
            symbol: Optional symbol filter

        Returns:
            List of raw position objects

        Raises:
            APIError: If API request fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "_get_raw_positions_list"

        if not self._authenticator:
            raise APIError(
                message="Authentication required for fetching positions",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        try:
            # Build request
            request_params = self._request_builder.build_get_positions_params(symbol=symbol)

            # Execute request
            raw_response, status_code, _headers = await self._http_client_requester(
                method="GET",
                endpoint="/api/v1/position",
                params=request_params.model_dump(exclude_none=True),
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.SIGNED,
                    endpoint_group="private",
                    request_weight=1,
                ),
            )

            # Ensure list response
            raw_data = ensure_list_response(raw_response, "positions", status_code)

            # Handle response
            raw_positions = self._response_handler.handle_get_positions_response(
                raw_data, symbol, status_code
            )

            if not raw_positions:
                logger.debug(
                    "no_positions_found",
                    exchange=self._exchange_name,
                    method=current_method,
                    symbol=symbol,
                    message="No positions returned from exchange",
                )
                return []

            # Filter by symbol if provided
            if symbol:
                filtered_positions = [
                    pos
                    for pos in raw_positions
                    if pos.symbol and pos.symbol.upper() == symbol.upper()
                ]

                logger.debug(
                    "positions_filtered",
                    exchange=self._exchange_name,
                    method=current_method,
                    symbol=symbol,
                    original_count=len(raw_positions),
                    filtered_count=len(filtered_positions),
                    message="Filtered positions by symbol",
                )

                return filtered_positions

        except APIError:
            raise
        except Exception as e:
            logger.exception(
                "get_raw_positions_failed",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e),
                message="Failed to get raw positions",
            )
            raise APIError(
                message="Failed to retrieve position data",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        else:
            return raw_positions

    async def _transform_raw_positions(
        self,
        raw_positions: list[BackpackRawPositionResponse],
        symbol: str | None,
    ) -> list[DerivativePosition]:
        """Transform raw position data to internal models.

        Args:
            raw_positions: List of raw position objects
            symbol: Symbol filter for logging context

        Returns:
            List of DerivativePosition objects

        Raises:
            TransformationError: If transformation fails for all positions
        """
        internal_positions: list[DerivativePosition] = []
        transform_errors: list[str] = []

        for raw_position in raw_positions:
            try:
                internal_position = self._mapper.transform_raw_position_to_internal(raw_position)
                internal_positions.append(internal_position)

            except TransformationError as e:
                position_symbol = getattr(raw_position, "symbol", "unknown")
                error_msg = f"Failed to transform position for {position_symbol}: {e}"
                transform_errors.append(error_msg)
                logger.exception(
                    "position_transform_error",
                    exchange=self._exchange_name,
                    symbol=position_symbol,
                    error=str(e),
                    message=error_msg,
                )
                continue

        if transform_errors and not internal_positions:
            raise TransformationError(
                message="Failed to transform any position data",
                source_data={"errors": transform_errors, "symbol_filter": symbol},
            )

        return internal_positions

    def _validate_position_symbol(self, symbol: str | None, method_name: str) -> None:
        """Validate position symbol parameter.

        Args:
            symbol: Symbol to validate
            method_name: Name of calling method for error context

        Raises:
            EmptyStringParameterError: If symbol is empty string
        """
        if symbol is not None and not symbol:
            raise EmptyStringParameterError(
                parameter_name="symbol",
                method_name=method_name,
            )

    def _handle_position_value_error(
        self,
        error: ValueError,
        current_method: str,
        symbol: str | None,
    ) -> list[DerivativePosition]:
        """Handle ValueError during position operations.

        Args:
            error: The ValueError that occurred
            current_method: Name of the calling method
            symbol: Symbol filter if provided

        Returns:
            Empty list or raises the error
        """
        error_msg = str(error).lower()

        # Check for specific known errors
        if "empty string" in error_msg or "symbol" in error_msg:
            # Re-raise validation errors
            raise error

        # Log and return empty list for other value errors
        logger.warning(
            "position_value_error",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol,
            error=str(error),
            message="Value error during position operation, returning empty list",
        )
        return []

    def _handle_position_exception(
        self,
        error: Exception,
        current_method: str,
        symbol: str | None,
    ) -> list[DerivativePosition]:
        """Handle exceptions during position operations.

        Args:
            error: The exception that occurred
            current_method: Name of the calling method
            symbol: Symbol filter if provided

        Returns:
            Empty list or raises APIError

        Raises:
            APIError: If error processing fails or for certain error types
        """
        if isinstance(error, ValueError):
            return self._handle_position_value_error(error, current_method, symbol)

        if isinstance(error, TransformationError):
            logger.error(
                "position_transformation_failed",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(error),
                message="Failed to transform position data",
            )
            # Return empty list for transformation errors
            return []

        if isinstance(error, ValidationError):
            logger.error(
                "position_validation_failed",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(error),
                message="Position data validation failed",
            )
            # Return empty list for validation errors
            return []

        if isinstance(error, EmptyResponseError):
            logger.info(
                "no_positions_response",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                message="No positions returned by exchange",
            )
            return []

        logger.error(
            "unexpected_position_error",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol,
            error=str(error),
            message="Unexpected error during position operation",
        )

        # For unexpected errors, wrap and raise
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected error retrieving positions",
            original_exception=error,
        ) from error
