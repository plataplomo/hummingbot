"""Backpack Price Ticker Service.

This service handles all price ticker operations for the Backpack exchange,
extracted from the monolithic market data service to improve maintainability and testability.

Focused on:
- Single ticker retrieval
- All tickers retrieval (when implemented)
- Ticker transformation and mapping
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackTickerMapper
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawTickerResponse
from cyberdelta.apis.backpack.request_builders.bp_market_data_request_builder import (
    BackpackMarketDataRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_market_data_response_handler import (
    BackpackMarketDataResponseHandler,
)
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.market_data_service import (
    EmptySymbolError,
    NotImplementedServiceError,
)
from cyberdelta.apis.exceptions.response_validation import UnreachableCodeError
from cyberdelta.apis.utils.response_validation import ensure_dict_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market import Ticker
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackPriceTickerService:
    """Focused service for Backpack price ticker operations.

    Handles validation, processing, and transformation of ticker requests
    with comprehensive error handling and support for single and bulk operations.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackMarketDataRequestBuilder,
        response_handler: BackpackMarketDataResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str = "backpack",
        ticker_mapper: BackpackTickerMapper | None = None,
    ) -> None:
        """Initialize the price ticker service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            ticker_mapper: Optional ticker mapper instance (defaults to new instance)
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._ticker_mapper = ticker_mapper or BackpackTickerMapper()

    async def get_ticker(self, symbol: str) -> Ticker:
        """Retrieve the current ticker data for a specific symbol.

        Args:
            symbol: The trading symbol to get ticker data for

        Returns:
            Ticker: Current ticker information including price and volume data

        Raises:
            APIError: If ticker retrieval fails or processing fails
            EmptySymbolError: If symbol is empty or whitespace
            TypeError: If symbol is not a string
            ValueError: If validation fails
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_ticker"

        if not symbol:
            raise EmptySymbolError(current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.info(
                "retrieving_ticker",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                message="Retrieving ticker data from exchange",
            )

            endpoint = "/api/v1/ticker"
            params = self._request_builder.build_get_ticker_params(symbol)

            response_tuple = await self._http_client_requester(
                method="GET",
                endpoint=endpoint,
                params=params.model_dump(by_alias=True, exclude_none=True),
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.UNSIGNED,
                    endpoint_group="public",
                    request_weight=1,
                ),
            )
            raw_data, status_code, headers = response_tuple

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                "ticker_response",
                exchange=self._exchange_name,
                symbol=symbol,
                raw_data=raw_data,
                status_code=status_code,
                headers=headers,
                message="Received raw ticker response",
            )

            validated_data = ensure_dict_response(raw_data, f"ticker ({symbol})", status_code)

            raw_ticker_model: BackpackRawTickerResponse = (
                self._response_handler.handle_get_ticker_response(
                    validated_data,
                    symbol,
                    status_code,
                    headers,
                )
            )
            internal_ticker = self._ticker_mapper.transform_raw_ticker_to_internal(
                raw_ticker_model,
                symbol_override=symbol,
            )

            logger.info(
                "ticker_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                last_price=str(internal_ticker.price),
                volume=str(internal_ticker.volume),
                message="Successfully retrieved ticker data",
            )

            logger.debug(
                "ticker_mapped",
                exchange=self._exchange_name,
                symbol=symbol,
                internal_ticker=internal_ticker,
                message="Mapped ticker to internal model",
            )

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
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
        except ValidationError as e_val:
            logger.exception(
                "validation_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
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
        except (ValueError, TypeError) as e_service_logic:
            # Check if this is from our own input parameter validation
            error_msg = str(e_service_logic)
            if current_method in error_msg and "symbol" in error_msg:
                # This is likely from our input parameter validation - re-raise as is
                raise
            # This is from service internal logic - wrap as APIError
            logger.exception(
                "logic_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_service_logic),
                message="Service internal logic error",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unhandled:
            logger.exception(
                "unexpected_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_unhandled),
                message="Unexpected error occurred",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error occurred.",
                original_exception=e_unhandled,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unhandled
        else:
            return internal_ticker

    async def get_all_tickers(self) -> dict[str, Ticker]:
        """Retrieves tickers for all available markets.

        Note: This method is not yet implemented for Backpack as the API
        does not provide a single endpoint for all tickers.

        Raises:
            APIError: If future implementation encounters API errors
            UnreachableCodeError: If code path validation fails
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_all_tickers"

        # No input parameters to validate for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            # TODO: Backpack API does not seem to have a single endpoint for all tickers.
            # This might require fetching all symbols first, then getting ticker for each.
            # Or, the OpenAPI spec might list all tickers directly under /markets.
            # For now, this is not implemented as the builder/handler methods are missing.
            logger.warning(
                "get_all_tickers_not_implemented",
                exchange=self._exchange_name,
                message="Method not implemented for Backpack",
            )
            self._validate_get_all_tickers_implementation()

        except APIError:
            # Re-raise APIErrors from any future implementation
            raise
        except TransformationError as e_transform:
            logger.exception(
                "all_tickers_transform_error",
                exchange=self._exchange_name,
                method=current_method,
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
        except ValidationError as e_val:
            logger.exception(
                "all_tickers_validation_error",
                exchange=self._exchange_name,
                method=current_method,
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
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "all_tickers_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_service_logic),
                message="Service internal logic error",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "all_tickers_unexpected_error",
                exchange=self._exchange_name,
                method=current_method,
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
        # This code is unreachable because _validate_get_all_tickers_implementation always raises
        # But we need this to satisfy mypy's return type checking
        raise UnreachableCodeError(
            reason=(
                "This code should never be reached - "
                "_validate_get_all_tickers_implementation always raises"
            )
        )

    @staticmethod
    def _validate_get_all_tickers_implementation() -> None:
        """Validate that get_all_tickers is implemented.

        Raises:
            NotImplementedServiceError: Always, as this method is not yet implemented.
        """
        raise NotImplementedServiceError(
            "BackpackPriceTickerService",
            "get_all_tickers",
        )
