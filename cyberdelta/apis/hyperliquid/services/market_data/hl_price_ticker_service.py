"""Hyperliquid Price Ticker Service.

This service handles price and ticker data operations for the Hyperliquid exchange,
extracted from the monolithic market data service to improve maintainability and testability.

Focused on:
- Individual ticker data retrieval with symbol filtering
- All mid prices fetching for efficient market order pricing
- Asset context data retrieval (raw metadata and contexts)
- Price-related data validation and transformation
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import NoReturn

from pydantic import ValidationError

from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.response_validation import EmptyResponseError
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import MarketDataRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import (
    MarketDataResponseHandlerProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    HistoricalDataMapperProtocol,
    PriceTickerMapperProtocol,
)
from cyberdelta.apis.utils.response_validation import ensure_dict_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.models import FundingRate, Ticker
from cyberdelta.models.market.mid_prices import MidPrices
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidPriceTickerService:
    """Focused service for Hyperliquid price and ticker operations.

    Handles individual ticker data, mid prices, and asset context retrieval
    with comprehensive error handling and data validation.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: MarketDataRequestBuilderProtocol,
        response_handler: MarketDataResponseHandlerProtocol,
        mapper: PriceTickerMapperProtocol,
        historical_data_mapper: HistoricalDataMapperProtocol,
        exchange_name: str = "hyperliquid",
    ) -> None:
        """Initialize the price ticker service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            mapper: Mapper for converting raw data to internal domain models
            historical_data_mapper: Mapper for converting historical data (funding rates, etc.)
            exchange_name: Name identifier for this exchange instance
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._historical_data_mapper = historical_data_mapper
        self._exchange_name = exchange_name

    async def get_all_asset_contexts_raw(self) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Retrieve the metadata for all listed assets and their current context.

        Fetches mark price, funding rate, etc. by calling the /info endpoint.
        Hyperliquid's /info endpoint returns multiple data types; the handler
        extracts and validates the metaAndAssetCtxs part.

        Returns:
            HyperliquidRawMetaAndAssetCtxsResponse object containing validated raw data

        Raises:
            APIError: If the API request fails or the response is invalid
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_all_asset_contexts_raw"

        # Initialize context for error handling
        raw_response_content_parsed: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content_str: str | None = None

        try:
            logger.debug(
                "fetching_asset_contexts",
                exchange=self._exchange_name,
                method=current_method,
                message="Fetching all asset contexts from /info endpoint",
            )

            # Core operational logic
            endpoint_path = "/info"

            # Build request for metaAndAssetCtxs
            request_payload_model = self._request_builder.build_info_request_payload()
            request_payload_data_dict = request_payload_model.model_dump(
                by_alias=True,
                exclude_none=True,
            )

            headers: Mapping[str, str] = {}

            # Execute API request
            raw_response_content_parsed, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=request_payload_data_dict,
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.UNSIGNED,
                    endpoint_group="public",
                    request_weight=2,
                ),
            )

            if raw_response_content_parsed is not None:
                raw_response_content_str = str(raw_response_content_parsed)

            logger.debug(
                "raw_asset_contexts_response",
                exchange=self._exchange_name,
                method=current_method,
                status_code=status_code,
                has_data=raw_response_content_parsed is not None,
                message="Raw asset contexts response received",
            )

            # Validate response not None
            validated_response = self._validate_response_not_none(
                raw_response_content_parsed,
                "metaAndAssetCtxs data",
                "metaAndAssetCtxs",
                status_code,
            )

            # Process response through handler
            # The meta and asset contexts response is a list, not a dict
            validated_raw_meta_and_asset_ctxs = (
                self._response_handler.handle_info_meta_and_asset_ctxs_response(
                    validated_response,
                    status_code,
                    headers,
                )
            )

            logger.info(
                "asset_contexts_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                asset_count=len(validated_raw_meta_and_asset_ctxs.asset_ctxs)
                if validated_raw_meta_and_asset_ctxs.asset_ctxs
                else 0,
                universe_count=len(validated_raw_meta_and_asset_ctxs.meta.universe)
                if validated_raw_meta_and_asset_ctxs.meta
                else 0,
                message="Successfully retrieved asset contexts",
            )

        except APIError:
            # Re-raise APIErrors from lower layers
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_transform),
                message="Failed to transform exchange data for asset contexts",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_transform
        except ValidationError as e_val:
            logger.exception(
                "validation_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_val),
                message="Internal data validation failed for asset contexts",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_service_logic),
                message="Service internal logic error for asset contexts",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_unexpected),
                message="Unexpected service failure for asset contexts",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_unexpected
        else:
            return validated_raw_meta_and_asset_ctxs

    async def get_ticker(self, symbol: Symbol) -> Ticker | None:
        """Retrieve the latest ticker/context information for a specific symbol.

        Fetches all asset contexts and finds the specific one for the requested symbol.

        Args:
            symbol: The Symbol domain object

        Returns:
            Ticker object if the symbol is found, otherwise None.
            Contains ticker-like data (mark price, funding, etc.)

        Raises:
            APIError: If the underlying API request to fetch all contexts fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_ticker"

        # Validate symbol object directly
        self._validate_symbol(symbol.value, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "fetching_ticker",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                symbol_exchange=symbol.exchange.value,
                message="Fetching ticker data for symbol",
            )

            # Core operational logic
            all_contexts_response = await self.get_all_asset_contexts_raw()
            if (
                all_contexts_response
                and all_contexts_response.asset_ctxs
                and all_contexts_response.meta
            ):
                # Match asset contexts with universe names by index
                # The asset contexts are in the same order as the universe
                universe = all_contexts_response.meta.universe
                for i, asset_def in enumerate(universe):
                    if asset_def.name == symbol.value and i < len(all_contexts_response.asset_ctxs):
                        asset_ctx = all_contexts_response.asset_ctxs[i]
                        # Create a copy with the name field populated for the mapper
                        asset_ctx_with_name = asset_ctx.model_copy(update={"name": symbol.value})

                        ticker = self._mapper.transform_raw_asset_ctx_to_ticker(asset_ctx_with_name)

                        logger.info(
                            "ticker_retrieved",
                            exchange=self._exchange_name,
                            method=current_method,
                            symbol=symbol,
                            symbol_exchange=symbol.exchange.value,
                            price=ticker.price if ticker else None,
                            message="Successfully retrieved ticker data",
                        )

                        return ticker

            # Symbol not found in the contexts
            logger.warning(
                "ticker_data_not_found",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                message="Ticker data (asset context) not found for symbol after "
                "fetching all asset contexts",
            )
        except APIError:
            # Re-raise APIErrors from get_all_asset_contexts_raw, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "ticker_transform_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_transform),
                message="Failed to transform exchange data for ticker",
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
                message="Internal data validation failed for ticker",
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
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_service_logic),
                message="Service internal logic error for ticker",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_unexpected),
                message="Unexpected service failure for ticker",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected
        else:
            return None

    async def get_all_mids(self) -> MidPrices:
        """Fetch all mid prices efficiently for market order pricing.

        Uses the /info endpoint with {"type": "allMids"} to get a mapping
        of all symbol mid prices in a single request.

        Returns:
            MidPrices model containing symbol to mid price mapping

        Raises:
            APIError: If the API request fails or the response is invalid
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_all_mids"

        # Initialize context for error handling
        raw_response_content: ParsedJsonResponse | None = None
        status_code: int = 0

        try:
            logger.info(
                "fetching_all_mids",
                exchange=self._exchange_name,
                method=current_method,
                message="Fetching all mid prices for efficient market order pricing",
            )

            # Build request payload
            endpoint_path = "/info"
            request_payload_model = self._request_builder.build_all_mids_request_payload()
            request_payload_data_dict = request_payload_model.model_dump(
                by_alias=True,
                exclude_none=True,
            )

            headers: Mapping[str, str] = {}

            # Make the API request
            raw_response_content, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=request_payload_data_dict,
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.UNSIGNED,
                    endpoint_group="public",
                    request_weight=2,  # AllMids has weight 2
                ),
            )

            logger.debug(
                "raw_all_mids_response",
                exchange=self._exchange_name,
                method=current_method,
                status_code=status_code,
                has_data=raw_response_content is not None,
                message="Raw all mids response received",
            )

            # Use centralized validation
            validated_raw_data = ensure_dict_response(raw_response_content, "all mids", status_code)

            # Parse response with proper validation
            raw_all_mids = self._response_handler.handle_all_mids_response(
                validated_raw_data,
                status_code,
                headers,
            )

            # Transform to internal MidPrices model
            mid_prices = self._mapper.transform_raw_all_mids_to_internal(raw_all_mids)

            logger.info(
                "all_mids_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol_count=len(mid_prices.prices),  # MidPrices always has prices attribute
                message="Successfully retrieved all mid prices",
            )

        except APIError:
            # Re-raise APIErrors as-is
            raise
        except ValidationError as e:
            logger.exception(
                "all_mids_validation_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                status_code=status_code,
                message="Validation error for all mids response",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to validate AllMids response",
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_response_content) if raw_response_content else None,
            ) from e
        except Exception as e:
            logger.exception(
                "all_mids_unexpected_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                status_code=status_code,
                message="Unexpected error fetching all mid prices",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error fetching all mid prices",
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_response_content) if raw_response_content else None,
            ) from e
        else:
            return mid_prices

    def _validate_response_not_none(
        self,
        response: ParsedJsonResponse | None,
        response_type: str,
        operation: str,
        status_code: int,
    ) -> ParsedJsonResponse:
        """Validate that response is not None.

        Args:
            response: The response to validate
            response_type: Type of response expected
            operation: Operation that returned the response
            status_code: HTTP status code

        Returns:
            The validated non-None response for type narrowing

        Raises:
            EmptyResponseError: If response is None
        """
        if response is None:
            raise EmptyResponseError(
                response_type=response_type,
                operation=operation,
                http_status=status_code,
                exchange=self._exchange_name,
            )
        return response

    def _validate_symbol(self, symbol: str, current_method: str) -> None:
        """Validate symbol parameter.

        Args:
            symbol: Trading symbol to validate
            current_method: Calling method name for error context

        Raises:
            ValueError: If symbol is invalid
        """
        if not symbol:
            error_msg = f"[{current_method}] 'symbol' must be a non-empty string."
            raise ValueError(error_msg)

        # Strip and check again
        if not symbol.strip():
            error_msg = f"[{current_method}] 'symbol' cannot be empty or whitespace only."
            raise ValueError(error_msg)

    async def get_funding_rate(self, symbol: Symbol) -> FundingRate | None:
        """Retrieve the current funding rate information for a specific perpetual contract symbol.

        This is typically part of the broader asset context. It calls get_all_asset_contexts
        and extracts the relevant context.

        Args:
            symbol: The Symbol domain object for the perpetual contract.

        Returns:
            A FundingRate object if the symbol is found, otherwise None.
            The 'funding' field of this object contains the funding rate.

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_funding_rate"

        # Validate symbol object directly
        self._validate_symbol(symbol.value, current_method)

        try:
            # Get funding rate from asset contexts
            return await self._get_funding_rate_from_contexts(symbol)
        except APIError:
            # Re-raise API errors as-is
            raise
        except Exception as e:
            logger.exception(
                "unexpected_service_failure",
                action=current_method,
                exchange=self._exchange_name,
                symbol=symbol,
                symbol_exchange=symbol.exchange.value,
                error=str(e),
                message=f"Unexpected service failure for {symbol}: {e}",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e,
            ) from e

    async def _get_funding_rate_from_contexts(self, symbol: Symbol) -> FundingRate | None:
        """Get funding rate for symbol from asset contexts.

        Returns:
            FundingRate | None: The funding rate or None if not found
        """
        all_contexts_response = await self.get_all_asset_contexts_raw()
        if (
            all_contexts_response
            and all_contexts_response.asset_ctxs
            and all_contexts_response.meta
        ):
            # Match asset contexts with universe names by index
            # The asset contexts are in the same order as the universe
            universe = all_contexts_response.meta.universe
            for i, asset_def in enumerate(universe):
                if asset_def.name == symbol.value and i < len(all_contexts_response.asset_ctxs):
                    asset_ctx = all_contexts_response.asset_ctxs[i]
                    # Create a copy with the name field populated for the mapper
                    asset_ctx_with_name = asset_ctx.model_copy(update={"name": symbol.value})
                    return self._historical_data_mapper.transform_raw_asset_ctx_to_funding_rate(
                        asset_ctx_with_name,
                    )

        logger.warning(
            "funding_rate_not_found",
            action="_get_funding_rate_from_contexts",
            exchange=self._exchange_name,
            symbol=symbol.value,
            message=f"Symbol {symbol.value} not found in universe or no funding rate available",
        )
        return None

    def _raise_invalid_meta_response_type_error(self, status_code: int) -> NoReturn:
        """Raise an APIError for invalid meta response type.

        Args:
            status_code: HTTP status code

        Raises:
            APIError: Always raises with INVALID_RESPONSE code.
        """
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Expected dict response for meta and asset contexts",
            http_status=status_code,
        )
