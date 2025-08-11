"""Hyperliquid Market Metadata Service.

This service handles market metadata operations for the Hyperliquid exchange,
extracted from the monolithic market data service to improve maintainability and testability.

Focused on:
- Market listing and metadata retrieval for all available assets
- Individual market metadata lookup by symbol
- Market validation and symbol lookup utilities
- Asset definition and market context processing
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import cast

from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.market_data_service import SymbolNotFoundError
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import MarketDataRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import (
    MarketDataResponseHandlerProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import MarketMetadataMapperProtocol
from cyberdelta.apis.models.service_args.market_data import GetMarketArgs, GetMarketsArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.models.market import Market
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidMarketMetadataService:
    """Focused service for Hyperliquid market metadata operations.

    Handles market listings, individual market metadata retrieval,
    and symbol validation with comprehensive error handling.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: MarketDataRequestBuilderProtocol,
        response_handler: MarketDataResponseHandlerProtocol,
        mapper: MarketMetadataMapperProtocol,
        exchange_name: ExchangeName = ExchangeName.HYPERLIQUID,
    ) -> None:
        """Initialize the market metadata service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            mapper: Mapper for converting raw data to internal domain models
            exchange_name: Name identifier for this exchange instance
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._exchange_name = exchange_name

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Retrieve market metadata for all available markets.

        Uses Hyperliquid's /info endpoint with metaAndAssetCtxs to get
        asset definitions and current contexts, then transforms them to
        internal Market models.

        Args:
            args: GetMarketsArgs (currently unused but maintains interface consistency)

        Returns:
            List of Market objects with metadata for all available assets

        Raises:
            APIError: If API request fails or data transformation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_markets"

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content_str: str | None = None

        try:
            logger.info(
                "fetching_markets",
                exchange=self._exchange_name,
                method=current_method,
                message="Fetching metadata for all available markets",
            )

            # Get raw meta and asset contexts
            raw_meta_and_asset_ctxs = await self._get_all_asset_contexts_raw()

            # Transform to internal Market models using mapper
            markets = self._mapper.transform_raw_meta_and_asset_ctxs_to_markets(
                raw_meta_and_asset_ctxs,
            )

            logger.info(
                "markets_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                markets_count=len(markets),
                message="Successfully retrieved market metadata for all assets",
            )
        except APIError:
            # Re-raise APIErrors from get_all_asset_contexts_raw or mapper
            raise
        except TransformationError as e_transform:
            logger.exception(
                "markets_transform_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_transform),
                message="Failed to transform exchange data for markets",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_transform
        except Exception as e_unhandled:
            logger.exception(
                "markets_unexpected_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_unhandled),
                message="Unexpected error for markets retrieval",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error occurred.",
                original_exception=e_unhandled,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_unhandled
        else:
            return markets

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Retrieve market metadata for a specific symbol.

        Gets all market metadata and filters for the requested symbol.
        This is necessary because Hyperliquid doesn't have a single-market endpoint.

        Args:
            args: Parameters for market metadata request including symbol

        Returns:
            Market object with metadata for the specified symbol

        Raises:
            APIError: If symbol is not found or API request fails
        """
        symbol = args.symbol
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_market"

        # Service Input Parameter Validation is handled by GetMarketArgs Pydantic model

        try:
            logger.info(
                "fetching_market_metadata",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                message="Fetching market metadata for specific symbol",
            )

            # Get all markets and filter for the requested symbol
            all_markets = await self.get_markets(GetMarketsArgs())

            # Find the specific market (will raise if not found)
            market = self._find_market_or_raise(symbol.value, all_markets)

            logger.info(
                "market_metadata_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                market_name=getattr(cast("object", market), "name", None),
                message="Successfully retrieved market metadata for symbol",
            )
        except APIError:
            # Re-raise APIErrors (including SYMBOL_NOT_FOUND)
            raise
        except Exception as e_unhandled:
            logger.exception(
                "market_unexpected_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_unhandled),
                message="Unexpected error for market metadata retrieval",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error occurred.",
                original_exception=e_unhandled,
            ) from e_unhandled
        else:
            return market

    async def _get_all_asset_contexts_raw(self) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Retrieve all asset contexts for market metadata processing.

        Note: This is a simplified version that delegates to the request/response flow.
        In a production setup, this might be injected as a dependency to avoid
        circular dependencies between services.

        Returns:
            HyperliquidRawMetaAndAssetCtxsResponse with asset contexts and metadata

        Raises:
            APIError: If API request fails or response processing fails
        """
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

        logger.debug(
            "raw_asset_contexts_response",
            exchange=self._exchange_name,
            status_code=status_code,
            has_data=raw_response_content_parsed is not None,
            message="Raw asset contexts response received for market metadata",
        )

        if raw_response_content_parsed is None:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="No content received for asset contexts",
                http_status=status_code,
            )

        # Type guard for response handler - meta and asset contexts returns a list
        if not isinstance(raw_response_content_parsed, list):
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=(
                    f"Expected list response for meta and asset contexts, "
                    f"got {type(raw_response_content_parsed)}"
                ),
                http_status=status_code,
            )

        # Process response through handler
        return self._response_handler.handle_info_meta_and_asset_ctxs_response(
            raw_response_content_parsed,
            status_code,
            headers,
        )

    def _find_market_or_raise(
        self,
        symbol: str,
        all_markets: list[Market],
    ) -> Market:
        """Find market by symbol in the markets list or raise error.

        Args:
            symbol: Symbol to find
            all_markets: List of available markets

        Returns:
            Market if found

        Raises:
            SymbolNotFoundError: If symbol not found in available markets
        """
        for market in all_markets:
            if market.symbol.value == symbol:
                logger.debug(
                    "market_found",
                    exchange=self._exchange_name,
                    symbol=symbol,
                    market_name=getattr(cast("object", market), "name", None),
                    message="Market found for symbol",
                )
                return market

        # Symbol not found - collect available symbols for error context
        available_symbols = [m.symbol for m in all_markets]

        logger.warning(
            "symbol_not_found",
            exchange=self._exchange_name,
            symbol=symbol,
            available_symbols_count=len(available_symbols),
            message="Requested symbol not found in available markets",
        )

        raise SymbolNotFoundError(
            symbol=symbol,
            available_symbols=[s.value for s in available_symbols],  # Convert Symbols to strings
            exchange=self._exchange_name,
        )
