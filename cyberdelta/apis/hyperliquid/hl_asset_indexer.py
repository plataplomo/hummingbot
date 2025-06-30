"""CyberDeltaEngine: Hyperliquid Asset Index Resolver.

--------------------------------------------------

Utility class responsible for fetching Hyperliquid's asset metadata and resolving
string symbols to their integer asset indices. Implements caching for efficiency.

This class extracts the asset index resolution logic from HyperliquidAPI to improve
modularity and reduce the complexity of the main API client.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


class HyperliquidAssetIndexResolver:
    """Utility class for fetching Hyperliquid asset metadata and resolving symbols to asset indices.

    Implements caching for efficiency.

    This resolver handles the complex logic of fetching metaAndAssetCtxs from the /info
    endpoint, parsing the response, and maintaining a cache of symbol-to-index mappings.
    """

    def __init__(
        self,
        requester: Callable[
            ...,
            Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
        ],
        response_handler: HyperliquidResponseHandler,
        request_builder: HyperliquidRequestBuilder,
        exchange_name_for_log: str = "hyperliquid_asset_indexer",
    ) -> None:
        """Initialize the asset index resolver.

        Args:
            requester: HTTP client request function (typically HyperliquidAPI._request)
            response_handler: Response handler instance for validating API responses
            request_builder: Request builder instance for constructing API requests
            exchange_name_for_log: Exchange name for logging context

        """
        self._requester = requester
        self._response_handler = response_handler
        self._request_builder = request_builder
        self._exchange_name_for_log = exchange_name_for_log
        self._asset_to_index_cache: dict[str, int] = {}
        self.logger = get_logger(__name__)

    async def get_asset_index(self, symbol: str) -> int:
        """Fetch or retrieve from cache the asset_index for a given symbol.

        Args:
            symbol: The asset symbol to resolve (e.g., "BTC", "ETH")

        Returns:
            The integer asset index for the symbol

        Raises:
            APIError: If the symbol is invalid, API request fails, or symbol not found

        """
        self._validate_symbol(symbol)

        # Cache check
        cached_index = self._get_cached_index(symbol)
        if cached_index is not None:
            return cached_index

        # Cache miss - fetch and populate
        await self._fetch_and_populate_cache(symbol)

        # Return value or error
        return self._get_index_or_raise(symbol)

    def _validate_symbol(self, symbol: str) -> None:
        """Validate the input symbol."""
        if not symbol:
            self.logger.error(
                "invalid_symbol_for_asset_index",
                exchange=self._exchange_name_for_log,
                symbol=symbol,
                message="[%s] Invalid symbol for asset index resolution: %r",
                message_args=[self._exchange_name_for_log, symbol],
            )
            invalid_symbol_msg = "Invalid symbol for asset index resolution."
            raise APIError(
                invalid_symbol_msg,
                code=APIErrorCode.INVALID_PARAMS.value,
            )

    def _get_cached_index(self, symbol: str) -> int | None:
        """Get asset index from cache if available."""
        if symbol in self._asset_to_index_cache:
            self.logger.debug(
                "asset_index_found_in_cache",
                exchange=self._exchange_name_for_log,
                symbol=symbol,
                asset_index=self._asset_to_index_cache[symbol],
                message="[%s] Asset index for %s found in cache: %s",
                message_args=[
                    self._exchange_name_for_log,
                    symbol,
                    self._asset_to_index_cache[symbol],
                ],
            )
            return self._asset_to_index_cache[symbol]
        return None

    async def _fetch_and_populate_cache(self, symbol: str) -> None:
        """Fetch metadata from API and populate the cache."""
        self.logger.debug(
            "asset_index_not_cached_fetching_meta",
            exchange=self._exchange_name_for_log,
            symbol=symbol,
            message="[%s] Asset index for %s not cached, fetching meta...",
            message_args=[self._exchange_name_for_log, symbol],
        )

        raw_response_content, status_code = await self._make_api_request(symbol)
        validated_response = self._process_response(raw_response_content, status_code, symbol)
        self._populate_cache(validated_response)

    async def _make_api_request(self, symbol: str) -> tuple[ParsedJsonResponse | None, int]:
        """Make the API request to fetch metadata."""
        request_payload_model = self._request_builder.build_info_request_payload()
        request_payload_data_dict = request_payload_model.model_dump(
            by_alias=True,
            exclude_none=True,
        )

        try:
            raw_response_content, status_code, _ = await self._requester(
                method="POST",
                endpoint="/info",
                data=request_payload_data_dict,
            )
        except APIError as e_api:
            self.logger.exception(
                "api_error_fetching_asset_index",
                exchange=self._exchange_name_for_log,
                symbol=symbol,
                error=str(e_api),
                message="[%s] API Error fetching asset index for %s: %s",
                message_args=[self._exchange_name_for_log, symbol, str(e_api)],
            )
            fetch_failed_msg = f"Failed to fetch asset index for symbol '{symbol}': {e_api.message}"
            raise APIError(
                fetch_failed_msg,
                code=e_api.code,
                original_exception=e_api,
                http_status=e_api.http_status,
            ) from e_api
        except Exception as e_req:
            self.logger.exception(
                "unexpected_error_api_request_asset_index",
                exchange=self._exchange_name_for_log,
                symbol=symbol,
                error=str(e_req),
                message="[%s] Unexpected error during API request for asset index %s: %s",
                message_args=[self._exchange_name_for_log, symbol, str(e_req)],
            )
            unexpected_fetch_error_msg = (
                f"Unexpected error fetching asset index for symbol '{symbol}': {e_req}"
            )
            raise APIError(
                unexpected_fetch_error_msg,
                code=APIErrorCode.NETWORK_ISSUE.value,
                original_exception=e_req,
            ) from e_req

        return raw_response_content, status_code

    def _process_response(
        self,
        raw_response_content: ParsedJsonResponse | None,
        status_code: int,
        symbol: str,
    ) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Process and validate the API response."""
        if raw_response_content is None:
            self.logger.error(
                "received_none_response_meta_and_asset_ctxs",
                exchange=self._exchange_name_for_log,
                message="[%s] Received None response from requester for metaAndAssetCtxs.",
                message_args=[self._exchange_name_for_log],
            )
            no_data_received_msg = "No data received for market metadata."
            raise APIError(
                no_data_received_msg,
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        try:
            validated_response: HyperliquidRawMetaAndAssetCtxsResponse = (
                self._response_handler.handle_info_meta_and_asset_ctxs_response(
                    raw_response_content,
                )
            )
        except ValidationError as e_val:
            self.logger.exception(
                "failed_to_validate_meta_and_asset_ctxs",
                exchange=self._exchange_name_for_log,
                validation_error=str(e_val),
                raw_response=raw_response_content,
                message="[%s] Failed to validate metaAndAssetCtxs: %s. Raw: %r",
                message_args=[self._exchange_name_for_log, str(e_val), raw_response_content],
            )
            parse_failed_msg = "Failed to parse market metadata for asset index mapping."
            raise APIError(
                parse_failed_msg,
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
            ) from e_val
        except APIError:
            # Re-raise APIErrors from handler directly
            raise
        except Exception as e_parse:
            self.logger.exception(
                "unexpected_error_parsing_meta_and_asset_ctxs",
                exchange=self._exchange_name_for_log,
                symbol=symbol,
                error=str(e_parse),
                message="[%s] Unexpected error parsing metaAndAssetCtxs for %s: %s",
                message_args=[self._exchange_name_for_log, symbol, str(e_parse)],
            )
            unexpected_parse_error_msg = (
                f"Unexpected error parsing market metadata for {symbol}: {e_parse}"
            )
            raise APIError(
                unexpected_parse_error_msg,
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_parse,
                http_status=status_code,
            ) from e_parse

        return validated_response

    def _populate_cache(self, validated_response: HyperliquidRawMetaAndAssetCtxsResponse) -> None:
        """Populate the asset index cache with the validated response."""
        self._asset_to_index_cache.clear()
        for index, asset_def in enumerate(validated_response.meta.universe):
            self._asset_to_index_cache[asset_def.name] = index

        self.logger.debug(
            "repopulated_asset_index_cache",
            exchange=self._exchange_name_for_log,
            asset_count=len(self._asset_to_index_cache),
            message="[%s] Repopulated asset index cache with %s assets",
            message_args=[self._exchange_name_for_log, len(self._asset_to_index_cache)],
        )

    def _get_index_or_raise(self, symbol: str) -> int:
        """Get the index from cache or raise an error if not found."""
        if symbol in self._asset_to_index_cache:
            return self._asset_to_index_cache[symbol]
        self.logger.error(
            "asset_index_not_found_after_fetch",
            exchange=self._exchange_name_for_log,
            symbol=symbol,
            message="[%s] Asset index for %s not found after fetch.",
            message_args=[self._exchange_name_for_log, symbol],
        )
        symbol_not_found_msg = f"Asset index for symbol '{symbol}' not found."
        raise APIError(
            symbol_not_found_msg,
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
