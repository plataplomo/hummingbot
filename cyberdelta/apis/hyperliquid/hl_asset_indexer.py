"""CyberDeltaEngine: Hyperliquid Asset Index Resolver
--------------------------------------------------

Utility class responsible for fetching Hyperliquid's asset metadata and resolving
string symbols to their integer asset indices. Implements caching for efficiency.

This class extracts the asset index resolution logic from HyperliquidAPI to improve
modularity and reduce the complexity of the main API client.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import cast

from pydantic import ValidationError

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger


class HyperliquidAssetIndexResolver:
    """Utility class responsible for fetching Hyperliquid's asset metadata and resolving
    string symbols to their integer asset indices. Implements caching for efficiency.

    This resolver handles the complex logic of fetching metaAndAssetCtxs from the /info
    endpoint, parsing the response, and maintaining a cache of symbol-to-index mappings.
    """

    def __init__(
        self,
        requester: Callable[
            ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
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
        # Input validation
        if not symbol:
            self.logger.error(
                f"[{self._exchange_name_for_log}] Invalid symbol for asset index resolution: "
                f"{symbol!r}",
            )
            raise APIError(
                "Invalid symbol for asset index resolution.",
                code=APIErrorCode.INVALID_PARAMS.value,
            )

        # Cache check
        if symbol in self._asset_to_index_cache:
            self.logger.debug(
                f"[{self._exchange_name_for_log}] Asset index for {symbol} found in cache: "
                f"{self._asset_to_index_cache[symbol]}",
            )
            return self._asset_to_index_cache[symbol]

        # Cache miss - fetch data
        self.logger.debug(
            f"[{self._exchange_name_for_log}] Asset index for {symbol} not cached, fetching meta...",
        )

        # Build request
        request_payload_model = self._request_builder.build_info_request_payload()
        request_payload_data_dict = request_payload_model.model_dump(
            by_alias=True, exclude_none=True,
        )

        # Make API call
        try:
            raw_response_content, status_code, _ = await self._requester(
                method="POST",
                endpoint="/info",
                data=request_payload_data_dict,
            )
        except APIError as e_api:
            self.logger.error(
                f"[{self._exchange_name_for_log}] API Error fetching asset index for "
                f"{symbol}: {e_api}",
            )
            raise APIError(
                f"Failed to fetch asset index for symbol '{symbol}': {e_api.message}",
                code=e_api.code,
                original_exception=e_api,
                http_status=e_api.http_status,
            ) from e_api
        except Exception as e_req:
            self.logger.error(
                f"[{self._exchange_name_for_log}] Unexpected error during API request for "
                f"asset index {symbol}: {e_req}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching asset index for symbol '{symbol}': {e_req}",
                code=APIErrorCode.NETWORK_ISSUE.value,
                original_exception=e_req,
            ) from e_req

        # Handle empty response
        if raw_response_content is None:
            self.logger.error(
                f"[{self._exchange_name_for_log}] Received None response from requester "
                f"for metaAndAssetCtxs.",
            )
            raise APIError(
                "No data received for market metadata.",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        # Process response
        try:
            validated_response: HyperliquidRawMetaAndAssetCtxsResponse = (
                self._response_handler.handle_info_meta_and_asset_ctxs_response(
                    cast(RawJsonResponse, raw_response_content),
                )
            )
        except ValidationError as e_val:
            self.logger.error(
                f"[{self._exchange_name_for_log}] Failed to validate metaAndAssetCtxs: {e_val}. "
                f"Raw: {raw_response_content!r}",
            )
            raise APIError(
                "Failed to parse market metadata for asset index mapping.",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
            ) from e_val
        except APIError:
            # Re-raise APIErrors from handler directly
            raise
        except Exception as e_parse:
            self.logger.error(
                f"[{self._exchange_name_for_log}] Unexpected error parsing metaAndAssetCtxs "
                f"for {symbol}: {e_parse}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error parsing market metadata for {symbol}: {e_parse}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_parse,
                http_status=status_code,
            ) from e_parse

        # Populate cache
        self._asset_to_index_cache.clear()
        for index, asset_def in enumerate(validated_response.meta.universe):
            self._asset_to_index_cache[asset_def.name] = index

        self.logger.debug(
            f"[{self._exchange_name_for_log}] Repopulated asset index cache with "
            f"{len(self._asset_to_index_cache)} assets",
        )

        # Return value or error
        if symbol in self._asset_to_index_cache:
            return self._asset_to_index_cache[symbol]
        else:
            self.logger.error(
                f"[{self._exchange_name_for_log}] Asset index for {symbol} not found after fetch.",
            )
            raise APIError(
                f"Asset index for symbol '{symbol}' not found.",
                code=APIErrorCode.SYMBOL_NOT_FOUND.value,
                http_status=status_code,
            )
