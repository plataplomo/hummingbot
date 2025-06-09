"""Response Handler for Hyperliquid API Raw Responses.

Validates raw JSON data against Pydantic models specific to Hyperliquid's API endpoints.
"""

from collections.abc import Mapping
from typing import Any, NoReturn, TypeGuard

from pydantic import ValidationError  # BaseModel, Field no longer used directly here

# Corrected imports for processed models
from cyberdelta.apis.hyperliquid.models.hl_processed_exchange_responses import (
    HyperliquidErrorStatus,
    HyperliquidSuccessfulOrderStatus,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2Book as HyperliquidRawOrderBookResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState as HyperliquidRawUserStateResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_vault_details import (
    HyperliquidRawVaultDetailsResponse,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)

# Type alias for raw JSON response from HTTP client
RawJsonPrim = str | int | float | bool | None
RawJson = dict[str, "RawJson"] | list["RawJson"] | RawJsonPrim
type RawJsonResponse = RawJson


# --- Processed Status Models (REMOVED) ---
# Definitions were moved to cyberdelta/apis/hyperliquid/models/hl_processed_exchange_responses.py


def _is_dict_str_any(value: object) -> TypeGuard[dict[str, Any]]:
    """Type guard to check if value is a dict[str, Any]."""
    return isinstance(value, dict)


def _is_list_any(value: object) -> TypeGuard[list[Any]]:
    """Type guard to check if value is a list[Any]."""
    return isinstance(value, list)


class HyperliquidResponseHandler:
    """Handles validation of raw JSON responses from Hyperliquid REST API endpoints.

    Uses Pydantic models defined in `cyberdelta.apis.hyperliquid.models` to validate
    the structure and types of the raw data. Raises APIError if validation fails.
    """

    @staticmethod
    def _handle_validation_error(
        e: ValidationError,
        context: str,
        raw_data: RawJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> APIError:
        """Helper to create a standardized APIError from a ValidationError."""
        log_message = (
            f"[HyperliquidResponseHandler] Pydantic validation failed for {context}: {e}. "
            f"Status: {status_code if status_code is not None else 'N/A'}. "
            f"Headers: {headers if headers is not None else 'N/A'}. "
            f"Raw data: {raw_data!r}"
        )
        logger.error(log_message)
        return APIError(
            message=f"Invalid {context} response from exchange: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=e,
            http_status=status_code,
        )

    @staticmethod
    def handle_info_meta_and_asset_ctxs_response(
        raw_response_content: RawJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Validates the /info response expected to be MetaAndAssetCtxs."""
        context = "info (MetaAndAssetCtxs)"

        # Validate basic structure
        HyperliquidResponseHandler._validate_meta_asset_ctxs_structure(
            raw_response_content, context, status_code, headers
        )

        try:
            # After validation, we know raw_response_content is a 2-element list
            if not isinstance(raw_response_content, list) or len(raw_response_content) != 2:
                raise APIError(
                    message=f"Unexpected {context} response format: not a 2-element list",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            meta_data_raw = raw_response_content[0]
            asset_ctxs_data_raw = raw_response_content[1]

            # Cast to proper types after validation confirms the structure
            if not isinstance(meta_data_raw, dict):
                raise APIError(
                    message=f"Unexpected {context} response format: first element is not dict",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            if not isinstance(asset_ctxs_data_raw, list):
                raise APIError(
                    message=f"Unexpected {context} response format: second element is not list",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # Process and validate meta data
            meta_model = HyperliquidResponseHandler._process_meta_data(
                meta_data_raw, context, status_code
            )

            # Process and validate asset contexts
            validated_asset_ctxs = HyperliquidResponseHandler._process_asset_contexts(
                asset_ctxs_data_raw, meta_model
            )

            # Construct final response
            return HyperliquidRawMetaAndAssetCtxsResponse(
                meta=meta_model,
                asset_ctxs=validated_asset_ctxs,
            )

        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
                status_code,
                headers,
            ) from e
        except Exception as e_generic:
            logger.error(
                f"[{HyperliquidResponseHandler.__name__}] Unexpected generic error processing "
                f"{context}: {e_generic}. Raw: {raw_response_content!r}",
            )
            raise APIError(
                message=f"Unexpected error processing {context}: {e_generic}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_generic,
                http_status=status_code,
            ) from e_generic

    @staticmethod
    def _validate_meta_asset_ctxs_structure(
        raw_response_content: RawJsonResponse,
        context: str,
        status_code: int | None,
        headers: Mapping[str, str] | None,
    ) -> None:
        """Validate the basic structure of meta and asset contexts response."""
        if not isinstance(raw_response_content, list):
            logger.error(
                f"Unexpected {context} format. "
                f"Status: {status_code}, Headers: {headers}, Raw: {raw_response_content!r}",
            )
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        if len(raw_response_content) != 2:
            raise APIError(
                message=f"Unexpected {context} response format: expected 2-element list, "
                f"got {len(raw_response_content)} elements",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        meta_data_raw, asset_ctxs_data_raw = raw_response_content

        if not isinstance(meta_data_raw, dict):
            raise APIError(
                message=f"Unexpected {context} response format: first element (meta) "
                f"expected dict, got {type(meta_data_raw).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        if not isinstance(asset_ctxs_data_raw, list):
            raise APIError(
                message=f"Unexpected {context} response format: second element (asset_ctxs) "
                f"expected list, got {type(asset_ctxs_data_raw).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

    @staticmethod
    def _process_meta_data(
        meta_data_raw: dict[str, Any],
        context: str,
        status_code: int | None,
    ) -> HyperliquidRawMetaResponse:
        """Process and validate meta data."""
        from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
            HyperliquidRawMetaResponse,
        )

        # Create mutable copy and clean unwanted fields
        meta_data = dict(meta_data_raw)
        meta_data.pop("marginTables", None)

        # Preprocess universe items
        HyperliquidResponseHandler._preprocess_universe_items(meta_data)

        # Validate and return
        return HyperliquidRawMetaResponse.model_validate(meta_data)

    @staticmethod
    def _preprocess_universe_items(meta_data: dict[str, Any]) -> None:
        """Preprocess universe items within meta data."""
        if "universe" in meta_data and _is_list_any(meta_data["universe"]):
            for item in meta_data["universe"]:
                if _is_dict_str_any(item):
                    # Remove fields not in our model directly on the dict
                    item.pop("marginTableId", None)
                    item.pop("isDelisted", None)
                    # Add 'onlyIsolated' if missing
                    if "onlyIsolated" not in item:
                        item["onlyIsolated"] = False

    @staticmethod
    def _process_asset_contexts(
        asset_ctxs_data_raw: list[Any],
        meta_model: HyperliquidRawMetaResponse,
    ) -> list[HyperliquidRawAssetCtx]:
        """Process and validate asset contexts with enrichment from meta universe."""
        validated_asset_ctxs: list[HyperliquidRawAssetCtx] = []

        for i, raw_ctx_dict_from_api in enumerate(asset_ctxs_data_raw):
            if not _is_dict_str_any(raw_ctx_dict_from_api):
                logger.warning(
                    f"Skipping non-dict item in asset_ctxs_data at index {i}: "
                    f"{raw_ctx_dict_from_api!r}",
                )
                continue

            # Check if we have a corresponding universe entry
            if i >= len(meta_model.universe):
                logger.warning(
                    f"Asset context at index {i} has no corresponding universe entry. "
                    f"Universe has {len(meta_model.universe)} entries. Skipping.",
                )
                continue

            # Process single asset context - TypeGuard confirmed it's dict[str, Any]
            validated_asset_ctx = HyperliquidResponseHandler._process_single_asset_context(
                raw_ctx_dict_from_api, meta_model.universe[i].name
            )
            validated_asset_ctxs.append(validated_asset_ctx)

        return validated_asset_ctxs

    @staticmethod
    def _process_single_asset_context(
        raw_ctx_dict_from_api: dict[str, Any],
        universe_name: str,
    ) -> HyperliquidRawAssetCtx:
        """Process a single asset context with name enrichment and field filtering."""
        # Create temporary dictionary and add the name from the universe
        temp_ctx_dict = dict(raw_ctx_dict_from_api)
        temp_ctx_dict["name"] = universe_name

        # Filter to only allowed fields for HyperliquidRawAssetCtx
        allowed_json_keys_for_asset_ctx = HyperliquidResponseHandler._get_allowed_asset_ctx_keys()
        filtered_ctx_dict = {
            k: v for k, v in temp_ctx_dict.items() if k in allowed_json_keys_for_asset_ctx
        }

        # Validate and return
        return HyperliquidRawAssetCtx.model_validate(filtered_ctx_dict)

    @staticmethod
    def _get_allowed_asset_ctx_keys() -> set[str]:
        """Get allowed JSON keys for HyperliquidRawAssetCtx model."""
        allowed_json_keys_for_asset_ctx: set[str] = set()
        for field_name, field_info in HyperliquidRawAssetCtx.model_fields.items():
            if field_info.alias:
                allowed_json_keys_for_asset_ctx.add(field_info.alias)
            else:
                allowed_json_keys_for_asset_ctx.add(field_name)
        return allowed_json_keys_for_asset_ctx

    @staticmethod
    def handle_info_user_state_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
    ) -> HyperliquidRawUserStateResponse:
        """Validates the /info response for user_state."""
        context = f"info (user state for {user_address})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawUserStateResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
            ) from e

    @staticmethod
    def handle_info_open_orders_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
    ) -> HyperliquidRawOpenOrdersResponse:
        """Validates the /info response for open_orders."""
        context = f"info (open orders for {user_address})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawOpenOrdersResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
            ) from e

    @staticmethod
    def handle_info_user_fills_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
    ) -> HyperliquidRawUserFillsResponse:
        """Validates the /info response for user_fills."""
        context = f"info (user fills for {user_address})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawUserFillsResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
            ) from e

    @staticmethod
    def handle_info_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
    ) -> HyperliquidRawAssetCtx:
        """Validates the /info response for funding rate (per symbol)."""
        context = f"info (funding rate for {symbol})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawAssetCtx.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
            ) from e
        except AttributeError:
            logger.error(
                f"Model HyperliquidRawAssetCtx appears incomplete or unavailable for {context}. "
                "Validation skipped.",
            )
            raise APIError(
                message=f"Asset context model (for funding rate) not fully available for {context}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            ) from None

    @staticmethod
    def handle_info_l2_book_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawOrderBookResponse:
        """Validates the /info response for l2Book."""
        context = f"info (l2 book for {symbol})"
        
        # Handle None response (no order book data available)
        if raw_response_content is None:
            logger.info(f"No order book data available for {symbol}, returning empty order book")
            # Return a minimal empty order book with proper structure
            empty_order_book = {
                "coin": symbol,
                "levels": [[], []],  # [bids, asks] - both empty lists
                "time": 0  # zero timestamp for empty book
            }
            return HyperliquidRawOrderBookResponse.model_validate(empty_order_book)
        
        if not isinstance(raw_response_content, dict):
            logger.error(
                f"Unexpected {context} format for {symbol}. "
                f"Status: {status_code}, Headers: {headers}, Raw: {raw_response_content!r}",
            )
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )
        try:
            return HyperliquidRawOrderBookResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
                status_code,
                headers,
            ) from e

    @staticmethod
    def handle_info_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawPublicTrade]:
        """Validates the /info response for recentTrades."""
        context = f"info (recent trades for {symbol})"
        if not isinstance(raw_response_content, list):
            logger.error(
                f"Unexpected {context} format for {symbol}. "
                f"Status: {status_code}, Headers: {headers}, Raw: {raw_response_content!r}",
            )
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        validated_trades: list[HyperliquidRawPublicTrade] = []
        for i, item in enumerate(raw_response_content):
            if not isinstance(item, dict):
                logger.warning(
                    f"Skipping non-dict item at index {i} in {context} for {symbol}. "
                    f"Item: {item!r}",
                )
                continue  # Skip non-dict items

            try:
                trade = HyperliquidRawPublicTrade.model_validate(item)
                validated_trades.append(trade)
            except ValidationError as e:
                logger.error(
                    f"[HyperliquidResponseHandler] Pydantic validation failed "
                    f"for trade item at index {i} "
                    f"in {context}: {e}. "
                    f"Status: {status_code}. Headers: {headers}. Raw data: {item!r}",
                )
                # Make the error message more generic to match test expectations
                error_message = (
                    f"Invalid single recent trade item (index {i}) in {context} "
                    f"response from exchange. Details: {e.errors()}"
                )
                raise APIError(
                    message=error_message,
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                    original_exception=e,
                ) from e
            except Exception as e:  # Catch any other unexpected error during instantiation
                logger.error(
                    f"[HyperliquidResponseHandler] Unexpected error validating "
                    f"trade item at index {i} "
                    f"in {context}: {e}. "
                    f"Status: {status_code}. Headers: {headers}. Raw data: {item!r}",
                )
                raise APIError(
                    message=(
                        f"Unexpected error processing trade item at index {i} in {context} "
                        f"response from exchange: {e}"
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,  # Changed from UNEXPECTED_ERROR
                    http_status=status_code,
                ) from e
        return validated_trades

    @staticmethod
    def handle_info_candle_snapshot_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        interval: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawCandleSnapshot:
        """Validates the /info response for candle_snapshot."""
        context = f"info (candle snapshot for {symbol})"
        
        # Handle empty list response (no candle data available)
        if isinstance(raw_response_content, list) and len(raw_response_content) == 0:
            logger.info(f"Empty candle data for {symbol} {interval}, returning empty snapshot")
            # Return a minimal empty candle snapshot with proper OHLCV structure
            empty_snapshot = {
                "t": [],  # timestamps
                "o": [],  # open prices
                "h": [],  # high prices  
                "l": [],  # low prices
                "c": [],  # close prices
                "v": [],  # volumes
                "s": "ok"  # status
            }
            return HyperliquidRawCandleSnapshot.model_validate(empty_snapshot)
        
        if not isinstance(raw_response_content, dict):
            logger.error(
                f"Unexpected {context} format for {symbol} {interval}. "
                f"Status: {status_code}, Headers: {headers}, Raw: {raw_response_content!r}",
            )
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )
        try:
            return HyperliquidRawCandleSnapshot.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
                status_code,
                headers,
            ) from e

    @staticmethod
    def handle_info_order_status_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
        order_id: int,
    ) -> HyperliquidRawHistoricalOrderResponse:
        """Validates the /info response for order_status."""
        context = f"info (OrderStatus for user {user_address}, oid {order_id})"

        # Handle direct "Order not found" string
        if HyperliquidResponseHandler._is_direct_order_not_found_string(raw_response_content):
            # TypeGuard confirmed it's a string
            HyperliquidResponseHandler._raise_direct_order_not_found(
                raw_response_content, context, user_address, order_id
            )

        # Handle dict responses
        if isinstance(raw_response_content, dict):
            return HyperliquidResponseHandler._handle_dict_order_status(
                raw_response_content, context
            )

        # Handle list responses
        if isinstance(raw_response_content, list):
            return HyperliquidResponseHandler._handle_list_order_status(
                raw_response_content, context, user_address, order_id
            )

        # Handle unexpected types
        raise APIError(
            message=f"Unexpected {context} response format: expected list or dict, "
            f"got {type(raw_response_content).__name__}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    @staticmethod
    def _is_direct_order_not_found_string(raw_response_content: RawJsonResponse) -> TypeGuard[str]:
        """Check if response is a direct 'Order not found' string."""
        return isinstance(raw_response_content, str) and "Order not found" in raw_response_content

    @staticmethod
    def _raise_direct_order_not_found(
        raw_response_content: str,
        context: str,
        user_address: str,
        order_id: int,
    ) -> NoReturn:
        """Raise APIError for direct order not found string."""
        logger.debug(f"{context}: Received direct string '{raw_response_content}'.")
        raise APIError(
            message=(
                f"Order {order_id} for user {user_address} not found "
                f"(direct string: '{raw_response_content}')"
            ),
            code=APIErrorCode.ORDER_NOT_FOUND.value,
            metadata={"original_response": raw_response_content},
        )

    @staticmethod
    def _handle_dict_order_status(
        raw_response_content: dict[str, Any],
        context: str,
    ) -> HyperliquidRawHistoricalOrderResponse:
        """Handle dict response format for order status."""
        try:
            return HyperliquidRawHistoricalOrderResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                f"order status object in {context}",
                raw_response_content,
            ) from e

    @staticmethod
    def _handle_list_order_status(
        raw_response_content: list[Any],
        context: str,
        user_address: str,
        order_id: int,
    ) -> HyperliquidRawHistoricalOrderResponse:
        """Handle list response format for order status."""
        if not raw_response_content:
            HyperliquidResponseHandler._raise_empty_list_order_not_found(
                context, user_address, order_id
            )

        status_item = raw_response_content[0]

        # Handle string items in list
        if isinstance(status_item, str):
            HyperliquidResponseHandler._handle_string_status_item(
                status_item, context, user_address, order_id
            )

        # Handle non-dict items
        if not _is_dict_str_any(status_item):
            HyperliquidResponseHandler._raise_unexpected_item_type(status_item, context)

        # Validate dict item - TypeGuard confirmed it's dict[str, Any]
        try:
            return HyperliquidRawHistoricalOrderResponse.model_validate(status_item)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                f"order status object in {context}",
                status_item,
            ) from e

    @staticmethod
    def _raise_empty_list_order_not_found(
        context: str, user_address: str, order_id: int
    ) -> NoReturn:
        """Raise APIError for empty list (order not found)."""
        logger.debug(f"{context}: Received empty list, interpreting as order not found.")
        raise APIError(
            message=f"Order {order_id} for {user_address} not found (empty list).",
            code=APIErrorCode.ORDER_NOT_FOUND.value,
            metadata={"original_response": []},
        )

    @staticmethod
    def _handle_string_status_item(
        status_item: str, context: str, user_address: str, order_id: int
    ) -> NoReturn:
        """Handle string items in order status list."""
        if "order not found" in status_item.lower():
            raise APIError(
                message=(
                    f"Order {order_id} for user {user_address} not found "
                    f"(string response: '{status_item}')."
                ),
                code=APIErrorCode.ORDER_NOT_FOUND.value,
                metadata={"original_response_item": status_item},
            )
        else:
            logger.warning(f"{context}: Unexpected string content in list: '{status_item}'.")
            raise APIError(
                message=f"Unexpected string content in {context} response: {status_item}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"original_response_item": status_item},
            )

    @staticmethod
    def _raise_unexpected_item_type(status_item: object, context: str) -> NoReturn:
        """Raise APIError for unexpected item type in order status list."""
        logger.warning(f"{context}: Unexpected item type in list: {type(status_item)}.")
        raise APIError(
            message=(
                f"Unexpected item type in {context} response list: "
                f"expected dict, got {type(status_item).__name__}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
            metadata={"original_response_item": status_item},
        )

    @staticmethod
    def handle_info_spot_asset_contexts_response(
        raw_response_content: RawJsonResponse,
    ) -> list[HyperliquidRawAssetCtx]:
        """Validates the /info response for spot asset contexts."""
        context = "info (SpotAssetCtxs)"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        validated_ctxs: list[HyperliquidRawAssetCtx] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_ctxs.append(HyperliquidRawAssetCtx.model_validate(item))
            except ValidationError as e:
                raise HyperliquidResponseHandler._handle_validation_error(
                    e,
                    f"single spot asset context item in {context}",
                    item,
                ) from e
            except AttributeError:
                logger.error(
                    f"Model HyperliquidRawAssetCtx appears incomplete or unavailable for "
                    f"{context} items. Validation skipped.",
                )
                raise APIError(
                    message=f"Spot asset context model item not fully available for {context}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                ) from None
        return validated_ctxs

    @staticmethod
    def handle_info_vault_details_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
    ) -> HyperliquidRawVaultDetailsResponse:
        """Validates the /info response for vault details."""
        context = f"info (VaultDetails for {user_address})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawVaultDetailsResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
            ) from e
        except AttributeError:
            logger.error(
                f"Placeholder or missing model for HyperliquidRawVaultDetailsResponse used for "
                f"{context}. Validation skipped.",
            )
            raise APIError(
                message=f"Vault details model not fully available for {context}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            ) from None

    @staticmethod
    def handle_exchange_response(
        raw_response_content: RawJsonResponse,
        action_type: str,
    ) -> HyperliquidRawExchangeResponse:
        """Validates the /exchange response (for actions like order, cancel, withdraw)."""
        context = f"exchange ({action_type})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawExchangeResponse.model_validate(raw_response_content)
        except ValidationError as e:
            logger.warning(f"Initial validation of {context} failed. Raw: {raw_response_content!r}")
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
            ) from e

    @staticmethod
    def process_first_exchange_status(
        first_status_raw: RawJsonPrim | RawJson,
        action_description: str,
    ) -> HyperliquidSuccessfulOrderStatus | HyperliquidErrorStatus:
        """Processes the first status item from an /exchange endpoint response.

        Args:
            first_status_raw: The raw status item (dict or string).
            action_description: Description of the action (e.g., "place_order", "cancel_order").

        Returns:
            A HyperliquidSuccessfulOrderStatus or HyperliquidErrorStatus model.

        Raises:
            APIError: If the status structure is unknown or invalid.

        """
        if isinstance(first_status_raw, dict):
            return HyperliquidResponseHandler._process_dict_exchange_status(
                first_status_raw, action_description
            )

        if isinstance(first_status_raw, str):
            return HyperliquidResponseHandler._process_string_exchange_status(
                first_status_raw, action_description
            )

        # Invalid type
        raise APIError(
            message=(
                f"Invalid status type for {action_description}: {type(first_status_raw)}. "
                f"Expected dict or str."
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    @staticmethod
    def _process_dict_exchange_status(
        first_status_raw: dict[str, Any],
        action_description: str,
    ) -> HyperliquidSuccessfulOrderStatus | HyperliquidErrorStatus:
        """Process dict-type exchange status."""
        # Handle resting status
        if "resting" in first_status_raw and isinstance(first_status_raw["resting"], dict):
            return HyperliquidResponseHandler._process_resting_status(
                first_status_raw, action_description
            )

        # Handle filled status
        if "filled" in first_status_raw and isinstance(first_status_raw["filled"], dict):
            return HyperliquidResponseHandler._process_filled_status(
                first_status_raw, action_description
            )

        # Handle canceled status
        if "canceled" in first_status_raw and isinstance(first_status_raw["canceled"], dict):
            return HyperliquidResponseHandler._process_canceled_status(
                first_status_raw, action_description
            )

        # Handle error status
        if "error" in first_status_raw and isinstance(first_status_raw["error"], str):
            return HyperliquidErrorStatus(message=first_status_raw["error"])

        # Unknown dict structure
        raise APIError(
            message=f"Unknown status structure for {action_description}: {first_status_raw!r}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    @staticmethod
    def _process_resting_status(
        first_status_raw: dict[str, Any],
        action_description: str,
    ) -> HyperliquidSuccessfulOrderStatus:
        """Process resting status from exchange response."""
        oid_raw = first_status_raw["resting"].get("oid")
        if not isinstance(oid_raw, int):
            raise APIError(
                message=(
                    f"Invalid or missing 'oid' (expected int) in resting status "
                    f"for {action_description}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_status": first_status_raw},
            )
        return HyperliquidSuccessfulOrderStatus(status_type="resting", oid=oid_raw)

    @staticmethod
    def _process_filled_status(
        first_status_raw: dict[str, Any],
        action_description: str,
    ) -> HyperliquidSuccessfulOrderStatus:
        """Process filled status from exchange response."""
        filled_details = first_status_raw["filled"]
        oid_raw = filled_details.get("oid")
        total_sz_raw = filled_details.get("totalSz")
        avg_px_raw = filled_details.get("avgPx")

        # Validate oid
        if not isinstance(oid_raw, int):
            raise APIError(
                message=(
                    f"Invalid or missing 'oid' (expected int) in filled status "
                    f"for {action_description}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_status": first_status_raw},
            )

        # Validate totalSz
        if not isinstance(total_sz_raw, str):
            raise APIError(
                message=f"Invalid or missing 'totalSz' (expected str) in filled status for "
                f"{action_description}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_status": first_status_raw},
            )

        # Validate avgPx
        if not isinstance(avg_px_raw, str):
            raise APIError(
                message=(
                    f"Invalid or missing 'avgPx' (expected str) in filled "
                    f"status for {action_description}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_status": first_status_raw},
            )

        return HyperliquidSuccessfulOrderStatus(
            status_type="filled",
            oid=oid_raw,
            total_sz=total_sz_raw,
            avg_px=avg_px_raw,
        )

    @staticmethod
    def _process_canceled_status(
        first_status_raw: dict[str, Any],
        action_description: str,
    ) -> HyperliquidSuccessfulOrderStatus:
        """Process canceled status from exchange response."""
        oid_raw = first_status_raw["canceled"].get("oid")
        if not isinstance(oid_raw, int):
            raise APIError(
                message=(
                    f"Invalid or missing 'oid' (expected int) in canceled status object "
                    f"for {action_description}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_status": first_status_raw},
            )
        return HyperliquidSuccessfulOrderStatus(status_type="canceled", oid=oid_raw)

    @staticmethod
    def _process_string_exchange_status(
        first_status_raw: str,
        action_description: str,
    ) -> HyperliquidSuccessfulOrderStatus | HyperliquidErrorStatus:
        """Process string-type exchange status."""
        if first_status_raw.lower() == "canceled":
            return HyperliquidSuccessfulOrderStatus(status_type="canceled_str")

        # Any other string is treated as an error message
        logger.warning(
            f"Encountered direct string status for {action_description}: '{first_status_raw}'. "
            f"Treating as error.",
        )
        return HyperliquidErrorStatus(message=first_status_raw)

    @staticmethod
    def handle_query_order_history_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
    ) -> list[HyperliquidRawHistoricalOrderResponse]:
        """Validates the /query_order_history response."""
        context = f"query_order_history (for {user_address})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        validated_orders: list[HyperliquidRawHistoricalOrderResponse] = []
        for item_index, item in enumerate(raw_response_content):
            if not isinstance(item, dict):
                logger.warning(
                    f"Skipping non-dict item in {context} list at index {item_index}: {item!r}",
                )
                continue
            try:
                # Validate each item against the new historical order model
                validated_orders.append(HyperliquidRawHistoricalOrderResponse.model_validate(item))
            except ValidationError as e:
                raise HyperliquidResponseHandler._handle_validation_error(
                    e,
                    (f"single order history item (index {item_index}) in {context}"),
                    item,
                ) from e
        return validated_orders

    @staticmethod
    def handle_historical_funding_rates_response(
        raw_response_content: RawJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawFundingHistoryItem]:
        """Validates the /info response for historical funding rates."""
        context = "historical_funding_rates"
        if not isinstance(raw_response_content, list):
            # Construct the more specific error message expected by the test
            error_message = (
                f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content).__name__}"
            )
            logger.error(f"{error_message}. Raw: {raw_response_content!r}")
            raise APIError(message=error_message, code=APIErrorCode.INVALID_RESPONSE.value)

        validated_rates: list[HyperliquidRawFundingHistoryItem] = []
        for i, item_raw in enumerate(raw_response_content):
            if not isinstance(item_raw, dict):
                # Revert to a more specific message about type mismatch for this test
                error_message = (
                    f"Expected dict for historical funding rate item, "
                    f"got {type(item_raw).__name__} at index {i}"
                )
                logger.error(
                    f"[HyperliquidResponseHandler] {error_message}. "
                    f"Raw item: {item_raw!r}. Full raw response: {raw_response_content!r}",
                )
                raise APIError(
                    message=error_message,
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )
            try:
                # Assuming HyperliquidRawFundingHistoryItem.model_validate exists and is correct
                validated_item = HyperliquidRawFundingHistoryItem.model_validate(item_raw)
                validated_rates.append(validated_item)
            except ValidationError as e:
                error_message = (
                    f"Invalid single funding history item (index {i}) in {context} "
                    f"response from exchange. Details: {e.errors()}"
                )
                logger.error(
                    f"{error_message} Full raw response: {raw_response_content!r}. "
                    f"Raw item: {item_raw!r}",
                )
                raise APIError(
                    message=error_message,
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    original_exception=e,  # Pass the original ValidationError
                ) from e
            except Exception as e:  # Catch any other unexpected error
                error_message = f"Unexpected error processing item at index {i} in {context}: {e}"
                logger.error(f"[HyperliquidResponseHandler] {error_message}")
                raise APIError(
                    message=error_message,
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    original_exception=e,
                ) from e
        return validated_rates
