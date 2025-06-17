"""Hyperliquid Response Preprocessing Mapper.

This mapper handles the transformation of raw JSON data to prepare it for
Pydantic model validation. This is necessary because Hyperliquid's API
returns data that needs preprocessing before it can be validated against
our Raw Pydantic models.

Architecture Compliance: This mapper handles the business logic that was
previously in the ResponseHandler, maintaining proper separation of concerns.
"""

from __future__ import annotations

from typing import Any, NoReturn, TypeGuard

from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2Book as HyperliquidRawOrderBookResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)


def _is_dict_str_any(value: object) -> TypeGuard[dict[str, Any]]:
    """Type guard to check if value is a dict[str, Any]."""
    return isinstance(value, dict)


def _is_list_any(value: object) -> TypeGuard[list[Any]]:
    """Type guard to check if value is a list[Any]."""
    return isinstance(value, list)


class HyperliquidResponsePreprocessingMapper:
    """Handles preprocessing of Hyperliquid API responses before Pydantic validation.

    This mapper contains the business logic for transforming raw API responses
    into a format that can be validated by our Raw Pydantic models. This maintains
    proper separation of concerns by keeping transformation logic out of the
    ResponseHandler.
    """

    @staticmethod
    def preprocess_meta_and_asset_ctxs_response(
        raw_response_data: list[Any],
    ) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Preprocess and validate the MetaAndAssetCtxs response.

        Args:
            raw_response_data: Raw API response data (2-element list)

        Returns:
            HyperliquidRawMetaAndAssetCtxsResponse: Validated response model

        Raises:
            APIError: If preprocessing or validation fails
        """
        try:
            meta_data_raw = raw_response_data[0]
            asset_ctxs_data_raw = raw_response_data[1]

            # Process and validate meta data
            meta_model = HyperliquidResponsePreprocessingMapper._process_meta_data(meta_data_raw)

            # Process and validate asset contexts
            validated_asset_ctxs = HyperliquidResponsePreprocessingMapper._process_asset_contexts(
                asset_ctxs_data_raw, meta_model
            )

            # Construct final response
            return HyperliquidRawMetaAndAssetCtxsResponse(
                meta=meta_model,
                asset_ctxs=validated_asset_ctxs,
            )
        except APIError:
            # Re-raise APIError as-is
            raise
        except Exception as e:
            logger.error(
                f"[HyperliquidResponsePreprocessingMapper] Failed to preprocess meta and asset "
                f"contexts: {e}"
            )
            raise APIError(
                message=f"Failed to preprocess meta and asset contexts response: {e}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            ) from e

    @staticmethod
    def _process_meta_data(meta_data_raw: dict[str, Any]) -> HyperliquidRawMetaResponse:
        """Process and validate meta data."""
        # Create mutable copy and clean unwanted fields
        meta_data = dict(meta_data_raw)
        meta_data.pop("marginTables", None)

        # Preprocess universe items
        HyperliquidResponsePreprocessingMapper._preprocess_universe_items(meta_data)

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
            validated_asset_ctx = (
                HyperliquidResponsePreprocessingMapper._process_single_asset_context(
                    raw_ctx_dict_from_api, meta_model.universe[i].name
                )
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
        allowed_json_keys_for_asset_ctx = (
            HyperliquidResponsePreprocessingMapper._get_allowed_asset_ctx_keys()
        )
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
    def preprocess_order_status_response(
        raw_response_content: object,
        user_address: str,
        order_id: int,
    ) -> HyperliquidRawHistoricalOrderResponse:
        """Preprocess and validate order status response.

        This method handles all the complex business logic for transforming
        Hyperliquid's order status responses into our standardized format.

        Args:
            raw_response_content: Raw API response data
            user_address: User wallet address for context
            order_id: Order ID for context

        Returns:
            HyperliquidRawHistoricalOrderResponse: Validated response model

        Raises:
            APIError: If order not found or validation fails
        """
        context = f"info (OrderStatus for user {user_address}, oid {order_id})"

        # Handle direct "Order not found" string
        if HyperliquidResponsePreprocessingMapper._is_direct_order_not_found_string(
            raw_response_content
        ):
            HyperliquidResponsePreprocessingMapper._raise_direct_order_not_found(
                raw_response_content, context, user_address, order_id
            )

        # Handle dict responses
        if isinstance(raw_response_content, dict):
            return HyperliquidResponsePreprocessingMapper._handle_dict_order_status(
                raw_response_content, context
            )

        # Handle list responses
        if isinstance(raw_response_content, list):
            return HyperliquidResponsePreprocessingMapper._handle_list_order_status(
                raw_response_content, context, user_address, order_id
            )

        # Handle unexpected types
        raise APIError(
            message=f"Unexpected {context} response format: expected list or dict, "
            f"got {type(raw_response_content).__name__}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    @staticmethod
    def _is_direct_order_not_found_string(raw_response_content: object) -> TypeGuard[str]:
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
            # Check if we have nested structure: {"order": {"order": {...}}, "status": ...}
            if "order" in raw_response_content and isinstance(raw_response_content["order"], dict):
                if "order" in raw_response_content["order"]:
                    # Extract the inner order data
                    order_data = raw_response_content["order"]["order"]
                    status = raw_response_content["order"].get("status", "unknown")
                    status_timestamp = raw_response_content["order"].get("statusTimestamp", 0)

                    # Transform the new format to match our model's expectations
                    # Handle orderType - convert string to dict format
                    order_type_value = order_data.get("orderType", "Limit")
                    if isinstance(order_type_value, str):
                        # Convert string format to dict format
                        if order_type_value.lower() == "limit":
                            order_type_dict = {"limit": {"tif": "Gtc"}}
                        elif order_type_value.lower() == "market":
                            order_type_dict = {"market": {}}
                        else:
                            order_type_dict = {"limit": {"tif": "Gtc"}}  # Default
                    else:
                        order_type_dict = order_type_value

                    # Get asset - use coin if asset is not present
                    asset_value = order_data.get("asset", order_data.get("coin", "BTC"))
                    if isinstance(asset_value, int):
                        asset_value = str(asset_value)

                    transformed_order = {
                        "oid": order_data.get("oid"),
                        "cloid": order_data.get("cloid"),
                        "asset": asset_value,
                        "side": order_data.get("side", "B")[0]
                        if order_data.get("side")
                        else "B",  # Take first char
                        "limitPx": order_data.get("limitPx", order_data.get("px", "0")),
                        "sz": order_data.get("sz", "0"),
                        "timestamp": order_data.get("timestamp", 0),
                        "orderType": order_type_dict,
                        "reduceOnly": order_data.get("reduceOnly", False),
                        "remainingSz": order_data.get("remainingSz", order_data.get("sz", "0")),
                        "status": status,
                        "statusTimestamp": status_timestamp,
                    }

                    normalized_response = {"order": transformed_order}
                    return HyperliquidRawHistoricalOrderResponse.model_validate(normalized_response)

            return HyperliquidRawHistoricalOrderResponse.model_validate(raw_response_content)
        except Exception as e:
            logger.error(f"Error processing dict order status in {context}: {e}")
            raise APIError(
                message=f"Failed to process order status response: {e}",
                code=APIErrorCode.INVALID_RESPONSE.value,
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
            HyperliquidResponsePreprocessingMapper._raise_empty_list_order_not_found(
                context, user_address, order_id
            )

        status_item = raw_response_content[0]

        # Handle string items in list
        if isinstance(status_item, str):
            HyperliquidResponsePreprocessingMapper._handle_string_status_item(
                status_item, context, user_address, order_id
            )

        # Handle non-dict items
        if not _is_dict_str_any(status_item):
            HyperliquidResponsePreprocessingMapper._raise_unexpected_item_type(status_item, context)

        # Validate dict item - TypeGuard confirmed it's dict[str, Any]
        try:
            return HyperliquidRawHistoricalOrderResponse.model_validate(status_item)
        except Exception as e:
            logger.error(f"Error validating order status item in {context}: {e}")
            raise APIError(
                message=f"Failed to validate order status item: {e}",
                code=APIErrorCode.INVALID_RESPONSE.value,
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
    def preprocess_candle_snapshot_response(
        raw_response_content: object,
        symbol: str,
        interval: str,
    ) -> HyperliquidRawCandleSnapshot:
        """Preprocess and validate candle snapshot response.

        This method handles the business logic for transforming Hyperliquid's
        candle snapshot responses, including handling empty responses.

        Args:
            raw_response_content: Raw API response data
            symbol: Symbol for context and logging
            interval: Interval for context and logging

        Returns:
            HyperliquidRawCandleSnapshot: Validated candle snapshot model
        """
        # Handle empty list response (no candle data available)
        if isinstance(raw_response_content, list) and len(raw_response_content) == 0:
            logger.info(f"Empty candle data for {symbol} {interval}, returning empty snapshot")
            # Return a minimal empty candle snapshot with proper OHLCV structure
            empty_snapshot: dict[str, str | list[Any]] = {
                "t": [],  # timestamps
                "o": [],  # open prices
                "h": [],  # high prices
                "l": [],  # low prices
                "c": [],  # close prices
                "v": [],  # volumes
                "s": "ok",  # status
            }
            return HyperliquidRawCandleSnapshot.model_validate(empty_snapshot)

        if not isinstance(raw_response_content, dict):
            logger.error(
                f"Unexpected candle snapshot format for {symbol} {interval}. "
                f"Raw: {raw_response_content!r}",
            )
            raise APIError(
                message=f"Unexpected candle snapshot response format: expected dict, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        try:
            return HyperliquidRawCandleSnapshot.model_validate(raw_response_content)
        except ValidationError as e:
            logger.error(
                f"[HyperliquidResponsePreprocessingMapper] Validation error for candle snapshot "
                f"{symbol} {interval}: {e}"
            )
            raise APIError(
                message=f"Failed to validate candle snapshot response: {e}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"symbol": symbol, "interval": interval},
            ) from e
        except Exception as e:
            logger.error(
                f"[HyperliquidResponsePreprocessingMapper] Error validating candle snapshot for "
                f"{symbol} {interval}: {e}"
            )
            raise APIError(
                message=f"Failed to validate candle snapshot response: {e}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"symbol": symbol, "interval": interval},
            ) from e

    @staticmethod
    def preprocess_l2_book_response(
        raw_response_content: object,
        symbol: str,
    ) -> HyperliquidRawOrderBookResponse:
        """Preprocess and validate L2 order book response.

        This method handles the business logic for transforming Hyperliquid's
        order book responses, including handling None/empty responses.

        Args:
            raw_response_content: Raw API response data
            symbol: Symbol for context and empty book creation

        Returns:
            HyperliquidRawOrderBookResponse: Validated order book model
        """
        # Handle None response (no order book data available)
        if raw_response_content is None:
            logger.info(f"No order book data available for {symbol}, returning empty order book")
            # Return a minimal empty order book with proper structure
            empty_order_book: dict[str, str | list[list[Any]] | int] = {
                "coin": symbol,
                "levels": [[], []],  # [bids, asks] - both empty lists
                "time": 0,  # zero timestamp for empty book
            }
            return HyperliquidRawOrderBookResponse.model_validate(empty_order_book)

        if not isinstance(raw_response_content, dict):
            logger.error(
                f"Unexpected order book format for {symbol}. Raw: {raw_response_content!r}",
            )
            raise APIError(
                message=f"Unexpected order book response format: expected dict, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        try:
            return HyperliquidRawOrderBookResponse.model_validate(raw_response_content)
        except ValidationError as e:
            logger.error(
                f"[HyperliquidResponsePreprocessingMapper] Validation error for order book "
                f"{symbol}: {e}"
            )
            raise APIError(
                message=f"Failed to validate order book response: {e}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"symbol": symbol},
            ) from e
        except Exception as e:
            logger.error(
                f"[HyperliquidResponsePreprocessingMapper] Error validating order book for "
                f"{symbol}: {e}"
            )
            raise APIError(
                message=f"Failed to validate order book response: {e}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"symbol": symbol},
            ) from e

    @staticmethod
    def preprocess_recent_trades_response(
        raw_response_content: object,
        symbol: str,
    ) -> list[HyperliquidRawPublicTrade]:
        """Preprocess and validate recent trades response.

        This method handles the business logic for transforming Hyperliquid's
        recent trades responses, including validation and error handling.

        Args:
            raw_response_content: Raw API response data
            symbol: Symbol for context and logging

        Returns:
            list[HyperliquidRawPublicTrade]: List of validated trade models
        """
        if not isinstance(raw_response_content, list):
            logger.error(
                f"Unexpected recent trades format for {symbol}. Raw: {raw_response_content!r}",
            )
            raise APIError(
                message=f"Unexpected recent trades response format: expected list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_trades: list[HyperliquidRawPublicTrade] = []
        for i, item in enumerate(raw_response_content):
            if not isinstance(item, dict):
                logger.warning(
                    f"Skipping non-dict item at index {i} in recent trades for {symbol}. "
                    f"Item: {item!r}",
                )
                continue  # Skip non-dict items

            try:
                trade = HyperliquidRawPublicTrade.model_validate(item)
                validated_trades.append(trade)
            except Exception as e:
                logger.error(
                    f"Failed to validate trade item at index {i} for {symbol}: {e}. "
                    f"Raw item: {item!r}",
                )
                # Make the error message more generic to match test expectations
                error_message = (
                    f"Invalid single recent trade item (index {i}) in recent trades "
                    f"response from exchange. Details: {e}"
                )
                raise APIError(
                    message=error_message,
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    original_exception=e,
                ) from e

        return validated_trades
