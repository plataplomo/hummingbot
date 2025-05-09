"""
Response Handler for Hyperliquid API Raw Responses.

Validates raw JSON data against Pydantic models specific to Hyperliquid\'s API endpoints.
"""

from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_candle_snapshot import (
    HyperliquidRawCandleSnapshotResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
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
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)

# Type alias for raw JSON response from HTTP client
RawJsonPrim = str | int | float | bool | None
RawJson = dict[str, "RawJson"] | list["RawJson"] | RawJsonPrim
type RawJsonResponse = RawJson


class HyperliquidResponseHandler:
    """
    Handles validation of raw JSON responses from Hyperliquid REST API endpoints.

    Uses Pydantic models defined in `cyberdelta.apis.hyperliquid.models` to validate
    the structure and types of the raw data. Raises APIError if validation fails.
    """

    @staticmethod
    def _handle_validation_error(
        e: ValidationError, context: str, raw_data: RawJsonResponse
    ) -> APIError:
        """Helper to create a standardized APIError from a ValidationError."""
        logger.error(
            f"[HyperliquidResponseHandler] Pydantic validation failed for {context}: {e}. "
            f"Raw data: {raw_data!r}"
        )
        return APIError(
            message=f"Invalid {context} response from exchange: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=e,
        )

    @staticmethod
    def handle_info_meta_and_asset_ctxs_response(
        raw_response_content: RawJsonResponse,
    ) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Validates the /info response expected to be MetaAndAssetCtxs."""
        context = "info (MetaAndAssetCtxs)"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawMetaAndAssetCtxsResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_info_user_state_response(
        raw_response_content: RawJsonResponse, user_address: str
    ) -> HyperliquidRawUserStateResponse:
        """Validates the /info response for user_state."""
        context = f"info (UserState for {user_address})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawUserStateResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_info_open_orders_response(
        raw_response_content: RawJsonResponse, user_address: str
    ) -> HyperliquidRawOpenOrdersResponse:
        """Validates the /info response for open_orders."""
        context = f"info (OpenOrders for {user_address})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawOpenOrdersResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_info_user_fills_response(
        raw_response_content: RawJsonResponse, user_address: str
    ) -> HyperliquidRawUserFillsResponse:
        """Validates the /info response for user_fills."""
        context = f"info (UserFills for {user_address})"
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
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_info_funding_rate_response(
        raw_response_content: RawJsonResponse, symbol: str
    ) -> HyperliquidRawAssetCtx:
        """Validates the /info response for funding rate (per symbol)."""
        context = f"info (FundingRate for {symbol})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawAssetCtx.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e
        except AttributeError:
            logger.error(
                f"Model HyperliquidRawAssetCtx appears incomplete or unavailable for {context}. "
                "Validation skipped."
            )
            raise APIError(
                message=f"Asset context model (for funding rate) not fully available for {context}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            ) from None

    @staticmethod
    def handle_info_l2_book_response(
        raw_response_content: RawJsonResponse, symbol: str
    ) -> HyperliquidRawOrderBookResponse:
        """Validates the /info response for l2Book."""
        context = f"info (L2Book for {symbol})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawOrderBookResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_info_recent_trades_response(
        raw_response_content: RawJsonResponse, symbol: str
    ) -> list[HyperliquidRawPublicTrade]:
        """Validates the /info response for recent_trades."""
        context = f"info (RecentTrades for {symbol})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        validated_trades: list[HyperliquidRawPublicTrade] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_trades.append(HyperliquidRawPublicTrade.model_validate(item))
            except ValidationError as e:
                raise HyperliquidResponseHandler._handle_validation_error(
                    e, f"single recent trade item in {context}", item
                ) from e
        return validated_trades

    @staticmethod
    def handle_info_candle_snapshot_response(
        raw_response_content: RawJsonResponse, symbol: str, interval: str
    ) -> HyperliquidRawCandleSnapshotResponse:
        """Validates the /info response for candle_snapshot."""
        context = f"info (CandleSnapshot for {symbol} {interval})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawCandleSnapshotResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_info_order_status_response(
        raw_response_content: RawJsonResponse, user_address: str, order_id: int
    ) -> HyperliquidRawHistoricalOrderResponse:
        """Validates the /info response for order_status."""
        context = f"info (OrderStatus for user {user_address}, oid {order_id})"

        # Handle direct "Order not found" string before list check
        if isinstance(raw_response_content, str) and "Order not found" in raw_response_content:
            logger.debug(f"{context}: Received direct string '{raw_response_content}'.")
            raise APIError(
                message=f"Order {order_id} for user {user_address} not found (direct string:"
                f" '{raw_response_content}')",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
                metadata={"original_response": raw_response_content},
            )

        # Based on observed API behavior and previous logic, response can be a list.
        if not isinstance(raw_response_content, list):
            # If it's already a dict, it might be a direct valid response
            # (or an error dict not yet handled)
            if isinstance(raw_response_content, dict):
                try:
                    # Attempt to validate directly if it's a dict that matches the model
                    return HyperliquidRawHistoricalOrderResponse.model_validate(
                        raw_response_content
                    )
                except ValidationError:
                    # If direct dict validation fails, it could be an unhandled error structure
                    # or just not the expected order status structure.
                    # The original logic mostly expected a list, so we proceed to that check
                    # if direct dict validation fails, or raise a more generic error.
                    # For now, let's assume if it's a dict and fails, it's an invalid
                    # format unless handled by specific error checks.
                    pass  # Fall through to list processing or general error if not a list
            else:
                raise APIError(
                    message=f"Unexpected {context} response format: expected list or dict, "
                    f"got {type(raw_response_content).__name__}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

        # If it IS a list (common case from API)
        if isinstance(raw_response_content, list):
            if not raw_response_content:  # Empty list implies order not found
                logger.debug(f"{context}: Received empty list, interpreting as order not found.")
                raise APIError(
                    message=f"Order {order_id} for {user_address} not found (empty list).",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                    metadata={"original_response": []},
                )

            status_item = raw_response_content[0]  # Get the first (and usually only) item

            if isinstance(status_item, str):  # Handle string messages like "Order not found"
                if "order not found" in status_item.lower():  # Case-insensitive check
                    raise APIError(
                        message=(
                            f"Order {order_id} for user {user_address} not found "
                            f"(string response: '{status_item}')."
                        ),
                        code=APIErrorCode.ORDER_NOT_FOUND.value,
                        metadata={"original_response_item": status_item},
                    )
                else:
                    logger.warning(
                        f"{context}: Unexpected string content in list: '{status_item}'."
                    )
                    raise APIError(
                        message=f"Unexpected string content in {context} response: {status_item}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                        metadata={"original_response_item": status_item},
                    )

            if not isinstance(status_item, dict):
                logger.warning(f"{context}: Unexpected item type in list: {type(status_item)}.")
                raise APIError(
                    message=f"Unexpected item type in {context} response list: expected dict, "
                    f"got {type(status_item).__name__}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    metadata={"original_response_item": status_item},
                )
            # At this point, status_item is a dictionary from the list
            try:
                return HyperliquidRawHistoricalOrderResponse.model_validate(status_item)
            except ValidationError as e:
                raise HyperliquidResponseHandler._handle_validation_error(
                    e, f"order status object in {context}", status_item
                ) from e

        # Fallback if raw_response_content was a dict but didn't validate directly and wasn't a list
        # This case should ideally be caught by initial isinstance(dict) and direct
        # validation/failure but as a safeguard:
        try:
            # Assuming raw_response_content is a dict here due to prior checks/raises
            return HyperliquidRawHistoricalOrderResponse.model_validate(raw_response_content)
        except ValidationError as e_final_dict:
            raise HyperliquidResponseHandler._handle_validation_error(
                e_final_dict, f"order status object in {context}", raw_response_content
            ) from e_final_dict

        # Should not be reached if logic above is complete for list/dict
        raise APIError(
            message=f"Unhandled response structure in {context}: "
            f"{type(raw_response_content).__name__}",
            code=APIErrorCode.INVALID_RESPONSE.value,
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
                    e, f"single spot asset context item in {context}", item
                ) from e
            except AttributeError:
                logger.error(
                    f"Model HyperliquidRawAssetCtx appears incomplete or unavailable for "
                    f"{context} items. Validation skipped."
                )
                raise APIError(
                    message=f"Spot asset context model item not fully available for {context}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                ) from None
        return validated_ctxs

    @staticmethod
    def handle_info_vault_details_response(
        raw_response_content: RawJsonResponse, user_address: str
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
                e, context, raw_response_content
            ) from e
        except AttributeError:
            logger.error(
                f"Placeholder or missing model for HyperliquidRawVaultDetailsResponse used for "
                f"{context}. Validation skipped."
            )
            raise APIError(
                message=f"Vault details model not fully available for {context}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            ) from None

    @staticmethod
    def handle_exchange_response(
        raw_response_content: RawJsonResponse, action_type: str
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
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_query_order_history_response(
        raw_response_content: RawJsonResponse, user_address: str
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
                    f"Skipping non-dict item in {context} list at index {item_index}: {item!r}"
                )
                continue
            try:
                # Validate each item against the new historical order model
                validated_orders.append(HyperliquidRawHistoricalOrderResponse.model_validate(item))
            except ValidationError as e:
                raise HyperliquidResponseHandler._handle_validation_error(
                    e, f"single order history item (index {item_index}) in {context}", item
                ) from e
        return validated_orders
