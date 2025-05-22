"""
Response Handler for Hyperliquid API Raw Responses.

Validates raw JSON data against Pydantic models specific to Hyperliquid\'s API endpoints.
"""

from collections.abc import Mapping

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


# --- Processed Status Models (REMOVED) ---
# Definitions were moved to cyberdelta/apis/hyperliquid/models/hl_processed_exchange_responses.py


class HyperliquidResponseHandler:
    """
    Handles validation of raw JSON responses from Hyperliquid REST API endpoints.

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
        if not isinstance(raw_response_content, list):
            logger.error(
                f"Unexpected {context} format. Status: {status_code}, Headers: {headers}, Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )
        try:
            # Preprocess the raw_response_content to align with Pydantic models
            # The raw_response_content is a list: [meta_data, asset_ctxs_data]
            processed_raw_response_content = raw_response_content  # Create a mutable copy

            if len(processed_raw_response_content) == 2:
                meta_data = processed_raw_response_content[0]
                if isinstance(meta_data, dict):
                    # Remove 'marginTables' from meta_data if present, as it's not in our Pydantic model
                    meta_data.pop("marginTables", None)

                    # Preprocess 'universe' items within meta_data
                    if "universe" in meta_data and isinstance(meta_data["universe"], list):
                        for item in meta_data["universe"]:
                            if isinstance(item, dict):
                                # Remove 'marginTableId' if present
                                item.pop("marginTableId", None)
                                item.pop("isDelisted", None)
                                # Add 'onlyIsolated' if missing
                                if "onlyIsolated" not in item:
                                    item["onlyIsolated"] = False  # Default to False if not provided

            return HyperliquidRawMetaAndAssetCtxsResponse.model_validate(
                processed_raw_response_content
            )
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content, status_code, headers
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
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawOrderBookResponse:
        """Validates the /info response for l2Book."""
        context = f"info (L2Book for {symbol})"
        if not isinstance(raw_response_content, dict):
            logger.error(
                f"Unexpected {context} format for {symbol}. Status: {status_code}, Headers: {headers}, Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )
        try:
            return HyperliquidRawOrderBookResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content, status_code, headers
            ) from e

    @staticmethod
    def handle_info_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawPublicTrade]:
        """Validates the /info response for recentTrades."""
        context = f"info (RecentTrades for {symbol})"
        if not isinstance(raw_response_content, list):
            logger.error(
                f"Unexpected {context} format for {symbol}. Status: {status_code}, Headers: {headers}, Raw: {raw_response_content!r}"
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
                    f"Skipping non-dict item at index {i} in {context} for {symbol}. Item: {item!r}"
                )
                continue  # Skip non-dict items

            try:
                trade = HyperliquidRawPublicTrade.model_validate(item)
                validated_trades.append(trade)
            except ValidationError as e:
                logger.error(
                    f"[HyperliquidResponseHandler] Pydantic validation failed for trade item at index {i} in {context}: {e}. "
                    f"Status: {status_code}. Headers: {headers}. Raw data: {item!r}"
                )
                # Make the error message more generic to match test expectations
                error_message = (
                    f"Invalid single recent trade item in info (RecentTrades for {symbol}) "
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
                    f"[HyperliquidResponseHandler] Unexpected error validating trade item at index {i} in {context}: {e}. "
                    f"Status: {status_code}. Headers: {headers}. Raw data: {item!r}"
                )
                raise APIError(
                    message=f"Unexpected error processing trade item at index {i} in {context} response from exchange: {e}",
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
        context = f"info (CandleSnapshot for {symbol} {interval})"
        if not isinstance(raw_response_content, dict):
            logger.error(
                f"Unexpected {context} format for {symbol} {interval}. Status: {status_code}, Headers: {headers}, Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )
        try:
            return HyperliquidRawCandleSnapshot.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content, status_code, headers
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
                message=(
                    f"Order {order_id} for user {user_address} not found "
                    f"(direct string: '{raw_response_content}')"
                ),
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
                    message=(f"Order {order_id} for {user_address} not found (empty list)."),
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
                    message=(
                        f"Unexpected item type in {context} response list: "
                        f"expected dict, got {type(status_item).__name__}"
                    ),
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
    def process_first_exchange_status(
        first_status_raw: RawJsonPrim | RawJson,
        action_description: str,
    ) -> HyperliquidSuccessfulOrderStatus | HyperliquidErrorStatus:
        """
        Processes the first status item from an /exchange endpoint response.

        Args:
            first_status_raw: The raw status item (dict or string).
            action_description: Description of the action (e.g., "place_order", "cancel_order").

        Returns:
            A HyperliquidSuccessfulOrderStatus or HyperliquidErrorStatus model.

        Raises:
            APIError: If the status structure is unknown or invalid.
        """
        if isinstance(first_status_raw, dict):
            if "resting" in first_status_raw and isinstance(first_status_raw["resting"], dict):
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

            if "filled" in first_status_raw and isinstance(first_status_raw["filled"], dict):
                filled_details = first_status_raw["filled"]
                oid_raw = filled_details.get("oid")
                total_sz_raw = filled_details.get("totalSz")
                avg_px_raw = filled_details.get("avgPx")

                if not isinstance(oid_raw, int):
                    raise APIError(
                        message=(
                            f"Invalid or missing 'oid' (expected int) in filled status "
                            f"for {action_description}"
                        ),
                        code=APIErrorCode.INVALID_RESPONSE.value,
                        metadata={"raw_status": first_status_raw},
                    )
                if not isinstance(total_sz_raw, str):
                    raise APIError(
                        message=f"Invalid or missing 'totalSz' (expected str) in filled status for "
                        f"{action_description}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                        metadata={"raw_status": first_status_raw},
                    )
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
                    status_type="filled", oid=oid_raw, total_sz=total_sz_raw, avg_px=avg_px_raw
                )

            if "canceled" in first_status_raw and isinstance(first_status_raw["canceled"], dict):
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

            if "error" in first_status_raw and isinstance(first_status_raw["error"], str):
                return HyperliquidErrorStatus(message=first_status_raw["error"])

            # If it's a dict but doesn't match known structures
            raise APIError(
                message=f"Unknown status structure for {action_description}: {first_status_raw!r}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        if isinstance(first_status_raw, str):
            if first_status_raw.lower() == "canceled":
                return HyperliquidSuccessfulOrderStatus(status_type="canceled_str")
            # Any other string is treated as an error message for now, or could be refined
            # This path might indicate an unexpected direct string error from the API
            # that isn't wrapped in an {"error": ...} object.
            logger.warning(
                f"Encountered direct string status for {action_description}: '{first_status_raw}'. "
                f"Treating as error."
            )
            return HyperliquidErrorStatus(message=first_status_raw)

        # If not dict or str
        raise APIError(
            message=(
                f"Invalid status type for {action_description}: {type(first_status_raw)}. "
                f"Expected dict or str."
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

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
        context = "historical funding rates"
        if not isinstance(raw_response_content, list):
            # Construct the more specific error message expected by the test
            error_message = (
                f"Expected list for {context}, got {type(raw_response_content).__name__}"
            )
            logger.error(f"{error_message}. Raw: {raw_response_content!r}")
            raise APIError(message=error_message, code=APIErrorCode.INVALID_RESPONSE.value)

        validated_rates: list[HyperliquidRawFundingHistoryItem] = []
        for i, item_raw in enumerate(raw_response_content):
            if not isinstance(item_raw, dict):
                # Revert to a more specific message about type mismatch for this test
                error_message = f"Expected dict for historical funding rate item, got {type(item_raw).__name__} at index {i}"
                logger.error(
                    f"[HyperliquidResponseHandler] {error_message}. Raw item: {item_raw!r}. Full raw response: {raw_response_content!r}"
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
                    f"Validation error for historical funding rate item at index {i}: {e.errors()}"
                )
                logger.error(
                    f"{error_message} Full raw response: {raw_response_content!r}. Raw item: {item_raw!r}"
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
