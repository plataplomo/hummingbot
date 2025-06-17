"""Response Handler for Hyperliquid API Raw Responses.

Validates raw JSON data against Pydantic models specific to Hyperliquid's API endpoints.
"""

from collections.abc import Mapping
from typing import Any, TypeGuard

from pydantic import ValidationError  # BaseModel, Field no longer used directly here

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse

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

            # Direct Pydantic validation - no business logic
            return HyperliquidRawMetaAndAssetCtxsResponse.model_validate(raw_response_content)

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
    def handle_info_user_state_response(
        raw_response_content: ParsedJsonResponse,
        user_address: str,
    ) -> HyperliquidRawUserStateResponse:
        """Validates the /info response for user_state.
        
        Architecture Compliance: Pure validation with Pydantic boundary protection.
        All transformation logic delegated to preprocessing mapper when needed.
        """
        context = f"info (user state for {user_address})"
        
        # Type validation at boundary
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
            
        try:
            # Pydantic validation at boundary - no business logic here
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
        """Validates the /info response for open_orders.
        
        Architecture Compliance: Pure validation with Pydantic boundary protection.
        Returns validated Raw model for service layer transformation.
        """
        context = f"info (open orders for {user_address})"
        
        # Type validation at boundary
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
            
        try:
            # Pydantic validation at boundary - list of orders
            return HyperliquidRawOpenOrdersResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
            ) from e

    @staticmethod
    def handle_info_user_fills_response(
        raw_response_content: ParsedJsonResponse,
        user_address: str,
    ) -> HyperliquidRawUserFillsResponse:
        """Validates the /info response for user_fills.
        
        Architecture Compliance: Pure validation with Pydantic boundary protection.
        Trade history transformation handled by service layer mappers.
        """
        context = f"info (user fills for {user_address})"
        
        # Type validation at boundary
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
            
        try:
            # Pydantic validation at boundary - list of fills
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
        """Validates the /info response for l2Book.
        
        Architecture Compliance: Only structural validation per ERROR_HANDLING.md.
        """
        # Basic type validation
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected L2 book response format: expected dict, "
                       f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        
        # Direct Pydantic validation - no business logic
        try:
            return HyperliquidRawOrderBookResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, f"L2 book ({symbol})", raw_response_content
            ) from e

    @staticmethod
    def handle_info_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawPublicTrade]:
        """Validates the /info response for recentTrades.
        
        Architecture Compliance: Only structural validation per ERROR_HANDLING.md.
        """
        # Basic type validation
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected recent trades response format: expected list, "
                       f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        
        # Direct Pydantic validation - no business logic
        try:
            return [HyperliquidRawPublicTrade.model_validate(trade) for trade in raw_response_content]
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, f"recent trades ({symbol})", raw_response_content
            ) from e

    @staticmethod
    def handle_info_candle_snapshot_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        interval: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawCandleSnapshot:
        """Validates the /info response for candle_snapshot.

        Architecture Compliance: Only structural validation per ERROR_HANDLING.md.
        """
        # Basic type validation
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected candle snapshot response format: expected list, "
                       f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        
        # Direct Pydantic validation - no business logic
        try:
            return HyperliquidRawCandleSnapshot.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, f"candle snapshot ({symbol}, {interval})", raw_response_content
            ) from e

    @staticmethod
    def handle_info_order_status_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
        order_id: int,
    ) -> HyperliquidRawHistoricalOrderResponse:
        """Validates the /info response for order_status.

        Architecture Compliance: Only structural validation per ERROR_HANDLING.md.
        """
        # Basic type validation
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected order status response format: expected list, "
                       f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        
        # Direct Pydantic validation - no business logic
        try:
            return HyperliquidRawHistoricalOrderResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, f"order status (user: {user_address}, order: {order_id})", raw_response_content
            ) from e

    @staticmethod
    def handle_info_spot_asset_contexts_response(
        raw_response_content: RawJsonResponse,
    ) -> list[HyperliquidRawAssetCtx]:
        """Validates the /info response for spot asset contexts.
        
        Architecture Compliance: Only structural validation per ERROR_HANDLING.md.
        """
        # Basic type validation
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected spot asset contexts response format: expected list, "
                       f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        
        # Direct Pydantic validation - no business logic
        try:
            return [HyperliquidRawAssetCtx.model_validate(item) for item in raw_response_content]
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, "spot asset contexts", raw_response_content
            ) from e

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
        raw_response_content: ParsedJsonResponse,
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
        # Direct Pydantic validation - no business logic per ERROR_HANDLING.md
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
