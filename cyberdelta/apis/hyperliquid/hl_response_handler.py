"""Response Handler for Hyperliquid API Raw Responses.

Validates raw JSON data against Pydantic models specific to Hyperliquid's API endpoints.
"""

from collections.abc import Mapping

from pydantic import ValidationError  # BaseModel, Field no longer used directly here

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
    HyperliquidRawFundingHistoryResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
    HyperliquidRawHistoricalOrdersResponse,
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
    HyperliquidRawRecentTradesResponse,
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
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# Type alias for raw JSON response from HTTP client
# Aligned with ParsedJsonResponse from http_client.py
type RawJsonResponse = ParsedJsonResponse


# --- Processed Status Models (REMOVED) ---
# Definitions were moved to cyberdelta/apis/hyperliquid/models/hl_processed_exchange_responses.py


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
            "[HyperliquidResponseHandler] Pydantic validation failed for %s: %s. "
            "Status: %s. Headers: %s. Raw data: %r"
        )
        logger.error(
            "pydantic_validation_failed",
            action="validate_response",
            context=context,
            validation_error=str(e),
            status_code=status_code,
            headers=headers,
            raw_data=repr(raw_data),
            message=log_message,
            message_args=(
                context,
                str(e),
                status_code if status_code is not None else "N/A",
                headers if headers is not None else "N/A",
                raw_data,
            ),
        )
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
        """Validates the /info response expected to be MetaAndAssetCtxs.

        Architecture Compliance: Pure validation with Pydantic boundary protection.
        All structure validation is delegated to the model's validator.
        """
        context = "info (MetaAndAssetCtxs)"

        try:
            # Direct Pydantic validation - all preprocessing handled by model
            return HyperliquidRawMetaAndAssetCtxsResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
                status_code,
                headers,
            ) from e

    @staticmethod
    def handle_info_user_state_response(
        raw_response_content: ParsedJsonResponse,
        user_address: str,
        status_code: int,
    ) -> HyperliquidRawUserStateResponse:
        """Validates the /info response for user_state.

        Architecture Compliance: Pure validation with Pydantic boundary protection.
        All transformation logic delegated to preprocessing mapper when needed.
        """
        context = f"info (user state for {user_address})"

        # Type validation at boundary
        validated_data = ensure_dict_response(raw_response_content, context, status_code)

        try:
            # Pydantic validation at boundary - no business logic here
            return HyperliquidRawUserStateResponse.model_validate(validated_data)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_info_open_orders_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
        status_code: int,
    ) -> HyperliquidRawOpenOrdersResponse:
        """Validates the /info response for open_orders.

        Architecture Compliance: Pure validation with Pydantic boundary protection.
        Returns validated Raw model for service layer transformation.
        """
        context = f"info (open orders for {user_address})"

        # Type validation at boundary
        validated_list = ensure_list_response(raw_response_content, context, status_code)

        try:
            # Pydantic validation at boundary - list of orders
            return HyperliquidRawOpenOrdersResponse.model_validate(validated_list)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                validated_list,
            ) from e

    @staticmethod
    def handle_info_user_fills_response(
        raw_response_content: ParsedJsonResponse,
        user_address: str,
        status_code: int,
    ) -> HyperliquidRawUserFillsResponse:
        """Validates the /info response for user_fills.

        Architecture Compliance: Pure validation with Pydantic boundary protection.
        Trade history transformation handled by service layer mappers.
        """
        context = f"info (user fills for {user_address})"

        # Type validation at boundary
        validated_list = ensure_list_response(raw_response_content, context, status_code)

        try:
            # Pydantic validation at boundary - list of fills
            return HyperliquidRawUserFillsResponse.model_validate(validated_list)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                validated_list,
            ) from e

    @staticmethod
    def handle_info_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
    ) -> HyperliquidRawAssetCtx:
        """Validates the /info response for funding rate (per symbol)."""
        context = f"info (funding rate for {symbol})"
        validated_data = ensure_dict_response(raw_response_content, context, status_code)
        try:
            return HyperliquidRawAssetCtx.model_validate(validated_data)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e
        except AttributeError:
            logger.error(
                "model_incomplete_or_unavailable",
                context=context,
                model_name="HyperliquidRawAssetCtx",
                message="Model %s appears incomplete or unavailable for %s. Validation skipped.",
                message_args=("HyperliquidRawAssetCtx", context),
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

        Architecture Compliance: Pure validation with Pydantic boundary protection.
        Preprocessing (including None handling) is delegated to the model's validator.
        """
        context = f"L2 book ({symbol})"

        try:
            # Pass symbol through validation context for None response handling
            return HyperliquidRawOrderBookResponse.model_validate(
                raw_response_content,
                context={"symbol": symbol},
            )
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
            ) from e

    @staticmethod
    def handle_info_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawPublicTrade]:
        """Validates the /info response for recentTrades.

        Architecture Compliance: Pure validation with Pydantic boundary protection.
        All list validation is delegated to the RootModel.
        """
        context = f"recent trades ({symbol})"

        try:
            # Use RootModel for validation - it handles list structure
            validated_response = HyperliquidRawRecentTradesResponse.model_validate(
                raw_response_content,
            )
            # Return the items from the RootModel
            return validated_response.items
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
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
        Empty list handling is done by the model's preprocessing validator.
        """
        # Direct Pydantic validation - preprocessing is handled by the model
        try:
            return HyperliquidRawCandleSnapshot.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                f"candle snapshot ({symbol}, {interval})",
                raw_response_content,
            ) from e

    @staticmethod
    def handle_info_order_status_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
        order_id: int,
    ) -> HyperliquidRawHistoricalOrderResponse:
        """Validates the /info response for order_status.

        Architecture Compliance: Only structural validation per ERROR_HANDLING.md.
        All data transformation and error handling is done by the model's preprocessing validator.
        """
        # Direct Pydantic validation - preprocessing is handled by the model
        try:
            return HyperliquidRawHistoricalOrderResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                f"order status (user: {user_address}, order: {order_id})",
                raw_response_content,
            ) from e

    @staticmethod
    def handle_info_spot_asset_contexts_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> list[HyperliquidRawAssetCtx]:
        """Validates the /info response for spot asset contexts.

        Architecture Compliance: Only structural validation per ERROR_HANDLING.md.
        """
        # Basic type validation
        validated_list = ensure_list_response(
            raw_response_content,
            "spot asset contexts",
            status_code,
        )

        # Direct Pydantic validation - no business logic
        try:
            return [HyperliquidRawAssetCtx.model_validate(item) for item in validated_list]
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                "spot asset contexts",
                validated_list,
            ) from e

    @staticmethod
    def handle_info_vault_details_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
        status_code: int,
    ) -> HyperliquidRawVaultDetailsResponse:
        """Validates the /info response for vault details."""
        context = f"info (VaultDetails for {user_address})"
        validated_data = ensure_dict_response(raw_response_content, context, status_code)
        try:
            return HyperliquidRawVaultDetailsResponse.model_validate(validated_data)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e
        except AttributeError:
            logger.error(
                "placeholder_or_missing_model",
                context=context,
                model_name="HyperliquidRawVaultDetailsResponse",
                message="Placeholder or missing model for %s used for %s. Validation skipped.",
                message_args=("HyperliquidRawVaultDetailsResponse", context),
            )
            raise APIError(
                message=f"Vault details model not fully available for {context}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            ) from None

    @staticmethod
    def handle_exchange_response(
        raw_response_content: ParsedJsonResponse,
        action_type: str,
        status_code: int,
    ) -> HyperliquidRawExchangeResponse:
        """Validates the /exchange response (for actions like order, cancel, withdraw)."""
        context = f"exchange ({action_type})"
        validated_data = ensure_dict_response(raw_response_content, context, status_code)
        # Direct Pydantic validation - no business logic per ERROR_HANDLING.md
        try:
            return HyperliquidRawExchangeResponse.model_validate(validated_data)
        except ValidationError as e:
            logger.warning(
                "response_validation_failed",
                action="validate_response",
                context=context,
                raw_data=repr(validated_data),
                message="Initial validation of %s failed. Raw: %r",
                message_args=(context, validated_data),
            )
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_historical_orders_response(
        raw_response_content: RawJsonResponse,
        user_address: str,
    ) -> list[HyperliquidRawHistoricalOrderResponse]:
        """Validates the historicalOrders response.

        Architecture Compliance: Pure validation with Pydantic boundary protection.
        All list validation and item filtering is delegated to the RootModel.
        """
        context = f"historicalOrders (for {user_address})"

        try:
            # Use RootModel for validation - it handles list structure and filtering
            validated_response = HyperliquidRawHistoricalOrdersResponse.model_validate(
                raw_response_content,
            )
            # Return the items from the RootModel
            return validated_response.items
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
            ) from e

    @staticmethod
    def handle_historical_funding_rates_response(
        raw_response_content: RawJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawFundingHistoryItem]:
        """Validates the /info response for historical funding rates.

        Architecture Compliance: Pure validation with Pydantic boundary protection.
        All list validation and error handling is delegated to the RootModel.
        """
        context = "historical_funding_rates"

        try:
            # Use RootModel for validation - it handles all preprocessing
            validated_response = HyperliquidRawFundingHistoryResponse.model_validate(
                raw_response_content,
            )
            # Return the items from the RootModel
            return validated_response.items
        except ValidationError as e:
            # Extract the first error for specific error messages expected by tests
            if e.errors():
                first_error = e.errors()[0]
                # Check if it's a type error at the root level
                if first_error.get("loc") == () and "expected list" in str(
                    first_error.get("msg", ""),
                ):
                    error_message = str(first_error.get("msg"))
                    logger.error(
                        "api_error_response",
                        action="handle_error",
                        error_message=error_message,
                        raw_content=repr(raw_response_content),
                        message="%s. Raw: %r",
                        message_args=(error_message, raw_response_content),
                    )
                    raise APIError(
                        message=error_message,
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    ) from e
                # Check if it's an item type error
                if len(first_error.get("loc", ())) > 1 and "Expected dict" in str(
                    first_error.get("msg", ""),
                ):
                    error_message = str(first_error.get("msg"))
                    logger.error(
                        "validation_error_item_type",
                        action="handle_error",
                        error_message=error_message,
                        raw_response_content=repr(raw_response_content),
                        message="[HyperliquidResponseHandler] %s. Full raw response: %r",
                        message_args=(error_message, raw_response_content),
                    )
                    raise APIError(
                        message=error_message,
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    ) from e

            # Default error handling
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
                status_code,
                headers,
            ) from e

    @staticmethod
    def handle_all_mids_response(
        raw_response_content: RawJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawAllMids:
        """Validates the /info response for allMids.

        Architecture Compliance: Pure validation with Pydantic boundary protection.
        Returns validated mapping of symbol to mid price.
        """
        context = "all mids"

        try:
            # Use the RootModel validation for the dict structure
            return HyperliquidRawAllMids.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e,
                context,
                raw_response_content,
                status_code,
                headers,
            ) from e
