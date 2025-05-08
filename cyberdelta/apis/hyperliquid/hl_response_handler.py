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
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
    HyperliquidRawOrderStatusResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2Book as HyperliquidRawOrderBookResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawRecentTradesResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState as HyperliquidRawUserStateResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_vault_details import (
    HyperliquidRawVaultDetailsResponse
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
    ) -> list[HyperliquidRawOpenOrdersResponse]:
        """Validates the /info response for open_orders."""
        context = f"info (OpenOrders for {user_address})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        validated_orders: list[HyperliquidRawOpenOrdersResponse] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_orders.append(HyperliquidRawOpenOrdersResponse.model_validate(item))
            except ValidationError as e:
                raise HyperliquidResponseHandler._handle_validation_error(
                    e, f"single open order item in {context}", item
                ) from e
        return validated_orders

    @staticmethod
    def handle_info_user_fills_response(
        raw_response_content: RawJsonResponse, user_address: str
    ) -> list[HyperliquidRawUserFillsResponse]:
        """Validates the /info response for user_fills."""
        context = f"info (UserFills for {user_address})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        validated_fills: list[HyperliquidRawUserFillsResponse] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_fills.append(HyperliquidRawUserFillsResponse.model_validate(item))
            except ValidationError as e:
                raise HyperliquidResponseHandler._handle_validation_error(
                    e, f"single user fill item in {context}", item
                ) from e
        return validated_fills

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
    ) -> list[HyperliquidRawRecentTradesResponse]:
        """Validates the /info response for recent_trades."""
        context = f"info (RecentTrades for {symbol})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        validated_trades: list[HyperliquidRawRecentTradesResponse] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_trades.append(HyperliquidRawRecentTradesResponse.model_validate(item))
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
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
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
    ) -> HyperliquidRawOrderStatusResponse:
        """Validates the /info response for order_status."""
        context = f"info (OrderStatus for user {user_address}, oid {order_id})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return HyperliquidRawOrderStatusResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise HyperliquidResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

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
    ) -> list[HyperliquidRawOrderStatusResponse]:
        """Validates the /query_order_history response."""
        context = f"query_order_history (for {user_address})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        validated_orders: list[HyperliquidRawOrderStatusResponse] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_orders.append(HyperliquidRawOrderStatusResponse.model_validate(item))
            except ValidationError as e:
                raise HyperliquidResponseHandler._handle_validation_error(
                    e, f"single order history item in {context}", item
                ) from e
            except AttributeError:
                logger.error(
                    f"Model HyperliquidRawOrderStatusResponse not available for {context} items. "
                    "Validation skipped."
                )
                raise APIError(
                    message=f"Order history item model not fully available for {context}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                ) from None
        return validated_orders
