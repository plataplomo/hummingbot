"""Response Handler for Backpack API Raw Responses.

Validates raw JSON data against Pydantic models specific to Backpack's API endpoints.
"""

from __future__ import annotations  # Ensure this is at the top if not already

import json
from collections.abc import Mapping
from typing import Any, TypeGuard

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFill

# BackpackRawApiError import removed as validation is the focus here. Error mapping is separate.
from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRate,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_limits import (
    BackpackRawMaxBorrowQuantity,
    BackpackRawMaxOrderQuantity,
    BackpackRawMaxWithdrawalQuantity,
)
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawMarket,
    BackpackRawOrderBook,
    BackpackRawTicker,
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import (
    BackpackRawWithdrawalResponse,
)
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import CancelOrderResultStatus
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)


# Type alias for raw JSON response from HTTP client
# Aligned with ParsedJsonResponse from http_client.py
type RawJsonResponse = ParsedJsonResponse


def _safe_json_repr(value: object) -> str:
    """Convert any value to a JSON string representation for logging.

    This helper function ensures type safety when logging dynamic data
    from API responses while avoiding pyright strict mode issues.

    Args:
        value: Any value to convert to JSON representation

    Returns:
        JSON string representation or string fallback
    """
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        # Fallback for non-JSON-serializable objects
        return repr(value)


def _is_list_of_any(value: object) -> TypeGuard[list[Any]]:
    """Type guard to check if value is a list.

    This helps pyright understand type narrowing in strict mode.

    Args:
        value: Value to check

    Returns:
        True if value is a list, False otherwise
    """
    return isinstance(value, list)


class BackpackResponseHandler:
    """Handles validation of raw JSON responses from Backpack REST API endpoints.

    Uses Pydantic models defined in `cyberdelta.apis.backpack.models` to validate
    the structure and types of the raw data. Raises APIError if validation fails.
    """

    @staticmethod
    def _handle_validation_error(
        e: ValidationError,
        context: str,
        raw_data: RawJsonResponse,
    ) -> APIError:
        """Create a standardized APIError from a ValidationError.

        Returns:
            APIError with INVALID_RESPONSE code and validation details.
        """
        logger.error(
            "backpack_pydantic_validation_failed",
            context=context,
            validation_error=str(e),
            raw_data=raw_data,
            handler_class="BackpackResponseHandler",
            message="Pydantic validation failed",
        )
        # Use INVALID_RESPONSE code as per architecture rules
        return APIError(
            message=f"Invalid {context} response from exchange: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=e,
        )

    @staticmethod
    def handle_get_ticker_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawTicker:
        """Validate the raw response for the Get Ticker endpoint.

        Returns:
            Validated BackpackRawTicker model.

        Raises:
            APIError: If validation fails or response format is invalid.
        """
        context = f"ticker ({symbol}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawTicker.model_validate(validated_data)
        except ValidationError as e:
            api_error = BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            )
            raise api_error from e

    @staticmethod
    def handle_get_order_book_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawOrderBook:
        """Validate the raw response for the Get Order Book endpoint.

        Returns:
            Validated BackpackRawOrderBook model.

        Raises:
            APIError: If validation fails or response format is invalid.
        """
        context = f"order book ({symbol}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawOrderBook.model_validate(validated_data)
        except ValidationError as e:
            api_error = BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            )
            raise api_error from e

    @staticmethod
    def handle_get_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawRecentPublicTrade]:
        """Validate the raw response for the Get Recent Trades endpoint.

        Returns:
            List of validated BackpackRawRecentPublicTrade models.

        Raises:
            APIError: If validation fails or response format is invalid.
        """
        context = f"recent trades ({symbol}) - Status: {status_code}"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_items: list[BackpackRawRecentPublicTrade] = []
        for i, item in enumerate(validated_list):
            # Ensure item is a dict before validating
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_items.append(BackpackRawRecentPublicTrade.model_validate(validated_item))
            except ValidationError as e:
                api_error = BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single trade item in {context}",
                    validated_item,
                )
                raise api_error from e
        return validated_items

    @staticmethod
    def handle_get_balances_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> dict[str, BackpackRawBalance]:
        """Validate the raw response for the Get Balances endpoint.

        Returns:
            Dictionary mapping asset symbols to BackpackRawBalance models.

        Raises:
            APIError: If validation fails or response format is invalid.
        """
        context = "balances"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_balances: dict[str, BackpackRawBalance] = {}
        # validated_data is known to be a dict here
        for asset_symbol, balance_details in validated_data.items():
            # Ensure balance_details is dict before validating
            validated_balance = ensure_dict_response(
                balance_details,
                f"{context} for asset '{asset_symbol}'",
                status_code,
            )
            try:
                # Ensure asset_symbol is string for the key
                validated_balances[str(asset_symbol)] = BackpackRawBalance.model_validate(
                    validated_balance,
                )
            except ValidationError as e:
                api_error = BackpackResponseHandler._handle_validation_error(
                    e,
                    f"balance details for {asset_symbol}",
                    validated_balance,
                )
                raise api_error from e
        return validated_balances

    @staticmethod
    def handle_get_positions_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawPosition]:
        """Validate the raw response for the Get Positions endpoint.

        Handles a single position dictionary if a symbol is provided,
        or a list of position dictionaries if no symbol is provided.

        Returns:
            List of validated BackpackRawPosition models.

        Raises:
            APIError: If symbol not found or validation of position data fails.
        """
        context = f"positions ({symbol or 'all'})"
        validated_positions: list[BackpackRawPosition] = []

        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                position = BackpackRawPosition.model_validate(validated_item)
                if symbol is None or position.symbol == symbol:
                    validated_positions.append(position)
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single position item in {context}",
                    validated_item,
                ) from e

        if symbol is not None and not validated_positions:
            raise APIError(
                message=f"No position found for symbol '{symbol}' in {context} response.",
                code=APIErrorCode.SYMBOL_NOT_FOUND.value,
            )
        return validated_positions

    @staticmethod
    def handle_place_order_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> BackpackRawOrder:
        """Validate the raw response for the Place Order endpoint.

        Returns:
            Validated BackpackRawOrder model.

        Raises:
            APIError: If validation of order data fails.
        """
        context = "place order response"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawOrder.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_cancel_order_response(
        raw_response_content: RawJsonResponse,
        order_id: str,
        symbol: str,
    ) -> CancelOrderResult:
        """Validate the raw response for the Cancel Order endpoint.

        Expects no content on success.

        Returns:
            CancelOrderResult indicating successful cancellation.
        """
        if raw_response_content not in [None, {}]:
            # If we get content, it might be an error structure or unexpected success data.
            # For Backpack, successful cancel usually returns 200 OK with empty body or {}.
            logger.warning(
                "backpack_unexpected_cancel_content",
                order_id=order_id,
                symbol=symbol,
                raw_response_content=raw_response_content,
                message="Received unexpected content after cancelling order",
            )

        # Create CancelOrderResult for successful cancellation
        return CancelOrderResult(
            symbol=symbol,
            order_id=order_id,
            client_order_id=None,
            success=True,
            message=None,
            status=CancelOrderResultStatus.SUCCESS,
            raw_response=raw_response_content if isinstance(raw_response_content, dict) else None,
        )

    @staticmethod
    def handle_get_open_orders_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawOrder]:
        """Validate the raw response for the Get Open Orders endpoint.

        Returns:
            List of validated BackpackRawOrder models for open orders.

        Raises:
            APIError: If validation of order data fails.
        """
        context = f"open orders ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_orders: list[BackpackRawOrder] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_orders.append(BackpackRawOrder.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single open order item in {context}",
                    validated_item,
                ) from e
        return validated_orders

    @staticmethod
    def handle_get_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRate:
        """Validate the raw response for the Get Funding Rate endpoint.

        Returns:
            Validated BackpackRawFundingRate model.

        Raises:
            APIError: If response format is unexpected (empty list or non-dict/list type) or
                validation of funding rate data fails.
        """
        context = f"funding rate ({symbol}) - Status: {status_code}"

        # Try to handle as dict first
        if isinstance(raw_response_content, dict):
            raw_data_to_validate = raw_response_content
        elif isinstance(raw_response_content, list):
            # Check if it is a list, as HL funding rate is a list
            if not raw_response_content:  # Empty list
                raise APIError(
                    message=(
                        f"Empty list for {context} response, expected dict or non-empty list."
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )
            # Assuming the first element is the target if it's a list
            # (adapting for HL-like structures)
            raw_data_to_validate = ensure_dict_response(
                raw_response_content[0],
                f"{context} (first item in list)",
                status_code,
            )
        else:
            raise APIError(
                message=f"Unexpected {context} response format: expected dict or list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        try:
            return BackpackRawFundingRate.model_validate(raw_data_to_validate)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                raw_data_to_validate,
            ) from e

    @staticmethod
    def handle_get_account_info_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> BackpackRawAccountSummary:
        """Validate the raw response for the Get Account Info endpoint.

        Returns:
            Validated BackpackRawAccountSummary model.

        Raises:
            APIError: If validation of account data fails.
        """
        context = "account info"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawAccountSummary.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_get_markets_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> list[BackpackRawMarket]:
        """Validate the raw response for the Get Markets endpoint.

        Returns:
            List of validated BackpackRawMarket models.

        Raises:
            APIError: If validation of market data fails.
        """
        context = "markets"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        markets: list[BackpackRawMarket] = []
        for i, market_data in enumerate(validated_list):
            validated_item = ensure_dict_response(
                market_data,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                market_model = BackpackRawMarket.model_validate(validated_item)
                markets.append(market_model)
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"{context} item {i}",
                    validated_item,
                ) from e

        return markets

    @staticmethod
    def handle_get_market_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMarket:
        """Validate the raw response for the Get Market endpoint.

        Args:
            raw_response_content: The raw response data from the API.
            symbol: The requested symbol for context in error messages.
            status_code: HTTP status code of the response.
            headers: HTTP response headers.

        Returns:
            BackpackRawMarket: The validated market model.

        Raises:
            APIError: If validation fails or response format is unexpected.
        """
        context = f"market for {symbol}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )

        try:
            return BackpackRawMarket.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_withdraw_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> BackpackRawWithdrawalResponse:
        """Validate the raw response for the Withdraw endpoint.

        Returns:
            Validated BackpackRawWithdrawalResponse model.

        Raises:
            APIError: If validation of withdrawal data fails.
        """
        context = "withdraw response"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawWithdrawalResponse.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_get_order_history_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawOrder]:
        """Validate the raw response for the Get Order History endpoint.

        Returns:
            List of validated BackpackRawOrder models from order history.

        Raises:
            APIError: If validation of order data fails.
        """
        context = f"order history ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_orders: list[BackpackRawOrder] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_orders.append(BackpackRawOrder.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single order history item in {context}",
                    validated_item,
                ) from e
        return validated_orders

    @staticmethod
    def handle_get_trade_history_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawPublicTrade]:
        """Validate the raw response for the Get Trade History endpoint.

        Now returns list[BackpackRawPublicTrade] as per user request.

        Returns:
            List of validated BackpackRawPublicTrade models.

        Raises:
            APIError: If validation of trade data fails.
        """
        context = f"trade history ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_items: list[BackpackRawPublicTrade] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_items.append(BackpackRawPublicTrade.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single trade item in {context}",
                    validated_item,
                ) from e
        return validated_items

    @staticmethod
    def handle_get_fills_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawFill]:
        """Validate the raw response for the Get Fills (/wapi/v1/history/fills) endpoint.

        This endpoint returns BackpackRawFill format, different from BackpackRawPublicTrade.

        Returns:
            List of validated BackpackRawFill models.

        Raises:
            APIError: If validation of fill data fails.
        """
        context = f"fills history ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_fills: list[BackpackRawFill] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_fills.append(BackpackRawFill.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single fill item in {context}",
                    validated_item,
                ) from e
        return validated_fills

    @staticmethod
    def handle_get_market_data_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        timeframe: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawKline]:  # Changed return type
        """Validate the raw response for the Get Market Data (Klines) endpoint.

        Returns:
            List of validated BackpackRawKline models.

        Raises:
            APIError: If unexpected error occurs validating kline items or validation of
                kline data fails.
        """
        context = f"market data (klines {timeframe}) for {symbol} - Status: {status_code}"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_klines: list[BackpackRawKline] = []
        for item_raw in validated_list:
            if not _is_list_of_any(item_raw):  # Backpack klines are lists of values
                # Convert item_raw to string representation for logging
                item_str = str(item_raw) if item_raw is not None else "None"
                logger.warning(
                    "backpack_non_list_kline_item",
                    context=context,
                    item_raw=item_str,
                    module_name=__name__,
                    message="Skipping non-list kline item",
                )
                continue
            # Now item_raw is known to be list[Any] thanks to the type guard
            # Backpack klines are lists of values (typically 12 elements)
            # For logging, we'll convert the entire list to string at once
            # This avoids iterating over unknown types
            try:
                # BackpackRawKline is now imported at module level
                validated_klines.append(BackpackRawKline.model_validate(item_raw))
            except ValidationError as e:
                # Log the specific item that failed validation
                # Use JSON-style formatting for better readability of kline data
                logger.exception(
                    "backpack_kline_validation_failed",
                    context=context,
                    validation_error=str(e),
                    item_raw=_safe_json_repr(item_raw),
                    module_name=__name__,
                    message="Pydantic validation failed for single kline item",
                )
                # Re-raise to fail the entire response if one kline is bad, or collect valid ones
                # Pass the full validated_list since item_raw type is not fully known
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single kline item in {context}",
                    validated_list,
                ) from e
            except Exception as e_unk_item:
                # Format the list for logging
                logger.exception(
                    "backpack_kline_unexpected_error",
                    context=context,
                    error=str(e_unk_item),
                    item_raw=_safe_json_repr(item_raw),
                    module_name=__name__,
                    message="Unexpected error validating single kline item",
                )
                raise APIError(
                    message=f"Unexpected error validating kline item: {e_unk_item}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    original_exception=e_unk_item,
                ) from e_unk_item

        return validated_klines

    @staticmethod
    def handle_get_historical_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawPublicTrade]:
        """Validate the raw response for the Get Historical Trades endpoint.

        Returns:
            List of validated BackpackRawPublicTrade models.

        Raises:
            APIError: If validation of trade data fails.
        """
        context = f"historical trades ({symbol}) - Status: {status_code}"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_trades: list[BackpackRawPublicTrade] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_trades.append(BackpackRawPublicTrade.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single historical trade item in {context}",
                    validated_item,
                ) from e
        return validated_trades

    @staticmethod
    def handle_get_order_status_response(
        raw_response_content: RawJsonResponse,
        identifier: str,
        status_code: int,
    ) -> BackpackRawOrder:
        """Validate the raw response for the Get Order Status endpoint.

        Returns:
            Validated BackpackRawOrder model with current order status.
        """
        context = f"order status (id={identifier})"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawOrder.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_cancel_all_orders_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawOrder]:
        """Validate the raw response for the Cancel All Orders endpoint.

        (DELETE /api/v1/orders/cancelAll).
        Expects a list of successfully cancelled orders.

        Returns:
            List of validated BackpackRawOrder models for cancelled orders.
        """
        context = f"cancel all orders ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_orders: list[BackpackRawOrder] = []
        for i, item in enumerate(validated_list):
            # Skip non-dict items with logging, but ensure dict before validation
            try:
                validated_item = ensure_dict_response(
                    item,
                    f"{context} item[{i}]",
                    status_code,
                )
            except APIError:
                logger.warning(
                    "backpack_non_dict_item_skip",
                    context=context,
                    item=item,
                    raw_response_content=raw_response_content,
                    module_name=__name__,
                    message="Skipping non-dict item in response list",
                )
                continue  # Skip non-dict items, but don't fail the whole batch

            try:
                validated_orders.append(BackpackRawOrder.model_validate(validated_item))
            except ValidationError as e:
                # Log the specific item that failed validation but continue processing others
                # to return successfully validated items if any.
                # Or, re-raise if strictness is required. For cancelAll, it might be better
                # to return what was successfully parsed as cancelled.
                logger.exception(
                    "backpack_order_validation_failed",
                    context=context,
                    validation_error=str(e),
                    validated_item=validated_item,
                    module_name=__name__,
                    message="Pydantic validation failed for single order item",
                )
                # Optionally, re-raise if any single item failing should invalidate
                # the whole response:
                # raise BackpackResponseHandler._handle_validation_error(
                # ) from e
                # For now, we'll be lenient and collect valid ones.
                # Consider if this behavior is desired or if it should be stricter.
        return validated_orders

    @staticmethod
    def handle_transfer_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> RawJsonResponse:  # Returns the validated raw dict
        """Validate the raw response for an internal capital transfer.

        Expects a dict with 'success' (bool), optional 'message' (str), and
        optional 'transferId' (str).

        Returns:
            Validated raw response dictionary with transfer details.

        Raises:
            APIError: If 'success' field is missing or not a boolean.
        """
        context = "internal transfer response"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )

        # Basic structure validation
        if "success" not in validated_data or not isinstance(
            validated_data["success"],
            bool,
        ):
            raise APIError(
                message=(
                    f"Invalid {context}: 'success' field missing or not a boolean. "
                    f"Got: {validated_data.get('success')}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        if "message" in validated_data and not isinstance(
            validated_data["message"],
            str,
        ):
            logger.warning(
                "backpack_transfer_message_not_string",
                context=context,
                message_field=validated_data["message"],
                module_name=__name__,
                message="Transfer response 'message' field is not a string",
            )
            # Don't raise, but log. Message is optional and for info.

        if "transferId" in validated_data and not isinstance(
            validated_data["transferId"],
            str,
        ):
            logger.warning(
                "backpack_transfer_id_not_string",
                context=context,
                transfer_id_field=validated_data["transferId"],
                module_name=__name__,
                message="Transfer response 'transferId' field is not a string",
            )
            # Don't raise, but log. TransferId is optional and for info.

        # If successful, the mapper will use this dict to create an internal Transfer model.
        # If not successful (success=False), the mapper should handle this appropriately.
        return validated_data

    @staticmethod
    def handle_get_current_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRate:
        """Validate the raw response for the Get Current Funding Rate endpoint.

        Returns:
            Validated BackpackRawFundingRate model.
        """
        context = f"current funding rate ({symbol}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            # Assuming BackpackRawFundingRate is the correct model for a single, current rate
            return BackpackRawFundingRate.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_get_historical_funding_rates_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawFundingIntervalRate]:
        """Validate the raw response for the Get Historical Funding Rates endpoint.

        (/api/v1/fundingRates).

        Returns:
            List of validated BackpackRawFundingIntervalRate models.
        """
        context = f"historical funding rates ({symbol}) - Status: {status_code}"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_rates: list[BackpackRawFundingIntervalRate] = []
        for i, item_raw in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item_raw,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_rates.append(
                    BackpackRawFundingIntervalRate.model_validate(validated_item),
                )
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single historical funding rate item in {context}",
                    validated_item,
                ) from e
        return validated_rates

    @staticmethod
    def handle_get_collateral_response(
        raw_response_content: RawJsonResponse,
        subaccount_id: int | None,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawCollateralResponse:
        """Validate the raw response for the Get Collateral endpoint.

        (/api/v1/capital/collateral).

        Returns:
            Validated BackpackRawCollateralResponse model.
        """
        context = f"collateral data (subaccount_id={subaccount_id}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawCollateralResponse.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    # --- Account Limits Response Handlers (INTERNAL USE ONLY) ---

    @staticmethod
    def handle_max_borrow_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxBorrowQuantity:
        """Validate the raw response for the Max Borrow Quantity endpoint.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.

        (/api/v1/account/limits/borrow).

        Returns:
            Validated BackpackRawMaxBorrowQuantity model.
        """
        context = f"max borrow quantity ({symbol}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawMaxBorrowQuantity.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_max_order_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        side: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxOrderQuantity:
        """Validate the raw response for the Max Order Quantity endpoint.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.

        (/api/v1/account/limits/order).

        Returns:
            Validated BackpackRawMaxOrderQuantity model.
        """
        context = f"max order quantity ({symbol} {side}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawMaxOrderQuantity.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_max_withdrawal_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxWithdrawalQuantity:
        """Validate the raw response for the Max Withdrawal Quantity endpoint.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.

        (/api/v1/account/limits/withdrawal).

        Returns:
            Validated BackpackRawMaxWithdrawalQuantity model.
        """
        context = f"max withdrawal quantity ({symbol}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawMaxWithdrawalQuantity.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e
