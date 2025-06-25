"""Response Handler for Backpack API Raw Responses.

Validates raw JSON data against Pydantic models specific to Backpack's API endpoints.
"""

from __future__ import annotations  # Ensure this is at the top if not already

from collections.abc import Mapping

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
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)


# Type alias for raw JSON response from HTTP client
# Aligned with ParsedJsonResponse from http_client.py
type RawJsonResponse = ParsedJsonResponse


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
        """Create a standardized APIError from a ValidationError."""
        logger.error(
            f"[BackpackResponseHandler] Pydantic validation failed for {context}: {e}. "
            f"Raw data: {raw_data!r}",
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
        """Validate the raw response for the Get Ticker endpoint."""
        context = f"ticker ({symbol}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawTicker.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_get_order_book_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawOrderBook:
        """Validate the raw response for the Get Order Book endpoint."""
        context = f"order book ({symbol}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawOrderBook.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_get_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawRecentPublicTrade]:
        """Validate the raw response for the Get Recent Trades endpoint."""
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
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single trade item in {context}",
                    validated_item,
                ) from e
        return validated_items

    @staticmethod
    def handle_get_balances_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> dict[str, BackpackRawBalance]:
        """Validate the raw response for the Get Balances endpoint."""
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
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"balance details for {asset_symbol}",
                    validated_balance,
                ) from e
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
        """Validate the raw response for the Place Order endpoint."""
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
        """
        if raw_response_content not in [None, {}]:
            # If we get content, it might be an error structure or unexpected success data.
            # For Backpack, successful cancel usually returns 200 OK with empty body or {}.
            logger.warning(
                f"Received unexpected content after cancelling order {order_id} "
                f"for {symbol}: {raw_response_content!r}",
            )

        # Create CancelOrderResult for successful cancellation
        from cyberdelta.core.models.enums import CancelOrderResultStatus
        from cyberdelta.core.models.market.order import CancelOrderResult

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
        """Validate the raw response for the Get Open Orders endpoint."""
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
        """Validate the raw response for the Get Funding Rate endpoint."""
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
        """Validate the raw response for the Get Account Info endpoint."""
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
        """Validate the raw response for the Get Markets endpoint."""
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
            market_model = BackpackRawMarket.model_validate(validated_data)
            return market_model
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
        """Validate the raw response for the Withdraw endpoint."""
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
        """Validate the raw response for the Get Order History endpoint."""
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
        """Validate the raw response for the Get Market Data (Klines) endpoint."""
        context = f"market data (klines {timeframe}) for {symbol} - Status: {status_code}"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_klines: list[BackpackRawKline] = []
        for _i, item_raw in enumerate(validated_list):
            if not isinstance(item_raw, list):  # Backpack klines are lists of values
                logger.warning(
                    f"[{__name__}] Skipping non-list kline item in {context}: {item_raw!r}",
                )
                continue
            try:
                # BackpackRawKline is now imported at module level
                validated_klines.append(BackpackRawKline.model_validate(item_raw))
            except ValidationError as e:
                # Log the specific item that failed validation
                logger.error(
                    f"[{__name__}] Pydantic validation failed for single kline item in "
                    f"{context}: {e}. Item: {item_raw!r}",
                )
                # Re-raise to fail the entire response if one kline is bad, or collect valid ones
                # Pass the full validated_list since item_raw type is not fully known
                raise BackpackResponseHandler._handle_validation_error(
                    e,
                    f"single kline item in {context}",
                    validated_list,
                ) from e
            except Exception as e_unk_item:
                logger.error(
                    f"[{__name__}] Unexpected error validating single kline item in "
                    f"{context}: {e_unk_item}. Item: {item_raw!r}",
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
        """Validate the raw response for the Get Historical Trades endpoint."""
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
        """Validate the raw response for the Get Order Status endpoint."""
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
                    f"[{__name__}] Skipping non-dict item in {context} list: {item!r}. "
                    f"Full response: {raw_response_content!r}",
                )
                continue  # Skip non-dict items, but don't fail the whole batch

            try:
                validated_orders.append(BackpackRawOrder.model_validate(validated_item))
            except ValidationError as e:
                # Log the specific item that failed validation but continue processing others
                # to return successfully validated items if any.
                # Or, re-raise if strictness is required. For cancelAll, it might be better
                # to return what was successfully parsed as cancelled.
                logger.error(
                    f"[{__name__}] Pydantic validation failed for single order item in "
                    f"{context}: {e}. Item: {validated_item!r}.",
                )
                # Optionally, re-raise if any single item failing should invalidate
                # the whole response:
                # raise BackpackResponseHandler._handle_validation_error(
                #     e, f"single order item in {context}", validated_item
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
                f"[{__name__}] {context} 'message' field is not a string: "
                f"{validated_data['message']}",
            )
            # Don't raise, but log. Message is optional and for info.

        if "transferId" in validated_data and not isinstance(
            validated_data["transferId"],
            str,
        ):
            logger.warning(
                f"[{__name__}] {context} 'transferId' field is not a string: "
                f"{validated_data['transferId']}",
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
        """Validate the raw response for the Get Current Funding Rate endpoint."""
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
                    BackpackRawFundingIntervalRate.model_validate(validated_item)
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
