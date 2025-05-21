"""
Response Handler for Backpack API Raw Responses.

Validates raw JSON data against Pydantic models specific to Backpack\'s API endpoints.
"""

from __future__ import annotations  # Ensure this is at the top if not already

from collections.abc import Mapping

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary

# BackpackRawApiError import removed as validation is the focus here. Error mapping is separate.
from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRate,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawOrderBook, BackpackRawTicker
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import (
    BackpackRawWithdrawalResponse,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


# Type alias for raw JSON response from HTTP client
RawJsonPrim = str | int | float | bool | None
RawJson = dict[str, "RawJson"] | list["RawJson"] | RawJsonPrim
type RawJsonResponse = RawJson  # Use type for clarity


class BackpackResponseHandler:
    """
    Handles validation of raw JSON responses from Backpack REST API endpoints.

    Uses Pydantic models defined in `cyberdelta.apis.backpack.models` to validate
    the structure and types of the raw data. Raises APIError if validation fails.
    """

    @staticmethod
    def _handle_validation_error(
        e: ValidationError, context: str, raw_data: RawJsonResponse
    ) -> APIError:
        """Helper to create a standardized APIError from a ValidationError."""
        logger.error(
            f"[BackpackResponseHandler] Pydantic validation failed for {context}: {e}. "
            f"Raw data: {raw_data!r}"
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
        """Validates the raw response for the Get Ticker endpoint."""
        context = f"ticker ({symbol}) - Status: {status_code}"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return BackpackRawTicker.model_validate(raw_response_content)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_get_order_book_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawOrderBook:
        """Validates the raw response for the Get Order Book endpoint."""
        context = f"order book ({symbol}) - Status: {status_code}"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return BackpackRawOrderBook.model_validate(raw_response_content)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_get_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawTrade]:
        """Validates the raw response for the Get Recent Trades endpoint."""
        context = f"recent trades ({symbol}) - Status: {status_code}"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_items: list[BackpackRawTrade] = []
        for item in raw_response_content:
            # Ensure item is a dict before validating
            if not isinstance(item, dict):
                logger.warning(f"[{__name__}] Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_items.append(BackpackRawTrade.model_validate(item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single trade item in {context}", item
                ) from e
        return validated_items

    @staticmethod
    def handle_get_balances_response(
        raw_response_content: RawJsonResponse,
    ) -> dict[str, BackpackRawBalance]:
        """Validates the raw response for the Get Balances endpoint."""
        context = "balances"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_balances: dict[str, BackpackRawBalance] = {}
        # raw_response_content is known to be a dict here
        for asset_symbol, balance_details in raw_response_content.items():
            # Ensure balance_details is dict before validating
            if not isinstance(balance_details, dict):
                logger.warning(
                    f"[{__name__}] Skipping non-dict balance details for asset "
                    f"'{asset_symbol}' in {context}: {balance_details!r}"
                )
                continue
            try:
                # Ensure asset_symbol is string for the key
                validated_balances[str(asset_symbol)] = BackpackRawBalance.model_validate(
                    balance_details
                )
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"balance details for {asset_symbol}", balance_details
                ) from e
        return validated_balances

    @staticmethod
    def handle_get_positions_response(
        raw_response_content: RawJsonResponse, symbol: str | None
    ) -> list[BackpackRawPosition]:
        """
        Validates the raw response for the Get Positions endpoint.

        Handles a single position dictionary if a symbol is provided,
        or a list of position dictionaries if no symbol is provided.
        """
        context = f"positions ({symbol or 'all'})"
        validated_positions: list[BackpackRawPosition] = []

        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"[{__name__}] Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                position = BackpackRawPosition.model_validate(item)
                if symbol is None or position.symbol == symbol:
                    validated_positions.append(position)
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single position item in {context}", item
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
    ) -> BackpackRawOrder:
        """Validates the raw response for the Place Order endpoint."""
        context = "place order response"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return BackpackRawOrder.model_validate(raw_response_content)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_cancel_order_response(
        raw_response_content: RawJsonResponse, order_id: str, symbol: str
    ) -> bool:
        """Validates the raw response for the Cancel Order endpoint.
        Expects no content on success.
        """
        if raw_response_content not in [None, {}]:
            # If we get content, it might be an error structure or unexpected success data.
            # For Backpack, successful cancel usually returns 200 OK with empty body or {}.
            logger.warning(
                f"Received unexpected content after cancelling order {order_id} "
                f"for {symbol}: {raw_response_content!r}"
            )
        return True

    @staticmethod
    def handle_get_open_orders_response(
        raw_response_content: RawJsonResponse, symbol: str | None
    ) -> list[BackpackRawOrder]:
        """Validates the raw response for the Get Open Orders endpoint."""
        context = f"open orders ({symbol or 'all'})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_orders: list[BackpackRawOrder] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"[{__name__}] Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_orders.append(BackpackRawOrder.model_validate(item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single open order item in {context}", item
                ) from e
        return validated_orders

    @staticmethod
    def handle_get_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRate:
        """Validates the raw response for the Get Funding Rate endpoint."""
        context = f"funding rate ({symbol}) - Status: {status_code}"
        if not isinstance(raw_response_content, dict):
            # Check if it is a list, as HL funding rate is a list
            if isinstance(raw_response_content, list):
                if not raw_response_content:  # Empty list
                    raise APIError(
                        message=(
                            f"Empty list for {context} response, expected dict or non-empty list."
                        ),
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )
                # Assuming the first element is the target if it's a list
                # (adapting for HL-like structures)
                if isinstance(raw_response_content[0], dict):
                    raw_data_to_validate = raw_response_content[0]
                else:
                    raise APIError(
                        message=(
                            f"Unexpected item type in list for {context} response: "
                            f"expected dict, got {type(raw_response_content[0])}"
                        ),
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )
            else:
                raise APIError(
                    message=f"Unexpected {context} response format: expected dict or list, "
                    f"got {type(raw_response_content)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )
        else:
            raw_data_to_validate = raw_response_content

        try:
            return BackpackRawFundingRate.model_validate(raw_data_to_validate)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e, context, raw_data_to_validate
            ) from e

    @staticmethod
    def handle_get_account_info_response(
        raw_response_content: RawJsonResponse,
    ) -> BackpackRawAccountSummary:
        """Validates the raw response for the Get Account Info endpoint."""
        context = "account info"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return BackpackRawAccountSummary.model_validate(raw_response_content)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_withdraw_response(
        raw_response_content: RawJsonResponse,
    ) -> BackpackRawWithdrawalResponse:
        """Validates the raw response for the Withdraw endpoint."""
        context = "withdraw response"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return BackpackRawWithdrawalResponse.model_validate(raw_response_content)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_get_order_history_response(
        raw_response_content: RawJsonResponse, symbol: str | None
    ) -> list[BackpackRawOrder]:
        """Validates the raw response for the Get Order History endpoint."""
        context = f"order history ({symbol or 'all'})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_orders: list[BackpackRawOrder] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"[{__name__}] Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_orders.append(BackpackRawOrder.model_validate(item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single order history item in {context}", item
                ) from e
        return validated_orders

    @staticmethod
    def handle_get_trade_history_response(
        raw_response_content: RawJsonResponse, symbol: str | None
    ) -> list[BackpackRawTrade]:
        """Validates the raw response for the Get Trade History endpoint.

        Now returns list[BackpackRawTrade] as per user request.
        """
        context = f"trade history ({symbol or 'all'})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_items: list[BackpackRawTrade] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"[{__name__}] Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_items.append(BackpackRawTrade.model_validate(item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single trade item in {context}", item
                ) from e
        return validated_items

    @staticmethod
    def handle_get_market_data_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        timeframe: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawKline]:  # Changed return type
        """Validates the raw response for the Get Market Data (Klines) endpoint."""
        context = f"market data (klines {timeframe}) for {symbol} - Status: {status_code}"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_klines: list[BackpackRawKline] = []
        for item_raw in raw_response_content:
            if not isinstance(item_raw, list):  # Backpack klines are lists of values
                logger.warning(
                    f"[{__name__}] Skipping non-list kline item in {context}: {item_raw!r}"
                )
                continue
            try:
                # BackpackRawKline is now imported at module level
                validated_klines.append(BackpackRawKline.model_validate(item_raw))
            except ValidationError as e:
                # Log the specific item that failed validation
                logger.error(
                    f"[{__name__}] Pydantic validation failed for single kline item in "
                    f"{context}: {e}. Item: {item_raw!r}"
                )
                # Re-raise to fail the entire response if one kline is bad, or collect valid ones
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single kline item in {context}", item_raw
                ) from e
            except Exception as e_unk_item:
                logger.error(
                    f"[{__name__}] Unexpected error validating single kline item in "
                    f"{context}: {e_unk_item}. Item: {item_raw!r}"
                )
                raise APIError(
                    message=f"Unexpected error validating kline item: {e_unk_item}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    original_exception=e_unk_item,
                )

        return validated_klines

    @staticmethod
    def handle_get_historical_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawTrade]:
        """Validates the raw response for the Get Historical Trades endpoint."""
        context = f"historical trades ({symbol}) - Status: {status_code}"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_trades: list[BackpackRawTrade] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(f"[{__name__}] Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_trades.append(BackpackRawTrade.model_validate(item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single historical trade item in {context}", item
                ) from e
        return validated_trades

    @staticmethod
    def handle_get_order_status_response(
        raw_response_content: RawJsonResponse, identifier: str
    ) -> BackpackRawOrder:
        """Validates the raw response for the Get Order Status endpoint."""
        context = f"order status (id={identifier})"
        if raw_response_content is None:
            raise APIError(
                f"Order {identifier} not found (empty response).",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
            )

        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return BackpackRawOrder.model_validate(raw_response_content)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_cancel_all_orders_response(
        raw_response_content: RawJsonResponse, symbol: str | None
    ) -> list[BackpackRawOrder]:
        """
        Validates the raw response for the Cancel All Orders endpoint
        (DELETE /api/v1/orders/cancelAll).
        Expects a list of successfully cancelled orders.
        """
        context = f"cancel all orders ({symbol or 'all'})"
        if not isinstance(raw_response_content, list):
            # According to OpenAPI spec, this should be a list of orders that were cancelled.
            # If it's not a list, it might be an error structure or an unexpected empty response.
            # For now, assume an empty list is a valid response if it's not an error.
            # If it's an empty dict {} and success, it might mean "no orders to cancel".
            # However, the spec says `type: array, items: $ref: '#/components/schemas/Order'`
            # Let's strictly expect a list or raise.
            logger.error(
                f"[{__name__}] Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}. Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_response": raw_response_content},  # Pass raw content in metadata
            )

        validated_orders: list[BackpackRawOrder] = []
        for item in raw_response_content:
            if not isinstance(item, dict):
                logger.warning(
                    f"[{__name__}] Skipping non-dict item in {context} list: {item!r}. "
                    f"Full response: {raw_response_content!r}"
                )
                continue  # Skip non-dict items, but don't fail the whole batch

            try:
                validated_orders.append(BackpackRawOrder.model_validate(item))
            except ValidationError as e:
                # Log the specific item that failed validation but continue processing others
                # to return successfully validated items if any.
                # Or, re-raise if strictness is required. For cancelAll, it might be better
                # to return what was successfully parsed as cancelled.
                logger.error(
                    f"[{__name__}] Pydantic validation failed for single order item in "
                    f"{context}: {e}. Item: {item!r}. Full response: {raw_response_content!r}"
                )
                # Optionally, re-raise if any single item failing should invalidate
                # the whole response:
                # raise BackpackResponseHandler._handle_validation_error(
                #     e, f"single order item in {context}", item
                # ) from e
                # For now, we'll be lenient and collect valid ones.
                # Consider if this behavior is desired or if it should be stricter.
        return validated_orders

    @staticmethod
    def handle_transfer_response(
        raw_response_content: RawJsonResponse,
    ) -> RawJsonResponse:  # Returns the validated raw dict
        """Validates the raw response for an internal capital transfer.
        Expects a dict with 'success' (bool), optional 'message' (str), and
        optional 'transferId' (str).
        """
        context = "internal transfer response"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=(
                    f"Unexpected {context} format: expected dict, got {type(raw_response_content)}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        # Basic structure validation
        if "success" not in raw_response_content or not isinstance(
            raw_response_content["success"], bool
        ):
            raise APIError(
                message=(
                    f"Invalid {context}: 'success' field missing or not a boolean. "
                    f"Got: {raw_response_content.get('success')}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        if "message" in raw_response_content and not isinstance(
            raw_response_content["message"], str
        ):
            logger.warning(
                f"[{__name__}] {context} 'message' field is not a string: "
                f"{raw_response_content['message']}"
            )
            # Don't raise, but log. Message is optional and for info.

        if "transferId" in raw_response_content and not isinstance(
            raw_response_content["transferId"], str
        ):
            logger.warning(
                f"[{__name__}] {context} 'transferId' field is not a string: "
                f"{raw_response_content['transferId']}"
            )
            # Don't raise, but log. TransferId is optional and for info.

        # If successful, the mapper will use this dict to create an internal Transfer model.
        # If not successful (success=False), the mapper should handle this appropriately.
        return raw_response_content

    @staticmethod
    def handle_get_current_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRate:
        """Validates the raw response for the Get Current Funding Rate endpoint."""
        context = f"current funding rate ({symbol}) - Status: {status_code}"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            # Assuming BackpackRawFundingRate is the correct model for a single, current rate
            return BackpackRawFundingRate.model_validate(raw_response_content)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e, context, raw_response_content
            ) from e

    @staticmethod
    def handle_get_historical_funding_rates_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawFundingIntervalRate]:
        """Validates the raw response for the Get Historical Funding Rates endpoint
        (/api/v1/fundingRates)."""
        context = f"historical funding rates ({symbol}) - Status: {status_code}"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_rates: list[BackpackRawFundingIntervalRate] = []
        for item_raw in raw_response_content:
            if not isinstance(item_raw, dict):
                logger.warning(
                    f"[{__name__}] Skipping non-dict item in {context} list: {item_raw!r}"
                )
                continue
            try:
                validated_rates.append(BackpackRawFundingIntervalRate.model_validate(item_raw))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single historical funding rate item in {context}", item_raw
                ) from e
        return validated_rates
