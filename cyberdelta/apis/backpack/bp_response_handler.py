"""
Response Handler for Backpack API Raw Responses.

Validates raw JSON data against Pydantic models specific to Backpack\'s API endpoints.
"""

from typing import TypeAlias

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary

# BackpackRawApiError import removed as validation is the focus here. Error mapping is separate.
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
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


# More specific type alias for raw JSON potentially returned by HTTP client
# Base case is dict or list, but can technically be other primitives too.
RawJsonPrim = str | int | float | bool | None
RawJson = dict[str, "RawJson"] | list["RawJson"] | RawJsonPrim
RawJsonResponse: TypeAlias = RawJson  # Use TypeAlias for clarity


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
        raw_response_content: RawJsonResponse, symbol: str
    ) -> BackpackRawTicker:
        """Validates the raw response for the Get Ticker endpoint."""
        context = f"ticker ({symbol})"
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
        raw_response_content: RawJsonResponse, symbol: str
    ) -> BackpackRawOrderBook:
        """Validates the raw response for the Get Order Book endpoint."""
        context = f"order book ({symbol})"
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
        raw_response_content: RawJsonResponse, symbol: str
    ) -> list[BackpackRawTrade]:
        """Validates the raw response for the Get Recent Trades endpoint."""
        context = f"recent trades ({symbol})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_trades: list[BackpackRawTrade] = []
        for item in raw_response_content:
            # Ensure item is a dict before validating
            if not isinstance(item, dict):
                logger.warning(f"[{__name__}] Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_trades.append(BackpackRawTrade.model_validate(item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single trade item in {context}", item
                ) from e
        return validated_trades

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
        """Validates the raw response for the Get Positions endpoint."""
        context = f"positions ({symbol or 'all'})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        validated_positions: list[BackpackRawPosition] = []
        for item in raw_response_content:
            # Ensure item is dict before validating
            if not isinstance(item, dict):
                logger.warning(f"[{__name__}] Skipping non-dict item in {context} list: {item!r}")
                continue
            try:
                validated_positions.append(BackpackRawPosition.model_validate(item))
            except ValidationError as e:
                raise BackpackResponseHandler._handle_validation_error(
                    e, f"single position item in {context}", item
                ) from e
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
        """Validates the raw response for the Cancel Order endpoint (expects no content on success)."""
        if raw_response_content not in [None, {}]:
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
        raw_response_content: RawJsonResponse, symbol: str
    ) -> BackpackRawFundingRate:
        """Validates the raw response for the Get Funding Rate endpoint."""
        context = f"funding rate ({symbol})"
        if not isinstance(raw_response_content, dict):
            raise APIError(
                message=f"Unexpected {context} response format: expected dict, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        try:
            return BackpackRawFundingRate.model_validate(raw_response_content)
        except ValidationError as e:
            raise BackpackResponseHandler._handle_validation_error(
                e, context, raw_response_content
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
        """Validates the raw response for the Get Trade History (fills) endpoint."""
        context = f"trade history ({symbol or 'all'})"
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
                    e, f"single trade history item in {context}", item
                ) from e
        return validated_trades

    @staticmethod
    def handle_get_market_data_response(
        raw_response_content: RawJsonResponse, symbol: str, timeframe: str
    ) -> list[RawJson]:  # Return list of RawJson until specific Kline model exists
        """Validates the raw response for the Get Market Data (Klines) endpoint."""
        context = f"market data (klines {symbol} {timeframe})"
        if not isinstance(raw_response_content, list):
            raise APIError(
                message=f"Unexpected {context} response format: expected list, "
                f"got {type(raw_response_content)}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        # Basic validation: ensure items in list are also lists (typical kline structure)
        validated_klines: list[RawJson] = []
        for item in raw_response_content:
            if not isinstance(item, list):
                logger.warning(f"[{__name__}] Skipping non-list item in {context} list: {item!r}")
                # Depending on strictness, could raise APIError here too
                continue
            validated_klines.append(item)

        # TODO: Add validation against BackpackRawKline model when available
        logger.warning(
            f"Raw kline validation not yet fully implemented for {context}. Returning list."
        )
        return validated_klines

    @staticmethod
    def handle_get_historical_trades_response(
        raw_response_content: RawJsonResponse, symbol: str
    ) -> list[BackpackRawTrade]:
        """Validates the raw response for the Get Historical Trades endpoint (/trades/history)."""
        context = f"historical trades ({symbol})"
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
