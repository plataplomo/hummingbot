"""Response Handler for Hyperliquid API Raw Responses.

This module serves as a legacy adapter that combines the decomposed response handlers
for backward compatibility. New code should import the specific handlers directly.
"""

from collections.abc import Mapping

from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
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
    HyperliquidRawOrderStatusResponse,
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
from cyberdelta.apis.hyperliquid.response_handlers.hl_account_response_handler import (
    HyperliquidAccountResponseHandler,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_market_data_response_handler import (
    HyperliquidMarketDataResponseHandler,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_trading_response_handler import (
    HyperliquidTradingResponseHandler,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import exchanges
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# Type alias for raw JSON response from HTTP client
# Aligned with ParsedJsonResponse from http_client.py
type RawJsonResponse = ParsedJsonResponse


class HyperliquidResponseHandler:
    """Legacy adapter that combines decomposed response handlers for backward compatibility.

    This class maintains the same interface as the original monolithic response handler
    but delegates to the appropriate decomposed handlers internally.
    """

    def __init__(self) -> None:
        """Initialize the adapter with all decomposed handlers."""
        self._account_handler = HyperliquidAccountResponseHandler()
        self._market_handler = HyperliquidMarketDataResponseHandler()
        self._trading_handler = HyperliquidTradingResponseHandler()

    # Account-related methods
    def handle_info_user_state_response(
        self,
        raw_response_content: ParsedJsonResponse,
        user_address: str,
        status_code: int,
    ) -> HyperliquidRawUserStateResponse:
        """Delegate to account handler.

        Args:
            raw_response_content: Raw JSON response from the API
            user_address: User's wallet address
            status_code: HTTP status code

        Returns:
            Validated user state response
        """
        return self._account_handler.handle_info_user_state_response(
            raw_response_content,
            user_address,
            status_code,
        )

    def handle_info_vault_details_response(
        self,
        raw_response_content: RawJsonResponse,
        user_address: str,
        status_code: int,
    ) -> HyperliquidRawVaultDetailsResponse:
        """Delegate to account handler.

        Args:
            raw_response_content: Raw JSON response from the API
            user_address: User's wallet address (unused by handler)
            status_code: HTTP status code

        Returns:
            Validated vault details response
        """
        return self._account_handler.handle_info_vault_details_response(
            raw_response_content,
            status_code,
        )

    def handle_info_open_orders_response(
        self,
        raw_response_content: RawJsonResponse,
        user_address: str,
        status_code: int,
    ) -> HyperliquidRawOpenOrdersResponse:
        """Delegate to account handler.

        Args:
            raw_response_content: Raw JSON response from the API
            user_address: User's wallet address (unused by handler)
            status_code: HTTP status code

        Returns:
            Validated open orders response
        """
        return self._account_handler.handle_info_open_orders_response(
            raw_response_content,
            status_code,
        )

    def handle_info_user_fills_response(
        self,
        raw_response_content: ParsedJsonResponse,
        user_address: str,
        status_code: int,
    ) -> HyperliquidRawUserFillsResponse:
        """Delegate to account handler.

        Args:
            raw_response_content: Raw JSON response from the API
            user_address: User's wallet address (unused by handler)
            status_code: HTTP status code

        Returns:
            Validated user fills response
        """
        return self._account_handler.handle_info_user_fills_response(
            raw_response_content,
            status_code,
        )

    def handle_historical_orders_response(
        self,
        raw_response_content: RawJsonResponse,
        user_address: str,
    ) -> list[HyperliquidRawHistoricalOrderResponse]:
        """Delegate to account handler and return items.

        Args:
            raw_response_content: Raw JSON response from the API
            user_address: User's wallet address

        Returns:
            List of validated historical order responses
        """
        return self._account_handler.handle_historical_orders_response(
            raw_response_content,
            user_address,
        )

    # Market data methods
    def handle_info_meta_and_asset_ctxs_response(
        self,
        raw_response_content: RawJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Delegate to market data handler.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code (optional)
            headers: Response headers (optional)

        Returns:
            Validated meta and asset contexts response
        """
        return self._market_handler.handle_info_meta_and_asset_ctxs_response(
            raw_response_content,
            status_code,
            headers,
        )

    def handle_info_funding_rate_response(
        self,
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
    ) -> HyperliquidRawAssetCtx:
        """Delegate to market data handler.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol (unused by handler)
            status_code: HTTP status code

        Returns:
            Validated asset context with funding rate
        """
        return self._market_handler.handle_info_funding_rate_response(
            raw_response_content,
            status_code,
        )

    def handle_info_l2_book_response(
        self,
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawOrderBookResponse:
        """Delegate to market data handler.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol
            status_code: HTTP status code (optional)
            headers: Response headers (optional)

        Returns:
            Validated L2 order book response
        """
        symbol_obj = exchanges.hyperliquid(symbol)
        return self._market_handler.handle_info_l2_book_response(
            raw_response_content,
            symbol_obj,
            status_code,
            headers,
        )

    def handle_info_recent_trades_response(
        self,
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawPublicTrade]:
        """Delegate to market data handler and return items.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol
            status_code: HTTP status code (optional)
            headers: Response headers (optional)

        Returns:
            List of validated recent public trades
        """
        return self._market_handler.handle_info_recent_trades_response(
            raw_response_content,
            symbol,
            status_code,
            headers,
        )

    def handle_info_candle_snapshot_response(
        self,
        raw_response_content: RawJsonResponse,
        symbol: str,
        interval: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawCandleSnapshot:
        """Delegate to market data handler.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol
            interval: Candle interval
            status_code: HTTP status code (optional)
            headers: Response headers (optional)

        Returns:
            Validated candle snapshot
        """
        symbol_obj = exchanges.hyperliquid(symbol)
        return self._market_handler.handle_info_candle_snapshot_response(
            raw_response_content,
            symbol_obj,
            interval,
            status_code,
            headers,
        )

    def handle_all_mids_response(
        self,
        raw_response_content: RawJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawAllMids:
        """Delegate to market data handler.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code (optional)
            headers: Response headers (optional)

        Returns:
            Validated all mid prices response
        """
        return self._market_handler.handle_all_mids_response(
            raw_response_content,
            status_code,
            headers,
        )

    def handle_historical_funding_rates_response(
        self,
        raw_response_content: RawJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawFundingHistoryItem]:
        """Delegate to market data handler and return items.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code (optional)
            headers: Response headers (optional)

        Returns:
            List of validated funding history items
        """
        response = self._market_handler.handle_historical_funding_rates_response(
            raw_response_content,
            status_code,
            headers,
        )
        return response.items

    def handle_info_spot_asset_contexts_response(
        self,
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> list[HyperliquidRawAssetCtx]:
        """Spot asset contexts - not yet implemented in decomposed handlers."""
        # TODO: Implement when spot asset models are available
        raise NotImplementedError("Spot asset contexts not yet implemented")

    # Trading methods
    def handle_exchange_response(
        self,
        raw_response_content: ParsedJsonResponse,
        action_type: str,
        status_code: int,
    ) -> HyperliquidRawExchangeResponse:
        """Delegate to trading handler.

        Args:
            raw_response_content: Raw JSON response from the API
            action_type: Type of exchange action (unused by handler)
            status_code: HTTP status code

        Returns:
            Validated exchange response
        """
        return self._trading_handler.handle_exchange_response(raw_response_content, status_code)

    def handle_info_order_status_response(
        self,
        raw_response_content: RawJsonResponse,
        user_address: str,
        order_id: int,
    ) -> HyperliquidRawOrderStatusResponse:
        """Delegate to trading handler.

        Args:
            raw_response_content: Raw JSON response from the API
            user_address: User's wallet address (unused by handler)
            order_id: Order ID (unused by handler)

        Returns:
            Validated order status response
        """
        return self._trading_handler.handle_info_order_status_response(raw_response_content)
