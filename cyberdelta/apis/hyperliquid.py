import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, TypedDict, TypeVar, cast

import aiohttp
from aiohttp import ClientTimeout
from eth_account.messages import encode_typed_data
from web3.auto import w3

# from websockets import WebSocketClientProtocol  # Use modern API for compatibility
from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI, MessageHandler
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidMapper
from cyberdelta.core.models import (
    Balance,
    FundingRate,
    MarketData,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Ticker,
    Trade,
)

logger = logging.getLogger(__name__)

# Type definitions for Hyperliquid responses
T = TypeVar("T")


class AssetContext(TypedDict, total=False):
    name: str
    markPx: str
    funding: str


class TradeData(TypedDict, total=False):
    tid: int | str
    px: str
    sz: str
    time: int
    side: str


class OrderStatusData(TypedDict, total=False):
    filled: dict[str, Any]
    resting: dict[str, Any]
    error: str


class OrderResponseData(TypedDict, total=False):
    status: str
    data: dict[str, Any]


class OrderBookResponse(TypedDict, total=False):
    levels: list[list[list[int | float | str]]]
    time: int


class HyperliquidAPI(ExchangeAPI):
    """API Client for Hyperliquid DEX."""

    BASE_URL = "https://api.hyperliquid.xyz"
    INFO_URL = "https://info.hyperliquid.xyz"
    WS_URL = "wss://api.hyperliquid.xyz/ws"
    CHAIN_ID = 1337  # Hyperliquid L1 chain ID (adjust if necessary)

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        """
        Initialize the HyperliquidAPI client.

        Args:
            api_config: Configuration dictionary with connection parameters
            secrets: Dictionary containing private_key and wallet_address
        """
        # Set base URLs or use supplied ones
        self.rest_endpoint = api_config.get("rest_endpoint", self.BASE_URL)
        self.ws_endpoint = api_config.get("ws_endpoint", self.WS_URL)

        # Initialize API from base class
        super().__init__(
            exchange_name="hyperliquid",
            config={
                "rest_endpoint": self.rest_endpoint,
                "ws_endpoint": self.ws_endpoint,
                "rate_limits": api_config.get("rate_limits", {}),
            },
            secrets=secrets,
        )

        # Setup default headers
        self.default_headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Setup callback handlers
        self.trade_callback: MessageHandler | None = None
        self.order_update_callback: MessageHandler | None = None
        self.fill_callback: MessageHandler | None = None
        self.orderbook_callback: MessageHandler | None = None

        # Nonce for request signatures
        self._nonce_counter: int = 0
        self._nonce_lock = asyncio.Lock()

        # Get wallet address from secrets
        self._wallet_address = secrets.get("wallet_address")
        self._private_key = secrets.get("private_key")

        # Session for HTTP requests
        self._session: aiohttp.ClientSession | None = None

        # Validate required credentials
        if not self._wallet_address:
            raise ValueError("Wallet address is required for HyperliquidAPI")

        if not self._private_key:
            logger.warning("Private key not provided, only public endpoints will be available")

        # Generate auth signature if we have the private key
        if self._private_key:
            try:
                # Ensure private key is in the expected format
                if self._private_key.startswith("0x"):
                    self._private_key = self._private_key[2:]

                # Create account from private key
                self._account = w3.eth.account.from_key(self._private_key)

                # Verify wallet address matches the account address
                derived_address = self._account.address.lower()
                if self._wallet_address.lower() != derived_address:
                    logger.warning(
                        f"Provided wallet address {self._wallet_address} "
                        f"does not match derived address {derived_address}"
                    )

            except Exception as e:
                logger.error(f"Error initializing Ethereum account: {e}")
                raise ValueError(f"Invalid private key: {e}") from e

        self.session: aiohttp.ClientSession | None = None
        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_lock = asyncio.Lock()
        self._symbol_map: dict[str, str] = {}
        self._ws_handlers: dict[str, MessageHandler] = {}
        self._is_connected = False

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Sign a request using EIP-712 and wallet private key.

        Args:
            method: HTTP method
            path: API endpoint
            params: URL parameters (dictionary)
            data: Request body (dictionary)

        Returns:
            Authentication data for the request
        """
        if not self._account:
            if "test" in path or (data and data.get("test")):
                logger.info(
                    f"[{self.exchange_name}] Using mock authentication for test environment "
                    f"(no private key)"
                )
                timestamp_str = str(int(time.time() * 1000))
                nonce_str = "12345"
                signature = "0x" + "0" * 130  # Mock signature
                return {
                    "headers": {
                        "X-HL-Signature": signature,
                        "X-HL-Timestamp": timestamp_str,
                        "X-HL-Nonce": nonce_str,
                    },
                    "params": params,
                    "data": data,
                }
            else:
                raise APIError(
                    "Private key not provided for signing",
                    code=APIErrorCode.AUTHENTICATION_FAILED.value,
                )

        async with self._nonce_lock:
            self._nonce_counter += 1
            nonce = self._nonce_counter

        timestamp = int(time.time() * 1000)

        # Separate the type definitions from the data to be signed
        eip712_types = {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Agent": [
                {"name": "source", "type": "string"},
                {"name": "connectionId", "type": "bytes32"},
            ],
        }

        # Data structure for signing (domain and message)
        structured_data_to_sign = {
            "types": eip712_types,
            "primaryType": "Agent",
            "domain": {
                "name": "Hyperliquid",
                "version": "1",
                "chainId": self.CHAIN_ID,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": {
                "source": "aix",
                "connectionId": b"\x00" * 32,
                "timestamp": timestamp,
            },
        }

        try:
            # Pass the structured data including types to encode_typed_data
            signable_message = encode_typed_data(full_message=structured_data_to_sign)
            signed_message = self._account.sign_message(signable_message)
            signature = signed_message.signature.hex()
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error signing message: {e}", exc_info=True)
            raise APIError(
                "Failed to sign message",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

        return {
            "headers": {
                "X-HL-Signature": signature,
                "X-HL-Timestamp": str(timestamp),
                "X-HL-Nonce": str(nonce),
            },
            "params": params,
            "data": data,
        }

    # --- WebSocket Implementation --- #

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Route incoming WebSocket messages."""
        channel = message.get("channel")
        data = message.get("data")
        if not channel or not data:
            logger.debug(f"[{self.exchange_name}] Received unroutable message: {message}")
            return

        handler = self._ws_handlers.get(channel)
        if handler:
            try:
                await handler(data)
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Error in handler for channel {channel}: {e}",
                    exc_info=True,
                )
        else:
            logger.debug(f"[{self.exchange_name}] No handler registered for channel: {channel}")

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Subscribe to a WebSocket topic."""
        self._ws_handlers[topic] = handler

        if self.ws_connection and self._is_connected:
            try:
                await self.ws_connection.send_json({"method": "subscribe", "subscription": topic})
                logger.info(f"[{self.exchange_name}] Subscribed to {topic}")
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Error subscribing to {topic}: {e}", exc_info=True
                )

    async def _resubscribe(self) -> None:
        """Resubscribe to all registered topics upon reconnection."""
        logger.info(
            f"[{self.exchange_name}] Resubscribing to topics: {list(self._ws_handlers.keys())}"
        )
        handlers_copy = self._ws_handlers.copy()
        for topic, handler in handlers_copy.items():
            await self.subscribe(topic, handler)
            await asyncio.sleep(0.1)

    # --- REST API Implementation --- #

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        signed: bool = False,
        retry_count: int = 3,
        timeout: float = 30.0,
    ) -> dict[str, Any] | list[Any] | str | None:
        """
        Make a request to the API with proper error handling and type annotations.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path
            params: Query parameters for the request (dictionary)
            data: JSON data to send in the request body (dictionary)
            headers: Additional headers to include (dictionary)
            signed: Whether the request requires authentication
            retry_count: Maximum number of retry attempts
            timeout: Request timeout in seconds

        Returns:
            The parsed JSON response as a dict, list, string or None

        Raises:
            APIError: If an error occurs during the request
        """
        try:
            # Construct full URL
            url = (
                path
                if path.startswith(("http://", "https://"))
                else f"{self.rest_endpoint}/{path.lstrip('/')}"
            )

            # Prepare headers
            full_headers: dict[str, str] = {}
            if hasattr(self, "default_headers"):
                default_headers: dict[str, str] = self.default_headers
                full_headers.update(default_headers)
            if headers:
                full_headers.update(headers)

            # Ensure session is available
            if not self._session:
                raise APIError(
                    "HTTP session not initialized. Call connect() first.",
                    code=APIErrorCode.CONNECTION_ERROR.value,
                )

            # Create a proper ClientTimeout object
            timeout_obj = ClientTimeout(total=timeout)

            async with self._session.request(
                method, url, params=params, json=data, headers=full_headers, timeout=timeout_obj
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        f"[{self.exchange_name}] API error: {response.status} - {error_text}"
                    )
                    # Try to parse as JSON and use HyperliquidMapper.map_error_response if possible
                    try:
                        error_json: dict[str, Any] = json.loads(error_text)
                        if "error" in error_json:
                            from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidMapper

                            raise HyperliquidMapper.map_error_response(
                                error_json, http_status=response.status
                            )
                    except Exception:
                        pass
                    # Fallback: raise generic APIError
                    raise APIError(
                        f"API Error: {response.status} - {error_text}",
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                    )

                if response.content_type == "application/json":
                    json_data: dict[str, Any] | list[Any] = await response.json()
                    if isinstance(json_data, dict):
                        return json_data
                    # If not a dict, must be a list (by type annotation), so just return
                    return json_data
                else:
                    return await response.text()

        except aiohttp.ClientError as e:
            logger.error(f"[{self.exchange_name}] HTTP error: {e}", exc_info=True)
            raise APIError(
                f"HTTP Error: {e}", code=APIErrorCode.NETWORK_ISSUE.value, original_exception=e
            ) from e
        except json.JSONDecodeError as e:
            logger.error(f"[{self.exchange_name}] JSON decode error: {e}", exc_info=True)
            raise APIError(
                f"JSON decode error: {e}",
                code=APIErrorCode.INVALID_REQUEST.value,
                original_exception=e,
            ) from e
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Request error: {e}", exc_info=True)
            raise APIError(
                f"Request error: {e}", code=APIErrorCode.SERVER_ERROR.value, original_exception=e
            ) from e

    async def get_balances(self) -> dict[str, Balance]:
        """Get account balances."""
        try:
            payload: dict[str, Any] = {"type": "clearinghouseState", "user": self._wallet_address}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)
            # --- User State (clearinghouseState) ---
            from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
                HyperliquidRawClearinghouseState,
            )

            # Validate and parse the clearinghouse state response
            validated = HyperliquidRawClearinghouseState.model_validate(response)
            state_data = validated  # The validated object is the clearinghouse state
            balances: dict[str, Balance] = {}
            if state_data and state_data.asset_positions:
                for asset_pos in state_data.asset_positions:
                    if asset_pos.asset == "USDC" and asset_pos.position:
                        total_balance = asset_pos.position.position_value
                        total_balance_dec = Decimal(total_balance)
                        balances["USDC"] = Balance(
                            asset="USDC",
                            total=total_balance_dec,
                            available=total_balance_dec,
                        )
            return balances
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting balances: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting balances: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get balances: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_positions(self, symbol: str | None = None) -> list[Position]:
        """Get current positions, optionally filtering by symbol."""
        try:
            payload: dict[str, Any] = {"type": "clearinghouseState", "user": self._wallet_address}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)
            # --- User State (clearinghouseState) ---
            from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
                HyperliquidRawClearinghouseState,
            )

            validated = HyperliquidRawClearinghouseState.model_validate(response)
            state_data = validated
            positions_list: list[Position] = []
            if state_data and state_data.asset_positions:
                for asset_pos in state_data.asset_positions:
                    position_data = asset_pos.position
                    if position_data:
                        pos_symbol: str | None = asset_pos.asset
                        if symbol is not None and pos_symbol != symbol:
                            continue
                        size = position_data.szi
                        entry_price = position_data.entry_px
                        unrealized_pnl = position_data.unrealized_pnl
                        # Defensive conversion to Decimal for all numeric fields
                        if entry_price is not None:
                            try:
                                size_dec = Decimal(size)
                                entry_price_dec = Decimal(entry_price)
                                unrealized_pnl_dec = Decimal(unrealized_pnl)
                            except Exception:
                                # Log or handle conversion error, skip this position
                                continue
                            if size_dec != 0 and pos_symbol:
                                side: OrderSide = OrderSide.BUY if size_dec > 0 else OrderSide.SELL
                                leverage_placeholder: Decimal = Decimal("1")
                                positions_list.append(
                                    Position(
                                        symbol=pos_symbol,
                                        side=side,
                                        size=abs(size_dec),
                                        entry_price=entry_price_dec,
                                        leverage=leverage_placeholder,
                                        unrealized_pnl=unrealized_pnl_dec,
                                        timestamp=int(time.time() * 1000),
                                    )
                                )
            return positions_list
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting positions: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting positions: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get positions: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get open orders for a specific symbol or all symbols."""
        try:
            payload = {"type": "openOrders", "user": self._wallet_address}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)
            # --- Open Orders ---
            from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
                HyperliquidRawOpenOrdersResponse,
            )

            validated = HyperliquidRawOpenOrdersResponse.model_validate(response)
            open_orders: list[Order] = []
            for order_obj in validated.__root__:
                order_data = order_obj.order
                order_symbol = order_data.asset
                if symbol is None or order_symbol == symbol:
                    order_status_str = order_data.status
                    if order_status_str == "open":
                        pass
                    elif order_status_str == "filled":
                        continue
                    elif order_status_str == "canceled":
                        continue
                    order = self.parse_order(order_data.model_dump())
                    if order:
                        open_orders.append(order)
            return open_orders
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting open orders: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting open orders: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get open orders: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_ticker(self, symbol: str) -> Ticker:
        """
        Get current ticker information for a symbol.
        """
        try:
            payload = {"type": "metaAndAssetCtxs"}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)
            from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
                HyperliquidRawMetaAndAssetCtxsResponse,
            )

            validated = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(response)
            for asset_ctx in validated.asset_ctxs:
                if asset_ctx.name == symbol:
                    return HyperliquidMapper.map_raw_ctx_to_ticker(asset_ctx.model_dump())
            logger.warning(
                f"[{self.exchange_name}] Ticker data not found for {symbol} in response: {response}"
            )
            raise APIError(
                f"Ticker data not found for {symbol}",
                code=APIErrorCode.SYMBOL_NOT_FOUND.value,
            )
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting ticker for {symbol}: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting ticker for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get ticker for {symbol}: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook:
        """
        Get order book for a symbol.
        """
        try:
            payload = {"type": "l2Book", "coin": symbol}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)
            from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
                HyperliquidRawL2Book,
            )

            validated = HyperliquidRawL2Book.model_validate(response)
            return HyperliquidMapper.map_raw_order_book(symbol, validated.model_dump(), depth)
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting order book for {symbol}: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting order book for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get order book for {symbol}: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """
        Get recent trades for a symbol.
        """
        try:
            payload = {"type": "recentTrades", "coin": symbol}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)
            from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
                HyperliquidRawRecentTradesResponse,
            )

            validated = HyperliquidRawRecentTradesResponse.model_validate(response)
            trade_dicts = [t.model_dump() for t in validated.__root__]
            return HyperliquidMapper.map_raw_trades(symbol, trade_dicts, limit)
        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API Error getting recent trades for {symbol}: {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting recent trades for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get recent trades for {symbol}: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """
        Get funding rate for a symbol.
        """
        try:
            payload = {"type": "metaAndAssetCtxs"}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)
            from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
                HyperliquidRawMetaAndAssetCtxsResponse,
            )

            validated = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(response)
            asset_ctx = next((ctx for ctx in validated.asset_ctxs if ctx.name == symbol), None)
            if asset_ctx is None:
                logger.warning(f"[{self.exchange_name}] No asset context found for {symbol}.")
                return None
            return HyperliquidMapper.map_raw_ctx_to_funding_rate(
                symbol, asset_ctx.model_dump(by_alias=True)
            )
        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API Error getting funding rate for {symbol}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting funding rate for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get funding rate for {symbol}: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    # --- Optional/Advanced Endpoints --- #

    async def transfer(
        self, asset: str, amount: Decimal, from_account: str, to_account: str
    ) -> dict[str, Any]:
        logger.warning(
            f"[{self.exchange_name}] Transfer function called, verify Hyperliquid support/mapping."
        )
        raise NotImplementedError(
            "Transfer functionality needs specific Hyperliquid implementation."
        )

    async def withdraw(
        self, asset: str, amount: Decimal, address: str, network: str | None = None
    ) -> dict[str, Any]:
        if not self._account:
            raise APIError(
                "Cannot withdraw without initialized account/private key",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        logger.warning(
            f"[{self.exchange_name}] Withdrawal endpoint requires careful implementation "
            f"and testing."
        )
        raise NotImplementedError(
            "Withdrawal functionality needs specific Hyperliquid implementation."
        )

    def _map_error_response(
        self, status_code: int, error_body: str, error_data: dict[str, Any] | None = None
    ) -> APIError:
        """Map HTTP errors and Hyperliquid specific errors to standard APIErrorCode."""
        if status_code == 400:
            code = APIErrorCode.INVALID_REQUEST.value
        elif status_code == 401 or status_code == 403:
            code = APIErrorCode.AUTHENTICATION_FAILED.value
        elif status_code == 404:
            code = APIErrorCode.SYMBOL_NOT_FOUND.value
        elif status_code == 429:
            code = APIErrorCode.RATE_LIMITED.value
        elif 500 <= status_code < 600:
            code = APIErrorCode.SERVER_ERROR.value
        else:
            code = APIErrorCode.UNKNOWN.value

        message = f"HTTP error {status_code}"
        exchange_code = None
        exchange_message = error_body

        if error_data:
            exchange_message = error_data.get("error", exchange_message)
            if "Invalid order size" in exchange_message:
                code = APIErrorCode.QUANTITY_OUT_OF_RANGE.value
            elif "Order not found" in exchange_message:
                code = APIErrorCode.ORDER_NOT_FOUND.value
            elif "Insufficient margin" in exchange_message:
                code = APIErrorCode.INSUFFICIENT_FUNDS.value

        return APIError(
            message=message,
            code=code,
            http_status=status_code,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
        )

    # --- WebSocket Subscription Helpers --- #

    async def subscribe_to_order_updates(self, handler: MessageHandler) -> None:
        """Subscribe to user order updates."""
        await self.subscribe("user", handler)

    async def subscribe_to_trades(self, symbol: str, handler: MessageHandler | None = None) -> None:
        """Subscribe to trades for a symbol."""
        # In the implementation we can maintain our own handler registry
        if not hasattr(self, "_trade_handlers"):
            self._trade_handlers: dict[str, list[MessageHandler]] = {}

        # Actual subscription logic would go here
        logger.info(f"Subscribed to trades for {symbol}")

    async def subscribe_to_ticker(self, symbol: str, handler: MessageHandler | None = None) -> None:
        """Subscribe to ticker updates for a symbol."""
        # In the implementation we can maintain our own handler registry
        if not hasattr(self, "_ticker_handlers"):
            self._ticker_handlers: dict[str, list[MessageHandler]] = {}

        # Actual subscription logic would go here
        logger.info(f"Subscribed to ticker updates for {symbol}")

    async def subscribe_to_order_book(
        self, symbol: str, handler: MessageHandler | None = None
    ) -> None:
        """Subscribe to order book updates for a symbol."""
        # In the implementation we can maintain our own handler registry
        if not hasattr(self, "_orderbook_handlers"):
            self._orderbook_handlers: dict[str, list[MessageHandler]] = {}

        # Actual subscription logic would go here
        logger.info(f"Subscribed to order book updates for {symbol}")

    # --- Helper Methods (Parsing, etc.) ---

    def parse_order(self, data: dict[str, Any]) -> Order:
        """
        Parse exchange-specific order format to standard Order object.

        Args:
            data: Exchange-specific order data (dictionary)

        Returns:
            Standardized Order object
        """
        if not data:
            raise ValueError("Invalid order data provided")
        try:
            order_id = str(data.get("oid", ""))
            if not order_id:
                raise ValueError("Order ID missing from order data")
            symbol = data.get("coin", "")
            if not symbol:
                raise ValueError("Symbol missing from order data")
            side_str = data.get("side", "B")
            type_str = data.get("orderType", "limit").lower()
            status_str = data.get("status", "open").lower()
            price_str = data.get("limitPx", "0")
            size_str = data.get("sz", "0")
            remaining_str = data.get("remainingSz", size_str)
            filled_qty = (
                Decimal(str(size_str)) - Decimal(str(remaining_str))
                if remaining_str
                else Decimal("0")
            )
            timestamp = data.get("time", int(time.time() * 1000))
            side = OrderSide.BUY if side_str == "B" else OrderSide.SELL
            order_type = OrderType.LIMIT
            if type_str == "market":
                order_type = OrderType.MARKET
            elif type_str == "postonly":
                order_type = OrderType.LIMIT  # Post-only is a flag, not an order type
            status = OrderStatus.OPEN
            if status_str == "filled":
                status = OrderStatus.FILLED
            elif status_str in ["cancelled", "canceled"]:
                status = OrderStatus.CANCELED
            elif status_str == "rejected":
                status = OrderStatus.REJECTED
            elif Decimal(str(remaining_str)) < Decimal(str(size_str)):
                status = OrderStatus.PARTIALLY_FILLED
            return Order(
                client_order_id=data.get("cloid", order_id),
                exchange_order_id=order_id,
                related_order_id=None,
                symbol=symbol,
                side=side,
                order_type=order_type,
                status=status,
                quantity_requested=Decimal(str(size_str)),
                quantity_filled=filled_qty,
                price=Decimal(str(price_str)),
                average_fill_price=None,  # Set if available
                created_at=datetime.fromtimestamp(timestamp / 1000, tz=UTC),
                exchange="hyperliquid",
                executed_quote_quantity=None,
                trigger_by=None,
                self_trade_prevention=None,
                updated_at=None,
                triggered_at=None,
                expiry_reason=None,
                origin=None,
                strategy_name=None,
                signal_id=None,
            )
        except Exception as e:
            logger.error(f"Error parsing order data: {e}")
            raise ValueError(f"Failed to parse order: {e}") from e

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        raise NotImplementedError("get_order not implemented for HyperliquidAPI")

    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        raise NotImplementedError("cancel_all_orders not implemented for HyperliquidAPI")

    async def get_order_history(self, symbol: str | None = None, limit: int = 100) -> list[Order]:
        raise NotImplementedError("get_order_history not implemented for HyperliquidAPI")

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        raise NotImplementedError("get_trade_history not implemented for HyperliquidAPI")

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """
        Get current funding rates, optionally filtering by symbol.

        Args:
            symbols: If provided, only return funding rate for these symbols.

        Returns:
            List of FundingRate objects.
        """
        try:
            # Get all market information which includes funding rates
            url = f"{self.INFO_URL}/info"
            payload = {"type": "allMeta"}
            response = await self._request("POST", url, data=payload)

            result: list[FundingRate] = []

            # Extract funding rates from response
            if response and isinstance(response, list) and len(response) > 0:
                universe_data: list[dict[str, Any]] = response[0].get("universe", [])

                for market in universe_data:
                    market_symbol: str = market.get("name", "")
                    # If symbols list is provided, only include markets from that list
                    if symbols is not None and market_symbol not in symbols:
                        continue

                    funding_info: dict[str, Any] | None = market.get("funding")
                    if funding_info is not None:
                        rate_str: str = str(funding_info.get("fundingRate", "0"))
                        # Convert from percentage to decimal (e.g., 0.01% -> 0.0001)
                        rate_decimal = Decimal(rate_str) / Decimal(100)
                        timestamp = int(time.time() * 1000)  # Current time in milliseconds

                        funding_rate = FundingRate(
                            symbol=market_symbol,
                            funding_rate=rate_decimal,
                            timestamp=timestamp,
                        )
                        result.append(funding_rate)

            return result

        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting funding rates: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting funding rates: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get funding rates: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_market_data(
        self, symbol: str, timeframe: str, limit: int = 100
    ) -> list[MarketData]:
        raise NotImplementedError(
            "get_market_data (kline/OHLCV) not implemented for HyperliquidAPI"
        )

    async def connect_websocket(self) -> None:
        await self._connect_ws()

    async def _handle_websocket_message(self, message: dict[str, Any] | list[Any] | str) -> None:
        # Accepts dict, list, or str; list[Any] is explicit for linter
        if isinstance(message, dict):
            await self._route_ws_message(message)
        else:
            logger.debug(f"[{self.exchange_name}] Received non-dict WS message: {type(message)}")

    async def subscribe_to_account_updates(self) -> None:
        raise NotImplementedError("subscribe_to_account_updates needs handler implementation")

    def parse_ticker(self, data: dict[str, Any], symbol: str) -> Ticker:
        raise NotImplementedError("parse_ticker needs implementation")

    def parse_order_book(self, data: dict[str, Any], symbol: str) -> OrderBook:
        raise NotImplementedError("parse_order_book needs implementation")

    def parse_trade(self, data: dict[str, Any], symbol: str) -> Trade:
        raise NotImplementedError("parse_trade needs implementation")

    def parse_balance(self, data: dict[str, Any]) -> Balance:
        raise NotImplementedError("parse_balance needs implementation")

    def parse_position(self, data: dict[str, Any]) -> Position:
        raise NotImplementedError("parse_position needs implementation")

    def parse_funding_rate(self, data: dict[str, Any]) -> FundingRate:
        raise NotImplementedError("parse_funding_rate needs implementation")

    async def ping_websocket(self) -> None:
        if self._ws_connection and not self._ws_connection.closed:
            try:
                await self._ws_connection.ping()
                logger.debug(f"[{self.exchange_name}] Sent WebSocket ping")
            except Exception as e:
                logger.warning(f"[{self.exchange_name}] Failed to send WebSocket ping: {e}")

    def get_message_type(self, message: dict[str, Any]) -> str:
        channel = message.get("channel", "unknown")
        return str(channel) if channel is not None else "unknown"

    def parse_ticker_message(self, message: dict[str, Any]) -> Ticker | None:
        """Parse ticker message from WebSocket."""
        if message.get("channel") == "allMids":
            logger.warning(
                f"[{self.exchange_name}] parse_ticker_message needs specific implementation "
                f"for 'allMids' structure."
            )
            return None
        return None

    def parse_orderbook_message(self, message: dict[str, Any]) -> OrderBook | None:
        if message.get("channel", "").startswith("l2Book:"):
            logger.warning(
                f"[{self.exchange_name}] parse_orderbook_message needs full implementation."
            )
            return None
        return None

    def parse_trade_message(self, message: dict[str, Any]) -> Trade | None:
        """Parse trade message from the WebSocket feed."""
        if message.get("channel") == "allMids":
            logger.warning(
                f"[{self.exchange_name}] parse_ticker_message needs specific implementation "
                f"for 'allMids' structure."
            )
            return None
        # Defensive: message is already dict[str, Any]
        if "channel" in message and message["channel"] == "trades":
            data_raw = message.get("data", [])
            data_raw_any: list[Any] = data_raw  # Explicitly typed for static analysis
            # Defensive: ensure data_raw is a list of dicts
            if all(isinstance(x, dict) for x in data_raw_any):
                data = cast(list[dict[str, Any]], data_raw_any)
                if data:
                    first_trade: dict[str, Any] = data[0]
                    symbol = message.get("coin", "")
                    if symbol:
                        try:
                            return self.parse_trade(first_trade, symbol)
                        except Exception as e:
                            logger.error(f"Error parsing trade message: {e}")
        return None

    def parse_order_update_message(self, message: dict[str, Any]) -> Order | None:
        """
        Parse an order update message from the WebSocket feed.

        Args:
            message: The WebSocket message to parse

        Returns:
            An Order object or None if the message cannot be parsed
        """
        # Defensive: message is already dict[str, Any]
        if "type" in message and message["type"] == "userEvent" and "userEvents" in message:
            events_raw = message.get("userEvents", [])
            events_raw_any: list[Any] = events_raw
            # Defensive: ensure events_raw is a list of dicts
            if all(isinstance(x, dict) for x in events_raw_any):
                events = cast(list[dict[str, Any]], events_raw_any)
                for event in events:
                    event_type = event.get("eventType", "")
                    if event_type == "order":
                        order_data = event.get("data", {})
                        if order_data:
                            try:
                                return self.parse_order(order_data)
                            except Exception as e:
                                logger.error(f"Error parsing order update message: {e}")
        return None

    def parse_funding_rate_message(self, message: dict[str, Any]) -> FundingRate | None:
        logger.warning(f"[{self.exchange_name}] parse_funding_rate_message needs WS update impl.")
        return None

    async def _on_message(self, ws: "aiohttp.ClientWebSocketResponse", message: str) -> None:
        """Handle WebSocket messages.

        Args:
            ws: The WebSocket connection
            message: The message received from the WebSocket
        """
        if not message:
            return

        try:
            data = json.loads(message)
            message_type = self.get_message_type(data)

            if message_type == "ping":
                # Respond to ping message
                await ws.send_str(json.dumps({"type": "pong"}))
                return

            if message_type == "orderbook":
                # Parse and forward orderbook updates
                orderbook = self.parse_orderbook_message(data)
                if orderbook and self.orderbook_callback:
                    # Convert OrderBook to dict to match callback signature
                    await self.orderbook_callback({"orderbook": orderbook})
                return

            if message_type == "trade":
                # Parse and forward trade updates
                trade = self.parse_trade_message(data)
                if trade and self.trade_callback:
                    # Convert Trade to dict to match callback signature
                    await self.trade_callback({"trade": trade})
                return

            if message_type == "order_update":
                # Parse and forward order updates
                order = self.parse_order_update_message(data)
                if order and self.order_update_callback:
                    # Convert Order to dict to match callback signature
                    await self.order_update_callback({"order": order})
                return

            if message_type == "fill":
                # Parse and forward fill updates (trade executions)
                fill_trades = self.parse_fill_message(data)
                if fill_trades and self.fill_callback:
                    for trade in fill_trades:
                        # Convert Trade to dict to match callback signature
                        await self.fill_callback({"trade": trade})
                return

            # Log unhandled message types
            logger.debug(f"[{self.exchange_name}] Unhandled WebSocket message type: {message_type}")

        except json.JSONDecodeError:
            logger.warning(
                f"[{self.exchange_name}] Received invalid JSON message: {message[:100]}..."
            )
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error processing WebSocket message: {e}", exc_info=True
            )

    def parse_fill_message(self, message: dict[str, Any]) -> list[Trade]:
        """Parse a fill update message and convert to standard Trade objects.

        Args:
            message: The message containing fill updates

        Returns:
            List of Trade objects representing the fills
        """
        trades: list[Trade] = []

        # Defensive: message is already dict[str, Any]
        if message.get("type") == "userFill":
            fill_data_raw = message.get("data", {})
            # Defensive: ensure fill_data_raw is a dict
            if isinstance(fill_data_raw, dict):
                fill_data: dict[str, Any] = fill_data_raw
                coin = fill_data.get("coin", "")
                if not coin:
                    return trades

                side_str = fill_data.get("side", "")
                px_str = fill_data.get("px", "0")
                sz_str = fill_data.get("sz", "0")
                time_ms = fill_data.get("time", int(time.time() * 1000))
                order_id = fill_data.get("oid", "")

                try:
                    side = OrderSide.BUY if side_str == "B" else OrderSide.SELL
                    price = Decimal(str(px_str))
                    quantity = Decimal(str(sz_str))

                    trade = Trade(
                        id=f"{order_id}_{time_ms}",
                        symbol=coin,
                        executed_at=datetime.fromtimestamp(time_ms / 1000, tz=UTC),
                        side=side,
                        order_id=order_id,
                        exchange="hyperliquid",
                        client_order_id="",
                        price=price,
                        quantity=quantity,
                        cost=price * quantity,
                        fee=Decimal("0"),
                        fee_asset="USDC",
                        is_maker=False,  # Assuming fills are taker trades
                        timestamp=time_ms,
                    )
                    trades.append(trade)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing fill data: {e}")
        return trades
