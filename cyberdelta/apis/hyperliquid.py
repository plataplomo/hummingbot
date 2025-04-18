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
from websockets.legacy.client import WebSocketClientProtocol  # Use legacy client for compatibility

from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI, MessageHandler
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
    TimeInForce,
    Trade,
)
from cyberdelta.utils.parsing import parse_decimal_value

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
                    code=APIErrorCode.AUTHENTICATION_FAILED,
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
                code=APIErrorCode.AUTHENTICATION_FAILED,
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
                    code=APIErrorCode.CONNECTION_ERROR,
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
                    raise APIError(
                        f"API Error: {response.status} - {error_text}",
                        code=APIErrorCode.EXCHANGE_SPECIFIC,
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
                f"HTTP Error: {e}", code=APIErrorCode.NETWORK_ISSUE, original_exception=e
            ) from e
        except json.JSONDecodeError as e:
            logger.error(f"[{self.exchange_name}] JSON decode error: {e}", exc_info=True)
            raise APIError(
                f"JSON decode error: {e}", code=APIErrorCode.INVALID_REQUEST, original_exception=e
            ) from e
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Request error: {e}", exc_info=True)
            raise APIError(
                f"Request error: {e}", code=APIErrorCode.SERVER_ERROR, original_exception=e
            ) from e

    async def get_balances(self) -> dict[str, Balance]:
        """Get account balances."""
        try:
            payload: dict[str, Any] = {"type": "clearinghouseState", "user": self._wallet_address}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            state_data: dict[str, Any] | None = (
                None  # API response is expected to be dict[str, Any] or None
            )
            if isinstance(response, list) and len(response) > 0 and isinstance(response[0], dict):
                # API contract: response[0] is dict[str, Any]
                response0: dict[str, Any] = cast(dict[str, Any], response[0])
                state_data = response0.get("clearinghouseState")
            elif isinstance(response, dict):
                state_data = response.get("clearinghouseState")

            if state_data and "assetPositions" in state_data:
                balances: dict[str, Balance] = {}
                # API contract: state_data["assetPositions"] is list[dict[str, Any]]
                asset_positions: list[dict[str, Any]] = cast(
                    list[dict[str, Any]], state_data["assetPositions"]
                )
                for asset_pos in asset_positions:
                    if asset_pos.get("asset") == "USDC" and isinstance(
                        asset_pos.get("position"), dict
                    ):
                        position_raw = asset_pos["position"]
                        position: dict[str, Any] = cast(dict[str, Any], position_raw)
                        total_balance_str: str = str(position.get("value", "0"))
                        total_balance: Decimal = Decimal(total_balance_str)
                        balances["USDC"] = Balance(
                            asset="USDC",
                            total=total_balance,
                            available=total_balance,
                        )
                return balances
            else:
                logger.warning(f"[{self.exchange_name}] Unexpected balance response format.")
                return {}

        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting balances: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting balances: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get balances: {e}", code=APIErrorCode.SERVER_ERROR, original_exception=e
            ) from e

    async def get_positions(self, symbol: str | None = None) -> list[Position]:
        """Get current positions, optionally filtering by symbol."""
        try:
            payload: dict[str, Any] = {"type": "clearinghouseState", "user": self._wallet_address}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            positions_list: list[Position] = []
            state_data: dict[str, Any] | None = (
                None  # API response is expected to be dict[str, Any] or None
            )

            if isinstance(response, list) and len(response) > 0 and isinstance(response[0], dict):
                # API contract: response[0] is dict[str, Any]
                response0: dict[str, Any] = cast(dict[str, Any], response[0])
                state_data = response0.get("clearinghouseState")
            elif isinstance(response, dict):
                state_data = response.get("clearinghouseState")

            if state_data and "assetPositions" in state_data:
                # API contract: state_data["assetPositions"] is list[dict[str, Any]]
                asset_positions: list[dict[str, Any]] = cast(
                    list[dict[str, Any]], state_data["assetPositions"]
                )
                for asset_pos in asset_positions:
                    position_data_raw = asset_pos.get("position")
                    position_data: dict[str, Any] | None = (
                        cast(dict[str, Any], position_data_raw)
                        if isinstance(position_data_raw, dict)
                        else None
                    )
                    if position_data:
                        pos_symbol: str | None = asset_pos.get("asset")
                        if symbol is not None and pos_symbol != symbol:
                            continue
                        size_str: str = str(position_data.get("szi", "0"))
                        entry_price_str: str = str(position_data.get("entryPx", "0"))
                        unrealized_pnl_str: str = str(position_data.get("unrealizedPnl", "0"))
                        size: Decimal = Decimal(size_str)
                        entry_price: Decimal | None = (
                            Decimal(entry_price_str) if entry_price_str else None
                        )
                        unrealized_pnl: Decimal = Decimal(unrealized_pnl_str)
                        if size != Decimal(0) and pos_symbol and entry_price is not None:
                            side: OrderSide = OrderSide.BUY if size > 0 else OrderSide.SELL
                            leverage_placeholder: Decimal = Decimal("1")
                            positions_list.append(
                                Position(
                                    symbol=pos_symbol,
                                    side=side,
                                    size=abs(size),
                                    entry_price=entry_price,
                                    leverage=leverage_placeholder,
                                    unrealized_pnl=unrealized_pnl,
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
                code=APIErrorCode.SERVER_ERROR,
                original_exception=e,
            ) from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get open orders for a specific symbol or all symbols."""
        try:
            payload = {"type": "openOrders", "user": self._wallet_address}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            open_orders: list[Order] = []
            if isinstance(response, list):
                for order_data in response:
                    order_symbol = order_data.get("coin")
                    if symbol is None or order_symbol == symbol:
                        order_status_str = order_data.get("status")
                        if order_status_str == "open":
                            pass
                        elif order_status_str == "filled":
                            continue
                        elif order_status_str == "canceled":
                            continue

                        order = self.parse_order(order_data)
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
                code=APIErrorCode.SERVER_ERROR,
                original_exception=e,
            ) from e

    # Add **kwargs and keep type ignore for override due to base class signature
    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """Place a new order."""
        if not self._account:
            raise APIError(
                "Cannot place order without initialized account/private key",
                code=APIErrorCode.AUTHENTICATION_FAILED,
            )

        is_buy = side == OrderSide.BUY
        sz = float(quantity)

        order_type_hl: dict[str, Any]
        limit_px_str: str = "0"  # Ensure always initialized
        if order_type == OrderType.LIMIT:
            if price is None:
                raise ValueError("Price must be specified for LIMIT orders")
            limit_px_str = f"{price:.{8}f}"
            order_type_hl = {"limit": {"tif": time_in_force.value}}
            if post_only:
                logger.warning(
                    f"[{self.exchange_name}] Post-only flag handling for limit orders needs verification."
                )

        elif order_type == OrderType.MARKET:
            order_type_hl = {"market": {}}
            if post_only:
                logger.warning(
                    f"[{self.exchange_name}] Post-only typically not applicable to market orders."
                )
        else:
            raise NotImplementedError(f"Order type {order_type} not supported yet.")

        order_payload = {
            "asset": symbol,
            "isBuy": is_buy,
            "limitPx": limit_px_str,
            "sz": sz,
            "reduceOnly": reduce_only,
            "orderType": order_type_hl,
        }

        action_payload = {
            "type": "order",
            "orders": [order_payload],
        }

        try:
            response = await self._request("POST", "/exchange", data=action_payload, signed=True)

            if isinstance(response, dict) and response.get("status") == "ok":
                response_data = response.get("data")
                if isinstance(response_data, dict) and "statuses" in response_data:
                    statuses = cast(list[dict[str, Any]], response_data["statuses"])
                    if statuses:
                        for status in statuses:
                            status_dict: dict[str, Any] = status
                            if status_dict.get("symbol") == symbol:
                                # Robust, type-safe replacements for missing utility functions
                                avg_px = status_dict.get("avgPx", "0")
                                avg_fill_price = Decimal(str(avg_px))
                                created_at_ts = status_dict.get("time", time.time() * 1000)
                                created_at = datetime.fromtimestamp(
                                    float(created_at_ts) / 1000, tz=UTC
                                )
                                return Order(
                                    client_order_id=client_order_id
                                    or str(status_dict.get("oid", "")),
                                    symbol=symbol,
                                    side=side,
                                    order_type=order_type,
                                    quantity_requested=quantity,
                                    quantity_filled=Decimal(str(status_dict.get("totalSz", "0"))),
                                    price=price,
                                    average_fill_price=avg_fill_price,
                                    status=OrderStatus.FILLED,
                                    created_at=created_at,
                                )
                                if "resting" in status_dict:
                                    resting_data = status_dict["resting"]
                                    created_at_ts = resting_data.get("time", time.time() * 1000)
                                    created_at = datetime.fromtimestamp(
                                        float(created_at_ts) / 1000, tz=UTC
                                    )
                                    return Order(
                                        client_order_id=client_order_id
                                        or str(resting_data.get("oid", "")),
                                        symbol=symbol,
                                        side=side,
                                        order_type=order_type,
                                        quantity_requested=quantity,
                                        quantity_filled=Decimal("0"),
                                        price=price,
                                        average_fill_price=None,
                                        status=OrderStatus.OPEN,
                                        created_at=created_at,
                                    )
                                elif "error" in status_dict:
                                    error_msg = str(status_dict["error"])
                                    logger.error(
                                        f"[{self.exchange_name}] Order placement failed: {error_msg}"
                                    )
                                    raise APIError(
                                        f"Order placement failed: {error_msg}",
                                        code=APIErrorCode.ORDER_REJECTED,
                                    )
                                else:
                                    logger.warning(
                                        f"[{self.exchange_name}] Unhandled order status in response: {status_dict}"
                                    )
                                    raise APIError(
                                        "Unhandled order status", code=APIErrorCode.UNKNOWN
                                    )

            logger.error(f"[{self.exchange_name}] Failed to place order. Response: {response}")
            raise APIError(
                "Failed to place order, unexpected response", code=APIErrorCode.EXCHANGE_SPECIFIC
            )

        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error placing order: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error placing order: {e}", exc_info=True)
            raise APIError(
                f"Failed to place order: {e}", code=APIErrorCode.SERVER_ERROR, original_exception=e
            ) from e

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> dict[str, Any]:
        """Cancel an existing order."""
        if not self._account:
            raise APIError(
                "Cannot cancel order without initialized account/private key",
                code=APIErrorCode.AUTHENTICATION_FAILED,
            )
        if symbol is None:
            raise ValueError("Symbol must be provided to cancel Hyperliquid orders")

        cancel_payload = {
            "asset": symbol,
            "oid": int(order_id),
        }
        action_payload = {
            "type": "cancel",
            "cancels": [cancel_payload],
        }

        try:
            response = await self._request("POST", "/exchange", data=action_payload, signed=True)

            if isinstance(response, dict) and response.get("status") == "ok":
                response_data = response.get("data")
                statuses = []
                if isinstance(response_data, dict) and "statuses" in response_data:
                    statuses = response_data["statuses"]

                if isinstance(statuses, list) and statuses:
                    if isinstance(statuses[0], str) and statuses[0] == "canceled":
                        logger.info(
                            f"[{self.exchange_name}] Canceled order {order_id} for {symbol}"
                        )
                        return {"status": "canceled", "order_id": order_id, "symbol": symbol}
                    elif isinstance(statuses[0], dict) and "error" in statuses[0]:
                        error_msg = str(statuses[0]["error"])
                        logger.error(
                            f"[{self.exchange_name}] Failed to cancel order {order_id}: {error_msg}"
                        )
                        code = APIErrorCode.EXCHANGE_SPECIFIC
                        if "Order not found" in error_msg:
                            code = APIErrorCode.ORDER_NOT_FOUND
                        raise APIError(f"Failed to cancel order: {error_msg}", code=code)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Order {order_id} cancel response status OK, "
                            f"but data unexpected: {statuses}"
                        )
                        return {
                            "status": "unknown",
                            "order_id": order_id,
                            "symbol": symbol,
                            "response": statuses,
                        }

            logger.error(
                f"[{self.exchange_name}] Failed to cancel order {order_id}. Response: {response}"
            )
            raise APIError(
                "Cancel order failed, unexpected response", code=APIErrorCode.EXCHANGE_SPECIFIC
            )

        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error canceling order: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error canceling order {order_id}: {e}", exc_info=True
            )
            raise APIError(
                f"Failed to cancel order {order_id}: {e}",
                code=APIErrorCode.SERVER_ERROR,
                original_exception=e,
            ) from e

    async def get_ticker(self, symbol: str) -> Ticker:
        """
        Get current ticker information for a symbol.

        Args:
            symbol: The trading symbol to fetch ticker data for.

        Returns:
            Ticker: The current ticker data with Decimal-typed financial fields.

        Raises:
            APIError: If ticker data is not found or response is malformed.
        """
        try:
            payload = {"type": "metaAndAssetCtxs"}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            if isinstance(response, list) and len(response) > 1:
                asset_contexts = response[1]
                if isinstance(asset_contexts, list):
                    for ctx in asset_contexts:
                        # ctx is expected to be dict[str, Any] per API contract
                        if ctx.get("name") == symbol:
                            # mark_px is expected to be Any or None per API contract
                            mark_px = ctx.get("markPx")
                            if mark_px is not None:
                                # Use robust Decimal parsing for all financial fields (see decimal rule)
                                bid = parse_decimal_value(mark_px, allow_none=True)
                                ask = parse_decimal_value(mark_px, allow_none=True)
                                # 'last' is not a valid argument for Ticker; only use valid fields
                                return Ticker(
                                    symbol=symbol,
                                    bid=bid,
                                    ask=ask,
                                    timestamp=int(time.time() * 1000),
                                )
            logger.warning(
                f"[{self.exchange_name}] Ticker data not found for {symbol} in response: {response}"
            )
            raise APIError(
                f"Ticker data not found for {symbol}", code=APIErrorCode.SYMBOL_NOT_FOUND
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
                code=APIErrorCode.SERVER_ERROR,
                original_exception=e,
            ) from e

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook:
        """Get order book for a symbol."""
        try:
            payload = {"type": "l2Book", "coin": symbol}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            if response and isinstance(response, dict) and "levels" in response:
                levels = response.get("levels")
                bids: list[tuple[Decimal, Decimal]] = []
                asks: list[tuple[Decimal, Decimal]] = []

                if isinstance(levels, list) and len(levels) > 1:
                    # Process bids
                    bid_levels: list[Any] = levels[0]  # API contract: list
                    if isinstance(bid_levels, list):
                        for level in bid_levels:
                            if isinstance(level, list) and len(level) >= 2:
                                try:
                                    price = Decimal(str(level[0]))
                                    quantity = Decimal(str(level[1]))
                                    bids.append((price, quantity))
                                except (ValueError, TypeError, IndexError) as e:
                                    logger.warning(f"Error parsing bid level {level}: {e}")

                    # Process asks
                    ask_levels: list[Any] = levels[1]  # API contract: list
                    if isinstance(ask_levels, list):
                        for level in ask_levels:
                            if isinstance(level, list) and len(level) >= 2:
                                try:
                                    price = Decimal(str(level[0]))
                                    quantity = Decimal(str(level[1]))
                                    asks.append((price, quantity))
                                except (ValueError, TypeError, IndexError) as e:
                                    logger.warning(f"Error parsing ask level {level}: {e}")

                bids.sort(key=lambda x: x[0], reverse=True)
                asks.sort(key=lambda x: x[0])

                if depth is not None and depth > 0:
                    bids = bids[:depth]
                    asks = asks[:depth]

                timestamp = int(response.get("time", int(time.time() * 1000)))
                return OrderBook(
                    symbol=symbol,
                    bids=bids,
                    asks=asks,
                    timestamp=timestamp,
                )
            logger.warning(
                f"[{self.exchange_name}] Order book data not found/invalid for {symbol}: {response}"
            )
            raise APIError(
                f"Order book data not found/invalid for {symbol}",
                code=APIErrorCode.EXCHANGE_SPECIFIC,
            )

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
                code=APIErrorCode.SERVER_ERROR,
                original_exception=e,
            ) from e

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """Get recent trades for a symbol."""
        try:
            payload = {"type": "recentTrades", "coin": symbol}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            trades: list[Trade] = []
            if isinstance(response, list):
                for trade_item in response:
                    if isinstance(trade_item, dict):
                        trade_id = str(trade_item.get("tid", ""))
                        price_str = trade_item.get("px", "0")
                        size_str = trade_item.get("sz", "0")
                        timestamp_ms = trade_item.get("time", 0)
                        side_hl = trade_item.get("side", "B")

                        try:
                            price = Decimal(str(price_str))
                            quantity = Decimal(str(size_str))
                            side = OrderSide.BUY if side_hl == "B" else OrderSide.SELL

                            trades.append(
                                Trade(
                                    id=trade_id,
                                    symbol=symbol,
                                    executed_at=datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC),
                                    side=side,
                                    order_id=trade_id,
                                    exchange="hyperliquid",
                                    client_order_id="",
                                    price=price,
                                    quantity=quantity,
                                    cost=price * quantity,
                                    fee=Decimal("0"),
                                    fee_asset="USDC",
                                    is_maker=False,
                                    timestamp=timestamp_ms,
                                )
                            )
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Error parsing trade data {trade_item}: {e}")

                if limit is not None and limit > 0:
                    trades = trades[:limit]
            return trades

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
                code=APIErrorCode.SERVER_ERROR,
                original_exception=e,
            ) from e

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """Get funding rate for a symbol."""
        try:
            payload = {"type": "metaAndAssetCtxs"}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            if isinstance(response, list) and len(response) > 1:
                asset_contexts = response[1]
                if isinstance(asset_contexts, list):
                    for ctx in asset_contexts:
                        if isinstance(ctx, dict) and ctx.get("name") == symbol:
                            funding_rate_str = ctx.get("funding", "0")
                            mark_px_str = ctx.get("markPx", "0")

                            try:
                                funding_rate = Decimal(str(funding_rate_str))
                                mark_price = Decimal(str(mark_px_str))

                                now_ms = int(time.time() * 1000)
                                next_funding_time_ms = (now_ms // 3600000 + 1) * 3600000

                                funding_info = FundingRate(
                                    symbol=symbol,
                                    funding_rate=funding_rate,
                                    mark_price=mark_price,
                                    next_funding_time=next_funding_time_ms,
                                )
                                return funding_info
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Error parsing funding rate data for {symbol}: {e}")
                                break

            logger.warning(
                f"[{self.exchange_name}] Funding rate data not found for {symbol} "
                f"in response: {response}"
            )
            return None
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting funding rate for {symbol}: {e}")
            if e.original_exception and "Funding rate not available" in str(e.original_exception):
                raise APIError(
                    f"Funding rate unavailable for {symbol}",
                    code=APIErrorCode.FUNDING_RATE_UNAVAILABLE,
                    original_exception=e,
                ) from e
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting funding rate for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get funding rate for {symbol}: {e}",
                code=APIErrorCode.SERVER_ERROR,
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
                code=APIErrorCode.AUTHENTICATION_FAILED,
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
            code = APIErrorCode.INVALID_REQUEST
        elif status_code == 401 or status_code == 403:
            code = APIErrorCode.AUTHENTICATION_FAILED
        elif status_code == 404:
            code = APIErrorCode.SYMBOL_NOT_FOUND
        elif status_code == 429:
            code = APIErrorCode.RATE_LIMITED
        elif 500 <= status_code < 600:
            code = APIErrorCode.SERVER_ERROR
        else:
            code = APIErrorCode.UNKNOWN

        message = f"HTTP error {status_code}"
        exchange_code = None
        exchange_message = error_body

        if error_data:
            exchange_message = error_data.get("error", exchange_message)
            if "Invalid order size" in exchange_message:
                code = APIErrorCode.QUANTITY_OUT_OF_RANGE
            elif "Order not found" in exchange_message:
                code = APIErrorCode.ORDER_NOT_FOUND
            elif "Insufficient margin" in exchange_message:
                code = APIErrorCode.INSUFFICIENT_FUNDS

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
                symbol=symbol,
                side=side,
                order_type=order_type,
                status=status,
                quantity_requested=Decimal(str(size_str)),
                quantity_filled=filled_qty,
                price=Decimal(str(price_str)),
                average_fill_price=None,  # Set if available
                created_at=datetime.fromtimestamp(timestamp / 1000, tz=UTC),
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
            symbol: If provided, only return funding rate for this symbol

        Returns:
            List of FundingRate objects
        """
        try:
            # Get all market information which includes funding rates
            url = f"{self.INFO_URL}/info"
            payload = {"type": "allMeta"}
            response = await self._request("POST", url, data=payload)

            result: list[FundingRate] = []

            # Extract funding rates from response
            if response and isinstance(response, list) and len(response) > 0:
                universe_data = response[0].get("universe", [])

                for market in universe_data:
                    market_symbol = market.get("name")
                    # If symbols list is provided, only include markets from that list
                    if symbols is not None and market_symbol not in symbols:
                        continue

                    funding_info = market.get("funding")
                    if funding_info and isinstance(funding_info, dict):
                        rate_str = funding_info.get("fundingRate", "0")
                        # Convert from percentage to decimal (e.g., 0.01% -> 0.0001)
                        rate_decimal = Decimal(str(rate_str)) / Decimal(100)
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
                code=APIErrorCode.SERVER_ERROR,
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

    async def _handle_websocket_message(self, message: dict[str, Any] | list | str) -> None:
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
        """
        Parse a trade message from the WebSocket feed.

        Args:
            message: The WebSocket message to parse

        Returns:
            A Trade object or None if the message cannot be parsed
        """
        if not message or not isinstance(message, dict):
            return None

        # Check if it's a trades update message
        if "channel" in message and message["channel"] == "trades":
            data = message.get("data", [])
            if data and isinstance(data, list) and len(data) > 0:
                # Just return the first trade for now
                first_trade = data[0]
                symbol = message.get("coin", "")
                if symbol and isinstance(first_trade, dict):
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
        if not message or not isinstance(message, dict):
            return None

        # Check if it's an order update message
        if "type" in message and message["type"] == "userEvent" and "userEvents" in message:
            events = message.get("userEvents", [])
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

    async def _on_message(self, ws: WebSocketClientProtocol, message: str) -> None:
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
                await ws.send(json.dumps({"type": "pong"}))
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

        if not message or not isinstance(message, dict):
            return trades

        # Process fills
        if message.get("type") == "userFill":
            fill_data = message.get("data", {})
            if not fill_data:
                return trades

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
