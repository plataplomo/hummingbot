import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp
from aiohttp import ClientTimeout
from eth_account.messages import encode_typed_data
from pydantic import ValidationError
from web3.auto import w3

# from websockets import WebSocketClientProtocol  # Use modern API for compatibility
from cyberdelta.apis.base_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidMapper, HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.hl_ws_mapper import HyperliquidWebsocketMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawTriggerInfo,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.models.enums import OrderStatus
from cyberdelta.core.models.market import Candle
from cyberdelta.utils.parsing import parse_decimal_value

logger = logging.getLogger(__name__)


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
            The parsed JSON response as a dict, list, string or None.

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
                    response_data: dict[str, Any] | list[Any] = await response.json()
                    return response_data
                else:
                    text_response: str = await response.text()
                    return text_response

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

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances using the user state endpoint and mapper.
        NOTE: Currently relies on mapper logic that assumes accountValue is USDC total.
        """
        try:
            payload: dict[str, Any] = {"type": "clearinghouseState", "user": self._wallet_address}
            response: object = await self._request("POST", self.INFO_URL + "/info", data=payload)

            from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
                HyperliquidRawClearinghouseState,
            )

            # Validate the entire state response
            validated_state = HyperliquidRawClearinghouseState.model_validate(response)

            # Use mapper to extract balances (and other state parts, though ignored here)
            _positions, _orders, _margins, spot_balances_dict = HyperliquidMapper.map_user_state(
                validated_state
            )

            # Return the dictionary of SpotBalance objects
            return spot_balances_dict

        except ValidationError as e:
            logger.error(f"[{self.exchange_name}] Error validating clearinghouseState: {e}")
            raise APIError(
                f"Failed to validate balance data structure: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except (ValueError, TypeError, InvalidOperation) as e:
            # Catch parsing/mapping errors from the mapper
            logger.error(
                f"[{self.exchange_name}] Error parsing/mapping user state for balances: {e}"
            )
            raise APIError(
                f"Failed to parse balance data: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except APIError as e:
            # Re-raise APIErrors from _request or mapper
            logger.error(f"[{self.exchange_name}] API Error getting balances: {e}")
            raise e
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_balances: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error fetching balances: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Get current positions using the user state endpoint and mapper."""
        try:
            payload: dict[str, Any] = {"type": "clearinghouseState", "user": self._wallet_address}
            response: object = await self._request("POST", self.INFO_URL + "/info", data=payload)

            from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
                HyperliquidRawClearinghouseState,
            )

            validated_state = HyperliquidRawClearinghouseState.model_validate(response)

            # Use mapper to extract positions
            positions_dict, _orders, _margins, _balances = HyperliquidMapper.map_user_state(
                validated_state
            )

            # Convert dict of positions to list, filtering by symbol if needed
            positions_list: list[DerivativePosition] = list(positions_dict.values())
            if symbol:
                positions_list = [p for p in positions_list if p.symbol == symbol]

            return positions_list

        except (ValidationError, ValueError) as e:  # Catch validation/mapping errors
            logger.error(
                f"[{self.exchange_name}] Error parsing/mapping user state for positions: {e}"
            )
            raise APIError(
                f"Failed to parse position data: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting positions: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting positions: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting positions: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get open orders for a specific symbol or all symbols."""
        try:
            payload = {"type": "openOrders", "user": self._wallet_address}
            response: object = await self._request("POST", self.INFO_URL + "/info", data=payload)
            # --- Open Orders ---
            from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
                HyperliquidRawOpenOrdersResponse,
                HyperliquidRawOrder,
            )

            validated: HyperliquidRawOpenOrdersResponse = (
                HyperliquidRawOpenOrdersResponse.model_validate(response)
            )
            open_orders: list[Order] = []
            for order_obj in validated.items:
                order_data: HyperliquidRawOrder = order_obj.order
                order_symbol: str = order_data.asset
                if symbol is None or order_symbol == symbol:
                    # Extract trigger info (might be None)
                    trigger_info: HyperliquidRawTriggerInfo | None = getattr(
                        order_obj, "trigger", None
                    )
                    # Use the mapper function
                    order = HyperliquidOrderMapper.transform_raw_order_to_internal(
                        raw=order_data, trigger=trigger_info
                    )
                    if order:
                        # Filter for open statuses after transformation
                        if order.status in [
                            OrderStatus.OPEN,
                            OrderStatus.PARTIALLY_FILLED,
                            OrderStatus.NEW,
                        ]:
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
            response: object = await self._request("POST", self.INFO_URL + "/info", data=payload)
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
            response: object = await self._request("POST", self.INFO_URL + "/info", data=payload)
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
            response: object = await self._request("POST", self.INFO_URL + "/info", data=payload)
            from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
                HyperliquidRawRecentTradesResponse,
            )

            validated: HyperliquidRawRecentTradesResponse = (
                HyperliquidRawRecentTradesResponse.model_validate(response)
            )
            trade_dicts: list[dict[str, Any]] = [t.model_dump() for t in validated.items]
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
            response: object = await self._request("POST", self.INFO_URL + "/info", data=payload)
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

    async def connect_websocket(self) -> None:
        """(Placeholder) Connect to the WebSocket endpoint."""

    async def _handle_websocket_message(self, message: dict[str, Any] | list[Any] | str) -> None:
        # Accepts dict, list, or str; list[Any] is explicit for linter
        if isinstance(message, dict):
            await self._route_ws_message(message)
        else:
            logger.debug(f"[{self.exchange_name}] Received non-dict WS message: {type(message)}")

    async def subscribe_to_account_updates(self) -> None:
        raise NotImplementedError("subscribe_to_account_updates needs handler implementation")

    async def ping_websocket(self) -> None:
        if self._ws_connection and not self._ws_connection.closed:
            try:
                await self._ws_connection.ping()
                logger.debug(f"[{self.exchange_name}] Sent WebSocket ping")
            except Exception as e:
                logger.warning(f"[{self.exchange_name}] Failed to send WebSocket ping: {e}")

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
                orderbook = HyperliquidWebsocketMapper.parse_orderbook_message(data, logger)
                if orderbook and self.orderbook_callback:
                    # Convert OrderBook to dict to match callback signature
                    await self.orderbook_callback({"orderbook": orderbook})
                elif orderbook and not self.orderbook_callback:
                    logger.warning(
                        f"[{self.exchange_name}] Received orderbook update but no "
                        f"orderbook_callback is set."
                    )
                return

            if message_type == "trade":
                # Parse and forward trade updates
                trade = HyperliquidWebsocketMapper.parse_trade_message(data, logger)
                if trade and self.trade_callback:
                    # Convert Trade to dict to match callback signature
                    await self.trade_callback({"trade": trade})
                elif trade and not self.trade_callback:
                    logger.warning(
                        f"[{self.exchange_name}] Received trade update but no "
                        f"trade_callback is set."
                    )
                return

            if message_type == "order_update":
                # Parse and forward order updates
                order = HyperliquidWebsocketMapper.parse_order_update_message(
                    data, logger, self.parse_order
                )
                if order and self.order_update_callback:
                    # Convert Order to dict to match callback signature
                    await self.order_update_callback({"order": order})
                elif order and not self.order_update_callback:
                    logger.warning(
                        f"[{self.exchange_name}] Received order update but no "
                        f"order_update_callback is set."
                    )
                return

            if message_type == "fill":
                # Parse and forward fill updates (trade executions)
                fill_trades = HyperliquidWebsocketMapper.parse_fill_message(data, logger)
                if fill_trades and self.fill_callback:
                    for trade in fill_trades:
                        # Convert Trade to dict to match callback signature
                        await self.fill_callback({"trade": trade})
                elif fill_trades and not self.fill_callback:
                    logger.warning(
                        f"[{self.exchange_name}] Received fill update but no fill_callback is set."
                    )
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

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        """
        Required by ExchangeAPI base class. Not implemented for HyperliquidAPI.
        """
        raise NotImplementedError("get_order not implemented for HyperliquidAPI")

    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        """
        Required by ExchangeAPI base class. Not implemented for HyperliquidAPI.
        """
        raise NotImplementedError("cancel_all_orders not implemented for HyperliquidAPI")

    async def get_order_history(self, symbol: str | None = None, limit: int = 100) -> list[Order]:
        """
        Required by ExchangeAPI base class. Not implemented for HyperliquidAPI.
        """
        raise NotImplementedError("get_order_history not implemented for HyperliquidAPI")

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Fetch user trade history (fills) using the 'userFills' info endpoint."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for fetching trade history",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        response_raw: object = None  # Initialize
        try:
            payload = {"type": "userFills", "user": self._wallet_address}
            response_raw = await self._request("POST", self.INFO_URL + "/info", data=payload)

            # DEFENSIVE CHECK: Ensure response is list
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected userFills response type: "
                    f"{type(response_raw)}. Expected list. Returning empty list."
                )
                return []

            trades: list[Trade] = []
            # Parse and transform each fill
            for fill_data_raw in response_raw:
                if not isinstance(fill_data_raw, dict):
                    logger.warning(f"Skipping non-dict item in userFills list: {fill_data_raw}")
                    continue
                try:
                    # Validate with Raw model
                    raw_fill = HyperliquidRawFill.model_validate(fill_data_raw)
                    # Filter by symbol if requested AFTER validation
                    if symbol is not None and raw_fill.coin != symbol:
                        continue
                    # Transform using mapper
                    internal_trade = HyperliquidMapper.transform_raw_fill_to_internal(raw_fill)
                    trades.append(internal_trade)

                except (ValidationError, ValueError) as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping fill due to validation/transformation "
                        f"error: {e}. Data: {fill_data_raw}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing user fill: {e}. "
                        f"Data: {fill_data_raw}",
                        exc_info=True,
                    )
                    continue

            # Apply limit after fetching and filtering
            # Sorting by time (descending) before limiting might be desirable
            trades.sort(key=lambda t: t.executed_at, reverse=True)
            return trades[:limit]

        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting trade history: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting trade history: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting trade history: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """
        Get current funding rates, optionally filtering by symbol.
        Uses the 'allMeta' endpoint and validates/transforms via Raw models and Mapper.

        Args:
            symbols: If provided, only return funding rate for these symbols.

        Returns:
            List of FundingRate objects.
        """
        try:
            # Get all market information which includes funding rates
            url = f"{self.INFO_URL}/info"
            payload = {"type": "allMeta"}
            response_raw: object = await self._request("POST", url, data=payload)

            # DEFENSIVE CHECK: Runtime check before validation
            if (
                not isinstance(response_raw, list)
                or len(response_raw) != 1
                or not isinstance(response_raw[0], dict)
            ):
                raise APIError(
                    f"Unexpected response structure for allMeta: {type(response_raw)}",
                    code=APIErrorCode.UNKNOWN.value,
                )

            from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
                HyperliquidRawMetaResponse,
            )

            validated_meta = HyperliquidRawMetaResponse.model_validate(response_raw[0])

            result: list[FundingRate] = []
            for asset_def in validated_meta.universe:
                market_symbol: str = asset_def.name
                # If symbols list is provided, only include markets from that list
                if symbols is not None and market_symbol not in symbols:
                    continue

                # Use mapper to transform asset definition to FundingRate
                # Note: This assumes asset_def contains funding info.
                # The mapper function handles missing keys or parsing errors.
                funding_rate = HyperliquidMapper.transform_raw_asset_def_to_funding_rate(
                    asset_def.model_dump()  # Pass the dict representation
                )

                if funding_rate:
                    result.append(funding_rate)

            return result

        except (ValidationError, ValueError) as e:  # Catch validation/mapping errors
            logger.error(
                f"[{self.exchange_name}] Error parsing/mapping allMeta response "
                f"for funding rates: {e}"
            )
            raise APIError(
                f"Failed to parse funding rate data: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting funding rates: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting funding rates: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting funding rates: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_market_data(
        self, symbol: str, timeframe: str, limit: int = 100
    ) -> list[Candle]:  # Type hint should now work
        raise NotImplementedError(
            "get_market_data (kline/OHLCV) not implemented for HyperliquidAPI"
        )

    def _parse_spot_balance(self, balance_data: dict[str, Any]) -> SpotBalance | None:
        """Parse raw spot balance data into a SpotBalance object."""
        try:
            asset = balance_data.get("coin")
            total_qty_str = balance_data.get("total")
            available_qty_str = balance_data.get("available")

            if not asset or total_qty_str is None or available_qty_str is None:
                logger.warning(f"Skipping spot balance due to missing fields: {balance_data}")
                return None

            # Parse quantities to Decimal
            total_quantity = parse_decimal_value(
                total_qty_str, allow_none=False, field_name=f"{asset}_total"
            )
            available_quantity = parse_decimal_value(
                available_qty_str, allow_none=False, field_name=f"{asset}_available"
            )

            if total_quantity is None or available_quantity is None:
                logger.warning(f"Skipping spot balance due to invalid quantity: {balance_data}")
                return None  # Skip if parsing failed

            # Assuming no specific hl_details for spot balance for now
            # Need to import HyperliquidSpotBalanceDetails if used
            # from cyberdelta.core.models import HyperliquidSpotBalanceDetails
            hl_details = None

            # Correct instantiation
            return SpotBalance(
                exchange=self.exchange_name,
                asset=str(asset),  # Ensure asset is string
                timestamp=datetime.now(UTC),  # Use current time as timestamp is not in raw data
                total_quantity=total_quantity,  # Pass parsed Decimal
                available_quantity=available_quantity,  # Pass parsed Decimal
                hl_details=hl_details,
            )
        except (ValidationError, ValueError, TypeError, InvalidOperation) as e:
            logger.error(f"Error parsing spot balance item: {e}. Data: {balance_data}")
            return None

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
        """Place an order on Hyperliquid."""
        # TODO: Map internal enums/types to Hyperliquid action payload format
        # This requires knowing the asset index, isBuy, limitPx format, sz format, etc.
        # Placeholder for the action payload structure
        action_payload: dict[str, Any] = {
            # Map parameters to Hyperliquid specific fields (e.g., asset index, isBuy)
            # ... implementation needed ...
            "cloid": client_order_id,  # Assuming cloid mapping
        }
        request_data = {"type": "order", "action": action_payload}

        response_raw: object = None  # Initialize
        try:
            response_raw = await self._request(
                "POST", self.rest_endpoint + "/exchange", data=request_data, signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            if validated_response.status != "ok" or not validated_response.data:
                raise APIError(
                    f"Order placement failed on exchange: {validated_response.model_dump()}",
                    code=APIErrorCode.ORDER_REJECTED.value,
                )

            # Process statuses to find the resulting order/fill info
            if validated_response.data.statuses:
                first_status = validated_response.data.statuses[0]
                if isinstance(first_status, HyperliquidRawExchangeStatusObject):
                    if first_status.resting:
                        # TODO: Map HyperliquidRawExchangeStatusResting back to internal Order
                        # Requires reverse mapping or fetching order status separately
                        logger.info(f"Order resting: {first_status.resting.oid}")
                        # Placeholder - ideally return mapped Order
                        return await self.get_order_status(str(first_status.resting.oid))
                    elif first_status.filled:
                        # TODO: Map HyperliquidRawExchangeStatusFilled back to internal Order/Trade
                        logger.info(f"Order filled immediately: {first_status.filled.oid}")
                        # Placeholder - ideally return mapped Order or Trade info
                        return await self.get_order_status(str(first_status.filled.oid))
                    elif first_status.error:
                        raise APIError(
                            f"Order placement error: {first_status.error}",
                            code=APIErrorCode.ORDER_REJECTED.value,
                        )

            # Fallback if status isn't helpful or structure unexpected
            logger.warning(f"Order placement status unclear: {validated_response.data.statuses}")
            # Maybe fetch order status using cloid if available?
            raise APIError("Order placement status unclear", code=APIErrorCode.UNKNOWN.value)

        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Failed to validate order placement response: {e}. "
                f"Raw: {response_raw}"
            )
            raise APIError(
                f"Invalid response after placing order: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error placing order: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error placing order: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error placing order: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def cancel_order(
        self, order_id: str, symbol: str | None = None
    ) -> dict[str, Any]:  # Keep return type for now
        """Cancel an existing order."""
        if not symbol:
            raise ValueError("Symbol is required to cancel Hyperliquid orders")
        # TODO: Map symbol to asset index
        asset_index = 0  # Placeholder
        action_payload = {"asset": asset_index, "oid": int(order_id)}
        request_data = {"type": "cancel", "action": action_payload}

        response_raw: object = None  # Initialize
        try:
            response_raw = await self._request(
                "POST", self.rest_endpoint + "/exchange", data=request_data, signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            if validated_response.status != "ok" or not validated_response.data:
                raise APIError(
                    f"Order cancellation failed on exchange: {validated_response.model_dump()}",
                    code=APIErrorCode.ORDER_REJECTED.value,
                )

            # Check statuses for confirmation
            if (
                validated_response.data.statuses
                and validated_response.data.statuses[0] == "canceled"
            ):
                logger.info(f"Successfully cancelled order {order_id} for asset {asset_index}")
                return {"success": True, "order_id": order_id, "status": "canceled"}
            else:
                logger.warning(
                    f"Order cancellation status unclear: {validated_response.data.statuses}"
                )
                # Might have failed, or status object has details
                first_status = validated_response.data.statuses[0]
                if (
                    isinstance(first_status, HyperliquidRawExchangeStatusObject)
                    and first_status.error
                ):
                    raise APIError(
                        f"Cancel failed: {first_status.error}",
                        code=APIErrorCode.ORDER_REJECTED.value,
                    )
                # Fallback success even if status isn't clear string "canceled"
                return {"success": True, "order_id": order_id, "status": "unknown"}

        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Failed to validate order cancellation response: {e}. "
                f"Raw: {response_raw}"
            )
            raise APIError(
                f"Invalid response after cancelling order: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error canceling order: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error canceling order: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error canceling order: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e
