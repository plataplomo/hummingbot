import asyncio
import logging
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

import aiohttp
from eth_account.messages import encode_typed_data
from pydantic import ValidationError
from web3.auto import w3

# from websockets import WebSocketClientProtocol  # Use modern API for compatibility
from cyberdelta.apis.base_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_mapper import (
    HyperliquidCandleMapper,
    HyperliquidMapper,
    HyperliquidOrderMapper,
)
from cyberdelta.apis.hyperliquid.hl_ws_mapper import HyperliquidWebsocketMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawOrderStatusResponse,
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
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderStatus,
)
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
        # Store handler by channel key for routing
        self._ws_handlers: dict[str, MessageHandler] = {}
        # Store original topic string and handler for resubscription
        self._ws_subscriptions: dict[str, MessageHandler] = {}
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

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Handle raw WebSocket message, routing it for processing."""
        # For Hyperliquid, no special pre-processing needed currently.
        # Directly call the routing logic.
        await self._route_ws_message(message)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Route incoming WebSocket messages based on Hyperliquid's 'channel' field."""
        channel = message.get("channel")
        data = message.get("data")
        if not channel or data is None:  # Check data is not None explicitly
            logger.debug(f"[{self.exchange_name}] Received unroutable WS message: {message}")
            return

        if not isinstance(data, dict):
            logger.warning(
                f"[{self.exchange_name}] Received WS data for channel {channel} "
                f"is not a dict: {type(data)}"
            )
            return
        # Cast data to satisfy mapper type hints (borderline use of cast due to
        # external data format)
        # #[CAST-REVIEW-REQUIRED] Justification: External WS data structure isn't strictly typed.
        data_dict = cast(dict[str, Any], data)

        # Map channel names to parsing functions and registered handlers
        if channel == "l2Book":
            handler = self._ws_handlers.get(channel)  # Get handler registered for "l2Book" key
            if handler:
                try:
                    # Parse using the specific mapper function
                    parsed_book: OrderBook | None = (
                        HyperliquidWebsocketMapper.parse_orderbook_message(data_dict, logger)
                    )
                    if parsed_book:  # Pass parsed data if successful
                        # Convert parsed object back to dict for the handler
                        await handler(parsed_book.model_dump())
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Failed to parse l2Book data: {data_dict}"
                        )
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Error in l2Book handler/parser: {e}", exc_info=True
                    )
            else:
                logger.debug(f"[{self.exchange_name}] No handler for channel: {channel}")
        elif channel == "trades":
            handler = self._ws_handlers.get(channel)  # Get handler for "trades"
            if handler:
                try:
                    # Parse using the specific mapper function
                    parsed_trade: Trade | None = HyperliquidWebsocketMapper.parse_trade_message(
                        data_dict, logger
                    )
                    if parsed_trade:
                        # Convert parsed object back to dict for the handler
                        await handler(parsed_trade.model_dump())
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Failed to parse trades data: {data_dict}"
                        )
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Error in trades handler/parser: {e}", exc_info=True
                    )
            else:
                logger.debug(f"[{self.exchange_name}] No handler for channel: {channel}")
        elif channel == "userEvents":
            handler = self._ws_handlers.get(channel)  # Get handler for "userEvents"
            if handler:
                try:
                    # User events need specific parsing based on internal type (order, fill, etc.)
                    # This might need a dedicated mapper function in HyperliquidWebsocketMapper
                    # e.g., parsed_event = HyperliquidWebsocketMapper.parse_user_event(
                    #    data_dict, logger
                    # )
                    # For now, pass raw dict - handler MUST parse internally.
                    logger.debug(
                        f"[{self.exchange_name}] Passing raw userEvent dict data to handler."
                    )
                    await handler(
                        data_dict
                    )  # TODO: Implement parsing in handler or add mapper func
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Error in userEvents handler: {e}", exc_info=True
                    )
            else:
                logger.debug(f"[{self.exchange_name}] No handler for channel: {channel}")
        # Add more channel handlers as needed
        else:
            logger.debug(f"[{self.exchange_name}] Unhandled WS channel: {channel}")

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Subscribe to a WebSocket topic (channel) using Hyperliquid format."""
        # Map internal topic concept to Hyperliquid subscription message
        # Example: topic might be "l2Book:BTC" or "trades:ETH"
        # Hyperliquid subscription format might vary based on channel
        # This needs specific mapping based on Hyperliquid docs
        subscription_payload: dict[str, Any] = {}
        if topic.startswith("l2Book:"):
            coin = topic.split(":", 1)[1]
            subscription_payload = {
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": coin},
            }
        elif topic.startswith("trades:"):
            coin = topic.split(":", 1)[1]
            subscription_payload = {
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": coin},
            }
        elif topic == "userEvents":
            if not self._wallet_address:
                logger.error(
                    f"[{self.exchange_name}] Wallet address needed to subscribe to userEvents"
                )
                return
            subscription_payload = {
                "method": "subscribe",
                "subscription": {"type": "userEvents", "user": self._wallet_address},
            }
        else:
            logger.error(f"[{self.exchange_name}] Unsupported subscription topic format: {topic}")
            return

        # Store handler using the original topic string as the key FOR RESUBSCRIPTION
        self._ws_subscriptions[topic] = handler

        # Determine channel key for routing incoming messages and store handler FOR ROUTING
        channel_key = topic.split(":", 1)[0]  # e.g., "l2Book", "trades", "userEvents"
        self._ws_handlers[channel_key] = handler  # Use channel key for routing lookup

        if self.ws_connection and self._is_connected:
            try:
                await self.ws_connection.send_json(subscription_payload)
                logger.info(f"[{self.exchange_name}] Sent subscription request for {topic}")
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Error sending subscription for {topic}: {e}",
                    exc_info=True,
                )
        else:
            logger.warning(
                f"[{self.exchange_name}] Cannot subscribe to {topic}, WebSocket not connected."
            )

    async def _resubscribe(self) -> None:
        """Resubscribe to all registered topics upon reconnection."""
        logger.info(
            f"[{self.exchange_name}] Resubscribing to topics: "
            f"{list(self._ws_subscriptions.keys())}\n"
        )
        # Iterate through the stored topic->handler mapping
        subscriptions_copy = self._ws_subscriptions.copy()
        if not subscriptions_copy:
            logger.info(
                f"[{self.exchange_name}] No subscriptions registered, nothing to resubscribe.\n"
            )
            return

        for topic, handler in subscriptions_copy.items():
            # Call subscribe again for each stored topic and handler
            try:
                logger.debug(f"[{self.exchange_name}] Attempting to resubscribe to {topic}\n")
                # Subscribe uses the topic to generate payload AND store handler again
                await self.subscribe(topic, handler)  # Resend subscription message
                await asyncio.sleep(0.1)  # Small delay between resubscriptions
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Failed to resubscribe to topic {topic}: {e}",
                    exc_info=True,
                )

    # --- REST API Implementation --- #

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
                    # Extract trigger info if present
                    # (might require extra parsing logic if nested differently in history)
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
        """Establish the WebSocket connection using the base class logic."""
        if not self.is_connected:
            await self._connect_ws()
        else:
            logger.debug(f"[{self.exchange_name}] WebSocket already connected.")

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        """Fetch a single order by its ID.

        Args:
            order_id: The exchange-assigned order ID.
            symbol: Optional market symbol (ignored by Hyperliquid for ID lookup).

        Returns:
            The Order object if found, otherwise None.
        """
        try:
            # Reuse get_order_status which handles fetching and transformation
            # Symbol is ignored by Hyperliquid's get_order_status implementation
            return await self.get_order_status(order_id=order_id, symbol=symbol)
        except APIError as e:
            # If get_order_status raises ORDER_NOT_FOUND, return None as per this method's contract
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.debug(f"[{self.exchange_name}] Order {order_id} not found (get_order).")
                return None
            # Re-raise other API errors
            logger.error(f"[{self.exchange_name}] API error fetching order {order_id}: {e}")
            raise
        except Exception as e:
            # Re-raise unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error fetching order {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching order {order_id}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def cancel_all_orders(self, symbol: str | None = None) -> None:
        """
        Cancel all open orders, optionally filtering by symbol.
        Required by ExchangeAPI base class.
        NOTE: Hyperliquid doesn't have a bulk cancel. This fetches open orders
        and cancels them individually.
        """
        logger.info(
            f"[{self.exchange_name}] Attempting to cancel all orders for symbol: {symbol or 'all'}"
        )
        try:
            # Fetch open orders, potentially filtered by symbol
            open_orders = await self.get_open_orders(symbol=symbol)

            if not open_orders:
                logger.info(
                    f"[{self.exchange_name}] No open orders found for {symbol or 'all'} to cancel."
                )
                return

            logger.info(f"[{self.exchange_name}] Found {len(open_orders)} open orders to cancel.")

            cancelled_count = 0
            failed_count = 0

            # Iterate and cancel each order individually
            for order in open_orders:
                try:
                    # Ensure required fields are present
                    if order.exchange_order_id and order.symbol:
                        logger.debug(
                            f"[{self.exchange_name}] Cancelling order "
                            f"{order.exchange_order_id} for {order.symbol}"
                        )
                        # Call the existing cancel_order method
                        await self.cancel_order(
                            order_id=order.exchange_order_id, symbol=order.symbol
                        )
                        cancelled_count += 1
                        # Add a small delay to avoid potential rate limiting
                        await asyncio.sleep(0.1)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping order cancellation "
                            f"due to missing ID or symbol: {order}"
                        )
                        failed_count += 1
                except APIError as e:
                    logger.error(
                        f"[{self.exchange_name}] Failed to cancel order "
                        f"{order.exchange_order_id}: {e}"
                    )
                    failed_count += 1
                    # Continue to next order even if one fails
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error cancelling order "
                        f"{order.exchange_order_id}: {e}"
                    )
                    failed_count += 1
                    # Continue to next order

            logger.info(
                f"[{self.exchange_name}] Cancellation summary for {symbol or 'all'}: "
                f"{cancelled_count} succeeded, {failed_count} failed."
            )

        except APIError as e:
            # Error during get_open_orders
            logger.error(f"[{self.exchange_name}] API Error fetching open orders to cancel: {e}")
            raise  # Re-raise the error
        except Exception as e:
            # Unexpected error during the overall process
            logger.error(
                f"[{self.exchange_name}] Unexpected error during cancel_all_orders "
                f"for {symbol or 'all'}: {e}",
                exc_info=True,
            )
            # Wrap in a generic APIError
            raise APIError(
                message=f"Unexpected error during cancel_all_orders: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def get_order_history(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> list[Order]:
        """Fetches historical orders from Hyperliquid.

        Note: Hyperliquid's query_order_history takes startTime and endTime in milliseconds.
              The base `ExchangeAPI` uses datetimes, so we convert here.
              Hyperliquid doesn't directly support filtering by orderId or clientOrderId
              in this endpoint; filtering would happen post-fetch if needed.

        Args:
            symbol: Optional symbol filter (Not used by Hyperliquid query_order_history).
            start_time: Optional start time filter (datetime UTC).
            end_time: Optional end time filter (datetime UTC).
            limit: Maximum number of orders (Not used by Hyperliquid query_order_history).
            order_id: Optional filter (Not used by Hyperliquid query_order_history).
            client_order_id: Optional filter (Not used by Hyperliquid query_order_history).

        Returns:
            List of Order objects.

        Raises:
            APIError: If the request fails or the response is invalid.
        """
        # Convert datetimes to milliseconds for Hyperliquid
        # Default start_time to 0 if None, as HL requires it
        start_time_ms = int(start_time.timestamp() * 1000) if start_time else 0
        # Default end_time to current time if None
        end_time_ms = int(end_time.timestamp() * 1000) if end_time else int(time.time() * 1000)

        payload = {
            "type": "queryOrderHistory",
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }

        response_raw: object = None
        orders: list[Order] = []
        try:
            response_raw = await self._request("POST", "/info", data=payload, is_signed=False)

            if not isinstance(response_raw, list):
                raise APIError(
                    f"Invalid response type for queryOrderHistory: expected list, "
                    f"got {type(response_raw)}",
                    code=APIErrorCode.UNKNOWN.value,
                    http_status=None,
                )

            for order_data_raw in response_raw:
                try:
                    # Validate using Raw model (imported at top level)
                    raw_order = HyperliquidRawOrder.model_validate(order_data_raw)
                    # Extract trigger info if present
                    # (might require extra parsing logic if nested differently in history)
                    trigger_info_raw = order_data_raw.get("trigger")
                    trigger_info = (
                        HyperliquidRawTriggerInfo.model_validate(trigger_info_raw)
                        if isinstance(trigger_info_raw, dict)
                        else None
                    )

                    # Map using the order mapper
                    internal_order = HyperliquidOrderMapper.transform_raw_order_to_internal(
                        raw=raw_order, trigger=trigger_info
                    )
                    if internal_order:  # Mapper might return None on error
                        orders.append(internal_order)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping order history item due to "
                            f"transform returning None. Raw: {order_data_raw}"
                        )
                except (ValidationError, ValueError) as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping order history item due to "
                        f"validation/transform error: {e}. Raw: {order_data_raw}"
                    )
                    continue

            # Apply filtering post-fetch if needed
            if symbol:
                orders = [o for o in orders if o.symbol == symbol]
            if order_id:
                orders = [o for o in orders if o.exchange_order_id == order_id]
            if client_order_id:
                orders = [o for o in orders if o.client_order_id == client_order_id]
            if limit is not None and limit > 0:
                orders = orders[:limit]

            return orders

        except APIError:
            raise
        except (ValidationError, ValueError) as e:
            logger.error(
                f"[{self.exchange_name}] Error processing order history response: {e}. "
                f"Raw: {response_raw}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to process order history response: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting order history: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error getting order history: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e

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

    async def get_market_data(self, symbol: str, timeframe: str, limit: int = 100) -> list[Candle]:
        """
        Fetches historical market data (candlesticks) for a given symbol and timeframe.

        Args:
            symbol: Trading symbol (e.g., 'BTC', 'ETH').
            timeframe: Candlestick interval (e.g., '1m', '1h', '1d').
            limit: Number of candles to retrieve (exchange-specific limits may apply).

        Returns:
            A list of Candle objects.

        Raises:
            APIError: If the request fails or the response is malformed.
        """
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol.upper(),  # Ensure coin is uppercase as per typical API expectations
                "interval": timeframe,
                "startTime": 0,  # HL API takes startTime = 0 for latest `limit` candles
                "endTime": int(time.time() * 1000),  # Current time in ms for endTime
            },
        }
        # Note: Hyperliquid's candleSnapshot doesn't directly use a 'limit' in request body.
        # It returns candles between startTime/endTime or a fixed number if startTime=0.
        # The 'limit' param is conceptual; actual data points depend on API.
        # For "latest N candles", startTime=0 is often used.

        try:
            # Use self.INFO_URL for candle snapshots
            raw_response = await self._request("POST", "/info", data=payload, endpoint_group="info")
            if raw_response is None:
                raise APIError(
                    f"No response received for candle snapshot {symbol} {timeframe}",
                    code=APIErrorCode.TIMEOUT.value,
                )

            # DEFENSIVE CHECK: response_raw is Any. Runtime check needed for safety.
            # Pyright=[reportUnknownVariableType,
            # reportUnknownArgumentType, reportUnknownMemberType]
            if not isinstance(raw_response, dict):
                raise APIError(
                    f"Unexpected response format for candle snapshot: {type(raw_response)}",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )

            # Validate the entire snapshot
            # Assuming raw_response is the snapshot dict itself
            raw_snapshot = HyperliquidRawCandleSnapshot.model_validate(raw_response)

            # Map the validated snapshot to a list of internal Candle objects
            # Use the class method instead of the removed standalone function
            internal_candles = HyperliquidCandleMapper.map(raw_snapshot, symbol, timeframe)

            # Apply limit if necessary (though HL API might not support it directly)
            # This is post-processing.
            if limit > 0 and len(internal_candles) > limit:
                internal_candles = internal_candles[-limit:]

            return internal_candles

        except ValidationError as e:
            logger.error(f"Pydantic validation error for {symbol} candles: {e}")
            raise APIError(
                "Failed to validate candle data from exchange",
                code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                original_exception=e,
            ) from e
        except APIError:  # Re-raise APIErrors
            raise
        except Exception as e:
            logger.error(f"Error fetching or processing {symbol} candles: {e}", exc_info=True)
            raise APIError(
                f"An unexpected error occurred while fetching candles for {symbol}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

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
        # TODO: Implement robust symbol to asset_index mapping (similar to cancel_order).
        asset_index = 0  # Placeholder
        logger.warning(f"Using placeholder asset_index=0 for symbol {symbol} in place_order")

        # Map internal types to Hyperliquid format
        is_buy = side == OrderSide.BUY
        sz = str(quantity)
        limit_px = (
            str(price) if price is not None else "0"
        )  # HL requires price string, use "0" for market?

        # Map OrderType and TimeInForce to Hyperliquid order type structure
        hl_order_type: dict[str, Any]
        if order_type == OrderType.MARKET:
            hl_order_type = {"market": {}}
        elif order_type == OrderType.LIMIT:
            if price is None:
                raise ValueError("Price is required for LIMIT orders")
            # Map TIF
            tif_map = {
                TimeInForce.GTC: "Gtc",
                TimeInForce.IOC: "Ioc",
                TimeInForce.ALO: "Alo",  # Add Limit Only (Post Only)
                # TimeInForce.FOK mapping? Check HL docs
            }
            tif_str = tif_map.get(time_in_force, "Gtc")  # Default GTC
            hl_order_type = {"limit": {"tif": tif_str}}
        # TODO: Add mapping for STOP_MARKET, STOP_LIMIT, TP/SL orders if supported
        else:
            raise NotImplementedError(f"Order type {order_type} not yet supported for Hyperliquid")

        # Assemble the action payload
        action_payload: dict[str, Any] = {
            "asset": asset_index,
            "isBuy": is_buy,
            "sz": sz,
            "limitPx": limit_px,
            "orderType": hl_order_type,
            "reduceOnly": reduce_only,
        }
        if client_order_id:
            # Ensure client_order_id is bytes32 hex string if cloid is bytes32
            # This assumes cloid is a string, check HL API spec if it needs hashing/padding.
            # For now, pass as string if provided.
            action_payload["cloid"] = client_order_id

        request_data = {"type": "order", "actions": [action_payload]}  # Actions should be a list

        response_raw: object = None  # Initialize
        try:
            response_raw = await self._request(
                "POST", self.rest_endpoint + "/exchange", data=request_data, is_signed=True
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
                        raise NotImplementedError(
                            "Mapping resting order status to internal Order not implemented"
                        )
                    elif first_status.filled:
                        # TODO: Map HyperliquidRawExchangeStatusFilled back to internal Order/Trade
                        logger.info(f"Order filled immediately: {first_status.filled.oid}")
                        # Placeholder - ideally return mapped Order or Trade info
                        raise NotImplementedError(
                            "Mapping filled order status to internal Order not implemented"
                        )
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
        except Exception as e:  # Catch broader exceptions
            logger.error(
                f"[{self.exchange_name}] Unexpected error placing order: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error placing order: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def cancel_order(
        self, order_id: str, symbol: str | None = None
    ) -> bool:  # Updated return type
        """Cancel an existing order. Returns True if successful."""
        if not symbol:
            raise ValueError("Symbol is required to cancel Hyperliquid orders")

        # TODO: Implement robust symbol to asset_index mapping.
        # This likely requires fetching market metadata (e.g., from /info type=metaAndAssetCtxs)
        # and caching the mapping {symbol_name: asset_index}.
        # Using placeholder 0 for now.
        try:
            asset_index = 0  # Placeholder - MUST BE REPLACED
            logger.warning(f"Using placeholder asset_index=0 for symbol {symbol} in cancel_order")
        except Exception as e:
            # Handle potential errors during symbol->index lookup
            raise APIError(
                f"Failed to map symbol {symbol} to asset index: {e}",
                code=APIErrorCode.INVALID_PARAMS.value,
            ) from e

        response_raw: object = None  # Initialize for error reporting
        try:
            action_payload = {"asset": asset_index, "oid": int(order_id)}
            request_data = {"type": "cancel", "action": action_payload}

            response_raw = await self._request(
                "POST", self.rest_endpoint + "/exchange", data=request_data, is_signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            if validated_response.status != "ok" or not validated_response.data:
                raise APIError(
                    f"Order cancellation failed on exchange: {validated_response.model_dump()}",
                    code=APIErrorCode.ORDER_REJECTED.value,
                )

            # Check statuses for confirmation - expecting "canceled"
            if (
                validated_response.data.statuses
                and validated_response.data.statuses[0] == "canceled"
            ):
                logger.info(f"Successfully cancelled order {order_id} for asset {asset_index}")
                return True  # Return True on success
            else:
                # Handle unclear or error status
                error_message = "Cancellation status unclear"
                if validated_response.data.statuses and isinstance(
                    validated_response.data.statuses[0], HyperliquidRawExchangeStatusObject
                ):
                    error_status = validated_response.data.statuses[0]
                    if error_status.error:
                        error_message = f"Cancel failed: {error_status.error}"
                        raise APIError(error_message, code=APIErrorCode.ORDER_REJECTED.value)

                logger.warning(f"{error_message}: {validated_response.data.statuses}")
                # Even if status isn't explicitly 'canceled', if no APIError was raised,
                # assume cancellation was likely accepted by the exchange.
                # Consider raising an error here if strict confirmation is required.
                return True  # Or potentially False/raise error if confirmation is needed

        except (ValidationError, ValueError) as e:  # Include ValueError for int(order_id)
            logger.error(
                f"[{self.exchange_name}] Failed to validate order cancellation response or "
                f"invalid OID: {e}. "
                f"Raw: {response_raw}"
            )
            raise APIError(
                f"Invalid response/OID after cancelling order {order_id}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error canceling order {order_id}: {e}")
            raise  # Re-raise APIError on failure
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error canceling order {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error canceling order {order_id}: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_recent_fills(
        self, symbol: str | None = None, limit: int | None = None
    ) -> list[Trade]:
        """Fetch recent fills/trades for the account.

        Uses the existing get_trade_history method.
        """
        # Ensure limit is handled correctly, default in get_trade_history is 100
        effective_limit = limit if limit is not None else 100
        return await self.get_trade_history(symbol=symbol, limit=effective_limit)

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol."""
        # Hyperliquid get_open_orders fetches all if symbol is None
        return await self.get_open_orders(symbol=symbol)

    async def ping_websocket(self) -> None:
        # Use the default implementation from the base class
        await super().ping_websocket()

    # --- Error Mapping --- #

    def _update_rate_limit_from_headers(
        self, headers: Mapping[str, str], method: str, path: str
    ) -> None:
        """
        Update rate limit information based on response headers.
        Hyperliquid does not typically provide rate limit info in headers.
        This is a placeholder implementation.
        """
        # Hyperliquid does not seem to provide standard rate limit headers.
        # If specific headers are discovered, parse them here.
        # logger.debug(f"[{self.exchange_name}] Received headers: {headers}")
        pass  # No standard headers known for HL

    def _map_error_response(
        self,
        status_code: int,
        error_body: str,
        error_data: dict[str, Any] | None,
    ) -> APIError:
        """Maps Hyperliquid specific error responses to a standardized APIError."""
        # Use the dedicated mapper from the new file
        return HyperliquidErrorMapper.map_error_response(
            error_body=error_body,
            response_data=error_data,
            http_status=status_code,
            # original_exception can be added if available from the calling context in _request
        )

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order:
        """Fetches the status of a specific order."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for fetching order status",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        payload = {
            "type": "orderStatus",
            "user": self._wallet_address,
            "oid": int(order_id),
        }

        try:
            response_data = await self._request("POST", self.INFO_URL + "/info", data=payload)

            if not response_data or not isinstance(response_data, list):
                raise APIError(
                    f"Unexpected response format from get_order_status: {response_data}",
                    code=APIErrorCode.INVALID_REQUEST.value,
                )

            status_part_raw = response_data[0]

            if isinstance(status_part_raw, str):
                if status_part_raw.lower() == "order not found":
                    raise APIError(
                        f"Order not found: id={order_id}", code=APIErrorCode.ORDER_NOT_FOUND.value
                    )
                # Handle other potential string error messages if necessary
                raise APIError(
                    f"Received string error from get_order_status: {status_part_raw}",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )

            if not isinstance(status_part_raw, dict):
                raise APIError(
                    f"Unexpected status object format, expected dict: {status_part_raw}",
                    code=APIErrorCode.INVALID_REQUEST.value,
                )

            validated_status_response: HyperliquidRawOrderStatusResponse
            try:
                validated_status_response = HyperliquidRawOrderStatusResponse.model_validate(
                    status_part_raw
                )
            except ValidationError as e:
                logger.error(
                    f"[{self.exchange_name}] Failed to validate raw order status response: {e}. Data: {status_part_raw}"
                )
                raise APIError(
                    message="Failed to parse order status response from exchange.",
                    code=APIErrorCode.UNKNOWN.value,
                    original_exception=e,
                ) from e

            # validated_status_response.order is HyperliquidRawOrder
            # transform_raw_order_to_internal expects raw order and optional trigger
            # For single order status, trigger is usually not part of the direct order object
            internal_order = HyperliquidOrderMapper.transform_raw_order_to_internal(
                raw=validated_status_response.order, trigger=None
            )

            return internal_order

        except APIError:
            raise
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Outer Pydantic validation error in get_order_status: {e}. Payload: {payload}"
            )
            raise APIError(
                message="Outer Pydantic validation error processing order status.",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_order_status for oid {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error fetching order status: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
