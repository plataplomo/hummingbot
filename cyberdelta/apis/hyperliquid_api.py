import asyncio
import json
import time
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

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
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawOrderStatusResponse,
    HyperliquidRawTriggerInfo,
    HyperliquidRawTriggerSpec,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
    HyperliquidRawWithdrawalToL1ActionPayload,
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
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


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
        else:
            self._account = None  # Ensure _account is None if no private key

        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_lock = asyncio.Lock()
        self._symbol_map: dict[str, str] = {}
        # Store handler by channel key for routing
        self._ws_handlers: dict[str, MessageHandler] = {}
        # Store original topic string and handler for resubscription
        self._ws_subscriptions: dict[str, MessageHandler] = {}
        self._is_connected = False
        self._asset_to_index_cache: dict[str, int] = {}  # Cache for symbol to asset_index

    async def _get_asset_index(self, symbol: str) -> int:
        """Fetch or retrieve from cache the asset_index for a given symbol."""
        if symbol in self._asset_to_index_cache:
            return self._asset_to_index_cache[symbol]

        # If not cached, fetch metaAndAssetCtxs
        logger.debug(
            f"[{self.exchange_name}] Asset index for {symbol} not cached, fetching meta..."
        )
        payload = {"type": "metaAndAssetCtxs"}
        response_raw: object = await self._request("POST", self.INFO_URL + "/info", data=payload)

        try:
            validated_response = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(response_raw)
            # The asset_index is the index in the universe list from the meta part.
            # Each HyperliquidRawAssetDefinition in meta.universe corresponds to an asset,
            # and its position (index) in this list is its asset_index.
            for index, asset_def in enumerate(validated_response.meta.universe):
                # asset_def is HyperliquidRawAssetDefinition, which has a 'name' field (the symbol)
                self._asset_to_index_cache[asset_def.name] = index

            # Retry fetching from cache
            if symbol in self._asset_to_index_cache:
                return self._asset_to_index_cache[symbol]
            else:
                logger.error(
                    f"[{self.exchange_name}] Asset index for {symbol} not found after "
                    f"fetching meta."
                )
                raise APIError(
                    f"Asset index for symbol '{symbol}' not found.",
                    code=APIErrorCode.SYMBOL_NOT_FOUND.value,
                )
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Failed to validate metaAndAssetCtxs response: {e}. "
                f"Raw: {response_raw}"
            )
            raise APIError(
                "Failed to parse market metadata for asset index mapping.",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except APIError:
            raise  # Re-raise APIErrors from _request
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error fetching asset index for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching asset index for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Sign a request using EIP-712 and wallet private key for Hyperliquid.
        The signature protects the action payload via connectionId.

        Args:
            method: HTTP method (unused for HL agent signing).
            path: API endpoint (unused for HL agent signing).
            params: URL parameters (unused for HL agent signing for /exchange).
            data: Request body (the action payload for /exchange, used for connectionId).

        Returns:
            Dictionary with headers for authentication.
        """
        if not self._account:
            # This method is for signed requests. If no account, it's an error.
            # Public requests should not call _authenticate.
            logger.error(
                f"[{self.exchange_name}] _authenticate called but no account is configured. "
                f"This indicates a logic error for a signed request to {path}."
            )
            raise APIError(
                "Private key not provided for signing; authentication cannot proceed.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        # Actual signing logic proceeds from here
        timestamp_ms = int(time.time() * 1000)
        async with self._nonce_lock:
            self._nonce_counter += 1
            nonce_val = self._nonce_counter

        if data is None:
            logger.error(
                f"[{self.exchange_name}] Signing attempted for an action, but no 'data' "
                f"(action payload) was provided for {path}."
            )
            raise APIError(
                "Action payload (data) is required for signing Hyperliquid exchange requests.",
                code=APIErrorCode.INVALID_PARAMS.value,
            )

        try:
            action_json_string = json.dumps(data, separators=(",", ":"))
        except TypeError as e:
            logger.error(
                f"[{self.exchange_name}] Failed to serialize action data to JSON for hashing: {e}. "
                f"Data: {data!r}",
                exc_info=True,
            )
            raise APIError(
                "Failed to serialize action data for signing",
                code=APIErrorCode.INVALID_PARAMS.value,
            ) from e

        connection_id_bytes = w3.keccak(text=action_json_string)

        structured_data_to_sign = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Agent": [
                    {"name": "source", "type": "string"},
                    {"name": "connectionId", "type": "bytes32"},
                    {"name": "timestamp", "type": "uint64"},
                ],
            },
            "primaryType": "Agent",
            "domain": {
                "name": "Hyperliquid",
                "version": "1",
                "chainId": self.CHAIN_ID,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": {
                "source": "Hyperliquid",
                "connectionId": connection_id_bytes,
                "timestamp": timestamp_ms,
            },
        }

        try:
            signable_message = encode_typed_data(full_message=structured_data_to_sign)
            signed_message = self._account.sign_message(signable_message)
            signature_hex = signed_message.signature.hex()
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error signing EIP-712 message: {e}", exc_info=True
            )
            raise APIError(
                "Failed to sign EIP-712 message for Hyperliquid action",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

        return {
            "headers": {
                "X-HL-Signature": signature_hex,
                "X-HL-Timestamp": str(timestamp_ms),
                "X-HL-Nonce": str(nonce_val),
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

        data_dict: dict[str, Any] = data  # Explicitly type hint after check

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
                        await handler(parsed_book.model_dump())
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Failed to parse l2Book data: {data_dict!r}"
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
                    # parse_trade_message now returns list[Trade]
                    parsed_trades_list: list[Trade] = (
                        HyperliquidWebsocketMapper.parse_trade_message(
                            message,
                            logger,  # Pass the whole message as parse_trade_message expects channel and data fields
                        )
                    )
                    if parsed_trades_list:
                        for trade_item in parsed_trades_list:
                            await handler(trade_item.model_dump())
                    else:
                        # This condition might be hit if the list is empty, which is not an error
                        logger.debug(
                            f"[{self.exchange_name}] No trades parsed from trades data: {data_dict!r}"
                        )
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Error in trades handler/parser: {e}", exc_info=True
                    )
            else:
                logger.debug(f"[{self.exchange_name}] No handler for channel: {channel}")
        elif channel == "userEvents":
            handler = self._ws_handlers.get(channel)
            if handler:
                user_events_data_raw: Any = data_dict.get("userEvents")
                if isinstance(user_events_data_raw, list):
                    for event_item_obj_raw in user_events_data_raw:  # Type hint Any for clarity
                        if not isinstance(event_item_obj_raw, dict):
                            logger.warning(
                                f"[{self.exchange_name}] Skipping non-dict item in userEvents list: {event_item_obj_raw!r}"
                            )
                            continue
                        event_item_dict: dict[str, Any] = event_item_obj_raw

                        event_type_raw = event_item_dict.get("event")
                        if not isinstance(event_type_raw, str):
                            logger.warning(
                                f"[{self.exchange_name}] Skipping userEvent item with non-string type: {event_type_raw!r}. Item: {event_item_dict!r}"
                            )
                            continue
                        event_type: str = event_type_raw

                        mapped_object: Any = None  # Initialize for this event item
                        try:
                            if event_type == "fill":
                                # parse_fill_message expects the fill data dict directly
                                mapped_object = HyperliquidWebsocketMapper.parse_fill_message(
                                    event_item_dict, logger
                                )
                            elif event_type == "order":
                                order_payload_dict = event_item_dict.get("data")
                                if isinstance(order_payload_dict, dict):
                                    actual_order_payload: dict[str, Any] = order_payload_dict
                                    mapped_object = (
                                        HyperliquidWebsocketMapper.parse_order_update_message(
                                            actual_order_payload, logger
                                        )
                                    )
                                else:
                                    logger.warning(
                                        f"[{self.exchange_name}] 'data' field missing or not a dict in order event: {event_item_dict!r}"
                                    )
                            else:
                                logger.debug(
                                    f"[{self.exchange_name}] Unhandled userEvent type: {event_type}. Data: {event_item_dict!r}"
                                )

                            if mapped_object:  # Check moved inside try, before specific excepts
                                await handler(mapped_object.model_dump(exclude_none=True))

                        except ValidationError as ve:
                            logger.warning(
                                f"[{self.exchange_name}] Validation failed for userEvent type '{event_type}': {ve}. Data: {event_item_dict!r}"
                            )
                        except Exception as e:
                            logger.error(
                                f"[{self.exchange_name}] Error processing userEvent type '{event_type}': {e}. Data: {event_item_dict!r}",
                                exc_info=True,
                            )
                else:
                    logger.warning(
                        f"[{self.exchange_name}] 'userEvents' field in data is not a list or missing: {data_dict!r}"
                    )
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

            # Use specific mapper for spot balances
            spot_balances_dict = HyperliquidMapper.map_raw_clearinghouse_state_to_spot_balances(
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

            # Use specific mapper for derivative positions
            positions_dict = HyperliquidMapper.map_raw_clearinghouse_state_to_derivative_positions(
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
                    trigger_info: HyperliquidRawTriggerInfo | None = order_obj.trigger
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
                    return HyperliquidMapper.map_raw_ctx_to_ticker(
                        asset_ctx
                    )  # Pass asset_ctx directly
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
            return HyperliquidMapper.map_raw_order_book(validated, depth)  # Corrected call
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
            return HyperliquidMapper.map_raw_trades(
                validated.items, limit
            )  # Pass validated.items directly
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
                asset_ctx
            )  # Pass asset_ctx directly
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
        """Initiates an L2 USDC transfer on Hyperliquid.

        Args:
            asset: The asset to transfer (must be "USDC" for Hyperliquid L2 transfer).
            amount: The amount to transfer.
            from_account: Ignored for Hyperliquid L2 transfers (always from main trading account).
            to_account: The destination 0x address on L2.

        Returns:
            A dictionary representing the exchange's response status.
        """
        if asset.upper() != "USDC":
            raise ValueError("Hyperliquid L2 transfers are currently only supported for USDC.")
        if not to_account:
            raise ValueError(
                "Destination address (to_account) is required for Hyperliquid L2 transfer."
            )

        transfer_action_payload = HyperliquidRawL2UsdTransferPayload(
            destination=to_account,
            token="USDC",
            amount=str(amount),
        )
        action_details = {
            "chain": "L2",
            "payload": transfer_action_payload.model_dump(by_alias=True),
        }
        request_data = {"type": "usdTransfer", "action": action_details}

        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST", self.rest_endpoint + "/exchange", data=request_data, is_signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            # Since status is Literal["ok"], Pydantic ensures it's "ok" if validation passed.
            # We now inspect validated_response.data.statuses for logical errors/success.

            if not validated_response.data or not validated_response.data.statuses:
                # This case implies status="ok" but no meaningful data/statuses list, which is unusual.
                logger.warning(
                    f"[{self.exchange_name}] L2 Transfer response 'ok' but data or statuses list is missing/empty. Raw: {response_raw!r}"
                )
                # Consider raising an error or returning a specific failure if this state is unexpected.
                raise APIError(
                    "L2 Transfer 'ok' but no status details returned.",
                    code=APIErrorCode.UNKNOWN.value,
                )

            first_status_obj_raw = validated_response.data.statuses[0]

            if isinstance(first_status_obj_raw, str):
                # Handle simple string statuses (e.g., "Error: Amount must be > 0")
                if "error" in first_status_obj_raw.lower():
                    logger.warning(
                        f"[{self.exchange_name}] L2 Transfer failed with status: {first_status_obj_raw}"
                    )
                    raise APIError(
                        first_status_obj_raw,
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                        exchange_message=first_status_obj_raw,
                    )
                # Potentially other non-error string statuses if API defines them
                logger.info(f"[{self.exchange_name}] L2 Transfer status: {first_status_obj_raw}")
                return {"status": "success_with_info", "data": first_status_obj_raw}
            else:  # If not str, it must be HyperliquidRawExchangeStatusObject due to list type hint
                status_object = first_status_obj_raw
                if status_object.error:
                    logger.warning(
                        f"[{self.exchange_name}] L2 Transfer failed: {status_object.error}"
                    )
                    raise APIError(
                        status_object.error,
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                        exchange_message=status_object.error,
                    )
                elif status_object.filled:  # Check for filled (though less common for transfer)
                    logger.info(
                        f"[{self.exchange_name}] L2 Transfer resulted in fill-like status: {status_object.filled.model_dump()}"
                    )
                    return {"status": "success", "data": status_object.filled.model_dump()}
                elif status_object.resting:  # Check for resting (less common for transfer)
                    logger.info(
                        f"[{self.exchange_name}] L2 Transfer resulted in resting-like status: {status_object.resting.model_dump()}"
                    )
                    return {"status": "success", "data": status_object.resting.model_dump()}
                else:
                    # This case implies HyperliquidRawExchangeStatusObject without error/filled/resting which is unusual.
                    logger.warning(
                        f"[{self.exchange_name}] L2 Transfer status object has no clear error/filled/resting state: {status_object.model_dump()}"
                    )
                    return {
                        "status": "success_unknown_details",
                        "data": status_object.model_dump(),
                    }  # Default success if no error
            # The Pydantic model for statuses list already ensures items are either str or HyperliquidRawExchangeStatusObject.

        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Failed to validate L2 transfer response: {e}. Raw: {response_raw}"
            )
            raise APIError(
                f"Invalid response after L2 transfer: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error during L2 transfer: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error during L2 transfer: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error during L2 transfer: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def withdraw(
        self, asset: str, amount: Decimal, address: str, network: str | None = None
    ) -> dict[str, Any]:
        """Initiates a withdrawal to L1 on Hyperliquid.

        Args:
            asset: The asset to withdraw (e.g., "USDC", "ETH").
            amount: The amount to withdraw.
            address: The destination L1 0x address.
            network: Ignored for Hyperliquid (L1 is implied).

        Returns:
            A dictionary representing the exchange's response status.
        """
        if not address:
            raise ValueError("Destination address is required for withdrawal.")

        # Determine action type and construct payload
        action_type: str
        action_payload_dict: dict[str, Any]
        if asset.upper() == "ETH":
            # Hyperliquid uses "withdrawEth" for ETH, different structure
            action_type = "withdrawEth"
            # For withdrawEth, the payload is simpler: just amount and destination
            # No separate Raw model needed if it's just these two string fields.
            action_payload_dict = {"amount": str(amount), "destination": address}
        else:
            action_type = "withdraw"
            withdrawal_payload = HyperliquidRawWithdrawalToL1ActionPayload(
                token=asset.upper(),
                amount=str(amount),
                destination=address,
            )
            action_payload_dict = withdrawal_payload.model_dump()

        # Correct request_data structure for /exchange endpoint
        # It should be the action type and its corresponding payload (action_payload_dict).
        # The EIP-712 nonce and signature are handled by _authenticate.
        request_data = {"type": action_type, "action": action_payload_dict}

        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST", self.rest_endpoint + "/exchange", data=request_data, is_signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            if not validated_response.data or not validated_response.data.statuses:
                logger.warning(
                    f"[{self.exchange_name}] Withdraw response 'ok' but data or statuses list is missing/empty. "
                    f"Raw: {response_raw!r}"
                )
                raise APIError(
                    "Withdrawal status unclear: 'ok' but no status details provided.",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )

            first_status_obj_raw = validated_response.data.statuses[0]

            if isinstance(first_status_obj_raw, str):
                if "error" in first_status_obj_raw.lower():
                    logger.warning(
                        f"[{self.exchange_name}] Withdraw failed with status string: {first_status_obj_raw}"
                    )
                    raise APIError(
                        first_status_obj_raw,
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                        exchange_message=first_status_obj_raw,
                    )
                logger.info(
                    f"[{self.exchange_name}] Withdraw status (string): {first_status_obj_raw}"
                )
                return {
                    "status": "success_with_info",
                    "message": first_status_obj_raw,
                    "tx_hash": None,
                }

            else:  # It's HyperliquidRawExchangeStatusObject
                status_object = first_status_obj_raw

                if status_object.error:
                    logger.warning(f"[{self.exchange_name}] Withdraw failed: {status_object.error}")
                    raise APIError(
                        status_object.error,
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                        exchange_message=status_object.error,
                    )
                elif status_object.withdrawal_submitted:
                    tx_hash = status_object.withdrawal_submitted
                    logger.info(
                        f"[{self.exchange_name}] Withdrawal for {asset} successful. TxHash: {tx_hash}"
                    )
                    return {"status": "success", "tx_hash": tx_hash}
                elif status_object.success:
                    logger.info(
                        f"[{self.exchange_name}] Withdraw successful with message: {status_object.success}"
                    )
                    return {
                        "status": "success_with_info",
                        "message": status_object.success,
                        "tx_hash": None,
                    }
                else:
                    logger.warning(
                        f"[{self.exchange_name}] Withdraw status 'ok' but unrecognized status object structure: "
                        f"{status_object.model_dump_json()!r}. Raw: {response_raw!r}"
                    )
                    raise APIError(
                        "Withdrawal status unclear: Unrecognized success object structure.",
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                    )

        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Validation error processing withdraw response: {e}. Raw: {response_raw!r}"
            )
            raise APIError(
                f"Failed to validate withdraw response: {e}",
                code=APIErrorCode.EXCHANGE_SPECIFIC.value,  # Corrected
                original_exception=e,
            ) from e
        except APIError:  # Re-raise APIErrors directly
            raise
        except Exception as e:
            logger.exception(
                f"[{self.exchange_name}] Unexpected error during withdraw for {asset} to {address}: {e}. Raw: {response_raw!r}"
            )
            raise APIError(
                f"Unexpected error during withdraw: {e}",
                code=APIErrorCode.UNKNOWN.value,  # Corrected
                original_exception=e,
            ) from e

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

            # DEFENSIVE CHECK: Runtime check for allMeta structure
            # allMeta typically returns: [{"universe": [...]}, [assetCtx1, assetCtx2, ...]]
            if (
                not isinstance(response_raw, list)
                or len(response_raw) != 2
                or not isinstance(response_raw[0], dict)  # meta part
                or not isinstance(response_raw[1], list)  # asset contexts part
            ):
                raise APIError(
                    f"Unexpected response structure for allMeta: {type(response_raw)}, length {len(response_raw) if isinstance(response_raw, list) else 'N/A'}",
                    code=APIErrorCode.UNKNOWN.value,
                )

            from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
                HyperliquidRawAssetCtx,  # For validating individual asset contexts
            )

            asset_ctx_list_raw: list[Any] = response_raw[1]
            result: list[FundingRate] = []

            for asset_ctx_raw in asset_ctx_list_raw:
                if not isinstance(asset_ctx_raw, dict):
                    logger.warning(
                        f"[{self.exchange_name}] Skipping non-dict asset_ctx in allMeta response: {asset_ctx_raw!r}"
                    )
                    continue
                try:
                    # Validate each asset context individually
                    validated_asset_ctx = HyperliquidRawAssetCtx.model_validate(asset_ctx_raw)
                    market_symbol: str = validated_asset_ctx.name

                    if symbols is not None and market_symbol not in symbols:
                        continue

                    # Use the correct mapper for AssetCtx
                    funding_rate = HyperliquidMapper.map_raw_ctx_to_funding_rate(
                        validated_asset_ctx
                    )

                    if funding_rate:
                        result.append(funding_rate)
                except ValidationError as ve_ctx:
                    logger.warning(
                        f"[{self.exchange_name}] Failed to validate asset_ctx for funding rate: {ve_ctx}. Data: {asset_ctx_raw!r}"
                    )
                except Exception as e_map:
                    logger.error(
                        f"[{self.exchange_name}] Error mapping asset_ctx to funding rate: {e_map}. Data: {asset_ctx_raw!r}",
                        exc_info=True,
                    )

            return result

        except (ValidationError, ValueError) as e:  # Catch outer validation/mapping errors
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

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """Place an order on Hyperliquid, with support for trigger orders."""
        asset_index = await self._get_asset_index(symbol)
        is_buy = side == OrderSide.BUY
        sz_str = str(quantity)

        # Determine base Hyperliquid order type (underlying order) and its limit price
        # This will be the 'orderType' and 'limitPx' for the main action if not a trigger,
        # or for the triggered order if it is a trigger type.
        underlying_hl_order_type_dict: dict[str, Any] = {}
        underlying_limit_px_str: str = "0"
        trigger_payload: dict[str, Any] | None = None

        # Default TIF mapping
        tif_map: dict[TimeInForce, Literal["Gtc", "Ioc", "Alo"]] = {
            TimeInForce.GTC: "Gtc",
            TimeInForce.IOC: "Ioc",
            TimeInForce.ALO: "Alo",
        }

        raw_tif_str_candidate = tif_map.get(time_in_force)

        if post_only and order_type in [
            OrderType.LIMIT,
            OrderType.STOP_LIMIT,
            OrderType.TAKE_PROFIT_LIMIT,
        ]:
            raw_tif_str_candidate = "Alo"

        if raw_tif_str_candidate is None:
            # This case implies time_in_force was not a valid key and post_only didn't override
            # Defaulting to Gtc, but this indicates an issue with input `time_in_force`
            logger.warning(
                f"[{self.exchange_name}] Unmapped time_in_force '{time_in_force}' and post_only='{post_only}', defaulting TIF to Gtc."
            )
            raw_tif_str_candidate = "Gtc"

        # Now, assign effective_tif based on raw_tif_str_candidate
        if raw_tif_str_candidate == "Gtc":
            effective_tif: Literal["Gtc", "Ioc", "Alo"] = "Gtc"
        elif raw_tif_str_candidate == "Ioc":
            effective_tif = "Ioc"
        elif raw_tif_str_candidate == "Alo":
            effective_tif = "Alo"
        else:
            # This path should be logically unreachable due to the upstream mapping of TimeInForce enum
            # and the explicit assignment of raw_tif_str_candidate in all cases.
            # Adding AssertionError to satisfy linters like Pylance about `effective_tif` being bound.
            # Mypy correctly infers this is unreachable if previous logic is sound.
            raise AssertionError(
                f"Internal TIF logic error: unexpected raw_tif_str_candidate '{raw_tif_str_candidate}' after mapping."
            )

        if order_type == OrderType.MARKET:
            underlying_hl_order_type_dict = {
                "market": HyperliquidRawMarketOrderTypeDetails().model_dump()
            }
            underlying_limit_px_str = "0"  # Market orders use "0" for limitPx
        elif order_type == OrderType.LIMIT:
            if price is None:
                raise ValueError("Price is required for LIMIT orders.")
            underlying_limit_px_str = str(price)
            underlying_hl_order_type_dict = {
                "limit": HyperliquidRawLimitOrderTypeDetails(tif=effective_tif).model_dump()
            }
        elif order_type in [OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET]:
            if stop_price is None:
                raise ValueError(f"stop_price is required for {order_type.value} orders.")
            underlying_limit_px_str = "0"  # Triggered market order
            trigger_details = HyperliquidRawTriggerSpec(
                triggerPx=str(stop_price),
                isMarket=True,
                tpsl="sl" if order_type == OrderType.STOP_MARKET else "tp",
            )
            trigger_payload = trigger_details.model_dump(by_alias=True)
        elif order_type in [OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT]:
            if price is None:  # This is the limit price of the triggered order
                raise ValueError(
                    f"price (for the triggered limit order) is required for {order_type.value} orders."
                )
            if stop_price is None:
                raise ValueError(f"stop_price is required for {order_type.value} orders.")
            underlying_limit_px_str = str(price)
            underlying_hl_order_type_dict = {
                "limit": HyperliquidRawLimitOrderTypeDetails(tif=effective_tif).model_dump()
            }
            trigger_details = HyperliquidRawTriggerSpec(
                triggerPx=str(stop_price),
                isMarket=False,
                tpsl="sl" if order_type == OrderType.STOP_LIMIT else "tp",
            )
            trigger_payload = trigger_details.model_dump(by_alias=True)
        else:
            # Should not be reached if OrderType enum is exhaustive and handled
            raise NotImplementedError(f"Order type {order_type.value} is not supported.")

        action_payload: dict[str, Any] = {
            "asset": asset_index,
            "isBuy": is_buy,
            "sz": sz_str,
            "limitPx": underlying_limit_px_str,
            "orderType": underlying_hl_order_type_dict,
            "reduceOnly": reduce_only,
        }
        if client_order_id:
            action_payload["cloid"] = client_order_id
        if trigger_payload:
            action_payload["trigger"] = trigger_payload

        request_data = {"type": "order", "actions": [action_payload]}

        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST", self.rest_endpoint + "/exchange", data=request_data, is_signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            if validated_response.status != "ok" or not validated_response.data:
                # If status is not "ok", or if it is "ok" but there's no data field, map error
                mapped_error = HyperliquidErrorMapper.map_error_response(
                    error_body=str(response_raw),
                    response_data=validated_response.model_dump() if validated_response else None,
                    http_status=200,  # Assuming 200 for non-"ok" if HTTP itself was fine
                )
                raise mapped_error

            # At this point, status is "ok" and data field exists.
            if not validated_response.data.statuses:
                # Status "ok", data exists, but statuses list is empty - unusual for place_order
                logger.warning(
                    f"[{self.exchange_name}] Order placement response 'ok' but statuses list is empty: {validated_response.data.model_dump()}"
                )
                raise APIError(
                    "Order placement 'ok' but no status details returned.",
                    code=APIErrorCode.UNKNOWN.value,
                )

            # Correctly determine first_status_obj_raw based on actual API response structure
            # The API returns a list of status objects or strings within response_data.statuses
            if not validated_response.data or not validated_response.data.statuses:
                logger.error(
                    f"[{self.exchange_name}] Order placement response missing statuses field or data. Raw: {response_raw}"
                )
                raise APIError(
                    "Order placement response missing 'statuses' field or 'data'.",
                    code=APIErrorCode.SERVER_ERROR.value,  # Corrected Error Code
                )

            first_status_obj_raw = validated_response.data.statuses[0]

            first_status_obj: HyperliquidRawExchangeStatusObject | None = None
            error_to_raise_from_status: APIError | None = None

            if isinstance(first_status_obj_raw, str):
                log_message = f"[{self.exchange_name}] Order placement returned string status: {first_status_obj_raw}"
                logger.warning(log_message)
                # Specific error mapping for known string messages
                if (
                    "liquidation order too large" in first_status_obj_raw.lower()
                    or "LiquidationLimitOrderTooLargeError" in first_status_obj_raw
                ):
                    error_to_raise_from_status = APIError(
                        f"Order placement failed: {first_status_obj_raw}",
                        code=APIErrorCode.INSUFFICIENT_FUNDS.value,
                        exchange_message=first_status_obj_raw,
                    )
                else:
                    error_to_raise_from_status = APIError(
                        f"Order placement failed with string status: {first_status_obj_raw}",
                        code=APIErrorCode.ORDER_REJECTED.value,
                        exchange_message=first_status_obj_raw,
                    )
            else:
                # If not a string, and given the Union type HyperliquidRawExchangeStatusObject | str
                # for items in statuses, first_status_obj_raw must be HyperliquidRawExchangeStatusObject.
                # The linter has indicated multiple times that an explicit isinstance check
                # for HyperliquidRawExchangeStatusObject is redundant here.
                # We therefore assign directly, trusting the type system and Pydantic's parsing.
                first_status_obj = first_status_obj_raw
                # Note: If first_status_obj_raw could be something else entirely
                # (violating the Union[HyperliquidRawExchangeStatusObject, str] contract from Pydantic's parsing of the list),
                # this would be a runtime error later or a broader type system issue.
                # For now, we follow the strong type hinting and linter feedback.

            if error_to_raise_from_status:
                raise error_to_raise_from_status

            if first_status_obj is None:
                # This should only be reached if there was a string status that wasn't mapped to an error above,
                # or a logical flaw in the branches.
                logger.error(
                    f"[{self.exchange_name}] Critical logic error: first_status_obj is None after status processing without raising an error. Raw status item: {first_status_obj_raw}"
                )
                raise APIError(
                    "Internal error processing order status after placement.",
                    code=APIErrorCode.UNKNOWN.value,
                )

            order_id_to_fetch: int | None = None
            log_message_prefix = "Order placement reported"

            if resting_details := first_status_obj.resting:
                order_id_to_fetch = resting_details.oid
                log_message_prefix = f"Order OID:{order_id_to_fetch} reported as resting"
            elif filled_details := first_status_obj.filled:
                order_id_to_fetch = filled_details.oid
                log_message_prefix = f"Order OID:{order_id_to_fetch} reported as filled (avgPx: {filled_details.avg_px}, totalSz: {filled_details.total_sz})"
            elif error_msg := first_status_obj.error:
                # Specific error string checks can be more robust here if known patterns exist
                if "LiquidationLimitOrderTooLargeError" in error_msg:
                    raise APIError(
                        f"Order placement failed: {error_msg}",
                        code=APIErrorCode.INSUFFICIENT_FUNDS.value,
                        exchange_message=error_msg,
                    )
                raise APIError(
                    f"Order placement failed: {error_msg}",
                    code=APIErrorCode.ORDER_REJECTED.value,
                    exchange_message=error_msg,
                )

            if order_id_to_fetch is not None:
                logger.info(
                    f"[{self.exchange_name}] {log_message_prefix}. Fetching canonical status after delay."
                )
                await asyncio.sleep(0.2)  # Delay to allow order to propagate
                try:
                    return await self.get_order_status(
                        order_id=str(order_id_to_fetch), symbol=symbol
                    )
                except APIError as e_fetch:
                    logger.error(
                        f"[{self.exchange_name}] {log_message_prefix}, but failed to fetch canonical status for OID {order_id_to_fetch}: {e_fetch}"
                    )
                    # Re-raise, making it clear this is a post-placement fetch failure
                    raise APIError(
                        f"{log_message_prefix}, but failed to retrieve its final status: {e_fetch.message}",
                        code=APIErrorCode.UNKNOWN.value,  # Changed from ORDER_FETCH_ERROR
                        original_exception=e_fetch.original_exception or e_fetch,
                        http_status=e_fetch.http_status,
                        exchange_code=e_fetch.exchange_code,
                        exchange_message=e_fetch.exchange_message,
                    ) from e_fetch
            else:
                # This case implies the status object was not resting, filled, nor had an error message.
                logger.warning(
                    f"[{self.exchange_name}] Order placement status unclear, no OID or error in status object: {first_status_obj.model_dump()}"
                )
                raise APIError(
                    "Order placement status unclear, no OID retrievable from response status object.",
                    code=APIErrorCode.UNKNOWN.value,
                )
        except ValidationError as e:  # Handles validation of HyperliquidRawExchangeResponse
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

        try:
            asset_index = await self._get_asset_index(symbol)
        except Exception as e:
            # Handle potential errors during symbol->index lookup, e.g., symbol not found in meta
            raise APIError(
                f"Failed to map symbol {symbol} to asset index for cancellation: {e}",
                code=APIErrorCode.INVALID_PARAMS.value,  # Or SYMBOL_NOT_FOUND if more appropriate
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

            # At this point, response_data is a list (guaranteed by the check above).
            # The redundant 'if isinstance(response_data, list):' check and its 'else' branch are removed.
            if not response_data:  # Check for empty list
                logger.warning(
                    f"[{self.exchange_name}] get_order_status for {order_id} received empty list."
                )
                raise APIError(
                    f"Order not found (empty list): id={order_id}",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                )

            status_part_raw = response_data[0]  # We know response_data is a non-empty list here.

            if isinstance(status_part_raw, str):
                if status_part_raw.lower() == "order not found":
                    raise APIError(
                        f"Order not found: id={order_id}", code=APIErrorCode.ORDER_NOT_FOUND.value
                    )
                logger.warning(
                    f"[{self.exchange_name}] get_order_status for {order_id} received unexpected string: {status_part_raw}"
                )
                raise APIError(
                    f"Received unexpected string from get_order_status: {status_part_raw}",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )

            if not isinstance(status_part_raw, dict):
                logger.warning(
                    f"[{self.exchange_name}] get_order_status for {order_id} received non-dict status part: {type(status_part_raw)}"
                )
                raise APIError(
                    f"Unexpected status object format, expected dict: {status_part_raw}",
                    code=APIErrorCode.UNKNOWN.value,
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
