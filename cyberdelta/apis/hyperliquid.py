import asyncio
import time
from decimal import Decimal
from typing import Any

import aiohttp
import structlog
from eth_account.messages import encode_typed_data
from eth_account.signers.local import LocalAccount
from web3.auto import w3

from ..core.models import (
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
from .base import APIError, APIErrorCode, ExchangeAPI, MessageHandler

logger = structlog.get_logger(__name__)


class HyperliquidAPI(ExchangeAPI):
    """API Client for Hyperliquid DEX."""

    BASE_URL = "https://api.hyperliquid.xyz"
    INFO_URL = "https://info.hyperliquid.xyz"
    WS_URL = "wss://api.hyperliquid.xyz/ws"
    CHAIN_ID = 1337  # Hyperliquid L1 chain ID (adjust if necessary)

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        super().__init__("hyperliquid", api_config, secrets)
        # Specific Hyperliquid initialization
        self._private_key = secrets.get("HYPERLIQUID_WALLET_PRIVATE_KEY")
        self._wallet_address = secrets.get("HYPERLIQUID_WALLET_ADDRESS")
        self._nonce_counter: int = 0
        self._nonce_lock = asyncio.Lock()

        # Check if we can sign transactions
        if not self._private_key:
            logger.warning("Hyperliquid private key not provided. Signed operations will fail.")
            self.account: LocalAccount | None = None
        else:
            self.account = w3.eth.account.from_key(self._private_key)
            if self.account is None:
                raise ValueError("Failed to create account from private key.")
            if (
                self._wallet_address is None
                or self.account.address.lower() != self._wallet_address.lower()
            ):
                raise ValueError(
                    f"Wallet address mismatch or not provided: "
                    f"Account: {self.account.address}, Provided: {self._wallet_address}"
                )

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
        params: dict | None = None,
        data: dict | None = None,
    ) -> dict[str, Any]:
        """
        Sign a request using EIP-712 and wallet private key.

        Args:
            method: HTTP method
            path: API endpoint
            params: URL parameters
            data: Request body

        Returns:
            Authentication data for the request
        """
        if not self.account:
            if "test" in path or (data and isinstance(data, dict) and data.get("test")):
                logger.info(
                    f"[{self.exchange_name}] Using mock authentication for test environment (no private key)"
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

        structured_data = {
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
                "source": "aix",
                "connectionId": b"\x00" * 32,
                "timestamp": timestamp,
            },
        }

        try:
            signable_message = encode_typed_data(structured_data)
            signed_message = self.account.sign_message(signable_message)
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
        """Subscribe to a Hyperliquid WebSocket topic."""
        if not self._ws_connection or not self.is_connected:
            logger.error(f"[{self.exchange_name}] Cannot subscribe, WebSocket not connected.")
            self._ws_handlers[topic] = handler
            return

        subscription_payload: dict[str, Any] = {
            "type": topic.split(":")[0],
        }
        if ":" in topic:
            subscription_payload["coin"] = topic.split(":")[1]

        subscription_message = {
            "method": "subscribe",
            "subscription": subscription_payload,
        }

        try:
            await self._ws_connection.send_json(subscription_message)
            self._ws_handlers[topic] = handler
            logger.info(f"[{self.exchange_name}] Subscribed to topic: {topic}")
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Failed to subscribe to topic {topic}: {e}")

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

    async def get_balances(self) -> dict[str, Balance]:
        """Get account balances."""
        try:
            payload = {"type": "clearinghouseState", "user": self._wallet_address}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            if isinstance(response, list) and len(response) > 0:
                state_data = response[0].get("clearinghouseState")
            elif isinstance(response, dict):
                state_data = response.get("clearinghouseState")
            else:
                state_data = None

            if state_data and "assetPositions" in state_data:
                balances: dict[str, Balance] = {}
                for asset_pos in state_data["assetPositions"]:
                    if asset_pos.get("asset") == "USDC":
                        total_balance_str = asset_pos["position"].get("value", "0")
                        total_balance = Decimal(total_balance_str)
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
            payload = {"type": "clearinghouseState", "user": self._wallet_address}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            positions_list: list[Position] = []
            if isinstance(response, list) and len(response) > 0:
                state_data = response[0].get("clearinghouseState")
            elif isinstance(response, dict):
                state_data = response.get("clearinghouseState")
            else:
                state_data = None

            if state_data and "assetPositions" in state_data:
                for asset_pos in state_data["assetPositions"]:
                    position_data = asset_pos.get("position")
                    if position_data:
                        pos_symbol = asset_pos.get("asset")
                        if symbol is not None and pos_symbol != symbol:
                            continue

                        size_str = position_data.get("szi", "0")
                        entry_price_str = position_data.get("entryPx", "0")
                        unrealized_pnl_str = position_data.get("unrealizedPnl", "0")

                        size = Decimal(size_str)
                        entry_price = Decimal(entry_price_str) if entry_price_str else None
                        unrealized_pnl = Decimal(unrealized_pnl_str)

                        if size != Decimal(0) and pos_symbol and entry_price is not None:
                            side = OrderSide.BUY if size > 0 else OrderSide.SELL
                            leverage_placeholder = Decimal("1")

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
        price: Decimal | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
        **kwargs: Any,
    ) -> Order:
        """Place a new order."""
        if not self.account:
            raise APIError(
                "Cannot place order without initialized account/private key",
                code=APIErrorCode.AUTHENTICATION_FAILED,
            )

        is_buy = side == OrderSide.BUY
        sz = float(quantity)

        order_type_hl: dict[str, Any]
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
            "limitPx": limit_px_str if order_type == OrderType.LIMIT else "0",
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

            if response and response.get("status") == "ok":
                statuses = response.get("data", {}).get("statuses", [])
                if statuses:
                    order_status_data = statuses[0]
                    if "filled" in order_status_data:
                        filled_data = order_status_data["filled"]
                        return Order(
                            id=str(filled_data.get("oid", "")),
                            symbol=symbol,
                            side=side,
                            type=order_type,
                            quantity=quantity,
                            filled_quantity=Decimal(str(filled_data.get("totalSz", "0"))),
                            price=price,
                            avg_fill_price=Decimal(str(filled_data.get("avgPx", "0"))),
                            status=OrderStatus.FILLED,
                            time=int(filled_data.get("time", time.time() * 1000)),
                            client_order_id=client_order_id or "",
                        )
                    elif "resting" in order_status_data:
                        resting_data = order_status_data["resting"]
                        return Order(
                            id=str(resting_data.get("oid", "")),
                            symbol=symbol,
                            side=side,
                            type=order_type,
                            quantity=quantity,
                            filled_quantity=Decimal("0"),
                            price=price,
                            avg_fill_price=None,
                            status=OrderStatus.OPEN,
                            time=int(resting_data.get("time", time.time() * 1000)),
                            client_order_id=client_order_id or "",
                        )
                    elif "error" in order_status_data:
                        error_msg = order_status_data["error"]
                        logger.error(f"[{self.exchange_name}] Order placement failed: {error_msg}")
                        raise APIError(
                            f"Order placement failed: {error_msg}", code=APIErrorCode.ORDER_REJECTED
                        )
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Unhandled order status in response: {order_status_data}"
                        )
                        raise APIError("Unhandled order status", code=APIErrorCode.UNKNOWN)

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
        if not self.account:
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

            if response and response.get("status") == "ok":
                statuses = response.get("data", {}).get("statuses", [])
                if statuses and statuses[0] == "canceled":
                    logger.info(f"[{self.exchange_name}] Canceled order {order_id} for {symbol}")
                    return {"status": "canceled", "order_id": order_id, "symbol": symbol}
                elif statuses and "error" in statuses[0]:
                    error_msg = statuses[0]["error"]
                    logger.error(
                        f"[{self.exchange_name}] Failed to cancel order {order_id}: {error_msg}"
                    )
                    code = APIErrorCode.EXCHANGE_SPECIFIC
                    if "Order not found" in error_msg:
                        code = APIErrorCode.ORDER_NOT_FOUND
                    raise APIError(f"Failed to cancel order: {error_msg}", code=code)
                else:
                    logger.warning(
                        f"[{self.exchange_name}] Order {order_id} cancel response status OK, but data unexpected: {statuses}"
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
        """Get current ticker information for a symbol."""
        try:
            payload = {"type": "metaAndAssetCtxs"}
            response = await self._request("POST", self.INFO_URL + "/info", data=payload)

            if isinstance(response, list) and len(response) > 1:
                asset_contexts = response[1]
                for ctx in asset_contexts:
                    if ctx.get("name") == symbol:
                        mark_px = ctx.get("markPx", "0")
                        return Ticker(
                            symbol=symbol,
                            price=Decimal(mark_px),
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

            if response and "levels" in response:
                levels = response["levels"]
                bids = []
                asks = []
                for level in levels[0]:
                    try:
                        if isinstance(level, list) and len(level) == 2:
                            bids.append((Decimal(str(level[0])), Decimal(str(level[1]))))
                    except Exception as parse_err:
                        logger.warning(f"Error parsing bid level {level}: {parse_err}")
                for level in levels[1]:
                    try:
                        if isinstance(level, list) and len(level) == 2:
                            asks.append((Decimal(str(level[0])), Decimal(str(level[1]))))
                    except Exception as parse_err:
                        logger.warning(f"Error parsing ask level {level}: {parse_err}")

                bids.sort(key=lambda x: x[0], reverse=True)
                asks.sort(key=lambda x: x[0])

                if depth:
                    bids = bids[:depth]
                    asks = asks[:depth]

                return OrderBook(
                    symbol=symbol,
                    bids=bids,
                    asks=asks,
                    timestamp=int(response.get("time", time.time() * 1000)),
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
                for trade_data in response:
                    trade_id = str(trade_data.get("tid", ""))
                    price_str = trade_data.get("px", "0")
                    size_str = trade_data.get("sz", "0")
                    timestamp_ms = trade_data.get("time", 0)
                    side_hl = trade_data.get("side", "B")

                    side = OrderSide.BUY if side_hl == "B" else OrderSide.SELL

                    trades.append(
                        Trade(
                            id=trade_id,
                            symbol=symbol,
                            timestamp=timestamp_ms,
                            side=side,
                            price=Decimal(price_str),
                            quantity=Decimal(size_str),
                        )
                    )
                if limit is not None:
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
                for ctx in asset_contexts:
                    if ctx.get("name") == symbol:
                        funding_rate_str = ctx.get("funding", "0")
                        mark_px_str = ctx.get("markPx", "0")

                        funding_rate = Decimal(funding_rate_str)
                        mark_price = Decimal(mark_px_str)

                        now_ms = int(time.time() * 1000)
                        next_funding_time_ms = (now_ms // 3600000 + 1) * 3600000

                        funding_info = FundingRate(
                            symbol=symbol,
                            funding_rate=funding_rate,
                            mark_price=mark_price,
                            next_funding_time=next_funding_time_ms,
                        )
                        return funding_info

            logger.warning(
                f"[{self.exchange_name}] Funding rate data not found for {symbol} in response: {response}"
            )
            return None
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting funding rate for {symbol}: {e}")
            if "Funding rate not available" in str(e.original_exception or ""):
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
        if not self.account:
            raise APIError(
                "Cannot withdraw without initialized account/private key",
                code=APIErrorCode.AUTHENTICATION_FAILED,
            )

        logger.warning(
            f"[{self.exchange_name}] Withdrawal endpoint requires careful implementation and testing."
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

        if error_data and isinstance(error_data, dict):
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


    async def subscribe_to_trades(self, symbol: str, handler: MessageHandler) -> None:
        """Subscribe to public trades for a symbol."""
        await self.subscribe(f"trades:{symbol}", handler)


    async def subscribe_to_ticker(self, symbol: str, handler: MessageHandler) -> None:
        """Subscribe to ticker updates for a symbol."""
        await self.subscribe("allMids", handler)
        logger.warning(f"[{self.exchange_name}] Ticker subscription needs topic verification.")


    async def subscribe_to_order_book(self, symbol: str, handler: MessageHandler) -> None:
        """Subscribe to order book updates for a symbol."""
        await self.subscribe(f"l2Book:{symbol}", handler)

    # --- Helper Methods (Parsing, etc.) ---

    def parse_order(self, order_data: dict[str, Any]) -> Order:
        try:
            order_id = str(order_data.get("oid"))
            parsed_symbol = order_data.get("coin")
            if parsed_symbol is None:
                raise ValueError(f"Missing 'coin' (symbol) in order data: {order_data}")

            cloid = order_data.get("cloid")
            side_hl = order_data.get("side")
            qty_str = order_data.get("sz")
            limit_px_str = order_data.get("limitPx")
            order_type_hl = order_data.get("orderType")
            timestamp_ms = order_data.get("timestamp")
            if timestamp_ms is None:
                raise ValueError(f"Missing 'timestamp' in order data: {order_data}")

            status_str = order_data.get("status", "unknown")

            if not all([order_id, side_hl, qty_str]):
                logger.warning(
                    f"[{self.exchange_name}] Incomplete essential order data: {order_data}"
                )
                raise ValueError(f"Incomplete essential order data: {order_data}")

            side = OrderSide.BUY if side_hl == "B" else OrderSide.SELL
            quantity = Decimal(qty_str) if qty_str else Decimal("0")
            price = Decimal(limit_px_str) if limit_px_str and limit_px_str != "0" else None

            order_type = OrderType.LIMIT
            if isinstance(order_type_hl, dict) and "market" in order_type_hl:
                order_type = OrderType.MARKET
                price = None

            status = OrderStatus.UNKNOWN
            if status_str == "open":
                status = OrderStatus.OPEN
            elif status_str == "filled":
                status = OrderStatus.FILLED
            elif status_str == "canceled":
                status = OrderStatus.CANCELED

            filled_qty = Decimal("0")
            avg_fill_price = None

            return Order(
                id=order_id,
                symbol=parsed_symbol,
                side=side,
                type=order_type,
                quantity=quantity,
                price=price,
                filled_quantity=filled_qty,
                avg_fill_price=avg_fill_price,
                status=status,
                time=int(timestamp_ms),
                client_order_id=cloid or "",
            )
        except (ValueError, TypeError, KeyError) as e:
            logger.error(
                f"[{self.exchange_name}] Failed to parse order data: {order_data}, Error: {e}",
                exc_info=True,
            )
            raise ValueError(f"Failed to parse order data: {e}") from e

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        raise NotImplementedError("get_order not implemented for HyperliquidAPI")

    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        raise NotImplementedError("cancel_all_orders not implemented for HyperliquidAPI")

    async def get_order_history(self, symbol: str | None = None, limit: int = 100) -> list[Order]:
        raise NotImplementedError("get_order_history not implemented for HyperliquidAPI")

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        raise NotImplementedError("get_trade_history not implemented for HyperliquidAPI")

    async def get_funding_rates(self, symbol: str | None = None) -> list[FundingRate]:
        raise NotImplementedError(
            "get_funding_rates (historical) not implemented for HyperliquidAPI"
        )

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
        if message.get("channel") == "allMids":
            logger.warning(
                f"[{self.exchange_name}] parse_ticker_message needs specific implementation for 'allMids' structure."
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
        if message.get("channel", "").startswith("trades:"):
            symbol = message["channel"].split(":")[1]
            data_list = message.get("data")
            if isinstance(data_list, list) and data_list:
                trade_data = data_list[0]
                trade = Trade(
                    id=str(trade_data.get("tid", "")),
                    symbol=symbol,
                    timestamp=int(trade_data.get("time", 0)),
                    side=OrderSide.BUY if trade_data.get("side", "B") == "B" else OrderSide.SELL,
                    price=Decimal(trade_data.get("px", "0")),
                    quantity=Decimal(trade_data.get("sz", "0")),
                )
                return trade
            logger.warning(f"[{self.exchange_name}] parse_trade_message needs full implementation.")
        return None

    def parse_account_update_message(
        self, message: dict[str, Any]
    ) -> tuple[dict[str, Balance] | None, dict[str, Position] | None]:
        if message.get("channel") == "user":
            data = message.get("data")
            if data and "clearinghouseState" in data:
                logger.warning(
                    f"[{self.exchange_name}] parse_account_update_message needs full implementation."
                )
            return None, None
        return None, None

    def parse_order_update_message(self, message: dict[str, Any]) -> Order | None:
        if message.get("channel") == "user":
            data = message.get("data")
            if data and isinstance(data, dict) and data.get("order"):
                order_data = data["order"]
                return self.parse_order(order_data)
            elif data and isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("order"):
                        return self.parse_order(item["order"])

        logger.debug(f"[{self.exchange_name}] Could not parse order update from message: {message}")
        return None

    def parse_funding_rate_message(self, message: dict[str, Any]) -> FundingRate | None:
        logger.warning(f"[{self.exchange_name}] parse_funding_rate_message needs WS update impl.")
        return None
