import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS, backpack_web_utils as web_utils
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange


class BackpackAPIUserStreamDataSource(UserStreamTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = CONSTANTS.HEARTBEAT_TIME_INTERVAL

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: BackpackAuth,
        trading_pairs: List[str],
        connector: "BackpackExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    async def _get_ws_assistant(self) -> WSAssistant:
        return await self._api_factory.get_ws_assistant()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws = await self._get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.ws_private_url(self._domain),
            ping_timeout=self.HEARTBEAT_TIME_INTERVAL,
        )
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            timestamp = str(self._auth._get_timestamp())
            window = str(CONSTANTS.AUTH_WINDOW_MS)

            auth_payload = f"instruction={CONSTANTS.WS_AUTH_INSTRUCTION}&timestamp={timestamp}&window={window}"
            signature = self._auth._generate_signature(auth_payload)

            subscriptions = [CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL]

            subscription_payload = {
                "method": "SUBSCRIBE",
                "params": subscriptions,
                "signature": [
                    self._auth.api_key,
                    signature,
                    timestamp,
                    window,
                ],
            }

            subscribe_request = WSJSONRequest(payload=subscription_payload)
            await websocket_assistant.send(subscribe_request)

            self.logger().info(f"Subscribed to private channels: {subscriptions}")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to private user stream...",
                exc_info=True,
            )
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        try:
            if not isinstance(event_message, dict) or not event_message:
                return

            if event_message.get("result") == "success" or event_message.get("type") == "authenticated":
                return

            stream_name = event_message.get("stream", "")
            if not stream_name:
                return

            data = event_message.get("data", event_message)

            if stream_name == CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL or "orderUpdate" in stream_name:
                queue.put_nowait(
                    {
                        "stream": stream_name,
                        "data": data,
                        "message_type": "order_update",
                    }
                )
                if isinstance(data, dict) and "balances" in data:
                    queue.put_nowait(
                        {
                            "stream": stream_name,
                            "data": data,
                            "message_type": "balance_update",
                        }
                    )
            elif "fill" in stream_name.lower() or "trade" in stream_name.lower():
                queue.put_nowait(
                    {
                        "stream": stream_name,
                        "data": data,
                        "message_type": "trade_update",
                    }
                )
        except Exception:
            self.logger().error("Error processing user stream message", exc_info=True)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        await super()._on_user_stream_interruption(websocket_assistant)
