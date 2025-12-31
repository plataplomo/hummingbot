import asyncio
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

from bidict import bidict

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack.backpack_api_user_stream_data_source import BackpackAPIUserStreamDataSource
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class BackpackUserStreamDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_{cls.quote_asset}"
        cls.domain = CONSTANTS.DEFAULT_DOMAIN

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

        self.throttler = AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000
        self.auth = BackpackAuth(api_key="TEST_API_KEY", api_secret="", time_provider=self.mock_time_provider)
        self.auth._get_timestamp = MagicMock(return_value=1650000000000)
        self.auth._generate_signature = MagicMock(return_value="TEST_SIGNATURE")
        self.time_synchronizer = TimeSynchronizer()
        self.time_synchronizer.add_time_offset_ms_sample(0)

        self.connector = BackpackExchange(
            backpack_api_key="",
            backpack_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain=self.domain,
        )
        self.connector._web_assistants_factory._auth = self.auth

        self.data_source = BackpackAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            domain=self.domain,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _user_update_event(self) -> str:
        resp = {
            "stream": CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL,
            "data": {
                "e": "orderAccepted",
                "s": self.ex_trading_pair,
            },
        }
        return json.dumps(resp)

    def _expected_order_update_message(self) -> dict:
        return {
            "stream": CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL,
            "data": {
                "e": "orderAccepted",
                "s": self.ex_trading_pair,
            },
            "message_type": "order_update",
        }

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_get_user_update_event(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, self._user_update_event())

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        msg = await msg_queue.get()
        self.assertEqual(self._expected_order_update_message(), msg)
        mock_ws.return_value.ping.assert_called()

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_subscribes_to_private_channels(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, json.dumps({}))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(mock_ws.return_value)

        expected_message = {
            "method": "SUBSCRIBE",
            "params": [CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL],
            "signature": [
                self.auth.api_key,
                "TEST_SIGNATURE",
                "1650000000000",
                str(CONSTANTS.AUTH_WINDOW_MS),
            ],
        }
        self.assertEqual(expected_message, sent_messages[0])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_does_not_queue_empty_payload(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, "")

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_connection_failed(self, mock_ws):
        mock_ws.side_effect = lambda *arg, **kwars: self._create_exception_and_unlock_test_with_event(
            Exception("TEST ERROR.")
        )

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        await self.resume_test_event.wait()

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds...")
        )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_iter_message_throws_exception(self, mock_ws):
        msg_queue: asyncio.Queue = asyncio.Queue()
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        mock_ws.return_value.receive.side_effect = lambda *args, **kwargs: self._create_exception_and_unlock_test_with_event(
            Exception("TEST ERROR")
        )
        mock_ws.close.return_value = None

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        await self.resume_test_event.wait()

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds...")
        )
