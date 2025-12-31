import asyncio
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Dict
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses.core import aioresponses
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_api_order_book_data_source import (
    BackpackPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import BackpackPerpetualDerivative
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BackpackPerpetualAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_{cls.quote_asset}_PERP"
        cls.domain = CONSTANTS.DEFAULT_DOMAIN

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = BackpackPerpetualDerivative(
            client_config_map,
            backpack_perpetual_api_key="",
            backpack_perpetual_api_secret="",
            trading_pairs=[self.trading_pair],
            trading_required=False,
            domain=self.domain,
        )
        self.data_source = BackpackPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            domain=self.domain,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self._original_full_order_book_reset_time = self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = -1

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _trade_update_event(self) -> Dict:
        return {
            "stream": f"{CONSTANTS.WS_TRADES_CHANNEL}.{self.ex_trading_pair}",
            "data": {
                "e": "trade",
                "s": self.ex_trading_pair,
                "t": 12345,
                "p": "0.001",
                "q": "100",
                "m": True,
                "T": 1234567890123,
            },
        }

    def _order_diff_event(self) -> Dict:
        return {
            "stream": f"{CONSTANTS.WS_DEPTH_CHANNEL}.{self.ex_trading_pair}",
            "data": {
                "e": "depth",
                "s": self.ex_trading_pair,
                "U": 157,
                "u": 160,
                "b": [["0.0024", "10"]],
                "a": [["0.0026", "100"]],
                "T": 1234567890123,
            },
        }

    def _funding_info_event(self) -> Dict:
        return {
            "stream": f"{CONSTANTS.WS_MARK_PRICE_CHANNEL}.{self.ex_trading_pair}",
            "data": {
                "e": "markPrice",
                "s": self.ex_trading_pair,
                "p": "46353.99600757",
                "i": "46358.63622407",
                "f": "0.00010000",
                "n": 1641312000000,
                "T": 1641312000000000,
            },
        }

    def _snapshot_response(self) -> Dict:
        return {
            "lastUpdateId": "1027024",
            "bids": [["10", "1"]],
            "asks": [["11", "1"]],
            "timestamp": 1589436922959000,
        }

    @aioresponses()
    async def test_get_new_order_book_successful(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_URL, domain=self.domain)
        regex_url = re.compile(f"^{re.escape(url)}")

        resp = self._snapshot_response()
        mock_api.get(regex_url, body=json.dumps(resp))

        order_book: OrderBook = await self.data_source.get_new_order_book(self.trading_pair)

        expected_update_id = int(resp["lastUpdateId"])

        self.assertEqual(expected_update_id, order_book.snapshot_uid)
        bids = list(order_book.bid_entries())
        asks = list(order_book.ask_entries())
        self.assertEqual(1, len(bids))
        self.assertEqual(10, bids[0].price)
        self.assertEqual(1, bids[0].amount)
        self.assertEqual(expected_update_id, bids[0].update_id)
        self.assertEqual(1, len(asks))
        self.assertEqual(11, asks[0].price)
        self.assertEqual(1, asks[0].amount)
        self.assertEqual(expected_update_id, asks[0].update_id)

    @aioresponses()
    async def test_get_new_order_book_raises_exception(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_URL, domain=self.domain)
        regex_url = re.compile(f"^{re.escape(url)}")

        mock_api.get(regex_url, status=400)
        with self.assertRaises(IOError):
            await self.data_source.get_new_order_book(self.trading_pair)

    @aioresponses()
    async def test_get_snapshot_successful(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_URL, domain=self.domain)
        regex_url = re.compile(f"^{re.escape(url)}")
        mock_response = self._snapshot_response()
        mock_api.get(regex_url, status=200, body=json.dumps(mock_response))

        result: Dict = await self.data_source._request_order_book_snapshot(trading_pair=self.trading_pair)
        self.assertEqual(mock_response, result)

    @aioresponses()
    async def test_get_snapshot_exception_raised(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_URL, domain=self.domain)
        regex_url = re.compile(f"^{re.escape(url)}")
        mock_api.get(regex_url, status=400, body=json.dumps(["ERROR"]))

        with self.assertRaises(IOError):
            await self.data_source._order_book_snapshot(trading_pair=self.trading_pair)

    @aioresponses()
    async def test_get_funding_info(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARK_PRICE_URL, domain=self.domain)
        regex_url = re.compile(f"^{re.escape(url)}")

        mock_response = [
            {
                "symbol": self.ex_trading_pair,
                "markPrice": "46382.32704603",
                "indexPrice": "46385.80064948",
                "fundingRate": "0.00010000",
                "nextFundingTimestamp": 1641312000000,
            }
        ]
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = await self.data_source.get_funding_info(trading_pair=self.trading_pair)

        self.assertIsInstance(result, FundingInfo)
        self.assertEqual(result.trading_pair, self.trading_pair)
        self.assertEqual(result.index_price, Decimal(mock_response[0]["indexPrice"]))
        self.assertEqual(result.mark_price, Decimal(mock_response[0]["markPrice"]))
        self.assertEqual(result.rate, Decimal(mock_response[0]["fundingRate"]))
        self.assertEqual(result.next_funding_utc_timestamp, int(mock_response[0]["nextFundingTimestamp"]) // 1000)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_subscribes_to_trades_and_order_diffs_and_funding_info(
        self, ws_connect_mock
    ):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        result_subscribe = {
            "result": "success",
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe),
        )

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_subscriptions())

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value
        )

        self.assertEqual(1, len(sent_subscription_messages))
        expected_subscription = {
            "method": "SUBSCRIBE",
            "params": [
                f"{CONSTANTS.WS_DEPTH_CHANNEL}.{self.ex_trading_pair}",
                f"{CONSTANTS.WS_TRADES_CHANNEL}.{self.ex_trading_pair}",
                f"{CONSTANTS.WS_MARK_PRICE_CHANNEL}.{self.ex_trading_pair}",
            ],
        }
        self.assertEqual(expected_subscription, sent_subscription_messages[0])

        self.assertTrue(self._is_logged(
            "INFO",
            f"Subscribed to public channels: {expected_subscription['params']}"
        ))

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_listen_for_subscriptions_raises_cancel_exception(self, mock_ws, _: AsyncMock):
        mock_ws.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_subscriptions()

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_logs_exception_details(self, mock_ws, sleep_mock):
        mock_ws.side_effect = Exception("TEST ERROR.")
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_subscriptions())

        await self.resume_test_event.wait()

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
            )
        )

    async def test_subscribe_channels_raises_cancel_exception(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_ws)

    async def test_subscribe_channels_raises_exception_and_logs_error(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Test Error")

        with self.assertRaises(Exception):
            await self.data_source._subscribe_channels(mock_ws)

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error occurred subscribing to order book and trade streams")
        )

    async def test_channel_originating_message_returns_correct(self):
        event_type = self._order_diff_event()
        event_message = self.data_source._channel_originating_message(event_type)
        self.assertEqual(self.data_source._diff_messages_queue_key, event_message)

        event_type = self._funding_info_event()
        event_message = self.data_source._channel_originating_message(event_type)
        self.assertEqual(self.data_source._funding_info_messages_queue_key, event_message)

        event_type = self._trade_update_event()
        event_message = self.data_source._channel_originating_message(event_type)
        self.assertEqual(self.data_source._trade_messages_queue_key, event_message)

    async def test_listen_for_trades_cancelled_when_listening(self):
        mock_queue = MagicMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_trades(self.local_event_loop, msg_queue)

    async def test_listen_for_trades_logs_exception(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = ["bad_message", asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            await self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public trade updates from exchange")
        )

    async def test_listen_for_trades_successful(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [self._trade_update_event(), asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        )

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(12345, int(msg.trade_id))
        self.assertEqual(1234567890123 / 1_000_000, msg.timestamp)

    async def test_listen_for_order_book_diffs_cancelled(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue)

    async def test_listen_for_order_book_diffs_logs_exception(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = ["bad_message", asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            await self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public order book updates from exchange")
        )

    async def test_listen_for_order_book_diffs_successful(self):
        mock_queue = AsyncMock()
        diff_event = self._order_diff_event()
        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue)
        )

        msg: OrderBookMessage = await msg_queue.get()
        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(-1, msg.trade_id)
        self.assertEqual(1234567890123 / 1_000_000, msg.timestamp)
        self.assertEqual(diff_event["data"]["u"], msg.update_id)

        bids = msg.bids
        asks = msg.asks
        self.assertEqual(1, len(bids))
        self.assertEqual(0.0024, bids[0].price)
        self.assertEqual(10, bids[0].amount)
        self.assertEqual(1, len(asks))
        self.assertEqual(0.0026, asks[0].price)
        self.assertEqual(100, asks[0].amount)

    @aioresponses()
    async def test_listen_for_order_book_snapshots_cancelled_when_fetching_snapshot(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_URL, domain=self.domain)
        regex_url = re.compile(f"^{re.escape(url)}")

        mock_api.get(regex_url, exception=asyncio.CancelledError, repeat=True)

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_order_book_snapshots(self.local_event_loop, asyncio.Queue())

    @aioresponses()
    @patch("hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_api_order_book_data_source"
           ".BackpackPerpetualAPIOrderBookDataSource._sleep")
    async def test_listen_for_order_book_snapshots_log_exception(self, mock_api, sleep_mock):
        msg_queue: asyncio.Queue = asyncio.Queue()
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_URL, domain=self.domain)
        regex_url = re.compile(f"^{re.escape(url)}")

        mock_api.get(regex_url, exception=Exception, repeat=True)

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.local_event_loop, msg_queue)
        )
        await self.resume_test_event.wait()

        self.assertTrue(
            self._is_logged("ERROR", f"Unexpected error fetching order book snapshot for {self.trading_pair}.")
        )

    @aioresponses()
    async def test_listen_for_order_book_snapshots_successful(self, mock_api):
        msg_queue: asyncio.Queue = asyncio.Queue()
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_URL, domain=self.domain)
        regex_url = re.compile(f"^{re.escape(url)}")

        mock_api.get(regex_url, body=json.dumps(self._snapshot_response()))

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.local_event_loop, msg_queue)
        )

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(-1, msg.trade_id)
        self.assertEqual(1589436922959000 / 1_000_000, msg.timestamp)
        expected_update_id = int(self._snapshot_response()["lastUpdateId"])
        self.assertEqual(expected_update_id, msg.update_id)

        bids = msg.bids
        asks = msg.asks
        self.assertEqual(1, len(bids))
        self.assertEqual(10, bids[0].price)
        self.assertEqual(1, bids[0].amount)
        self.assertEqual(1, len(asks))
        self.assertEqual(11, asks[0].price)
        self.assertEqual(1, asks[0].amount)

    async def test_listen_for_funding_info_cancelled_when_listening(self):
        mock_queue = MagicMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._funding_info_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_funding_info(msg_queue)

    async def test_listen_for_funding_info_logs_exception(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = ["bad_message", asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._funding_info_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            await self.data_source.listen_for_funding_info(msg_queue)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public funding info updates from exchange")
        )

    async def test_listen_for_funding_info_successful(self):
        funding_info_event = self._funding_info_event()

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [funding_info_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._funding_info_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_funding_info(msg_queue))

        msg: FundingInfoUpdate = await msg_queue.get()
        funding_update = funding_info_event["data"]

        self.assertEqual(self.trading_pair, msg.trading_pair)
        expected_index_price = Decimal(str(funding_update["i"]))
        self.assertEqual(expected_index_price, msg.index_price)
        expected_mark_price = Decimal(str(funding_update["p"]))
        self.assertEqual(expected_mark_price, msg.mark_price)
        expected_funding_time = int(funding_update["n"]) // 1000
        self.assertEqual(expected_funding_time, msg.next_funding_utc_timestamp)
