import asyncio
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses.core import aioresponses
from bidict import bidict

import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_constants as CONSTANTS
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_api_order_book_data_source import (
    BackpackPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import BackpackPerpetualDerivative
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BackpackPerpetualAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_PERP"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.async_tasks: List[asyncio.Task] = []

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = BackpackPerpetualDerivative(
            client_config_map=client_config_map,
            backpack_perpetual_api_key="testAPIKey",
            backpack_perpetual_api_secret="testSecret",
            trading_pairs=[self.trading_pair],
        )

        self.data_source = BackpackPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)
        self.resume_test_event = asyncio.Event()

        # Set up trading pair symbol map
        self.connector._trading_pair_symbol_map = bidict({self.ex_trading_pair: self.trading_pair})

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        for task in self.async_tasks:
            task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def resume_test_callback(self, *_, **__):
        self.resume_test_event.set()
        return None

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def _raise_exception(self, exception_class):
        raise exception_class

    def _raise_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _orderbook_update_event(self):
        resp = {
            "stream": f"depth@{self.ex_trading_pair}",
            "data": {
                "type": "depth",
                "symbol": self.ex_trading_pair,
                "bids": [
                    ["50000.00", "0.100"],
                    ["49999.00", "0.200"],
                ],
                "asks": [
                    ["50001.00", "0.150"],
                    ["50002.00", "0.250"],
                ],
                "lastUpdateId": 123456789,
                "firstUpdateId": 123456780,
                "timestamp": 1641288825000,
            },
        }
        return resp

    def _orderbook_trade_event(self):
        resp = {
            "stream": f"trades@{self.ex_trading_pair}",
            "data": {
                "type": "trade",
                "symbol": self.ex_trading_pair,
                "tradeId": "12345",
                "price": "50000.00",
                "quantity": "0.100",
                "side": "Buy",
                "timestamp": 1641288825000,
            },
        }
        return resp

    def _funding_info_event(self):
        resp = {
            "stream": f"funding@{self.ex_trading_pair}",
            "data": {
                "type": "funding",
                "symbol": self.ex_trading_pair,
                "fundingRate": "0.0001",
                "markPrice": "50001.00",
                "indexPrice": "50000.00",
                "nextFundingTime": 1641312000000,
            },
        }
        return resp

    @aioresponses()
    def test_get_last_traded_prices(self, mock_api):
        """Test fetching last traded prices."""
        url = web_utils.public_rest_url(CONSTANTS.TICKER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "symbol": self.ex_trading_pair,
            "lastPrice": "50000.00",
            "markPrice": "50001.00",
            "indexPrice": "50000.50",
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.data_source.get_last_traded_prices([self.trading_pair])
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[self.trading_pair], 50000.00)

    @aioresponses()
    def test_fetch_trading_pairs(self, mock_api):
        """Test fetching available trading pairs."""
        url = web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "symbols": [
                {
                    "symbol": "BTC_PERP",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDC",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "ETH_PERP",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDC",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "BTC_USDC",  # Spot pair - should be filtered out
                    "baseAsset": "BTC",
                    "quoteAsset": "USDC",
                    "status": "TRADING",
                },
            ]
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(self.data_source.fetch_trading_pairs())

        self.assertEqual(len(result), 2)
        self.assertIn("BTC-USDC", result)
        self.assertIn("ETH-USDC", result)

    @aioresponses()
    def test_get_order_book_data(self, mock_api):
        """Test fetching order book snapshot."""
        url = web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "symbol": self.ex_trading_pair,
            "bids": [
                ["50000.00", "0.100"],
                ["49999.00", "0.200"],
            ],
            "asks": [
                ["50001.00", "0.150"],
                ["50002.00", "0.250"],
            ],
            "timestamp": 1641288825000,
        }
        mock_api.get(regex_url, status=200, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.data_source.get_order_book_data(self.trading_pair)
        )

        self.assertEqual(result["trading_pair"], self.trading_pair)
        self.assertEqual(result["symbol"], self.ex_trading_pair)
        self.assertEqual(len(result["bids"]), 2)
        self.assertEqual(len(result["asks"]), 2)
        self.assertEqual(result["timestamp"], 1641288825000)

    @aioresponses()
    def test_get_order_book_data_exception_raised(self, mock_api):
        """Test order book snapshot with error response."""
        url = web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=400, body=json.dumps({"error": "Invalid symbol"}))

        with self.assertRaises(IOError) as context:
            self.async_run_with_timeout(
                self.data_source.get_order_book_data(self.trading_pair)
            )

        self.assertIn("Error fetching order book", str(context.exception))

    @aioresponses()
    def test_get_new_order_book(self, mock_api):
        """Test creating new order book from snapshot."""
        url = web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "symbol": self.ex_trading_pair,
            "bids": [["50000.00", "0.100"]],
            "asks": [["50001.00", "0.150"]],
            "timestamp": 1641288825000,
        }
        mock_api.get(regex_url, status=200, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.data_source.get_new_order_book(self.trading_pair)
        )

        self.assertIsInstance(result, OrderBook)
        self.assertEqual(result.snapshot_uid, 1641288825000)

    @aioresponses()
    def test_get_funding_info_from_exchange_successful(self, mock_api):
        """Test fetching funding info."""
        url = web_utils.public_rest_url(CONSTANTS.TICKER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "symbol": self.ex_trading_pair,
            "lastPrice": "50000.00",
            "markPrice": "50001.00",
            "indexPrice": "50000.50",
            "fundingRate": "0.0001",
            "nextFundingTime": 1641312000000,
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.data_source.get_funding_info(self.trading_pair)
        )

        self.assertIsInstance(result, FundingInfo)
        self.assertEqual(result.trading_pair, self.trading_pair)
        self.assertEqual(result.index_price, Decimal("50000.50"))
        self.assertEqual(result.mark_price, Decimal("50001.00"))
        self.assertEqual(result.next_funding_utc_timestamp, 1641312000)
        self.assertEqual(result.rate, Decimal("0.0001"))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_trades_cancelled_when_connecting(self, mock_ws):
        """Test trade listener cancelled during connection."""
        mock_ws.side_effect = asyncio.CancelledError

        msg_queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.async_run_with_timeout(
                self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
            )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    def test_listen_for_trades_logs_exception(self, mock_sleep, mock_ws):
        """Test trade listener error handling."""
        msg_queue: asyncio.Queue = asyncio.Queue()
        mock_sleep.side_effect = lambda _: self._raise_exception_and_unlock_test_with_event(asyncio.CancelledError)
        mock_ws.side_effect = Exception("TEST ERROR")

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        )

        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Error in trade WebSocket listener",
            )
        )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_trades_successful(self, mock_ws):
        """Test successful trade message processing."""
        msg_queue: asyncio.Queue = asyncio.Queue()
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(self._orderbook_trade_event()["data"])
        )

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        )

        msg = self.async_run_with_timeout(msg_queue.get())

        self.assertIsInstance(msg, OrderBookMessage)
        self.assertEqual(msg.type, OrderBookMessageType.TRADE)
        self.assertEqual(msg.content["trading_pair"], self.trading_pair)
        self.assertEqual(msg.content["trade_id"], "12345")
        self.assertEqual(msg.content["price"], "50000.00")
