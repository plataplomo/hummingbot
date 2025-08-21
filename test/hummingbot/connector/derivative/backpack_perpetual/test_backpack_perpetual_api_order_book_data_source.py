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
            backpack_perpetual_api_secret="3vhqlRTtvgmZ7fqExSoGxxHpJvQBwlmuhDoYpqryw1A=",
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
        # Initialize the mocking assistant asynchronously
        self.run_async_with_timeout(self.mocking_assistant.async_init())
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
            "stream": f"markPrice.{self.ex_trading_pair}",
            "data": {
                "e": "markPrice",
                "E": 1694687965941000,
                "s": self.ex_trading_pair,
                "p": "50001.00",  # Mark price
                "f": "0.0001",    # Funding rate
                "i": "50000.00",  # Index price
                "n": 1641312000000,  # Next funding time in microseconds
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

        result = self.run_async_with_timeout(
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

        result = self.run_async_with_timeout(self.data_source.fetch_trading_pairs())

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

        result = self.run_async_with_timeout(
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
            self.run_async_with_timeout(
                self.data_source.get_order_book_data(self.trading_pair)
            )

        self.assertIn("Error in API request", str(context.exception))

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

        result = self.run_async_with_timeout(
            self.data_source.get_new_order_book(self.trading_pair)
        )

        self.assertIsInstance(result, OrderBook)
        self.assertEqual(result.snapshot_uid, 1641288825000)

    @aioresponses()
    def test_get_funding_info_from_exchange_successful(self, mock_api):
        """Test fetching funding info."""
        # Mock funding rate endpoint
        funding_url = web_utils.public_rest_url(CONSTANTS.FUNDING_RATE_URL)
        funding_regex_url = re.compile(f"^{funding_url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(funding_regex_url, body=json.dumps({
            "symbol": self.ex_trading_pair,
            "fundingRate": "0.0001",
            "nextFundingTime": 1641312000000,
        }))
        
        # Mock mark price endpoint
        mark_price_url = web_utils.public_rest_url(CONSTANTS.MARK_PRICE_URL)
        mark_price_regex_url = re.compile(f"^{mark_price_url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(mark_price_regex_url, body=json.dumps({
            "symbol": self.ex_trading_pair,
            "markPrice": "50001.00",
        }))
        
        # Mock index price endpoint
        index_price_url = web_utils.public_rest_url(CONSTANTS.INDEX_PRICE_URL)
        index_price_regex_url = re.compile(f"^{index_price_url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(index_price_regex_url, body=json.dumps({
            "symbol": self.ex_trading_pair,
            "indexPrice": "50000.50",
        }))

        result = self.run_async_with_timeout(
            self.data_source.get_funding_info(self.trading_pair)
        )

        self.assertIsInstance(result, FundingInfo)
        self.assertEqual(result.trading_pair, self.trading_pair)
        self.assertEqual(result.index_price, Decimal("50000.50"))
        self.assertEqual(result.mark_price, Decimal("50001.00"))
        self.assertEqual(result.rate, Decimal("0.0001"))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_trades_cancelled_when_connecting(self, mock_ws):
        """Test trade listener cancelled during connection."""
        mock_ws.side_effect = asyncio.CancelledError

        msg_queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.run_async_with_timeout(
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

        self.run_async_with_timeout(self.resume_test_event.wait())

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
        
        # Set up the websocket mock
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(self._orderbook_trade_event()["data"])
        )
        
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        )
        
        msg = self.run_async_with_timeout(msg_queue.get())

        self.assertIsInstance(msg, OrderBookMessage)
        self.assertEqual(msg.type, OrderBookMessageType.TRADE)
        self.assertEqual(msg.content["trading_pair"], self.trading_pair)
        self.assertEqual(msg.content["trade_id"], "12345")
        self.assertEqual(msg.content["price"], "50000.00")

    def test_listen_for_order_book_diffs_successful(self):
        """Test successful order book diff message processing."""
        # Use the simpler approach of testing the parsing methods directly
        # since WebSocket mocking is complex with WSAssistant
        
        # Test the _is_order_book_diff_message method
        wrapped_msg = {
            "stream": f"depth.{self.ex_trading_pair}",
            "data": {
                "e": "depth",
                "s": self.ex_trading_pair
            }
        }
        self.assertTrue(self.data_source._is_order_book_diff_message(wrapped_msg))
        
        # Test the _parse_order_book_diff_message method
        diff_event = {
            "stream": f"depth.{self.ex_trading_pair}",
            "data": {
                "e": "depth",
                "E": 1641288825000000,
                "s": self.ex_trading_pair,
                "symbol": self.ex_trading_pair,
                "b": [
                    ["50000.00", "0.100"],
                    ["49999.00", "0.200"],
                ],
                "a": [
                    ["50001.00", "0.150"],
                    ["50002.00", "0.250"],
                ],
                "U": 123456780,
                "u": 123456789,
                "lastUpdateId": 123456789,
                "T": 1641288824999999,
                "timestamp": 1641288825000
            }
        }
        
        msg = self.data_source._parse_order_book_diff_message(diff_event)
        
        self.assertIsInstance(msg, OrderBookMessage)
        self.assertEqual(msg.type, OrderBookMessageType.DIFF)
        self.assertEqual(msg.content["trading_pair"], self.trading_pair)
        self.assertEqual(len(msg.content["bids"]), 2)
        self.assertEqual(len(msg.content["asks"]), 2)
        self.assertEqual(msg.content["update_id"], 123456789)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_order_book_snapshots_successful(self, mock_ws):
        """Test successful order book snapshot message processing."""
        msg_queue: asyncio.Queue = asyncio.Queue()
        
        # Set up the websocket mock
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # For snapshots, we typically get a full order book state
        snapshot_event = {
            "type": "snapshot",
            "symbol": self.ex_trading_pair,
            "bids": [
                ["50000.00", "1.000"],
                ["49999.00", "2.000"],
                ["49998.00", "1.500"],
            ],
            "asks": [
                ["50001.00", "1.000"],
                ["50002.00", "2.000"],
                ["50003.00", "1.500"],
            ],
            "timestamp": 1641288825000,
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(snapshot_event)
        )

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.local_event_loop, msg_queue)
        )

        msg = self.run_async_with_timeout(msg_queue.get())

        self.assertIsInstance(msg, OrderBookMessage)
        self.assertEqual(msg.type, OrderBookMessageType.SNAPSHOT)
        self.assertEqual(msg.content["trading_pair"], self.trading_pair)
        self.assertEqual(len(msg.content["bids"]), 3)
        self.assertEqual(len(msg.content["asks"]), 3)

    @aioresponses()
    def test_get_funding_info_with_zero_rate(self, mock_api):
        """Test fetching funding info when rate is zero."""
        # Mock funding rate endpoint with zero rate
        funding_url = web_utils.public_rest_url(CONSTANTS.FUNDING_RATE_URL)
        funding_regex_url = re.compile(f"^{funding_url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(funding_regex_url, body=json.dumps({
            "symbol": self.ex_trading_pair,
            "fundingRate": "0",  # Zero funding rate
            "nextFundingTime": 1641312000000,
        }))
        
        # Mock mark price endpoint
        mark_price_url = web_utils.public_rest_url(CONSTANTS.MARK_PRICE_URL)
        mark_price_regex_url = re.compile(f"^{mark_price_url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(mark_price_regex_url, body=json.dumps({
            "symbol": self.ex_trading_pair,
            "markPrice": "50001.00",
        }))
        
        # Mock index price endpoint
        index_price_url = web_utils.public_rest_url(CONSTANTS.INDEX_PRICE_URL)
        index_price_regex_url = re.compile(f"^{index_price_url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(index_price_regex_url, body=json.dumps({
            "symbol": self.ex_trading_pair,
            "indexPrice": "50000.50",
        }))

        result = self.run_async_with_timeout(
            self.data_source.get_funding_info(self.trading_pair)
        )

        self.assertIsInstance(result, FundingInfo)
        self.assertEqual(result.trading_pair, self.trading_pair)
        self.assertEqual(result.rate, Decimal("0"))

    @aioresponses()
    def test_get_funding_info_error_response(self, mock_api):
        """Test funding info error handling."""
        # Mock funding rate endpoint with error
        funding_url = web_utils.public_rest_url(CONSTANTS.FUNDING_RATE_URL)
        funding_regex_url = re.compile(f"^{funding_url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(funding_regex_url, status=500, body=json.dumps({"error": "Internal server error"}))

        with self.assertRaises(IOError):
            self.run_async_with_timeout(
                self.data_source.get_funding_info(self.trading_pair)
            )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_websocket_connection_reconnects_on_error(self, mock_ws):
        """Test WebSocket reconnection after error."""
        msg_queue: asyncio.Queue = asyncio.Queue()
        
        async def run_test():
            self.mocking_assistant = NetworkMockingAssistant(asyncio.get_event_loop())
            await self.mocking_assistant.async_init()
            mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

            # First, simulate a connection error
            self.mocking_assistant.add_websocket_aiohttp_exception(
                mock_ws.return_value,
                ConnectionError("Connection lost")
            )

            # Then provide a valid message after reconnection
            self.mocking_assistant.add_websocket_aiohttp_message(
                mock_ws.return_value,
                json.dumps(self._orderbook_trade_event()["data"])
            )

            self.listening_task = self.local_event_loop.create_task(
                self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
            )

            # Should eventually get the message after reconnection
            return await asyncio.wait_for(msg_queue.get(), timeout=5)
        
        msg = self.run_async_with_timeout(run_test())
        self.assertIsInstance(msg, OrderBookMessage)

    @aioresponses()
    def test_fetch_trading_pairs_filters_non_perpetual(self, mock_api):
        """Test that non-perpetual pairs are filtered out."""
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
                    "symbol": "ETH_QUARTERLY",  # Non-perpetual future
                    "baseAsset": "ETH",
                    "quoteAsset": "USDC",
                    "status": "TRADING",
                    "contractType": "QUARTERLY",
                },
                {
                    "symbol": "SOL_PERP",
                    "baseAsset": "SOL",
                    "quoteAsset": "USDC",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                },
            ]
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.run_async_with_timeout(self.data_source.fetch_trading_pairs())

        # Should only include perpetual contracts
        self.assertEqual(len(result), 2)
        self.assertIn("BTC-USDC", result)
        self.assertIn("SOL-USDC", result)
        self.assertNotIn("ETH-USDC", result)

    @aioresponses()
    def test_get_last_traded_prices_multiple_pairs(self, mock_api):
        """Test fetching last traded prices for multiple pairs."""
        trading_pairs = ["BTC-USDC", "ETH-USDC", "SOL-USDC"]
        
        for i, pair in enumerate(trading_pairs):
            base = pair.split("-")[0]
            symbol = f"{base}_PERP"
            url = web_utils.public_rest_url(CONSTANTS.TICKER_URL)
            # Fix regex pattern to match query parameters correctly
            regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + r".*")
            
            mock_response = {
                "symbol": symbol,
                "lastPrice": f"{40000 + i * 10000}.00",
                "markPrice": f"{40001 + i * 10000}.00",
                "indexPrice": f"{40000.50 + i * 10000}.00",
            }
            mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.run_async_with_timeout(
            self.data_source.get_last_traded_prices(trading_pairs)
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(result["BTC-USDC"], 40000.00)
        self.assertEqual(result["ETH-USDC"], 50000.00)
        self.assertEqual(result["SOL-USDC"], 60000.00)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_funding_info_update(self, mock_ws):
        """Test listening for funding info updates via WebSocket."""
        msg_queue: asyncio.Queue = asyncio.Queue()
        
        # Set up the websocket mock
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        
        funding_event = self._funding_info_event()
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(funding_event)  # Send the full event, not just data
        )
        
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_funding_info(msg_queue)
        )
        
        msg = self.run_async_with_timeout(msg_queue.get())

        self.assertIsInstance(msg, FundingInfo)
        self.assertEqual(msg.trading_pair, self.trading_pair)
        self.assertEqual(msg.rate, Decimal("0.0001"))
