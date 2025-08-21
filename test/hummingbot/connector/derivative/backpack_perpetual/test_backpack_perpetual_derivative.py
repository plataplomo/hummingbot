import asyncio
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
from aioresponses.core import aioresponses
from bidict import bidict

import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_web_utils as web_utils
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import BackpackPerpetualDerivative
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent


class BackpackPerpetualDerivativeUnitTest(IsolatedAsyncioWrapperTestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    start_timestamp: float = pd.Timestamp("2021-01-01", tz="UTC").timestamp()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.symbol = f"{cls.base_asset}_PERP"
        cls.listen_key = "TEST_LISTEN_KEY"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []

        self.ws_sent_messages = []
        self.ws_incoming_messages = asyncio.Queue()
        self.resume_test_event = asyncio.Event()
        self.client_config_map = ClientConfigAdapter(ClientConfigMap())

        self.exchange = BackpackPerpetualDerivative(
            client_config_map=self.client_config_map,
            backpack_perpetual_api_key="testAPIKey",
            backpack_perpetual_api_secret="testSecret",
            trading_pairs=[self.trading_pair],
        )

        self.exchange._set_current_timestamp(1640780000)
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.exchange._order_tracker.logger().setLevel(1)
        self.exchange._order_tracker.logger().addHandler(self)
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)
        self.test_task: Optional[asyncio.Task] = None
        self.resume_test_event = asyncio.Event()
        self._initialize_event_loggers()

    @property
    def all_symbols_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_URL)
        return url

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_URL)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def network_status_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.PING_URL)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_URL)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.ACCOUNT_INFO_URL)
        return url

    @property
    def funding_info_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_URL)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def funding_payment_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.FUNDING_HISTORY_URL)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    def tearDown(self) -> None:
        self.test_task and self.test_task.cancel()
        super().tearDown()

    def _initialize_event_loggers(self):
        self.buy_order_completed_logger = EventLogger()
        self.sell_order_completed_logger = EventLogger()
        self.order_cancelled_logger = EventLogger()
        self.order_filled_logger = EventLogger()
        self.funding_payment_completed_logger = EventLogger()

        events_and_loggers = [
            (MarketEvent.BuyOrderCompleted, self.buy_order_completed_logger),
            (MarketEvent.SellOrderCompleted, self.sell_order_completed_logger),
            (MarketEvent.OrderCancelled, self.order_cancelled_logger),
            (MarketEvent.OrderFilled, self.order_filled_logger),
            (MarketEvent.FundingPaymentCompleted, self.funding_payment_completed_logger)]

        for event, logger in events_and_loggers:
            self.exchange.add_listener(event, logger)

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _get_position_risk_api_endpoint_single_position_list(self) -> List[Dict[str, Any]]:
        positions = [
            {
                "symbol": self.symbol,
                "side": "LONG",
                "size": "1",
                "entryPrice": "50000",
                "markPrice": "51000",
                "unrealizedPnl": "1000",
                "realizedPnl": "0",
                "margin": "500",
                "marginRatio": "0.05",
                "liquidationPrice": "45000",
                "leverage": "10",
                "notional": "51000",
                "updateTime": int(self.start_timestamp * 1000),
            }
        ]
        return positions

    def _get_account_update_ws_event_single_position_dict(self) -> Dict[str, Any]:
        account_update = {
            "type": "position",
            "data": {
                "symbol": self.symbol,
                "side": "LONG",
                "size": "1",
                "entryPrice": "50000",
                "markPrice": "51000",
                "unrealizedPnl": "1000",
                "realizedPnl": "0",
                "margin": "500",
                "marginRatio": "0.05",
                "liquidationPrice": "45000",
                "leverage": "10",
                "timestamp": int(self.start_timestamp * 1000),
            }
        }
        return account_update

    def _get_income_history_dict(self) -> List[Dict[str, Any]]:
        income_history = [
            {
                "symbol": self.symbol,
                "incomeType": "FUNDING_FEE",
                "income": "0.5",
                "asset": "USDC",
                "info": "funding payment",
                "time": int(self.start_timestamp * 1000),
                "tranId": "123456789",
                "tradeId": "",
            }
        ]
        return income_history

    @aioresponses()
    def test_check_network_success(self, mock_api):
        """Test successful network check."""
        url = self.network_status_url
        mock_response = {"status": "ok"}
        mock_api.get(url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(self.exchange.check_network())
        self.assertEqual(result, 0)

    @aioresponses()
    def test_check_network_failure(self, mock_api):
        """Test network check failure."""
        url = self.network_status_url
        mock_api.get(url, status=500)

        with self.assertRaises(Exception):
            self.async_run_with_timeout(self.exchange.check_network())

    @aioresponses()
    def test_update_trading_rules(self, mock_api):
        """Test updating trading rules from exchange info."""
        url = self.trading_rules_url

        mock_response = {
            "symbols": [
                {
                    "symbol": self.symbol,
                    "baseAsset": self.base_asset,
                    "quoteAsset": self.quote_asset,
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "filters": {
                        "minQuantity": "0.001",
                        "maxQuantity": "1000",
                        "stepSize": "0.001",
                        "minNotional": "10",
                        "tickSize": "0.01",
                    }
                }
            ]
        }
        mock_api.get(url, body=json.dumps(mock_response))

        self.async_run_with_timeout(self.exchange._update_trading_rules())

        trading_rule = self.exchange._trading_rules[self.trading_pair]
        self.assertIsInstance(trading_rule, TradingRule)
        self.assertEqual(trading_rule.trading_pair, self.trading_pair)
        self.assertEqual(trading_rule.min_order_size, Decimal("0.001"))
        self.assertEqual(trading_rule.max_order_size, Decimal("1000"))
        self.assertEqual(trading_rule.min_order_value, Decimal("10"))
        self.assertEqual(trading_rule.min_base_amount_increment, Decimal("0.001"))
        self.assertEqual(trading_rule.min_price_increment, Decimal("0.01"))

    @aioresponses()
    def test_update_balances(self, mock_api):
        """Test updating account balances."""
        url = self.balance_url

        mock_response = {
            "balances": [
                {
                    "asset": "USDC",
                    "free": "10000",
                    "locked": "500",
                }
            ],
            "positions": self._get_position_risk_api_endpoint_single_position_list(),
        }
        mock_api.get(url, body=json.dumps(mock_response))

        self.async_run_with_timeout(self.exchange._update_balances())

        available_balance = self.exchange.get_balance("USDC")
        self.assertEqual(available_balance, Decimal("10000"))

    @aioresponses()
    def test_get_open_orders(self, mock_api):
        """Test fetching open orders."""
        url = web_utils.private_rest_url(path_url=CONSTANTS.OPEN_ORDERS_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")

        mock_response = [
            {
                "orderId": "123456",
                "clientOrderId": "HBOT-1",
                "symbol": self.symbol,
                "side": "Buy",
                "orderType": "Limit",
                "price": "50000",
                "quantity": "0.1",
                "executedQuantity": "0",
                "status": "NEW",
                "timestamp": int(self.start_timestamp * 1000),
            }
        ]
        mock_api.get(regex_url, body=json.dumps(mock_response))

        orders = self.async_run_with_timeout(self.exchange.get_open_orders())

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].trading_pair, self.trading_pair)
        self.assertEqual(orders[0].price, Decimal("50000"))
        self.assertEqual(orders[0].amount, Decimal("0.1"))

    @aioresponses()
    def test_place_order_buy_market_order(self, mock_api):
        """Test placing a market buy order."""
        order_id = "HBOT-1"
        amount = Decimal("0.1")

        # Mock order placement
        url = web_utils.private_rest_url(path_url=CONSTANTS.PLACE_ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "orderId": "123456",
            "clientOrderId": order_id,
            "symbol": self.symbol,
            "side": "Buy",
            "orderType": "Market",
            "quantity": str(amount),
            "status": "NEW",
            "timestamp": int(self.start_timestamp * 1000),
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        # Mock balance check
        balance_url = self.balance_url
        balance_response = {
            "balances": [{"asset": "USDC", "free": "10000", "locked": "0"}],
            "positions": [],
        }
        mock_api.get(balance_url, body=json.dumps(balance_response))

        order_id_result = self.async_run_with_timeout(
            self.exchange._place_order(
                order_id=order_id,
                trading_pair=self.trading_pair,
                amount=amount,
                trade_type=TradeType.BUY,
                order_type=OrderType.MARKET,
                price=None,
                position_action=PositionAction.OPEN,
            )
        )

        self.assertEqual(order_id, order_id_result)

        in_flight_order = self.exchange._order_tracker.fetch_order(order_id)
        self.assertIsNotNone(in_flight_order)

    @aioresponses()
    def test_place_order_sell_limit_order(self, mock_api):
        """Test placing a limit sell order."""
        order_id = "HBOT-2"
        amount = Decimal("0.1")
        price = Decimal("51000")

        # Mock order placement
        url = web_utils.private_rest_url(path_url=CONSTANTS.PLACE_ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "orderId": "123457",
            "clientOrderId": order_id,
            "symbol": self.symbol,
            "side": "Sell",
            "orderType": "Limit",
            "price": str(price),
            "quantity": str(amount),
            "status": "NEW",
            "timestamp": int(self.start_timestamp * 1000),
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        # Mock balance check
        balance_url = self.balance_url
        balance_response = {
            "balances": [{"asset": "USDC", "free": "10000", "locked": "0"}],
            "positions": self._get_position_risk_api_endpoint_single_position_list(),
        }
        mock_api.get(balance_url, body=json.dumps(balance_response))

        order_id_result = self.async_run_with_timeout(
            self.exchange._place_order(
                order_id=order_id,
                trading_pair=self.trading_pair,
                amount=amount,
                trade_type=TradeType.SELL,
                order_type=OrderType.LIMIT,
                price=price,
                position_action=PositionAction.CLOSE,
            )
        )

        self.assertEqual(order_id, order_id_result)

        in_flight_order = self.exchange._order_tracker.fetch_order(order_id)
        self.assertIsNotNone(in_flight_order)
        self.assertEqual(in_flight_order.price, price)

    @aioresponses()
    def test_cancel_order(self, mock_api):
        """Test cancelling an order."""
        order_id = "HBOT-3"
        exchange_order_id = "123458"

        # Add order to tracker
        self.exchange._order_tracker.start_tracking_order(
            InFlightOrder(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=self.trading_pair,
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("50000"),
                amount=Decimal("0.1"),
                creation_timestamp=self.start_timestamp,
            )
        )

        # Mock cancel request
        url = web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "orderId": exchange_order_id,
            "clientOrderId": order_id,
            "status": "CANCELLED",
        }
        mock_api.delete(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange._execute_cancel(trading_pair=self.trading_pair, order_id=order_id)
        )

        self.assertTrue(result)

        # Order should be marked as cancelled
        cancelled_order = self.exchange._order_tracker.fetch_order(order_id)
        self.assertTrue(cancelled_order.is_cancelled)

    @aioresponses()
    def test_get_funding_info(self, mock_api):
        """Test fetching funding info."""
        url = self.funding_info_url

        mock_response = {
            "symbol": self.symbol,
            "markPrice": "50000",
            "indexPrice": "50001",
            "fundingRate": "0.0001",
            "nextFundingTime": int((self.start_timestamp + 28800) * 1000),
        }
        mock_api.get(url, body=json.dumps(mock_response))

        funding_info = self.async_run_with_timeout(
            self.exchange.get_funding_info(self.trading_pair)
        )

        self.assertEqual(funding_info.trading_pair, self.trading_pair)
        self.assertEqual(funding_info.mark_price, Decimal("50000"))
        self.assertEqual(funding_info.index_price, Decimal("50001"))
        self.assertEqual(funding_info.rate, Decimal("0.0001"))

    @aioresponses()
    def test_get_funding_payment_history(self, mock_api):
        """Test fetching funding payment history."""
        url = self.funding_payment_url

        mock_response = self._get_income_history_dict()
        mock_api.get(url, body=json.dumps(mock_response))

        funding_payments = self.async_run_with_timeout(
            self.exchange.get_funding_payment_history(
                trading_pair=self.trading_pair,
                start_time=self.start_timestamp,
            )
        )

        self.assertEqual(len(funding_payments), 1)
        self.assertEqual(funding_payments[0].trading_pair, self.trading_pair)
        self.assertEqual(funding_payments[0].amount, Decimal("0.5"))

    def test_supported_position_modes(self):
        """Test that only ONE_WAY position mode is supported."""
        supported_modes = self.exchange.supported_position_modes
        self.assertEqual(len(supported_modes), 1)
        self.assertIn(PositionMode.ONEWAY, supported_modes)

    @aioresponses()
    def test_set_leverage(self, mock_api):
        """Test setting leverage."""
        url = web_utils.private_rest_url(path_url=CONSTANTS.SET_LEVERAGE_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        leverage = 5
        mock_response = {
            "symbol": self.symbol,
            "leverage": leverage,
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.set_leverage(self.trading_pair, leverage)
        )

        self.assertEqual(result, leverage)
