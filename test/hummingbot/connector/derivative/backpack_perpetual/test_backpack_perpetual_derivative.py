import asyncio
import base64
import json
import re
from decimal import Decimal
from typing import Any, Callable, List, Optional, Tuple
from unittest.mock import patch

from aioresponses import aioresponses
from aioresponses.core import RequestCall

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import BackpackPerpetualDerivative
from hummingbot.connector.test_support.perpetual_derivative_test import AbstractPerpetualDerivativeTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase


class BackpackPerpetualDerivativeTests(AbstractPerpetualDerivativeTests.PerpetualDerivativeTests):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.api_key = "someKey"
        cls.api_secret = base64.b64encode(bytes(range(32))).decode("utf-8")
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"

    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_URL)

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_URL)
        url = f"{url}?symbol={self.exchange_trading_pair}"
        return url

    @property
    def network_status_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.PING_URL)

    @property
    def trading_rules_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_URL)

    @property
    def order_creation_url(self):
        return web_utils.private_rest_url(path_url=CONSTANTS.ORDER_URL)

    @property
    def balance_url(self):
        return web_utils.private_rest_url(path_url=CONSTANTS.COLLATERAL_URL)

    @property
    def funding_info_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARK_PRICE_URL)
        url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?") + ".*")
        return url

    @property
    def funding_payment_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.FUNDING_HISTORY_URL)
        url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?") + ".*")
        return url

    @property
    def all_symbols_request_mock_response(self):
        return {
            "data": [
                {
                    "symbol": self.exchange_trading_pair,
                    "baseSymbol": self.base_asset,
                    "quoteSymbol": self.quote_asset,
                    "marketType": "PERP",
                    "status": "TRADING",
                    "filters": {
                        "price": {"minPrice": "0.01", "tickSize": "0.01"},
                        "quantity": {"minQuantity": "0.001", "stepSize": "0.001", "maxQuantity": "100"},
                        "notional": {"minNotional": "5"},
                    },
                },
            ]
        }

    @property
    def latest_prices_request_mock_response(self):
        return {
            "symbol": self.exchange_trading_pair,
            "lastPrice": str(self.expected_latest_price),
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = {
            "data": [
                {
                    "symbol": self.exchange_trading_pair,
                    "baseSymbol": self.base_asset,
                    "quoteSymbol": self.quote_asset,
                    "marketType": "PERP",
                    "status": "TRADING",
                    "filters": {
                        "price": {"minPrice": "0.01", "tickSize": "0.01"},
                        "quantity": {"minQuantity": "0.001", "stepSize": "0.001"},
                    },
                },
                {
                    "symbol": "INVALID_PAIR",
                    "baseSymbol": "INVALID",
                    "quoteSymbol": "PAIR",
                    "marketType": "SPOT",
                    "status": "TRADING",
                    "filters": {"price": {"tickSize": "0.01"}, "quantity": {"stepSize": "0.01"}},
                },
            ]
        }

        return "INVALID-PAIR", response

    @property
    def network_status_request_successful_mock_response(self):
        return {}

    @property
    def trading_rules_request_mock_response(self):
        return self.all_symbols_request_mock_response

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {
            "data": [
                {
                    "symbol": self.exchange_trading_pair,
                    "baseSymbol": self.base_asset,
                    "quoteSymbol": self.quote_asset,
                    "marketType": "PERP",
                    "status": "TRADING",
                    "filters": {"price": {"tickSize": "0.01"}, "quantity": {}},
                },
            ]
        }

    @property
    def order_creation_request_successful_mock_response(self):
        return {
            "symbol": self.exchange_trading_pair,
            "id": str(self.expected_exchange_order_id),
            "createdAt": 1640780000000,
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "collateral": [
                {
                    "symbol": self.base_asset,
                    "totalQuantity": "15",
                    "availableQuantity": "10",
                    "lendQuantity": "0",
                },
                {
                    "symbol": self.quote_asset,
                    "totalQuantity": "2000",
                    "availableQuantity": "2000",
                    "lendQuantity": "0",
                },
            ]
        }

    @property
    def balance_request_mock_response_only_base(self):
        return {
            "collateral": [
                {
                    "symbol": self.base_asset,
                    "totalQuantity": "15",
                    "availableQuantity": "10",
                    "lendQuantity": "0",
                }
            ]
        }

    @property
    def balance_event_websocket_update(self):
        return {
            "type": "balance",
            "data": {
                "balances": {
                    self.base_asset: {
                        "available": "10",
                        "total": "15",
                    }
                }
            },
        }

    @property
    def expected_latest_price(self):
        return 9999.9

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.001"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.001"),
            max_order_size=Decimal("100"),
            min_notional_size=Decimal("5"),
            buy_order_collateral_token=self.quote_asset,
            sell_order_collateral_token=self.quote_asset,
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        symbol_info = self.trading_rules_request_erroneous_mock_response["data"][0]
        return f"Error parsing trading rule for {symbol_info}. Error: 'minQuantity'"

    @property
    def expected_exchange_order_id(self):
        return 11111

    def is_cancel_request_executed_synchronously_by_server(self):
        return True

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return False

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("100")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("30"))],
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return "12345"

    @property
    def expected_supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    @property
    def funding_info_mock_response(self):
        return [
            {
                "symbol": self.exchange_trading_pair,
                "markPrice": str(self.target_funding_info_mark_price),
                "indexPrice": str(self.target_funding_info_index_price),
                "fundingRate": str(self.target_funding_info_rate),
                "nextFundingTimestamp": str(self.target_funding_info_next_funding_utc_timestamp * 1000),
            }
        ]

    @property
    def empty_funding_payment_mock_response(self):
        return []

    @property
    def funding_payment_mock_response(self):
        return [
            {
                "symbol": self.exchange_trading_pair,
                "quantity": str(self.target_funding_payment_payment_amount),
                "fundingRate": str(self.target_funding_payment_funding_rate),
                "timestamp": self.target_funding_payment_timestamp,
            }
        ]

    @property
    def target_funding_payment_timestamp(self):
        return 1657110053000

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}_{quote_token}_PERP"

    def create_exchange_instance(self):
        return BackpackPerpetualDerivative(
            backpack_perpetual_api_key=self.api_key,
            backpack_perpetual_api_secret=self.api_secret,
            trading_pairs=[self.trading_pair],
            trading_required=True,
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        request_headers = request_call.kwargs["headers"]
        self.assertIn("X-API-Key", request_headers)
        self.assertIn("X-Timestamp", request_headers)
        self.assertIn("X-Signature", request_headers)
        self.assertIn("X-Window", request_headers)
        self.assertEqual(self.api_key, request_headers["X-API-Key"])

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data_raw = request_call.kwargs["data"]
        request_data = json.loads(request_data_raw) if isinstance(request_data_raw, str) else dict(request_data_raw)
        self.assertEqual(self.exchange_trading_pair, request_data["symbol"])
        expected_side = "Bid" if order.trade_type == TradeType.BUY else "Ask"
        self.assertEqual(expected_side, request_data["side"])
        self.assertEqual("Limit", request_data["orderType"])
        self.assertEqual(Decimal("100"), Decimal(request_data["quantity"]))
        self.assertEqual(Decimal("10000"), Decimal(request_data["price"]))
        self.assertIsInstance(request_data["clientId"], int)
        self.assertEqual(order.client_order_id, self.exchange._id_mapper.get_hb_id(int(request_data["clientId"])))
        if order.position == PositionAction.CLOSE and "reduceOnly" in request_data:
            self.assertTrue(request_data["reduceOnly"])

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data_raw = request_call.kwargs["data"]
        request_data = json.loads(request_data_raw) if isinstance(request_data_raw, str) else dict(request_data_raw)
        self.assertEqual(self.exchange_trading_pair, request_data["symbol"])
        self.assertEqual(order.exchange_order_id, request_data["orderId"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_trading_pair, request_params["symbol"])
        self.assertEqual(order.exchange_order_id, request_params["orderId"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_trading_pair, request_params["symbol"])
        self.assertEqual(order.exchange_order_id, request_params["orderId"])

    def configure_successful_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        response = {"status": "success"}
        mock_api.delete(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        mock_api.delete(regex_url, status=400, callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
        self,
        successful_order: InFlightOrder,
        erroneous_order: InFlightOrder,
        mock_api: aioresponses,
    ) -> List[str]:
        urls = []
        urls.append(self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api))
        urls.append(self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api))
        return urls

    def configure_completely_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        response = {
            "id": order.exchange_order_id,
            "clientId": self.exchange._id_mapper.get_numeric_id(order.client_order_id),
            "symbol": self.exchange_trading_pair,
            "status": "Filled",
        }
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_canceled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        response = {
            "id": order.exchange_order_id,
            "clientId": self.exchange._id_mapper.get_numeric_id(order.client_order_id),
            "symbol": self.exchange_trading_pair,
            "status": "Cancelled",
        }
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.FILLS_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        mock_api.get(regex_url, status=400, callback=callback)
        return url

    def configure_open_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        response = {
            "id": order.exchange_order_id,
            "clientId": self.exchange._id_mapper.get_numeric_id(order.client_order_id),
            "symbol": self.exchange_trading_pair,
            "status": "New",
        }
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_http_error_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        mock_api.get(regex_url, status=400, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        response = {
            "id": order.exchange_order_id,
            "clientId": self.exchange._id_mapper.get_numeric_id(order.client_order_id),
            "symbol": self.exchange_trading_pair,
            "status": "PartiallyFilled",
        }
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_partial_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.FILLS_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        response = [
            {
                "tradeId": self.expected_fill_trade_id,
                "orderId": order.exchange_order_id,
                "price": str(self.expected_partial_fill_price),
                "quantity": str(self.expected_partial_fill_amount),
                "timestamp": 1000000,
                "side": "Bid",
                "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                "feeSymbol": self.quote_asset,
            }
        ]
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_full_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.FILLS_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        response = [
            {
                "tradeId": self.expected_fill_trade_id,
                "orderId": order.exchange_order_id,
                "price": str(order.price),
                "quantity": str(order.amount),
                "timestamp": 1000000,
                "side": "Bid",
                "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                "feeSymbol": self.quote_asset,
            }
        ]
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "type": "order",
            "data": {
                "e": "orderAccepted",
                "i": order.exchange_order_id,
                "c": self.exchange._id_mapper.get_numeric_id(order.client_order_id),
                "s": self.exchange_trading_pair,
                "S": "Bid" if order.trade_type == TradeType.BUY else "Ask",
                "X": "New",
            },
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "type": "order",
            "data": {
                "e": "orderCancelled",
                "i": order.exchange_order_id,
                "c": self.exchange._id_mapper.get_numeric_id(order.client_order_id),
                "s": self.exchange_trading_pair,
                "X": "Cancelled",
            },
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "type": "order",
            "data": {
                "e": "orderFill",
                "i": order.exchange_order_id,
                "c": self.exchange._id_mapper.get_numeric_id(order.client_order_id),
                "s": self.exchange_trading_pair,
                "X": "Filled",
                "t": self.expected_fill_trade_id,
                "l": str(order.amount),
                "L": str(order.price),
                "n": str(self.expected_fill_fee.flat_fees[0].amount),
                "N": self.quote_asset,
            },
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return None

    def position_event_for_full_fill_websocket_update(self, order: InFlightOrder, unrealized_pnl: float):
        return {
            "type": "position",
            "data": {
                "e": "positionOpened",
                "s": self.exchange_trading_pair,
                "q": str(order.amount),
                "B": str(order.price),
                "P": str(unrealized_pnl),
            },
        }

    def funding_info_event_for_websocket_update(self):
        return {
            "stream": f"{CONSTANTS.WS_MARK_PRICE_CHANNEL}.{self.exchange_trading_pair}",
            "data": {
                "e": "markPrice",
                "s": self.exchange_trading_pair,
                "p": str(self.target_funding_info_mark_price_ws_updated),
                "i": str(self.target_funding_info_index_price_ws_updated),
                "f": str(self.target_funding_info_rate_ws_updated),
                "n": self.target_funding_info_next_funding_utc_timestamp_ws_updated * 1000,
            },
        }

    def configure_successful_set_position_mode(
        self,
        position_mode: PositionMode,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        callback()

    def configure_failed_set_position_mode(
        self,
        position_mode: PositionMode,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> Tuple[str, str]:
        callback()
        return "", "Backpack perpetuals only support ONE-WAY position mode"

    def configure_failed_set_leverage(
        self,
        leverage: int,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> Tuple[str, str]:
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        error_body = "{\"message\": \"Invalid leverage\"}"
        mock_api.patch(regex_url, status=400, body=error_body, callback=callback)
        return url, f"Failed to set leverage: HTTP 400: {error_body}"

    def configure_successful_set_leverage(
        self,
        leverage: int,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        mock_api.patch(regex_url, status=200, body="{}", callback=callback)

    def test_get_buy_and_sell_collateral_tokens(self):
        self._simulate_trading_rules_initialized()

        buy_collateral_token = self.exchange.get_buy_collateral_token(self.trading_pair)
        sell_collateral_token = self.exchange.get_sell_collateral_token(self.trading_pair)

        self.assertEqual(self.quote_asset, buy_collateral_token)
        self.assertEqual(self.quote_asset, sell_collateral_token)

    @aioresponses()
    def test_set_position_mode_failure(self, mock_api):
        self.exchange.set_position_mode(PositionMode.HEDGE)
        self.assertTrue(
            self.is_logged(
                log_level="ERROR",
                message="Position mode PositionMode.HEDGE is not supported. Mode not set.",
            )
        )

    @aioresponses()
    def test_set_position_mode_success(self, mock_api):
        self.exchange.set_position_mode(PositionMode.ONEWAY)
        self.async_run_with_timeout(asyncio.sleep(0.1))
        self.assertTrue(
            self.is_logged(
                log_level="DEBUG",
                message=f"Position mode switched to {PositionMode.ONEWAY}.",
            )
        )

    def test_time_synchronizer_related_request_error_detection(self):
        exception = Exception("Request timestamp is invalid")
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

    @aioresponses()
    async def test_update_order_fills_from_trades_triggers_filled_event(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._last_poll_timestamp = 0
        self.exchange._set_current_timestamp(11)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="8886774",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            position_action=PositionAction.OPEN,
        )

        fills = [
            {
                "tradeId": "999",
                "orderId": "8886774",
                "price": "100",
                "quantity": "1",
                "timestamp": 1000000,
                "side": "Bid",
                "fee": "30",
                "feeSymbol": self.quote_asset,
            }
        ]

        url = web_utils.private_rest_url(CONSTANTS.FILLS_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        mock_api.get(regex_url, body=json.dumps(fills))

        await self.exchange._update_order_fills_from_trades()

        tracked_order = self.exchange.in_flight_orders["OID1"]
        self.assertEqual(Decimal("1"), tracked_order.executed_amount_base)
        self.assertEqual(Decimal("100"), tracked_order.executed_amount_quote)
        self.assertIn("999", tracked_order.order_fills)

    @aioresponses()
    async def test_update_order_fills_request_parameters(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._last_poll_timestamp = 0
        self.exchange._set_current_timestamp(11)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="8886774",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            position_action=PositionAction.OPEN,
        )

        url = web_utils.private_rest_url(CONSTANTS.FILLS_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        mock_api.get(regex_url, body=json.dumps([]))

        await self.exchange._update_order_fills_from_trades()

        request = self._all_executed_requests(mock_api, url)[0]
        self.validate_auth_credentials_present(request)
        self.assertEqual(self.exchange_trading_pair, request.kwargs["params"]["symbol"])

    @aioresponses()
    async def test_update_order_fills_from_trades_with_repeated_fill_triggers_only_one_event(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._last_poll_timestamp = 0
        self.exchange._set_current_timestamp(11)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="8886774",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            position_action=PositionAction.OPEN,
        )

        fills = [
            {
                "tradeId": "999",
                "orderId": "8886774",
                "price": "100",
                "quantity": "0.5",
                "timestamp": 1000000,
                "side": "Bid",
                "fee": "30",
                "feeSymbol": self.quote_asset,
            },
            {
                "tradeId": "999",
                "orderId": "8886774",
                "price": "100",
                "quantity": "0.5",
                "timestamp": 1000000,
                "side": "Bid",
                "fee": "30",
                "feeSymbol": self.quote_asset,
            },
        ]

        url = web_utils.private_rest_url(CONSTANTS.FILLS_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))
        mock_api.get(regex_url, body=json.dumps(fills))

        await self.exchange._update_order_fills_from_trades()

        tracked_order = self.exchange.in_flight_orders["OID1"]
        self.assertEqual(1, len(tracked_order.order_fills))
        self.assertIn("999", tracked_order.order_fills)

    @aioresponses()
    @patch("hummingbot.connector.time_synchronizer.TimeSynchronizer._current_seconds_counter")
    def test_update_time_synchronizer_successfully(self, mock_api, seconds_counter_mock):
        request_sent_event = asyncio.Event()
        seconds_counter_mock.side_effect = [0, 0, 0]

        self.exchange._time_synchronizer.clear_time_offset_ms_samples()
        url = web_utils.public_rest_url(CONSTANTS.TIME_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))

        response = {"timestamp": 1640000003000}
        mock_api.get(regex_url, body=json.dumps(response), callback=lambda *args, **kwargs: request_sent_event.set())

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())

        self.assertEqual(response["timestamp"] * 1e-3, self.exchange._time_synchronizer.time())

    @aioresponses()
    def test_update_time_synchronizer_failure_is_logged(self, mock_api):
        request_sent_event = asyncio.Event()

        url = web_utils.public_rest_url(CONSTANTS.TIME_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))

        response = {"error": "Dummy error"}
        mock_api.get(regex_url, body=json.dumps(response), callback=lambda *args, **kwargs: request_sent_event.set())

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())

        self.assertTrue(self.is_logged("NETWORK", "Error getting server time."))

    @aioresponses()
    def test_update_time_synchronizer_raises_cancelled_error(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.TIME_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\\?"))

        mock_api.get(regex_url, exception=asyncio.CancelledError)

        self.assertRaises(
            asyncio.CancelledError,
            self.async_run_with_timeout,
            self.exchange._update_time_synchronizer(),
        )
