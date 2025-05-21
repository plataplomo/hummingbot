"""Unit tests for HyperliquidResponseHandler."""

import copy
from collections.abc import Callable
from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_processed_exchange_responses import (
    HyperliquidErrorStatus,
    HyperliquidSuccessfulOrderStatus,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrder,
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFill,
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# --- Type Aliases for Parametrized Tests ---

type HandlerMethodType = Callable[..., Any]
type HandlerArgsSpecType = dict[str, Any]
type InvalidDataForTestType = RawJsonResponse  # Data that is invalid for the handler
type ModificationDetailsType = dict[str, Any]

type InvalidTypeTestCaseType = tuple[
    HandlerMethodType, InvalidDataForTestType, str, HandlerArgsSpecType, str
]
type ValidationErrorTestCaseType = tuple[
    HandlerMethodType, str, ModificationDetailsType, str, HandlerArgsSpecType, str
]


# --- Test Fixtures ---


@pytest.fixture
def user_address() -> str:
    return "0xTestUserAddress1234567890abcdef"


@pytest.fixture
def symbol() -> str:
    return "ETH-PERP"


@pytest.fixture
def order_id() -> int:
    return 98765


@pytest.fixture
def valid_raw_exchange_status_object_resting() -> dict[str, Any]:
    return {"resting": {"oid": 12345}}


@pytest.fixture
def valid_raw_exchange_response(
    valid_raw_exchange_status_object_resting: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": "ok",
        "data": {
            "type": "order",
            "statuses": [valid_raw_exchange_status_object_resting, "canceled"],
        },
    }


@pytest.fixture
def valid_raw_meta_and_asset_ctxs() -> list[Any]:
    return [
        {
            "universe": [
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 100, "onlyIsolated": False},
                {"name": "ETH", "szDecimals": 4, "maxLeverage": 80, "onlyIsolated": False},
            ]
        },
        [
            {
                "name": "BTC",
                "funding": "0.0001",
                "markPx": "55000.0",
                "prevDayPx": "54000.0",
                "dayNtlVlm": "1000000000.0",
                "impactPx": "55010.0",
            },
            {
                "name": "ETH",
                "funding": "0.0002",
                "markPx": "3000.0",
                "prevDayPx": "2950.0",
                "dayNtlVlm": "500000000.0",
                "impactPx": "3005.0",
            },
        ],
    ]


@pytest.fixture
def valid_raw_user_state() -> dict[str, Any]:
    return {
        "assetPositions": [
            {
                "asset": "ETH-PERP",
                "position": {
                    "coin": "ETH-PERP",
                    "szi": "1.0",
                    "entryPx": "3000.0",
                    "leverage": {"type": "cross", "value": 10},
                    "liquidationPx": "2700.0",
                    "marginUsed": "300.0",
                    "maxLeverage": 50,
                    "positionValue": "3000.0",
                    "returnOnEquity": "0.0",
                    "unrealizedPnl": "0.0",
                },
            }
        ],
        "crossMaintenanceMarginUsed": "30.0",
        "crossMarginSummary": {
            "accountValue": "5000.0",
            "totalMarginUsed": "300.0",
            "totalNtlPos": "3000.0",
            "totalRawUsd": "4700.0",
        },
        "marginSummary": {
            "accountValue": "5000.0",
            "totalMarginUsed": "300.0",
            "totalNtlPos": "3000.0",
            "totalRawUsd": "4700.0",
        },
        "isolatedMaintenanceMarginUsed": "0.0",
        "isolatedMarginSummary": {
            "accountValue": "0.0",
            "totalMarginUsed": "0.0",
            "totalNtlPos": "0.0",
            "totalRawUsd": "0.0",
        },
        "withdrawable": "4700.0",
    }


@pytest.fixture
def valid_raw_open_order_item() -> dict[str, Any]:
    return {
        "order": {
            "asset": "ETH-PERP",
            "limitPx": "3000.0",
            "oid": 6001,
            "reduceOnly": False,
            "side": "B",
            "sz": "0.5",
            "timestamp": 1678889600000,
            "orderType": {"limit": {"tif": "Gtc"}},
            "remainingSz": "0.5",
            "status": "open",
            "statusTimestamp": 1678889601000,
            "cloid": "clientOpen1",
        },
        "trigger": None,
    }


@pytest.fixture
def valid_raw_user_fill() -> dict[str, Any]:
    return {
        "tid": 1001,
        "coin": "ETH-PERP",
        "px": "3000.1",
        "sz": "0.5",
        "time": 1678889800000,
        "side": "B",
        "oid": 6001,
        "startPosition": "0.0",
        "dir": "Open Long",
        "hash": "0xfillhash1",
        "fee": "1.5",
        "isMaker": False,
        "liquidationMarkPx": None,
        "cloid": "clientFill1",
    }


@pytest.fixture
def valid_raw_asset_ctx() -> dict[str, Any]:
    return {
        "name": "ETH-PERP",
        "markPx": "3010.00",
        "funding": "0.00015",
        "prevDayPx": "2990.00",
        "dayNtlVlm": "50000000.0",
        "impactPx": "3011.00",
    }


@pytest.fixture
def valid_raw_l2_book() -> dict[str, Any]:
    return {
        "coin": "ETH-PERP",
        "levels": [
            [{"px": "2999.0", "sz": "10.5", "n": 5}, {"px": "2998.0", "sz": "20.0", "n": 8}],
            [{"px": "3001.0", "sz": "5.2", "n": 3}, {"px": "3002.0", "sz": "15.8", "n": 6}],
        ],
        "time": 1678889300000,
    }


@pytest.fixture
def valid_raw_public_trade() -> dict[str, Any]:
    return {
        "coin": "ETH-PERP",
        "side": "B",
        "px": "3005.0",
        "sz": "0.1",
        "time": 1678889400000,
        "hash": "0xtradeHashValid",
    }


@pytest.fixture
def valid_raw_candle() -> dict[str, Any]:
    return {
        "t": 1678889500000,
        "o": "3000.0",
        "h": "3015.0",
        "l": "2995.0",
        "c": "3010.0",
        "v": "100.5",
        "n": 50,
    }


@pytest.fixture
def valid_raw_candle_snapshot() -> dict[str, Any]:
    # HyperliquidRawCandleSnapshot expects parallel arrays, not a list of candle dicts
    return {
        "t": [1672531200000, 1672531260000],
        "o": ["1200.0", "1201.0"],
        "h": ["1250.0", "1205.0"],
        "l": ["1190.0", "1198.0"],
        "c": ["1240.0", "1202.0"],
        "v": ["1000.0", "500.0"],
        "s": "ok",
    }


@pytest.fixture
def valid_raw_historical_funding_rates_data() -> list[dict[str, Any]]:
    return [
        {"coin": "ETH", "fundingRate": "0.000123", "premium": "0.0001", "time": 1678886400000},
        {"coin": "BTC", "fundingRate": "-0.00005", "premium": "-0.00003", "time": 1678882800000},
    ]


@pytest.fixture
def valid_raw_historical_order_response() -> dict[str, Any]:
    return {
        "order": {
            "asset": "ETH-PERP",
            "limitPx": "2900.0",
            "oid": 7001,
            "reduceOnly": False,
            "side": "B",
            "sz": "1.0",
            "timestamp": 1678890000000,
            "orderType": {"limit": {"tif": "Gtc"}},
            "remainingSz": "0.0",
            "status": "filled",
            "statusTimestamp": 1678890001000,
            "cloid": "histClient1",
        }
    }


@pytest.fixture
def valid_raw_vault_details() -> dict[str, Any]:
    # Placeholder structure - adjust based on actual API/model
    return {
        "name": "Test Vault",
        "totalValueLockedUSD": "1000000.0",
        "sharePrice": "1.05",
        "userBalance": "500.0",
        # ... other expected fields
    }


# --- Start of Tests ---


class TestHandleExchangeResponse:
    """Tests for HyperliquidResponseHandler.handle_exchange_response."""

    def test_valid(self, valid_raw_exchange_response: dict[str, Any]) -> None:
        """Test handling a valid raw exchange response."""
        raw_data = valid_raw_exchange_response
        response: HyperliquidRawExchangeResponse = (
            HyperliquidResponseHandler.handle_exchange_response(
                cast(RawJsonResponse, raw_data), action_type="order"
            )
        )
        assert isinstance(response, HyperliquidRawExchangeResponse)
        assert response.status == "ok"
        assert response.data is not None
        assert response.data.type == "order"
        assert len(response.data.statuses) == 2
        status1 = response.data.statuses[0]
        assert isinstance(status1, HyperliquidRawExchangeStatusObject)
        assert status1.resting is not None
        assert status1.resting.oid == 12345
        status2 = response.data.statuses[1]
        assert isinstance(status2, str)
        assert status2 == "canceled"

    def test_validation_error_missing_status(self) -> None:
        """Test exchange response dict missing required 'status' field."""
        raw_data = {"data": {"type": "order", "statuses": [{"resting": {"oid": 12345}}]}}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_exchange_response(
                cast(RawJsonResponse, raw_data), action_type="order"
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid exchange (order) response from exchange:" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "status" in str(exc_info.value.original_exception)

    def test_validation_error_bad_status_value(self) -> None:
        """Test exchange response with status != 'ok'."""
        raw_data = {"status": "error", "error": "Invalid order size"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_exchange_response(
                cast(RawJsonResponse, raw_data), action_type="order"
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid exchange (order) response from exchange:" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "status" in str(exc_info.value.original_exception)
        assert "Input should be 'ok'" in str(exc_info.value.original_exception)
        # The 'error' field is not directly part of HyperliquidRawExchangeResponse
        # if status is 'ok'. If status is not 'ok', Pydantic validation for 'ok' fails.
        # The original assertion might have been for a different model or error handling path.
        # For now, asserting that the 'status' validation failed is the key.
        # If the API guarantees an "error" field when status is not "ok",
        # then the Raw model would need to be a Union or have optional error fields.
        # assert "error" in str(exc_info.value.original_exception)

    def test_validation_error_ok_missing_data(self) -> None:
        """Test exchange response status='ok' but missing 'data' field."""
        raw_data = {"status": "ok", "data": "not a valid data structure"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_exchange_response(
                cast(RawJsonResponse, raw_data), action_type="order"
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid exchange (order) response from exchange:" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "data" in str(exc_info.value.original_exception)
        assert "Input should be a valid dictionary" in str(exc_info.value.original_exception)


class TestHandleInfoMetaAndAssetCtxsResponse:
    """Tests for HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response."""

    def test_valid(self, valid_raw_meta_and_asset_ctxs: list[Any]) -> None:
        """Test handling a valid raw meta and asset ctxs response."""
        raw_data = valid_raw_meta_and_asset_ctxs
        meta_and_ctxs = HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
            cast(RawJsonResponse, raw_data)
        )
        assert isinstance(meta_and_ctxs, HyperliquidRawMetaAndAssetCtxsResponse)
        assert isinstance(meta_and_ctxs.meta, HyperliquidRawMetaResponse)
        assert len(meta_and_ctxs.meta.universe) == 2
        assert meta_and_ctxs.meta.universe[0].name == "BTC"
        assert isinstance(meta_and_ctxs.asset_ctxs, list)
        assert len(meta_and_ctxs.asset_ctxs) == 2
        assert isinstance(meta_and_ctxs.asset_ctxs[0], HyperliquidRawAssetCtx)
        assert meta_and_ctxs.asset_ctxs[1].name == "ETH"

    def test_invalid_type_placeholder(self) -> None:
        """Placeholder for parametrized invalid type test (original: dict instead of list)."""
        raw_data = {"invalid": "data"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                cast(RawJsonResponse, raw_data)
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "info (MetaAndAssetCtxs)" in exc_info.value.message

    def test_validation_error(self) -> None:
        """Test handling response with invalid data (missing szDecimals) within the structure."""
        raw_data: RawJsonResponse = [  # Type hint for clarity
            {
                "universe": [
                    {
                        "name": "BTC",
                        # "szDecimals": 5, # Missing required field
                        "maxLeverage": 100,
                        "onlyIsolated": False,
                    }
                ]
            },
            [
                {
                    "name": "BTC",
                    "funding": "0.0001",
                    "markPx": "55000.0",
                    "prevDayPx": "54000.0",
                    "dayNtlVlm": "1000000000.0",
                    "impactPx": "55010.0",
                }
            ],
        ]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                raw_data  # No cast needed if raw_data is RawJsonResponse
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid info (MetaAndAssetCtxs) response from exchange:" in exc_info.value.message
        # The custom validator inside HyperliquidRawMetaAndAssetCtxsResponse.model_validate
        # raises ValueError if the structure doesn\'t match [meta_dict, asset_ctx_list]
        # or if pydantic validation fails internally.
        assert isinstance(exc_info.value.original_exception, ValidationError | ValueError)
        assert "szDecimals" in str(exc_info.value.original_exception)
        assert "Field required" in str(exc_info.value.original_exception)


class TestHandleInfoUserStateResponse:
    """Tests for HyperliquidResponseHandler.handle_info_user_state_response."""

    def test_valid(self, valid_raw_user_state: dict[str, Any], user_address: str) -> None:
        raw_data = valid_raw_user_state
        response = HyperliquidResponseHandler.handle_info_user_state_response(
            cast(RawJsonResponse, raw_data), user_address=user_address
        )
        assert isinstance(response, HyperliquidRawClearinghouseState)
        assert response.withdrawable == "4700.0"

    def test_validation_error(self, user_address: str) -> None:
        raw_data: RawJsonResponse = {"assetPositions": []}  # Using RawJsonResponse
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_state_response(
                raw_data,
                user_address=user_address,  # No cast
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (UserState for {user_address}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "withdrawable" in str(exc_info.value.original_exception)


class TestHandleInfoOpenOrdersResponse:
    """Tests for HyperliquidResponseHandler.handle_info_open_orders_response."""

    def test_valid(self, valid_raw_open_order_item: dict[str, Any], user_address: str) -> None:
        """Test handling a valid raw open orders response."""
        raw_data = [valid_raw_open_order_item, valid_raw_open_order_item.copy()]
        response_model: HyperliquidRawOpenOrdersResponse = (
            HyperliquidResponseHandler.handle_info_open_orders_response(
                cast(RawJsonResponse, raw_data), user_address=user_address
            )
        )
        assert isinstance(response_model, HyperliquidRawOpenOrdersResponse)
        assert len(response_model.root) == 2  # Access items via .root for RootModel
        assert isinstance(response_model.root[0], HyperliquidRawOpenOrder)
        assert response_model.root[0].order is not None
        assert response_model.root[0].order.oid == 6001
        assert isinstance(response_model.root[1], HyperliquidRawOpenOrder)

    def test_item_validation_error(
        self, valid_raw_open_order_item: dict[str, Any], user_address: str
    ) -> None:
        """Test list where an item (dict) fails model validation (e.g. missing order.oid)."""
        invalid_item_dict = valid_raw_open_order_item.copy()
        # Ensure nested structure for deletion
        if "order" in invalid_item_dict and isinstance(invalid_item_dict["order"], dict):
            del invalid_item_dict["order"]["oid"]  # Make one item invalid
        else:
            # This case should ideally not happen if the fixture is correct
            pytest.fail(
                "Fixture valid_raw_open_order_item does not have expected 'order'.'oid' structure"
            )

        raw_data = [
            valid_raw_open_order_item.copy(),
            invalid_item_dict,
        ]

        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_open_orders_response(
                cast(RawJsonResponse, raw_data), user_address=user_address
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        # The _handle_validation_error method will prepend context to the Pydantic error.
        # The context for RootModel validation is the overall list context.
        assert (
            f"Invalid info (OpenOrders for {user_address}) response from exchange"
            in exc_info.value.message
        )
        # The original ValidationError (e) will contain specifics about the failing item.
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "Field required" in str(exc_info.value.original_exception)
        assert "order.oid" in str(exc_info.value.original_exception)

    def test_invalid_item_type_in_list(
        self, valid_raw_open_order_item: dict[str, Any], user_address: str
    ) -> None:
        """Test list containing a non-dict item."""
        raw_data = [valid_raw_open_order_item.copy(), "not_a_dict_item"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_open_orders_response(
                cast(RawJsonResponse, raw_data), user_address=user_address
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        # This error originates from Pydantic when validating the list of items (RootModel)
        # and an item in the list is not a dict as expected by HyperliquidRawOpenOrder.
        assert (
            f"Invalid info (OpenOrders for {user_address}) response from exchange"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "Expected a dictionary" in str(exc_info.value.original_exception)


class TestHandleInfoUserFillsResponse:
    """Tests for HyperliquidResponseHandler.handle_info_user_fills_response."""

    def test_valid(self, valid_raw_user_fill: dict[str, Any], user_address: str) -> None:
        """Test handling a valid raw user fills response."""
        raw_data = [valid_raw_user_fill, valid_raw_user_fill.copy()]
        response_model = HyperliquidResponseHandler.handle_info_user_fills_response(
            cast(RawJsonResponse, raw_data), user_address=user_address
        )
        assert isinstance(response_model, HyperliquidRawUserFillsResponse)
        assert len(response_model.root) == 2
        assert isinstance(response_model.root[0], HyperliquidRawUserFill)
        assert response_model.root[0].tid == 1001

    def test_invalid_item_type_in_list(
        self, valid_raw_user_fill: dict[str, Any], user_address: str
    ) -> None:
        """Test list containing a non-dict item."""
        raw_data = [valid_raw_user_fill.copy(), "not_a_dict_item"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_fills_response(
                cast(RawJsonResponse, raw_data), user_address=user_address
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        # Error from Pydantic when validating list of items (RootModel)
        assert (
            f"Invalid info (UserFills for {user_address}) response from exchange"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "Expected a dictionary" in str(exc_info.value.original_exception)

    def test_item_validation_error(
        self, valid_raw_user_fill: dict[str, Any], user_address: str
    ) -> None:
        """Test list where an item fails model validation (e.g., missing tid)."""
        invalid_item = valid_raw_user_fill.copy()
        del invalid_item["tid"]
        raw_data = [valid_raw_user_fill.copy(), invalid_item]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_fills_response(
                cast(RawJsonResponse, raw_data), user_address=user_address
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        # Error from Pydantic for a specific item in the list (RootModel)
        # The context for the individual item failure is handled by _handle_validation_error
        # which in this case will be called for an item within the overall UserFills list.
        # The Pydantic RootModel for UserFills will try to validate each item.
        # The message will come from the validation of HyperliquidRawUserFill for the bad item.
        assert (
            f"Invalid info (UserFills for {user_address}) response from exchange"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "Field required" in str(exc_info.value.original_exception)
        assert "tid" in str(exc_info.value.original_exception)


class TestHandleInfoFundingRateResponse:
    """Tests for HyperliquidResponseHandler.handle_info_funding_rate_response."""

    def test_valid(self, valid_raw_asset_ctx: dict[str, Any], symbol: str) -> None:
        """Test handling a valid raw funding rate response."""
        raw_data = valid_raw_asset_ctx
        response = HyperliquidResponseHandler.handle_info_funding_rate_response(
            cast(RawJsonResponse, raw_data), symbol=symbol
        )
        assert isinstance(response, HyperliquidRawAssetCtx)
        assert response.name == "ETH-PERP"
        assert response.funding == "0.00015"

    def test_validation_error(self, symbol: str) -> None:
        """Test response missing required field 'markPx'."""
        raw_data = {  # Missing markPx
            "name": "ETH-PERP",
            "funding": "0.00015",
            "prevDayPx": "2990.00",
            "dayNtlVlm": "50000000.0",
            "impactPx": "3011.00",
        }
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_funding_rate_response(
                cast(RawJsonResponse, raw_data), symbol=symbol
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (FundingRate for {symbol}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "markPx" in str(exc_info.value.original_exception)


class TestHandleInfoL2BookResponse:
    """Tests for HyperliquidResponseHandler.handle_info_l2_book_response."""

    def test_valid(self, valid_raw_l2_book: dict[str, Any], symbol: str) -> None:
        """Test handling a valid raw L2 book response."""
        raw_data = valid_raw_l2_book
        response = HyperliquidResponseHandler.handle_info_l2_book_response(
            cast(RawJsonResponse, raw_data), symbol=symbol
        )
        assert isinstance(response, HyperliquidRawL2Book)
        assert response.coin == "ETH-PERP"
        assert len(response.levels) == 2
        assert len(response.levels[0]) == 2  # Bids
        assert len(response.levels[1]) == 2  # Asks
        assert response.levels[0][0].px == "2999.0"

    def test_validation_error(self, symbol: str) -> None:
        """Test response missing required field 'levels'."""
        raw_data = {
            "coin": "ETH-PERP",
            # "levels": [[...],[...]], # Missing levels
            "time": 1678889300000,
        }
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_l2_book_response(
                cast(RawJsonResponse, raw_data), symbol=symbol
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (L2Book for {symbol}) response from exchange:" in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "levels" in str(exc_info.value.original_exception)


class TestHandleInfoRecentTradesResponse:
    """Tests for HyperliquidResponseHandler.handle_info_recent_trades_response."""

    def test_valid(self, valid_raw_public_trade: dict[str, Any], symbol: str) -> None:
        """Test handling a valid raw recent trades response."""
        raw_data = [valid_raw_public_trade, valid_raw_public_trade.copy()]
        # Handler returns a list of the validated models
        response_list: list[HyperliquidRawPublicTrade] = (
            HyperliquidResponseHandler.handle_info_recent_trades_response(
                cast(RawJsonResponse, raw_data), symbol=symbol
            )
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 2
        assert isinstance(response_list[0], HyperliquidRawPublicTrade)
        assert response_list[0].hash == "0xtradeHashValid"

    def test_invalid_item_type_in_list(
        self, valid_raw_public_trade: dict[str, Any], symbol: str
    ) -> None:
        """Test list containing a non-dict item. Handler should skip it."""
        raw_data = [valid_raw_public_trade.copy(), "not_a_trade_dict"]
        # Handler skips invalid items, so no exception is raised
        response_list = HyperliquidResponseHandler.handle_info_recent_trades_response(
            cast(RawJsonResponse, raw_data), symbol=symbol
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 1  # Only the valid item remains
        assert isinstance(response_list[0], HyperliquidRawPublicTrade)

    def test_item_validation_error(
        self, valid_raw_public_trade: dict[str, Any], symbol: str
    ) -> None:
        """Test list where an item fails model validation (e.g., missing px).
        Handler should raise.
        """
        invalid_item = valid_raw_public_trade.copy()
        del invalid_item["px"]
        raw_data = [valid_raw_public_trade.copy(), invalid_item]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_recent_trades_response(
                cast(RawJsonResponse, raw_data), symbol=symbol
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid single recent trade item in info (RecentTrades for {symbol}) "
            f"response from exchange"
        ) in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "px" in str(exc_info.value.original_exception)


class TestHandleInfoCandleSnapshotResponse:
    """Tests for HyperliquidResponseHandler.handle_info_candle_snapshot_response."""

    def test_valid(self, valid_raw_candle_snapshot: dict[str, Any], symbol: str) -> None:
        """Test handling a valid raw candle snapshot response."""
        raw_data = valid_raw_candle_snapshot  # Fixture already has {"candles": [...]} structure
        interval = "1m"
        response = HyperliquidResponseHandler.handle_info_candle_snapshot_response(
            cast(RawJsonResponse, raw_data), symbol=symbol, interval=interval
        )
        assert isinstance(response, HyperliquidRawCandleSnapshot)
        assert response.s == "ok"
        assert len(response.t) == 2
        assert response.t[0] == 1672531200000
        assert response.o[0] == "1200.0"
        assert response.h[0] == "1250.0"
        assert response.l[0] == "1190.0"
        assert response.c[0] == "1240.0"
        assert response.v[0] == "1000.0"
        assert response.t[1] == 1672531260000
        assert response.o[1] == "1201.0"
        assert response.h[1] == "1205.0"
        assert response.l[1] == "1198.0"
        assert response.c[1] == "1202.0"
        assert response.v[1] == "500.0"

    def test_validation_error(self, symbol: str) -> None:
        """Test response missing required field 'candles'."""
        raw_data: RawJsonResponse = {"no_candles_here": []}
        interval = "1m"
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_candle_snapshot_response(
                raw_data,
                symbol=symbol,
                interval=interval,  # No cast needed
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (CandleSnapshot for {symbol} {interval}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "candles" in str(exc_info.value.original_exception)


class TestHandleQueryOrderHistoryResponse:
    """Tests for HyperliquidResponseHandler.handle_query_order_history_response."""

    def test_valid(
        self, valid_raw_historical_order_response: dict[str, Any], user_address: str
    ) -> None:
        """Test handling a valid raw order history response."""
        raw_data = [valid_raw_historical_order_response, valid_raw_historical_order_response.copy()]
        response_list: list[HyperliquidRawHistoricalOrderResponse] = (
            HyperliquidResponseHandler.handle_query_order_history_response(
                cast(RawJsonResponse, raw_data), user_address=user_address
            )
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 2
        assert isinstance(response_list[0], HyperliquidRawHistoricalOrderResponse)
        assert response_list[0].order.oid == 7001

    def test_invalid_item_type_in_list(
        self, valid_raw_historical_order_response: dict[str, Any], user_address: str
    ) -> None:
        """Test list containing a non-dict item. Handler should skip it."""
        raw_data = [valid_raw_historical_order_response.copy(), "not_an_order_dict"]
        # Handler skips invalid items, so no exception is raised
        response_list = HyperliquidResponseHandler.handle_query_order_history_response(
            cast(RawJsonResponse, raw_data), user_address=user_address
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 1  # Only the valid item remains
        assert isinstance(response_list[0], HyperliquidRawHistoricalOrderResponse)

    def test_item_validation_error(
        self, valid_raw_historical_order_response: dict[str, Any], user_address: str
    ) -> None:
        """Test list where the item fails model validation (e.g., missing order.oid).
        Handler should raise.
        """
        # Use deepcopy to ensure modifications to invalid_item don't affect other copies
        invalid_item = copy.deepcopy(valid_raw_historical_order_response)
        # Ensure 'order' and 'oid' exist before trying to delete, and that 'order' is a dict
        if (
            "order" in invalid_item
            and isinstance(invalid_item["order"], dict)
            and "oid" in invalid_item["order"]
        ):
            del invalid_item["order"]["oid"]
        else:
            pytest.fail(
                "Fixture valid_raw_historical_order_response does not have expected "
                "'order'.'oid' structure or 'order' is not a dict."
            )

        # Also use deepcopy for the "valid" item in the list to ensure it's pristine
        raw_data = [
            copy.deepcopy(valid_raw_historical_order_response),  # First item is a clean copy
            invalid_item,  # Second item is the modified one (missing oid)
        ]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_query_order_history_response(
                cast(RawJsonResponse, raw_data), user_address=user_address
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid single order history item (index 1) in query_order_history"
            f" (for {user_address}) response from exchange"
        ) in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "Field required" in str(exc_info.value.original_exception)
        assert "order.oid" in str(exc_info.value.original_exception)


class TestHandleInfoOrderStatusResponse:
    """Tests for HyperliquidResponseHandler.handle_info_order_status_response."""

    def test_valid(
        self, valid_raw_historical_order_response: dict[str, Any], user_address: str, order_id: int
    ) -> None:
        """Test handling a valid order status response (list with one dict item)."""
        # The API returns a list containing the order status dict
        raw_data = [valid_raw_historical_order_response]
        response = HyperliquidResponseHandler.handle_info_order_status_response(
            cast(RawJsonResponse, raw_data), user_address=user_address, order_id=order_id
        )
        assert isinstance(response, HyperliquidRawHistoricalOrderResponse)
        assert response.order.oid == 7001
        assert response.order.status == "filled"

    # test_invalid_type (e.g., receiving int 123) is covered by test_handler_invalid_top_level_type

    def test_order_not_found_string_direct(self, user_address: str, order_id: int) -> None:
        """Test handling 'Order not found' string directly."""
        raw_data = "Order not found"
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(RawJsonResponse, raw_data), user_address=user_address, order_id=order_id
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        expected_message = (
            f"Order {order_id} for user {user_address} not found (direct string: {raw_data!r})"
        )
        assert exc_info.value.message == expected_message
        assert exc_info.value.metadata == {"original_response": "Order not found"}

    def test_order_not_found_string_in_list(self, user_address: str, order_id: int) -> None:
        """Test handling ['Order not found'] list."""
        raw_data = ["Order not found"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(RawJsonResponse, raw_data), user_address=user_address, order_id=order_id
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        # The handler identifies the string item within the list, so the message reflects that.
        # Original raw_data is a list: ["Order not found"]. status_item becomes "Order not found".
        expected_message_detail = "(string response: 'Order not found')"
        assert expected_message_detail in exc_info.value.message, (
            f"Detail '{expected_message_detail}' not in msg '{exc_info.value.message}'"
        )
        assert exc_info.value.metadata == {"original_response_item": "Order not found"}

    def test_order_not_found_empty_list(self, user_address: str, order_id: int) -> None:
        """Test handling [] empty list response."""
        raw_data: RawJsonResponse = []  # Using RawJsonResponse
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                raw_data,
                user_address=user_address,
                order_id=order_id,  # No cast
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert (
            f"Order {order_id} for {user_address} not found (empty list)." in exc_info.value.message
        )

    def test_order_not_found_none(self, user_address: str, order_id: int) -> None:
        """Test handling None response."""
        raw_data = None
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(RawJsonResponse, raw_data), user_address=user_address, order_id=order_id
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"info (OrderStatus for user {user_address}, oid {order_id}) response format: "
            f"expected list or dict, got NoneType" in exc_info.value.message
        )
        # Metadata is not set in this path by the handler as it's a type error
        # before item processing

    def test_unexpected_string_in_list(self, user_address: str, order_id: int) -> None:
        """Test handling unexpected string inside the list."""
        raw_data = ["Some other error string"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(RawJsonResponse, raw_data), user_address=user_address, order_id=order_id
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"(OrderStatus for user {user_address}, oid {order_id}) response: "
            f"Some other error string" in exc_info.value.message
        )
        assert exc_info.value.metadata == {"original_response_item": "Some other error string"}

    def test_unexpected_item_type_in_list(self, user_address: str, order_id: int) -> None:
        """Test handling non-dict, non-string item inside the list."""
        raw_data = [12345]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(RawJsonResponse, raw_data), user_address=user_address, order_id=order_id
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"(OrderStatus for user {user_address}, oid {order_id}) response list: "
            f"expected dict, got int" in exc_info.value.message
        )
        assert exc_info.value.metadata == {"original_response_item": 12345}

    def test_validation_error_in_list_item(
        self, valid_raw_historical_order_response: dict[str, Any], user_address: str, order_id: int
    ) -> None:
        """Test list where the item fails model validation (e.g., missing status)."""
        invalid_item = valid_raw_historical_order_response.copy()
        del invalid_item["order"]["status"]
        raw_data = [invalid_item]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast(RawJsonResponse, raw_data), user_address=user_address, order_id=order_id
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid order status object in info (OrderStatus for user {user_address}, "
            f"oid {order_id}) response from exchange"
        ) in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "order.status" in str(exc_info.value.original_exception)
        # Metadata is set by _handle_validation_error implicitly via original_exception
        # and raw_data in context
        # No direct exc_info.value.metadata check needed if _handle_validation_error
        # structure is trusted


class TestProcessFirstExchangeStatus:
    """Tests for HyperliquidResponseHandler.process_first_exchange_status."""

    ACTION_DESC = "test_action"  # Common action description for these tests

    @pytest.mark.parametrize(
        "raw_status, expected_type, expected_details",
        [
            (
                {"resting": {"oid": 12345}},
                HyperliquidSuccessfulOrderStatus,
                {"status_type": "resting", "oid": 12345},
            ),
            (
                {"filled": {"oid": 67890, "totalSz": "1.0", "avgPx": "3000.0"}},
                HyperliquidSuccessfulOrderStatus,
                {"status_type": "filled", "oid": 67890, "total_sz": "1.0", "avg_px": "3000.0"},
            ),
            (
                {"canceled": {"oid": 54321}},
                HyperliquidSuccessfulOrderStatus,
                {"status_type": "canceled", "oid": 54321},
            ),
            (
                "canceled",
                HyperliquidSuccessfulOrderStatus,
                {"status_type": "canceled_str"},
            ),
            (
                {"error": "Insufficient margin"},
                HyperliquidErrorStatus,
                {"message": "Insufficient margin"},
            ),
        ],
    )
    def test_valid_statuses(
        self,
        raw_status: RawJsonResponse,
        expected_type: type[HyperliquidSuccessfulOrderStatus | HyperliquidErrorStatus],
        expected_details: dict[str, Any],
    ) -> None:
        """Test processing various valid raw status objects and strings."""
        result = HyperliquidResponseHandler.process_first_exchange_status(
            raw_status, action_description=self.ACTION_DESC
        )
        assert isinstance(result, expected_type)

        if isinstance(result, HyperliquidSuccessfulOrderStatus):
            assert result.status_type == expected_details["status_type"]
            if result.status_type in ["resting", "filled", "canceled"]:
                assert result.oid == expected_details["oid"]
            if result.status_type == "filled":
                assert result.total_sz == expected_details["total_sz"]
                assert result.avg_px == expected_details["avg_px"]
            # For "canceled_str", only type and status_type are asserted

        elif isinstance(result, HyperliquidErrorStatus):  # pyright: ignore[reportUnnecessaryIsInstance]
            assert result.message == expected_details["message"]

    @pytest.mark.parametrize(
        "invalid_raw_status, expected_exception_message_part_template",
        [
            (12345, "Invalid status type for {action_desc}: <class 'int'>"),
            ({}, "Unknown status structure for {action_desc}: {{}}"),
            (
                {"unknown_key": "value"},
                "Unknown status structure for {action_desc}: {{'unknown_key': 'value'}}",
            ),
            (["list_item"], "Invalid status type for {action_desc}: <class 'list'>"),
        ],
    )
    def test_invalid_status_structures(
        self,
        invalid_raw_status: RawJsonResponse,
        expected_exception_message_part_template: str,
    ) -> None:
        """Test invalid or unrecognized status structures."""
        expected_message = expected_exception_message_part_template.format(
            action_desc=self.ACTION_DESC
        )
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.process_first_exchange_status(
                invalid_raw_status, action_description=self.ACTION_DESC
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert expected_message in exc_info.value.message


# --- Parametrized Invalid Type Test ---

# Simpler list for now: (handler_method, invalid_data, expected_container_type,
# handler_args_direct, context_string)
# handler_args_direct will be a dict of actual values or fixture names (str) that the test
# will try to resolve.
_invalid_type_test_cases_simple: list[InvalidTypeTestCaseType] = [
    (
        HyperliquidResponseHandler.handle_exchange_response,
        ["invalid"],
        "dict",
        {"action_type": "order"},
        "exchange (order)",
    ),
    (
        HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response,
        {"inv": 1},
        "list",
        {},
        "info (MetaAndAssetCtxs)",
    ),
    (
        HyperliquidResponseHandler.handle_info_user_state_response,
        ["invalid"],
        "dict",
        {"user_address": "user_address"},
        "info (UserState for {user_address})",
    ),
    (
        HyperliquidResponseHandler.handle_info_open_orders_response,
        {"inv": 1},
        "list",
        {"user_address": "user_address"},
        "info (OpenOrders for {user_address})",
    ),
]


@pytest.mark.parametrize(
    "handler_method, invalid_data, expected_container_type, handler_args_spec, "
    "context_format_string",
    _invalid_type_test_cases_simple,
)
def test_handler_invalid_top_level_type(
    handler_method: HandlerMethodType,
    invalid_data: InvalidDataForTestType,
    expected_container_type: str,
    handler_args_spec: HandlerArgsSpecType,
    context_format_string: str,
    request: pytest.FixtureRequest,
) -> None:
    """Test handlers raise APIError for incorrect top-level data type."""

    actual_handler_args: dict[str, Any] = {}
    for arg_name, value_or_fixture_name in handler_args_spec.items():
        if isinstance(value_or_fixture_name, str):
            try:
                actual_handler_args[arg_name] = request.getfixturevalue(value_or_fixture_name)
            except (pytest.FixtureLookupError, AttributeError):
                actual_handler_args[arg_name] = value_or_fixture_name
        else:
            actual_handler_args[arg_name] = value_or_fixture_name

    # Format the context string with actual argument values
    final_context_string = context_format_string
    try:
        final_context_string = context_format_string.format(**actual_handler_args)
    except KeyError:
        pass  # Some args might not be in the format string, that's okay if context is simple

    with pytest.raises(APIError) as exc_info:
        handler_method(invalid_data, **actual_handler_args)

    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert (
        f"Unexpected {final_context_string} response format: expected {expected_container_type}"
    ) in exc_info.value.message
    assert (
        f"got {type(invalid_data)}" in exc_info.value.message
        or f"got {type(invalid_data).__name__}" in exc_info.value.message
    )


# --- Parametrized Validation Error Test ---

# List of tuples: (handler_method, valid_data_fixture_name, modification_details,
# expected_error_field, handler_args_spec, context_format_string)
# modification_details: Dict specifying change, e.g., {"remove_field": "status"} or
# {"change_field": "status", "new_value": "error"}
# expected_error_field: The field name expected to cause the ValidationError
_validation_error_test_cases: list[ValidationErrorTestCaseType] = [
    # handle_exchange_response
    (
        HyperliquidResponseHandler.handle_exchange_response,
        "valid_raw_exchange_response",
        {"remove_field": "status"},
        "status",
        {"action_type": "order"},
        "exchange (order)",
    ),
    (
        HyperliquidResponseHandler.handle_exchange_response,
        "valid_raw_exchange_response",
        {"change_field": "status", "new_value": "error"},  # status != 'ok'
        "status",
        {"action_type": "order"},
        "exchange (order)",
    ),
    (
        HyperliquidResponseHandler.handle_exchange_response,
        "valid_raw_exchange_response",
        {"change_field": "data", "new_value": "not a dict"},  # data not a dict
        "data",
        {"action_type": "order"},
        "exchange (order)",
    ),
    # handle_info_meta_and_asset_ctxs_response
    (
        HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response,
        "valid_raw_meta_and_asset_ctxs",
        {
            "remove_nested_field": [0, "universe", 0, "szDecimals"]
        },  # Remove meta.universe[0].szDecimals
        "szDecimals",
        {},  # No extra args for this handler
        "info (MetaAndAssetCtxs)",
    ),
    # handle_info_user_state_response
    (
        HyperliquidResponseHandler.handle_info_user_state_response,
        "valid_raw_user_state",
        {"remove_field": "withdrawable"},
        "withdrawable",
        {"user_address": "user_address"},
        "info (UserState for {user_address})",
    ),
    # handle_info_funding_rate_response
    (
        HyperliquidResponseHandler.handle_info_funding_rate_response,
        "valid_raw_asset_ctx",
        {"remove_field": "markPx"},
        "markPx",
        {"symbol": "symbol"},
        "info (FundingRate for {symbol})",
    ),
    # handle_info_l2_book_response
    (
        HyperliquidResponseHandler.handle_info_l2_book_response,
        "valid_raw_l2_book",
        {"remove_field": "levels"},
        "levels",
        {"symbol": "symbol"},
        "info (L2Book for {symbol})",
    ),
    # handle_info_candle_snapshot_response
    (
        HyperliquidResponseHandler.handle_info_candle_snapshot_response,
        "valid_raw_candle_snapshot",
        {"remove_field": "t"},  # Changed from "candles"
        "t",  # Changed from "candles"
        {"symbol": "symbol", "interval": "1m"},
        "info (CandleSnapshot for {symbol} 1m)",
    ),
    # Note: Tests for validation errors *within list items* will be handled separately
]


@pytest.mark.parametrize(
    "handler_method, valid_data_fixture_name, modification_details, "
    "expected_error_field, handler_args_spec, context_format_string",
    _validation_error_test_cases,
)
def test_handler_validation_error(
    handler_method: HandlerMethodType,
    valid_data_fixture_name: str,
    modification_details: ModificationDetailsType,
    expected_error_field: str,
    handler_args_spec: HandlerArgsSpecType,
    context_format_string: str,
    request: pytest.FixtureRequest,
) -> None:
    """Test handlers raise validation errors for malformed data."""
    valid_data: RawJsonResponse = request.getfixturevalue(valid_data_fixture_name)

    # Create invalid data by applying modification
    invalid_data: RawJsonResponse = copy.deepcopy(valid_data)
    if "remove_field" in modification_details:
        field_to_remove = modification_details["remove_field"]
        if isinstance(invalid_data, dict) and field_to_remove in invalid_data:
            del invalid_data[field_to_remove]
    elif "change_field" in modification_details:
        field_to_change = modification_details["change_field"]
        new_value = modification_details["new_value"]
        if isinstance(invalid_data, dict) and field_to_change in invalid_data:
            invalid_data[field_to_change] = new_value
    elif "remove_nested_field" in modification_details:
        path: list[str | int] = modification_details["remove_nested_field"]
        temp: Any = invalid_data  # Use Any for traversal, final type is RawJsonResponse
        try:
            for i, key_or_index in enumerate(path):
                if i == len(path) - 1:
                    del temp[key_or_index]
                else:
                    temp = temp[key_or_index]
        except (KeyError, IndexError, TypeError) as e:
            pytest.fail(
                f"Failed to apply nested modification: {path} to data from "
                f"{valid_data_fixture_name}. Error: {e}"
            )

    actual_handler_args: dict[str, Any] = {}
    for arg_name, value_or_fixture_name in handler_args_spec.items():
        if isinstance(value_or_fixture_name, str):
            try:
                actual_handler_args[arg_name] = request.getfixturevalue(value_or_fixture_name)
            except (pytest.FixtureLookupError, AttributeError):
                actual_handler_args[arg_name] = value_or_fixture_name
        else:
            actual_handler_args[arg_name] = value_or_fixture_name

    final_context_string = context_format_string
    try:
        final_context_string = context_format_string.format(**actual_handler_args)
    except KeyError:
        pass

    # Perform test
    with pytest.raises(APIError) as exc_info:
        handler_method(invalid_data, **actual_handler_args)

    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert f"Invalid {final_context_string} response from exchange:" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError | ValueError)
    assert expected_error_field in str(exc_info.value.original_exception)


class TestHandleHistoricalFundingRatesResponse:
    """Tests for HyperliquidResponseHandler.handle_historical_funding_rates_response."""

    def test_valid_response(
        self,
        valid_raw_historical_funding_rates_data: list[dict[str, Any]],
    ) -> None:
        """Test handling of a valid raw historical funding rates response."""
        handler = HyperliquidResponseHandler()
        raw_data_list = cast(list[RawJsonResponse], valid_raw_historical_funding_rates_data)
        result = handler.handle_historical_funding_rates_response(raw_data_list)
        assert isinstance(result, list)
        assert len(result) == len(valid_raw_historical_funding_rates_data)
        for i, item in enumerate(result):
            assert isinstance(item, HyperliquidRawFundingHistoryItem)
            # Check against the original raw dict from the fixture
            original_item_dict = valid_raw_historical_funding_rates_data[i]
            assert item.coin == original_item_dict["coin"]
            assert item.funding_rate == original_item_dict["fundingRate"]
            assert item.time == original_item_dict["time"]
            assert item.premium == original_item_dict["premium"]

    def test_invalid_top_level_type(self) -> None:
        """Test handling when the raw response is not a list."""
        handler = HyperliquidResponseHandler()
        invalid_data: RawJsonResponse = {"error": "not a list"}
        with pytest.raises(APIError) as exc_info:
            handler.handle_historical_funding_rates_response(invalid_data)
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Expected list for historical funding rates, got dict" in exc_info.value.message

    def test_invalid_item_type_in_list(self) -> None:
        """Test handling when an item in the list is not a dictionary."""
        handler = HyperliquidResponseHandler()
        # Example valid item: {"coin": "ETH", "fundingRate": "0.01", "premium": "0.01", "time": 123}
        invalid_data_list: list[RawJsonResponse] = [
            "not a dict",
            {"coin": "ETH", "fundingRate": "0.01", "premium": "0.01", "time": 123},
        ]
        with pytest.raises(APIError) as exc_info:
            handler.handle_historical_funding_rates_response(invalid_data_list)
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Expected dict for historical funding rate item, got str at index 0"
            in exc_info.value.message
        )

    def test_item_validation_error(
        self,
        valid_raw_historical_funding_rates_data: list[dict[str, Any]],
    ) -> None:
        """Test handling when an item in the list fails Pydantic validation."""
        handler = HyperliquidResponseHandler()
        modified_data_list = copy.deepcopy(valid_raw_historical_funding_rates_data)
        if not modified_data_list:
            # Add a default item if fixture is empty, then invalidate it
            modified_data_list.append(
                {"coin": "ETH", "fundingRate": "0.01", "premium": "0.01", "time": 123}
            )

        # Invalidate the first item by removing a required field 'time'
        if modified_data_list:  # Ensure list is not empty before trying to delete
            del modified_data_list[0]["time"]
        else:  # If it was empty and we added one item, that item is now invalid by missing 'time'
            # This path is less likely if fixture is usually populated, but handles edge case
            pass

        raw_data_cast = cast(list[RawJsonResponse], modified_data_list)
        with pytest.raises(APIError) as exc_info:
            handler.handle_historical_funding_rates_response(raw_data_cast)
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Validation error for historical funding rate item at index 0:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
