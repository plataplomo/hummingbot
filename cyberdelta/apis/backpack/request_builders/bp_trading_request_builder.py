"""Backpack Trading Request Builder.

This module handles the construction of request payloads for trading operations,
extracted from the monolithic request builder to improve maintainability and testability.

Focused on:
- Order placement requests
- Order cancellation requests (single and batch)
- Order query requests
- Trade history requests
- Trading-related validation and formatting
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawOrderCancelAllRequest,
    BackpackRawOrderCancelRequest,
    BackpackRawOrderExecuteRequest,
)
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetOpenOrdersParams,
    BackpackRawGetOrderHistoryParams,
    BackpackRawGetOrderParams,
    BackpackRawGetTradeHistoryParams,
)
from cyberdelta.apis.backpack.protocols.builder_protocols import TradingRequestBuilderProtocol
from cyberdelta.apis.base.trading_execution_domain import (
    LiquidityRequirement,
    OrderExecution,
    PositionIntent,
)
from cyberdelta.apis.exceptions import (
    InvalidEnumValueError,
    MissingRequiredParameterError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


logger = get_logger(__name__)


class BackpackTradingRequestBuilder(TradingRequestBuilderProtocol):
    """Focused request builder for Backpack trading operations.

    This class contains methods for constructing validated request payloads
    for all trading related API endpoints.
    """

    def __init__(self) -> None:
        """Initialize the trading request builder."""
        logger.debug("Initializing Backpack trading request builder")

    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Generic request builder dispatch method.

        This method serves as the entry point for the registry system
        and dispatches to the appropriate specific builder method based on context.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments including 'operation' to specify the request type

        Returns:
            Request payload dictionary

        Raises:
            NotImplementedError: If operation is not supported or parameters are insufficient
        """
        operation = kwargs.get("operation")
        if not operation:
            raise NotImplementedError(
                "Trading request builder requires 'operation' parameter for generic build_request"
            )

        # Note: Trading request builders require specific parameters for each operation
        # which are not available in the generic build_request interface.
        # This dispatcher is implemented for protocol consistency but most operations
        # will require direct method calls with proper parameters.

        if operation in {
            "place_order",
            "cancel_order",
            "get_order",
            "get_open_orders",
            "get_order_history",
            "get_trade_history",
            "cancel_all_orders",
        }:
            # Trading operations require specific parameters not available in generic interface
            raise NotImplementedError(
                f"Trading operation '{operation}' requires specific parameters not available "
                f"in generic build_request interface. Use specific builder methods directly."
            )
        raise NotImplementedError(
            f"Trading operation '{operation}' not supported by registry dispatch"
        )

    @staticmethod
    def map_order_enums_to_api_strings(
        order_type: OrderType,
        order_side: OrderSide,
        time_in_force: TimeInForce | None,
    ) -> tuple[str, str, str | None]:
        """Map internal enum values to Backpack API string values.

        Args:
            order_type: Internal OrderType enum value
            order_side: Internal OrderSide enum value
            time_in_force: Optional TimeInForce enum value

        Returns:
            Tuple of (order_type_str, order_side_str, time_in_force_str)
        """
        # Map order type enum to API string (Backpack API format)
        order_type_mapping = {
            OrderType.LIMIT: "Limit",
            OrderType.MARKET: "Market",
            OrderType.STOP_LIMIT: "Limit",  # With triggerPrice it becomes a stop limit
            OrderType.STOP_MARKET: "Market",  # With triggerPrice it becomes a stop
            OrderType.TAKE_PROFIT_LIMIT: "Limit",  # With triggerPrice becomes take profit limit
            OrderType.TAKE_PROFIT_MARKET: "Market",  # With triggerPrice it becomes a take profit
        }

        if order_type not in order_type_mapping:
            raise InvalidEnumValueError(
                parameter_name="order_type",
                value=order_type.value,
                valid_values=[ot.value for ot in OrderType],
                enum_type="OrderType",
            )

        # Map order side enum to API string
        order_side_str = "Bid" if order_side == OrderSide.BUY else "Ask"

        # Map time in force enum to API string if provided
        tif_str = None
        if time_in_force is not None:
            tif_mapping = {
                TimeInForce.GTC: "GTC",
                TimeInForce.IOC: "IOC",
                TimeInForce.FOK: "FOK",
                TimeInForce.ALO: "PO",
            }
            if time_in_force not in tif_mapping:
                raise InvalidEnumValueError(
                    parameter_name="time_in_force",
                    value=time_in_force.value,
                    valid_values=[tif.value for tif in TimeInForce],
                    enum_type="TimeInForce",
                )
            tif_str = tif_mapping[time_in_force]

        return order_type_mapping[order_type], order_side_str, tif_str

    @staticmethod
    def add_basic_order_fields(
        request_dict: dict[str, Any],
        order_type: OrderType,
        order_type_str: str,
        order_side_str: str,
        quantity: Decimal,
        price: Decimal | None,
        time_in_force_str: str | None,
        client_order_id: str | None,
        execution: OrderExecution,
    ) -> None:
        """Add basic order fields to the request dictionary.

        Args:
            request_dict: Dictionary to add fields to
            order_type: Internal OrderType enum value
            order_type_str: API order type string
            order_side_str: API order side string
            quantity: Order quantity
            price: Limit price (required for limit orders)
            time_in_force_str: API time in force string
            client_order_id: Optional client order ID
            execution: Order execution configuration with validated policies
        """
        request_dict["orderType"] = order_type_str
        request_dict["side"] = order_side_str

        BackpackTradingRequestBuilder._add_quantity_fields(request_dict, order_type, quantity)
        BackpackTradingRequestBuilder._add_price_fields(request_dict, order_type_str, price)
        BackpackTradingRequestBuilder._add_optional_fields(
            request_dict, time_in_force_str, client_order_id
        )
        BackpackTradingRequestBuilder._add_execution_fields(request_dict, order_type_str, execution)

    @staticmethod
    def _add_quantity_fields(
        request_dict: dict[str, Any], order_type: OrderType, quantity: Decimal
    ) -> None:
        """Add quantity fields based on order type."""
        trigger_order_types = {
            OrderType.STOP_LIMIT,
            OrderType.STOP_MARKET,
            OrderType.TAKE_PROFIT_LIMIT,
            OrderType.TAKE_PROFIT_MARKET,
        }
        if order_type not in trigger_order_types:
            request_dict["quantity"] = str(quantity)

    @staticmethod
    def _add_price_fields(
        request_dict: dict[str, Any], order_type_str: str, price: Decimal | None
    ) -> None:
        """Add price fields for limit orders."""
        if order_type_str == "Limit":
            if price is None:
                raise MissingRequiredParameterError(
                    parameter_name="price",
                    operation=f"{order_type_str} order placement",
                )
            request_dict["price"] = str(price)

    @staticmethod
    def _add_optional_fields(
        request_dict: dict[str, Any], time_in_force_str: str | None, client_order_id: str | None
    ) -> None:
        """Add optional fields to request."""
        if time_in_force_str is not None:
            request_dict["timeInForce"] = time_in_force_str
        if client_order_id is not None:
            request_dict["clientId"] = client_order_id

    @staticmethod
    def _add_execution_fields(
        request_dict: dict[str, Any], order_type_str: str, execution: OrderExecution
    ) -> None:
        """Add execution configuration fields."""
        # Market orders cannot have postOnly flag
        if order_type_str != "Market":
            BackpackTradingRequestBuilder._add_liquidity_fields(request_dict, execution)

        BackpackTradingRequestBuilder._add_position_intent_fields(request_dict, execution)

    @staticmethod
    def _add_liquidity_fields(request_dict: dict[str, Any], execution: OrderExecution) -> None:
        """Add liquidity requirement fields."""
        if execution.liquidity_requirement == LiquidityRequirement.POST_ONLY:
            request_dict["postOnly"] = True
        elif execution.liquidity_requirement == LiquidityRequirement.ANY:
            request_dict["postOnly"] = False

    @staticmethod
    def _add_position_intent_fields(
        request_dict: dict[str, Any], execution: OrderExecution
    ) -> None:
        """Add position intent fields."""
        if execution.position_intent == PositionIntent.REDUCE_ONLY:
            request_dict["reduceOnly"] = True
        elif execution.position_intent == PositionIntent.OPEN_OR_INCREASE:
            request_dict["reduceOnly"] = False

    @staticmethod
    def add_stop_loss_fields(
        request_dict: dict[str, Any],
        stop_price: Decimal,
        quantity: Decimal,
    ) -> None:
        """Add stop loss fields to the request dictionary.

        Args:
            request_dict: Dictionary to add fields to
            stop_price: Stop trigger price
            quantity: Order quantity to use as trigger quantity
        """
        request_dict["triggerPrice"] = str(stop_price)
        request_dict["triggerQuantity"] = str(quantity)

    @staticmethod
    def add_take_profit_fields(
        request_dict: dict[str, Any],
        take_profit_price: Decimal,
        quantity: Decimal,
    ) -> None:
        """Add take profit fields to the request dictionary.

        Args:
            request_dict: Dictionary to add fields to
            take_profit_price: Take profit trigger price
            quantity: Order quantity to use as trigger quantity
        """
        request_dict["triggerPrice"] = str(take_profit_price)
        request_dict["triggerQuantity"] = str(quantity)

    @staticmethod
    def build_place_order_payload(
        symbol: str,
        order_type: OrderType,
        order_side: OrderSide,
        quantity: Decimal,
        price: Decimal | None = None,
        time_in_force: TimeInForce | None = None,
        client_order_id: str | None = None,
        execution: OrderExecution | None = None,
        stop_price: Decimal | None = None,
        take_profit_price: Decimal | None = None,
        self_trade_prevention: str | None = None,
    ) -> BackpackRawOrderExecuteRequest:
        """Build the request payload for placing an order.

        Args:
            symbol: Trading symbol
            order_type: Type of order
            order_side: Buy or sell side
            quantity: Order quantity
            price: Limit price for limit orders
            time_in_force: Time in force option
            client_order_id: Optional client order ID
            execution: Order execution configuration with validated policies
            stop_price: Stop price for stop orders
            take_profit_price: Take profit price for TP orders
            self_trade_prevention: Self trade prevention mode

        Returns:
            BackpackRawOrderExecuteRequest: Validated request payload
        """
        logger.debug(
            "building_place_order_payload",
            symbol=symbol,
            order_type=order_type.value,
            order_side=order_side.value,
            quantity=str(quantity),
            price=str(price) if price else None,
        )

        # Map enums to API strings
        order_type_str, order_side_str, tif_str = (
            BackpackTradingRequestBuilder.map_order_enums_to_api_strings(
                order_type, order_side, time_in_force
            )
        )

        # Build base request dictionary
        request_dict: dict[str, Any] = {"symbol": symbol}

        # Default execution if not provided
        if execution is None:
            execution = OrderExecution()

        # Add basic order fields
        BackpackTradingRequestBuilder.add_basic_order_fields(
            request_dict,
            order_type,
            order_type_str,
            order_side_str,
            quantity,
            price,
            tif_str,
            client_order_id,
            execution,
        )

        # Add stop fields if applicable
        if order_type in {OrderType.STOP_LIMIT, OrderType.STOP_MARKET}:
            if stop_price is None:
                raise MissingRequiredParameterError(
                    parameter_name="stop_price",
                    operation=f"{order_type.value} order placement",
                )
            BackpackTradingRequestBuilder.add_stop_loss_fields(request_dict, stop_price, quantity)

        # Add take profit fields if applicable
        # For TAKE_PROFIT orders, use stop_price as the trigger price (like master branch)
        if order_type in {OrderType.TAKE_PROFIT_LIMIT, OrderType.TAKE_PROFIT_MARKET}:
            if stop_price is None:
                raise MissingRequiredParameterError(
                    parameter_name="stop_price",
                    operation=(
                        f"{order_type.value} order placement (use stop_price as trigger price)"
                    ),
                )
            BackpackTradingRequestBuilder.add_take_profit_fields(request_dict, stop_price, quantity)

        # Add self trade prevention if provided
        if self_trade_prevention is not None:
            request_dict["selfTradePrevention"] = self_trade_prevention

        # Return validated Pydantic model
        return BackpackRawOrderExecuteRequest(**request_dict)

    @staticmethod
    def build_cancel_order_payload(
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> BackpackRawOrderCancelRequest:
        """Build the request payload for cancelling an order.

        Args:
            symbol: Trading symbol
            order_id: Exchange order ID
            client_order_id: Client order ID

        Returns:
            BackpackRawOrderCancelRequest: Validated request payload

        Raises:
            MissingRequiredParameterError: If neither order_id nor client_order_id is provided
        """
        if order_id is None and client_order_id is None:
            raise MissingRequiredParameterError(
                parameter_name="order_id or client_order_id",
                operation="order cancellation",
            )

        logger.debug(
            "building_cancel_order_payload",
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_order_id,
        )

        request_dict: dict[str, Any] = {"symbol": symbol}

        if order_id is not None:
            request_dict["orderId"] = order_id
        if client_order_id is not None:
            request_dict["clientId"] = client_order_id

        return BackpackRawOrderCancelRequest(**request_dict)

    @staticmethod
    def build_cancel_all_orders_payload(
        symbol: str | None = None,
    ) -> BackpackRawOrderCancelAllRequest:
        """Build the request payload for cancelling all orders.

        Args:
            symbol: Optional symbol to cancel orders for (all symbols if None)

        Returns:
            BackpackRawOrderCancelAllRequest: Validated request payload
        """
        logger.debug(
            "building_cancel_all_orders_payload",
            symbol=symbol,
        )

        request_dict: dict[str, Any] = {}

        if symbol is not None:
            request_dict["symbol"] = symbol

        return BackpackRawOrderCancelAllRequest(**request_dict)

    @staticmethod
    def build_get_open_orders_params(symbol: str | None) -> BackpackRawGetOpenOrdersParams:
        """Build query parameters for fetching open orders.

        Args:
            symbol: Optional symbol to filter orders

        Returns:
            BackpackRawGetOpenOrdersParams: Validated query parameters
        """
        logger.debug(
            "building_get_open_orders_params",
            symbol=symbol,
        )

        params_dict: dict[str, Any] = {}

        if symbol is not None:
            params_dict["symbol"] = symbol

        return BackpackRawGetOpenOrdersParams(**params_dict)

    @staticmethod
    def build_get_order_params(symbol: str) -> BackpackRawGetOrderParams:
        """Build query parameters for fetching a specific order.

        Args:
            symbol: Trading symbol

        Returns:
            BackpackRawGetOrderParams: Validated query parameters
        """
        logger.debug(
            "building_get_order_params",
            symbol=symbol,
        )

        return BackpackRawGetOrderParams(symbol=symbol)

    @staticmethod
    def build_get_order_history_params(
        symbol: str | None = None,
        order_id: str | None = None,
        client_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 100,
    ) -> BackpackRawGetOrderHistoryParams:
        """Build query parameters for fetching order history.

        Args:
            symbol: Optional symbol to filter orders
            order_id: Optional specific order ID to fetch
            client_id: Optional client order ID to fetch
            start_time: Optional start timestamp (milliseconds)
            end_time: Optional end timestamp (milliseconds)
            limit: Maximum number of results (default 100)

        Returns:
            BackpackRawGetOrderHistoryParams: Validated query parameters
        """
        logger.debug(
            "building_get_order_history_params",
            symbol=symbol,
            order_id=order_id,
            client_id=client_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

        params_dict: dict[str, Any] = {
            "limit": limit,
        }

        if symbol is not None:
            params_dict["symbol"] = symbol
        if order_id is not None:
            params_dict["orderId"] = order_id
        if client_id is not None:
            params_dict["clientId"] = client_id
        if start_time is not None:
            params_dict["start_time"] = start_time
        if end_time is not None:
            params_dict["end_time"] = end_time

        return BackpackRawGetOrderHistoryParams(**params_dict)

    @staticmethod
    def build_get_trade_history_params(
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 100,
        from_id: str | None = None,
    ) -> BackpackRawGetTradeHistoryParams:
        """Build query parameters for fetching trade history.

        Args:
            symbol: Optional symbol to filter trades
            start_time: Optional start timestamp (milliseconds)
            end_time: Optional end timestamp (milliseconds)
            limit: Maximum number of results (default 100)
            from_id: Optional trade ID to start pagination from

        Returns:
            BackpackRawGetTradeHistoryParams: Validated query parameters
        """
        logger.debug(
            "building_get_trade_history_params",
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            from_id=from_id,
        )

        params_dict: dict[str, Any] = {
            "limit": limit,
        }

        if symbol is not None:
            params_dict["symbol"] = symbol
        if start_time is not None:
            params_dict["start_time"] = start_time
        if end_time is not None:
            params_dict["end_time"] = end_time
        if from_id is not None:
            params_dict["fromId"] = from_id

        return BackpackRawGetTradeHistoryParams(**params_dict)
