"""
CyberDeltaEngine: Hyperliquid WebSocket Message Router
-----------------------------------------------------

This module implements the `HyperliquidWsMessageRouter` class, responsible for:
- Constructing subscription payloads for WebSocket topics
- Routing and processing incoming WebSocket messages
- Validating raw payloads and transforming them to internal domain models

This router isolates all WebSocket-specific logic from the main HyperliquidAPI client,
providing a cleaner separation of concerns and better testability.
"""

from __future__ import annotations

from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.base.exchange_api import MessageHandler
from cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler import HyperliquidWsRawMessageHandler
from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper import HyperliquidMarketDataMapper
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import (
    HyperliquidRawWsAllMidsSubscriptionPayload,
    HyperliquidRawWsCandleSubscriptionPayload,
    HyperliquidRawWsL2BookSubscriptionPayload,
    HyperliquidRawWsSubscribeRequest,
    HyperliquidRawWsTradesSubscriptionPayload,
    HyperliquidRawWsUserEventsSubscriptionPayload,
)
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger


class HyperliquidWsMessageRouter:
    """
    Routes and processes WebSocket messages for Hyperliquid exchange.

    This class handles the entire WebSocket message processing pipeline:
    1. Constructs subscription payloads for various topics
    2. Validates incoming raw WebSocket payloads
    3. Transforms raw data to internal domain models
    4. Routes processed data to appropriate application handlers
    """

    def __init__(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        account_data_mapper: HyperliquidAccountDataMapper,
        trading_data_mapper: HyperliquidTradingDataMapper,
        raw_ws_handler: HyperliquidWsRawMessageHandler,
        exchange_name: str,
    ) -> None:
        """
        Initialize the WebSocket message router.

        Args:
            market_data_mapper: Mapper for market data transformations
            account_data_mapper: Mapper for account data transformations
            trading_data_mapper: Mapper for trading data transformations
            raw_ws_handler: Handler for raw WebSocket message validation
            exchange_name: Name of the exchange for logging purposes
        """
        self._market_data_mapper = market_data_mapper
        self._account_data_mapper = account_data_mapper
        self._trading_data_mapper = trading_data_mapper
        self._raw_ws_handler = raw_ws_handler
        self._exchange_name = exchange_name
        self.logger = get_logger(__name__)

    def construct_subscription_payload(
        self, topic: str, wallet_address: str | None
    ) -> HyperliquidRawWsSubscribeRequest:
        """
        Construct the subscription payload for a given topic for Hyperliquid.

        Hyperliquid uses a format like:
        {"method": "subscribe", "subscription": payload}
        Payload depends on the subscription type.

        Args:
            topic: The WebSocket topic to subscribe to
            wallet_address: User's wallet address (required for userEvents)

        Returns:
            HyperliquidRawWsSubscribeRequest model

        Raises:
            ValueError: If topic format is invalid or required info is missing
            APIError: If topic is not supported by the exchange
        """
        # Parse the topic to determine subscription type and parameters
        # Hyperliquid topics: "l2Book:ETH", "trades:BTC", "userEvents", "candle:BTC:1m", "allMids"
        parts = topic.split(":", 2)  # Split into at most 3 parts
        sub_type = parts[0]

        inner_payload: (
            HyperliquidRawWsL2BookSubscriptionPayload
            | HyperliquidRawWsTradesSubscriptionPayload
            | HyperliquidRawWsUserEventsSubscriptionPayload
            | HyperliquidRawWsCandleSubscriptionPayload
            | HyperliquidRawWsAllMidsSubscriptionPayload
        )

        try:
            if sub_type == "l2Book" and len(parts) >= 2:
                coin = parts[1]
                inner_payload = HyperliquidRawWsL2BookSubscriptionPayload(type="l2Book", coin=coin)
            elif sub_type == "trades" and len(parts) >= 2:
                coin = parts[1]
                inner_payload = HyperliquidRawWsTradesSubscriptionPayload(type="trades", coin=coin)
            elif sub_type == "userEvents":
                if wallet_address is None:
                    raise ValueError(
                        "Cannot subscribe to userEvents without wallet address. "
                        "Ensure private_key is configured in secrets."
                    )
                inner_payload = HyperliquidRawWsUserEventsSubscriptionPayload(
                    type="userEvents", user=wallet_address
                )
            elif sub_type == "candle" and len(parts) >= 3:
                coin = parts[1]
                interval = parts[2]
                inner_payload = HyperliquidRawWsCandleSubscriptionPayload(
                    type="candle", coin=coin, interval=interval
                )
            elif sub_type == "allMids":
                inner_payload = HyperliquidRawWsAllMidsSubscriptionPayload(type="allMids")
            else:
                raise APIError(
                    f"Unsupported WebSocket topic: {topic}. "
                    f"Supported formats: 'l2Book:COIN', 'trades:COIN', 'userEvents', "
                    f"'candle:COIN:INTERVAL', 'allMids'",
                    code=APIErrorCode.INVALID_PARAMS.value,
                )

            # Create the top-level subscription request
            return HyperliquidRawWsSubscribeRequest(
                method="subscribe",  # Hyperliquid uses lowercase
                subscription=inner_payload,
            )

        except (ValueError, APIError):
            # Re-raise these exceptions as they are already informative
            raise
        except Exception as e:
            # Wrap unexpected exceptions
            raise ValueError(
                f"Failed to construct subscription payload for topic '{topic}': {e}"
            ) from e

    async def route_message(
        self, message: dict[str, Any], ws_handlers: dict[str, MessageHandler]
    ) -> None:
        """
        Route incoming WebSocket messages from Hyperliquid.

        This method processes the complete WebSocket message handling pipeline:
        1. Extracts channel and data from the message
        2. Finds the appropriate application handler
        3. Validates the raw payload using HyperliquidWsRawMessageHandler
        4. Transforms the validated payload to internal domain models
        5. Calls the application handler with the transformed data

        Args:
            message: The raw WebSocket message dictionary
            ws_handlers: Mapping of topic strings to application MessageHandler callbacks
        """
        channel: str | None = message.get("channel")
        raw_data_any: Any = message.get("data")  # Keep as Any initially

        if not channel:
            self.logger.debug(
                f"[{self._exchange_name}] Unroutable WS message (no channel): {message}"
            )
            return

        if channel in ["pong", "subscriptionResponse"]:
            self.logger.debug(f"[{self._exchange_name}] Control message on '{channel}': {message}")
            return

        # Construct topic key for handler lookup
        topic_key_for_handler = channel
        if channel == "l2Book":
            if isinstance(raw_data_any, dict):
                raw_data_dict = cast(dict[str, Any], raw_data_any)
                coin_from_data_any: Any = raw_data_dict.get("coin")
                if isinstance(coin_from_data_any, str):
                    topic_key_for_handler = f"{channel}:{coin_from_data_any}"
            else:
                self.logger.warning(
                    f"[{self._exchange_name}] Expected dict for 'l2Book' data to derive topic key, "
                    f"received other type. Msg: {message}"
                )
        elif channel == "trades":
            coin_for_topic_str: str | None = None
            if isinstance(raw_data_any, list):
                # Explicitly type the list after check, elements are still Any
                checked_list_for_topic_derivation: list[dict[str, Any]] = []
                # After isinstance check, we know it's a list
                # DEFENSIVE CHECK: raw_data_any is confirmed as list[Any] by isinstance.
                # Mypy=[redundant-cast]
                raw_list: list[Any] = cast(list[Any], raw_data_any)  # type: ignore[redundant-cast]
                for item in raw_list:
                    if isinstance(item, dict):
                        checked_list_for_topic_derivation.append(cast(dict[str, Any], item))

                if checked_list_for_topic_derivation:
                    first_item_for_topic_any: Any = checked_list_for_topic_derivation[0]
                    if isinstance(first_item_for_topic_any, dict):
                        first_item_dict = cast(dict[str, Any], first_item_for_topic_any)
                        coin_from_item_any: Any = first_item_dict.get("coin")
                        if isinstance(coin_from_item_any, str):
                            coin_for_topic_str = coin_from_item_any

            if coin_for_topic_str:
                topic_key_for_handler = f"{channel}:{coin_for_topic_str}"
        elif channel == "userEvents":
            topic_key_for_handler = "userEvents"

        # Find application handler
        app_handler = ws_handlers.get(topic_key_for_handler)
        if not app_handler:
            generic_app_handler = ws_handlers.get(channel)
            if generic_app_handler:
                app_handler = generic_app_handler
            else:
                log_parts = [f"[{self._exchange_name}] No WS handler for '{topic_key_for_handler}'"]
                if topic_key_for_handler != channel:
                    log_parts.append(f" (or base '{channel}')")
                log_parts.append(f". Msg: {message}")
                self.logger.debug("".join(log_parts))
                return

        if raw_data_any is None:
            self.logger.warning(
                f"[{self._exchange_name}] WS '{channel}' has no data. Msg: {message}"
            )
            return

        try:
            payload_for_handler: dict[str, Any] | None = None

            if channel == "l2Book":
                if not isinstance(raw_data_any, dict):
                    raise APIError(
                        "l2Book data not dict",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )
                validated_book_model = self._raw_ws_handler.handle_l2book_payload(
                    cast(dict[str, Any], raw_data_any)
                )

                try:
                    # Transform raw validated model to internal domain model
                    internal_orderbook = (
                        self._market_data_mapper.transform_ws_book_update_to_internal(
                            validated_book_model
                        )
                    )
                    # Convert internal model to dict for handler compatibility
                    orderbook_dict = internal_orderbook.model_dump(mode="json")
                    await app_handler(orderbook_dict, message)
                except TransformationError as e_transform:
                    self.logger.error(
                        f"[{self._exchange_name}] Failed to transform l2Book data: {e_transform}"
                    )
                return

            elif channel == "trades":
                if not isinstance(raw_data_any, list):
                    raise APIError(
                        "trades data not list",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                # Convert list items to dict format for validation
                trade_payloads: list[dict[str, Any]] = []
                # Explicitly type the list after check, elements are still Any
                checked_list_of_trades: list[dict[str, Any]] = []
                # After isinstance check, we know it's a list
                # DEFENSIVE CHECK: raw_data_any is confirmed as list[Any] by isinstance.
                # Mypy=[redundant-cast]
                raw_list_trades: list[Any] = cast(list[Any], raw_data_any)  # type: ignore[redundant-cast]
                for item in raw_list_trades:
                    if isinstance(item, dict):
                        item_dict = cast(dict[str, Any], item)
                        checked_list_of_trades.append(item_dict)
                        trade_payloads.append(item_dict)

                if trade_payloads:
                    validated_trade_models = self._raw_ws_handler.handle_public_trades_payload(
                        trade_payloads
                    )
                    for validated_trade_model in validated_trade_models:
                        try:
                            # Transform raw validated model to internal domain model
                            internal_trade = (
                                self._market_data_mapper.transform_ws_trade_event_to_internal(
                                    validated_trade_model
                                )
                            )
                            # Convert internal model to dict for handler compatibility
                            trade_dict = internal_trade.model_dump(mode="json")
                            await app_handler(trade_dict, message)
                        except TransformationError as e_transform:
                            self.logger.error(
                                f"[{self._exchange_name}] Failed to transform trade "
                                f"data: {e_transform}"
                            )
                return

            elif channel == "userEvents":
                if not isinstance(raw_data_any, list):
                    raise APIError(
                        "userEvents data not list",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )
                # Explicitly type the list after check, elements are still Any
                checked_list_of_any_events: list[dict[str, Any]] = []
                # After isinstance check, we know it's a list
                # DEFENSIVE CHECK: raw_data_any is confirmed as list[Any] by isinstance.
                # Mypy=[redundant-cast]
                raw_list_events: list[Any] = cast(list[Any], raw_data_any)  # type: ignore[redundant-cast]
                for event_loop_var_any in raw_list_events:
                    if isinstance(event_loop_var_any, dict):
                        checked_list_of_any_events.append(cast(dict[str, Any], event_loop_var_any))

                for event_item_dict in checked_list_of_any_events:
                    event_type_any = event_item_dict.get("type")

                    if not isinstance(event_type_any, str):
                        self.logger.warning(
                            f"[{self._exchange_name}] userEvent item has no 'type' string: "
                            f"{event_item_dict}, skipping."
                        )
                        continue

                    event_type_str: str = event_type_any

                    try:
                        if event_type_str == "fill":
                            validated_fill = self._raw_ws_handler.handle_user_fill_event_payload(
                                event_item_dict
                            )
                            try:
                                # Transform raw validated model to internal domain model
                                transform_method = (
                                    self._account_data_mapper.transform_ws_fill_event_to_internal
                                )
                                internal_trade = transform_method(validated_fill)
                                # Convert internal model to dict for handler compatibility
                                trade_dict = internal_trade.model_dump(mode="json")
                                await app_handler(trade_dict, message)
                            except TransformationError as e_transform:
                                self.logger.error(
                                    f"[{self._exchange_name}] Failed to transform fill "
                                    f"event: {e_transform}"
                                )

                        elif event_type_str == "order":
                            _handle_order_wrapper = (
                                self._raw_ws_handler.handle_user_order_update_wrapper_payload
                            )
                            order_update_wrapper = _handle_order_wrapper(event_item_dict)
                            _handle_order_event = (
                                self._raw_ws_handler.handle_user_order_event_payload
                            )
                            validated_order_details = _handle_order_event(order_update_wrapper.data)
                            try:
                                # Transform raw validated model to internal domain model
                                order_transform_method = (
                                    self._trading_data_mapper.transform_ws_order_update_to_internal_order
                                )
                                internal_order = order_transform_method(validated_order_details)
                                # Convert internal model to dict for handler compatibility
                                order_dict = internal_order.model_dump(mode="json")
                                await app_handler(order_dict, message)
                            except TransformationError as e_transform:
                                self.logger.error(
                                    f"[{self._exchange_name}] Failed to transform order "
                                    f"event: {e_transform}"
                                )

                        elif event_type_str == "positionUpdate":
                            _handle_pos_update = (
                                self._raw_ws_handler.handle_user_position_update_event_payload
                            )
                            validated_position_update = _handle_pos_update(event_item_dict)
                            try:
                                # Transform raw validated model to internal domain model
                                position_transform_method = (
                                    self._account_data_mapper.transform_ws_position_update_to_internal_position
                                )
                                internal_position = position_transform_method(
                                    validated_position_update
                                )
                                # Convert internal model to dict for handler compatibility
                                position_dict = internal_position.model_dump(mode="json")
                                await app_handler(position_dict, message)
                            except TransformationError as e_transform:
                                self.logger.error(
                                    f"[{self._exchange_name}] Failed to transform position "
                                    f"event: {e_transform}"
                                )

                        else:
                            self.logger.debug(
                                f"[{self._exchange_name}] Unhandled userEvent type: "
                                f"{event_type_str}. Passing raw item: {event_item_dict}"
                            )
                            await app_handler(event_item_dict, message)

                    except (APIError, ValidationError) as e_user_event_item:
                        self.logger.error(
                            f"[{self._exchange_name}] Error processing userEvent item "
                            f"(type: {event_type_str}): {e_user_event_item}. "
                            f"Item: {event_item_dict}. Skipping item."
                        )
                        continue
                return  # All user events handled, exit

            elif channel == "allMids":
                if not isinstance(raw_data_any, dict):
                    self.logger.warning(
                        f"[{self._exchange_name}] 'allMids' channel data is not a dict or is None. "
                        f"Data: {raw_data_any!r}. Skipping."
                    )
                    raise APIError(
                        "allMids data not dict or is None",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                raw_data_dict_all_mids = cast(dict[str, Any], raw_data_any)

                validated_all_mids = self._raw_ws_handler.handle_all_mids_payload(
                    raw_data_dict_all_mids
                )
                payload_for_handler = validated_all_mids.model_dump(mode="json")

            elif channel == "pong" or channel == "subscriptionResponse":
                self.logger.debug(
                    f"[{self._exchange_name}] Control message on '{channel}': {message}"
                )
                payload_for_handler = (
                    cast(dict[str, Any], raw_data_any) if isinstance(raw_data_any, dict) else {}
                )
            else:
                self.logger.debug(
                    f"[{self._exchange_name}] Unhandled channel '{channel}' by specific "
                    f"validation, passing raw data if dict. Msg: {message}"
                )
                payload_for_handler = (
                    cast(dict[str, Any], raw_data_any) if isinstance(raw_data_any, dict) else {}
                )

            # Final handler call for channels that set payload_for_handler and don't return early
            if payload_for_handler is not None:
                await app_handler(payload_for_handler, message)
            # If payload_for_handler is None here, it means a path was taken that didn't set it
            # and didn't explicitly return (e.g. trades/userEvents handle their own calls to
            # app_handler) or an empty list for trades was encountered and returned early.

        except APIError as e:
            self.logger.error(
                f"[{self._exchange_name}] APIError in WS routing for {channel}: {e.message}",
                exc_info=True,
            )
        except ValidationError as e_val:
            self.logger.error(
                f"[{self._exchange_name}] Unexpected Pydantic ValidationErr for {channel}: {e_val}",
                exc_info=True,
            )
        except Exception as e_app:
            self.logger.error(
                f"[{self._exchange_name}] Error in app_handler for {channel}: {e_app}",
                exc_info=True,
            )
