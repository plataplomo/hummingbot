"""
CyberDeltaEngine: Hyperliquid WebSocket Raw Message Handler
---------------------------------------------------------

This module defines the `HyperliquidWsRawMessageHandler` class, responsible for
validating raw WebSocket message payloads against their corresponding Pydantic
Raw WS Models for Hyperliquid.
"""

from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsOrderUpdate,  # Wrapper for order events in user stream
    HyperliquidRawWsPositionUpdateEvent,
    HyperliquidRawWsTradeEvent,  # For public trades stream
    # HyperliquidRawWsUserEvent, # If a general user event wrapper exists
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

_BM = TypeVar("_BM", bound=BaseModel)


class HyperliquidWsRawMessageHandler:
    """
    Handles the validation of raw WebSocket message payloads from Hyperliquid
    against their respective Pydantic Raw WS Models.
    """

    @staticmethod
    def _validate_payload(
        payload: dict[str, Any], model_class: type[_BM], event_type_description: str
    ) -> _BM:
        """Generic helper to validate a payload against a Pydantic model."""
        try:
            return model_class.model_validate(payload)
        except ValidationError as e:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"Invalid WebSocket {event_type_description} payload: {e}",
                original_exception=e,
                http_status=None,
            ) from e

    @staticmethod
    def handle_l2book_payload(payload: dict[str, Any]) -> HyperliquidRawWsBookUpdate:
        """Validates a raw WebSocket L2 book update payload."""
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload, HyperliquidRawWsBookUpdate, "L2 book update"
        )

    @staticmethod
    def handle_public_trades_payload(
        payload_list: list[dict[str, Any]],
    ) -> list[HyperliquidRawWsTradeEvent]:
        """Validates a list of raw WebSocket public trade event payloads.
        The 'trades' channel sends a list of trade objects.
        """
        validated_trades: list[HyperliquidRawWsTradeEvent] = []
        for i, trade_payload in enumerate(payload_list):
            try:
                validated_trades.append(
                    HyperliquidWsRawMessageHandler._validate_payload(
                        trade_payload, HyperliquidRawWsTradeEvent, f"public trade item #{i}"
                    )
                )
            except APIError as e:  # Catch and enrich error from _validate_payload
                # Or decide to collect errors and raise a single one, or skip invalid items
                # For now, let the first error propagate, or log and skip.
                # Prompt implies individual validation, raising APIError on first failure.
                # To make it robust, we might want to log and skip, returning only valid ones.
                # For now, re-raising directly for simplicity as per prompt structure.
                raise APIError(
                    code=e.code,
                    message=f"Error in public trades list at index {i}: {e.message}",
                    original_exception=e.original_exception,
                    http_status=None,
                ) from e
        return validated_trades

    # --- User Event Stream Payloads --- #
    # Hyperliquid 'userEvents' stream sends a list of events, each with a type.
    # The _route_ws_message will iterate through this list and call appropriate handlers here.

    @staticmethod
    def handle_user_fill_event_payload(payload: dict[str, Any]) -> HyperliquidRawWsFillEvent:
        """Validates a raw WebSocket user fill event payload from userEvents stream."""
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload, HyperliquidRawWsFillEvent, "user fill event"
        )

    @staticmethod
    def handle_user_order_event_payload(payload: dict[str, Any]) -> HyperliquidRawOrder:
        """
        Validates the inner 'order' part of a user order event from userEvents stream.
        The top-level user event might be wrapped in something like HyperliquidRawWsOrderUpdate,
        but the actual order data is what this method validates against HyperliquidRawOrder.
        """
        # This assumes `payload` is the actual dictionary representing the order details,
        # not the outer HyperliquidRawWsOrderUpdate wrapper.
        # The calling code in hl_api.py will need to extract this specific dict.
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload, HyperliquidRawOrder, "user order event (inner detail)"
        )

    @staticmethod
    def handle_user_order_update_wrapper_payload(
        payload: dict[str, Any],
    ) -> HyperliquidRawWsOrderUpdate:
        """Validates the outer wrapper of a user order update event (eventType, data)."""
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload, HyperliquidRawWsOrderUpdate, "user order update wrapper"
        )

    @staticmethod
    def handle_user_position_update_event_payload(
        payload: dict[str, Any],
    ) -> HyperliquidRawWsPositionUpdateEvent:
        """Validates a raw WebSocket user position update event payload from userEvents stream."""
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload, HyperliquidRawWsPositionUpdateEvent, "user position update event"
        )

    # Potentially add a handler for 'allMids' if needed, assuming a HyperliquidRawWsAllMids model
    # @staticmethod
    # def handle_all_mids_payload(payload: dict[str, Any]) -> HyperliquidRawWsAllMids:
    #     return HyperliquidWsRawMessageHandler._validate_payload(
    #         payload, HyperliquidRawWsAllMids, "all mids update"
    #     )
