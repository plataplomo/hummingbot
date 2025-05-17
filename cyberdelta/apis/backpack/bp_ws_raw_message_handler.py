"""
CyberDeltaEngine: Backpack WebSocket Raw Message Handler
-------------------------------------------------------

This module defines the `BackpackWsRawMessageHandler` class, responsible for
validating raw WebSocket message payloads against their corresponding Pydantic
Raw WS Models.
"""

from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.models import (
    BackpackRawOrderBook,  # Assuming this might be used for depth snapshot in WS
    BackpackRawOrderUpdate,
    BackpackRawPositionUpdate,  # Added based on likelihood
    BackpackRawTicker,  # Assuming this might be used for ticker updates in WS
    BackpackRawTradeEvent,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


class BackpackWsRawMessageHandler:
    """
    Handles the validation of raw WebSocket message payloads from Backpack
    against their respective Pydantic Raw WS Models.
    """

    @staticmethod
    def handle_depth_payload(payload: dict[str, Any]) -> BackpackRawOrderBook:
        """
        Validates a raw WebSocket depth/order book update payload.

        Args:
            payload: The raw dictionary payload of the WebSocket message.

        Returns:
            A validated BackpackRawOrderBook instance.

        Raises:
            APIError: If validation against BackpackRawOrderBook fails.
        """
        try:
            # Assuming BackpackRawOrderBook is suitable for WS depth messages.
            # If there's a specific BackpackRawWsDepthUpdate, use that.
            validated_model = BackpackRawOrderBook.model_validate(payload)
            return validated_model
        except ValidationError as e:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"Invalid WebSocket depth payload: {e}",
                original_exception=e,
                http_status=None,  # Not an HTTP response
            ) from e

    @staticmethod
    def handle_ticker_payload(payload: dict[str, Any]) -> BackpackRawTicker:
        """
        Validates a raw WebSocket ticker update payload.

        Args:
            payload: The raw dictionary payload of the WebSocket message.

        Returns:
            A validated BackpackRawTicker instance.

        Raises:
            APIError: If validation against BackpackRawTicker fails.
        """
        try:
            # Assuming BackpackRawTicker is suitable for WS ticker messages.
            # If there's a specific BackpackRawWsTickerUpdate, use that.
            validated_model = BackpackRawTicker.model_validate(payload)
            return validated_model
        except ValidationError as e:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"Invalid WebSocket ticker payload: {e}",
                original_exception=e,
                http_status=None,
            ) from e

    @staticmethod
    def handle_trade_event_payload(payload: dict[str, Any]) -> BackpackRawTradeEvent:
        """
        Validates a raw WebSocket trade event (fill) payload.

        Args:
            payload: The raw dictionary payload of the WebSocket message.

        Returns:
            A validated BackpackRawTradeEvent instance.

        Raises:
            APIError: If validation against BackpackRawTradeEvent fails.
        """
        try:
            validated_model = BackpackRawTradeEvent.model_validate(payload)
            return validated_model
        except ValidationError as e:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"Invalid WebSocket trade event payload: {e}",
                original_exception=e,
                http_status=None,
            ) from e

    @staticmethod
    def handle_order_update_payload(payload: dict[str, Any]) -> BackpackRawOrderUpdate:
        """
        Validates a raw WebSocket order update payload.

        Args:
            payload: The raw dictionary payload of the WebSocket message.

        Returns:
            A validated BackpackRawOrderUpdate instance.

        Raises:
            APIError: If validation against BackpackRawOrderUpdate fails.
        """
        try:
            validated_model = BackpackRawOrderUpdate.model_validate(payload)
            return validated_model
        except ValidationError as e:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"Invalid WebSocket order update payload: {e}",
                original_exception=e,
                http_status=None,
            ) from e

    @staticmethod
    def handle_position_update_payload(payload: dict[str, Any]) -> BackpackRawPositionUpdate:
        """
        Validates a raw WebSocket position update payload.

        Args:
            payload: The raw dictionary payload of the WebSocket message.

        Returns:
            A validated BackpackRawPositionUpdate instance.

        Raises:
            APIError: If validation against BackpackRawPositionUpdate fails.
        """
        try:
            validated_model = BackpackRawPositionUpdate.model_validate(payload)
            return validated_model
        except ValidationError as e:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"Invalid WebSocket position update payload: {e}",
                original_exception=e,
                http_status=None,
            ) from e
