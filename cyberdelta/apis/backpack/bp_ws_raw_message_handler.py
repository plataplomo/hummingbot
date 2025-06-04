"""CyberDeltaEngine: Backpack WebSocket Raw Message Handler
-------------------------------------------------------

This module defines the `BackpackWsRawMessageHandler` class. This class is
responsible for validating raw WebSocket message payloads received from the
Backpack exchange against their corresponding Pydantic Raw WebSocket (WS) Models.

It acts as a crucial boundary validation layer, ensuring that any data passed
further into the application from the WebSocket stream has a known and verified
structure according to the defined Raw WS Pydantic models.
"""

from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.models import (
    BackpackRawOrderUpdate,
    BackpackRawPositionUpdate,
    BackpackRawTradeEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


class BackpackWsRawMessageHandler:
    """Handles the validation of raw WebSocket message payloads from Backpack.

    This class provides static methods, each designed to validate a specific
    type of WebSocket message payload (e.g., depth updates, ticker events)
    against its corresponding Pydantic Raw WS Model. If validation fails,
    it raises an `APIError` with an `INVALID_RESPONSE` code.
    """

    @staticmethod
    def handle_depth_payload(payload: dict[str, Any]) -> BackpackRawDepthUpdateEvent:
        """Validate a raw WebSocket depth/order book update payload.

        This method takes a raw dictionary payload, presumably from a Backpack
        WebSocket message concerning depth or order book changes, and validates
        it against the `BackpackRawDepthUpdateEvent` Pydantic model.

        Args:
            payload: The raw dictionary payload of the WebSocket message.
                     Expected to conform to the structure of a depth update.

        Returns:
            A validated `BackpackRawDepthUpdateEvent` instance if the payload
            conforms to the model.

        Raises:
            APIError: If `pydantic.ValidationError` occurs, indicating the payload
                      does not match the `BackpackRawDepthUpdateEvent` schema.
                      The error code will be `APIErrorCode.INVALID_RESPONSE`.

        """
        try:
            validated_model = BackpackRawDepthUpdateEvent.model_validate(payload)
            return validated_model
        except ValidationError as e:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"Invalid WebSocket depth payload: {e}",
                original_exception=e,
                http_status=None,  # Not an HTTP response
            ) from e

    @staticmethod
    def handle_ticker_payload(payload: dict[str, Any]) -> BackpackRawTickerEvent:
        """Validate a raw WebSocket ticker update payload.

        This method validates a raw dictionary payload, expected to represent
        a ticker update from Backpack's WebSocket stream, against the
        `BackpackRawTickerEvent` Pydantic model.

        Args:
            payload: The raw dictionary payload of the WebSocket message,
                     expected to represent a ticker event.

        Returns:
            A validated `BackpackRawTickerEvent` instance if the payload is valid.

        Raises:
            APIError: If `pydantic.ValidationError` occurs, signifying a mismatch
                      with the `BackpackRawTickerEvent` schema. The error code
                      will be `APIErrorCode.INVALID_RESPONSE`.

        """
        try:
            validated_model = BackpackRawTickerEvent.model_validate(payload)
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
        """Validate a raw WebSocket trade event (fill) payload.

        This method ensures that a raw dictionary payload, representing a trade
        or fill event from Backpack's WebSocket stream, conforms to the
        `BackpackRawTradeEvent` Pydantic model.

        Args:
            payload: The raw dictionary payload of the WebSocket message,
                     expected to represent a trade event.

        Returns:
            A validated `BackpackRawTradeEvent` instance upon successful validation.

        Raises:
            APIError: If `pydantic.ValidationError` occurs due to the payload not
                      matching the `BackpackRawTradeEvent` schema. The error code
                      will be `APIErrorCode.INVALID_RESPONSE`.

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
        """Validate a raw WebSocket order update payload.

        This method validates a raw dictionary payload, which should represent
        an order update event from Backpack's WebSocket stream, against the
        `BackpackRawOrderUpdate` Pydantic model.

        Args:
            payload: The raw dictionary payload of the WebSocket message,
                     expected to represent an order update.

        Returns:
            A validated `BackpackRawOrderUpdate` instance if the payload is compliant.

        Raises:
            APIError: If `pydantic.ValidationError` occurs because the payload
                      deviates from the `BackpackRawOrderUpdate` schema. The
                      error code will be `APIErrorCode.INVALID_RESPONSE`.

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
        """Validate a raw WebSocket position update payload.

        This method checks a raw dictionary payload, expected to detail a position
        update from Backpack's WebSocket stream, for conformity with the
        `BackpackRawPositionUpdate` Pydantic model.

        Args:
            payload: The raw dictionary payload of the WebSocket message,
                     expected to represent a position update.

        Returns:
            A validated `BackpackRawPositionUpdate` instance if the payload matches
            the schema.

        Raises:
            APIError: If `pydantic.ValidationError` occurs, indicating the payload
                      is not structured as per `BackpackRawPositionUpdate`. The
                      error code will be `APIErrorCode.INVALID_RESPONSE`.

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
