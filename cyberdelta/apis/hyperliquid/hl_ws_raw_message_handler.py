"""CyberDeltaEngine: Hyperliquid WebSocket Raw Message Handler.

---------------------------------------------------------

This module defines the `HyperliquidWsRawMessageHandler` class. This class is
dedicated to validating raw WebSocket message payloads received from the
Hyperliquid exchange against their corresponding Pydantic Raw WebSocket (WS) Models.

It serves as a critical boundary validation component, ensuring that all data
from Hyperliquid's WebSocket stream is structurally sound and type-consistent
according to the defined Raw WS Pydantic models before being processed further
by the application.
"""

from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
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
from cyberdelta.config.structlog_config import get_logger


_BM = TypeVar("_BM", bound=BaseModel)

# Get logger for the module
logger = get_logger(__name__)


class HyperliquidWsRawMessageHandler:
    """Validates raw WebSocket message payloads from Hyperliquid.

    This class provides static methods to validate different types of WebSocket
    message payloads (e.g., L2 book updates, public trades, user-specific events)
    from Hyperliquid against their specific Pydantic Raw WS Models. Failed
    validations result in an `APIError` with an `INVALID_RESPONSE` code.
    """

    @staticmethod
    def _validate_payload(
        payload: dict[str, Any],
        model_class: type[_BM],
        event_type_description: str,
    ) -> _BM:
        """Perform generic validation of a payload against a Pydantic model.

        This private helper method is used by other static methods in this class
        to centralize the Pydantic validation logic and error handling for
        different types of WebSocket payloads.

        Args:
            payload: The raw dictionary payload to validate.
            model_class: The Pydantic model class to validate against.
            event_type_description: A human-readable string describing the type
                                      of event being validated (for error messages).

        Returns:
            A validated Pydantic model instance of type `_BM`.

        Raises:
            APIError: If `pydantic.ValidationError` occurs, indicating the payload
                      does not conform to the `model_class` schema. The error
                      code will be `APIErrorCode.INVALID_RESPONSE`.

        """
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
        """Validate a raw WebSocket L2 book update payload from Hyperliquid.

        This method checks if the given payload, presumably from Hyperliquid's
        'l2Book' WebSocket channel, conforms to the `HyperliquidRawWsBookUpdate`
        Pydantic model.

        Args:
            payload: The raw dictionary payload of the L2 book update message.

        Returns:
            A validated `HyperliquidRawWsBookUpdate` instance.

        Raises:
            APIError: If validation fails, with code `APIErrorCode.INVALID_RESPONSE`.

        """
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload,
            HyperliquidRawWsBookUpdate,
            "L2 book update",
        )

    @staticmethod
    def handle_public_trades_payload(
        payload_list: list[dict[str, Any]],
    ) -> list[HyperliquidRawWsTradeEvent]:
        """Validate a list of raw WebSocket public trade event payloads from Hyperliquid.

        Hyperliquid's 'trades' channel sends a list of trade objects. This method
        iterates through the list, validating each item against the
        `HyperliquidRawWsTradeEvent` Pydantic model.

        Args:
            payload_list: A list of raw dictionary payloads, where each dictionary
                          represents a public trade event.

        Returns:
            A list of validated `HyperliquidRawWsTradeEvent` instances.

        Raises:
            APIError: If validation of any item in the list fails, with code
                      `APIErrorCode.INVALID_RESPONSE`. The error message will indicate
                      the index of the problematic item.

        """
        validated_trades: list[HyperliquidRawWsTradeEvent] = []
        for i, trade_payload in enumerate(payload_list):
            try:
                validated_trades.append(
                    HyperliquidWsRawMessageHandler._validate_payload(
                        trade_payload,
                        HyperliquidRawWsTradeEvent,
                        f"public trade item #{i}",
                    ),
                )
            except APIError as e:
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
        """Validate a raw WebSocket user fill event payload from Hyperliquid's userEvents stream.

        This method ensures the payload, representing a user's trade execution (fill),
        conforms to the `HyperliquidRawWsFillEvent` Pydantic model.

        Args:
            payload: The raw dictionary payload of the user fill event.

        Returns:
            A validated `HyperliquidRawWsFillEvent` instance.

        Raises:
            APIError: If validation fails, with code `APIErrorCode.INVALID_RESPONSE`.

        """
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload,
            HyperliquidRawWsFillEvent,
            "user fill event",
        )

    @staticmethod
    def handle_user_order_event_payload(payload: dict[str, Any]) -> HyperliquidRawOrder:
        """Validate the inner 'order' part of a user order event from Hyperliquid userEvents stream.

        User order events in Hyperliquid's WebSocket stream are often wrapped.
        This method specifically validates the nested dictionary that contains the actual
        order details against the `HyperliquidRawOrder` Pydantic model.

        Args:
            payload: The raw dictionary payload representing the core order details,
                     extracted from the `data` field of an outer order update wrapper.

        Returns:
            A validated `HyperliquidRawOrder` instance.

        Raises:
            APIError: If validation fails, with code `APIErrorCode.INVALID_RESPONSE`.

        """
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload,
            HyperliquidRawOrder,
            "user order event (inner detail)",
        )

    @staticmethod
    def handle_user_order_update_wrapper_payload(
        payload: dict[str, Any],
    ) -> HyperliquidRawWsOrderUpdate:
        """Validate the outer wrapper of a user order update event from Hyperliquid.

        This method validates the top-level structure of a user order update event
        (which typically contains fields like `eventType` and `data`) against the
        `HyperliquidRawWsOrderUpdate` Pydantic model.

        Args:
            payload: The raw dictionary payload of the entire user order update event.

        Returns:
            A validated `HyperliquidRawWsOrderUpdate` instance.

        Raises:
            APIError: If validation fails, with code `APIErrorCode.INVALID_RESPONSE`.

        """
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload,
            HyperliquidRawWsOrderUpdate,
            "user order update wrapper",
        )

    @staticmethod
    def handle_user_position_update_event_payload(
        payload: dict[str, Any],
    ) -> HyperliquidRawWsPositionUpdateEvent:
        """Validate a raw WebSocket user position update event payload from Hyperliquid userEvents.

        This method checks if the payload, detailing a change in a user's position,
        conforms to the `HyperliquidRawWsPositionUpdateEvent` Pydantic model.

        Args:
            payload: The raw dictionary payload of the user position update event.

        Returns:
            A validated `HyperliquidRawWsPositionUpdateEvent` instance.

        Raises:
            APIError: If validation fails, with code `APIErrorCode.INVALID_RESPONSE`.

        """
        return HyperliquidWsRawMessageHandler._validate_payload(
            payload,
            HyperliquidRawWsPositionUpdateEvent,
            "user position update event",
        )

    @staticmethod
    def handle_all_mids_payload(payload: dict[str, Any]) -> HyperliquidRawAllMids:
        """Validate the raw payload for a Hyperliquid 'allMids' WebSocket message.

        The 'allMids' channel provides a dictionary mapping asset symbols to their
        mid prices. This method validates this dictionary against the
        `HyperliquidRawAllMids` Pydantic model (which is a `RootModel`).

        Args:
            payload: The raw dictionary payload from the 'allMids' WebSocket message,
                     expected to be a map of asset names to string-represented prices.

        Returns:
            A validated `HyperliquidRawAllMids` instance.

        Raises:
            APIError: If validation against `HyperliquidRawAllMids` fails (e.g.,
                      if the payload is not a dictionary, or if keys/values
                      do not conform to `RawAssetString64HL` and `RawFiniteDecimalStr`).
                      The error code will be `APIErrorCode.INVALID_RESPONSE`.

        """
        try:
            # HyperliquidRawAllMids is a RootModel, expects the dict itself
            return HyperliquidRawAllMids.model_validate(payload)
        except ValidationError as e:
            logger.error(
                "invalid_all_mids_ws_payload",
                action="validate_ws_message",
                error=str(e),
                payload=repr(payload),
                message=f"Invalid Hyperliquid 'allMids' WS payload: {e}. Payload: {payload!r}",
            )
            raise APIError(
                f"Invalid Hyperliquid 'allMids' WS payload: {e}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e,
            ) from e
