"""Backpack WebSocket Message Envelope Models.

This module defines Pydantic models for the WebSocket message envelope structure
as specified in the Backpack API documentation.

According to the Backpack WebSocket API specification:
"All data from streams is wrapped in a JSON object of the following form:
{
  "stream": "<stream>",
  "data": "<payload>"
}"

This module provides proper type-safe envelope validation before content processing.
"""

import re
import time
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode
from pydantic_core.core_schema import ValidatorFunctionWrapHandler

from cyberdelta.apis.backpack.bp_validators import BackpackValidators
from cyberdelta.apis.exceptions import EmptyStringError
from cyberdelta.config.structlog_config import get_logger

from .bp_common_raw_types import RawBpNonEmptyStringMax128


logger = get_logger(__name__)

# Constants for stream validation
MIN_STREAM_PARTS = 2
MIN_KLINE_PARTS = 3
MAX_DICT_SIZE = 1000
MAX_LIST_SIZE = 10000
VALIDATION_THRESHOLD_MS = 0.001  # 1ms threshold for logging


class BackpackSubscriptionResponse(BaseModel):
    """Subscription confirmation response from Backpack WebSocket.

    This is Backpack's specific response format which differs from the base
    subscription response. Backpack uses 'result' instead of 'success' and
    doesn't include error messages or subscribed topics list.

    Format: {"result": true, "id": 1}
    """

    result: bool = Field(description="Whether the subscription was successful")
    id: int | None = Field(default=None, description="Request ID if provided")

    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="forbid",
    )


class BackpackRawWebSocketEnvelope(BaseModel):
    """Raw WebSocket message envelope from Backpack.

    This model validates the outer envelope structure of all Backpack WebSocket messages
    according to the official Backpack API specification:

    "All data from streams is wrapped in a JSON object of the following form:
    {
      "stream": "<stream>",
      "data": "<payload>"
    }"

    The actual payload validation is handled by stream-specific processors.

    Examples:
    - Public stream: {"stream": "depth.SOL_USDC", "data": {...}}
    - Private stream: {"stream": "account.orderUpdate", "data": {...}}
    - K-line stream: {"stream": "kline.1m.SOL_USDC", "data": {...}}
    """

    stream: RawBpNonEmptyStringMax128 = Field(
        ...,
        description=(
            "Stream identifier following Backpack's <type>.<symbol> "
            "or <type>.<interval>.<symbol> format"
        ),
    )
    data: dict[str, Any] | list[Any] = Field(
        ...,
        description="Stream-specific payload data as defined in Backpack API documentation",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
        json_schema_extra={
            "title": "Backpack WebSocket Message Envelope",
            "description": "Standard envelope structure for all Backpack WebSocket messages",
            "examples": [
                {
                    "stream": "depth.BTC_USDC",
                    "data": {"bids": [["50000.0", "1.5"]], "asks": [["50100.0", "0.8"]]},
                },
                {
                    "stream": "ticker.SOL_USDC",
                    "data": {"lastPrice": "180.50", "priceChange24h": "5.20"},
                },
                {
                    "stream": "kline.1m.ETH_USDC",
                    "data": {
                        "open": "3000.0",
                        "high": "3050.0",
                        "low": "2980.0",
                        "close": "3020.0",
                    },
                },
            ],
            "x-ws-message-type": "market-data",
            "x-exchange": "backpack",
            "x-api-version": "v1",
        },
    )

    @field_validator("stream", mode="before")
    @classmethod
    def normalize_and_validate_stream(cls, v: str | bytes | float | None) -> str:
        """Normalize stream field with pre-validation.

        This validator handles input normalization before main validation.

        Supports:
        - Stream format: "depth.SOL_USDC"
        - String normalization: whitespace trimming, case normalization
        """
        # Handle None input
        if v is None:
            # Let Pydantic's required field validation handle this
            raise EmptyStringError("stream")

        # Convert input to string with proper handling
        if isinstance(v, bytes):
            v = v.decode("utf-8")
        elif isinstance(v, (int, float)):
            v = str(v)
        # str case is handled implicitly - no conversion needed

        v = v.strip()
        return cls._normalize_stream_format(v)

    @classmethod
    def _normalize_stream_format(cls, v: str) -> str:
        """Normalize stream format patterns for consistency."""
        # Handle depth streams
        if v.startswith("depth."):
            return cls._normalize_depth_stream(v)

        # Handle other stream types with dot notation
        if "." in v:
            return cls._normalize_dotted_stream(v)

        # Handle special streams
        if v.lower() == "liquidation":
            return "liquidation"

        # Return normalized version
        return v

    @classmethod
    def _normalize_depth_stream(cls, v: str) -> str:
        """Normalize depth stream format."""
        parts = v.split(".")
        if len(parts) >= MIN_STREAM_PARTS:
            # Normalize symbol part to uppercase
            symbol = parts[1].upper()
            return f"{parts[0]}.{symbol}"
        return v

    @classmethod
    def _normalize_dotted_stream(cls, v: str) -> str:
        """Normalize dotted stream formats."""
        parts = v.split(".")
        stream_type = parts[0].lower()

        # Normalize specific stream types
        if stream_type in {"ticker", "trade", "bookticker", "markprice", "openinterest"}:
            return cls._normalize_basic_stream(stream_type, parts)

        # Handle k-line streams with interval normalization
        if stream_type == "kline" and len(parts) >= MIN_KLINE_PARTS:
            return cls._normalize_kline_stream(parts)

        # Handle account streams
        if stream_type == "account" and len(parts) >= MIN_STREAM_PARTS:
            return cls._normalize_account_stream(parts)

        return v

    @classmethod
    def _normalize_basic_stream(cls, stream_type: str, parts: list[str]) -> str:
        """Normalize basic stream types."""
        if len(parts) >= MIN_STREAM_PARTS:
            symbol = parts[1].upper()
            return f"{stream_type}.{symbol}"
        return ".".join(parts)

    @classmethod
    def _normalize_kline_stream(cls, parts: list[str]) -> str:
        """Normalize k-line stream format."""
        interval = parts[1].lower()
        symbol = parts[2].upper()
        return f"kline.{interval}.{symbol}"

    @classmethod
    def _normalize_account_stream(cls, parts: list[str]) -> str:
        """Normalize account stream format."""
        account_type = parts[1].lower()
        if len(parts) >= MIN_KLINE_PARTS:
            symbol = parts[2].upper()
            return f"account.{account_type}.{symbol}"
        return f"account.{account_type}"

    @field_validator("stream", mode="after")
    @classmethod
    def validate_stream_format(cls, v: str, _info: ValidationInfo) -> str:
        """Validate stream name format according to Backpack API specification.

        This validator performs business logic validation after normalization.

        Backpack streams follow these patterns (from API docs):

        Public streams:
        - "depth.<symbol>" (e.g., "depth.SOL_USDC")
        - "ticker.<symbol>" (e.g., "ticker.SOL_USDC")
        - "trade.<symbol>" (e.g., "trade.SOL_USDC")
        - "kline.<interval>.<symbol>" (e.g., "kline.1m.SOL_USDC")
        - "bookTicker.<symbol>" (e.g., "bookTicker.SOL_USDC")
        - "markPrice.<symbol>" (e.g., "markPrice.SOL_USDC")
        - "openInterest.<symbol>" (e.g., "openInterest.SOL_USDC_PERP")
        - "liquidation" (global liquidation events)

        Private streams:
        - "account.orderUpdate" (all markets)
        - "account.orderUpdate.<symbol>" (single market)
        - "account.positionUpdate" (all markets)
        - "account.positionUpdate.<symbol>" (single market)
        - "account.rfqUpdate" (all markets)
        - "account.rfqUpdate.<symbol>" (single market)
        """
        if not v or not v.strip():
            msg = "Stream name cannot be empty"
            raise ValueError(msg)

        # Basic format validation - stream names contain dots, alphanumeric chars, and underscores
        if not all(c.isalnum() or c in "._-" for c in v):
            msg = f"Invalid characters in stream name: {v}"
            raise ValueError(msg)

        # Validate specific stream patterns
        valid_patterns = [
            # Public streams
            r"^(depth|ticker|trade|bookTicker|markPrice|openInterest)\.[A-Z_]+$",
            r"^kline\.[0-9]+[mhd]\.[A-Z_]+$",  # K-line with interval
            r"^liquidation$",  # Global liquidation stream
            # Private streams
            r"^account\.(orderUpdate|positionUpdate|rfqUpdate)$",  # All markets
            r"^account\.(orderUpdate|positionUpdate|rfqUpdate)\.[A-Z_]+$",  # Single market
            r"^(orders|fills)$",  # Account-specific streams without symbols
        ]

        if not any(re.match(pattern, v) for pattern in valid_patterns):
            msg = (
                f"Stream name '{v}' does not match any valid Backpack stream pattern. "
                f"Expected formats: <type>.<symbol>, kline.<interval>.<symbol>, "
                f"or account.<type>[.<symbol>]"
            )
            raise ValueError(msg)

        return v

    @field_validator("data", mode="wrap")
    @classmethod
    def validate_and_monitor_data(
        cls,
        v: dict[str, Any] | list[Any],
        handler: ValidatorFunctionWrapHandler,
        _info: ValidationInfo,
    ) -> dict[str, Any] | list[Any]:
        """Wrap validator for data preprocessing with performance tracking.

        This validator provides monitoring and performance tracking during data validation.
        """
        start_time = time.perf_counter()

        try:
            # Perform size checks before expensive validation
            cls._validate_payload_size(v)

            # Call the normal validation chain
            result: dict[str, Any] | list[Any] = handler(v)

        except Exception as e:
            # Log validation failures with context for monitoring
            duration = time.perf_counter() - start_time
            logger.warning(
                "data_validation_failed",
                component="BackpackEnvelope",
                duration_ms=duration * 1000,
                error=str(e),
                data_type=type(v).__name__,
            )
            raise
        else:
            # Log successful validation for monitoring
            duration = time.perf_counter() - start_time
            if duration > VALIDATION_THRESHOLD_MS:
                logger.debug(
                    "data_validation_success",
                    component="BackpackEnvelope",
                    duration_ms=duration * 1000,
                    size=len(v),
                )

            return result

    @classmethod
    def _validate_payload_size(cls, v: dict[str, Any] | list[Any]) -> None:
        """Validate payload size constraints."""
        if isinstance(v, dict) and len(v) > MAX_DICT_SIZE:
            msg = f"Payload dict too large: {len(v)} items"
            raise ValueError(msg)
        if isinstance(v, list) and len(v) > MAX_LIST_SIZE:
            msg = f"Payload list too large: {len(v)} items"
            raise ValueError(msg)

    @model_validator(mode="after")
    def validate_stream_data_consistency(self) -> "BackpackRawWebSocketEnvelope":
        """Validate stream type matches data structure.

        This model validator performs cross-field validation to ensure
        stream type is consistent with data structure.
        """
        stream_type = self.stream.split(".")[0]

        # Define expected data structures per stream type
        data_expectations = {
            "depth": dict,
            "ticker": dict,
            "trade": (dict, list),  # Can be either dict or list
            "bookTicker": dict,
            "markPrice": dict,
            "openInterest": dict,
            "kline": dict,
            "liquidation": (dict, list),  # Can be either dict or list
            "account": dict,  # Account streams always have dict data
        }

        expected_types = data_expectations.get(stream_type)
        if expected_types:
            # Check if data is a dict when dict is expected
            if expected_types is dict and not isinstance(self.data, dict):
                msg = f"Stream '{self.stream}' expects dict data, got {type(self.data).__name__}"
                raise TypeError(msg)
            # Check if data matches one of multiple allowed types
            if isinstance(expected_types, tuple):
                # For trade and liquidation streams that can be dict or list
                is_valid = (dict in expected_types and isinstance(self.data, dict)) or (
                    list in expected_types and isinstance(self.data, list)
                )

                if not is_valid:
                    type_names = " or ".join(t.__name__ for t in expected_types)
                    msg = (
                        f"Stream '{self.stream}' expects {type_names} data, "
                        f"got {type(self.data).__name__}"
                    )
                    raise TypeError(msg)

        return self

    def get_routing_key(self) -> str:
        """Get routing key for message routing.

        Returns:
            String routing key extracted from stream identifier.

        Raises:
            ValueError: If routing key cannot be determined from stream.
        """
        try:
            routing_key, _ = BackpackValidators.validate_backpack_topic(self.stream)
        except ValueError as e:
            msg = f"Cannot extract routing key from stream '{self.stream}': {e}"
            raise ValueError(msg) from e
        else:
            return routing_key

    def get_payload(self) -> dict[str, Any] | list[Any]:
        """Get payload data for processing.

        Returns:
            The payload data from the envelope.
        """
        return self.data

    def get_envelope_type(self) -> str:
        """Get human-readable envelope type name.

        Returns:
            String identifying the envelope type.
        """
        return "BackpackRawWebSocketEnvelope"

    @classmethod
    def model_json_schema(
        cls,
        by_alias: bool = True,
        ref_template: str = "#/$defs/{model}",
        schema_generator: type[GenerateJsonSchema] = GenerateJsonSchema,
        mode: JsonSchemaMode = "validation",
    ) -> dict[str, Any]:
        """Generate OpenAPI-compatible JSON schema for this model.

        Args:
            by_alias: Whether to use field aliases in the schema
            ref_template: Template for generating references
            schema_generator: Optional schema generator class
            mode: Schema generation mode

        Returns:
            JSON schema dictionary with Backpack-specific extensions
        """
        schema = super().model_json_schema(
            by_alias=by_alias,
            ref_template=ref_template,
            schema_generator=schema_generator,
            mode=mode,
        )

        # Add WebSocket-specific extensions
        schema.update({
            "x-ws-protocol": "websocket",
            "x-exchange": "backpack",
            "x-message-category": "envelope",
            "x-api-docs": "https://docs.backpack.exchange/api-docs#websocket-api",
            "x-validation-rules": {
                "stream_format": "Follows <type>.<symbol> or <type>.<interval>.<symbol> pattern",
                "data_constraints": "Size limited to prevent DoS attacks",
                "frozen_model": "Immutable after creation for thread safety",
            },
        })

        return schema


def validate_backpack_envelope(
    message: dict[str, Any],
) -> BackpackRawWebSocketEnvelope | BackpackSubscriptionResponse:
    """Validate and parse a Backpack WebSocket message envelope.

    Args:
        message: Raw WebSocket message dictionary

    Returns:
        Validated envelope model instance (either stream message or subscription response)

    Raises:
        ValidationError: If the message doesn't match the expected envelope format
        ValueError: If the message format is unrecognized
    """
    # Check if it's a subscription response
    if "result" in message and "stream" not in message:
        try:
            return BackpackSubscriptionResponse.model_validate(message)
        except ValidationError:
            pass

    # Try to parse as regular stream message
    try:
        return BackpackRawWebSocketEnvelope.model_validate(message)
    except ValidationError:
        msg = (
            f"Invalid Backpack WebSocket message format. "
            f'Expected: {{"stream": "...", "data": "..."}} or {{"result": bool, "id": int}}. '
            f"Received keys: {list(message.keys())}"
        )
        raise ValueError(msg) from None
