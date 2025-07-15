"""CyberDeltaEngine: APIs base package.

This package contains base classes and interfaces for exchange API implementations.
"""

from __future__ import annotations

from .authenticator_interface import AuthenticatedRequestComponents, IAuthenticator
from .exchange_api import ExchangeAPI
from .payload_serialization_strategy import (
    DefaultSerializationStrategy,
    PayloadSerializationStrategy,
)
from .rate_limit_models import RateLimitRequestContext
from .rate_limit_strategy_interface import RateLimitStrategy
from .simple_rate_limit_strategy import SimpleTokenBucketStrategy

# WebSocket-related imports
from .ws_discriminated_unions import (
    DiscriminatedBackpackEnvelope,
    DiscriminatedHyperliquidEnvelope,
    DiscriminatedHyperliquidUserEvent,
    WebSocketEnvelopeUnion,
    detect_and_add_discriminator,
    validate_backpack_fast,
    validate_envelope_ultra_fast,
    validate_hyperliquid_fast,
    validate_hyperliquid_user_event_fast,
)
from .ws_error_handler import BaseErrorHandler
from .ws_metrics import MetricType, WebSocketMetricsCollector
from .ws_processor import PydanticWebSocketProcessor
from .ws_router import BaseWebSocketRouter, MessageHandler, MessageProcessor
from .ws_transformer import MapperTransformer, extract_symbol_from_context
from .ws_type_adapters import (
    SerializableModel,
    StreamingValidationAdapter,
    ValidationBenchmark,
    WebSocketTypeAdapters,
)
from .ws_validators import ExchangeSpecificValidators, WebSocketPayloadValidators


# Note: IErrorMapper is now in cyberdelta.apis.common to avoid circular imports

__all__ = [
    "AuthenticatedRequestComponents",
    # WebSocket exports
    "BaseErrorHandler",
    "BaseWebSocketRouter",
    "DefaultSerializationStrategy",
    "DiscriminatedBackpackEnvelope",
    "DiscriminatedHyperliquidEnvelope",
    "DiscriminatedHyperliquidUserEvent",
    "ExchangeAPI",
    "ExchangeSpecificValidators",
    "IAuthenticator",
    "MapperTransformer",
    "MessageHandler",
    "MessageProcessor",
    "MetricType",
    "PayloadSerializationStrategy",
    "PydanticWebSocketProcessor",
    "RateLimitRequestContext",
    "RateLimitStrategy",
    "SerializableModel",
    "SimpleTokenBucketStrategy",
    "StreamingValidationAdapter",
    "ValidationBenchmark",
    "WebSocketEnvelopeUnion",
    "WebSocketMetricsCollector",
    "WebSocketPayloadValidators",
    "WebSocketTypeAdapters",
    "detect_and_add_discriminator",
    "extract_symbol_from_context",
    "validate_backpack_fast",
    "validate_envelope_ultra_fast",
    "validate_hyperliquid_fast",
    "validate_hyperliquid_user_event_fast",
]
