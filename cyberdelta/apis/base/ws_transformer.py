"""Generic WebSocket Message Transformer.

This module provides a generic transformer implementation that eliminates
the need for exchange-specific transformer classes, reducing code duplication
from 12+ classes to 1 generic implementation.

Based on the analysis in ws_base_class_refactor.md, this consolidates ~80 lines
of duplicated transformer code per exchange into a single, reusable pattern.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from cyberdelta.apis.base.ws_context import (
    BackpackMessageContext,
    HyperliquidMessageContext,
    WebSocketContextUnion,
)


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class SymbolNotFoundError(KeyError):
    """Raised when symbol is not found in context for Backpack transformation."""

    def __init__(self) -> None:
        """Initialize symbol not found error."""
        super().__init__("Symbol not found in context for Backpack transformation")


class CoinNotFoundError(KeyError):
    """Raised when coin is not found in context for Hyperliquid transformation."""

    def __init__(self) -> None:
        """Initialize coin not found error."""
        super().__init__("Coin not found in context for Hyperliquid transformation")


class ControlMessageTransformer[T: BaseModel]:
    """Transformer for control/system messages that don't map to domain models.

    This transformer is used for messages like subscription responses, heartbeats,
    or other control messages that should be passed through to handlers without
    transformation to domain models.

    The raw model is made available in the context for handlers to process.
    """

    def transform(
        self,
        validated: T,
        context: WebSocketContextUnion | None = None,
    ) -> None:
        """Pass through control message without transformation.

        Args:
            validated: Validated control message model
            context: Optional WebSocket message context

        Returns:
            None - control messages don't produce domain models
        """
        # Control messages are accessed via context.raw_model
        # No transformation needed - validated model is already in context


class MapperTransformer[T: BaseModel, U: BaseModel]:
    """Transformer for single domain model results.

    This transformer wraps mapper methods that return a single domain model or None.
    Use BatchMapperTransformer for methods that return lists of models.

    Benefits:
    - Type-safe: Clearly indicates single model output
    - Eliminates transformer class duplication
    - Enables consistent error handling across all transformers
    - Simplifies testing with single transformer test suite
    """

    def __init__(
        self,
        mapper_method: Callable[..., U | None],
        context_extractor: Callable[[WebSocketContextUnion], dict[str, Any]] | None = None,
    ) -> None:
        """Initialize transformer with single-result mapper method.

        Args:
            mapper_method: The mapper method to call (returns single model or None)
            context_extractor: Optional function to extract additional parameters from typed context
        """
        self.mapper_method = mapper_method
        self.context_extractor = context_extractor

    def transform(self, validated: T, context: WebSocketContextUnion | None = None) -> U | None:
        """Transform using mapper method with optional context extraction.

        Args:
            validated: Validated raw model to transform
            context: Optional typed context containing additional data

        Returns:
            Single transformed domain model or None
        """
        if self.context_extractor and context:
            # Extract additional parameters from context (e.g., symbol, coin)
            extra_params = self.context_extractor(context)
            return self.mapper_method(validated, **extra_params)

        # Simple transformation without context
        return self.mapper_method(validated)


class BatchMapperTransformer[T: BaseModel, U: BaseModel]:
    """Transformer for batch domain model results.

    This transformer wraps mapper methods that return lists of domain models.
    Use MapperTransformer for methods that return single models.
    """

    def __init__(
        self,
        mapper_method: Callable[..., list[U]],
        context_extractor: Callable[[WebSocketContextUnion], dict[str, Any]] | None = None,
    ) -> None:
        """Initialize transformer with batch-result mapper method.

        Args:
            mapper_method: The mapper method to call (returns list of models)
            context_extractor: Optional function to extract additional parameters from typed context
        """
        self.mapper_method = mapper_method
        self.context_extractor = context_extractor

    def transform(self, validated: T, context: WebSocketContextUnion | None = None) -> list[U]:
        """Transform using mapper method with optional context extraction.

        Args:
            validated: Validated raw model to transform
            context: Optional typed context containing additional data

        Returns:
            List of transformed domain models
        """
        if self.context_extractor and context:
            # Extract additional parameters from context (e.g., symbol, coin)
            extra_params = self.context_extractor(context)
            return self.mapper_method(validated, **extra_params)

        # Simple transformation without context
        return self.mapper_method(validated)


class AsyncMapperTransformer[T: BaseModel, U]:
    """Async version of MapperTransformer for async mapper methods.

    This handles cases where the mapper method is async and returns an awaitable.
    """

    def __init__(
        self,
        async_mapper_method: Callable[..., Awaitable[U]],
        context_extractor: Callable[[WebSocketContextUnion], dict[str, Any]] | None = None,
    ) -> None:
        """Initialize async transformer with async mapper method.

        Args:
            async_mapper_method: The async mapper method to call for transformation
            context_extractor: Optional function to extract additional parameters from typed context
        """
        self.async_mapper_method = async_mapper_method
        self.context_extractor = context_extractor

    async def transform(self, validated: T, context: WebSocketContextUnion | None = None) -> U:
        """Transform using async mapper method with optional context extraction.

        Args:
            validated: Validated raw model to transform
            context: Optional typed context containing additional data

        Returns:
            Transformed domain model
        """
        if self.context_extractor and context:
            # Extract additional parameters from context (e.g., symbol, coin)
            extra_params = self.context_extractor(context)
            return await self.async_mapper_method(validated, **extra_params)

        # Simple async transformation without context
        return await self.async_mapper_method(validated)


# Context extractor utility functions
def extract_symbol_from_context(context: WebSocketContextUnion) -> dict[str, str]:
    """Extract symbol parameter from context for Backpack transformers.

    Args:
        context: Typed context containing symbol information

    Returns:
        Dictionary with symbol parameter for mapper method

    Raises:
        KeyError: If symbol is not found in context
    """
    # Handle typed context
    if isinstance(context, BackpackMessageContext):
        if context.symbol:
            return {"symbol": context.symbol}
        raise SymbolNotFoundError

    # For other context types, symbol might not be available
    raise SymbolNotFoundError


def extract_coin_from_context(context: WebSocketContextUnion) -> dict[str, str]:
    """Extract coin parameter from context for Hyperliquid transformers.

    Args:
        context: Typed context containing coin information

    Returns:
        Dictionary with coin parameter for mapper method

    Raises:
        KeyError: If coin is not found in context
    """
    # Handle typed context
    if isinstance(context, HyperliquidMessageContext):
        # Hyperliquid uses symbol field for coin
        if context.symbol:
            return {"coin": context.symbol}
        # Try computed field - mypy doesn't understand computed_field well
        # so we need to help it with type annotations
        coin_value: str | None = context.coin  # type: ignore[assignment]
        if coin_value:
            return {"coin": coin_value}
        raise CoinNotFoundError

    # For other context types, coin might not be available
    raise CoinNotFoundError


def no_context_extraction(context: dict[str, Any]) -> dict[str, Any]:
    """No-op context extractor for transformers that don't need context.

    Args:
        context: Processing context (ignored)

    Returns:
        Empty dictionary
    """
    return {}
