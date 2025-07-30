"""Hyperliquid Position Service.

This service handles all position-related operations for the Hyperliquid exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Derivative position retrieval
- Position filtering by symbol
- Position transformation and mapping
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import NoReturn

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import PositionMapperProtocol
from cyberdelta.apis.hyperliquid.services.account.hl_clearinghouse_state_service import (
    HyperliquidClearinghouseStateService,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import DerivativePosition
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.exceptions.service_validation import EmptyStringParameterError
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidPositionService:
    """Focused service for Hyperliquid position operations.

    Handles validation, processing, and transformation of position requests
    with comprehensive error handling and filtering capabilities.
    """

    def __init__(
        self,
        clearinghouse_service: HyperliquidClearinghouseStateService,
        mapper: PositionMapperProtocol,
        exchange_name: str = "hyperliquid",
    ) -> None:
        """Initialize the position service.

        Args:
            clearinghouse_service: Service for fetching clearinghouse state
            mapper: Position mapper for transforming raw responses to internal models
            exchange_name: Name identifier for this exchange instance
        """
        self._clearinghouse_service = clearinghouse_service
        self._mapper = mapper
        self._exchange_name = exchange_name

    async def get_positions(self, symbol: Symbol | None = None) -> list[DerivativePosition]:
        """Retrieve derivative positions, optionally filtered by symbol.

        Args:
            symbol: Optional symbol to filter positions by

        Returns:
            list[DerivativePosition]: List of derivative positions

        Raises:
            APIError: If position retrieval fails or processing fails
            EmptyStringParameterError: If symbol is an empty string
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_positions"

        if symbol is not None and not symbol:
            raise EmptyStringParameterError(
                parameter_name="symbol",
                method_name=current_method,
            )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.info(
                "retrieving_positions",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                message="Retrieving derivative positions from clearinghouse state",
            )

            raw_clearinghouse_state = await self._clearinghouse_service.get_clearinghouse_state()
            all_positions_dict = (
                self._mapper.transform_raw_clearinghouse_state_to_derivative_positions(
                    raw_clearinghouse_state,
                )
            )

            positions = self._filter_positions_by_symbol(all_positions_dict, symbol)

            logger.info(
                "positions_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                position_count=len(positions),
                message="Successfully retrieved derivative positions",
            )

        except APIError:
            # Re-raise APIErrors from _get_raw_clearinghouse_state, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            self._handle_positions_transformation_error(
                e_transform,
                current_method,
                status_code,
                raw_response_content,
            )
        except ValidationError as e_val:
            self._handle_positions_validation_error(
                e_val,
                current_method,
                status_code,
                raw_response_content,
            )
        except (ValueError, TypeError) as e_service_logic:
            self._handle_positions_service_logic_error(e_service_logic, current_method)
        except (AttributeError, KeyError, IndexError) as e_unexpected:
            self._handle_positions_unexpected_error(
                e_unexpected,
                current_method,
                status_code,
                raw_response_content,
            )
        else:
            return positions

    def _filter_positions_by_symbol(
        self,
        all_positions_dict: dict[str, DerivativePosition],
        symbol: Symbol | None,
    ) -> list[DerivativePosition]:
        """Filter positions by symbol or return all positions.

        Args:
            all_positions_dict: Dictionary of all positions keyed by symbol
            symbol: Optional symbol to filter by

        Returns:
            list[DerivativePosition]: Filtered positions
        """
        if symbol:
            position = all_positions_dict.get(symbol.value)
            if position:
                logger.debug(
                    "filtered_position_found",
                    action="get_positions",
                    exchange=self._exchange_name,
                    symbol=symbol,
                    position=position,
                    message=f"Filtered position for symbol '{symbol}': {position}",
                )
                return [position]
            logger.debug(
                "no_position_found",
                action="get_positions",
                exchange=self._exchange_name,
                symbol=symbol,
                available_positions=list(all_positions_dict.keys()),
                message=f"No position found for symbol '{symbol}'. "
                f"Available positions: {list(all_positions_dict.keys())}",
            )
            return []

        all_positions_list = list(all_positions_dict.values())
        logger.debug(
            "mapped_all_positions",
            action="get_positions",
            exchange=self._exchange_name,
            positions_count=len(all_positions_list),
            message=f"Mapped all internal positions: {all_positions_list}",
        )
        return all_positions_list

    def _handle_positions_transformation_error(
        self,
        e_transform: TransformationError,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> NoReturn:
        """Handle transformation errors for positions.

        Args:
            e_transform: The transformation error
            current_method: Name of the method where error occurred
            status_code: HTTP status code
            raw_response_content: Raw response content

        Raises:
            APIError: Standardized API error
        """
        logger.error(
            "transformation_error",
            action=current_method,
            exchange=self._exchange_name,
            error=str(e_transform),
            message="Failed to transform exchange data",
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=e_transform,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e_transform

    def _handle_positions_validation_error(
        self,
        e_val: ValidationError,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> NoReturn:
        """Handle validation errors for positions.

        Args:
            e_val: The validation error
            current_method: Name of the method where error occurred
            status_code: HTTP status code
            raw_response_content: Raw response content

        Raises:
            APIError: Standardized API error
        """
        logger.error(
            "validation_error",
            action=current_method,
            exchange=self._exchange_name,
            error=str(e_val),
            message="Internal data validation failed",
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=e_val,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e_val

    def _handle_positions_service_logic_error(
        self,
        e_service_logic: ValueError | TypeError,
        current_method: str,
    ) -> NoReturn:
        """Handle service logic errors for positions.

        Args:
            e_service_logic: The service logic error
            current_method: Name of the method where error occurred

        Raises:
            APIError: Always raised to wrap the service logic error (or re-raises the
                original exception for input validation errors)
        """
        # Distinguish input validation from internal errors per ERROR_HANDLING.md
        error_msg = str(e_service_logic)
        if current_method in error_msg and "symbol" in error_msg:
            # Re-raise input validation errors
            raise e_service_logic
        # Wrap internal errors as APIError
        logger.error(
            "service_logic_error",
            action=current_method,
            exchange=self._exchange_name,
            error=str(e_service_logic),
            message="Service internal logic error",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=e_service_logic,
        ) from e_service_logic

    def _handle_positions_unexpected_error(
        self,
        e_unexpected: AttributeError | KeyError | IndexError,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> NoReturn:
        """Handle unexpected errors for positions.

        Args:
            e_unexpected: The unexpected error
            current_method: Name of the method where error occurred
            status_code: HTTP status code
            raw_response_content: Raw response content

        Raises:
            APIError: Standardized API error
        """
        logger.error(
            "unexpected_service_failure",
            action=current_method,
            exchange=self._exchange_name,
            error=str(e_unexpected),
            message="Unexpected service failure",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e_unexpected,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e_unexpected
