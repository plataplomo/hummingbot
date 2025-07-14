"""Hyperliquid Balance Service.

This service handles all balance-related operations for the Hyperliquid exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Spot balance retrieval
- Clearinghouse state fetching
- Balance transformation and mapping
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import BalanceMapperProtocol
from cyberdelta.apis.hyperliquid.services.account.hl_clearinghouse_state_service import (
    HyperliquidClearinghouseStateService,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import SpotBalance
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidBalanceService:
    """Focused service for Hyperliquid balance operations.

    Handles validation, processing, and transformation of balance requests
    with comprehensive error handling and clearinghouse state management.
    """

    def __init__(
        self,
        clearinghouse_service: HyperliquidClearinghouseStateService,
        mapper: BalanceMapperProtocol,
        exchange_name: str = "hyperliquid",
    ) -> None:
        """Initialize the balance service.

        Args:
            clearinghouse_service: Service for fetching clearinghouse state
            mapper: Balance mapper for transforming raw responses to internal models
            exchange_name: Name identifier for this exchange instance
        """
        self._clearinghouse_service = clearinghouse_service
        self._mapper = mapper
        self._exchange_name = exchange_name

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Retrieve all account balances (spot balances derived from user state).

        Returns:
            dict[str, SpotBalance]: Dictionary mapping symbols to spot balances

        Raises:
            APIError: If balance retrieval fails or processing fails
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_balances"

        # No input parameters to validate for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.info(
                "retrieving_balances",
                exchange=self._exchange_name,
                method=current_method,
                message="Retrieving account balances from clearinghouse state",
            )

            raw_clearinghouse_state = await self._clearinghouse_service.get_clearinghouse_state()
            internal_balances = self._mapper.transform_raw_clearinghouse_state_to_spot_balances(
                raw_clearinghouse_state,
            )

            logger.info(
                "balances_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                balance_count=len(internal_balances),
                message="Successfully retrieved account balances",
            )

            logger.debug(
                "mapped_internal_balances",
                action="map_balances",
                exchange=self._exchange_name,
                balances=internal_balances,
                message=f"Mapped internal balances: {internal_balances}",
            )
        except APIError:
            # Re-raise APIErrors from _get_raw_clearinghouse_state, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
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
        except ValidationError as e_val:
            logger.exception(
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
        except (ValueError, TypeError) as e_service_logic:
            # Distinguish input validation from internal errors per ERROR_HANDLING.md
            # No input parameters to validate in get_balances, so wrap as internal error
            logger.exception(
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
        except Exception as e_unexpected:
            logger.exception(
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
        else:
            return internal_balances
