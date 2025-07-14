"""Hyperliquid Account Summary Service.

This service handles all account summary operations for the Hyperliquid exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Margin account summary retrieval
- Account information transformation
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import AccountSummaryMapperProtocol
from cyberdelta.apis.hyperliquid.services.account.hl_clearinghouse_state_service import (
    HyperliquidClearinghouseStateService,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import MarginAccountSummary
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidAccountSummaryService:
    """Focused service for Hyperliquid account summary operations.

    Handles validation, processing, and transformation of account summary requests
    with comprehensive error handling and clearinghouse state management.
    """

    def __init__(
        self,
        clearinghouse_service: HyperliquidClearinghouseStateService,
        mapper: AccountSummaryMapperProtocol,
        exchange_name: str = "hyperliquid",
    ) -> None:
        """Initialize the account summary service.

        Args:
            clearinghouse_service: Service for fetching clearinghouse state
            mapper: Data mapper for transforming raw responses to internal models
            exchange_name: Name identifier for this exchange instance
        """
        self._clearinghouse_service = clearinghouse_service
        self._mapper = mapper
        self._exchange_name = exchange_name

    async def get_account_summary(self) -> MarginAccountSummary:
        """Retrieve general account information or summary from the clearinghouse state.

        Returns:
            MarginAccountSummary: Account summary with margin information

        Raises:
            APIError: If account summary retrieval fails or processing fails
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_account_summary"

        # No input parameters to validate for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.info(
                "retrieving_account_summary",
                exchange=self._exchange_name,
                method=current_method,
                message="Retrieving account summary from clearinghouse state",
            )

            raw_clearinghouse_state = await self._clearinghouse_service.get_clearinghouse_state()
            internal_summary = self._mapper.transform_raw_clearinghouse_state_to_margin_summary(
                raw_clearinghouse_state,
            )

            logger.info(
                "account_summary_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                total_equity=str(internal_summary.total_equity),
                total_maintenance_margin=str(internal_summary.total_maintenance_margin_required)
                if internal_summary.total_maintenance_margin_required
                else "None",
                message="Successfully retrieved account summary",
            )

            logger.debug(
                "mapped_account_summary",
                action="get_account_summary",
                exchange=self._exchange_name,
                summary=internal_summary,
                message=f"Mapped internal account summary: {internal_summary}",
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
            # No input parameters to validate in get_account_summary, so wrap as internal error
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
            return internal_summary
