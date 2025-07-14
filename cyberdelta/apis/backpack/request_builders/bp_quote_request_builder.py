"""Backpack Quote Request Builder.

This module handles the construction of request payloads for quote and RFQ operations,
extracted from the monolithic request builder to improve maintainability and testability.

Focused on:
- Request for Quote (RFQ) creation
- Quote submission and management
- Quote acceptance operations
- RFQ cancellation and refresh
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawQuoteAcceptRequest,
    BackpackRawQuoteSubmitRequest,
    BackpackRawRequestForQuoteCancelRequest,
    BackpackRawRequestForQuoteRefreshRequest,
    BackpackRawRequestForQuoteRequest,
)
from cyberdelta.apis.exceptions import (
    MissingRequiredParameterError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import OrderSide


logger = get_logger(__name__)


class BackpackQuoteRequestBuilder:
    """Focused request builder for Backpack quote and RFQ operations.

    This class contains methods for constructing validated request payloads
    for all quote-related API endpoints.
    """

    def __init__(self) -> None:
        """Initialize the quote request builder."""
        logger.debug("Initializing Backpack quote request builder")

    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Generic request builder dispatch method.

        This method serves as the entry point for the registry system
        and dispatches to the appropriate specific builder method based on context.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments including 'operation' to specify the request type

        Returns:
            Request payload dictionary

        Raises:
            NotImplementedError: If operation is not supported or parameters are insufficient
        """
        operation = kwargs.get("operation")
        if not operation:
            raise NotImplementedError(
                "Quote request builder requires 'operation' parameter for generic build_request"
            )

        # Note: Quote request builders require specific parameters for each operation
        # which are not available in the generic build_request interface.
        # This dispatcher is implemented for protocol consistency but most operations
        # will require direct method calls with proper parameters.

        if operation in {
            "request_for_quote",
            "submit_quote",
            "accept_quote",
            "cancel_rfq",
            "refresh_rfq",
        }:
            # Quote operations require specific parameters not available in generic interface
            raise NotImplementedError(
                f"Quote operation '{operation}' requires specific parameters not available "
                f"in generic build_request interface. Use specific builder methods directly."
            )
        raise NotImplementedError(
            f"Quote operation '{operation}' not supported by registry dispatch"
        )

    @staticmethod
    def format_symbol(symbol: str) -> str:
        """Ensure symbol is in the format X_Y (e.g., SOL_USDC).

        Returns:
            Symbol formatted with underscores and uppercase.
        """
        return symbol.replace("-", "_").upper()

    @staticmethod
    def build_request_for_quote_payload(
        symbol: str,
        quantity: Decimal | None = None,
        quote_quantity: Decimal | None = None,
        auto_accept_threshold: Decimal | None = None,
        submission_time_ms: int | None = None,
        expiry_time_ms: int | None = None,
        client_id: str | None = None,
    ) -> BackpackRawRequestForQuoteRequest:
        """Build the payload for submitting a Request For Quote (RFQ).

        Args:
            symbol: The trading symbol
            quantity: Base quantity for the RFQ
            quote_quantity: Quote quantity for the RFQ
            auto_accept_threshold: Auto-accept threshold
            submission_time_ms: Submission time in milliseconds
            expiry_time_ms: Expiry time in milliseconds
            client_id: Optional client-provided ID

        Returns:
            BackpackRawRequestForQuoteRequest: The validated request payload model

        Raises:
            MissingRequiredParameterError: If neither quantity nor quote_quantity is provided

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.
        """
        if quantity is None and quote_quantity is None:
            raise MissingRequiredParameterError(
                parameter_name="quantity or quote_quantity",
                operation="Request for Quote submission",
            )

        logger.debug(
            "building_request_for_quote_payload",
            symbol=symbol,
            quantity=str(quantity) if quantity else None,
            quote_quantity=str(quote_quantity) if quote_quantity else None,
            auto_accept_threshold=str(auto_accept_threshold) if auto_accept_threshold else None,
        )

        request_data: dict[str, Any] = {
            "symbol": BackpackQuoteRequestBuilder.format_symbol(symbol),
        }

        if quantity is not None:
            request_data["quantity"] = str(quantity)
        if quote_quantity is not None:
            request_data["quoteQuantity"] = str(quote_quantity)
        if auto_accept_threshold is not None:
            request_data["autoAcceptThreshold"] = str(auto_accept_threshold)
        if submission_time_ms is not None:
            request_data["submissionTimeMs"] = submission_time_ms
        if expiry_time_ms is not None:
            request_data["expiryTimeMs"] = expiry_time_ms
        if client_id is not None:
            request_data["clientId"] = client_id

        return BackpackRawRequestForQuoteRequest(**request_data)

    @staticmethod
    def build_submit_quote_payload(
        rfq_id: str,
        side: OrderSide,
        price: Decimal,
        client_quote_id: str | None = None,
    ) -> BackpackRawQuoteSubmitRequest:
        """Build the payload for submitting a quote in response to an RFQ.

        Args:
            rfq_id: The RFQ ID to respond to
            side: The side of the quote (BUY or SELL)
            price: The quoted price
            client_quote_id: Optional client-provided quote ID

        Returns:
            BackpackRawQuoteSubmitRequest: The validated request payload model

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.
        """
        logger.debug(
            "building_submit_quote_payload",
            rfq_id=rfq_id,
            side=side.value,
            price=str(price),
            client_quote_id=client_quote_id,
        )

        # Map OrderSide to API string
        api_side = "Bid" if side == OrderSide.BUY else "Ask"

        request_data: dict[str, Any] = {
            "rfqId": rfq_id,
            "side": api_side,
            "price": str(price),
        }

        if client_quote_id is not None:
            request_data["clientQuoteId"] = client_quote_id

        return BackpackRawQuoteSubmitRequest(**request_data)

    @staticmethod
    def build_accept_quote_payload(
        rfq_id: str,
        quote_id: str,
    ) -> BackpackRawQuoteAcceptRequest:
        """Build the payload for accepting a quote.

        Args:
            rfq_id: The RFQ ID
            quote_id: The quote ID to accept

        Returns:
            BackpackRawQuoteAcceptRequest: The validated request payload model
        """
        logger.debug(
            "building_accept_quote_payload",
            rfq_id=rfq_id,
            quote_id=quote_id,
        )

        request_data: dict[str, Any] = {
            "rfqId": rfq_id,
            "quoteId": quote_id,
        }
        return BackpackRawQuoteAcceptRequest(**request_data)

    @staticmethod
    def build_cancel_rfq_payload(rfq_id: str) -> BackpackRawRequestForQuoteCancelRequest:
        """Build the payload for cancelling an RFQ.

        Args:
            rfq_id: The RFQ ID to cancel

        Returns:
            BackpackRawRequestForQuoteCancelRequest: The validated request payload model
        """
        logger.debug(
            "building_cancel_rfq_payload",
            rfq_id=rfq_id,
        )

        request_data: dict[str, Any] = {
            "rfqId": rfq_id,
        }
        return BackpackRawRequestForQuoteCancelRequest(**request_data)

    @staticmethod
    def build_refresh_rfq_payload(
        rfq_id: str,
        submission_time_ms: int | None = None,
        expiry_time_ms: int | None = None,
    ) -> BackpackRawRequestForQuoteRefreshRequest:
        """Build the payload for refreshing an RFQ.

        Args:
            rfq_id: The RFQ ID to refresh
            submission_time_ms: New submission time in milliseconds
            expiry_time_ms: New expiry time in milliseconds

        Returns:
            BackpackRawRequestForQuoteRefreshRequest: The validated request payload model
        """
        logger.debug(
            "building_refresh_rfq_payload",
            rfq_id=rfq_id,
            submission_time_ms=submission_time_ms,
            expiry_time_ms=expiry_time_ms,
        )

        request_data: dict[str, Any] = {
            "rfqId": rfq_id,
        }

        if submission_time_ms is not None:
            request_data["submissionTimeMs"] = submission_time_ms
        if expiry_time_ms is not None:
            request_data["expiryTimeMs"] = expiry_time_ms

        return BackpackRawRequestForQuoteRefreshRequest(**request_data)
