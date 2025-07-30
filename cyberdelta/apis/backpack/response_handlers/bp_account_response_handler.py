"""Backpack Account Response Handler.

This module handles validation of account responses from the Backpack API,
extracted from the monolithic response handler to improve maintainability and testability.

Focused on:
- Balance responses
- Position responses
- Account summary responses
- Collateral information
- Transfer and withdrawal responses
- Account limits (borrow, order, withdrawal)
"""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummaryResponse
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.models.bp_raw_limits import (
    BackpackRawMaxBorrowQuantity,
    BackpackRawMaxOrderQuantity,
    BackpackRawMaxWithdrawalQuantity,
)
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionResponse
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import (
    BackpackRawWithdrawalResponse,
)
from cyberdelta.apis.backpack.protocols.handler_protocols import AccountResponseHandlerProtocol
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# Type alias for raw JSON response from HTTP client
# Aligned with ParsedJsonResponse from http_client.py
type RawJsonResponse = ParsedJsonResponse


class BackpackAccountResponseHandler(AccountResponseHandlerProtocol):
    """Handles validation of account responses from Backpack REST API endpoints.

    Uses Pydantic models defined in `cyberdelta.apis.backpack.models` to validate
    the structure and types of the raw data. Raises APIError if validation fails.
    """

    @staticmethod
    def _handle_validation_error(
        e: ValidationError,
        context: str,
        raw_data: RawJsonResponse,
    ) -> APIError:
        """Create a standardized APIError from a ValidationError.

        Returns:
            APIError with INVALID_RESPONSE code and validation details.
        """
        logger.error(
            "backpack_pydantic_validation_failed",
            context=context,
            validation_error=str(e),
            raw_data=raw_data,
            handler_class="BackpackAccountResponseHandler",
            message="Pydantic validation failed",
        )
        # Use INVALID_RESPONSE code as per architecture rules
        return APIError(
            message=f"Invalid {context} response from exchange: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=e,
        )

    def handle_response(
        self,
        response: ParsedJsonResponse,
        status_code: int,
        headers: dict[str, str],
        context: str,
    ) -> object:
        """Generic response handler dispatch method.

        This method serves as the entry point for the registry system
        and dispatches to the appropriate specific handler method based on context.

        Args:
            response: Parsed JSON response data
            status_code: HTTP status code
            headers: Response headers
            context: Context string indicating the operation (e.g., "account.get_balances")

        Raises:
            NotImplementedError: If context is not supported
        """
        # Extract operation from context (format: "domain.operation")
        if "." in context:
            _, operation = context.split(".", 1)
        else:
            operation = context

        # Note: Account handlers require additional parameters (symbol, subaccount_id, etc.)
        # which are not available in the generic handle_response interface.
        # This dispatcher is implemented for protocol consistency but most operations
        # will require direct method calls with proper parameters.

        if operation in {
            "get_balances",
            "get_positions",
            "get_account_info",
            "withdraw",
            "transfer",
            "get_collateral",
            "max_borrow_quantity",
            "max_order_quantity",
            "max_withdrawal_quantity",
        }:
            # Account operations require additional context not available in generic interface
            raise NotImplementedError(
                f"Account operation '{operation}' requires specific parameters not available "
                f"in generic handle_response interface. Use specific handler methods directly.",
            )
        raise NotImplementedError(
            f"Account operation '{operation}' not supported by registry dispatch",
        )

    @staticmethod
    def handle_get_balances_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> dict[str, BackpackRawBalanceResponse]:
        """Validate the raw response for the Get Balances endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code

        Returns:
            Dictionary mapping asset symbols to BackpackRawBalanceResponse models.
        """
        context = "balances"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_balances: dict[str, BackpackRawBalanceResponse] = {}
        # validated_data is known to be a dict here
        for asset_symbol, balance_details in validated_data.items():
            # Ensure balance_details is dict before validating
            validated_balance = ensure_dict_response(
                balance_details,
                f"{context} for asset '{asset_symbol}'",
                status_code,
            )
            try:
                # Ensure asset_symbol is string for the key
                validated_balances[str(asset_symbol)] = BackpackRawBalanceResponse.model_validate(
                    validated_balance,
                )
            except ValidationError as e:
                api_error = BackpackAccountResponseHandler._handle_validation_error(
                    e,
                    f"balance details for {asset_symbol}",
                    validated_balance,
                )
                raise api_error from e
        return validated_balances

    @staticmethod
    def handle_get_positions_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol | None,
        status_code: int,
    ) -> list[BackpackRawPositionResponse]:
        """Validate the raw response for the Get Positions endpoint.

        Handles a single position dictionary if a symbol is provided,
        or a list of position dictionaries if no symbol is provided.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Optional symbol to filter positions
            status_code: HTTP status code

        Returns:
            List of validated BackpackRawPositionResponse models.

        Raises:
            APIError: If symbol not found or validation of position data fails.
            _handle_validation_error: Re-raised as APIError if validation fails.
        """
        context = f"positions ({symbol or 'all'})"
        validated_positions: list[BackpackRawPositionResponse] = []

        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                position = BackpackRawPositionResponse.model_validate(validated_item)
                if symbol is None or position.symbol == symbol.value:
                    validated_positions.append(position)
            except ValidationError as e:
                raise BackpackAccountResponseHandler._handle_validation_error(
                    e,
                    f"single position item in {context}",
                    validated_item,
                ) from e

        if symbol is not None and not validated_positions:
            raise APIError(
                message=f"No position found for symbol '{symbol}' in {context} response.",
                code=APIErrorCode.SYMBOL_NOT_FOUND.value,
            )
        return validated_positions

    @staticmethod
    def handle_get_account_info_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> BackpackRawAccountSummaryResponse:
        """Validate the raw response for the Get Account Info endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code

        Returns:
            Validated BackpackRawAccountSummaryResponse model.

        Raises:
            _handle_validation_error: Re-raised as APIError if validation fails.
        """
        context = "account info"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawAccountSummaryResponse.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackAccountResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_withdraw_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> BackpackRawWithdrawalResponse:
        """Validate the raw response for the Withdraw endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code

        Returns:
            Validated BackpackRawWithdrawalResponse model.

        Raises:
            _handle_validation_error: Re-raised as APIError if validation fails.
        """
        context = "withdraw response"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawWithdrawalResponse.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackAccountResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_transfer_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> RawJsonResponse:  # Returns the validated raw dict
        """Validate the raw response for an internal capital transfer.

        Expects a dict with 'success' (bool), optional 'message' (str), and
        optional 'transferId' (str).

        Returns:
            Validated raw response dictionary with transfer details.

        Raises:
            APIError: If 'success' field is missing or not a boolean.
        """
        context = "internal transfer response"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )

        # Basic structure validation
        if "success" not in validated_data or not isinstance(
            validated_data["success"],
            bool,
        ):
            raise APIError(
                message=(
                    f"Invalid {context}: 'success' field missing or not a boolean. "
                    f"Got: {validated_data.get('success')}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        if "message" in validated_data and not isinstance(
            validated_data["message"],
            str,
        ):
            logger.warning(
                "backpack_transfer_message_not_string",
                context=context,
                message_field=validated_data["message"],
                module_name=__name__,
                message="Transfer response 'message' field is not a string",
            )
            # Don't raise, but log. Message is optional and for info.

        if "transferId" in validated_data and not isinstance(
            validated_data["transferId"],
            str,
        ):
            logger.warning(
                "backpack_transfer_id_not_string",
                context=context,
                transfer_id_field=validated_data["transferId"],
                module_name=__name__,
                message="Transfer response 'transferId' field is not a string",
            )
            # Don't raise, but log. TransferId is optional and for info.

        # If successful, the mapper will use this dict to create an internal Transfer model.
        # If not successful (success=False), the mapper should handle this appropriately.
        return validated_data

    @staticmethod
    def handle_get_collateral_response(
        raw_response_content: RawJsonResponse,
        subaccount_id: int | None,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawCollateralResponse:
        """Validate the raw response for the Get Collateral endpoint.

        (/api/v1/capital/collateral).

        Args:
            raw_response_content: Raw JSON response from the API
            subaccount_id: Optional subaccount ID for context
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated BackpackRawCollateralResponse model.

        Raises:
            _handle_validation_error: Re-raised as APIError if validation fails.
        """
        context = f"collateral data (subaccount_id={subaccount_id}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawCollateralResponse.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackAccountResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    # --- Account Limits Response Handlers (INTERNAL USE ONLY) ---

    @staticmethod
    def handle_max_borrow_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxBorrowQuantity:
        """Validate the raw response for the Max Borrow Quantity endpoint.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.

        (/api/v1/account/limits/borrow).

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: The asset symbol
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated BackpackRawMaxBorrowQuantity model.

        Raises:
            _handle_validation_error: Re-raised as APIError if validation fails.
        """
        context = f"max borrow quantity ({symbol}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawMaxBorrowQuantity.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackAccountResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_max_order_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        side: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxOrderQuantity:
        """Validate the raw response for the Max Order Quantity endpoint.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.

        (/api/v1/account/limits/order).

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: The trading symbol
            side: The order side (buy/sell)
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated BackpackRawMaxOrderQuantity model.

        Raises:
            _handle_validation_error: Re-raised as APIError if validation fails.
        """
        context = f"max order quantity ({symbol} {side}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawMaxOrderQuantity.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackAccountResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_max_withdrawal_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxWithdrawalQuantity:
        """Validate the raw response for the Max Withdrawal Quantity endpoint.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.

        (/api/v1/account/limits/withdrawal).

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: The asset symbol
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated BackpackRawMaxWithdrawalQuantity model.

        Raises:
            _handle_validation_error: Re-raised as APIError if validation fails.
        """
        context = f"max withdrawal quantity ({symbol}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawMaxWithdrawalQuantity.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackAccountResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e
