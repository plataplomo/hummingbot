"""Backpack Transfer Service.

This service handles all transfer and withdrawal operations for the Backpack exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Internal transfers between account types (SPOT, MARGIN, FUTURES)
- External withdrawals to blockchain addresses
- Network validation and blockchain support
- Transfer validation and error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, ClassVar

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackTransferMapper
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.models.service_args.account import TransferArgs, WithdrawArgs
from cyberdelta.apis.utils import ensure_dict_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.service_validation import (
    InvalidAccountTypeError,
    NetworkRequiredError,
    UnsupportedNetworkError,
)
from cyberdelta.models.operations import Transfer, Withdrawal
from cyberdelta.symbols import exchanges
from cyberdelta.utils.typing import ParsedJsonResponse


# Type alias for raw JSON response from HTTP client
type RawJsonResponse = ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackTransferService:
    """Focused service for Backpack transfer and withdrawal operations.

    Handles internal transfers between account types and external withdrawals
    with comprehensive validation and error handling.
    """

    # Supported blockchain networks for Backpack withdrawals
    BLOCKCHAIN_MAPPING: ClassVar[dict[str, str]] = {
        "Arbitrum": "Arbitrum",
        "Base": "Base",
        "Bitcoin": "Bitcoin",
        "BitcoinCash": "BitcoinCash",
        "BNBSmartChain": "BNBSmartChain",
        "Cardano": "Cardano",
        "Dogecoin": "Dogecoin",
        "Ethereum": "Ethereum",
        "Litecoin": "Litecoin",
        "Polygon": "Polygon",
        "Solana": "Solana",
        "Story": "Story",
        "Sui": "Sui",
        "XRP": "XRP",
    }

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackAccountRequestBuilder,
        response_handler: BackpackAccountResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: ExchangeName = ExchangeName.BACKPACK,
        # NEW: Optional dependency injection for mapper
        mapper: BackpackTransferMapper | None = None,
    ) -> None:
        """Initialize the transfer service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            mapper: Optional transfer mapper instance for dependency injection
                (creates default if not provided)
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        # Provide sensible default if mapper not injected
        self._mapper = mapper or BackpackTransferMapper()

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Performs an internal transfer of funds between account types.

        Supports transfers between SPOT, MARGIN, and FUTURES account types
        within the same exchange account.

        Args:
            args: Transfer parameters including asset, amount, and account types

        Returns:
            Transfer object with transaction details

        Raises:
            APIError: If transfer request fails or validation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "transfer"

        # Validate account types
        self._validate_account_types(args, current_method)

        raw_response_content: str | None = None
        status_code: int = 0

        try:
            logger.info(
                "executing_internal_transfer",
                exchange=self._exchange_name,
                method=current_method,
                asset=args.asset,
                amount=str(args.amount),
                from_account=args.from_account_type,
                to_account=args.to_account_type,
                message="Executing internal transfer between account types",
            )

            # Execute transfer request
            raw_data, status_code = await self._execute_transfer_request(args)
            raw_response_content = str(raw_data)

            # Process and transform response
            transfer_result = self._process_transfer_response(raw_data, args, status_code)

            logger.info(
                "transfer_completed",
                exchange=self._exchange_name,
                method=current_method,
                transfer_id=transfer_result.id,
                status=transfer_result.status,
                message="Internal transfer completed successfully",
            )

        except APIError:
            raise
        except Exception as e:
            self._handle_transfer_exceptions(e, current_method, status_code, raw_response_content)
            # Defensive check - should never reach here
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in transfer",
                original_exception=e,
                http_status=status_code,
            ) from e
        else:
            return transfer_result

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Initiates a withdrawal of funds to an external address.

        Supports withdrawals to various blockchain networks with proper
        network validation and address verification.

        Args:
            args: Withdrawal parameters including asset, amount, address, and network

        Returns:
            Withdrawal object with transaction details

        Raises:
            APIError: If withdrawal request fails or validation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "withdraw"

        # Validate withdrawal network
        self._validate_withdrawal_network(args, current_method)

        raw_response_content: str | None = None
        status_code: int = 0

        try:
            logger.info(
                "executing_withdrawal",
                exchange=self._exchange_name,
                method=current_method,
                asset=args.asset,
                amount=str(args.amount),
                network=args.network,
                address_prefix=args.address[:10] if args.address else None,
                message="Executing withdrawal to external address",
            )

            # Execute withdrawal request
            raw_data, status_code = await self._execute_withdrawal_request(args)
            raw_response_content = str(raw_data)

            # Process and transform response
            withdrawal_result = self._process_withdrawal_response(raw_data, args, status_code)

            logger.info(
                "withdrawal_completed",
                exchange=self._exchange_name,
                method=current_method,
                withdrawal_id=withdrawal_result.id,
                status=withdrawal_result.status,
                message="Withdrawal completed successfully",
            )

        except APIError:
            raise
        except Exception as e:
            self._handle_withdrawal_exceptions(e, current_method, status_code, raw_response_content)
            # Defensive check - should never reach here
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in withdraw",
                original_exception=e,
                http_status=status_code,
            ) from e
        else:
            return withdrawal_result

    def _validate_account_types(self, args: TransferArgs, current_method: str) -> None:
        """Validate account types for transfer operations.

        Args:
            args: Transfer arguments containing account types
            current_method: Name of calling method for error context

        Raises:
            InvalidAccountTypeError: If account types are invalid
        """
        valid_accounts = {"SPOT", "MARGIN", "FUTURES"}

        if args.from_account_type not in valid_accounts:
            raise InvalidAccountTypeError(
                account_type=args.from_account_type,
                parameter_name="from_account_type",
                valid_types=valid_accounts,
                method_name=current_method,
            )

        if args.to_account_type not in valid_accounts:
            raise InvalidAccountTypeError(
                account_type=args.to_account_type,
                parameter_name="to_account_type",
                valid_types=valid_accounts,
                method_name=current_method,
            )

    def _validate_withdrawal_network(self, args: WithdrawArgs, current_method: str) -> None:
        """Validate withdrawal network support.

        Args:
            args: Withdrawal arguments containing network
            current_method: Name of calling method for error context

        Raises:
            NetworkRequiredError: If network is not specified
            UnsupportedNetworkError: If network is not supported
        """
        if args.network is None:
            raise NetworkRequiredError(
                operation="withdrawal",
                method_name=current_method,
            )

        # Validate if the network is supported using the predefined mapping
        if args.network not in self.BLOCKCHAIN_MAPPING:
            raise UnsupportedNetworkError(
                network=args.network,
                supported_networks=list(self.BLOCKCHAIN_MAPPING.keys()),
                method_name=current_method,
            )

    async def _execute_transfer_request(self, args: TransferArgs) -> tuple[ParsedJsonResponse, int]:
        """Execute the internal transfer API request.

        Args:
            args: Transfer arguments

        Returns:
            Tuple of validated response data and status code
        """
        endpoint_path = "/api/v1/capital/transfer"

        # Build transfer payload
        # Account types are already validated by _validate_account_types
        asset_symbol = exchanges.backpack(value=args.asset)
        payload = self._request_builder.build_internal_transfer_payload(
            asset_symbol=asset_symbol,
            amount=args.amount,
            from_wallet=args.from_account_type,
            to_wallet=args.to_account_type,
        )

        logger.debug(
            "requesting_transfer",
            exchange=self._exchange_name,
            endpoint_path=endpoint_path,
            payload=payload,
            message="Requesting transfer from endpoint",
        )

        # Execute API request
        raw_data, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload,
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,
                endpoint_group="private",
                request_weight=1,
            ),
        )

        logger.debug(
            "raw_transfer_response",
            exchange=self._exchange_name,
            status_code=status_code,
            message="Raw transfer response received",
        )

        # Validate response format
        validated_data = ensure_dict_response(raw_data, "transfer", status_code)
        return validated_data, status_code

    async def _execute_withdrawal_request(
        self,
        args: WithdrawArgs,
    ) -> tuple[ParsedJsonResponse, int]:
        """Execute the withdrawal API request.

        Args:
            args: Withdrawal arguments

        Returns:
            Tuple of validated response data and status code

        Raises:
            NetworkRequiredError: If network is None (defensive check)
        """
        endpoint_path = "/api/v1/capital/withdrawals"

        # Defensive check - ensure network is not None
        if args.network is None:
            raise NetworkRequiredError(operation="withdrawal")

        # Build withdrawal payload
        asset_symbol = exchanges.backpack(value=args.asset)
        payload = self._request_builder.build_withdraw_payload(
            asset_symbol=asset_symbol,
            amount=args.amount,
            address=args.address,
            network=args.network,
            tag=args.tag,
            client_withdraw_id=args.client_withdrawal_id,
        )

        logger.debug(
            "requesting_withdrawal",
            exchange=self._exchange_name,
            endpoint_path=endpoint_path,
            payload=payload,
            message="Requesting withdrawal from endpoint",
        )

        # Execute API request
        raw_data, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload,
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,
                endpoint_group="private",
                request_weight=1,
            ),
        )

        logger.debug(
            "raw_withdrawal_response",
            exchange=self._exchange_name,
            status_code=status_code,
            message="Raw withdrawal response received",
        )

        # Validate response format
        validated_data = ensure_dict_response(raw_data, "withdrawal", status_code)
        return validated_data, status_code

    def _process_transfer_response(
        self,
        raw_data: ParsedJsonResponse,
        args: TransferArgs,
        status_code: int,
    ) -> Transfer:
        """Process and transform transfer response.

        Args:
            raw_data: Raw response data from API
            args: Original transfer arguments
            status_code: HTTP status code

        Returns:
            Transformed Transfer object

        Raises:
            APIError: If response processing fails
        """
        # Handle response through response handler
        validated_raw_json_response: RawJsonResponse = (
            self._response_handler.handle_transfer_response(raw_data, status_code)
        )

        # Validate response type
        if not isinstance(validated_raw_json_response, dict):
            raise APIError(
                message=(
                    f"Transfer response handler returned unexpected type: "
                    f"{type(validated_raw_json_response)}, expected dict."
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        # Transform to internal model
        internal_transfer = self._mapper.transform_raw_transfer_to_internal(
            raw_response=validated_raw_json_response,
            exchange_name=self._exchange_name,
            asset=args.asset,
            quantity=args.amount,
            from_account_type_raw=args.from_account_type,
            to_account_type_raw=args.to_account_type,
            client_transfer_id=args.client_transfer_id,
        )

        logger.debug(
            "transfer_mapped",
            exchange=self._exchange_name,
            transfer_id=internal_transfer.id,
            message="Successfully mapped internal transfer",
        )

        return internal_transfer

    def _process_withdrawal_response(
        self,
        raw_data: ParsedJsonResponse,
        args: WithdrawArgs,
        status_code: int,
    ) -> Withdrawal:
        """Process and transform withdrawal response.

        Args:
            raw_data: Raw response data from API
            args: Original withdrawal arguments
            status_code: HTTP status code

        Returns:
            Transformed Withdrawal object
        """
        # Handle response through response handler
        raw_withdrawal_model: BackpackRawWithdrawalResponse = (
            self._response_handler.handle_withdraw_response(raw_data, status_code)
        )

        # Transform to internal model
        internal_withdrawal = self._mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_withdrawal_model,
            asset=args.asset,
            quantity=args.amount,
            address=args.address,
            network=args.network,
            client_withdrawal_id=args.client_withdrawal_id,
            tag=args.tag,
        )

        logger.debug(
            "withdrawal_mapped",
            exchange=self._exchange_name,
            withdrawal_id=internal_withdrawal.id,
            message="Successfully mapped internal withdrawal",
        )

        return internal_withdrawal

    def _handle_transfer_exceptions(
        self,
        e: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle various transfer-related exceptions.

        Args:
            e: The exception that occurred
            current_method: Name of the calling method
            status_code: HTTP status code if available
            raw_response_content: Raw response content for context

        Raises:
            APIError: Wrapped exception with appropriate error code
        """
        if isinstance(e, APIError):
            raise e

        if isinstance(e, TransformationError):
            logger.error(
                "transform_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Failed to transform exchange data for transfer",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

        if isinstance(e, ValidationError):
            logger.error(
                "validation_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Internal data validation failed for transfer",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

        if isinstance(e, (ValueError, TypeError)):
            error_msg = str(e)
            # Re-raise validation errors from our own parameter validation
            if current_method in error_msg and any(
                param in error_msg for param in ["from_account_type", "to_account_type"]
            ):
                raise e

            logger.error(
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Service internal logic error for transfer",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

        # Default case for unexpected exceptions
        logger.error(
            "unexpected_service_failure",
            exchange=self._exchange_name,
            method=current_method,
            error=str(e),
            message="Unexpected service failure for transfer",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e

    def _handle_withdrawal_exceptions(
        self,
        e: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle various withdrawal-related exceptions.

        Args:
            e: The exception that occurred
            current_method: Name of the calling method
            status_code: HTTP status code if available
            raw_response_content: Raw response content for context

        Raises:
            APIError: Wrapped exception with appropriate error code
        """
        if isinstance(e, APIError):
            raise e

        if isinstance(e, TransformationError):
            logger.error(
                "transform_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Failed to transform exchange data for withdrawal",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

        if isinstance(e, ValidationError):
            logger.error(
                "validation_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Internal data validation failed for withdrawal",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

        if isinstance(e, (ValueError, TypeError)):
            error_msg = str(e)
            # Re-raise validation errors from our own parameter validation
            if current_method in error_msg and "network" in error_msg:
                raise e

            logger.error(
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Service internal logic error for withdrawal",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

        # Default case for unexpected exceptions
        logger.error(
            "unexpected_service_failure",
            exchange=self._exchange_name,
            method=current_method,
            error=str(e),
            message="Unexpected service failure for withdrawal",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e
